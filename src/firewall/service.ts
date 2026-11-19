/** @file Owns one agent's message policy: its checks, the content they permit, its edits and its saved decisions. */

import {
  Clock,
  Context,
  Effect,
  Exit,
  Layer,
  Schedule,
  Schema,
  Struct,
} from "effect";
import { createHash } from "node:crypto";
import type { Message } from "../communication/index.js";
import {
  decodeJson,
  exactStruct,
  MAXIMUM_RUNTIME_ITEM_BYTES,
} from "../encoding.js";
import { type AgentCard, Identity } from "../identity.js";
import {
  type AssessmentText,
  checkCapturedMessage,
  type CheckEvidence,
  type DecisionProvider,
  type DecisionProviderError,
  decodeFrozen,
  deepFreeze,
  type FirewallCheck,
  type FirewallEvidence,
  FirewallRefusal,
  type HistoryEntry,
  type LocalGuidance,
  textQuestionEdition,
  validateTextRules,
} from "./check.js";
import {
  type ContractCheck,
  contractReply,
  messageKey,
  prepareContract,
} from "./contract-check.js";
import {
  type CompiledContract,
  compiledContractSchema,
  isDirectText,
} from "./contract.js";
import {
  type Candidate,
  contractRefusal,
  type Decision,
  mintCheckId,
} from "./decision.js";
import {
  editedContracts,
  editedRules,
  type PolicyEdit,
  resolvedChange,
} from "./editable-policy.js";
import { makeJevDecisionProvider } from "./jev.js";
import {
  type Direction,
  editableRulesSchema,
  type firewallDecisionsSchema,
  type firewallDecisionSummarySchema,
  FirewallPolicyError,
  type FirewallRefusalReason,
  type LoadedMessagePolicy,
  type policyReadSchema,
  type policyRevisionSchema,
  TextRule,
} from "./rules.js";
import {
  type CheckRecord,
  type FirewallActivation,
  type FirewallStore,
  makeFirewallStore,
  persistenceFailure,
} from "./store.js";

/**
 * A message policy as configured, which edits leave unchanged, and the
 * decision provider that judges its text rules. Reject and hold judgments,
 * including provider failures, refuse sends and withhold incoming messages.
 */
interface ConfiguredPolicy {
  readonly policy: LoadedMessagePolicy["policy"];
  readonly provider?: DecisionProvider;
}

type InstalledPolicy = Effect.Effect.Success<ReturnType<typeof initialPolicy>>;

/**
 * Run `save` before returning refusal or submitting this exact message.
 * Saving is repeatable and uses the caller's current SQL transaction.
 */
type OutgoingDecision = {
  readonly save: Effect.Effect<void, FirewallPolicyError>;
} & (
  | { readonly kind: "allowed" }
  | { readonly kind: "refused"; readonly refusal: FirewallRefusal }
);

/**
 * Run `save` in the same SQL transaction as the inbox write.
 * Saving is repeatable. A withheld message with `reply` gets that text back, as
 * an ordinary message from this agent in the same conversation.
 */
export type IncomingDecision = {
  readonly save: Effect.Effect<void, FirewallPolicyError>;
} & (
  | {
      readonly kind: "allowed";
      readonly guidance?: LocalGuidance;
    }
  | {
      readonly kind: "withheld";
      /** Why the message was withheld. */
      readonly reason: FirewallRefusalReason;
      readonly reply?: string;
    }
);
/** Message checks, policy edits and saved decision summaries. */
interface FirewallService {
  /** Checks without saving. Refusals expose feedback. */
  readonly checkOutgoing: (
    message: Message,
    evidence: FirewallEvidence,
  ) => Effect.Effect<OutgoingDecision>;
  /**
   * Checks a message a peer wrote with its preceding evidence. An allowed
   * message the provider was unsure about carries guidance; a withheld one
   * exposes none.
   */
  readonly checkIncoming: (
    message: Message,
    evidence: FirewallEvidence,
  ) => Effect.Effect<IncomingDecision>;
  readonly readPolicy: () => Effect.Effect<
    typeof policyReadSchema.Type,
    FirewallPolicyError
  >;
  /** The contracts the agent holds in its active revision. */
  readonly readContracts: () => Effect.Effect<readonly CompiledContract[]>;
  /**
   * Whether the agent has ever held a contract. A check then needs every
   * message before the one under check as `protocolHistory`, since an
   * instance outlives the removal of its contract.
   */
  readonly requiresProtocolHistory: () => boolean;
  /** Validates each edit before saving and activating it. */
  readonly updatePolicy: (
    request: PolicyEdit,
  ) => Effect.Effect<typeof policyRevisionSchema.Type, FirewallPolicyError>;
  readonly readDecisions: (request: {
    readonly since?: number;
  }) => Effect.Effect<typeof firewallDecisionsSchema.Type, FirewallPolicyError>;
}

/** One agent's firewall: it checks messages against the message policy, edits that policy and reads its saved decisions. */
export class Firewall extends Context.Tag("SocialHarness/Firewall")<
  Firewall,
  FirewallService
>() {
  /**
   * Policy resources are fixed for this scope. Identity supplies this agent's
   * card. An agent with no saved policy starts holding `contracts`; structure
   * and protocol checks of a direct message's act apply before the text rules
   * of `source`, and a contract's text rules join the fixed rules. `provider`
   * judges the text rules in place of the provider `source` names.
   */
  static readonly layer = (
    agentKey: string,
    source?: LoadedMessagePolicy,
    contracts: readonly CompiledContract[] = [],
    provider?: DecisionProvider,
  ) =>
    Layer.effect(
      Firewall,
      Effect.gen(function* () {
        const identity = yield* Identity;
        const store = yield* makeFirewallStore(agentKey);
        const policy =
          source === undefined
            ? undefined
            : yield* initialPolicy(store, source, contracts, provider);
        const installed: Installation = {
          store,
          ...(policy === undefined ? {} : { policy }),
          localAgent: identity.me,
        };
        return Firewall.of({
          readPolicy: () =>
            policy === undefined
              ? Effect.fail(
                  new FirewallPolicyError({ reason: "not-configured" }),
                )
              : Effect.sync(() => ({
                  rules: policy.active.editableRules,
                  fixedRules: policy.active.fixedRules,
                  norms: policy.active.contracts.map(
                    (contract) => contract.name,
                  ),
                  activeRevision: policy.active.revision,
                })),
          readContracts: () =>
            Effect.sync(() => policy?.active.contracts ?? []),
          requiresProtocolHistory: () => policy?.active.contractsUsed ?? false,
          updatePolicy: (request) =>
            policy === undefined
              ? Effect.fail(
                  new FirewallPolicyError({ reason: "not-configured" }),
                )
              : updatePolicy(policy, request),
          readDecisions: (request) => store.readDecisions(request),
          checkOutgoing: (message, evidence) =>
            Effect.gen(function* () {
              const decision = yield* decide(
                installed,
                { message, evidence },
                "outgoing",
              );
              const save = saveRecords(store, decision);
              if (decision.kind === "allowed") {
                return { kind: "allowed", save } satisfies OutgoingDecision;
              }
              const { refusal } = decision;
              return {
                kind: "refused",
                save,
                refusal: new FirewallRefusal({
                  reason: "firewall-refused",
                  checkId: decision.record.checkId,
                  refusal: refusal.reason,
                  ...(refusal.ruleId === undefined
                    ? {}
                    : { rule: refusal.ruleId }),
                  feedback: refusal.feedback,
                  ...(refusal.detail === undefined
                    ? {}
                    : { detail: refusal.detail }),
                }),
              } satisfies OutgoingDecision;
            }),
          checkIncoming: (message, evidence) =>
            Effect.gen(function* () {
              const decision = yield* decide(
                installed,
                { message, evidence },
                "incoming",
              );
              const save = saveRecords(store, decision);
              if (decision.kind === "allowed") {
                return {
                  kind: "allowed",
                  save,
                  ...(decision.guidance === undefined
                    ? {}
                    : { guidance: decision.guidance }),
                } satisfies IncomingDecision;
              }
              const { reply } = decision.refusal;
              return {
                kind: "withheld",
                reason: decision.refusal.reason,
                save,
                ...(reply === undefined ? {} : { reply }),
              } satisfies IncomingDecision;
            }),
        });
      }),
    );
}

/**
 * One candidate the text rules are to judge: the rules that apply to it, and
 * for an act a contract governs, the contract's check of it and what the
 * provider reads of its protocol.
 */
interface TextCheckPlan {
  readonly candidate: Candidate;
  readonly active: ActivePolicy;
  readonly contract?: Extract<ContractCheck, { readonly kind: "ready" }>;
  readonly protocol?: CheckEvidence["protocol"];
}

/** The message policy one firewall applies. */
interface Installation {
  readonly store: FirewallStore;
  readonly policy?: InstalledPolicy;
  readonly localAgent: AgentCard;
}

const storedPolicyFormat = "org.social-harness/editable-policy/v1";

/** How many checks run at once under a policy that sets no `maximumConcurrentChecks`. */
const defaultConcurrentChecks = 4;

const storedPolicySchema = exactStruct({
  format: Schema.Literal(storedPolicyFormat),
  rules: editableRulesSchema,
  contracts: Schema.Array(compiledContractSchema),
  contractsUsed: Schema.Boolean,
});

type ActivePolicy = Effect.Effect.Success<ReturnType<typeof buildPolicy>>;

type DecisionSummary = typeof firewallDecisionSummarySchema.Type;

/**
 * Decides a candidate. A direct message of an agent that has held a contract
 * is first read as an act and checked for structure and protocol; a failure
 * refuses it. The text rules then judge the candidate: the owner's rules and
 * the rules its contract gives its act. An agent without a message policy
 * allows every candidate.
 */
function decide(
  installed: Installation,
  candidate: Candidate,
  direction: Direction,
) {
  return Effect.gen(function* () {
    const policy = installed.policy;
    if (policy === undefined) {
      return { kind: "allowed" } satisfies Decision;
    }
    const active = policy.active;
    const contract =
      active.contractsUsed && isDirectText(candidate.message)
        ? yield* prepareContract(
            active.contracts,
            installed.store,
            candidate.message,
            candidate.evidence,
          )
        : undefined;
    if (contract?.kind === "refused") {
      return contractRefusal(active.revision, candidate, direction, contract);
    }
    return yield* checkMessageText(
      policy,
      installed.localAgent,
      textCheckPlan(active, candidate, contract),
      direction,
    );
  });
}

/**
 * The text rules that judge a candidate: the owner's and those its contract
 * gives its act. The contract's come from the contract its instance began
 * under, which the agent may have since replaced or removed.
 */
function textCheckPlan(
  active: ActivePolicy,
  candidate: Candidate,
  contract?: ContractCheck,
): TextCheckPlan {
  const ready = contract?.kind === "ready" ? contract : undefined;
  const ownerRules = active.textRules.filter(
    (rule) => !active.contractRuleIds.has(rule.id),
  );
  const act = ready?.record.act.name;
  const contractRules = (ready?.contract.textRules ?? [])
    .filter((rule) => rule.act === undefined || rule.act === act)
    .map((rule) => new TextRule(Struct.omit(rule, "act")));
  const form = ready?.contract.forms.find((declared) => declared.act === act);
  return {
    candidate,
    active: {
      ...active,
      textRules: [...ownerRules, ...contractRules],
      fixedRuleIds: [
        ...active.fixedRuleIds,
        ...contractRules.map((rule) => rule.id),
      ],
    },
    ...(ready === undefined ? {} : { contract: ready }),
    ...(ready === undefined || form === undefined
      ? {}
      : {
          protocol: {
            norm: ready.contract.name,
            digest: ready.contract.digest,
            act: ready.record.act.name,
            description: ready.contract.description,
            state: [...(ready.view.instance?.facts.keys() ?? [])],
            form: form.fields,
          },
        }),
  };
}

/** Saves the decision's check record when it has one. */
function saveRecords(store: FirewallStore, decision: Decision) {
  return decision.record === undefined
    ? Effect.void
    : store.saveCheckRecord(decision.record);
}

/**
 * One check of the planned candidate against its text rules, with the history
 * before it. Its record carries the exact revision and evidence that delivery
 * commits. The record of an act a contract governs also carries the
 * contract's norm and digest, and for an allowed act its protocol record,
 * which later checks replay. A refusal of such an act carries the contract's
 * diagnostics, and a withheld one is answered with the `:on-refusal` act when
 * one is declared.
 */
function checkMessageText(
  policy: InstalledPolicy,
  self: AgentCard,
  { candidate, active, contract, protocol }: TextCheckPlan,
  direction: Direction,
) {
  return Effect.gen(function* () {
    const { message } = candidate;
    const { configured } = policy;
    const now = yield* Clock.currentTimeMillis;
    const cards = new Map<string, AgentCard>();
    for (const card of [
      self,
      message.author,
      ...message.recipients,
      ...candidate.evidence.history.flatMap((entry) => [
        entry.message.author,
        ...entry.message.recipients,
      ]),
    ]) {
      cards.set(card.agentId, card);
    }
    const agents = [...cards.values()];
    const history: HistoryEntry[] = candidate.evidence.history.map((entry) => ({
      ...assessmentText(entry.message),
      visibleToAgent:
        entry.visibleToAgent || entry.message.author.agentId === self.agentId,
    }));
    const evidence: CheckEvidence = {
      agentId: self.agentId,
      principal: self.principalId,
      participants: agents.map((card) => ({
        agentId: card.agentId,
        principal: card.principalId,
      })),
      history,
      earlierHistoryOmitted: candidate.evidence.earlierHistoryOmitted,
      now,
      facts: configured.policy.facts,
      gaps: candidate.evidence.gaps,
      ...(protocol === undefined ? {} : { protocol }),
    };
    const text = assessmentText(message);
    return yield* Effect.gen(function* () {
      const checkId = mintCheckId();
      const concluded = yield* checkCapturedMessage(
        {
          textRules: active.textRules,
          revision: active.revision,
          ...(configured.provider === undefined
            ? {}
            : {
                providerSettings: configured.policy.textSettings,
                provider: configured.provider,
              }),
        },
        { event: { direction, message: text }, evidence },
      ).pipe(
        Effect.catchTag("InvalidCheckError", () =>
          Effect.succeed(unavailable()),
        ),
        markCheckActivity(direction),
      );
      const { check, guidance } = guidanceProjection(concluded, checkId);
      const detail =
        contract === undefined
          ? undefined
          : {
              norm: contract.contract.name,
              digest: contract.contract.digest,
              act: contract.record.act.name,
              stage: "semantic",
            };
      const record: CheckRecord = {
        checkId,
        messageKey: messageKey(message),
        direction,
        messageJson: JSON.stringify(message),
        inputJson: JSON.stringify({
          revision: active.revision,
          revisionContents: active.revisionContents,
          evidence,
          agents,
          message: text,
          ...(detail === undefined
            ? {}
            : { norm: detail.norm, digest: detail.digest }),
        }),
        allowed: check.reason === "eligible",
        resultJson: JSON.stringify({
          check: Struct.omit(check, "decidedBy"),
          summaries: (direction === "outgoing"
            ? message.recipients
            : [message.author]
          ).map((peer) =>
            decisionSummary(
              active,
              check,
              direction,
              cards.get(peer.agentId) ?? peer,
            ),
          ),
          ...(detail === undefined ? {} : { detail }),
          ...(contract === undefined || check.reason !== "eligible"
            ? {}
            : { protocol: contract.record }),
        }),
      };
      if (check.reason === "eligible") {
        const allowed: Decision = {
          kind: "allowed",
          record,
          ...(guidance === undefined ? {} : { guidance }),
        };
        return allowed;
      }
      const reply =
        direction === "incoming"
          ? (contractRefusalReply(contract, check) ?? textReply(active, check))
          : "";
      const refused: Decision = {
        kind: "refused",
        record,
        refusal: {
          reason: check.reason,
          feedback: check.feedback,
          ...(detail === undefined ? {} : { detail }),
          ...(reply === "" ? {} : { reply }),
        },
      };
      return refused;
    }).pipe(Effect.withSpan("assessMessage"), policy.permits.withPermits(1));
  });
}

function assessmentText(message: Message): AssessmentText {
  const [first, ...rest] = message.recipients;
  return {
    id: referenceId(`${message.author.agentId}:${message.msgId}`),
    sender: message.author.agentId,
    recipients: [first.agentId, ...rest.map((card) => card.agentId)],
    text: message.content
      .map((part) =>
        part.type === "text" ? part.text : JSON.stringify(part.value),
      )
      .join("\n"),
  };
}

function referenceId(reference: string): string {
  return `msg_${createHash("sha256").update(reference).digest("hex")}`;
}

/**
 * Installs the message policy `source` gives. Its revision covers the held
 * contracts, so a decision names the rules that applied. Without `provider`,
 * Jev judges the text rules of a policy that names it.
 */
function initialPolicy(
  store: FirewallStore,
  source: LoadedMessagePolicy,
  initialContracts: readonly CompiledContract[],
  provider?: DecisionProvider,
) {
  return Effect.gen(function* () {
    const configured: ConfiguredPolicy = {
      policy: source.policy,
      provider:
        provider ??
        (source.provider === undefined
          ? undefined
          : retryingTransientFailures(
              makeJevDecisionProvider(source.provider),
            )),
    };
    const { saved, editableRules, contracts, contractsUsed } =
      yield* loadEditablePolicy(store, configured, initialContracts);
    const active = yield* buildPolicy(
      configured,
      editableRules,
      contracts,
      contractsUsed,
    );
    if (saved === undefined) {
      yield* saveEditablePolicy(store, active);
    }
    return {
      store,
      configured,
      active,
      edits: yield* Effect.makeSemaphore(1),
      permits: yield* Effect.makeSemaphore(
        configured.policy.maximumConcurrentChecks ?? defaultConcurrentChecks,
      ),
    };
  });
}

/** The saved editable rules and contracts, or the initial ones for an agent with none saved. */
function loadEditablePolicy(
  store: FirewallStore,
  configured: ConfiguredPolicy,
  initialContracts: readonly CompiledContract[],
) {
  return Effect.gen(function* () {
    const savedJson = yield* store.readEditablePolicy();
    const saved =
      savedJson === undefined
        ? undefined
        : yield* decodeJson(storedPolicySchema, savedJson).pipe(
            Effect.mapError(persistenceFailure),
          );
    const editableRules =
      saved?.rules ?? configured.policy.text?.initialRules ?? [];
    const contracts =
      saved?.contracts ??
      (yield* decodeFrozen(
        Schema.Array(compiledContractSchema),
        initialContracts,
      ).pipe(
        Effect.catchTag("ParseError", () =>
          Effect.fail(new FirewallPolicyError({ reason: "build-failed" })),
        ),
      ));
    deepFreeze(editableRules);
    return {
      saved,
      editableRules,
      contracts,
      contractsUsed: saved?.contractsUsed === true || contracts.length !== 0,
    };
  });
}

/** Only transient transport, rate-limit and server failures share the stage's bounded retry window. */
function retryingTransientFailures(
  provider: DecisionProvider,
): DecisionProvider {
  return {
    ...provider,
    decide: (request) =>
      Effect.suspend(() => provider.decide(request)).pipe(
        Effect.retry({
          times: 2,
          schedule: Schedule.exponential("250 millis"),
          while: isTransient,
        }),
      ),
  };
}

function isTransient(error: DecisionProviderError): boolean {
  const status = error.status ?? 0;
  return (
    error.reason === "transport" ||
    (error.reason === "http" && (status === 429 || status >= 500))
  );
}

/**
 * What the sender of a withheld message is told: each violated `reply` rule's
 * registered feedback, one per line. Holds reply nothing: the sender broke no
 * settled rule.
 */
function textReply(active: ActivePolicy, check: FirewallCheck): string {
  if (check.reason !== "rule-violation") {
    return "";
  }
  const violated = new Set(check.decidedBy.map((rule) => rule.ruleId));
  return active.textRules
    .filter((rule) => violated.has(rule.id) && rule.action === "reply")
    .flatMap((rule) =>
      rule.feedback === undefined ? [] : [rule.feedback.text],
    )
    .join("\n");
}

/**
 * The act that answers the sender of an act the text rules withheld, when its
 * contract declares one: it carries the refusal's feedback.
 */
function contractRefusalReply(
  contract: TextCheckPlan["contract"],
  check: FirewallCheck,
) {
  return contract === undefined
    ? undefined
    : contractReply(
        contract,
        check.feedback.map((entry) => entry.text).join(" ") ||
          `The semantic policy check returned ${check.reason}.`,
      );
}

/** The summary a host reads of one check for one peer, naming the same captured rules as the decision. */
function decisionSummary(
  active: ActivePolicy,
  check: FirewallCheck,
  direction: Direction,
  peer: AgentCard,
): DecisionSummary {
  return {
    direction,
    peer,
    judgment: check.judgment,
    reason: check.reason,
    decidedBy: check.decidedBy.map((rule) => ({
      ruleId: rule.ruleId,
      from: active.fixedRuleIds.includes(rule.ruleId) ? "fixed" : "editable",
      ...(rule.advice === undefined ? {} : { advice: rule.advice }),
    })),
    checkedRuleIds: [
      ...new Set(
        check.findings.flatMap((finding) =>
          finding.kind === "not-applicable" ? [] : [finding.ruleId],
        ),
      ),
    ],
    activeRevision: active.revision,
  };
}

/**
 * The guidance of an allowed incoming message the provider was unsure about.
 * Guidance over `MAXIMUM_RUNTIME_ITEM_BYTES` makes the check unavailable.
 */
function guidanceProjection(check: FirewallCheck, checkId: string) {
  if (check.instructions.length === 0) {
    return { check };
  }
  const guidance: LocalGuidance = {
    checkId,
    instructions: check.instructions,
  };
  return Buffer.byteLength(JSON.stringify(guidance)) >
    MAXIMUM_RUNTIME_ITEM_BYTES
    ? { check: { ...unavailable(), findings: check.findings } }
    : { check, guidance };
}

/** A check that could not run holds without inventing a deciding rule. */
function unavailable(): FirewallCheck {
  return {
    judgment: "hold",
    reason: "check-unavailable",
    feedback: [],
    instructions: [],
    findings: [],
    decidedBy: [],
  };
}

function updatePolicy(policy: InstalledPolicy, request: PolicyEdit) {
  return policy.edits.withPermits(1)(
    Effect.gen(function* () {
      const change = yield* resolvedChange(request);
      const editableRules = yield* editedRules(
        policy.active.editableRules,
        policy.active.fixedRuleIds,
        change,
      );
      const contracts = yield* editedContracts(policy.active.contracts, change);
      const replacement = yield* buildPolicy(
        policy.configured,
        editableRules,
        contracts,
        policy.active.contractsUsed,
      );
      const removedContracts = new Set(
        change.removeContracts.map(({ name }) => name),
      );
      return yield* Effect.uninterruptible(
        Effect.gen(function* () {
          yield* saveEditablePolicy(policy.store, replacement, {
            time: yield* Clock.currentTimeMillis,
            activeRevision: replacement.revision,
            addedRuleIds: [
              ...change.addRules.map(({ rule }) => rule.id),
              ...change.addContracts.flatMap(({ contract }) =>
                contract.textRules.map((rule) => rule.id),
              ),
            ],
            removedRuleIds: [
              ...change.removeRules.map(({ ruleId }) => ruleId),
              ...policy.active.contracts
                .filter((contract) => removedContracts.has(contract.name))
                .flatMap((contract) =>
                  contract.textRules.map((rule) => rule.id),
                ),
            ],
          });
          yield* Effect.sync(() => {
            policy.active = replacement;
          });
          return { activeRevision: replacement.revision };
        }),
      );
    }),
  );
}

/**
 * One activation of the message policy: the configured fixed rules, the held
 * contracts' text rules and the editable rules. `contractsUsed` stays true once the agent has held a contract.
 */
function buildPolicy(
  configured: ConfiguredPolicy,
  editableRules: readonly TextRule[],
  contracts: readonly CompiledContract[],
  contractsUsed: boolean,
) {
  return Effect.gen(function* () {
    if (
      new Set(contracts.map((contract) => contract.name)).size !==
      contracts.length
    ) {
      return yield* new FirewallPolicyError({ reason: "build-failed" });
    }
    const contractRules = yield* decodeFrozen(
      Schema.Array(TextRule),
      contracts.flatMap((contract) =>
        contract.textRules.map((rule) => Struct.omit(rule, "act")),
      ),
    ).pipe(
      Effect.catchTag("ParseError", () =>
        Effect.fail(new FirewallPolicyError({ reason: "build-failed" })),
      ),
    );
    const { policy, provider } = configured;
    const fixed = [...(policy.text?.fixedRules ?? []), ...contractRules];
    const fixedRuleIds = fixed.map((rule) => rule.id);
    const collision = editableRules.find((rule) =>
      fixedRuleIds.includes(rule.id),
    );
    if (collision !== undefined) {
      return yield* new FirewallPolicyError({
        reason: "fixed-rule",
        ruleId: collision.id,
      });
    }
    const textRules = [...fixed, ...editableRules];
    if (textRules.length > 0 && provider === undefined) {
      return yield* new FirewallPolicyError({ reason: "not-configured" });
    }
    const revisionContents = {
      configuration: {
        facts: policy.facts,
        maximumConcurrentChecks:
          policy.maximumConcurrentChecks ?? defaultConcurrentChecks,
      },
      questionEdition: textQuestionEdition,
      textRules,
      fixedRuleIds,
      contracts: contracts.map((contract) => contract.digest),
      ...(provider === undefined
        ? {}
        : {
            providerSettings: policy.textSettings,
            provider: provider.provider ?? null,
            model: provider.model ?? null,
          }),
    };
    const revision = digest(revisionContents);
    yield* validateTextRules({
      textRules,
      revision,
      ...(provider === undefined
        ? {}
        : { providerSettings: policy.textSettings }),
    });
    return {
      revisionContents,
      editableRules,
      contracts,
      contractsUsed: contractsUsed || contracts.length !== 0,
      contractRuleIds: new Set(contractRules.map((rule) => rule.id)),
      revision,
      textRules,
      fixedRules: fixed,
      fixedRuleIds,
    };
  }).pipe(
    Effect.catchTag("InvalidCheckError", () =>
      Effect.fail(new FirewallPolicyError({ reason: "build-failed" })),
    ),
    Effect.catchAllDefect(() =>
      Effect.fail(new FirewallPolicyError({ reason: "build-failed" })),
    ),
  );
}

/**
 * Hashes the JSON text of what it is given. Every part of that is decoded
 * through a struct schema, which fixes the order of its members.
 */
function digest(hashed: unknown) {
  return createHash("sha256").update(JSON.stringify(hashed)).digest("hex");
}

function saveEditablePolicy(
  store: FirewallStore,
  active: ActivePolicy,
  activation?: FirewallActivation,
) {
  return Effect.sync(() =>
    JSON.stringify({
      format: storedPolicyFormat,
      rules: active.editableRules,
      contracts: active.contracts,
      contractsUsed: active.contractsUsed,
    }),
  ).pipe(
    Effect.flatMap((editablePolicyJson) =>
      store.writeEditablePolicy(editablePolicyJson, activation),
    ),
  );
}

const activityKind = "firewall-check";
/** How a marked check ended. */
type ActivityEnding = "checked" | "interrupted" | "failed";

/** Pairs a pending and a settled span around one check, even when it fails or is cancelled. */
function markCheckActivity(direction: Direction) {
  return <A, E, R>(work: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
    Effect.uninterruptibleMask((restore) =>
      checkPending(direction).pipe(
        Effect.zipRight(restore(work)),
        Effect.onExit((exit) => {
          if (Exit.isSuccess(exit)) {
            return checkSettled(direction, "checked");
          }
          return checkSettled(
            direction,
            Exit.isInterrupted(exit) ? "interrupted" : "failed",
          );
        }),
      ),
    );
}

/** Opens the span that `checkSettled` closes for the same direction. */
function checkPending(direction: Direction): Effect.Effect<void> {
  return Effect.withSpan(Effect.void, "retry.pending", {
    captureStackTrace: false,
    attributes: { kind: activityKind, id: direction },
  });
}

/** Closes the span `checkPending` opened. */
function checkSettled(
  direction: Direction,
  ending: ActivityEnding,
): Effect.Effect<void> {
  return Effect.withSpan(Effect.void, "retry.settled", {
    captureStackTrace: false,
    attributes: { kind: activityKind, id: direction, outcome: ending },
  });
}
