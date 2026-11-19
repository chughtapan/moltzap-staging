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
import type { Message, MessageReference } from "../communication/index.js";
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
  decidingRules,
  type DecisionProvider,
  type DecisionProviderError,
  decodeFrozen,
  deepFreeze,
  type FirewallCheck,
  type FirewallEvidence,
  FirewallRefusal,
  type HistoryEntry,
  InvalidCheckError,
  type LocalGuidance,
  type PermittedItem,
  textQuestionEdition,
  validateChecks,
} from "./check.js";
import {
  type ContractCheck,
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
  withContract,
} from "./decision.js";
import {
  editedContracts,
  editedRules,
  invalidRule,
  type PolicyEdit,
  resolvedChange,
} from "./editable-policy.js";
import { makeJevDecisionProvider } from "./jev.js";
import {
  CheckLimits,
  type Direction,
  editableRulesSchema,
  type FirewallConfiguration,
  firewallConfigurationSchema,
  type firewallDecisionsSchema,
  type firewallDecisionSummarySchema,
  FirewallPolicyError,
  type FirewallRefusalReason,
  type LoadedMessagePolicy,
  type policyReadSchema,
  type policyRevisionSchema,
  ProviderRequestSettings,
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
 * A message policy composed in code: its limits, owner facts and text rules
 * with their decision provider. The provider stays fixed while authorized
 * edits change text rules. Reject and hold judgments, including provider
 * failures, refuse sends and withhold incoming messages.
 */
export interface ComposedMessagePolicy {
  readonly limits: CheckLimits;
  readonly facts: CheckEvidence["facts"];
  readonly maximumHistoryRecords?: number;
  readonly maximumConcurrentChecks?: number;
  readonly text?: {
    readonly fixedRules?: readonly TextRule[];
    readonly initialRules?: readonly TextRule[];
    readonly provider: DecisionProvider;
    readonly providerSettings: ProviderRequestSettings;
  };
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
 * Run `save` in the same SQL transaction as the inbox write and visibility update.
 * Saving is repeatable. A withheld message with `reply` gets that text back, as
 * an ordinary message from this agent in the same conversation.
 */
export type IncomingDecision = {
  readonly save: Effect.Effect<void, FirewallPolicyError>;
  readonly visibility: ReadonlyArray<{
    readonly message: MessageReference;
    readonly visibleToAgent: boolean;
  }>;
} & (
  | {
      readonly kind: "allowed";
      readonly item: PermittedItem;
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
  ) => Effect.Effect<OutgoingDecision, InvalidCheckError>;
  /**
   * Checks a message a peer wrote with its preceding evidence. An allowed
   * message the provider was unsure about carries guidance; a withheld one
   * exposes none.
   */
  readonly checkIncoming: (
    message: Message,
    evidence: FirewallEvidence,
  ) => Effect.Effect<IncomingDecision, InvalidCheckError>;
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
   * of `source`, and a contract's text rules join the fixed rules.
   */
  static readonly layer = (
    agentKey: string,
    source?: ComposedMessagePolicy | LoadedMessagePolicy,
    contracts: readonly CompiledContract[] = [],
  ) =>
    Layer.effect(
      Firewall,
      Effect.gen(function* () {
        const identity = yield* Identity;
        const store = yield* makeFirewallStore(agentKey);
        const policy =
          source === undefined
            ? undefined
            : yield* initialPolicy(store, source, contracts);
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
              const item: PermittedItem = { kind: "message", message };
              const decision = yield* decide(
                installed,
                { message, evidence },
                "incoming",
              );
              const save = saveRecords(store, decision);
              const visibility = [
                {
                  message: {
                    authorAgentId: message.author.agentId,
                    msgId: message.msgId,
                  },
                  visibleToAgent: decision.kind === "allowed",
                },
              ];
              if (decision.kind === "allowed") {
                return {
                  kind: "allowed",
                  item,
                  save,
                  visibility,
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
                visibility,
                ...(reply === undefined ? {} : { reply }),
              } satisfies IncomingDecision;
            }),
        });
      }),
    );
}

/**
 * One candidate the text rules are to judge: the rules that apply to it, and
 * for an act a contract governs, what the provider reads of its protocol.
 */
interface TextCheckPlan {
  readonly candidate: Candidate;
  readonly active: ActivePolicy;
  readonly protocol?: CheckEvidence["protocol"];
}

/** One candidate prepared for a check of the text rules. */
interface PreparedTextCheck {
  readonly active: ActivePolicy;
  readonly direction: Direction;
  readonly messageKey: string;
  readonly messageJson: string;
  readonly message: AssessmentText;
  readonly evidence: CheckEvidence;
  readonly agents: readonly AgentCard[];
}

/** The message policy one firewall applies. */
interface Installation {
  readonly store: FirewallStore;
  readonly policy?: InstalledPolicy;
  readonly localAgent: AgentCard;
}

const storedPolicyFormat = "org.social-harness/editable-policy/v1";

const storedPolicySchema = exactStruct({
  format: Schema.Literal(storedPolicyFormat),
  rules: editableRulesSchema,
  contracts: Schema.Array(compiledContractSchema),
  contractsUsed: Schema.Boolean,
});

type CapturedMessagePolicy = Effect.Effect.Success<
  ReturnType<typeof capturePolicy>
>;

type ActivePolicy = Effect.Effect.Success<ReturnType<typeof buildPolicy>>;

type CapturedTextCheck = Effect.Effect.Success<
  ReturnType<typeof capturePrepared>
>;

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
    const decided: Decision = yield* checkTextRules(
      policy,
      installed.localAgent,
      textCheckPlan(active, candidate, contract),
      direction,
    );
    return contract?.kind === "ready"
      ? yield* withContract(decided, contract, direction)
      : decided;
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

/** Checks the planned candidate against its text rules, with the history before it. */
function checkTextRules(
  policy: InstalledPolicy,
  self: AgentCard,
  { candidate, active, protocol }: TextCheckPlan,
  direction: Direction,
) {
  return Effect.gen(function* () {
    const now = yield* Clock.currentTimeMillis;
    const cards = new Map<string, AgentCard>();
    for (const card of [
      self,
      candidate.message.author,
      ...candidate.message.recipients,
      ...candidate.evidence.history.flatMap(({ message }) => [
        message.author,
        ...message.recipients,
      ]),
    ]) {
      cards.set(card.agentId, card);
    }
    const history: HistoryEntry[] = candidate.evidence.history.map((entry) => ({
      // eslint-disable-next-line @typescript-eslint/no-misused-spread -- assessmentText returns plain fields with no prototype methods.
      ...assessmentText(entry.message),
      visibleToAgent:
        entry.visibleToAgent || entry.message.author.agentId === self.agentId,
    }));
    const evidence: CheckEvidence = {
      agentId: self.agentId,
      principal: self.principalId,
      participants: [...cards.values()].map((card) => ({
        agentId: card.agentId,
        principal: card.principalId,
      })),
      history,
      earlierHistoryOmitted: candidate.evidence.earlierHistoryOmitted,
      now,
      facts: policy.captured.configuration.facts,
      gaps: candidate.evidence.gaps,
      ...(protocol === undefined ? {} : { protocol }),
    };
    return yield* policy.permits.withPermits(1)(
      checkMessageText(policy.captured, active, {
        active,
        direction,
        messageKey: messageKey(candidate.message),
        messageJson: JSON.stringify(candidate.message),
        message: assessmentText(candidate.message),
        evidence,
        agents: [...cards.values()],
      }),
    );
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

function refusalReason(reason: FirewallCheck["reason"]): FirewallRefusalReason {
  return reason === "eligible" ? "unresolved-evidence" : reason;
}

function referenceId(reference: string): string {
  return `msg_${createHash("sha256").update(reference).digest("hex")}`;
}

/**
 * Installs the message policy `source` gives. Its revision covers the held
 * contracts, so a decision names the rules that applied.
 */
function initialPolicy(
  store: FirewallStore,
  source: ComposedMessagePolicy | LoadedMessagePolicy,
  initialContracts: readonly CompiledContract[],
) {
  return Effect.gen(function* () {
    const composed =
      "policy" in source ? yield* composedPolicy(source) : source;
    const captured = yield* capturePolicy(composed).pipe(
      Effect.catchTag("ParseError", () =>
        Effect.fail(new FirewallPolicyError({ reason: "build-failed" })),
      ),
      Effect.catchAllDefect(() =>
        Effect.fail(new FirewallPolicyError({ reason: "build-failed" })),
      ),
    );
    const { saved, editableRules, contracts, contractsUsed } =
      yield* loadEditablePolicy(store, captured, initialContracts);
    const active = yield* buildPolicy(
      captured,
      editableRules,
      contracts,
      contractsUsed,
    );
    if (saved === undefined) {
      yield* saveEditablePolicy(store, active);
    }
    return {
      store,
      captured,
      active,
      edits: yield* Effect.makeSemaphore(1),
      permits: yield* Effect.makeSemaphore(
        captured.configuration.maximumConcurrentChecks,
      ),
    };
  });
}

/** The saved editable rules and contracts, or the initial ones for an agent with none saved. */
function loadEditablePolicy(
  store: FirewallStore,
  capturedPolicy: CapturedMessagePolicy,
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
      saved?.rules ?? capturedPolicy.text?.initialRules ?? [];
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

/** Text rules without a decision provider fail to install instead of passing unchecked. */
function composedPolicy(loaded: LoadedMessagePolicy) {
  return Effect.gen(function* () {
    const { policy: messagePolicy, provider } = loaded;
    if (messagePolicy.text !== undefined && provider === undefined) {
      return yield* new FirewallPolicyError({ reason: "build-failed" });
    }
    return {
      limits: messagePolicy.settings,
      facts: messagePolicy.facts,
      maximumHistoryRecords: messagePolicy.maximumHistoryRecords,
      maximumConcurrentChecks: messagePolicy.maximumConcurrentChecks,
      text:
        provider === undefined
          ? undefined
          : {
              fixedRules: messagePolicy.text?.fixedRules ?? [],
              initialRules: messagePolicy.text?.initialRules ?? [],
              providerSettings: messagePolicy.textSettings,
              provider: retryingTransientFailures(
                makeJevDecisionProvider(provider),
              ),
            },
    } satisfies ComposedMessagePolicy;
  });
}

function capturePolicy(composed: ComposedMessagePolicy) {
  return Effect.gen(function* () {
    const configuration = yield* decodeFrozen(firewallConfigurationSchema, {
      facts: composed.facts,
      maximumHistoryRecords: composed.maximumHistoryRecords ?? 256,
      maximumConcurrentChecks: composed.maximumConcurrentChecks ?? 4,
    });
    const limits = yield* decodeFrozen(CheckLimits, composed.limits);
    const text =
      composed.text === undefined
        ? undefined
        : yield* captureText(composed.text);
    return {
      configuration,
      limits,
      text,
    };
  });
}

function captureText(selected: NonNullable<ComposedMessagePolicy["text"]>) {
  return Effect.gen(function* () {
    return {
      fixedRules: yield* decodeFrozen(
        Schema.Array(TextRule),
        selected.fixedRules ?? [],
      ),
      initialRules: yield* decodeFrozen(
        editableRulesSchema,
        selected.initialRules ?? [],
      ),
      providerSettings: yield* decodeFrozen(
        ProviderRequestSettings,
        selected.providerSettings,
      ),
      provider: {
        decide: selected.provider.decide.bind(selected.provider),
        ...(yield* decodeFrozen(
          Schema.Struct({
            provider: Schema.optional(Schema.NonEmptyTrimmedString),
            model: Schema.optional(Schema.NonEmptyTrimmedString),
          }),
          {
            provider: selected.provider.provider,
            model: selected.provider.model,
          },
        )),
      },
    };
  });
}

function validateIds(
  editableRules: readonly TextRule[],
  fixedRuleIds: readonly string[],
) {
  const collision = editableRules.find((rule) =>
    fixedRuleIds.includes(rule.id),
  );
  if (collision !== undefined) {
    return Effect.fail(
      new FirewallPolicyError({ reason: "fixed-rule", ruleId: collision.id }),
    );
  }
  const repeated = editableRules.find(
    (rule, index) =>
      editableRules.findIndex(({ id }) => id === rule.id) !== index,
  );
  return repeated === undefined
    ? Effect.void
    : Effect.fail(invalidRule("duplicate-id", { ruleId: repeated.id }));
}

/** Only transient transport, rate-limit and server failures share the stage's bounded retry window. */
function retryingTransientFailures(
  provider: DecisionProvider,
): DecisionProvider {
  return {
    ...(provider.provider === undefined ? {} : { provider: provider.provider }),
    ...(provider.model === undefined ? {} : { model: provider.model }),
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
 * One check of one message against the text rules. Its record carries the
 * exact revision and evidence that delivery commits.
 */
function checkMessageText(
  capturedPolicy: CapturedMessagePolicy,
  active: ActivePolicy,
  prepared: PreparedTextCheck,
) {
  return Effect.gen(function* () {
    const captured = yield* capturePrepared(
      capturedPolicy.configuration,
      prepared,
    );
    const checkId = mintCheckId();
    const concluded = yield* runTextCheck(
      capturedPolicy,
      active,
      captured,
    ).pipe(markCheckActivity(captured.direction));
    const projection = guidanceProjection(concluded, checkId);
    const { check } = projection;
    const allowed = check.judgment === "allow";
    const record: CheckRecord = {
      checkId,
      messageKey: captured.messageKey,
      direction: captured.direction,
      messageJson: captured.messageJson,
      inputJson: recordInputJson(active, captured),
      allowed,
      resultJson: JSON.stringify({
        check: Struct.omit(check, "trace"),
        summaries: yield* decisionSummaries(active, captured, check),
      }),
    };
    if (allowed) {
      return {
        kind: "allowed",
        record,
        ...(projection.guidance === undefined
          ? {}
          : { guidance: projection.guidance }),
      } satisfies Decision;
    }
    const reply =
      captured.direction === "incoming" ? textReply(active, check) : "";
    return {
      kind: "refused",
      record,
      refusal: {
        reason: refusalReason(check.reason),
        feedback: check.feedback,
        ...(reply === "" ? {} : { reply }),
      },
    } satisfies Decision;
  }).pipe(Effect.withSpan("assessMessage"));
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
  const violated = new Set(
    decidingRules(check.reason, active.textRules, check).map(
      (rule) => rule.ruleId,
    ),
  );
  return active.textRules
    .filter((rule) => violated.has(rule.id) && rule.action === "reply")
    .flatMap((rule) =>
      rule.feedback === undefined ? [] : [rule.feedback.text],
    )
    .join("\n");
}

/** The check and its saved record read the same detached message and evidence. */
function capturePrepared(
  configuration: FirewallConfiguration,
  prepared: PreparedTextCheck,
) {
  return Effect.gen(function* () {
    const captured = yield* Effect.try({
      try: () =>
        structuredClone({
          direction: prepared.direction,
          messageKey: prepared.messageKey,
          messageJson: prepared.messageJson,
          event: { direction: prepared.direction, message: prepared.message },
          evidence: Object.assign({}, prepared.evidence, {
            facts: configuration.facts,
          }),
          agents: prepared.agents,
        }),
      catch: () => new InvalidCheckError({ reason: "evidence" }),
    });
    deepFreeze(captured.event);
    deepFreeze(captured.evidence);
    deepFreeze(captured.agents);
    return captured;
  });
}

function runTextCheck(
  capturedPolicy: CapturedMessagePolicy,
  active: ActivePolicy,
  captured: CapturedTextCheck,
) {
  return checkCapturedMessage(
    {
      textRules: active.textRules,
      limits: capturedPolicy.limits,
      revision: active.revision,
      ...(capturedPolicy.text === undefined
        ? {}
        : {
            providerSettings: capturedPolicy.text.providerSettings,
            provider: capturedPolicy.text.provider,
          }),
    },
    { event: captured.event, evidence: captured.evidence },
  ).pipe(
    Effect.catchTag("InvalidCheckError", () => Effect.succeed(unavailable())),
  );
}

/** The revision, its contents and the evidence a check applied, as its record saves them. */
function recordInputJson(active: ActivePolicy, captured: CapturedTextCheck) {
  return JSON.stringify({
    revision: active.revision,
    revisionContents: active.revisionContents,
    evidence: captured.evidence,
    agents: captured.agents,
    message: captured.event.message,
  });
}

/** The summaries a host reads name the same captured rules and check as the decision. */
function decisionSummaries(
  active: ActivePolicy,
  captured: CapturedTextCheck,
  check: FirewallCheck,
): Effect.Effect<readonly DecisionSummary[], InvalidCheckError> {
  const deciding = decidingRules(check.reason, active.textRules, check);
  const decidedBy = deciding.map(
    (rule): DecisionSummary["decidedBy"][number] => ({
      ruleId: rule.ruleId,
      from: active.fixedRuleIds.includes(rule.ruleId) ? "fixed" : "editable",
      ...(rule.advice === undefined ? {} : { advice: rule.advice }),
    }),
  );
  const checkedRuleIds = [
    ...new Set(
      check.findings.flatMap((finding) =>
        finding.kind === "not-applicable" ? [] : [finding.ruleId],
      ),
    ),
  ];
  const message = captured.event.message;
  const peers =
    captured.direction === "outgoing" ? message.recipients : [message.sender];
  return Effect.forEach(
    peers,
    (identity) => {
      const peer = captured.agents.find((agent) => agent.agentId === identity);
      if (peer === undefined) {
        return Effect.fail(new InvalidCheckError({ reason: "evidence" }));
      }
      return Effect.succeed({
        direction: captured.direction,
        peer,
        judgment: check.judgment,
        reason: check.reason,
        decidedBy,
        checkedRuleIds,
        activeRevision: active.revision,
      });
    },
    { concurrency: 1 },
  );
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
        policy.captured,
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
  capturedPolicy: CapturedMessagePolicy,
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
    const fixed = [
      ...(capturedPolicy.text?.fixedRules ?? []),
      ...contractRules,
    ];
    yield* validateIds(
      editableRules,
      fixed.map((rule) => rule.id),
    );
    const textRules = [...fixed, ...editableRules];
    if (textRules.length > 0 && capturedPolicy.text === undefined) {
      return yield* new FirewallPolicyError({ reason: "not-configured" });
    }
    const revisionContents = {
      configuration: capturedPolicy.configuration,
      limits: capturedPolicy.limits,
      questionEdition: textQuestionEdition,
      textRules,
      fixedRuleIds: fixed.map((rule) => rule.id),
      contracts: contracts.map((contract) => contract.digest),
      ...(capturedPolicy.text === undefined
        ? {}
        : {
            providerSettings: capturedPolicy.text.providerSettings,
            provider: capturedPolicy.text.provider.provider ?? null,
            model: capturedPolicy.text.provider.model ?? null,
          }),
    };
    const revision = digest(revisionContents);
    yield* validateChecks({
      textRules,
      limits: capturedPolicy.limits,
      revision,
      providerSettings: capturedPolicy.text?.providerSettings,
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
      fixedRuleIds: revisionContents.fixedRuleIds,
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
