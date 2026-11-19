/** @file Owns one agent's message policy: its checks, the content they permit and its saved decisions. */

import { Clock, Context, Effect, Exit, Layer, Schedule, Struct } from "effect";
import { createHash } from "node:crypto";
import type { Message } from "../communication/index.js";
import type { CompiledContract } from "./contract.js";
import { type AgentCard, Identity } from "../identity.js";
import {
  type AssessmentText,
  checkCapturedMessage,
  type CheckEvidence,
  type DecisionProvider,
  type FirewallCheck,
  type FirewallEvidence,
  FirewallRefusal,
  type HistoryEntry,
  type LocalGuidance,
  textQuestionEdition,
} from "./check.js";
import {
  type ContractCheck,
  contractReply,
  messageKey,
  prepareContract,
} from "./contract-check.js";
import {
  type Candidate,
  contractRefusal,
  type Decision,
  mintCheckId,
} from "./decision.js";
import { makeJevDecisionProvider } from "./jev.js";
import {
  type Direction,
  type firewallDecisionsSchema,
  type firewallDecisionSummarySchema,
  FirewallPolicyError,
  type FirewallRefusalReason,
  type LoadedMessagePolicy,
  TextRule,
} from "./rules.js";
import {
  type CheckRecord,
  type FirewallStore,
  makeFirewallStore,
} from "./store.js";

/**
 * The message policy one firewall applies for its whole scope. Every reason
 * but `eligible`, including a provider failure, refuses a send and withholds
 * an incoming message.
 */
type InstalledPolicy = Effect.Effect.Success<ReturnType<typeof buildPolicy>>;

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
/**
 * Reads the evidence of one check. Only a check under a message policy runs
 * it. `protocolHistory` asks for every message before the one under check as
 * well, which the check of an agent that holds a contract needs.
 */
type ReadEvidence<E, R> = (
  protocolHistory: boolean,
) => Effect.Effect<FirewallEvidence, E, R>;

/** Message checks and saved decision summaries. */
interface FirewallService {
  /** Checks without saving. Refusals expose feedback. */
  readonly checkOutgoing: <E, R>(
    message: Message,
    evidence: ReadEvidence<E, R>,
  ) => Effect.Effect<OutgoingDecision, E, R>;
  /**
   * Checks a message a peer wrote with its preceding evidence. An allowed
   * message the provider was unsure about carries guidance; a withheld one
   * exposes none.
   */
  readonly checkIncoming: <E, R>(
    message: Message,
    evidence: ReadEvidence<E, R>,
  ) => Effect.Effect<IncomingDecision, E, R>;
  /** The contracts the agent holds. */
  readonly readContracts: () => Effect.Effect<readonly CompiledContract[]>;
  readonly readDecisions: (request: {
    readonly since?: number;
  }) => Effect.Effect<typeof firewallDecisionsSchema.Type, FirewallPolicyError>;
}

/** One agent's firewall: it checks messages against the message policy and reads its saved decisions. */
export class Firewall extends Context.Tag("SocialHarness/Firewall")<
  Firewall,
  FirewallService
>() {
  /**
   * Policy resources are fixed for this scope. Identity supplies this agent's
   * card. The agent holds `contracts`; structure and protocol checks of a
   * message's act apply before the text rules of `source`, and a
   * contract's text rules join them. `provider` judges the text rules in
   * place of the provider `source` names.
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
            : yield* buildPolicy(source, contracts, provider);
        const installed: Installation = {
          store,
          ...(policy === undefined ? {} : { policy }),
          localAgent: identity.me,
        };
        return Firewall.of({
          readContracts: () => Effect.sync(() => policy?.contracts ?? []),
          readDecisions: (request) => store.readDecisions(request),
          checkOutgoing: (message, evidence) =>
            Effect.gen(function* () {
              const decision = yield* decide(
                installed,
                message,
                evidence,
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
                message,
                evidence,
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
  readonly textRules: readonly TextRule[];
  readonly contract?: Extract<ContractCheck, { readonly kind: "ready" }>;
  readonly protocol?: CheckEvidence["protocol"];
}

/** The message policy one firewall applies. */
interface Installation {
  readonly store: FirewallStore;
  readonly policy?: InstalledPolicy;
  readonly localAgent: AgentCard;
}

/** How many checks run at once when no policy sets `maximumConcurrentChecks`. */
const defaultConcurrentChecks = 4;

type DecisionSummary = typeof firewallDecisionSummarySchema.Type;

/**
 * Decides a message. A message of an agent that holds a contract
 * is first read as an act and checked for structure and protocol; a failure
 * refuses it. The text rules then judge the candidate: the owner's rules and
 * the rules its contract gives its act. The firewall's own reply to a
 * withheld message, which is checked with that message as its reference, is
 * not an act of a norm unless it reads as one: a reply that fails structure
 * is judged by the text rules alone. An agent without a message policy allows
 * every message without reading its evidence.
 */
function decide<E, R>(
  installed: Installation,
  message: Message,
  readEvidence: ReadEvidence<E, R>,
  direction: Direction,
) {
  return Effect.gen(function* () {
    const policy = installed.policy;
    if (policy === undefined) {
      return { kind: "allowed" } satisfies Decision;
    }
    const contractsUsed = policy.contracts.length !== 0;
    const candidate: Candidate = {
      message,
      evidence: yield* readEvidence(contractsUsed),
    };
    const contract = contractsUsed
      ? yield* prepareContract(
          policy.contracts,
          installed.store,
          candidate.message,
          candidate.evidence,
        )
      : undefined;
    if (
      contract?.kind === "refused" &&
      !(
        candidate.evidence.references !== undefined &&
        contract.problem.stage === "structural"
      )
    ) {
      return contractRefusal(policy.revision, candidate, direction, contract);
    }
    return yield* checkMessageText(
      policy,
      installed.localAgent,
      textCheckPlan(policy, candidate, contract),
      direction,
    );
  });
}

/**
 * The text rules that judge a candidate: the owner's and those its contract
 * gives its act. The contract's come from the contract its instance began
 * under.
 */
function textCheckPlan(
  policy: InstalledPolicy,
  candidate: Candidate,
  contract?: ContractCheck,
): TextCheckPlan {
  const ready = contract?.kind === "ready" ? contract : undefined;
  const ownerRules = policy.textRules.filter(
    (rule) => !policy.contractRuleIds.has(rule.id),
  );
  const act = ready?.record.act.name;
  const contractRules = (ready?.contract.textRules ?? [])
    .filter((rule) => rule.act === undefined || rule.act === act)
    .map((rule) => new TextRule(Struct.omit(rule, "act")));
  const form = ready?.contract.forms.find((declared) => declared.act === act);
  return {
    candidate,
    textRules: [...ownerRules, ...contractRules],
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
  { candidate, textRules, contract, protocol }: TextCheckPlan,
  direction: Direction,
) {
  return Effect.gen(function* () {
    const { message } = candidate;
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
      gaps: candidate.evidence.gaps,
      ...(protocol === undefined ? {} : { protocol }),
    };
    const text = assessmentText(message);
    return yield* Effect.gen(function* () {
      const checkId = mintCheckId();
      const check = yield* checkCapturedMessage(direction, text, evidence, {
        textRules,
        revision: policy.revision,
        settings: policy.configured.settings,
        ...(policy.provider === undefined ? {} : { provider: policy.provider }),
      }).pipe(markCheckActivity(direction));
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
          revision: policy.revision,
          revisionContents: policy.revisionContents,
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
              policy.revision,
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
          ...(check.instructions.length === 0
            ? {}
            : { guidance: { checkId, instructions: check.instructions } }),
        };
        return allowed;
      }
      const reply =
        direction === "incoming"
          ? (contractRefusalReply(contract, check) ??
            textReply(textRules, check))
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
  return {
    id: referenceId(`${message.author.agentId}:${message.msgId}`),
    sender: message.author.agentId,
    recipients: [message.recipients[0].agentId],
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
          while: (error) => error.transient,
        }),
      ),
  };
}

/**
 * What the sender of a withheld message is told: each violated `reply` rule's
 * registered feedback, one per line. Holds reply nothing: the sender broke no
 * settled rule.
 */
function textReply(
  textRules: readonly TextRule[],
  check: FirewallCheck,
): string {
  if (check.reason !== "rule-violation") {
    return "";
  }
  const violated = new Set(check.decidedBy.map((rule) => rule.ruleId));
  return textRules
    .filter((rule) => violated.has(rule.id) && rule.action === "reply")
    .flatMap((rule) => (rule.feedback === undefined ? [] : [rule.feedback]))
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
        check.feedback.join(" ") ||
          `The semantic policy check returned ${check.reason}.`,
      );
}

/** The summary a host reads of one check for one peer, naming the same captured rules as the decision. */
function decisionSummary(
  activeRevision: string,
  check: FirewallCheck,
  direction: Direction,
  peer: AgentCard,
): DecisionSummary {
  return {
    direction,
    peer,
    reason: check.reason,
    decidedBy: check.decidedBy,
    checkedRuleIds: check.checkedRuleIds,
    activeRevision,
  };
}

/**
 * Installs the message policy `source` gives with the held `contracts`: the
 * configured rules and the contracts' text rules. Its revision covers the
 * contracts, so a decision names the rules that applied. Without `provider`,
 * Jev judges the text rules of a policy that names it.
 */
function buildPolicy(
  source: LoadedMessagePolicy,
  contracts: readonly CompiledContract[],
  injected?: DecisionProvider,
) {
  return Effect.gen(function* () {
    const configured = source.policy;
    const provider =
      injected ??
      (source.provider === undefined
        ? undefined
        : retryingTransientFailures(makeJevDecisionProvider(source.provider)));
    if (
      new Set(contracts.map((contract) => contract.name)).size !==
      contracts.length
    ) {
      return yield* new FirewallPolicyError({ reason: "build-failed" });
    }
    const contractRules = contracts.flatMap((contract) =>
      contract.textRules.map((rule) => new TextRule(Struct.omit(rule, "act"))),
    );
    const textRules = [...configured.rules, ...contractRules];
    if (textRules.length > 0 && provider === undefined) {
      return yield* new FirewallPolicyError({ reason: "not-configured" });
    }
    const revisionContents = {
      configuration: {
        maximumConcurrentChecks:
          configured.maximumConcurrentChecks ?? defaultConcurrentChecks,
      },
      questionEdition: textQuestionEdition,
      textRules,
      contracts: contracts.map((contract) => contract.digest),
      ...(provider === undefined
        ? {}
        : {
            providerSettings: configured.settings,
            provider: provider.provider ?? null,
            model: provider.model ?? null,
          }),
    };
    const revision = digest(revisionContents);
    return {
      configured,
      provider,
      revisionContents,
      contracts,
      contractRuleIds: new Set(contractRules.map((rule) => rule.id)),
      revision,
      textRules,
      permits: yield* Effect.makeSemaphore(
        configured.maximumConcurrentChecks ?? defaultConcurrentChecks,
      ),
    };
  });
}

/**
 * Hashes the JSON text of what it is given. Every part of that is decoded
 * through a struct schema, which fixes the order of its members.
 */
function digest(hashed: unknown) {
  return createHash("sha256").update(JSON.stringify(hashed)).digest("hex");
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
