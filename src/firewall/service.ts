/** @file Owns one agent's message policy: its checks, the content they permit and its saved decisions. */

import { Clock, Context, Effect, Exit, Layer, Schedule, Struct } from "effect";
import { createHash, randomBytes } from "node:crypto";
import type { Message } from "../communication/index.js";
import type {
  Direction,
  firewallDecisionsSchema,
  firewallDecisionSummarySchema,
  FirewallPolicyError,
  FirewallRefusalReason,
  LoadedMessagePolicy,
  TextRule,
} from "./rules.js";
import { type AgentCard, Identity } from "../identity.js";
import {
  type AssessmentText,
  checkCapturedMessage,
  type CheckEvidence,
  type DecisionProvider,
  type EarlierMessage,
  type FirewallCheck,
  type FirewallEvidence,
  FirewallRefusal,
  type HistoryEntry,
  type LocalGuidance,
  type StructureCheck,
  textQuestionEdition,
} from "./check.js";
import { makeJevDecisionProvider } from "./jev.js";
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

/** A refused or withheld message's reason, the rule that decided, and what its sender is told. */
interface Refusal {
  readonly reason: FirewallRefusalReason;
  readonly ruleId?: string;
  readonly feedback: readonly string[];
  readonly detail?: unknown;
  readonly reply?: string;
}

/**
 * One message's decision and the check record that saves it. `refused`
 * refuses a message the agent sends and withholds one it receives. Only a
 * message of an agent without a message policy has no record.
 */
type Decision =
  | {
      readonly kind: "allowed";
      readonly record?: CheckRecord;
      readonly guidance?: LocalGuidance;
    }
  | {
      readonly kind: "refused";
      readonly record: CheckRecord;
      readonly refusal: Refusal;
    };

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
 * well, which a firewall with a structure check needs.
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
   * card. `structureCheck` judges each message before the text rules of
   * `source`, and the text rules it gives a message join them. `provider`
   * judges the text rules in place of the provider `source` names.
   */
  static readonly layer = (
    agentKey: string,
    source?: LoadedMessagePolicy,
    structureCheck?: StructureCheck,
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
            : yield* buildPolicy(source, provider);
        const installed: Installation = {
          store,
          ...(policy === undefined ? {} : { policy }),
          ...(structureCheck === undefined ? {} : { structureCheck }),
          localAgent: identity.me,
        };
        return Firewall.of({
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

/** What a structure check gives of a message it passes. */
type Passed = Extract<
  Effect.Effect.Success<ReturnType<StructureCheck>>,
  { readonly kind: "passed" }
>;

/** The message policy one firewall applies. */
interface Installation {
  readonly store: FirewallStore;
  readonly policy?: InstalledPolicy;
  readonly structureCheck?: StructureCheck;
  readonly localAgent: AgentCard;
}

/** How many checks run at once when no policy sets `maximumConcurrentChecks`. */
const defaultConcurrentChecks = 4;

type DecisionSummary = typeof firewallDecisionSummarySchema.Type;

/**
 * Decides a message. The structure check judges it first, with the earlier
 * messages and this firewall's checks of them; its refusal refuses the
 * message, and a check that cannot read those checks is unavailable. The text
 * rules then judge the message: the owner's rules and those the structure
 * check gives it. The firewall's own reply to a withheld message, which is
 * checked with that message as its reference, is judged by the owner's text
 * rules alone. An agent without a message policy allows every message without
 * reading its evidence.
 */
function decide<E, R>(
  installed: Installation,
  message: Message,
  readEvidence: ReadEvidence<E, R>,
  direction: Direction,
) {
  return Effect.gen(function* () {
    const { policy, structureCheck } = installed;
    if (policy === undefined) {
      return { kind: "allowed" } satisfies Decision;
    }
    const evidence = yield* readEvidence(structureCheck !== undefined);
    const structure =
      structureCheck === undefined || evidence.references !== undefined
        ? undefined
        : yield* earlierMessages(installed.store, evidence).pipe(
            Effect.flatMap((earlier) => structureCheck(message, earlier)),
            Effect.map((result) =>
              result.kind === "passed"
                ? result
                : { ...result, feedback: [result.feedback] },
            ),
            Effect.catchTag("FirewallPolicyError", () =>
              Effect.succeed({
                kind: "refused" as const,
                reason: "check-unavailable" as const,
                feedback: [],
              }),
            ),
          );
    if (structure?.kind === "refused") {
      return refusal(
        policy.revision,
        message,
        direction,
        Struct.omit(structure, "kind"),
      );
    }
    return yield* checkMessageText(
      policy,
      installed.localAgent,
      { message, evidence },
      direction,
      structure,
    );
  });
}

/**
 * Each message before the one under check with this firewall's saved check of
 * it. A check that allowed it wins over one that did not, since a draft the
 * firewall refused can share its id with the message later sent.
 */
function earlierMessages(store: FirewallStore, evidence: FirewallEvidence) {
  const messages =
    evidence.protocolHistory?.messages ??
    evidence.history.map((entry) => entry.message);
  return Effect.map(
    store.readCheckRecords(messages.map((message) => messageKey(message))),
    (rows) =>
      messages.map((message): EarlierMessage => {
        const checks = rows.filter(
          (row) => row.messageKey === messageKey(message),
        );
        const check =
          checks.find((row) => row.decision === "allowed") ?? checks[0];
        return {
          message,
          allowed: check?.decision === "allowed",
          ...(check === undefined ? {} : { checkedAt: check.at }),
        };
      }),
  );
}

/**
 * The decision that refuses a message without a text check. A withheld
 * message is answered with the refusal's reply when it has one.
 */
function refusal(
  revision: string,
  message: Message,
  direction: Direction,
  { reply, ...refused }: Refusal,
): Decision {
  const ruleIds = refused.ruleId === undefined ? [] : [refused.ruleId];
  return {
    kind: "refused",
    record: {
      checkId: mintCheckId(),
      messageKey: messageKey(message),
      direction,
      messageJson: JSON.stringify(message),
      inputJson: JSON.stringify({ revision }),
      resultJson: JSON.stringify({
        ...(refused.detail === undefined ? {} : { detail: refused.detail }),
        summaries: (direction === "outgoing"
          ? message.recipients
          : [message.author]
        ).map(
          (peer): DecisionSummary => ({
            direction,
            peer,
            reason: refused.reason,
            decidedBy: ruleIds.map((ruleId) => ({ ruleId })),
            checkedRuleIds: ruleIds,
            activeRevision: revision,
          }),
        ),
      }),
      allowed: false,
    },
    refusal: {
      ...refused,
      ...(reply === undefined || direction === "outgoing" ? {} : { reply }),
    },
  };
}

/** Saves the decision's check record when it has one. */
function saveRecords(store: FirewallStore, decision: Decision) {
  return decision.record === undefined
    ? Effect.void
    : store.saveCheckRecord(decision.record);
}

/**
 * One check of a message against its text rules, with the history before it.
 * Its record carries the exact revision and evidence that delivery commits.
 * A message the structure check passed is also judged by the text rules that
 * check gives it, and the provider reads its protocol context. A refusal of
 * such a message carries the structure check's detail, and a withheld one is
 * answered as that check phrases the reply.
 */
function checkMessageText(
  policy: InstalledPolicy,
  self: AgentCard,
  candidate: {
    readonly message: Message;
    readonly evidence: FirewallEvidence;
  },
  direction: Direction,
  structure?: Passed,
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
      ...(structure?.protocol === undefined
        ? {}
        : { protocol: structure.protocol }),
    };
    const text = assessmentText(message);
    const textRules = [...policy.textRules, ...(structure?.textRules ?? [])];
    const detail = structure?.detail;
    return yield* Effect.gen(function* () {
      const checkId = mintCheckId();
      const check = yield* checkCapturedMessage(direction, text, evidence, {
        textRules,
        revision: policy.revision,
        settings: policy.configured.settings,
        ...(policy.provider === undefined ? {} : { provider: policy.provider }),
      }).pipe(markCheckActivity(direction));
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
          ? (structure?.replyTo?.(
              check.feedback.join(" ") ||
                `The semantic policy check returned ${check.reason}.`,
            ) ?? textReply(textRules, check))
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

/** The key a check record is saved under for its message: the author's agent id and the message id. */
function messageKey(message: Message): string {
  return `${message.author.agentId}/${message.msgId}`;
}

/** A check id correlates a check without granting lookup authority. */
function mintCheckId() {
  return `chk_${randomBytes(32).toString("hex")}`;
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
 * Installs the message policy `source` gives. Its revision covers the
 * configured rules, so a decision names the rules that applied. Without
 * `provider`, Jev judges the text rules of a policy that names it.
 */
function buildPolicy(source: LoadedMessagePolicy, injected?: DecisionProvider) {
  return Effect.gen(function* () {
    const configured = source.policy;
    const provider =
      injected ??
      (source.provider === undefined
        ? undefined
        : retryingTransientFailures(makeJevDecisionProvider(source.provider)));
    const textRules = configured.rules;
    const revisionContents = {
      configuration: {
        maximumConcurrentChecks:
          configured.maximumConcurrentChecks ?? defaultConcurrentChecks,
      },
      questionEdition: textQuestionEdition,
      textRules,
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
