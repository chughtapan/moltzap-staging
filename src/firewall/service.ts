/** @file One agent's message checks: its structure check, its text rules, and what each check concludes and gives to be saved. */

import { Clock, Effect, Exit, Schedule } from "effect";
import { createHash, randomBytes } from "node:crypto";
import type { Message } from "../communication/index.js";
import type { AgentCard } from "../identity.js";
import type {
  CheckRecord,
  Direction,
  FirewallRefusalReason,
  LoadedMessagePolicy,
  TextRule,
} from "./rules.js";
import { MAXIMUM_RUNTIME_ITEM_BYTES } from "../encoding.js";
import {
  type AssessmentText,
  checkCapturedMessage,
  type CheckEvidence,
  type DecisionProvider,
  type EarlierMessage,
  type FirewallCheck,
  type LocalGuidance,
  type StructureCheck,
  textQuestionEdition,
} from "./check.js";
import { makeJevDecisionProvider } from "./jev.js";

/**
 * The message policy one firewall applies for its whole scope. Every reason
 * but `eligible`, including a provider failure, refuses a message.
 */
type InstalledPolicy = ReturnType<typeof buildPolicy>;

/**
 * What a check concludes. `record` is what the caller saves of the decision;
 * a message of an agent without a message policy and a check that could not
 * run have none. A refused incoming message is withheld from the agent, and
 * `reply` is the text its sender is to receive as an ordinary message from
 * this agent, with the record of that reply as a message this firewall
 * allowed.
 */
export type CheckResult =
  | {
      readonly kind: "allowed";
      readonly guidance?: LocalGuidance;
      readonly record?: CheckRecord;
    }
  | {
      readonly kind: "refused";
      readonly checkId: string;
      readonly reason: FirewallRefusalReason;
      readonly rule?: string;
      readonly feedback: readonly string[];
      readonly detail?: unknown;
      readonly reply?: { readonly text: string; readonly record: CheckRecord };
      readonly record?: CheckRecord;
    };

/** One agent's message checks. `earlier` is the conversation's earlier messages in order. */
export interface Firewall {
  /** Checks a message its agent sends. A refusal exposes feedback and never a reply. */
  readonly checkOutgoing: (
    message: Message,
    earlier: readonly EarlierMessage[],
  ) => Effect.Effect<CheckResult>;
  /**
   * Checks a message a peer wrote. An allowed message the provider was unsure
   * about carries guidance.
   */
  readonly checkIncoming: (
    message: Message,
    earlier: readonly EarlierMessage[],
  ) => Effect.Effect<CheckResult>;
}

/** What a firewall is made with, besides its message policy. */
interface Installation {
  /** The card of the agent whose messages the firewall checks. */
  readonly me: AgentCard;
  /** The most earlier messages the provider reads. */
  readonly historyLimit: number;
  /**
   * What the sender of a message withheld for breaking a rule is told when no
   * rule and no structure check answers it; a structure check's explanation
   * follows it. Without it such a sender is told nothing.
   */
  readonly notice?: string;
  /** Judges each message before the text rules, and gives the text rules that join them. */
  readonly structureCheck?: StructureCheck;
  /** Judges the text rules in place of the provider the policy names. */
  readonly provider?: DecisionProvider;
}

/**
 * The firewall that applies the message policy `source` for its whole
 * lifetime. Without a policy it allows every message.
 */
export function makeFirewall(
  source: LoadedMessagePolicy | undefined,
  installation: Installation,
): Firewall {
  const installed = {
    ...installation,
    ...(source === undefined
      ? {}
      : { policy: buildPolicy(source, installation.provider) }),
  };
  return {
    checkOutgoing: (message, earlier) =>
      decide(installed, message, earlier, "outgoing"),
    checkIncoming: (message, earlier) =>
      decide(installed, message, earlier, "incoming"),
  };
}

/** A firewall's installation with the policy it applies. */
type Installed = Installation & { readonly policy?: InstalledPolicy };

/** What a structure check gives of a message it passes. */
type Passed = Extract<
  Effect.Effect.Success<ReturnType<StructureCheck>>,
  { readonly kind: "passed" }
>;

/** How many checks run at once when no policy sets `maximumConcurrentChecks`. */
const defaultConcurrentChecks = 4;

/**
 * Decides a message. The structure check judges it first, with the earlier
 * messages; its refusal refuses the message. The text rules then judge the
 * message: the owner's rules and those the structure check gives it. An agent
 * without a message policy allows every message.
 */
function decide(
  installed: Installed,
  message: Message,
  earlier: readonly EarlierMessage[],
  direction: Direction,
) {
  return Effect.gen(function* () {
    const { policy, structureCheck } = installed;
    if (policy === undefined) {
      return { kind: "allowed" } satisfies CheckResult;
    }
    const structure = yield* (
      structureCheck?.(message, earlier) ?? Effect.succeed(undefined)
    );
    if (structure?.kind === "refused") {
      const reply =
        structure.reply ??
        (structure.reason === "rule-violation" && installed.notice !== undefined
          ? `${installed.notice} ${structure.feedback}`
          : undefined);
      return refused(message, direction, yield* Clock.currentTimeMillis, {
        reason: structure.reason,
        rule: structure.ruleId,
        decidedBy: [{ ruleId: structure.ruleId }],
        checkedRuleIds: [structure.ruleId],
        feedback: [structure.feedback],
        ...(structure.detail === undefined ? {} : { detail: structure.detail }),
        ...(reply === undefined ? {} : { reply }),
      });
    }
    return yield* checkMessageText(
      { policy, ...installed },
      { message, earlier },
      direction,
      structure,
    );
  });
}

/** What a refusal is built from: the check's conclusion and the text that answers a withheld message. */
interface Refused extends Pick<CheckRecord, "decidedBy" | "checkedRuleIds"> {
  readonly reason: FirewallRefusalReason;
  readonly rule?: string;
  readonly feedback: readonly string[];
  readonly detail?: unknown;
  readonly reply?: string;
}

/**
 * The refusal of `message` at `time`, with its record. A check that could not
 * run gives no record. Only a withheld message has a reply: its record is
 * that of a message this agent sends to the withheld message's author, which
 * this firewall allows without checking its own words.
 */
function refused(
  message: Message,
  direction: Direction,
  time: number,
  { reply, rule, feedback, detail, ...concluded }: Refused,
): CheckResult {
  const checkId = mintCheckId();
  return {
    kind: "refused",
    checkId,
    reason: concluded.reason,
    ...(rule === undefined ? {} : { rule }),
    feedback,
    ...(detail === undefined ? {} : { detail }),
    ...(concluded.reason === "check-unavailable"
      ? {}
      : {
          record: {
            ...concluded,
            checkId,
            direction,
            decision: direction === "outgoing" ? "refused" : "withheld",
            time,
            peers:
              direction === "outgoing" ? message.recipients : [message.author],
          },
        }),
    ...(reply === undefined || direction === "outgoing"
      ? {}
      : {
          reply: {
            text: reply,
            record: {
              checkId: mintCheckId(),
              direction: "outgoing",
              decision: "allowed",
              time,
              peers: [message.author],
              reason: "eligible",
              decidedBy: [],
              checkedRuleIds: [],
            },
          },
        }),
  };
}

/**
 * The provider's window of the earlier messages: the latest `historyLimit`
 * of them within the byte limit of one runtime item. `gaps` names what the
 * byte limit cut.
 */
function historyWindow(
  earlier: readonly EarlierMessage[],
  historyLimit: number,
) {
  const latest = earlier.slice(-historyLimit);
  let bytes = 0;
  let kept = 0;
  for (const { message } of [...latest].reverse()) {
    bytes += Buffer.byteLength(JSON.stringify(message));
    if (bytes > MAXIMUM_RUNTIME_ITEM_BYTES) {
      break;
    }
    kept += 1;
  }
  return {
    history: latest.slice(latest.length - kept),
    earlierHistoryOmitted: kept < earlier.length,
    gaps:
      kept < latest.length
        ? ["The preceding history exceeds the evidence byte limit."]
        : [],
  };
}

/**
 * One check of a message against its text rules, with the history before it.
 * A message the structure check passed is also judged by the text rules that
 * check gives it, and the provider reads its protocol context. A refusal of
 * such a message carries the structure check's detail. A withheld message is
 * answered as that check phrases the reply, else with the feedback of the
 * `reply` rules it broke, else, when it broke a rule, with the notice.
 */
function checkMessageText(
  installed: Installed & { readonly policy: InstalledPolicy },
  candidate: {
    readonly message: Message;
    readonly earlier: readonly EarlierMessage[];
  },
  direction: Direction,
  structure?: Passed,
) {
  return Effect.gen(function* () {
    const { policy, me: self } = installed;
    const { message } = candidate;
    const now = yield* Clock.currentTimeMillis;
    const window = historyWindow(candidate.earlier, installed.historyLimit);
    const cards = new Map<string, AgentCard>();
    for (const card of [
      self,
      message.author,
      ...message.recipients,
      ...window.history.flatMap((entry) => [
        entry.message.author,
        ...entry.message.recipients,
      ]),
    ]) {
      cards.set(card.agentId, card);
    }
    const evidence: CheckEvidence = {
      agentId: self.agentId,
      principal: self.principalId,
      participants: [...cards.values()].map((card) => ({
        agentId: card.agentId,
        principal: card.principalId,
      })),
      history: window.history.map((entry) => ({
        ...assessmentText(entry.message),
        visibleToAgent:
          entry.allowed || entry.message.author.agentId === self.agentId,
      })),
      earlierHistoryOmitted: window.earlierHistoryOmitted,
      now,
      gaps: window.gaps,
      ...(structure?.protocol === undefined
        ? {}
        : { protocol: structure.protocol }),
    };
    const textRules = [...policy.textRules, ...(structure?.textRules ?? [])];
    const check = yield* checkCapturedMessage(
      direction,
      assessmentText(message),
      evidence,
      {
        textRules,
        revision: policy.revision,
        settings: policy.configured.settings,
        ...(policy.provider === undefined ? {} : { provider: policy.provider }),
      },
    ).pipe(
      markCheckActivity(direction),
      Effect.withSpan("assessMessage"),
      policy.permits.withPermits(1),
    );
    const { reason, decidedBy, checkedRuleIds } = check;
    if (reason === "eligible") {
      const checkId = mintCheckId();
      return {
        kind: "allowed",
        ...(check.instructions.length === 0
          ? {}
          : { guidance: { checkId, instructions: check.instructions } }),
        record: {
          checkId,
          direction,
          decision: "allowed",
          time: now,
          peers:
            direction === "outgoing" ? message.recipients : [message.author],
          reason,
          decidedBy,
          checkedRuleIds,
        },
      } satisfies CheckResult;
    }
    const reply =
      structure?.replyTo?.(
        check.feedback.join(" ") ||
          `The semantic policy check returned ${reason}.`,
      ) ??
      (textReply(textRules, check) ||
        (reason === "rule-violation" ? installed.notice : undefined));
    return refused(message, direction, now, {
      reason,
      decidedBy,
      checkedRuleIds,
      feedback: check.feedback,
      ...(structure?.detail === undefined ? {} : { detail: structure.detail }),
      ...(reply === undefined ? {} : { reply }),
    });
  });
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

/**
 * Installs the message policy `source` gives. Its revision, which the
 * provider reads, covers the configured rules and provider. Without
 * `provider`, Jev judges the text rules of a policy that names it.
 */
function buildPolicy(source: LoadedMessagePolicy, injected?: DecisionProvider) {
  const configured = source.policy;
  const provider =
    injected ??
    (source.provider === undefined
      ? undefined
      : retryingTransientFailures(makeJevDecisionProvider(source.provider)));
  const textRules = configured.rules;
  const maximumConcurrentChecks =
    configured.maximumConcurrentChecks ?? defaultConcurrentChecks;
  return {
    configured,
    provider,
    revision: digest({
      configuration: { maximumConcurrentChecks },
      questionEdition: textQuestionEdition,
      textRules,
      ...(provider === undefined
        ? {}
        : {
            providerSettings: configured.settings,
            provider: provider.provider ?? null,
            model: provider.model ?? null,
          }),
    }),
    textRules,
    permits: Effect.unsafeMakeSemaphore(maximumConcurrentChecks),
  };
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
