/**
 * @file The firewall's rule chain. Rules apply in order; each names a matcher
 * and an action, and the first rule whose matcher matches decides, as in a
 * Google Cloud firewall policy. A message no rule matches goes on to the
 * owner's text rules. Matchers are registered by name when the daemon starts,
 * so the firewall knows what a rule checks only through its matcher.
 */

import { Effect, Schema } from "effect";
import type { Message, Outgoing } from "../communication/service.js";
import type { AgentCard } from "../identity/agent-cards.js";
import { exactStruct } from "../encoding/canonical-json.js";
import {
  FirewallPolicyError,
  type firewallRefusalReasonSchema,
  TextRule,
} from "./rules.js";

type RefusalReason = typeof firewallRefusalReasonSchema.Type;

/** Carries bounded preceding history and each message's visibility to the agent. */
export interface FirewallEvidence {
  readonly history: ReadonlyArray<{
    readonly message: Message;
    readonly visibleToAgent: boolean;
  }>;
  readonly earlierHistoryOmitted: boolean;
  /**
   * This agent's sent messages that the Router has not ordered yet, oldest
   * first. They are not in `history`; the Router orders them before any later
   * message of this agent, and a peer's message may be ordered between them.
   */
  readonly pendingOutgoing: readonly Message[];
  /** What else the history lacks, such as messages beyond the evidence byte limit. */
  readonly gaps: readonly string[];
}

const chainRuleSchema = exactStruct({
  id: TextRule.fields.id,
  applies: TextRule.fields.applies,
  match: Schema.String.pipe(Schema.pattern(/^[a-z][a-z0-9.-]{0,63}$/u)),
  action: Schema.Literal("allow", "deny", "reply", "hold"),
});

/**
 * One rule of the chain. `id` and `applies` mean what they mean on a text rule,
 * and `match` names a registered matcher. `deny` refuses an outgoing draft or
 * silently withholds an incoming message; `reply` also sends the matcher's
 * reply to the sender of a withheld message. `hold` asks the owner to approve
 * an outgoing draft and, once approved, goes on to the next rule.
 */
type ChainRule = typeof chainRuleSchema.Type;

/**
 * A rule file. Ids are unique, and `hold` applies only to outgoing drafts
 * because an incoming message has no local sender to approve it.
 */
export const chainRuleFileSchema = exactStruct({
  rules: Schema.Array(chainRuleSchema),
}).pipe(
  Schema.filter(
    ({ rules }) =>
      new Set(rules.map((rule) => rule.id)).size === rules.length ||
      "rule ids must be unique",
  ),
  Schema.filter(
    ({ rules }) =>
      rules.every(
        (rule) => rule.action !== "hold" || rule.applies === "outgoing",
      ) || "hold rules must apply to outgoing messages only",
  ),
);

/** What a matcher sees: the message, its direction, the permitted history and this agent. */
export interface MatchInput {
  readonly direction: "incoming" | "outgoing";
  readonly message: Outgoing;
  readonly evidence: FirewallEvidence;
  readonly self: AgentCard;
}

/**
 * A match refuses for a rule violation unless it names another reason, such
 * as `unresolved-evidence` when the history it needs is missing. `detail` is a
 * JSON value the refused agent reads, and `reply` is the text a `reply` rule
 * sends to the sender of the message it withholds.
 */
export type MatchResult =
  | { readonly matched: false }
  | {
      readonly matched: true;
      readonly refusal?: Exclude<RefusalReason, "owner-approval-required">;
      readonly detail?: unknown;
      readonly reply?: string;
    };

/** A named check that chain rules refer to. A matcher that cannot decide matches with `check-unavailable`. */
export interface Matcher {
  readonly match: (input: MatchInput) => Effect.Effect<MatchResult>;
}

/**
 * The rules and the matchers they name. `approveHeld` asks the owner to
 * approve a draft a `hold` rule matched; without it, every held draft is
 * approved.
 */
export interface Chain {
  readonly rules: readonly ChainRule[];
  readonly matchers: Readonly<Record<string, Matcher>>;
  readonly approveHeld?: (
    rule: ChainRule,
    message: Outgoing,
  ) => Effect.Effect<boolean>;
}

/** Whether a rule decided the message, and how. */
export type ChainOutcome =
  | { readonly kind: "unmatched" }
  | { readonly kind: "allowed"; readonly rule: string }
  | {
      readonly kind: "refused";
      readonly rule: string;
      readonly refusal: RefusalReason;
      readonly detail?: unknown;
      readonly reply?: string;
    };

const unavailable: MatchResult = {
  matched: true,
  refusal: "check-unavailable",
};

/**
 * Fails with the first rule whose matcher is not registered, so a rule can
 * never be skipped for want of its check.
 */
export function validateChain(
  chain: Chain,
): Effect.Effect<void, FirewallPolicyError> {
  const missing = chain.rules.find(
    (rule) => !Object.hasOwn(chain.matchers, rule.match),
  );
  return missing === undefined
    ? Effect.void
    : Effect.fail(
        new FirewallPolicyError({ reason: "build-failed", ruleId: missing.id }),
      );
}

/** Applies the rules for the message's direction in order; the first that decides ends the chain. */
export function evaluateChain(
  chain: Chain,
  input: MatchInput,
): Effect.Effect<ChainOutcome> {
  return Effect.gen(function* () {
    const rules = chain.rules.filter(
      (rule) => rule.applies === "both" || rule.applies === input.direction,
    );
    for (const rule of rules) {
      const outcome = yield* applyRule(chain, rule, input);
      if (outcome !== undefined) {
        return outcome;
      }
    }
    return { kind: "unmatched" } satisfies ChainOutcome;
  }).pipe(Effect.withSpan("evaluateChain"));
}

/**
 * One rule's outcome, or nothing when the chain goes on: its matcher did not
 * match, or it held a draft the owner approved. A matcher that fails or is
 * missing counts as a match that could not be checked.
 */
function applyRule(
  chain: Chain,
  rule: ChainRule,
  input: MatchInput,
): Effect.Effect<ChainOutcome | undefined> {
  return Effect.gen(function* () {
    const matcher = Object.hasOwn(chain.matchers, rule.match)
      ? chain.matchers[rule.match]
      : undefined;
    const result =
      matcher === undefined
        ? unavailable
        : yield* matcher
            .match(input)
            .pipe(Effect.catchAllDefect(() => Effect.succeed(unavailable)));
    if (!result.matched) {
      return undefined;
    }
    const refused = {
      kind: "refused",
      rule: rule.id,
      refusal: result.refusal ?? "rule-violation",
      ...(result.detail === undefined ? {} : { detail: result.detail }),
    } satisfies ChainOutcome;
    switch (rule.action) {
      case "allow":
        return { kind: "allowed", rule: rule.id } satisfies ChainOutcome;
      case "deny":
        return refused;
      case "reply":
        return input.direction === "incoming" && result.reply !== undefined
          ? { ...refused, reply: result.reply }
          : refused;
      case "hold":
        return (yield* approved(chain, rule, input.message))
          ? undefined
          : ({
              kind: "refused",
              rule: rule.id,
              refusal: "owner-approval-required",
            } satisfies ChainOutcome);
      default: {
        const exhaustive: never = rule.action;
        return exhaustive;
      }
    }
  });
}

function approved(chain: Chain, rule: ChainRule, message: Outgoing) {
  return chain.approveHeld === undefined
    ? Effect.succeed(true)
    : chain.approveHeld(rule, message);
}
