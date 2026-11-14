/**
 * @file The message policy's chain. Chain rules apply in order; each names a
 * matcher and an action, and the first rule whose matcher matches decides, as
 * in a Google Cloud firewall policy. A message no chain rule matches goes on
 * to the text rules. Matchers are registered by name when the daemon starts,
 * so the firewall knows what a chain rule checks only through its matcher.
 */

import { Effect, Schema } from "effect";
import type { Message } from "../communication/index.js";
import type { Outgoing } from "../communication/service.js";
import type { AgentCard } from "../identity.js";
import { exactStruct } from "../encoding.js";
import {
  type Direction,
  FirewallPolicyError,
  type FirewallRefusalReason,
  TextRule,
} from "./rules.js";

/** The evidence a check may read: bounded preceding history and each message's visibility to the agent. */
export interface FirewallEvidence {
  readonly history: ReadonlyArray<{
    readonly message: Message;
    readonly visibleToAgent: boolean;
  }>;
  readonly earlierHistoryOmitted: boolean;
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
 * One chain rule. `id` and `applies` mean what they mean on a text rule,
 * and `match` names a registered matcher. `deny` refuses an outgoing draft or
 * silently withholds an incoming message; `reply` also sends the matcher's
 * reply to the sender of a withheld message. `hold` asks the owner to approve
 * an outgoing draft and, once approved, goes on to the next rule.
 */
type ChainRule = typeof chainRuleSchema.Type;

/**
 * A file of chain rules. Ids are unique, and `hold` applies only to outgoing
 * drafts because an incoming message has no local sender to approve it.
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

/** What a matcher sees: the message, its direction, its evidence and this agent. */
export interface MessageToMatch {
  readonly direction: Direction;
  readonly message: Outgoing;
  readonly evidence: FirewallEvidence;
  readonly self: AgentCard;
}

/**
 * What a matcher finds for one message. A match refuses for a rule violation
 * unless it names another reason, such as `unresolved-evidence` when the
 * history it needs is missing. `detail` is a JSON value the refused agent
 * reads, and `reply` is the text a `reply` rule sends to the sender of the
 * message it withholds.
 */
export type Match =
  | { readonly matched: false }
  | {
      readonly matched: true;
      readonly reason?: Exclude<
        FirewallRefusalReason,
        "owner-approval-required"
      >;
      readonly detail?: unknown;
      readonly reply?: string;
    };

/** A named check that chain rules refer to. A matcher that cannot decide matches with `check-unavailable`. */
export interface Matcher {
  readonly match: (toMatch: MessageToMatch) => Effect.Effect<Match>;
}

/**
 * The chain rules and the matchers they name. `approveHeld` asks the owner to
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

/**
 * What the chain decides for one message: the chain rule that allowed or
 * refused it and the reason it refused, or `unmatched` when no rule decided.
 * A refused incoming message is withheld.
 */
export type ChainDecision =
  | { readonly kind: "unmatched" }
  | { readonly kind: "allowed"; readonly ruleId: string }
  | {
      readonly kind: "refused";
      readonly ruleId: string;
      readonly reason: FirewallRefusalReason;
      readonly detail?: unknown;
      readonly reply?: string;
    };

const unavailable: Match = {
  matched: true,
  reason: "check-unavailable",
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

/** Applies the chain rules for the message's direction in order; the first that decides ends the chain. */
export const evaluateChain = Effect.fn("evaluateChain")(function* (
  chain: Chain,
  toMatch: MessageToMatch,
) {
  const rules = chain.rules.filter(
    (rule) => rule.applies === "both" || rule.applies === toMatch.direction,
  );
  for (const rule of rules) {
    const decision = yield* applyRule(chain, rule, toMatch);
    if (decision !== undefined) {
      return decision;
    }
  }
  return { kind: "unmatched" } satisfies ChainDecision;
});

/**
 * One chain rule's decision, or nothing when the chain goes on: its matcher
 * did not match, or it held a draft the owner approved. A matcher that fails
 * or is missing counts as a match that could not be checked.
 */
function applyRule(
  chain: Chain,
  rule: ChainRule,
  toMatch: MessageToMatch,
): Effect.Effect<ChainDecision | undefined> {
  return Effect.gen(function* () {
    const matcher = Object.hasOwn(chain.matchers, rule.match)
      ? chain.matchers[rule.match]
      : undefined;
    const match =
      matcher === undefined
        ? unavailable
        : yield* matcher
            .match(toMatch)
            .pipe(Effect.catchAllDefect(() => Effect.succeed(unavailable)));
    if (!match.matched) {
      return undefined;
    }
    const refused = {
      kind: "refused",
      ruleId: rule.id,
      reason: match.reason ?? "rule-violation",
      ...(match.detail === undefined ? {} : { detail: match.detail }),
    } satisfies ChainDecision;
    switch (rule.action) {
      case "allow":
        return { kind: "allowed", ruleId: rule.id } satisfies ChainDecision;
      case "deny":
        return refused;
      case "reply":
        return toMatch.direction === "incoming" && match.reply !== undefined
          ? { ...refused, reply: match.reply }
          : refused;
      case "hold":
        return (yield* approved(chain, rule, toMatch.message))
          ? undefined
          : ({
              kind: "refused",
              ruleId: rule.id,
              reason: "owner-approval-required",
            } satisfies ChainDecision);
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
