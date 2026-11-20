/** @file Text rules, the message-policy file and the schemas of decisions. */

import { FileSystem } from "@effect/platform";
import {
  Config,
  Data,
  Effect,
  Option,
  type Redacted,
  Schema,
  Struct,
} from "effect";
import { exactStruct } from "../encoding.js";
import { AgentCard } from "../identity.js";

const identifier = Schema.NonEmptyTrimmedString;

/** Operational holds remain distinct from a settled rule violation. */
const firewallRefusalReasonSchema = Schema.Literal(
  "rule-violation",
  "unresolved-evidence",
  "check-unavailable",
);

/** Why the firewall refused a message its agent sent, or withheld one it received. */
export type FirewallRefusalReason = typeof firewallRefusalReasonSchema.Type;

/** Whether the agent received the message under check or is sending it. */
export type Direction = "incoming" | "outgoing";

/**
 * One text rule: the prose a decision provider judges, the guidance it gives
 * the receiving agent and the feedback it gives the sender of a message it
 * refuses. Validation requires nonempty text without restricting its
 * language. An incoming message that violates a rule whose `action` is `reply`
 * is withheld, and its sender receives the rule's feedback text as a message;
 * `deny`, the default, withholds it silently.
 * The message policy specification defines who owns each rule and how edits
 * are validated.
 */
export class TextRule extends Schema.Class<TextRule>("TextRule")({
  id: identifier,
  guidance: Schema.optional(Schema.NonEmptyString),
  feedback: Schema.optional(Schema.NonEmptyString),
  action: Schema.optional(Schema.Literal("deny", "reply")),
  text: Schema.NonEmptyString,
  applies: Schema.Literal("incoming", "outgoing", "both"),
}) {}

/**
 * How long one request to the decision provider may take, and the
 * probability below which its answer is unsure.
 */
export class ProviderRequestSettings extends Schema.Class<ProviderRequestSettings>(
  "ProviderRequestSettings",
)({
  minimumProbability: Schema.Number.pipe(Schema.between(0.5, 1)),
  timeoutMilliseconds: Schema.Positive.pipe(Schema.int()),
}) {}

/** Inclusive Unix milliseconds permit polling across checks completed out of start order. */
export const firewallDecisionsRequestSchema = exactStruct({
  since: Schema.optional(Schema.NonNegativeInt),
});

/**
 * What one completed check concluded, as its caller saves it, without message
 * content: the decision applied to a message the agent sent (`allowed` or
 * `refused`) or received (`allowed` or `withheld`), and its peers. `time` is
 * Unix milliseconds. `decidedBy[].advice` contains the rule's feedback for a
 * refused or withheld message, or its guidance for an allowed one.
 * `checkedRuleIds` lists the rules the check judged: a rule that does not
 * apply to the message's direction is not listed.
 */
export const checkRecordSchema = exactStruct({
  checkId: Schema.NonEmptyString,
  direction: Schema.Literal("incoming", "outgoing"),
  decision: Schema.Literal("allowed", "refused", "withheld"),
  time: Schema.NonNegativeInt,
  peers: Schema.Array(AgentCard),
  reason: Schema.Union(Schema.Literal("eligible"), firewallRefusalReasonSchema),
  decidedBy: Schema.Array(
    exactStruct({
      ruleId: Schema.NonEmptyString,
      advice: Schema.optional(Schema.NonEmptyString),
    }),
  ),
  checkedRuleIds: Schema.Array(Schema.NonEmptyString),
});
/** @useDeclaredType */
export type CheckRecord = typeof checkRecordSchema.Type;

/** Reports each saved check once per peer. */
export const firewallDecisionsSchema = exactStruct({
  decisions: Schema.Array(
    exactStruct({
      ...Struct.omit(checkRecordSchema.fields, "peers"),
      peer: AgentCard,
    }),
  ),
});

/** A message-policy file or the decision provider's key that cannot be loaded, and why. */
class MessagePolicyConfigurationError extends Data.TaggedError(
  "MessagePolicyConfigurationError",
)<{ readonly message: string }> {}

const refused = (message: string) =>
  new MessagePolicyConfigurationError({ message });

/** Jev judges text rules with the key `SHARNESS_DECISION_PROVIDER_KEY` holds. */
const decisionProvider = Schema.Struct({
  model: Schema.NonEmptyTrimmedString,
});

const defaultProviderRequestSettings = {
  minimumProbability: 0.8,
  timeoutMilliseconds: 15_000,
};

const document = Schema.Struct({
  rules: Schema.optionalWith(Schema.Array(TextRule), { default: () => [] }),
  decisionProvider: Schema.optional(decisionProvider),
  settings: Schema.optionalWith(Schema.Struct(ProviderRequestSettings.fields), {
    default: () => defaultProviderRequestSettings,
  }),
  maximumConcurrentChecks: Schema.optional(
    Schema.Int.pipe(Schema.between(1, 32)),
  ),
});

/**
 * Fills omitted settings with defaults and requires a provider for text rules.
 * Rule ids must be unique.
 */
const MessagePolicy = document.pipe(
  Schema.filter(
    (policy) =>
      policy.decisionProvider !== undefined || policy.rules.length === 0,
    {
      message: () => "decisionProvider is required for text rules",
    },
  ),
  Schema.filter(
    (policy) => {
      const ids = policy.rules.map((rule) => rule.id);
      return new Set(ids).size === ids.length;
    },
    { message: () => "text rule ids must be unique" },
  ),
);

type MessagePolicy = typeof MessagePolicy.Type;

/** Rejects unknown properties so misspelled settings cannot silently weaken policy. */
export const decodeMessagePolicy = Schema.decodeUnknown(
  Schema.parseJson(MessagePolicy),
  { onExcessProperty: "error" },
);

/**
 * A message policy as loaded from its file, with the provider's credential
 * redacted. A policy that names no
 * `decisionProvider` has no provider.
 */
export interface LoadedMessagePolicy {
  readonly policy: MessagePolicy;
  readonly provider?: {
    readonly model: string;
    readonly apiKey: Redacted.Redacted;
  };
}

/**
 * Resolves the configured policy during setup, returning none when unset. A
 * policy that names a `decisionProvider` reads its key from the environment
 * variable `SHARNESS_DECISION_PROVIDER_KEY`, even without text rules of its
 * own: a structure check can add rules for it to judge. A file that
 * cannot be read or decoded fails with a message naming it.
 */
export const loadMessagePolicy = Effect.fn("loadMessagePolicy")(function* (
  path?: string,
) {
  if (path === undefined) {
    return Option.none<LoadedMessagePolicy>();
  }
  const filesystem = yield* FileSystem.FileSystem;
  const policy = yield* filesystem.readFileString(path).pipe(
    Effect.flatMap(decodeMessagePolicy),
    Effect.mapError((error) =>
      refused(
        `The message policy file ${path} is unreadable or invalid: ${error.message}`,
      ),
    ),
  );
  const setting = policy.decisionProvider;
  if (setting === undefined) {
    return Option.some<LoadedMessagePolicy>({ policy });
  }
  const apiKey = yield* Config.redacted(
    Config.nonEmptyString("SHARNESS_DECISION_PROVIDER_KEY"),
  ).pipe(
    Effect.mapError(() =>
      refused(
        `The message policy ${path} names a decisionProvider and SHARNESS_DECISION_PROVIDER_KEY does not hold its key`,
      ),
    ),
  );
  return Option.some<LoadedMessagePolicy>({
    policy,
    provider: { model: setting.model, apiKey },
  });
});
