/** @file Explicit firewall rules, edit contracts and bounded configuration loading. */

import { Config, Data, Effect, Option, type Redacted, Schema } from "effect";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- Nonblocking descriptor opens reject special files before a writer can block startup.
import { constants } from "node:fs";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- Configuration loads before daemon platform services are composed.
import { type FileHandle, lstat, open, realpath } from "node:fs/promises";
import { dirname, isAbsolute, relative, resolve, sep, win32 } from "node:path";
import { parseDocument } from "yaml";
import { exactStruct } from "../encoding.js";
import { AgentCard } from "../identity.js";

const identifier = Schema.NonEmptyTrimmedString;

/** Operational holds remain distinct from a settled rule violation. */
const firewallRefusalReasonSchema = Schema.Literal(
  "rule-violation",
  "unresolved-evidence",
  "check-unavailable",
);

/** Why the firewall refused a message. */
export type FirewallRefusalReason = typeof firewallRefusalReasonSchema.Type;

/** Whether the agent received the message under check or is sending it. */
export type Direction = "incoming" | "outgoing";

/** A fact the owner states for every check, with where it comes from. */
export class OwnerFact extends Schema.Class<OwnerFact>("OwnerFact")({
  id: identifier,
  source: identifier,
  text: Schema.NonEmptyString,
}) {}

/**
 * One text rule: the prose a decision provider judges, the guidance it gives
 * the receiving agent and the feedback it gives the sender of a message it
 * refuses. Validation requires nonempty text without restricting its
 * language.
 * The message policy specification defines who owns each rule and how edits
 * are validated.
 */
export class TextRule extends Schema.Class<TextRule>("TextRule")({
  id: identifier,
  guidance: Schema.optional(Schema.NonEmptyString),
  feedback: Schema.optional(Schema.NonEmptyString),
  text: Schema.NonEmptyString,
  applies: Schema.Literal("incoming", "outgoing", "both"),
}) {}

/**
 * How one request to the decision provider is bounded, and the probability
 * below which its answer is unsure.
 */
export class ProviderRequestSettings extends Schema.Class<ProviderRequestSettings>(
  "ProviderRequestSettings",
)({
  minimumProbability: Schema.Number.pipe(Schema.between(0.5, 1)),
  maximumRequestBytes: Schema.Positive.pipe(Schema.int()),
  timeoutMilliseconds: Schema.Positive.pipe(Schema.int()),
}) {}

/** Inclusive Unix milliseconds permit polling across checks completed out of start order. */
export const firewallDecisionsRequestSchema = exactStruct({
  since: Schema.optional(Schema.NonNegativeInt),
});

/**
 * Contains one row per peer without message content.
 * `decidedBy[].advice` contains the
 * rule's feedback for a refused message, or its guidance for an
 * allowed one. `from` identifies a contract stage as `contract`, or a text rule
 * as `fixed`. `checkedRuleIds` lists the rules the check judged: a
 * rule that does not apply to the message's direction is not listed.
 */
export const firewallDecisionSummarySchema = exactStruct({
  direction: Schema.Literal("incoming", "outgoing"),
  peer: AgentCard,
  reason: Schema.Union(Schema.Literal("eligible"), firewallRefusalReasonSchema),
  decidedBy: Schema.Array(
    exactStruct({
      ruleId: Schema.NonEmptyString,
      from: Schema.Literal("contract", "fixed"),
      advice: Schema.optional(Schema.NonEmptyString),
    }),
  ),
  checkedRuleIds: Schema.Array(Schema.NonEmptyString),
  activeRevision: Schema.NonEmptyString,
});

/**
 * Reports each saved check with the decision the daemon applied.
 */
export const firewallDecisionsSchema = exactStruct({
  decisions: Schema.Array(
    exactStruct({
      ...firewallDecisionSummarySchema.fields,
      decision: Schema.Literal("allowed", "refused"),
      checkId: Schema.NonEmptyString,
      time: Schema.NonNegativeInt,
    }),
  ),
});

const policyReasonText: Readonly<
  Record<FirewallPolicyError["reason"], string>
> = {
  "not-configured":
    "the daemon has no message policy, or its policy has no decisionProvider to judge text rules",
  "build-failed": "the rules could not be installed",
  "persistence-failed": "the decisions could not be stored",
};

/** The message policy could not be installed, or its decisions could not be stored or read. */
export class FirewallPolicyError extends Schema.TaggedError<FirewallPolicyError>()(
  "FirewallPolicyError",
  {
    reason: Schema.Literal(
      "not-configured",
      "build-failed",
      "persistence-failed",
    ),
  },
) {
  override get message(): string {
    return policyReasonText[this.reason];
  }
}

/** The message-policy file, a behavior it lists or the decision provider's key cannot be loaded. */
class MessagePolicyConfigurationError extends Data.TaggedError(
  "MessagePolicyConfigurationError",
) {}

const refused = () => new MessagePolicyConfigurationError();

const messagePolicyFormat = "org.social-harness/message-policy/v2";

/** Jev judges text rules with the key `SHARNESS_DECISION_PROVIDER_KEY` holds. */
const decisionProvider = Schema.Struct({
  model: Schema.NonEmptyTrimmedString,
});

const textRules = Schema.optionalWith(Schema.Array(TextRule), {
  default: () => [],
});
const defaultProviderRequestSettings = {
  minimumProbability: 0.8,
  maximumRequestBytes: 384_000,
  timeoutMilliseconds: 15_000,
};

const textDocument = Schema.Struct({
  fixedRules: textRules,
  fixedBehaviors: Schema.optional(Schema.Array(Schema.String)),
});

const document = Schema.Struct({
  format: Schema.Literal(messagePolicyFormat),
  text: Schema.optional(textDocument),
  decisionProvider: Schema.optional(decisionProvider),
  textSettings: Schema.optionalWith(
    Schema.Struct(ProviderRequestSettings.fields),
    {
      default: () => defaultProviderRequestSettings,
    },
  ),
  facts: Schema.optionalWith(Schema.Array(OwnerFact), {
    default: () => [],
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
      policy.decisionProvider !== undefined || policy.text === undefined,
    {
      message: () => "decisionProvider is required for text rules",
    },
  ),
  Schema.filter((policy) => uniqueTextRuleIds(policy), {
    message: () => "text rule ids must be unique",
  }),
);

type MessagePolicy = typeof MessagePolicy.Type;

/** Rejects unknown properties so misspelled settings cannot silently weaken policy. */
export const decodeMessagePolicy = Schema.decodeUnknown(
  Schema.parseJson(MessagePolicy),
  { onExcessProperty: "error" },
);

/** Limits each `BEHAVIOR.md` to 64 KiB. */
export const MAXIMUM_DOCUMENT_BYTES = 65_536;

/** YAML defines frontmatter syntax. Only Social Harness metadata rejects unknown fields. */
const behaviorFrontmatter = Schema.Struct({
  name: Schema.NonEmptyTrimmedString,
  description: Schema.NonEmptyString,
  metadata: Schema.Struct({
    "social-harness": exactStruct({
      applies: TextRule.fields.applies,
      guidance: TextRule.fields.guidance,
      feedback: TextRule.fields.feedback,
    }),
  }),
});

type BehaviorRefusal = "frontmatter" | "schema";

const MAXIMUM_POLICY_BYTES = 1_048_576;

/**
 * A message policy as loaded from its file, with its behaviors resolved to
 * text rules and the provider's credential redacted. A policy that names no
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
 * own: the norms an agent holds can add rules for it to judge. Invalid
 * documents or a missing key fail without exposing file contents.
 */
export const loadMessagePolicy = Effect.fn("loadMessagePolicy")(function* (
  path?: string,
) {
  if (path === undefined) {
    return Option.none<LoadedMessagePolicy>();
  }
  const policy = yield* readCanonicalFile(path, MAXIMUM_POLICY_BYTES).pipe(
    Effect.flatMap((bytes) =>
      decodeMessagePolicy(bytes.toString("utf8")).pipe(
        Effect.mapError(refused),
      ),
    ),
  );
  const resolved = yield* resolveBehaviors(policy, dirname(path));
  const setting = resolved.decisionProvider;
  if (setting === undefined) {
    return Option.some<LoadedMessagePolicy>({ policy: resolved });
  }
  const apiKey = yield* Config.redacted(
    Config.nonEmptyString("SHARNESS_DECISION_PROVIDER_KEY"),
  ).pipe(Effect.mapError(refused));
  return Option.some<LoadedMessagePolicy>({
    policy: resolved,
    provider: { model: setting.model, apiKey },
  });
});

/**
 * Maps a `BEHAVIOR.md` to a text rule. Callers must bound `bytes` by
 * {@link MAXIMUM_DOCUMENT_BYTES}. Only a closed failure category reaches
 * `refused`. YAML diagnostics stay private.
 */
const decodeBehavior = Effect.fn("decodeBehavior")(function* <Failure>(
  bytes: Buffer,
  refused: (reason: BehaviorRefusal) => Failure,
) {
  const parts = /^---\r?\n([\s\S]*?)^---[ \t]*(?:\r?\n|$)([\s\S]*)/mu.exec(
    bytes.toString("utf8").replace(/^\uFEFF/u, ""),
  );
  if (parts?.[1] === undefined || parts[2] === undefined || parts.index !== 0) {
    return yield* Effect.fail(refused("frontmatter"));
  }
  const frontmatter = yield* decodeFrontmatter(parts[1], refused);
  const metadata = frontmatter.metadata["social-harness"];
  return yield* Schema.decodeUnknown(exactStruct(TextRule.fields))({
    id: frontmatter.name,
    text: parts[2].trim(),
    ...metadata,
  }).pipe(Effect.mapError(() => refused("schema")));
});

function uniqueTextRuleIds(policy: typeof document.Type): boolean {
  const ids = (policy.text?.fixedRules ?? []).map((rule) => rule.id);
  return new Set(ids).size === ids.length;
}

/** Keeps YAML parser diagnostics, including unresolved tags, private. */
function decodeFrontmatter<Failure>(
  text: string,
  refused: (reason: BehaviorRefusal) => Failure,
) {
  return Effect.gen(function* () {
    const doc = yield* Effect.try({
      try: () =>
        parseDocument(text, {
          schema: "core",
          resolveKnownTags: false,
          logLevel: "error",
        }),
      catch: () => refused("frontmatter"),
    });
    if (doc.errors.length !== 0 || doc.warnings.length !== 0) {
      return yield* Effect.fail(refused("frontmatter"));
    }
    return yield* Effect.try({
      try: (): unknown => doc.toJS(),
      catch: () => refused("frontmatter"),
    }).pipe(
      Effect.flatMap((value) =>
        Schema.decodeUnknown(behaviorFrontmatter)(value).pipe(
          Effect.mapError(() => refused("schema")),
        ),
      ),
    );
  });
}

function readBehavior(root: string, directory: string) {
  if (
    isAbsolute(directory) ||
    win32.isAbsolute(directory) ||
    directory.split(/[\\/]/u).includes("..")
  ) {
    return Effect.fail(refused());
  }
  return Effect.gen(function* () {
    const canonical = (location: string) =>
      Effect.tryPromise({
        try: () => realpath(location),
        catch: refused,
      });
    const locations = yield* Effect.all({
      root: canonical(root),
      directory: canonical(resolve(root, directory)),
      file: canonical(resolve(root, directory, "BEHAVIOR.md")),
    });
    if (
      !contained(locations.root, locations.directory) ||
      !contained(locations.root, locations.file)
    ) {
      return yield* refused();
    }
    const bytes = yield* readBoundedFile(
      locations.file,
      MAXIMUM_DOCUMENT_BYTES,
    );
    return yield* decodeBehavior(bytes, refused);
  });
}

function resolveBehaviors(policy: MessagePolicy, root: string) {
  return Effect.gen(function* () {
    const text = policy.text;
    if (text === undefined) {
      return policy;
    }
    const ids = new Set(text.fixedRules.map((rule) => rule.id));
    const behaviors = yield* Effect.forEach(
      text.fixedBehaviors ?? [],
      (directory) =>
        Effect.gen(function* () {
          const behavior = yield* readBehavior(root, directory);
          if (ids.has(behavior.id)) {
            return yield* refused();
          }
          ids.add(behavior.id);
          return new TextRule(behavior);
        }),
      { concurrency: 1 },
    );
    const resolved: MessagePolicy = {
      ...policy,
      text: { fixedRules: [...text.fixedRules, ...behaviors] },
    };
    return resolved;
  });
}

/** Relative paths must stay in the canonical root, the policy directory, after symlink resolution. */
function contained(root: string, path: string) {
  const child = relative(root, path);
  return !isAbsolute(child) && child !== ".." && !child.startsWith(`..${sep}`);
}

/**
 * Reads the file `path` leads to through any symbolic links, as a Kubernetes
 * Secret volume presents every key.
 */
function readCanonicalFile(path: string, maximumBytes: number) {
  return Effect.tryPromise({
    try: () => realpath(path),
    catch: refused,
  }).pipe(
    Effect.flatMap((canonical) => readBoundedFile(canonical, maximumBytes)),
  );
}

/** The descriptor owns both the file-kind check and the bounded read. */
function readBoundedFile(path: string, maximumBytes: number) {
  return Effect.gen(function* () {
    const before = yield* Effect.tryPromise({
      try: () => lstat(path),
      catch: refused,
    });
    const file = yield* Effect.acquireRelease(
      Effect.tryPromise({
        try: () =>
          open(
            path,
            constants.O_RDONLY | constants.O_NONBLOCK | constants.O_NOFOLLOW,
          ),
        catch: refused,
      }),
      (file) =>
        Effect.tryPromise({
          try: () => file.close(),
          catch: refused,
        }).pipe(Effect.orDie),
    );
    const stat = yield* Effect.tryPromise({
      try: () => file.stat(),
      catch: refused,
    });
    if (!stat.isFile() || stat.dev !== before.dev || stat.ino !== before.ino) {
      return yield* refused();
    }
    const bytes = yield* readDescriptor(file, maximumBytes).pipe(
      Effect.mapError(refused),
    );
    if (bytes.byteLength > maximumBytes) {
      return yield* refused();
    }
    return bytes;
  }).pipe(Effect.scoped);
}

/** Continues after short reads until EOF or the byte beyond the limit. */
function readDescriptor(file: FileHandle, maximumBytes: number) {
  return Effect.gen(function* () {
    const buffer = Buffer.alloc(maximumBytes + 1);
    let length = 0;
    while (length < buffer.length) {
      const offset = length;
      const read = yield* Effect.tryPromise(() =>
        file.read(buffer, offset, buffer.length - offset, null),
      );
      if (read.bytesRead === 0) {
        break;
      }
      length += read.bytesRead;
    }
    return buffer.subarray(0, length);
  });
}
