/** @file Explicit firewall rules, edit contracts and bounded configuration loading. */

import { Data, Effect, Option, Redacted, Schema } from "effect";
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

/** Why the firewall refused a message its agent sent, or withheld one it received. */
export type FirewallRefusalReason = typeof firewallRefusalReasonSchema.Type;

/** Whether the agent received the message under check or is sending it. */
export type Direction = "incoming" | "outgoing";

/** Trusted callers supply facts. Visibility limits what guidance may reference. */
export class OwnerFact extends Schema.Class<OwnerFact>("OwnerFact")({
  id: identifier,
  source: identifier,
  text: Schema.NonEmptyString,
  visibility: Schema.Literal("runtime", "assessor"),
}) {}

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
  feedback: Schema.optional(exactStruct({ text: Schema.NonEmptyString })),
  action: Schema.optional(Schema.Literal("deny", "reply")),
  text: Schema.NonEmptyString,
  applies: Schema.Literal("incoming", "outgoing", "both"),
}) {}

/** The time and byte limits of one check against the text rules. */
export class CheckLimits extends Schema.Class<CheckLimits>("CheckLimits")({
  timeoutMilliseconds: Schema.Positive.pipe(Schema.int()),
  maximumInputBytes: Schema.Positive.pipe(Schema.int()),
  maximumResultBytes: Schema.Positive.pipe(Schema.int()),
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

/** One editable text rule, which admits no field a text rule lacks. */
export const editableRuleSchema = exactStruct(TextRule.fields);

/** Limits editable rules across startup, saved policy and updates. */
export const MAXIMUM_EDITABLE_RULES = 256;

/** Every editable text rule of one message policy. */
export const editableRulesSchema = Schema.Array(editableRuleSchema).pipe(
  Schema.maxItems(MAXIMUM_EDITABLE_RULES),
);

const behaviorAddition = exactStruct({ behavior: Schema.String });

/** A norm document for the agent to hold, compiled into a contract before the edit applies. */
const normAddition = exactStruct({ norm: Schema.String });

/** A norm the agent holds, by name, for it to stop holding. */
const normRemoval = exactStruct({ norm: Schema.NonEmptyString });

/**
 * Removing and adding the same editable id, or a norm of the same name,
 * replaces it in one activation.
 */
export const policyUpdateSchema = exactStruct({
  add: Schema.optional(
    Schema.Array(
      Schema.Union(editableRuleSchema, behaviorAddition, normAddition),
    ).pipe(Schema.maxItems(MAXIMUM_EDITABLE_RULES)),
  ),
  remove: Schema.optional(
    Schema.Array(Schema.Union(TextRule.fields.id, normRemoval)).pipe(
      Schema.maxItems(MAXIMUM_EDITABLE_RULES),
    ),
  ),
});

/**
 * `activeRevision` identifies the whole message policy in force, including its
 * fixed rules and held norms. `fixedRules` include the English rules of the
 * norms the agent holds, and `norms` names those norms.
 */
export const policyReadSchema = exactStruct({
  rules: editableRulesSchema,
  fixedRules: Schema.Array(TextRule),
  norms: Schema.Array(Schema.String),
  activeRevision: Schema.NonEmptyString,
});

/**
 * The revision in force once an edit applies. Edits affect future checks;
 * saved decisions stay unchanged.
 */
export const policyRevisionSchema = exactStruct({
  activeRevision: Schema.NonEmptyString,
});

/** Inclusive Unix milliseconds permit polling across checks completed out of start order. */
export const firewallDecisionsRequestSchema = exactStruct({
  since: Schema.optional(Schema.NonNegativeInt),
});

/** Activation metadata contains rule ids only, never their text or templates. */
const firewallActivationSchema = exactStruct({
  time: Schema.NonNegativeInt,
  activeRevision: Schema.NonEmptyString,
  addedRuleIds: Schema.Array(Schema.NonEmptyString),
  removedRuleIds: Schema.Array(Schema.NonEmptyString),
});

/**
 * Contains one row per peer without message content or provider traces.
 * `judgment` is what the check concluded. `decidedBy[].advice` contains the
 * rule's feedback for a refused or withheld message, or its guidance for an
 * allowed one. `from` identifies a contract stage as `contract`, or a fixed or
 * editable text rule. `checkedRuleIds` lists the rules the check judged: a
 * rule that does not apply to the message's direction is not listed.
 */
export const firewallDecisionSummarySchema = exactStruct({
  direction: Schema.Literal("incoming", "outgoing"),
  peer: AgentCard,
  judgment: Schema.Literal("allow", "reject", "hold"),
  reason: Schema.Union(Schema.Literal("eligible"), firewallRefusalReasonSchema),
  decidedBy: Schema.Array(
    exactStruct({
      ruleId: Schema.NonEmptyString,
      from: Schema.Literal("contract", "fixed", "editable"),
      advice: Schema.optional(Schema.NonEmptyString),
    }),
  ),
  checkedRuleIds: Schema.Array(Schema.NonEmptyString),
  activeRevision: Schema.NonEmptyString,
});

/**
 * Reports each saved check with its judgment and the decision the daemon
 * applied, and each policy activation. A sent message is `allowed` or
 * `refused`; a received one is `allowed` or `withheld`.
 */
export const firewallDecisionsSchema = exactStruct({
  activations: Schema.Array(firewallActivationSchema),
  decisions: Schema.Array(
    exactStruct({
      ...firewallDecisionSummarySchema.fields,
      decision: Schema.Literal("allowed", "refused", "withheld"),
      checkId: Schema.NonEmptyString,
      time: Schema.NonNegativeInt,
    }),
  ),
});

/**
 * Identifies invalid behavior documents or rule edits.
 * @inline
 */
export type PolicyProblem = typeof policyProblemSchema.Type;

const policyProblemSchema = Schema.Literal(
  "duplicate-id",
  "unknown-id",
  "editable-limit",
  "frontmatter",
  "schema",
  "too-large",
);

const policyProblemText: Readonly<Record<PolicyProblem, string>> = {
  "duplicate-id":
    "the policy already has a rule with this id or a norm with this name; remove it in the same update to replace it",
  "unknown-id":
    "no editable rule has this id and no held norm has this name; read_policy lists both",
  "editable-limit": `the update would leave more than ${String(MAXIMUM_EDITABLE_RULES)} editable rules or held norms; remove some first`,
  frontmatter:
    "the BEHAVIOR.md needs valid YAML frontmatter between --- lines (no code fence or other text before it)",
  schema:
    "it does not map to an editable rule: a BEHAVIOR.md needs name, description and metadata.social-harness.applies; allowed policy metadata is applies, guidance, feedback and action",
  "too-large": "the document is over 64 KiB",
};

const policyReasonText: Readonly<
  Record<FirewallPolicyError["reason"], string>
> = {
  "not-configured":
    "the daemon has no message policy, or its policy has no decisionProvider to judge text rules",
  "invalid-rule": "the update is invalid",
  "fixed-rule":
    "it is a fixed rule of the owner's policy, which update_policy cannot add, remove or replace",
  "build-failed": "the new rules could not be installed",
  "persistence-failed": "the rules could not be stored",
};

/**
 * Failed edits preserve active and saved rules. `ruleId` and `path` locate the
 * failure, such as `["add", 0]`. `problem` explains an invalid rule. Messages
 * include these details without rejected behavior text.
 */
export class FirewallPolicyError extends Schema.TaggedError<FirewallPolicyError>()(
  "FirewallPolicyError",
  {
    ruleId: Schema.optional(Schema.String),
    problem: Schema.optional(policyProblemSchema),
    path: Schema.optional(
      Schema.Tuple(Schema.Literal("add", "remove"), Schema.NonNegativeInt),
    ),
    reason: Schema.Literal(
      "not-configured",
      "invalid-rule",
      "fixed-rule",
      "build-failed",
      "persistence-failed",
    ),
  },
) {
  override get message(): string {
    const subject = editSubject(this.path, this.ruleId);
    const cause =
      this.problem === undefined
        ? policyReasonText[this.reason]
        : policyProblemText[this.problem];
    return subject === undefined ? cause : `${subject}: ${cause}`;
  }

  /** Returns available rule, path and problem details for error data. */
  get data(): Readonly<{
    ruleId?: string;
    path?: readonly [field: "add" | "remove", index: number];
    problem?: PolicyProblem;
  }> {
    return {
      ...(this.ruleId === undefined ? {} : { ruleId: this.ruleId }),
      ...(this.path === undefined ? {} : { path: this.path }),
      ...(this.problem === undefined ? {} : { problem: this.problem }),
    };
  }
}

/** Where the message-policy file and the decision provider's key file are. */
export interface PolicyFilePaths {
  readonly messagePolicyFile?: string;
  readonly decisionProviderKeyFile?: Redacted.Redacted;
}

/** The message-policy file, a behavior it lists or the decision provider's key file cannot be loaded. */
class MessagePolicyConfigurationError extends Data.TaggedError(
  "MessagePolicyConfigurationError",
) {}

const refused = () => new MessagePolicyConfigurationError();

const messagePolicyFormat = "org.social-harness/message-policy/v2";

/** Jev judges text rules with the key `decisionProviderKeyFile` holds. */
const decisionProvider = Schema.Struct({
  kind: Schema.Literal("jev"),
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

const defaultCheckLimits = {
  timeoutMilliseconds: 150_000,
  maximumInputBytes: 512_000,
  maximumResultBytes: 2_000_000,
};

const textDocument = Schema.Struct({
  fixedRules: textRules,
  initialRules: Schema.optionalWith(editableRulesSchema, {
    default: () => [],
  }),
  fixedBehaviors: Schema.optional(Schema.Array(Schema.String)),
  initialBehaviors: Schema.optional(Schema.Array(Schema.String)),
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
  settings: Schema.optionalWith(Schema.Struct(CheckLimits.fields), {
    default: () => defaultCheckLimits,
  }),
  facts: Schema.optionalWith(Schema.Array(OwnerFact), {
    default: () => [],
  }),
  maximumHistoryRecords: Schema.optional(
    Schema.Int.pipe(Schema.between(1, 1024)),
  ),
  maximumConcurrentChecks: Schema.optional(
    Schema.Int.pipe(Schema.between(1, 32)),
  ),
});

/**
 * Fills omitted settings with defaults and requires a provider for text rules.
 * Fixed and initial rule ids must be unique.
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
    message: () =>
      "text rule ids must be unique across fixedRules and initialRules",
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
      action: TextRule.fields.action,
    }),
  }),
});

type BehaviorRefusal = "frontmatter" | "schema";

const MAXIMUM_POLICY_BYTES = 1_048_576;

/** Trims surrounding whitespace from the single-token key. */
const decisionProviderKey = Schema.Trim.pipe(
  Schema.compose(
    Schema.String.pipe(
      Schema.minLength(1),
      Schema.maxLength(16_384),
      Schema.pattern(/^\S+$/u),
    ),
  ),
);

/**
 * A message policy as loaded from its file, with its behaviors resolved to
 * text rules and the provider's credential redacted. A policy that names no
 * `decisionProvider` has no provider.
 */
export interface LoadedMessagePolicy {
  readonly policy: MessagePolicy;
  readonly provider?: {
    readonly kind: "jev";
    readonly model: string;
    readonly apiKey: Redacted.Redacted;
  };
}

/**
 * Resolves the configured policy during setup, returning none when unset. A
 * policy that names a `decisionProvider` reads its key from
 * `decisionProviderKeyFile`, even without text rules of its own: the norms an
 * agent holds can add rules for it to judge. Invalid documents or keys fail
 * without exposing file contents.
 */
export const loadMessagePolicy = Effect.fn("loadMessagePolicy")(function* (
  paths: PolicyFilePaths,
) {
  const path = paths.messagePolicyFile;
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
  const apiKey = yield* readDecisionProviderKey(paths.decisionProviderKeyFile);
  return Option.some<LoadedMessagePolicy>({
    policy: resolved,
    provider: { kind: "jev", model: setting.model, apiKey },
  });
});

/** One load of a policy file's behaviors: its directory, the rule ids taken so far and the editable rules counted. */
interface BehaviorLoad {
  readonly root: string;
  readonly ids: Set<string>;
  readonly initialRuleCount: number;
}

/** The owner facts and the check concurrency limit are part of the installed policy identity. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- Installing a policy validates fixed settings with the same grammar saved checks decode.
export const firewallConfigurationSchema = Schema.Struct({
  facts: Schema.Array(OwnerFact),
  maximumConcurrentChecks: Schema.Int.pipe(Schema.between(1, 32)),
});

/** The fixed facts and the concurrency limit are immutable for a process installation. */
export type FirewallConfiguration = typeof firewallConfigurationSchema.Type;

/**
 * Maps a `BEHAVIOR.md` to an editable rule. Callers must bound `bytes` by
 * {@link MAXIMUM_DOCUMENT_BYTES}. Only a closed failure category reaches
 * `refused`. YAML diagnostics stay private.
 */
export const decodeBehavior = Effect.fn("decodeBehavior")(function* <Failure>(
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
  return yield* Schema.decodeUnknown(editableRuleSchema)({
    id: frontmatter.name,
    text: parts[2].trim(),
    ...metadata,
  }).pipe(Effect.mapError(() => refused("schema")));
});

function uniqueTextRuleIds(policy: typeof document.Type): boolean {
  const ids = [
    ...(policy.text?.fixedRules ?? []),
    ...(policy.text?.initialRules ?? []),
  ].map((rule) => rule.id);
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

/** Initial behaviors are validated even when persisted editable rules already exist. */
function resolveBehaviors(policy: MessagePolicy, root: string) {
  return Effect.gen(function* () {
    const text = policy.text;
    if (text === undefined) {
      return policy;
    }
    const load = {
      root,
      ids: new Set(
        [...text.fixedRules, ...text.initialRules].map((rule) => rule.id),
      ),
      initialRuleCount: text.initialRules.length,
    };
    const fixed = yield* loadBehaviors(
      text.fixedBehaviors ?? [],
      "fixedBehaviors",
      load,
    );
    const initial = yield* loadBehaviors(
      text.initialBehaviors ?? [],
      "initialBehaviors",
      load,
    );
    const resolved: MessagePolicy = {
      ...policy,
      text: {
        fixedRules: [
          ...text.fixedRules,
          ...fixed.map((rule) => new TextRule(rule)),
        ],
        initialRules: [...text.initialRules, ...initial],
      },
    };
    return resolved;
  });
}

function loadBehaviors(
  directories: readonly string[],
  field: "fixedBehaviors" | "initialBehaviors",
  load: BehaviorLoad,
) {
  let count = load.initialRuleCount;
  return Effect.forEach(
    directories,
    (directory) =>
      Effect.gen(function* () {
        const behavior = yield* readBehavior(load.root, directory);
        if (load.ids.has(behavior.id)) {
          return yield* refused();
        }
        if (field === "initialBehaviors") {
          count += 1;
          if (count > MAXIMUM_EDITABLE_RULES) {
            return yield* refused();
          }
        }
        load.ids.add(behavior.id);
        return behavior;
      }),
    { concurrency: 1 },
  );
}

/** Relative paths must stay in the canonical root, the policy directory, after symlink resolution. */
function contained(root: string, path: string) {
  const child = relative(root, path);
  return !isAbsolute(child) && child !== ".." && !child.startsWith(`..${sep}`);
}

/** The key `path` holds: bounded UTF-8 text of one token. */
function readDecisionProviderKey(path?: Redacted.Redacted) {
  if (path === undefined) {
    return Effect.fail(refused());
  }
  return readCanonicalFile(Redacted.value(path), 16_384).pipe(
    Effect.flatMap((bytes) =>
      Effect.try({
        try: () => new TextDecoder("utf-8", { fatal: true }).decode(bytes),
        catch: refused,
      }),
    ),
    Effect.flatMap((text) =>
      Schema.decodeUnknown(decisionProviderKey)(text).pipe(
        Effect.mapError(refused),
      ),
    ),
    Effect.map(Redacted.make),
  );
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

function editSubject(
  path?: FirewallPolicyError["path"],
  ruleId?: string,
): string | undefined {
  const place = path === undefined ? undefined : `${path[0]}[${path[1]}]`;
  const rule = ruleId === undefined ? undefined : `rule ${ruleId}`;
  return place !== undefined && rule !== undefined
    ? `${place} (${rule})`
    : (place ?? rule);
}
