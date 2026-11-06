/** @file Explicit firewall rules, edit contracts and bounded configuration loading. */

import {
  Data,
  Effect,
  Option,
  ParseResult,
  Redacted,
  Schema,
  Struct,
} from "effect";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- Nonblocking descriptor opens reject special files before a writer can block startup.
import { constants } from "node:fs";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- Configuration loads before daemon platform services are composed.
import { type FileHandle, lstat, open, realpath } from "node:fs/promises";
import { dirname, isAbsolute, relative, resolve, sep, win32 } from "node:path";
import { parseDocument } from "yaml";
import { exactStruct } from "../encoding/canonical-json.js";
import { AgentCard } from "../identity/agent-cards.js";

const identifier = Schema.NonEmptyTrimmedString;

/**
 * Operational holds remain distinct from a settled rule violation. A draft
 * that a rule holds for the owner, who has not approved it, has its own reason.
 */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- Assessment, saved summaries and host projection share one refusal grammar; package entry points expose opaque values.
export const firewallRefusalReasonSchema = Schema.Literal(
  "rule-violation",
  "unresolved-evidence",
  "check-unavailable",
  "owner-approval-required",
);

/** Trusted callers supply facts. Visibility limits what guidance may reference. */
export class OwnerFact extends Schema.Class<OwnerFact>("OwnerFact")({
  id: identifier,
  source: identifier,
  text: Schema.NonEmptyString,
  visibility: Schema.Literal("runtime", "assessor"),
}) {}

/**
 * Registers natural-language policy, admission guidance and refusal feedback.
 * Validation requires nonempty text without restricting its language. An
 * incoming message that violates a rule whose `action` is `reply` is withheld,
 * and its sender receives the rule's feedback text as a message; `deny`, the
 * default, withholds it silently.
 * The [editable policy contract](https://docs.moltzap.xyz/reference/spec/firewall/message-policy#editable-text-policy)
 * defines ownership and edit validation.
 */
export class TextRule extends Schema.Class<TextRule>("TextRule")({
  id: identifier,
  guidance: Schema.optional(Schema.NonEmptyString),
  feedback: Schema.optional(exactStruct({ text: Schema.NonEmptyString })),
  action: Schema.optional(Schema.Literal("deny", "reply")),
  text: Schema.NonEmptyString,
  applies: Schema.Literal("incoming", "outgoing", "both"),
  /**
   * A matcher the daemon registers, such as the one for a norm rule's acts.
   * The provider is asked about the rule only for messages it matches.
   */
  match: Schema.optional(Schema.NonEmptyTrimmedString),
}) {}

/** Bounds apply to one complete text assessment. */
export class FirewallSettings extends Schema.Class<FirewallSettings>(
  "FirewallSettings",
)({
  timeoutMilliseconds: Schema.Positive.pipe(Schema.int()),
  maximumInputBytes: Schema.Positive.pipe(Schema.int()),
  maximumResultBytes: Schema.Positive.pipe(Schema.int()),
}) {}

/** Natural-language inference has a probability floor and bounded request and stage duration. */
export class TextPolicySettings extends Schema.Class<TextPolicySettings>(
  "TextPolicySettings",
)({
  minimumProbability: Schema.Number.pipe(Schema.between(0.5, 1)),
  maximumRequestBytes: Schema.Positive.pipe(Schema.int()),
  timeoutMilliseconds: Schema.Positive.pipe(Schema.int()),
}) {}

/**
 * An editable rule names no matcher: matchers come with the daemon's
 * configuration, so only a fixed rule can be scoped to one.
 */
const editableRuleSchema = exactStruct(Struct.omit(TextRule.fields, "match"));

/** Limits editable rules across startup, saved policy and updates. */
export const MAXIMUM_EDITABLE_RULES = 256;

/** The complete editable document. */
export const editableDocumentSchema = Schema.Array(editableRuleSchema).pipe(
  Schema.maxItems(MAXIMUM_EDITABLE_RULES),
);

const behaviorAddition = exactStruct({ behavior: Schema.String });

/** A norm document for the agent to hold, which the daemon's norms read. */
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
 * The digest identifies the entire active installation, including fixed
 * policy and held norms. `fixedRules` include the English rules of the norms
 * the agent holds, and `norms` names those norms.
 */
export const policyReadSchema = exactStruct({
  rules: editableDocumentSchema,
  fixedRules: Schema.Array(TextRule),
  norms: Schema.Array(Schema.String),
  activeRevision: Schema.NonEmptyString,
});

/** Edits affect future checks. Saved decisions stay unchanged. */
export const policyUpdateResultSchema = exactStruct({
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
 * `decidedBy[].advice` contains registered feedback or admission guidance.
 * `from` identifies a chain rule, or a fixed or editable text rule.
 * `checkedRuleIds` lists the rules the check judged: a rule that does not
 * apply to the message, by its direction or its matcher, is not listed.
 */
export const firewallDecisionSummarySchema = exactStruct({
  direction: Schema.Literal("incoming", "outgoing"),
  peer: AgentCard,
  outcome: Schema.Literal("allow", "reject", "hold"),
  reason: Schema.Union(Schema.Literal("eligible"), firewallRefusalReasonSchema),
  decidedBy: Schema.Array(
    exactStruct({
      ruleId: Schema.NonEmptyString,
      from: Schema.Literal("chain", "fixed", "editable"),
      advice: Schema.optional(Schema.NonEmptyString),
    }),
  ),
  checkedRuleIds: Schema.Array(Schema.NonEmptyString),
  activeRevision: Schema.NonEmptyString,
});

/** Reports saved judgments, admission or withholding, and policy activations. */
export const firewallDecisionsSchema = exactStruct({
  activations: Schema.Array(firewallActivationSchema),
  decisions: Schema.Array(
    exactStruct({
      ...firewallDecisionSummarySchema.fields,
      disposition: Schema.Literal("admitted", "withheld"),
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
  "invalid-norm",
);

const policyProblemText: Readonly<Record<PolicyProblem, string>> = {
  "duplicate-id":
    "the policy already has a rule with this id or a norm with this name; remove it in the same update to replace it",
  "unknown-id":
    "no editable rule has this id and no held norm has this name; policy_read lists both",
  "editable-limit": `the update would leave more than ${String(MAXIMUM_EDITABLE_RULES)} editable rules or held norms; remove some first`,
  frontmatter:
    "the BEHAVIOR.md needs valid YAML frontmatter between --- lines (no code fence or other text before it)",
  schema:
    "it does not map to an editable rule: a BEHAVIOR.md needs name, description and metadata.social-harness.applies; allowed policy metadata is applies, guidance, feedback and action",
  "too-large": "the document is over 64 KiB",
  "invalid-norm":
    "it is not a norm document this daemon can hold: the daemon reads no norms, the document does not parse, or it names a built-in norm",
};

const policyReasonText: Readonly<
  Record<FirewallPolicyError["reason"], string>
> = {
  "not-configured":
    "the daemon has no message policy, or its policy has no decisionProvider to judge text rules",
  "invalid-rule": "the update is invalid",
  "fixed-rule":
    "it is a fixed rule of the owner's policy, which policy_update cannot add, remove or replace",
  "approval-refused": "the host did not approve this policy update",
  "build-failed": "the new rules could not be installed",
  "persistence-failed": "the rules could not be stored",
};

const editRefusalDocs =
  "Docs: https://docs.moltzap.xyz/reference/spec/firewall/message-policy#editable-text-policy";

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
      "approval-refused",
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
    const text = subject === undefined ? cause : `${subject}: ${cause}`;
    return ["invalid-rule", "fixed-rule", "build-failed"].includes(this.reason)
      ? `${text}. ${editRefusalDocs}`
      : text;
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

/** Closed reasons the policy settings cannot start the daemon. */
export type MessagePolicyConfigurationFailure =
  | "message-policy-file"
  | "message-policy"
  | "decision-provider-key-file"
  | "decision-provider-key";

/** Behavior diagnostics reveal only a closed category and the policy entry. */
export type BehaviorFailure =
  | "missing"
  | "too-large"
  | "not-a-file"
  | "frontmatter"
  | "schema"
  | "duplicate-id"
  | "editable-limit";

/** Policy and provider file locations. */
export interface PolicyConfiguration {
  readonly messagePolicyFile?: string;
  readonly decisionProviderKeyFile?: Redacted.Redacted;
}

/** Policy loading fails with a closed reason and a safe operator message. */
export class MessagePolicyConfigurationError extends Data.TaggedError(
  "MessagePolicyConfigurationError",
)<{
  readonly reason: MessagePolicyConfigurationFailure;
  readonly message: string;
  readonly behaviorReason?: BehaviorFailure;
}> {}

/** The daemon supplies secret-file access without granting policy loading bootstrap authority. */
export type CredentialReader = (
  path: Redacted.Redacted,
  grammar: Schema.Schema<string>,
) => Effect.Effect<Redacted.Redacted, { readonly stage: "file" | "value" }>;

const POLICY_SPEC =
  "https://docs.moltzap.xyz/reference/spec/firewall/message-policy";

const FIREWALL_GUIDE = "https://docs.moltzap.xyz/guides/message-firewall";

const failureMessages: Readonly<
  Record<
    MessagePolicyConfigurationFailure,
    { readonly text: string; readonly docs: string }
  >
> = {
  "message-policy-file": {
    text: "messagePolicyFile must name a readable regular file of at most 1 MiB",
    docs: `${FIREWALL_GUIDE}#message-policy-file`,
  },
  "message-policy": {
    text: "messagePolicyFile must contain a valid org.social-harness/message-policy/v2 document",
    docs: `${FIREWALL_GUIDE}#message-policy-document`,
  },
  "decision-provider-key-file": {
    text: "decisionProviderKeyFile must name a readable file for the message policy's jev decision provider",
    docs: `${FIREWALL_GUIDE}#decision-provider-key-file`,
  },
  "decision-provider-key": {
    text: "decisionProviderKeyFile must contain one key without inner whitespace",
    docs: `${FIREWALL_GUIDE}#decision-provider-key`,
  },
};

const messagePolicyFormat = "org.social-harness/message-policy/v2";

/** Jev decides natural-language rules with the key `decisionProviderKeyFile` holds. */
const decisionProvider = Schema.Struct({
  kind: Schema.Literal("jev"),
  model: Schema.NonEmptyTrimmedString,
});

const textRules = Schema.optionalWith(Schema.Array(TextRule), {
  default: () => [],
});
const defaultTextSettings = {
  minimumProbability: 0.8,
  maximumRequestBytes: 384_000,
  timeoutMilliseconds: 15_000,
};

const defaultSettings = {
  timeoutMilliseconds: 150_000,
  maximumInputBytes: 512_000,
  maximumResultBytes: 2_000_000,
};

const textDocument = Schema.Struct({
  fixedRules: textRules,
  initialRules: Schema.optionalWith(Schema.Array(editableRuleSchema), {
    default: () => [],
  }),
  fixedBehaviors: Schema.optional(Schema.Array(Schema.String)),
  initialBehaviors: Schema.optional(Schema.Array(Schema.String)),
});

const document = Schema.Struct({
  format: Schema.Literal(messagePolicyFormat),
  text: Schema.optional(textDocument),
  decisionProvider: Schema.optional(decisionProvider),
  textSettings: Schema.optionalWith(Schema.Struct(TextPolicySettings.fields), {
    default: () => defaultTextSettings,
  }),
  settings: Schema.optionalWith(Schema.Struct(FirewallSettings.fields), {
    default: () => defaultSettings,
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

/* eslint-disable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- The document Schema shares its domain name with the value it decodes. */

/**
 * Fills omitted settings with defaults and requires a provider for text rules.
 * Fixed and initial rule ids must be unique.
 */
const MessagePolicy = document.pipe(
  Schema.filter(
    (policy) =>
      policy.decisionProvider !== undefined || policy.text === undefined,
    {
      message: () => "decisionProvider is required for natural-language rules",
    },
  ),
  Schema.filter((policy) => uniqueTextRuleIds(policy), {
    message: () =>
      "text rule ids must be unique across fixedRules and initialRules",
  }),
);

type MessagePolicy = typeof MessagePolicy.Type;

/* eslint-enable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Restore naming after the Schema/type pair. */

/** Rejects unknown properties so misspelled settings cannot silently weaken policy. */
export const decodeMessagePolicy = Schema.decodeUnknown(
  Schema.parseJson(MessagePolicy),
  { onExcessProperty: "error" },
);

/** Limits each `BEHAVIOR.md`, and each norm document an edit adds, to 64 KiB. */
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

/** Loading validates provider parameters. Construction binds execution. */
interface LoadedDecisionProvider {
  readonly kind: "jev";
  readonly model: string;
  readonly apiKey: Redacted.Redacted;
}

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
 * Runtime configuration retains resolved rules and redacts the provider
 * credential. A policy that names no `decisionProvider` has no provider.
 */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- The owning policy codec is shared with resolved daemon configuration.
export const installedMessagePolicy = Schema.Struct({
  policy: MessagePolicy,
  provider: Schema.optional(
    Schema.Struct({
      kind: Schema.Literal("jev"),
      model: Schema.String,
      apiKey: Schema.Redacted(decisionProviderKey),
    }),
  ),
});

/** Contains validated policy and the provider parameters it selects. */
export type LoadedMessagePolicy = typeof installedMessagePolicy.Type;

/**
 * Setup reads bounded UTF-8 credential files before validating the provider's
 * token grammar. Like the policy file, the key file is read at its resolved
 * path, so a symbolic link to it works, as a Kubernetes Secret volume
 * presents every key.
 */
export function readPolicyCredential(
  path: Redacted.Redacted,
  grammar: Schema.Schema<string>,
) {
  return Effect.tryPromise({
    try: () => realpath(Redacted.value(path)),
    catch: () => ({ stage: "file" as const }),
  }).pipe(
    Effect.flatMap((canonical) =>
      readBoundedFile(canonical, 16_384, () =>
        policyConfigurationError("decision-provider-key-file"),
      ).pipe(Effect.mapError(() => ({ stage: "file" as const }))),
    ),
    Effect.flatMap((bytes) =>
      Effect.try({
        try: () => new TextDecoder("utf-8", { fatal: true }).decode(bytes),
        catch: () => ({ stage: "file" as const }),
      }),
    ),
    Effect.flatMap((value) =>
      Schema.decodeUnknown(grammar)(value).pipe(
        Effect.mapError(() => ({ stage: "value" as const })),
      ),
    ),
    Effect.map(Redacted.make),
  );
}

/**
 * Resolves the configured policy during setup, returning none when unset. Only Jev
 * reads `decisionProviderKeyFile` through the supplied credential
 * reader. Invalid documents or keys fail without exposing file contents.
 */
export const loadMessagePolicy = (
  configuration: PolicyConfiguration,
  readCredential: CredentialReader,
): Effect.Effect<
  Option.Option<LoadedMessagePolicy>,
  MessagePolicyConfigurationError
> =>
  Effect.gen(function* () {
    const path = configuration.messagePolicyFile;
    if (path === undefined) {
      return Option.none();
    }
    const policy = yield* readPolicyText(path).pipe(
      Effect.flatMap((text) =>
        decodeMessagePolicy(text).pipe(Effect.mapError(invalidPolicy)),
      ),
    );
    const resolved = yield* resolveBehaviors(policy, dirname(path));
    const provider = yield* loadDecisionProvider(
      resolved,
      configuration,
      readCredential,
    );
    return Option.some({
      policy: resolved,
      ...(provider === undefined ? {} : { provider }),
    });
  }).pipe(Effect.withSpan("loadMessagePolicy"));

interface BehaviorState {
  readonly root: string;
  readonly ids: Set<string>;
  readonly initialRuleCount: number;
}

/** Operational evidence limits are part of the installed policy identity. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- Installation validates fixed settings with the same grammar retained audits decode.
export const firewallConfigurationSchema = Schema.Struct({
  facts: Schema.Array(OwnerFact),
  maximumHistoryRecords: Schema.Int.pipe(Schema.between(1, 1024)),
  maximumConcurrentChecks: Schema.Int.pipe(Schema.between(1, 32)),
});

/** Evidence limits and fixed facts are immutable for a process installation. */
export type FirewallConfiguration = typeof firewallConfigurationSchema.Type;

/**
 * Maps a `BEHAVIOR.md` to an editable rule. Callers must bound `bytes` by
 * {@link MAXIMUM_DOCUMENT_BYTES}. Only a closed failure category reaches
 * `refused`. YAML diagnostics stay private.
 */
export function decodeBehavior<Failure>(
  bytes: Buffer,
  refused: (reason: BehaviorRefusal) => Failure,
) {
  return Effect.gen(function* () {
    const parts = /^---\r?\n([\s\S]*?)^---[ \t]*(?:\r?\n|$)([\s\S]*)/mu.exec(
      bytes.toString("utf8").replace(/^\uFEFF/u, ""),
    );
    if (
      parts?.[1] === undefined ||
      parts[2] === undefined ||
      parts.index !== 0
    ) {
      return yield* Effect.fail(refused("frontmatter"));
    }
    const frontmatter = yield* decodeFrontmatter(parts[1], refused);
    const metadata = frontmatter.metadata["social-harness"];
    return yield* Schema.decodeUnknown(editableRuleSchema)({
      id: frontmatter.name,
      text: parts[2].trim(),
      ...metadata,
    }).pipe(Effect.mapError(() => refused("schema")));
  }).pipe(Effect.withSpan("decodeBehavior"));
}

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

/** Reports only the policy entry because YAML diagnostics may expose private content. */
function invalidBehavior(path: string, reason: BehaviorFailure) {
  return policyConfigurationError("message-policy", path, reason);
}

function readBehavior(root: string, directory: string, path: string) {
  const refused = (reason: BehaviorFailure) => invalidBehavior(path, reason);
  if (
    isAbsolute(directory) ||
    win32.isAbsolute(directory) ||
    directory.split(/[\\/]/u).includes("..")
  ) {
    return Effect.fail(refused("schema"));
  }
  return Effect.gen(function* () {
    const canonical = (location: string) =>
      Effect.tryPromise({
        try: () => realpath(location),
        catch: () => refused("missing"),
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
      return yield* refused("schema");
    }
    const bytes = yield* readBoundedFile(
      locations.file,
      MAXIMUM_DOCUMENT_BYTES,
      refused,
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
    if (text.initialRules.length > MAXIMUM_EDITABLE_RULES) {
      return yield* policyConfigurationError(
        "message-policy",
        "text.initialRules",
      );
    }
    const state = {
      root,
      ids: new Set(
        [...text.fixedRules, ...text.initialRules].map((rule) => rule.id),
      ),
      initialRuleCount: text.initialRules.length,
    };
    const fixed = yield* loadBehaviors(
      text.fixedBehaviors ?? [],
      "fixedBehaviors",
      state,
    );
    const initial = yield* loadBehaviors(
      text.initialBehaviors ?? [],
      "initialBehaviors",
      state,
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
  state: BehaviorState,
) {
  let count = state.initialRuleCount;
  return Effect.forEach(
    directories,
    (directory, index) =>
      Effect.gen(function* () {
        const path = `text.${field}.${index}`;
        const behavior = yield* readBehavior(state.root, directory, path);
        if (state.ids.has(behavior.id)) {
          return yield* invalidBehavior(path, "duplicate-id");
        }
        if (field === "initialBehaviors") {
          count += 1;
          if (count > MAXIMUM_EDITABLE_RULES) {
            return yield* invalidBehavior(path, "editable-limit");
          }
        }
        state.ids.add(behavior.id);
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

function readPolicyText(path: string) {
  return Effect.tryPromise({
    try: () => realpath(path),
    catch: () => policyConfigurationError("message-policy-file"),
  }).pipe(
    Effect.flatMap((canonical) =>
      readBoundedFile(canonical, MAXIMUM_POLICY_BYTES, () =>
        policyConfigurationError("message-policy-file"),
      ),
    ),
    Effect.map((bytes) => bytes.toString("utf8")),
  );
}

/** The descriptor owns both the file-kind check and the bounded read. */
function readBoundedFile(
  path: string,
  maximumBytes: number,
  refused: (reason: BehaviorFailure) => MessagePolicyConfigurationError,
) {
  return Effect.gen(function* () {
    const before = yield* Effect.tryPromise({
      try: () => lstat(path),
      catch: () => refused("missing"),
    });
    const file = yield* Effect.acquireRelease(
      Effect.tryPromise({
        try: () =>
          open(
            path,
            constants.O_RDONLY | constants.O_NONBLOCK | constants.O_NOFOLLOW,
          ),
        catch: () => refused("missing"),
      }),
      (file) =>
        Effect.tryPromise({
          try: () => file.close(),
          catch: () => refused("missing"),
        }).pipe(Effect.orDie),
    );
    const stat = yield* Effect.tryPromise({
      try: () => file.stat(),
      catch: () => refused("missing"),
    });
    if (!stat.isFile()) {
      return yield* refused("not-a-file");
    }
    if (stat.dev !== before.dev || stat.ino !== before.ino) {
      return yield* refused("missing");
    }
    const bytes = yield* readDescriptor(file, maximumBytes).pipe(
      Effect.mapError(() => refused("missing")),
    );
    if (bytes.byteLength > maximumBytes) {
      return yield* refused("too-large");
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

/** Names where the document is invalid, never what it holds there. */
function invalidPolicy(
  error: ParseResult.ParseError,
): MessagePolicyConfigurationError {
  const paths = [
    ...new Set(
      ParseResult.ArrayFormatter.formatErrorSync(error).map((issue) =>
        issue.path.length === 0 ? "(document)" : issue.path.join("."),
      ),
    ),
  ];
  return new MessagePolicyConfigurationError({
    reason: "message-policy",
    message: policyFailureMessage(
      "message-policy",
      `; invalid at ${paths.join(", ")}`,
    ),
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

/**
 * A policy that names a `decisionProvider` reads its key, even without text
 * rules of its own: the norms an agent holds can add rules for it to judge.
 */
function loadDecisionProvider(
  policy: MessagePolicy,
  configuration: PolicyConfiguration,
  readCredential: CredentialReader,
): Effect.Effect<
  LoadedDecisionProvider | undefined,
  MessagePolicyConfigurationError
> {
  const setting = policy.decisionProvider;
  if (setting === undefined) {
    return Effect.succeed(undefined);
  }
  return jevKey(configuration, readCredential).pipe(
    Effect.map((apiKey) => ({
      kind: "jev" as const,
      model: setting.model,
      apiKey,
    })),
  );
}

function jevKey(
  configuration: PolicyConfiguration,
  readCredential: CredentialReader,
): Effect.Effect<Redacted.Redacted, MessagePolicyConfigurationError> {
  const path = configuration.decisionProviderKeyFile;
  return path === undefined
    ? Effect.fail(policyConfigurationError("decision-provider-key-file"))
    : readCredential(path, decisionProviderKey).pipe(
        Effect.mapError((error) =>
          policyConfigurationError(
            error.stage === "file"
              ? "decision-provider-key-file"
              : "decision-provider-key",
          ),
        ),
      );
}

function policyConfigurationError(
  reason: MessagePolicyConfigurationFailure,
  path = "",
  behaviorReason?: BehaviorFailure,
): MessagePolicyConfigurationError {
  const category = behaviorReason === undefined ? "" : ` (${behaviorReason})`;
  return new MessagePolicyConfigurationError({
    reason,
    behaviorReason,
    message: policyFailureMessage(
      reason,
      path === "" ? "" : `; invalid at ${path}${category}`,
      { path },
    ),
  });
}

/**
 * Links behavior failures to their rules and other failures to troubleshooting.
 * `link.docs` overrides that target. An empty value omits the link.
 */
function policyFailureMessage(
  reason: MessagePolicyConfigurationFailure,
  detail: string,
  link: { readonly path?: string; readonly docs?: string } = {},
): string {
  const docs = failureDocs(reason, link);
  return docs === ""
    ? `${failureMessages[reason].text}${detail}`
    : `${failureMessages[reason].text}${detail}. Docs: ${docs}`;
}

function failureDocs(
  reason: MessagePolicyConfigurationFailure,
  link: { readonly path?: string; readonly docs?: string },
): string {
  const path = link.path ?? "";
  if (/^text\.(?:fixed|initial)Behaviors\./u.test(path)) {
    return `${POLICY_SPEC}#behaviors`;
  }
  return link.docs ?? failureMessages[reason].docs;
}
