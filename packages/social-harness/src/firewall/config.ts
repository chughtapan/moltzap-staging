/** @file Explicit firewall rules, edit contracts and bounded configuration loading. */

import {
  Data,
  Effect,
  Option,
  ParseResult,
  type Redacted,
  Schema,
} from "effect";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- Nonblocking descriptor opens reject special files before a writer can block startup.
import { constants } from "node:fs";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- Configuration loads before daemon platform services are composed.
import { type FileHandle, lstat, open, realpath } from "node:fs/promises";
import { dirname, isAbsolute, relative, resolve, sep, win32 } from "node:path";
import { parseDocument } from "yaml";
import { exactStruct } from "../encoding/exact.js";
import { AgentView } from "../identity/cards.js";

const identifier = Schema.NonEmptyTrimmedString;

/** Registered feedback names one repair action independently of a judgment. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- Checking and host projection share this configuration-owned action grammar; public entries do not expose its constructor.
export const nextActionSchema = Schema.Literal(
  "revise",
  "wait",
  "stop",
  "seek-approval",
);

/** Facts are supplied by trusted composition; visibility limits references shown to the agent. */
export class OwnerFact extends Schema.Class<OwnerFact>("OwnerFact")({
  id: identifier,
  source: identifier,
  text: Schema.NonEmptyString,
  visibility: Schema.Literal("runtime", "assessor"),
}) {}

/**
 * The decision provider interprets the rule as natural-language text.
 * Validation requires nonempty text and imposes no language restriction.
 * Guidance is registered advice for the agent; feedback supplies refusal and
 * repair text. Direction is explicit for every natural-language rule.
 * See the [editable policy contract](https://docs.moltzap.xyz/reference/spec/firewall/message-policy#editable-text-policy)
 * for installation identifiers, ownership and edit validation.
 */
export class TextRule extends Schema.Class<TextRule>("TextRule")({
  id: identifier,
  guidance: Schema.optional(Schema.NonEmptyString),
  notifySender: Schema.optional(Schema.Boolean),
  feedback: Schema.optional(
    Schema.Struct({
      text: Schema.NonEmptyString,
      nextAction: nextActionSchema,
    }),
  ),
  text: Schema.NonEmptyString,
  applies: Schema.Literal("incoming", "outgoing", "both"),
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

const editableRuleSchema = exactStruct(TextRule.fields);

/** The most editable natural-language rules one daemon holds, across seeding, persistence and updates. */
export const MAXIMUM_EDITABLE_RULES = 256;

/** The complete editable document. */
export const editableDocumentSchema = Schema.Array(editableRuleSchema).pipe(
  Schema.maxItems(MAXIMUM_EDITABLE_RULES),
);

/**
 * A shared norm's `BEHAVIOR.md` exactly as proposed, mapped to its rule as a
 * policy file's behaviors are: docs/reference/spec/firewall/message-policy.md#behaviors.
 */
const behaviorAddition = exactStruct({ behavior: Schema.String });

/** Removing and adding the same editable id replaces it in one activation. */
export const policyUpdateSchema = exactStruct({
  add: Schema.optional(
    Schema.Array(Schema.Union(editableRuleSchema, behaviorAddition)).pipe(
      Schema.maxItems(MAXIMUM_EDITABLE_RULES),
    ),
  ),
  remove: Schema.optional(
    Schema.Array(TextRule.fields.id).pipe(
      Schema.maxItems(MAXIMUM_EDITABLE_RULES),
    ),
  ),
});

/** The digest identifies the entire active installation, including fixed policy. */
export const policyReadSchema = exactStruct({
  rules: editableDocumentSchema,
  fixedRules: Schema.Array(TextRule),
  activeRevision: Schema.NonEmptyString,
});

/** Activation acknowledges future checks only; retained dispositions are unchanged. */
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
 * How a decision's outcome came about. `judgment` is a settled natural-language
 * judgment and `uncertain` an unsettled one admitted with registered advice.
 * `unavailable` identifies failed, timed-out or unrunnable assessment.
 * `endpoint` identifies a hold without a deciding rule, and `default` an
 * admission without a qualifying finding.
 */
const decisionCause = Schema.Literal(
  "judgment",
  "uncertain",
  "unavailable",
  "endpoint",
  "default",
);

/**
 * One row per peer contains no message content or provider traces. The only
 * text is `decidedBy[].advice`, the deciding rule's own registered feedback,
 * or its guidance for an admission. `decidedBy[].from` says where the rule is
 * defined: a fixed or owner-editable natural-language rule. `checkedRuleIds` lists every rule with a
 * finding in the completed assessment.
 */
export const firewallDecisionSummarySchema = exactStruct({
  direction: Schema.Literal("incoming", "outgoing"),
  peer: Schema.NullOr(AgentView),
  outcome: Schema.Literal("allow", "reject", "hold"),
  cause: decisionCause,
  decidedBy: Schema.Array(
    exactStruct({
      ruleId: Schema.NonEmptyString,
      from: Schema.Literal("fixed", "editable"),
      advice: Schema.optional(Schema.NonEmptyString),
    }),
  ),
  checkedRuleIds: Schema.Array(Schema.NonEmptyString),
  activeRevision: Schema.NonEmptyString,
});

/** The saved publication outcome accompanies the assessment's captured summary. */
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
 * What made an edit's rule invalid. A `BEHAVIOR.md` addition fails as
 * `frontmatter`, `schema` or `too-large`; an edit as
 * `duplicate-id`, `unknown-id` or `editable-limit`.
 * @inline
 */
export type PolicyProblem =
  | "duplicate-id"
  | "unknown-id"
  | "editable-limit"
  | "frontmatter"
  | "schema"
  | "too-large";

const policyProblemText: Readonly<Record<PolicyProblem, string>> = {
  "duplicate-id":
    "its id is already in the policy; remove that rule in the same update to replace it, or use another id",
  "unknown-id":
    "no editable rule has this id; policy_read lists the editable rules",
  "editable-limit": `the update would leave more than ${String(MAXIMUM_EDITABLE_RULES)} editable rules; remove some rules first`,
  frontmatter:
    "the BEHAVIOR.md needs valid YAML frontmatter between --- lines (no code fence or other text before it)",
  schema:
    "it does not map to an editable rule: a BEHAVIOR.md needs name, description and metadata.social-harness.applies; allowed policy metadata is applies, guidance, feedback and notifySender",
  "too-large": "the BEHAVIOR.md is over 64 KiB",
};

const policyReasonText: Readonly<
  Record<FirewallPolicyError["reason"], string>
> = {
  "not-configured": "the message policy has no text block",
  "invalid-rule": "the update is invalid",
  "fixed-rule":
    "it is a fixed rule of the owner's policy, which policy_update cannot add, remove or replace",
  "build-failed": "the new rules could not be installed",
  "persistence-failed": "the rules could not be stored",
};

const editRefusalDocs =
  "Docs: https://docs.moltzap.xyz/reference/spec/firewall/message-policy#editable-text-policy";

/**
 * A failed edit never replaces the active installation or retained editable
 * document. `ruleId` and `path` name the failing rule when the edit had one:
 * `path` is its place in the update, such as `["add", 0]`. `problem` says why
 * an `invalid-rule` edit failed. The message names all three, and never the
 * text of a rejected `BEHAVIOR.md`.
 */
export class FirewallPolicyError extends Data.TaggedError(
  "FirewallPolicyError",
)<{
  readonly ruleId?: string;
  readonly problem?: PolicyProblem;
  readonly path?: readonly [field: "add" | "remove", index: number];
  readonly reason:
    | "not-configured"
    | "invalid-rule"
    | "fixed-rule"
    | "build-failed"
    | "persistence-failed";
}> {
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

  /**
   * The fields a refusal's error data carries beside its reason.
   * @returns `ruleId`, `path` and `problem`, each when the refusal has it.
   */
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
  | "message-policy-composed"
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

/** Only policy and provider file locations enter the loader. */
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

/** The message-firewall guide, whose troubleshooting entries policy failures link. */
const FIREWALL_GUIDE = "https://docs.moltzap.xyz/guides/message-firewall";

/** Each failure's operator text and the troubleshooting entry that matches it. */
const failureMessages: Readonly<
  Record<
    MessagePolicyConfigurationFailure,
    { readonly text: string; readonly docs: string }
  >
> = {
  "message-policy-file": {
    text: "SHARNESS_MESSAGE_POLICY_FILE must name a readable regular file of at most 1 MiB",
    docs: `${FIREWALL_GUIDE}#message-policy-file`,
  },
  "message-policy": {
    text: "SHARNESS_MESSAGE_POLICY_FILE must contain a valid org.social-harness/message-policy/v2 document",
    docs: `${FIREWALL_GUIDE}#message-policy-document`,
  },
  "message-policy-composed": {
    text: "SHARNESS_MESSAGE_POLICY_FILE cannot be set for a daemon whose composition installs its own firewall",
    docs: `${FIREWALL_GUIDE}#message-policy-composed`,
  },
  "decision-provider-key-file": {
    text: "SHARNESS_DECISION_PROVIDER_KEY_FILE must name a readable file for the message policy's jev decision provider",
    docs: `${FIREWALL_GUIDE}#decision-provider-key-file`,
  },
  "decision-provider-key": {
    text: "SHARNESS_DECISION_PROVIDER_KEY_FILE must contain one key without inner whitespace",
    docs: `${FIREWALL_GUIDE}#decision-provider-key`,
  },
};

/** The `format` every message-policy document names. */
const messagePolicyFormat = "org.social-harness/message-policy/v2";

/**
 * The service that assesses natural-language rules. `jev` needs the key file the
 * daemon names in `SHARNESS_DECISION_PROVIDER_KEY_FILE`; `unavailable`
 * installs the rules with no service, so every natural-language check is unavailable:
 * enforcement holds the message and guidance admits it.
 */
const decisionProvider = Schema.Union(
  Schema.Struct({
    kind: Schema.Literal("jev"),
    model: Schema.NonEmptyTrimmedString,
  }),
  Schema.Struct({ kind: Schema.Literal("unavailable") }),
);

const textRules = Schema.optionalWith(Schema.Array(TextRule), {
  default: () => [],
});
/** Natural-language assessment defaults: the lowest routing probability Client accepts, and request bounds for a full history window. */
const defaultTextSettings = {
  minimumProbability: 0.5,
  maximumRequestBytes: 384_000,
  timeoutMilliseconds: 15_000,
};

/** Per-check bounds sized for natural-language assessment of a full history window. */
const defaultSettings = {
  timeoutMilliseconds: 150_000,
  maximumInputBytes: 512_000,
  maximumResultBytes: 2_000_000,
};

const textDocument = Schema.Struct({
  fixedRules: textRules,
  initialRules: textRules,
  fixedBehaviors: Schema.optional(Schema.Array(Schema.String)),
  initialBehaviors: Schema.optional(Schema.Array(Schema.String)),
});

const document = Schema.Struct({
  format: Schema.Literal(messagePolicyFormat),
  mode: Schema.Literal("guidance", "enforcement"),
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
 * Explicit natural-language rules, provider options and check bounds for one daemon.
 * Missing settings take Client defaults. Text policy requires a provider and
 * unique rule ids across fixed and initial rules.
 */
export const MessagePolicy = document.pipe(
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

/** One decoded message-policy document, with Client's defaults filled in. */
export type MessagePolicy = typeof MessagePolicy.Type;

/* eslint-enable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Restore naming after the Schema/type pair. */

/**
 * Decodes the text of a message-policy document, refusing any property it
 * does not name, so a misspelled or newer setting never silently weakens the
 * policy.
 */
export const decodeMessagePolicy = Schema.decodeUnknown(
  Schema.parseJson(MessagePolicy),
  { onExcessProperty: "error" },
);

/** The Behaviors spec limits each BEHAVIOR.md to 64 KiB: docs/reference/spec/firewall/message-policy.md#behaviors. */
export const MAXIMUM_BEHAVIOR_BYTES = 65_536;

/** YAML owns frontmatter syntax; only Social Harness metadata is a closed namespace. */
const behaviorFrontmatter = Schema.Struct({
  name: Schema.NonEmptyTrimmedString,
  description: Schema.NonEmptyString,
  metadata: Schema.Struct({
    "social-harness": exactStruct({
      applies: TextRule.fields.applies,
      notifySender: TextRule.fields.notifySender,
      guidance: TextRule.fields.guidance,
      feedback: TextRule.fields.feedback,
    }),
  }),
});

/** The closed categories a `BEHAVIOR.md` that does not map to a rule fails with. */
type BehaviorRefusal = "frontmatter" | "schema";

/** Provider construction owns execution; loading returns only its validated parameters. */
type LoadedDecisionProvider =
  | { readonly kind: "unavailable" }
  | {
      readonly kind: "jev";
      readonly model: string;
      readonly apiKey: Redacted.Redacted;
    };

/** Larger than any document of 256 editable rules and the fixed rules beside them. */
const MAXIMUM_POLICY_BYTES = 1_048_576;

/** A key is one token; surrounding whitespace, such as a trailing newline, is ignored. */
const decisionProviderKey = Schema.Trim.pipe(
  Schema.compose(
    Schema.String.pipe(
      Schema.minLength(1),
      Schema.maxLength(16_384),
      Schema.pattern(/^\S+$/u),
    ),
  ),
);

/** A configured document with the decision provider it names, and its behavior sources. */
export interface LoadedMessagePolicy {
  readonly policy: MessagePolicy;
  readonly provider: LoadedDecisionProvider;
  readonly behaviorRulePaths: ReadonlyMap<string, string>;
}

/**
 * The document `SHARNESS_MESSAGE_POLICY_FILE` names, or none when it is
 * unset. A `jev` decision provider reads its key from
 * `SHARNESS_DECISION_PROVIDER_KEY_FILE` and returns validated provider parameters; the
 * key file is not read for any other provider. An unreadable or invalid
 * document and a missing or invalid key fail configuration naming no file content.
 * @param configuration The policy and provider file locations.
 * @param readCredential The daemon-owned secret-file reader.
 * @returns The document, its decision provider and resolved behavior sources.
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
    return Option.some({
      policy: resolved.policy,
      behaviorRulePaths: resolved.behaviorRulePaths,
      provider: yield* loadDecisionProvider(
        policy,
        configuration,
        readCredential,
      ),
    });
  }).pipe(Effect.withSpan("loadMessagePolicy"));

interface BehaviorState {
  readonly root: string;
  readonly ids: Set<string>;
  readonly behaviorRulePaths: Map<string, string>;
  readonly initialRuleCount: number;
}

/** Operational evidence limits are part of the installed policy identity. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- Installation validates fixed settings with the same grammar retained audits decode.
export const firewallConfigurationSchema = Schema.Struct({
  mode: Schema.Literal("guidance", "enforcement"),
  facts: Schema.Array(OwnerFact),
  maximumHistoryRecords: Schema.Int.pipe(Schema.between(1, 1024)),
  maximumConcurrentChecks: Schema.Int.pipe(Schema.between(1, 32)),
});

/** Evidence limits and fixed facts are immutable for a process installation. */
export type FirewallConfiguration = typeof firewallConfigurationSchema.Type;

/**
 * Where a rule installation failure's fix is written: the rules-not-installed
 * entry covers invalid rule sets that exceed the request budget.
 * Storage failures have no policy-document fix to link.
 */
const installationDocs: Readonly<
  Record<Exclude<FirewallPolicyError["reason"], "not-configured">, string>
> = {
  "invalid-rule": `${FIREWALL_GUIDE}#rules-not-installed`,
  "fixed-rule": `${FIREWALL_GUIDE}#rules-not-installed`,
  "build-failed": `${FIREWALL_GUIDE}#rules-not-installed`,
  "persistence-failed": "",
};

/**
 * The configuration failure for a document whose rules the daemon could not
 * install against its store, such as an initial rule reusing a code rule's
 * id. `not-configured` here means the store holds editable natural-language rules
 * that a document without `text` cannot install; the document itself is
 * valid, so that message names the stored rules instead.
 * @param cause The closed installation failure.
 * @param behaviorRulePaths The source entry for each file-loaded behavior.
 * @returns The failure naming `SHARNESS_MESSAGE_POLICY_FILE` and the reason.
 */
export function policyInstallationError(
  cause: FirewallPolicyError,
  behaviorRulePaths?: ReadonlyMap<string, string>,
): MessagePolicyConfigurationError {
  const path =
    cause.ruleId === undefined
      ? undefined
      : behaviorRulePaths?.get(cause.ruleId);
  const location =
    path === undefined ? "" : `; invalid at ${path} (duplicate-id)`;
  return new MessagePolicyConfigurationError({
    reason: "message-policy",
    behaviorReason: path === undefined ? undefined : "duplicate-id",
    message:
      cause.reason === "not-configured"
        ? `SHARNESS_MESSAGE_POLICY_FILE configures no natural-language policy, but the state directory holds natural-language rules an earlier policy added; restore the text block, remove those rules with policy_update, then remove the block. Docs: ${FIREWALL_GUIDE}#stored-text-rules`
        : policyFailureMessage(
            "message-policy",
            `; its rules could not be installed (${cause.reason})${location}`,
            { path: path ?? "", docs: installationDocs[cause.reason] },
          ),
  });
}

/**
 * The configuration failure for a message-policy setting, with its fixed
 * operator message.
 * @param reason Why the setting cannot start the daemon.
 * @param path The indexed behavior entry, if one fails.
 * @param behaviorReason The closed behavior failure category.
 * @returns The failure, naming no configured value.
 */
export function policyConfigurationError(
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
 * Maps one `BEHAVIOR.md` to its editable natural-language rule. YAML parser
 * diagnostics stay private; only a closed category reaches `refused`, which
 * builds the caller's own failure.
 * @param bytes The behavior's bytes; callers bound them by {@link MAXIMUM_BEHAVIOR_BYTES}.
 * @param refused The caller's failure for a closed category.
 * @returns The rule.
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
    const rules = yield* Schema.decodeUnknown(editableDocumentSchema)([
      {
        id: frontmatter.name,
        text: parts[2].trim(),
        ...metadata,
      },
    ]).pipe(Effect.mapError(() => refused("schema")));
    return yield* Effect.fromNullable(rules[0]).pipe(
      Effect.mapError(() => refused("schema")),
    );
  }).pipe(Effect.withSpan("decodeBehavior"));
}

function uniqueTextRuleIds(policy: typeof document.Type): boolean {
  const ids = [
    ...(policy.text?.fixedRules ?? []),
    ...(policy.text?.initialRules ?? []),
  ].map((rule) => rule.id);
  return new Set(ids).size === ids.length;
}

/**
 * Decodes the YAML between a behavior's `---` lines. Parser diagnostics,
 * including unresolved tags, stay inside this boundary.
 * @param text The frontmatter YAML.
 * @param refused The caller's failure for a closed category.
 * @returns The frontmatter fields.
 */
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

/** Reports only the policy entry; YAML diagnostics may contain private file content. */
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
      MAXIMUM_BEHAVIOR_BYTES,
      refused,
    );
    const rule = yield* decodeBehavior(bytes, refused);
    return { rule };
  });
}

/** Initial behaviors are validated even when persisted editable rules already exist. */
function resolveBehaviors(policy: MessagePolicy, root: string) {
  return Effect.gen(function* () {
    const behaviorRulePaths = new Map<string, string>();
    const text = policy.text;
    if (text === undefined) {
      return { policy, behaviorRulePaths };
    }
    yield* Schema.decodeUnknown(editableDocumentSchema)(text.initialRules).pipe(
      Effect.mapError(() =>
        policyConfigurationError("message-policy", "text.initialRules"),
      ),
    );
    const state = {
      root,
      ids: new Set(
        [...text.fixedRules, ...text.initialRules].map((rule) => rule.id),
      ),
      behaviorRulePaths,
      initialRuleCount: text.initialRules.length,
    };
    const fixed = yield* loadFixedBehaviors(text, state);
    const initial = yield* loadBehaviors(
      text.initialBehaviors ?? [],
      "initialBehaviors",
      state,
    );
    const resolved: MessagePolicy = {
      ...policy,
      text: {
        fixedRules: [...text.fixedRules, ...fixed.map(({ rule }) => rule)],
        initialRules: [
          ...text.initialRules,
          ...initial.map(({ rule }) => rule),
        ],
      },
    };
    return {
      policy: resolved,
      behaviorRulePaths,
    };
  });
}

/** Every fixed document resolves through the policy file's own directory. */
function loadFixedBehaviors(
  text: NonNullable<MessagePolicy["text"]>,
  state: BehaviorState,
) {
  return loadBehaviors(text.fixedBehaviors ?? [], "fixedBehaviors", state);
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
        if (state.ids.has(behavior.rule.id)) {
          return yield* invalidBehavior(path, "duplicate-id");
        }
        if (field === "initialBehaviors") {
          count += 1;
          if (count > MAXIMUM_EDITABLE_RULES) {
            return yield* invalidBehavior(path, "editable-limit");
          }
        }
        state.ids.add(behavior.rule.id);
        state.behaviorRulePaths.set(behavior.rule.id, path);
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

/** A short read need not be EOF; stop only at EOF or the extra byte. */
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

/**
 * The operator message for a message-policy failure: its text, any detail,
 * and one `Docs:` link. An entry under a behavior list links the behavior
 * rules, `docs` replaces the failure's own troubleshooting entry, and an
 * empty `docs` leaves the link out for a failure with no fix to point at.
 * @param reason The closed failure.
 * @param detail Text after the failure's own, starting with `; `.
 * @param link The entry to link in place of the failure's own.
 * @param link.path The failing policy entry, which links the behavior rules
 *     when it names a behavior list.
 * @param link.docs Another entry to link, or empty for none.
 * @returns The message, naming no configured value.
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

/**
 * The failing rule as a refusal names it: its place in the update, then its id.
 * @param path The rule's place in the update, when the refusal has one.
 * @param ruleId The rule's id, when the refusal has one.
 * @returns The subject, or undefined when the refusal names neither.
 */
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

function loadDecisionProvider(
  policy: MessagePolicy,
  configuration: PolicyConfiguration,
  readCredential: CredentialReader,
): Effect.Effect<LoadedDecisionProvider, MessagePolicyConfigurationError> {
  const setting = policy.decisionProvider;
  if (policy.text === undefined || setting?.kind !== "jev") {
    return Effect.succeed({ kind: "unavailable" });
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
