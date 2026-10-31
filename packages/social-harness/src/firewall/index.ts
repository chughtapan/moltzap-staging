/** @file Active policy, authorized edits and checks over captured message batches. */

import * as SqlClient from "@effect/sql/SqlClient";
import {
  Clock,
  Context,
  Effect,
  Layer,
  Schedule,
  Schema,
  Struct,
} from "effect";
import { createHash, randomBytes } from "node:crypto";
import {
  exitOutcome,
  markRetries,
} from "../communication/conversations/index.js";
import {
  decodeRuntimeValue,
  encodeRuntimeValue,
  EndpointStoreError,
} from "../communication/store/index.js";
import { exactStruct } from "../encoding/exact.js";
import { MAXIMUM_RUNTIME_ITEM_BYTES } from "../encoding/limits.js";
import { AgentView } from "../identity/cards.js";
import {
  captureInput,
  captureRegistration,
  checkCapturedMessage,
  type CheckedGuidance,
  type DecidingRule,
  decidingRules,
  type DecisionProvider,
  DecisionProviderError,
  decodeFrozen,
  ENDPOINT_SENDER,
  type FirewallCheck,
  type Message,
  MessageContext,
  type MessageMiddleware,
  projectFeedback,
  textQuestionEdition,
  validateChecks,
} from "./check.js";
import {
  decodeBehavior,
  editableDocumentSchema,
  type FirewallConfiguration,
  firewallConfigurationSchema,
  type firewallDecisionsSchema,
  type firewallDecisionSummarySchema,
  type FirewallInstallation,
  FirewallPolicyError,
  FirewallSettings,
  type LoadedMessagePolicy,
  MAXIMUM_BEHAVIOR_BYTES,
  MAXIMUM_EDITABLE_RULES,
  type PolicyProblem,
  policyReadSchema,
  type policyUpdateResultSchema,
  policyUpdateSchema,
  PrivateCheckTrace,
  TextPolicySettings,
  TextRule,
} from "./config.js";
import { makeJevDecisionProvider } from "./jev.js";
import {
  type FirewallActivation,
  type FirewallCompletion,
  readDecisions,
  readFirewallPolicy,
  withoutTraces,
  writeFirewallPolicy,
} from "./store/index.js";

/** Code checks and provider resources remain fixed while text rules can change. */
export interface FirewallOptions {
  readonly mode: "guidance" | "enforcement";
  readonly incoming: readonly MessageMiddleware[];
  readonly outgoing: readonly MessageMiddleware[];
  readonly settings: FirewallSettings;
  readonly facts: MessageContext["facts"];
  readonly codeDigest: string;
  readonly maximumHistoryRecords?: number;
  readonly maximumConcurrentChecks?: number;
  readonly text?: {
    readonly fixedRules?: readonly TextRule[];
    readonly initialRules?: readonly TextRule[];
    readonly provider: DecisionProvider;
    readonly settings: TextPolicySettings;
  };
}

/** The daemon supplies exact projected text and verified, authorized evidence. */
export interface PreparedFirewallCheck {
  readonly direction: "incoming" | "outgoing";
  readonly canonicalCandidate: Uint8Array;
  readonly message?: Message;
  readonly context: MessageContext;
  readonly agents: readonly AgentView[];
}

/** A completed assessment includes the evidence needed to retain its decision. */
export interface CheckedMessage {
  readonly completion: FirewallCompletion;
  readonly result: FirewallCheck;
  readonly guidance?: CheckedGuidance;
}

/** Checks capture one policy for all supplied messages. */
export interface FirewallOperations {
  readonly configuration: FirewallConfiguration;
  /** Returns fixed and editable rules with the active policy revision. */
  readonly readPolicy: () => Effect.Effect<
    typeof policyReadSchema.Type,
    FirewallPolicyError
  >;
  /** Saves and activates the requested change. Failure preserves the active policy. */
  readonly updatePolicy: (
    request: typeof policyUpdateSchema.Type,
  ) => Effect.Effect<typeof policyUpdateResultSchema.Type, FirewallPolicyError>;
  /** Returns saved decision summaries without private assessment content. */
  readonly readDecisions: (request: {
    readonly since?: number;
  }) => Effect.Effect<typeof firewallDecisionsSchema.Type, FirewallPolicyError>;
  /** Returns completed assessments in input order. The caller saves their decisions. */
  readonly check: (
    inputs: readonly PreparedFirewallCheck[],
  ) => Effect.Effect<readonly CheckedMessage[], EndpointStoreError>;
}

/** Construction validates the complete policy before seeding a missing document. */
export function makeFirewall(
  source: FirewallOptions | LoadedMessagePolicy,
): Effect.Effect<FirewallOperations, FirewallPolicyError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const { captured, fixed, active } = yield* initialPolicy(source);
    return yield* liveFirewall(sql, captured, fixed, active);
  }).pipe(Effect.withSpan("makeFirewall"));
}

interface DisabledFirewall extends Omit<FirewallOperations, "configuration"> {
  readonly configuration: undefined;
}

/** Owns one active policy and access to retained decisions for the daemon's lifetime. */
export class Firewall extends Context.Tag("SocialHarness/Firewall")<
  Firewall,
  FirewallOperations | DisabledFirewall
>() {
  /** Unconfigured policy keeps saved decisions readable and produces no assessments. */
  static readonly layer = (
    source?: FirewallOptions | LoadedMessagePolicy,
  ): Layer.Layer<Firewall, FirewallPolicyError, SqlClient.SqlClient> => {
    if (source === undefined) {
      return Layer.effect(Firewall, disabledFirewall());
    }
    return Layer.effect(Firewall, makeFirewall(source));
  };
}

type FixedRules = typeof policyReadSchema.fields.fixedRules.Type;
const storedPolicyFormat = "org.social-harness/text-rules/v1";
const storedPolicySchema = exactStruct({
  format: Schema.Literal(storedPolicyFormat),
  rules: editableDocumentSchema,
});
const persistenceFailure = () =>
  new FirewallPolicyError({ reason: "persistence-failed" });

function disabledFirewall() {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const notConfigured = Effect.fail(
      new FirewallPolicyError({ reason: "not-configured" }),
    );
    return {
      configuration: undefined,
      readPolicy: () => notConfigured,
      updatePolicy: () => notConfigured,
      readDecisions: (request) => savedDecisions(sql, request),
      check: () => Effect.succeed([]),
    } satisfies DisabledFirewall;
  });
}

function savedDecisions(
  sql: SqlClient.SqlClient,
  request: { readonly since?: number },
) {
  return readDecisions(request).pipe(
    Effect.provideService(SqlClient.SqlClient, sql),
    Effect.catchTag("EndpointStoreError", () =>
      Effect.fail(persistenceFailure()),
    ),
  );
}

type PolicyEditPath = NonNullable<FirewallPolicyError["path"]>;
const invalidRule = (
  problem: PolicyProblem,
  at: { readonly path?: PolicyEditPath; readonly ruleId?: string } = {},
) => new FirewallPolicyError({ reason: "invalid-rule", problem, ...at });

function initialPolicy(source: FirewallOptions | LoadedMessagePolicy) {
  return Effect.gen(function* () {
    const options =
      "policy" in source ? yield* configuredOptions(source) : source;
    const captured = yield* captureOptions(options).pipe(
      Effect.catchTags({
        CheckInputError: () =>
          Effect.fail(new FirewallPolicyError({ reason: "build-failed" })),
        ParseError: () =>
          Effect.fail(new FirewallPolicyError({ reason: "build-failed" })),
      }),
      Effect.catchAllDefect(() =>
        Effect.fail(new FirewallPolicyError({ reason: "build-failed" })),
      ),
    );
    const fixed = yield* decodeFrozen(
      policyReadSchema.fields.fixedRules,
      fixedRules(captured),
    ).pipe(
      Effect.catchTag("ParseError", () =>
        Effect.fail(new FirewallPolicyError({ reason: "build-failed" })),
      ),
    );
    const { stored, rules } = yield* loadRules(captured);
    yield* validateIds(
      rules,
      fixed.map((rule) => rule.id),
    );
    const active = yield* buildPolicy(captured, rules);
    if (stored === undefined) {
      yield* persistRules(rules);
    }
    return { captured, fixed, active };
  });
}

function loadRules(options: CapturedOptions) {
  return Effect.gen(function* () {
    const stored = yield* readFirewallPolicy().pipe(
      Effect.catchTag("EndpointStoreError", () =>
        Effect.fail(persistenceFailure()),
      ),
    );
    const selected =
      stored === undefined
        ? (options.text?.initialRules ?? [])
        : (yield* decodeRuntimeValue(storedPolicySchema, stored).pipe(
            Effect.catchTag("EndpointStoreError", () =>
              Effect.fail(persistenceFailure()),
            ),
          )).rules;
    const rules = yield* decodeFrozen(editableDocumentSchema, selected).pipe(
      Effect.catchTag("ParseError", () =>
        Effect.fail(
          new FirewallPolicyError({
            reason:
              stored === undefined ? "build-failed" : "persistence-failed",
          }),
        ),
      ),
    );
    return { stored, rules };
  });
}

function liveFirewall(
  sql: SqlClient.SqlClient,
  options: CapturedOptions,
  fixed: FixedRules,
  initial: ActiveRules,
) {
  return Effect.gen(function* () {
    let active = initial;
    const fixedIds = fixed.map((rule) => rule.id);
    const edits = yield* Effect.makeSemaphore(1);
    const permits = yield* Effect.makeSemaphore(
      options.configuration.maximumConcurrentChecks,
    );
    function activate(replacement: ActiveRules) {
      active = replacement;
    }
    function updatePolicy(request: typeof policyUpdateSchema.Type) {
      return edits.withPermits(1)(
        Effect.gen(function* () {
          const change = yield* preparePolicyChange(
            options,
            active.editableRules,
            fixedIds,
            request,
          );
          return yield* activatePolicy(sql, change, activate);
        }),
      );
    }
    const firewall: FirewallOperations = {
      configuration: options.configuration,
      readPolicy: () =>
        Effect.sync(() => ({
          rules: active.editableRules,
          fixedRules: fixed,
          activeRevision: active.configurationDigest,
        })),
      updatePolicy,
      readDecisions: (request) => savedDecisions(sql, request),
      check: (inputs) =>
        Effect.suspend(() => checkBatch(options, active, permits, inputs)),
    };
    return Object.freeze(firewall);
  });
}

function checkBatch(
  options: CapturedOptions,
  policy: ActiveRules,
  permits: Effect.Semaphore,
  inputs: readonly PreparedFirewallCheck[],
) {
  return Effect.forEach(
    inputs,
    (input) => permits.withPermits(1)(assess(options, policy, input)),
    {
      concurrency: options.configuration.maximumConcurrentChecks,
    },
  );
}

function preparePolicyChange(
  options: CapturedOptions,
  rules: readonly TextRule[],
  fixedIds: readonly string[],
  request: typeof policyUpdateSchema.Type,
) {
  return Effect.gen(function* () {
    const change = yield* decodeFrozen(policyUpdateSchema, request).pipe(
      Effect.catchTag("ParseError", () => Effect.fail(invalidRule("schema"))),
      Effect.flatMap(resolvedChange),
    );
    const next = yield* editedRules(rules, fixedIds, change);
    const replacement = yield* buildPolicy(options, next);
    yield* approveRuleChange();
    return { change, rules: next, replacement };
  });
}

/** Persistence and the sole active-policy assignment complete in one interruption boundary. */
function activatePolicy(
  sql: SqlClient.SqlClient,
  prepared: Effect.Effect.Success<ReturnType<typeof preparePolicyChange>>,
  activate: (replacement: ActiveRules) => void,
) {
  return Effect.uninterruptible(
    Effect.gen(function* () {
      const { rules, replacement, change } = prepared;
      yield* persistRules(rules, {
        time: yield* Clock.currentTimeMillis,
        activeRevision: replacement.configurationDigest,
        addedRuleIds: change.add.map((rule) => rule.id),
        removedRuleIds: change.remove,
      });
      activate(replacement);
      return { activeRevision: replacement.configurationDigest };
    }),
  ).pipe(Effect.provideService(SqlClient.SqlClient, sql));
}

/** Approval remains permissive until the host's human-confirmation boundary is implemented. */
function approveRuleChange(): Effect.Effect<void, FirewallPolicyError> {
  return Effect.void;
}

function configuredOptions(loaded: LoadedMessagePolicy) {
  return Effect.gen(function* () {
    const policy = loaded.policy;
    const codeDigest = yield* digest({
      policy: {
        mode: policy.mode,
        settings: policy.settings,
        facts: policy.facts,
        decisionProvider: policy.decisionProvider ?? null,
        textSettings: policy.textSettings,
        maximumHistoryRecords: policy.maximumHistoryRecords ?? null,
        maximumConcurrentChecks: policy.maximumConcurrentChecks ?? null,
        fixedRules: policy.text?.fixedRules ?? [],
      },
      fixedBehaviorDigests: loaded.fixedBehaviorDigests,
    }).pipe(
      Effect.catchTag("EndpointStoreError", () =>
        Effect.fail(new FirewallPolicyError({ reason: "build-failed" })),
      ),
    );
    const provider: DecisionProvider =
      loaded.provider.kind === "jev"
        ? retryingTransientFailures(makeJevDecisionProvider(loaded.provider))
        : {
            decide: () =>
              Effect.fail(new DecisionProviderError({ reason: "transport" })),
          };
    return {
      mode: policy.mode,
      incoming: [],
      outgoing: [],
      settings: policy.settings,
      facts: policy.facts,
      codeDigest,
      maximumHistoryRecords: policy.maximumHistoryRecords,
      maximumConcurrentChecks: policy.maximumConcurrentChecks,
      text:
        policy.text === undefined
          ? undefined
          : {
              fixedRules: policy.text.fixedRules,
              initialRules: policy.text.initialRules,
              settings: policy.textSettings,
              provider,
            },
    } satisfies FirewallOptions;
  });
}

function captureOptions(options: FirewallOptions) {
  return Effect.gen(function* () {
    const configuration = yield* decodeFrozen(firewallConfigurationSchema, {
      mode: options.mode,
      facts: options.facts,
      codeDigest: options.codeDigest,
      maximumHistoryRecords: options.maximumHistoryRecords ?? 256,
      maximumConcurrentChecks: options.maximumConcurrentChecks ?? 4,
    });
    const settings = yield* decodeFrozen(FirewallSettings, options.settings);
    const incoming = yield* Effect.forEach(
      options.incoming,
      captureRegistration,
      { concurrency: 1 },
    );
    const outgoing = yield* Effect.forEach(
      options.outgoing,
      captureRegistration,
      { concurrency: 1 },
    );
    const text =
      options.text === undefined ? undefined : yield* captureText(options.text);
    return { configuration, settings, incoming, outgoing, text };
  });
}
function captureText(selected: NonNullable<FirewallOptions["text"]>) {
  return Effect.gen(function* () {
    return {
      fixedRules: yield* decodeFrozen(
        Schema.Array(TextRule),
        selected.fixedRules ?? [],
      ),
      initialRules: yield* decodeFrozen(
        editableDocumentSchema,
        selected.initialRules ?? [],
      ),
      settings: yield* decodeFrozen(TextPolicySettings, selected.settings),
      provider: {
        decide: selected.provider.decide.bind(selected.provider),
        ...(selected.provider.provenance === undefined
          ? {}
          : {
              provenance: yield* decodeFrozen(
                PrivateCheckTrace,
                selected.provider.provenance,
              ),
            }),
      },
    };
  });
}
type CapturedOptions = Effect.Effect.Success<ReturnType<typeof captureOptions>>;

function buildPolicy(options: CapturedOptions, rules: readonly TextRule[]) {
  return Effect.gen(function* () {
    const text = yield* textRules(options, rules);
    yield* validateChecks({
      ...text,
      incoming: options.incoming,
      outgoing: options.outgoing,
      settings: options.settings,
    });
    const metadata = installationMetadata(options, text);
    return {
      metadata,
      editableRules: rules,
      configurationDigest: yield* digest(metadata),
      ...Struct.omit(text, "provider", "textSettings"),
    };
  }).pipe(
    Effect.catchTags({
      CheckInputError: () =>
        Effect.fail(new FirewallPolicyError({ reason: "build-failed" })),
      EndpointStoreError: () =>
        Effect.fail(new FirewallPolicyError({ reason: "build-failed" })),
    }),
    Effect.catchAllDefect(() =>
      Effect.fail(new FirewallPolicyError({ reason: "build-failed" })),
    ),
  );
}
function textRules(options: CapturedOptions, rules: readonly TextRule[]) {
  return Effect.gen(function* () {
    const fixed = options.text?.fixedRules ?? [];
    const settings = options.text?.settings;
    const documents = [...fixed, ...rules];
    if (documents.length > 0 && options.text === undefined) {
      return yield* new FirewallPolicyError({ reason: "not-configured" });
    }
    return {
      rules: documents,
      revision: yield* digest({
        rules: documents,
        settings: settings ?? null,
      }),
      fixedRuleIds: fixed.map((rule) => rule.id),
      textSettings: settings,
      provider: options.text?.provider,
    };
  });
}

function installationMetadata(
  options: CapturedOptions,
  text: Effect.Effect.Success<ReturnType<typeof textRules>>,
): FirewallInstallation {
  const describe = ({
    id,
    revision,
    rules,
    provenance,
  }: MessageMiddleware) => ({
    id,
    revision,
    rules,
    provenance: provenance ?? {
      format: "unavailable",
      data: "No descriptive provenance was declared.",
    },
  });
  const metadata: FirewallInstallation = {
    configuration: options.configuration,
    settings: options.settings,
    incoming: options.incoming.map(describe),
    outgoing: options.outgoing.map(describe),
    ...(text.rules.length === 0 || options.text === undefined
      ? {}
      : {
          text: {
            edition: textQuestionEdition,
            revision: text.revision,
            rules: text.rules,
            fixedRuleIds: text.fixedRuleIds,
            settings: options.text.settings,
            ...(options.text.provider.provenance === undefined
              ? {}
              : {
                  provider: options.text.provider.provenance,
                }),
          },
        }),
  };
  return metadata;
}
type ActiveRules = Effect.Effect.Success<ReturnType<typeof buildPolicy>>;
function digest(value: unknown) {
  return encodeRuntimeValue(value).pipe(
    Effect.map((bytes) => createHash("sha256").update(bytes).digest("hex")),
  );
}

/**
 * Fixed rules in installation order, code incoming then code outgoing then
 * fixed text; an id declared more than once is listed at its first
 * declaration, as one middleware registered in both directions declares its
 * rules twice.
 */
function fixedRules(options: CapturedOptions): FixedRules {
  const declared: FixedRules = [
    ...[...options.incoming, ...options.outgoing].flatMap((middleware) =>
      middleware.rules.map((rule) => ({
        from: "code" as const,
        middleware: middleware.id,
        ...Struct.omit(rule),
      })),
    ),
    ...(options.text?.fixedRules ?? []).map((rule) => ({
      from: "text" as const,
      ...Struct.omit(rule),
    })),
  ];
  const first = new Map<string, FixedRules[number]>();
  for (const rule of declared) {
    if (!first.has(rule.id)) {
      first.set(rule.id, rule);
    }
  }
  return [...first.values()];
}

function validateIds(
  rules: readonly TextRule[],
  fixedRuleIds: readonly string[],
) {
  const collision = rules.find((rule) => fixedRuleIds.includes(rule.id));
  if (collision !== undefined) {
    return Effect.fail(
      new FirewallPolicyError({ reason: "fixed-rule", ruleId: collision.id }),
    );
  }
  const repeated = rules.find(
    (rule, index) => rules.findIndex(({ id }) => id === rule.id) !== index,
  );
  return repeated === undefined
    ? Effect.void
    : Effect.fail(invalidRule("duplicate-id", { ruleId: repeated.id }));
}

function persistRules(
  rules: readonly TextRule[],
  activation?: FirewallActivation,
) {
  return encodeRuntimeValue({ format: storedPolicyFormat, rules }).pipe(
    Effect.flatMap((bytes) => writeFirewallPolicy(bytes, activation)),
    Effect.catchTag("EndpointStoreError", () =>
      Effect.fail(persistenceFailure()),
    ),
  );
}

interface RuleChange {
  readonly add: typeof editableDocumentSchema.Type;
  readonly remove: readonly string[];
}

/** Each `{behavior}` addition becomes its rule; a behavior the policy file would refuse is an invalid rule at its place in `add`. */
function resolvedChange(change: typeof policyUpdateSchema.Type) {
  return Effect.forEach(
    change.add ?? [],
    (item, index) =>
      "behavior" in item
        ? behaviorRule(item.behavior, ["add", index])
        : Effect.succeed(item),
    { concurrency: 1 },
  ).pipe(
    Effect.map((add): RuleChange => ({ add, remove: change.remove ?? [] })),
  );
}

/** A behavior supplies the same direction and registered advice as an inline rule. */
function behaviorRule(text: string, path: PolicyEditPath) {
  const bytes = Buffer.from(text, "utf8");
  if (bytes.length > MAXIMUM_BEHAVIOR_BYTES) {
    return Effect.fail(invalidRule("too-large", { path }));
  }
  return decodeBehavior(bytes, (problem) => invalidRule(problem, { path }));
}

/**
 * The editable document an edit leaves, or the first item it cannot apply:
 * a fixed id, a removal of an id the document lacks, an addition whose id the
 * document keeps or the edit adds twice, or more rules than the limit.
 */
function editedRules(
  rules: readonly TextRule[],
  fixedRuleIds: readonly string[],
  { add, remove }: RuleChange,
) {
  return Effect.gen(function* () {
    const kept = yield* rulesAfterRemovals(rules, fixedRuleIds, remove);
    const ids = new Set(kept.map((rule) => rule.id));
    for (const [index, rule] of add.entries()) {
      const path: PolicyEditPath = ["add", index];
      if (fixedRuleIds.includes(rule.id)) {
        return yield* new FirewallPolicyError({
          reason: "fixed-rule",
          ruleId: rule.id,
          path,
        });
      }
      if (ids.has(rule.id)) {
        return yield* invalidRule("duplicate-id", { ruleId: rule.id, path });
      }
      ids.add(rule.id);
    }
    if (kept.length + add.length > MAXIMUM_EDITABLE_RULES) {
      return yield* invalidRule("editable-limit");
    }
    return yield* decodeFrozen(editableDocumentSchema, [...kept, ...add]).pipe(
      Effect.mapError(() => invalidRule("schema")),
    );
  });
}

function rulesAfterRemovals(
  rules: readonly TextRule[],
  fixedRuleIds: readonly string[],
  remove: readonly string[],
) {
  return Effect.gen(function* () {
    for (const [index, ruleId] of remove.entries()) {
      const path: PolicyEditPath = ["remove", index];
      if (fixedRuleIds.includes(ruleId)) {
        return yield* new FirewallPolicyError({
          reason: "fixed-rule",
          ruleId,
          path,
        });
      }
      if (!rules.some((rule) => rule.id === ruleId)) {
        return yield* invalidRule("unknown-id", { ruleId, path });
      }
    }
    return rules.filter((rule) => !remove.includes(rule.id));
  });
}

/** Only transient transport, rate-limit and server failures share the stage's bounded retry window. */
function retryingTransientFailures(
  provider: DecisionProvider,
): DecisionProvider {
  return {
    ...(provider.provenance === undefined
      ? {}
      : { provenance: provider.provenance }),
    decide: (request) =>
      Effect.suspend(() => provider.decide(request)).pipe(
        Effect.retry({
          times: 2,
          schedule: Schedule.exponential("250 millis"),
          while: isTransient,
        }),
      ),
  };
}

function isTransient(error: DecisionProviderError): boolean {
  const status = error.status ?? 0;
  return (
    error.reason === "transport" ||
    (error.reason === "http" && (status === 429 || status >= 500))
  );
}

/** The evaluator and retained decision receive the same detached message and context. */
function capturePrepared(
  configuration: FirewallConfiguration,
  input: PreparedFirewallCheck,
) {
  return Effect.gen(function* () {
    const context = {
      ...Struct.omit(input.context),
      facts: configuration.facts,
    };
    const evidence =
      input.message === undefined
        ? {
            event: undefined,
            context: yield* decodeFrozen(MessageContext, context),
          }
        : yield* captureInput(
            { direction: input.direction, message: input.message },
            context,
          );
    return {
      direction: input.direction,
      canonicalCandidate: input.canonicalCandidate.slice(),
      evidence,
      agents: yield* decodeFrozen(Schema.Array(AgentView), input.agents),
    };
  }).pipe(
    Effect.catchTags({
      CheckInputError: () =>
        Effect.fail(new EndpointStoreError({ reason: "corrupt" })),
      ParseError: () =>
        Effect.fail(new EndpointStoreError({ reason: "corrupt" })),
    }),
  );
}

type CapturedCheck = Effect.Effect.Success<ReturnType<typeof capturePrepared>>;

/** Completed checks carry the exact policy and evidence that delivery commits. */
function assess(
  options: CapturedOptions,
  active: ActiveRules,
  requested: PreparedFirewallCheck,
) {
  return Effect.gen(function* () {
    const input = yield* capturePrepared(options.configuration, requested);
    const checkId = mintCheckId();
    const assessed = yield* evaluateMessage(options, active, input).pipe(
      markRetries("firewall-check", input.direction, (exit) =>
        exitOutcome(exit, "checked"),
      ),
    );
    const projection = yield* guidanceProjection(assessed, input, {
      mode: options.configuration.mode,
      checkId,
    });
    const admit =
      options.configuration.mode === "guidance" ||
      projection.result.judgment === "allow";
    return {
      result: projection.result,
      completion: {
        checkId,
        direction: input.direction,
        canonicalCandidate: input.canonicalCandidate,
        canonicalInput: yield* retainedInput(active, input),
        admit,
        canonicalResult: yield* encodeRuntimeValue({
          result: withoutTraces(projection.result),
          summaries: yield* decisionSummaries(active, input, projection.result),
        }),
      },
      ...(admit && projection.guidance !== undefined
        ? { guidance: projection.guidance }
        : {}),
    } satisfies CheckedMessage;
  }).pipe(Effect.withSpan("assessMessage"));
}

function evaluateMessage(
  options: CapturedOptions,
  active: ActiveRules,
  input: CapturedCheck,
) {
  const evidence = input.evidence;
  return evidence.event === undefined
    ? Effect.succeed(unavailable())
    : checkCapturedMessage(
        {
          code: options[input.direction],
          rules: active.rules,
          settings: options.settings,
          revision: active.revision,
          ...(options.text === undefined
            ? {}
            : {
                textSettings: options.text.settings,
                provider: options.text.provider,
              }),
        },
        evidence,
      ).pipe(
        Effect.catchTag("CheckInputError", () => Effect.succeed(unavailable())),
      );
}

function retainedInput(active: ActiveRules, input: CapturedCheck) {
  return encodeRuntimeValue({
    configurationDigest: active.configurationDigest,
    installation: active.metadata,
    context: input.evidence.context,
    agents: input.agents,
    message: input.evidence.event?.message ?? null,
  });
}

type DecisionSummary = typeof firewallDecisionSummarySchema.Type;

/** Final public attribution uses the same captured rules and result as admission. */
function decisionSummaries(
  active: ActiveRules,
  input: CapturedCheck,
  result: FirewallCheck,
): Effect.Effect<readonly DecisionSummary[], EndpointStoreError> {
  const registrations = [
    ...active.metadata[input.direction].map((entry) => ({
      ...entry,
      source: "code" as const,
    })),
    ...result.checks
      .filter((check) => check.source === "text")
      .map((check) => ({
        source: "text" as const,
        id: check.middlewareId,
        rules: active.rules,
      })),
  ];
  const deciding = decidingRules(result.reason, registrations, result.checks);
  const cause = summaryCause(result, deciding);
  const decidedBy = deciding.map(
    (rule): DecisionSummary["decidedBy"][number] => {
      let from: DecisionSummary["decidedBy"][number]["from"] = "editable";
      if (rule.source === "code") {
        from = "code";
      } else if (active.fixedRuleIds.includes(rule.ruleId)) {
        from = "fixed";
      }
      return {
        ruleId: rule.ruleId,
        from,
        ...(rule.advice === undefined ? {} : { advice: rule.advice }),
      };
    },
  );
  const checkedRuleIds = [
    ...new Set(
      result.checks.flatMap((check) =>
        check.findings.map((finding) => finding.ruleId),
      ),
    ),
  ];
  const message = input.evidence.event?.message;
  const kind = message?.operation?.kind;
  let peers: ReadonlyArray<string | null> = [null];
  if (message !== undefined && kind !== "result" && kind !== "failure") {
    peers =
      input.direction === "outgoing" ? message.recipients : [message.sender];
  }
  return Effect.forEach(
    peers,
    (identity) => {
      const peer =
        identity === null
          ? null
          : input.agents.find((agent) => agent.agentId === identity);
      if (peer === undefined) {
        return Effect.fail(new EndpointStoreError({ reason: "corrupt" }));
      }
      return Effect.succeed({
        direction: input.direction,
        peer,
        outcome: result.judgment,
        cause,
        decidedBy,
        checkedRuleIds,
        activeRevision: active.configurationDigest,
      });
    },
    { concurrency: 1 },
  );
}

function summaryCause(
  result: FirewallCheck,
  deciding: readonly DecidingRule[],
): DecisionSummary["cause"] {
  const first = deciding.find((rule) => rule.source === "text") ?? deciding[0];
  if (
    first?.finding === "unavailable" ||
    (first === undefined &&
      result.reason === "check-unavailable" &&
      result.checks.some((check) => check.status === "unavailable"))
  ) {
    return "unavailable";
  }
  if (first === undefined) {
    return result.judgment === "allow" ? "default" : "endpoint";
  }
  if (first.source === "code") {
    return "code";
  }
  return first.finding === "uncertain" ? "uncertain" : "judgment";
}

/** A local reference correlates an assessment without granting lookup authority. */
function mintCheckId() {
  return `chk_${randomBytes(32).toString("hex")}`;
}

/** Missing or invalid assessment input holds without inventing a deciding rule. */
function unavailable(): FirewallCheck {
  return {
    judgment: "hold",
    reason: "check-unavailable",
    feedback: projectFeedback([], [], "check-unavailable"),
    guidance: [],
    checks: [],
  };
}

/** Registered advice includes only the message references already visible to this agent. */
function guidanceProjection(
  result: FirewallCheck,
  input: CapturedCheck,
  capture: {
    readonly mode: "guidance" | "enforcement";
    readonly checkId: string;
  },
) {
  return Effect.gen(function* () {
    const instructions =
      input.direction === "incoming" || capture.mode === "guidance"
        ? result.guidance
        : result.guidance.filter((entry) => "uncertain" in entry);
    if (instructions.length === 0) {
      return { result };
    }
    const selected = new Set(
      instructions.flatMap((entry) =>
        "subjects" in entry ? entry.subjects : [],
      ),
    );
    const subjects = guidanceSubjects(selected, input);
    const guidance: CheckedGuidance = {
      checkId: capture.checkId,
      instructions,
      subjects,
    };
    const bytes = yield* encodeRuntimeValue(guidance);
    return subjects.length !== selected.size ||
      bytes.byteLength + input.canonicalCandidate.byteLength >
        MAXIMUM_RUNTIME_ITEM_BYTES
      ? { result: { ...unavailable(), checks: result.checks } }
      : { result, guidance };
  });
}

function guidanceSubjects(
  selected: ReadonlySet<string>,
  input: CapturedCheck,
): CheckedGuidance["subjects"] {
  const agents = new Map<string, AgentView>(
    input.agents.map((agent) => [agent.agentId, agent]),
  );
  const available = input.evidence.context.history.filter(
    (entry) => entry.visibility === "runtime" && entry.admission === "admitted",
  );
  const message = input.evidence.event?.message;
  const subjects = new Map(
    [...available, ...(message === undefined ? [] : [message])].map((entry) => [
      entry.id,
      entry,
    ]),
  );
  return [...subjects.values()]
    .filter((entry) => selected.has(entry.id))
    .flatMap((entry) => {
      const sender =
        entry.sender === ENDPOINT_SENDER
          ? ENDPOINT_SENDER
          : agents.get(entry.sender);
      const recipients = entry.recipients.flatMap((identity) => {
        const agent = agents.get(identity);
        return agent === undefined ? [] : [agent];
      });
      return sender === undefined ||
        recipients.length !== entry.recipients.length
        ? []
        : [{ id: entry.id, sender, recipients, text: entry.text }];
    });
}

/** The firewall's supported offline and managed callers share these check contracts. */
export {
  CheckInputError,
  type DecisionProvider,
  DecisionProviderError,
  type DecisionRequest,
  type DecisionResponse,
  type Message,
  type MessageContext,
  type MessageMiddleware,
  MiddlewareError,
  type MiddlewareInput,
  type MiddlewareResult,
} from "./check.js";
/** Owner configuration and rule schemas belong to the same firewall boundary. */
export {
  decodeMessagePolicy,
  type FirewallSettings,
  loadMessagePolicy,
  MessagePolicy,
  type RuleDescription,
  type TextPolicySettings,
  type TextRule,
} from "./config.js";
