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
  checkCapturedMessage,
  type CheckedGuidance,
  type DecidingRule,
  decidingRules,
  type DecisionProvider,
  DecisionProviderError,
  decodeFrozen,
  ENDPOINT_SENDER,
  type FirewallCheck,
  freezeData,
  type Message,
  MessageContext,
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
  FirewallPolicyError,
  FirewallSettings,
  type LoadedMessagePolicy,
  MAXIMUM_BEHAVIOR_BYTES,
  MAXIMUM_EDITABLE_RULES,
  type PolicyProblem,
  type policyReadSchema,
  type policyUpdateResultSchema,
  policyUpdateSchema,
  TextPolicySettings,
  TextRule,
} from "./config.js";
import { makeJevDecisionProvider } from "./jev.js";
import {
  type FirewallActivation,
  type FirewallCompletion,
  readDecisions,
  readFirewallPolicy,
  writeFirewallPolicy,
} from "./store/index.js";

/** Provider resources remain fixed while authorized edits change text rules. */
export interface FirewallOptions {
  readonly mode: "guidance" | "enforcement";
  readonly settings: FirewallSettings;
  readonly facts: MessageContext["facts"];
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
    const options = "policy" in source ? configuredOptions(source) : source;
    const captured = yield* captureOptions(options).pipe(
      Effect.catchTag("ParseError", () =>
        Effect.fail(new FirewallPolicyError({ reason: "build-failed" })),
      ),
      Effect.catchAllDefect(() =>
        Effect.fail(new FirewallPolicyError({ reason: "build-failed" })),
      ),
    );
    const fixed = captured.text?.fixedRules ?? [];
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
    const rules =
      stored === undefined
        ? (options.text?.initialRules ?? [])
        : (yield* decodeRuntimeValue(storedPolicySchema, stored).pipe(
            Effect.catchTag("EndpointStoreError", () =>
              Effect.fail(persistenceFailure()),
            ),
          )).rules;
    freezeData(rules);
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
    /** Persistence and activation complete in one interruption boundary. */
    const updatePolicy = Effect.fn(function* (
      request: typeof policyUpdateSchema.Type,
    ) {
      const { change, rules, replacement } = yield* preparePolicyChange(
        options,
        active.editableRules,
        fixedIds,
        request,
      );
      const addedRuleIds = change.add.map((rule) => rule.id);
      return yield* Effect.uninterruptible(
        Effect.gen(function* () {
          yield* persistRules(rules, {
            time: yield* Clock.currentTimeMillis,
            activeRevision: replacement.configurationDigest,
            addedRuleIds,
            removedRuleIds: change.remove,
          });
          active = replacement;
          return { activeRevision: replacement.configurationDigest };
        }),
      ).pipe(Effect.provideService(SqlClient.SqlClient, sql));
    }, edits.withPermits(1));
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
      check: Effect.fn(function* (inputs: readonly PreparedFirewallCheck[]) {
        const policy = active;
        return yield* Effect.forEach(
          inputs,
          (input) => permits.withPermits(1)(assess(options, policy, input)),
          { concurrency: options.configuration.maximumConcurrentChecks },
        );
      }),
    };
    return Object.freeze(firewall);
  });
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

/** Approval remains permissive until the host's human-confirmation boundary is implemented. */
function approveRuleChange(): Effect.Effect<void, FirewallPolicyError> {
  return Effect.void;
}

function configuredOptions(loaded: LoadedMessagePolicy) {
  const policy = loaded.policy;
  const provider: DecisionProvider =
    loaded.provider.kind === "jev"
      ? retryingTransientFailures(makeJevDecisionProvider(loaded.provider))
      : {
          decide: () =>
            Effect.fail(new DecisionProviderError({ reason: "transport" })),
        };
  return {
    mode: policy.mode,
    settings: policy.settings,
    facts: policy.facts,
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
}

function captureOptions(options: FirewallOptions) {
  return Effect.gen(function* () {
    const configuration = yield* decodeFrozen(firewallConfigurationSchema, {
      mode: options.mode,
      facts: options.facts,
      maximumHistoryRecords: options.maximumHistoryRecords ?? 256,
      maximumConcurrentChecks: options.maximumConcurrentChecks ?? 4,
    });
    const settings = yield* decodeFrozen(FirewallSettings, options.settings);
    const text =
      options.text === undefined ? undefined : yield* captureText(options.text);
    return { configuration, settings, text };
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
        ...(yield* decodeFrozen(
          Schema.Struct({
            provider: Schema.optional(Schema.NonEmptyTrimmedString),
            model: Schema.optional(Schema.NonEmptyTrimmedString),
          }),
          {
            provider: selected.provider.provider,
            model: selected.provider.model,
          },
        )),
      },
    };
  });
}
type CapturedOptions = Effect.Effect.Success<ReturnType<typeof captureOptions>>;

function buildPolicy(options: CapturedOptions, rules: readonly TextRule[]) {
  return Effect.gen(function* () {
    const fixed = options.text?.fixedRules ?? [];
    const documents = [...fixed, ...rules];
    if (documents.length > 0 && options.text === undefined) {
      return yield* new FirewallPolicyError({ reason: "not-configured" });
    }
    const metadata = {
      configuration: options.configuration,
      settings: options.settings,
      questionEdition: textQuestionEdition,
      rules: documents,
      fixedRuleIds: fixed.map((rule) => rule.id),
      ...(options.text === undefined
        ? {}
        : {
            textSettings: options.text.settings,
            provider: options.text.provider.provider ?? null,
            model: options.text.provider.model ?? null,
          }),
    };
    const configurationDigest = yield* digest(metadata);
    yield* validateChecks({
      rules: documents,
      settings: options.settings,
      revision: configurationDigest,
      textSettings: options.text?.settings,
    });
    return {
      metadata,
      editableRules: rules,
      configurationDigest,
      rules: documents,
      fixedRuleIds: metadata.fixedRuleIds,
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

type ActiveRules = Effect.Effect.Success<ReturnType<typeof buildPolicy>>;
function digest(value: unknown) {
  return encodeRuntimeValue(value).pipe(
    Effect.map((bytes) => createHash("sha256").update(bytes).digest("hex")),
  );
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
  return decodeBehavior(bytes, (problem) =>
    invalidRule(problem, { path }),
  ).pipe(
    Effect.tap((rule) =>
      Effect.sync(() => {
        freezeData(rule);
      }),
    ),
  );
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
    const kept = rules.filter((rule) => !remove.includes(rule.id));
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
    return Object.freeze([...kept, ...add]);
  });
}

/** Only transient transport, rate-limit and server failures share the stage's bounded retry window. */
function retryingTransientFailures(
  provider: DecisionProvider,
): DecisionProvider {
  return {
    ...(provider.provider === undefined ? {} : { provider: provider.provider }),
    ...(provider.model === undefined ? {} : { model: provider.model }),
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
          result: Struct.omit(projection.result, "trace"),
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
          rules: active.rules,
          settings: options.settings,
          revision: active.configurationDigest,
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
  const deciding = decidingRules(result.reason, active.rules, result);
  const cause = summaryCause(result, deciding);
  const decidedBy = deciding.map(
    (rule): DecisionSummary["decidedBy"][number] => ({
      ruleId: rule.ruleId,
      from: active.fixedRuleIds.includes(rule.ruleId) ? "fixed" : "editable",
      ...(rule.advice === undefined ? {} : { advice: rule.advice }),
    }),
  );
  const checkedRuleIds = [
    ...new Set(result.findings.map((finding) => finding.ruleId)),
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
  const first = deciding[0];
  if (
    first?.finding === "unavailable" ||
    (first === undefined &&
      result.reason === "check-unavailable" &&
      result.findings.some(
        (finding) =>
          finding.outcome === "hold" && finding.reason === "check-unavailable",
      ))
  ) {
    return "unavailable";
  }
  if (first === undefined) {
    return result.judgment === "allow" ? "default" : "endpoint";
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
    findings: [],
    expectations: [],
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
      ? {
          result: {
            ...unavailable(),
            findings: result.findings,
            expectations: result.expectations,
          },
        }
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
} from "./check.js";
/** Owner configuration and rule schemas belong to the same firewall boundary. */
export {
  decodeMessagePolicy,
  type FirewallSettings,
  loadMessagePolicy,
  MessagePolicy,
  type TextPolicySettings,
  type TextRule,
} from "./config.js";
