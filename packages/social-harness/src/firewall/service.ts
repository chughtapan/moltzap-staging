/** @file Owns local message policy, assessments, permitted content and retained decisions. */

import { SqlClient } from "@effect/sql";
import {
  Clock,
  Context,
  Data,
  Effect,
  Layer,
  Schedule,
  Schema,
  Struct,
} from "effect";
import { createHash, randomBytes } from "node:crypto";
import {
  AnswerOutcome,
  CollectiveId,
  type Incoming,
  Message,
  type MessageReference,
  type Outgoing,
  ReceivedRequest,
  type Request,
  RequestOperation,
  type ResultSources,
} from "../communication/service.js";
import { executeQuery, type Generated, makeQueries } from "../database/sql.js";
import { withAgentTransaction } from "../database/transactions.js";
import {
  canonicalBytes,
  decodeCanonical,
  encodeCanonicalJson,
  exactStruct,
  MAXIMUM_RUNTIME_ITEM_BYTES,
  sameBytes,
} from "../encoding/canonical-json.js";
import { AgentCard, agentCardOf } from "../identity/agent-cards.js";
import { Identity } from "../identity/service.js";
import { exitOutcome, markRetries } from "../tracing.js";
import {
  type Message as AssessmentText,
  checkCapturedMessage,
  type CheckedGuidance,
  checkedGuidanceSchema,
  CheckInputError,
  decidingRules,
  type DecisionProvider,
  DecisionProviderError,
  decodeFrozen,
  type FirewallCheck,
  freezeData,
  type HistoryEntry,
  type MessageContext,
  projectFeedback,
  textQuestionEdition,
  validateChecks,
} from "./check.js";
import { makeJevDecisionProvider } from "./jev.js";
import {
  decodeBehavior,
  editableDocumentSchema,
  type FirewallConfiguration,
  firewallConfigurationSchema,
  type firewallDecisionsSchema,
  firewallDecisionSummarySchema,
  FirewallPolicyError,
  firewallRefusalReasonSchema,
  FirewallSettings,
  type LoadedMessagePolicy,
  MAXIMUM_BEHAVIOR_BYTES,
  MAXIMUM_EDITABLE_RULES,
  type nextActionSchema,
  type PolicyProblem,
  type policyReadSchema,
  type policyUpdateResultSchema,
  type policyUpdateSchema,
  TextPolicySettings,
  TextRule,
} from "./rules.js";

/** Provider resources remain fixed while authorized edits change text rules. */
export interface FirewallOptions {
  /**
   * Guidance admits messages despite reject or hold judgments, including provider
   * failures. Enforcement refuses sends or withholds incoming text for those judgments.
   */
  readonly mode: "guidance" | "enforcement";
  readonly settings: FirewallSettings;
  readonly facts: MessageContext["facts"];
  readonly maximumHistoryRecords?: number;
  readonly maximumConcurrentChecks?: number;
  /** Host approval completes before an edit can replace the active policy. */
  readonly approvePolicyUpdate?: (
    request: typeof policyUpdateSchema.Type,
  ) => Effect.Effect<void, FirewallPolicyError>;
  readonly text?: {
    readonly fixedRules?: readonly TextRule[];
    readonly initialRules?: readonly TextRule[];
    readonly provider: DecisionProvider;
    readonly settings: TextPolicySettings;
  };
}

type Policy = Effect.Effect.Success<ReturnType<typeof initialPolicy>>;

type ReplyResult = Extract<Incoming, { readonly kind: "result" }>;
type Direction = "incoming" | "outgoing";

/** Carries bounded preceding history and each message's visibility to the agent. */
export interface FirewallEvidence {
  readonly history: ReadonlyArray<{
    readonly message: Message;
    readonly visibleToAgent: boolean;
  }>;
  readonly earlierHistoryOmitted: boolean;
  readonly gaps: readonly string[];
}

/** Peer questions and selected peer answers retain their own preceding history. */
export interface ResultEvidence {
  readonly question?: FirewallEvidence;
  readonly answers: ReadonlyArray<{
    readonly message: MessageReference;
    readonly evidence: FirewallEvidence;
  }>;
}

/* eslint-disable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Schema values share their domain type names. */

/** A result part the local policy prevents the agent from reading. */
export const Withheld = exactStruct({
  kind: Schema.Literal("withheld"),
  checkId: checkedGuidanceSchema.fields.checkId,
  reason: firewallRefusalReasonSchema,
});
/** @useDeclaredType */
export type Withheld = typeof Withheld.Type;

const resultGuidance = exactStruct({
  question: Schema.optionalWith(checkedGuidanceSchema, { exact: true }),
  answers: Schema.Array(
    exactStruct({ member: AgentCard, ...checkedGuidanceSchema.fields }),
  ),
});

/** Local instructions for one message, or for the question and answers in a result. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- Inbox persistence and MCP discovery compose this owning semantic schema.
export const LocalGuidance = Schema.Union(
  checkedGuidanceSchema,
  resultGuidance,
);
/** @useDeclaredType */
export type LocalGuidance = typeof LocalGuidance.Type;

/** A request result after applying local policy to each peer-controlled part. */
export const CollectiveResult = exactStruct({
  kind: Schema.Literal("collectiveResult"),
  op: RequestOperation,
  operationId: CollectiveId,
  closedAt: Schema.Number,
  closedBy: Schema.Literal("settled", "deadline"),
  question: Schema.Union(Schema.String, Withheld),
  outcomes: Schema.NonEmptyArray(
    exactStruct({
      member: AgentCard,
      outcome: Schema.Union(AnswerOutcome, Withheld),
    }),
  ),
});
/** @useDeclaredType */
export type CollectiveResult = typeof CollectiveResult.Type;

/** The exact content local policy permits the agent to receive. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- Inbox persistence and MCP discovery compose this owning semantic schema.
export const PermittedItem = Schema.Union(
  exactStruct({ kind: Schema.Literal("multicast"), message: Message }),
  exactStruct({
    kind: Schema.Literal("collectiveRequest"),
    message: ReceivedRequest,
  }),
  CollectiveResult,
  exactStruct({
    kind: Schema.Literal("operationFailed"),
    operationId: CollectiveId,
    error: Schema.String,
  }),
);
/** @useDeclaredType */
export type PermittedItem = typeof PermittedItem.Type;

/* eslint-enable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Restore naming after schema declarations. */

/** The sender can act on registered feedback without seeing assessment evidence. */
export class FirewallRefusal extends Data.TaggedError("FirewallRefusal")<{
  readonly reason: "firewall-refused";
  readonly checkId: string;
  readonly refusal: typeof firewallRefusalReasonSchema.Type;
  readonly feedback: ReadonlyArray<{
    readonly nextAction: typeof nextActionSchema.Type;
    readonly text: string;
  }>;
}> {
  override get message(): string {
    const feedback = this.feedback
      .map((entry) => `${entry.nextAction}: ${entry.text}`)
      .join(" ");
    return `Message refused by local policy (${this.refusal}, check ${this.checkId}). ${feedback}`;
  }
}

/**
 * Run `save` before returning refusal or submitting this exact message.
 * Saving is repeatable and uses the caller's current SQL transaction.
 */
type OutgoingDecision = {
  readonly save: Effect.Effect<void, FirewallPolicyError>;
} & (
  | { readonly kind: "allowed"; readonly guidance?: CheckedGuidance }
  | { readonly kind: "refused"; readonly refusal: FirewallRefusal }
);

/**
 * Run `save` in the same SQL transaction as the inbox write and visibility update.
 * Saving is repeatable.
 */
export type IncomingDecision = {
  readonly save: Effect.Effect<void, FirewallPolicyError>;
  readonly visibility: ReadonlyArray<{
    readonly message: MessageReference;
    readonly visibleToAgent: boolean;
  }>;
} & (
  | {
      readonly kind: "allowed";
      readonly item: PermittedItem;
      readonly guidance?: LocalGuidance;
    }
  | { readonly kind: "withheld" }
);
/** Message assessment, policy edits and saved decision summaries. */
interface FirewallService {
  /** Assesses without saving. Refusals expose feedback. Allowed work may carry guidance. */
  readonly checkOutgoing: (
    value: Outgoing,
    evidence: FirewallEvidence,
  ) => Effect.Effect<OutgoingDecision, CheckInputError>;
  /**
   * Assesses each peer-controlled part under one policy with its preceding evidence.
   * Withheld items expose no guidance.
   */
  readonly checkIncoming: (
    value: Incoming,
    evidence?: FirewallEvidence | ResultEvidence,
  ) => Effect.Effect<IncomingDecision, CheckInputError>;
  readonly readPolicy: () => Effect.Effect<
    typeof policyReadSchema.Type,
    FirewallPolicyError
  >;
  /**
   * Validates and approves each edit before saving and activating it.
   * Retries validate again and may repeat approval.
   */
  readonly updatePolicy: (
    request: typeof policyUpdateSchema.Type,
  ) => Effect.Effect<typeof policyUpdateResultSchema.Type, FirewallPolicyError>;
  readonly readDecisions: (request: {
    readonly since?: number;
  }) => Effect.Effect<typeof firewallDecisionsSchema.Type, FirewallPolicyError>;
}

/** One local policy owns assessment, advice, disclosure and completed decisions. */
export class Firewall extends Context.Tag("SocialHarness/Firewall")<
  Firewall,
  FirewallService
>() {
  /** Policy resources are fixed for this scope. Registration supplies immutable local attribution. */
  static readonly layer = (
    agentKey: string,
    source?: FirewallOptions | LoadedMessagePolicy,
  ) =>
    Layer.effect(
      Firewall,
      Effect.gen(function* () {
        const identity = yield* Identity;
        const store = yield* makeFirewallStore(agentKey);
        const policy =
          source === undefined
            ? undefined
            : yield* initialPolicy(store, source);
        const localAgent = Effect.gen(function* () {
          const state = yield* identity.me();
          if (state.kind !== "registered") {
            return yield* new CheckInputError({ reason: "context" });
          }
          return agentCardOf(state.identityCard);
        });
        return Firewall.of({
          readPolicy: () =>
            policy === undefined
              ? Effect.fail(
                  new FirewallPolicyError({ reason: "not-configured" }),
                )
              : Effect.sync(() => ({
                  rules: policy.active.editableRules,
                  fixedRules: policy.options.text?.fixedRules ?? [],
                  activeRevision: policy.active.configurationDigest,
                })),
          updatePolicy: (request) =>
            policy === undefined
              ? Effect.fail(
                  new FirewallPolicyError({ reason: "not-configured" }),
                )
              : updatePolicy(policy, request),
          readDecisions: (request) => store.readDecisions(request),
          checkOutgoing: (value, evidence) =>
            Effect.gen(function* () {
              if (policy === undefined) {
                return {
                  kind: "allowed",
                  save: Effect.void,
                } satisfies OutgoingDecision;
              }
              const agent = yield* localAgent;
              const [checked] = yield* assess(
                policy,
                agent,
                [{ value, evidence }],
                "outgoing",
              );
              if (checked === undefined) {
                return yield* new CheckInputError({ reason: "message" });
              }
              const save = store.saveDecisions([checked.completion]);
              return checked.completion.admit
                ? ({
                    kind: "allowed",
                    save,
                    ...(checked.guidance === undefined
                      ? {}
                      : { guidance: checked.guidance }),
                  } satisfies OutgoingDecision)
                : ({
                    kind: "refused",
                    save,
                    refusal: new FirewallRefusal({
                      reason: "firewall-refused",
                      checkId: checked.completion.checkId,
                      refusal: refusalReason(checked.result.reason),
                      feedback: checked.result.feedback,
                    }),
                  } satisfies OutgoingDecision);
            }),
          checkIncoming: (value, evidence) =>
            Effect.gen(function* () {
              if (value.kind === "request-failed") {
                return {
                  kind: "allowed",
                  item: {
                    kind: "operationFailed",
                    operationId: value.request.operationId,
                    error: value.error,
                  },
                  save: Effect.void,
                  visibility: [],
                } satisfies IncomingDecision;
              }
              const agent = yield* localAgent;
              if (value.kind === "result") {
                return yield* checkResult(
                  store,
                  agent,
                  value,
                  evidence,
                  policy,
                );
              }
              if (evidence === undefined || "answers" in evidence) {
                return yield* new CheckInputError({ reason: "context" });
              }
              const item: PermittedItem =
                value.kind === "request"
                  ? { kind: "collectiveRequest", message: value.message }
                  : { kind: "multicast", message: value.message };
              const reference = messageReference(value.message);
              if (policy === undefined) {
                return {
                  kind: "allowed",
                  item,
                  save: Effect.void,
                  visibility: [{ message: reference, visibleToAgent: true }],
                } satisfies IncomingDecision;
              }
              const [checked] = yield* assess(
                policy,
                agent,
                [{ value: value.message, evidence }],
                "incoming",
              );
              if (checked === undefined) {
                return yield* new CheckInputError({ reason: "message" });
              }
              const save = store.saveDecisions([checked.completion]);
              const visibility = [
                {
                  message: reference,
                  visibleToAgent: checked.completion.admit,
                },
              ];
              return checked.completion.admit
                ? ({
                    kind: "allowed",
                    item,
                    save,
                    visibility,
                    ...(checked.guidance === undefined
                      ? {}
                      : { guidance: checked.guidance }),
                  } satisfies IncomingDecision)
                : ({
                    kind: "withheld",
                    save,
                    visibility,
                  } satisfies IncomingDecision);
            }),
        });
      }),
    );
}

interface Candidate {
  readonly value: Outgoing;
  readonly evidence: FirewallEvidence;
  readonly text?: string;
}

interface RuleCheckInput {
  readonly direction: "incoming" | "outgoing";
  readonly canonicalCandidate: Uint8Array;
  readonly message: AssessmentText;
  readonly context: MessageContext;
  readonly agents: readonly AgentCard[];
}

interface RuleDecision {
  readonly completion: FirewallCompletion;
  readonly result: FirewallCheck;
  readonly guidance?: CheckedGuidance;
}

const storedPolicyFormat = "org.social-harness/text-rules/v1";

const storedPolicySchema = exactStruct({
  format: Schema.Literal(storedPolicyFormat),
  rules: editableDocumentSchema,
});

const persistenceFailure = () =>
  new FirewallPolicyError({ reason: "persistence-failed" });

type PolicyEditPath = NonNullable<FirewallPolicyError["path"]>;

type CapturedOptions = Effect.Effect.Success<ReturnType<typeof captureOptions>>;

type ActiveRules = Effect.Effect.Success<ReturnType<typeof buildPolicy>>;

interface RuleChange {
  readonly add: typeof editableDocumentSchema.Type;
  readonly remove: readonly string[];
}

type CapturedCheck = Effect.Effect.Success<ReturnType<typeof capturePrepared>>;

type DecisionSummary = typeof firewallDecisionSummarySchema.Type;

function checkResult(
  store: FirewallStore,
  self: AgentCard,
  incoming: ReplyResult,
  evidence?: FirewallEvidence | ResultEvidence,
  policy?: Policy,
) {
  return Effect.gen(function* () {
    const { result, sources } = incoming;
    const peers = sources.answers.filter(
      (message) => message.author.agentId !== self.agentId,
    );
    const question =
      sources.request?.author.agentId === self.agentId
        ? undefined
        : sources.request;
    const selected: Array<
      Candidate & {
        readonly message: Message;
        readonly part: "question" | "answer";
      }
    > = [];
    if (policy !== undefined) {
      if (
        evidence === undefined ||
        !("answers" in evidence) ||
        evidence.answers.length !== peers.length
      ) {
        return yield* new CheckInputError({ reason: "context" });
      }
      const found = new Set<string>();
      for (const message of peers) {
        const window = evidence.answers.find((entry) =>
          sameReference(entry.message, messageReference(message)),
        );
        const key = `${message.author.agentId}:${message.msgId}`;
        if (window === undefined || found.has(key)) {
          return yield* new CheckInputError({ reason: "context" });
        }
        found.add(key);
        const outcome = result.outcomes.find(
          (entry) => entry.member.agentId === message.author.agentId,
        );
        if (outcome === undefined) {
          return yield* new CheckInputError({ reason: "message" });
        }
        selected.push({
          value: message,
          message,
          part: "answer",
          evidence: window.evidence,
          text: outcomeText(outcome.outcome),
        });
      }
      if (question !== undefined) {
        if (evidence.question === undefined) {
          return yield* new CheckInputError({ reason: "context" });
        }
        selected.push({
          value: question,
          message: question,
          part: "question",
          evidence: evidence.question,
          text: JSON.stringify(result.request.question),
        });
      }
    }
    const checks =
      selected.length === 0 || policy === undefined
        ? []
        : yield* assess(policy, self, selected, "incoming");
    const checked = selected.map((source, index) => ({
      source,
      check: checks[index],
    }));
    if (checked.some((entry) => entry.check === undefined)) {
      return yield* new CheckInputError({ reason: "context" });
    }
    const questionCheck = checked.find(
      (entry) => entry.source.part === "question",
    )?.check;
    const permittedOutcome = (
      outcome: (typeof result.outcomes)[number],
    ): CollectiveResult["outcomes"][number] => {
      const check = checked.find(
        (entry) =>
          entry.source.part === "answer" &&
          entry.source.message.author.agentId === outcome.member.agentId,
      )?.check;
      return check !== undefined && !check.completion.admit
        ? { member: outcome.member, outcome: withheld(check) }
        : outcome;
    };
    const [first, ...rest] = result.outcomes;
    const item: CollectiveResult = {
      kind: "collectiveResult",
      op: result.request.op,
      operationId: result.request.operationId,
      closedAt: result.closedAt,
      closedBy: result.closedBy,
      question:
        questionCheck !== undefined && !questionCheck.completion.admit
          ? withheld(questionCheck)
          : result.request.question,
      outcomes: [permittedOutcome(first), ...rest.map(permittedOutcome)],
    };
    const answers = checked.flatMap(({ source, check }) =>
      source.part === "answer" && check?.guidance !== undefined
        ? [{ member: source.message.author, ...check.guidance }]
        : [],
    );
    const guidance: LocalGuidance | undefined =
      answers.length === 0 && questionCheck?.guidance === undefined
        ? undefined
        : {
            answers,
            ...(questionCheck?.guidance === undefined
              ? {}
              : { question: questionCheck.guidance }),
          };
    return {
      kind: "allowed",
      item,
      visibility: resultVisibility(item, sources),
      save: store.saveDecisions(checks.map((check) => check.completion)),
      ...(guidance === undefined ? {} : { guidance }),
    } satisfies IncomingDecision;
  });
}

function assess(
  policy: Policy,
  self: AgentCard,
  candidates: readonly Candidate[],
  direction: Direction,
) {
  return Effect.gen(function* () {
    const now = yield* Clock.currentTimeMillis;
    const inputs: RuleCheckInput[] = [];
    for (const candidate of candidates) {
      const cards = new Map<string, AgentCard>();
      for (const card of [
        self,
        candidate.value.author,
        ...candidate.value.recipients,
        ...candidate.evidence.history.flatMap((entry) => [
          entry.message.author,
          ...entry.message.recipients,
        ]),
      ]) {
        cards.set(card.agentId, card);
      }
      const history: HistoryEntry[] = candidate.evidence.history.map(
        (entry) => ({
          // eslint-disable-next-line @typescript-eslint/no-misused-spread -- assessmentText returns plain fields with no prototype methods.
          ...assessmentText(entry.message),
          visibleToAgent:
            entry.visibleToAgent ||
            entry.message.author.agentId === self.agentId,
        }),
      );
      const context: MessageContext = {
        agentId: self.agentId,
        principal: self.principalId,
        participants: [...cards.values()].map((card) => ({
          agentId: card.agentId,
          principal: card.principalId,
        })),
        history,
        earlierHistoryOmitted: candidate.evidence.earlierHistoryOmitted,
        now,
        facts: policy?.options.configuration.facts ?? [],
        gaps: candidate.evidence.gaps,
      };
      inputs.push({
        direction,
        canonicalCandidate: yield* encodeCanonicalJson(candidate.value),
        message: {
          // eslint-disable-next-line @typescript-eslint/no-misused-spread -- assessmentText returns plain fields with no prototype methods.
          ...assessmentText(candidate.value),
          ...(candidate.text === undefined ? {} : { text: candidate.text }),
        },
        context,
        agents: [...cards.values()],
      });
    }
    const active = policy.active;
    return yield* Effect.forEach(
      inputs,
      (input) =>
        policy.permits.withPermits(1)(
          assessRule(policy.options, active, input),
        ),
      { concurrency: policy.options.configuration.maximumConcurrentChecks },
    );
  }).pipe(
    Effect.catchTag("CanonicalJsonError", () =>
      Effect.fail(new CheckInputError({ reason: "context" })),
    ),
  );
}

function assessmentText(value: Outgoing): AssessmentText {
  const [first, ...rest] = value.recipients;
  const participants: Pick<AssessmentText, "sender" | "recipients"> = {
    sender: value.author.agentId,
    recipients: [first.agentId, ...rest.map((card) => card.agentId)],
  };
  if ("kind" in value) {
    return {
      id: referenceId(`${value.author.agentId}:${value.operationId}`),
      ...participants,
      text: questionText(value),
      operation: {
        kind: "request",
        op: value.op,
        operationId: value.operationId,
        deadlineAt: value.deadlineAt,
      },
    };
  }
  const base = {
    id: referenceId(`${value.author.agentId}:${value.msgId}`),
    ...participants,
  };
  const operation = value.operation;
  if (operation?.kind === "request") {
    return {
      ...base,
      text: questionText({ ...value, ...operation }),
      operation: {
        kind: "request",
        op: operation.op,
        operationId: operation.operationId,
        deadlineAt: operation.deadlineAt,
      },
    };
  }
  if (operation?.kind === "response") {
    return {
      ...base,
      text:
        operation.action === "accept"
          ? `answered ${JSON.stringify(operation.content)}`
          : "declined",
      operation: {
        kind: "answer",
        op: value.recipients.length === 1 ? "gather" : "all_gather",
        operationId: operation.operationId,
      },
    };
  }
  return {
    ...base,
    text: value.content
      .map((part) =>
        part.type === "text" ? part.text : JSON.stringify(part.value),
      )
      .join("\n"),
  };
}

function questionText(
  value: Pick<
    Request,
    | "author"
    | "recipients"
    | "question"
    | "op"
    | "deadlineAt"
    | "requestedSchema"
  >,
) {
  return [
    `${value.op} from ${value.author.agentName} to ${value.recipients.map((agent) => agent.agentName).join(", ")}, open until ${new Date(value.deadlineAt).toISOString()}.`,
    `Question: ${value.question}`,
    `Form: ${JSON.stringify(value.requestedSchema)}`,
  ].join("\n");
}

function resultVisibility(
  item: CollectiveResult,
  sources: ResultSources,
): IncomingDecision["visibility"] {
  return [
    ...sources.answers.map((message) => ({
      message: messageReference(message),
      visibleToAgent: item.outcomes.some(
        (entry) =>
          entry.member.agentId === message.author.agentId &&
          (entry.outcome.kind === "answered" ||
            entry.outcome.kind === "declined"),
      ),
    })),
    ...(sources.request === undefined
      ? []
      : [
          {
            message: messageReference(sources.request),
            visibleToAgent: typeof item.question === "string",
          },
        ]),
  ];
}

function outcomeText(outcome: typeof AnswerOutcome.Type): string {
  switch (outcome.kind) {
    case "answered":
      return `answered ${JSON.stringify(outcome.content)}`;
    case "declined":
      return "declined";
    case "invalid":
      return `answered outside the form (${outcome.reason})`;
    case "no-answer":
      return "no answer";
    default: {
      const exhaustive: never = outcome;
      return exhaustive;
    }
  }
}

function withheld(check: RuleDecision): Withheld {
  return {
    kind: "withheld",
    checkId: check.completion.checkId,
    reason: refusalReason(check.result.reason),
  };
}

function refusalReason(
  reason: RuleDecision["result"]["reason"],
): typeof firewallRefusalReasonSchema.Type {
  return reason === "eligible" ? "unresolved-evidence" : reason;
}

function messageReference(message: Message): MessageReference {
  return { authorAgentId: message.author.agentId, msgId: message.msgId };
}

function sameReference(
  left: MessageReference,
  right: MessageReference,
): boolean {
  return (
    left.authorAgentId === right.authorAgentId && left.msgId === right.msgId
  );
}

function referenceId(value: string): string {
  return `msg_${createHash("sha256").update(value).digest("hex")}`;
}

function initialPolicy(
  store: FirewallStore,
  source: FirewallOptions | LoadedMessagePolicy,
) {
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
    const { stored, rules } = yield* loadRules(store, captured);
    yield* validateIds(
      rules,
      fixed.map((rule) => rule.id),
    );
    const active = yield* buildPolicy(captured, rules);
    if (stored === undefined) {
      yield* persistRules(store, rules);
    }
    return {
      store,
      options: captured,
      active,
      edits: yield* Effect.makeSemaphore(1),
      permits: yield* Effect.makeSemaphore(
        captured.configuration.maximumConcurrentChecks,
      ),
    };
  });
}

function loadRules(store: FirewallStore, options: CapturedOptions) {
  return Effect.gen(function* () {
    const stored = yield* store.readPolicy();
    const rules =
      stored === undefined
        ? (options.text?.initialRules ?? [])
        : (yield* decodeCanonical(storedPolicySchema, stored).pipe(
            Effect.mapError(persistenceFailure),
          )).rules;
    freezeData(rules);
    return { stored, rules };
  });
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
    const approvePolicyUpdate =
      options.approvePolicyUpdate?.bind(options) ?? (() => Effect.void);
    return { configuration, settings, text, approvePolicyUpdate };
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

/** Completed checks carry the exact policy and evidence that delivery commits. */
function assessRule(
  options: CapturedOptions,
  active: ActiveRules,
  requested: RuleCheckInput,
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
        canonicalResult: yield* canonicalBytes({
          result: Struct.omit(projection.result, "trace"),
          summaries: yield* decisionSummaries(active, input, projection.result),
        }),
      },
      ...(admit && projection.guidance !== undefined
        ? { guidance: projection.guidance }
        : {}),
    } satisfies RuleDecision;
  }).pipe(
    Effect.catchTag("ClientRepresentationError", () =>
      Effect.fail(new CheckInputError({ reason: "context" })),
    ),
    Effect.withSpan("assessMessage"),
  );
}

/** The evaluator and retained decision receive the same detached message and context. */
function capturePrepared(
  configuration: FirewallConfiguration,
  input: RuleCheckInput,
) {
  return Effect.gen(function* () {
    const captured = yield* Effect.try({
      try: () =>
        structuredClone({
          direction: input.direction,
          canonicalCandidate: input.canonicalCandidate,
          evidence: {
            event: { direction: input.direction, message: input.message },
            context: Object.assign({}, input.context, {
              facts: configuration.facts,
            }),
          },
          agents: input.agents,
        }),
      catch: () => new CheckInputError({ reason: "context" }),
    });
    freezeData(captured.evidence);
    freezeData(captured.agents);
    return captured;
  });
}

function evaluateMessage(
  options: CapturedOptions,
  active: ActiveRules,
  input: CapturedCheck,
) {
  const evidence = input.evidence;
  return checkCapturedMessage(
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
    { event: evidence.event, context: evidence.context },
  ).pipe(
    Effect.catchTag("CheckInputError", () => Effect.succeed(unavailable())),
  );
}

function retainedInput(active: ActiveRules, input: CapturedCheck) {
  return canonicalBytes({
    configurationDigest: active.configurationDigest,
    installation: active.metadata,
    context: input.evidence.context,
    agents: input.agents,
    message: input.evidence.event.message,
  });
}

/** Final public attribution uses the same captured rules and result as admission. */
function decisionSummaries(
  active: ActiveRules,
  input: CapturedCheck,
  result: FirewallCheck,
): Effect.Effect<readonly DecisionSummary[], CheckInputError> {
  const deciding = decidingRules(result.reason, active.rules, result);
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
  const message = input.evidence.event.message;
  const peers =
    input.direction === "outgoing" ? message.recipients : [message.sender];
  return Effect.forEach(
    peers,
    (identity) => {
      const peer = input.agents.find((agent) => agent.agentId === identity);
      if (peer === undefined) {
        return Effect.fail(new CheckInputError({ reason: "context" }));
      }
      return Effect.succeed({
        direction: input.direction,
        peer,
        outcome: result.judgment,
        reason: result.reason,
        decidedBy,
        checkedRuleIds,
        activeRevision: active.configurationDigest,
      });
    },
    { concurrency: 1 },
  );
}

/** A local reference correlates an assessment without granting lookup authority. */
function mintCheckId() {
  return `chk_${randomBytes(32).toString("hex")}`;
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
    const bytes = yield* canonicalBytes(guidance);
    return subjects.length !== selected.size ||
      bytes.byteLength > MAXIMUM_RUNTIME_ITEM_BYTES
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

function guidanceSubjects(
  selected: ReadonlySet<string>,
  input: CapturedCheck,
): CheckedGuidance["subjects"] {
  const agents = new Map<string, AgentCard>(
    input.agents.map((agent) => [agent.agentId, agent]),
  );
  const available = input.evidence.context.history.filter(
    (entry) => entry.visibleToAgent,
  );
  const message = input.evidence.event.message;
  const subjects = new Map(
    [...available, ...(message === undefined ? [] : [message])].map((entry) => [
      entry.id,
      entry,
    ]),
  );
  return [...subjects.values()]
    .filter((entry) => selected.has(entry.id))
    .flatMap((entry) => {
      const sender = agents.get(entry.sender);
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

function updatePolicy(policy: Policy, request: typeof policyUpdateSchema.Type) {
  return policy.edits.withPermits(1)(
    Effect.gen(function* () {
      const { request: captured, change } = yield* resolvedChange(request);
      const next = yield* editedRules(
        policy.active.editableRules,
        policy.active.fixedRuleIds,
        change,
      );
      const replacement = yield* buildPolicy(policy.options, next);
      yield* policy.options.approvePolicyUpdate(captured);
      return yield* Effect.uninterruptible(
        Effect.gen(function* () {
          yield* persistRules(policy.store, next, {
            time: yield* Clock.currentTimeMillis,
            activeRevision: replacement.configurationDigest,
            addedRuleIds: change.add.map((rule) => rule.id),
            removedRuleIds: change.remove,
          });
          yield* Effect.sync(() => {
            policy.active = replacement;
          });
          return { activeRevision: replacement.configurationDigest };
        }),
      );
    }),
  );
}

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
      ClientRepresentationError: () =>
        Effect.fail(new FirewallPolicyError({ reason: "build-failed" })),
    }),
    Effect.catchAllDefect(() =>
      Effect.fail(new FirewallPolicyError({ reason: "build-failed" })),
    ),
  );
}

function digest(value: unknown) {
  return canonicalBytes(value).pipe(
    Effect.map((bytes) => createHash("sha256").update(bytes).digest("hex")),
  );
}

function persistRules(
  store: FirewallStore,
  rules: readonly TextRule[],
  activation?: FirewallActivation,
) {
  return canonicalBytes({ format: storedPolicyFormat, rules }).pipe(
    Effect.flatMap((bytes) => store.writePolicy(bytes, activation)),
    Effect.catchTag("ClientRepresentationError", () =>
      Effect.fail(persistenceFailure()),
    ),
  );
}

/** Maps each `{behavior}` to a rule and reports invalid behavior at its index in `add`. */
function resolvedChange(change: typeof policyUpdateSchema.Type) {
  return Effect.gen(function* () {
    const captured = yield* Effect.try({
      try: () => structuredClone(change),
      catch: () => invalidRule("schema"),
    });
    freezeData(captured);
    const add = yield* Effect.forEach(
      captured.add ?? [],
      (item, index) =>
        "behavior" in item
          ? behaviorRule(item.behavior, ["add", index])
          : Effect.succeed(item),
      { concurrency: 1 },
    );
    return {
      request: captured,
      change: { add, remove: captured.remove ?? [] } satisfies RuleChange,
    };
  });
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

function invalidRule(
  problem: PolicyProblem,
  at: { readonly path?: PolicyEditPath; readonly ruleId?: string } = {},
) {
  return new FirewallPolicyError({ reason: "invalid-rule", problem, ...at });
}

const bytes = Schema.Uint8ArrayFromSelf.pipe(
  Schema.filter((value) => value.byteLength > 0),
);
const time = Schema.Int.pipe(
  Schema.nonNegative(),
  Schema.lessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
);
const ruleIds = Schema.parseJson(Schema.Array(Schema.NonEmptyString));
const storedTime = Schema.Union(
  time,
  Schema.NumberFromString.pipe(
    Schema.int(),
    Schema.nonNegative(),
    Schema.lessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
  ),
);
const activationRow = Schema.Struct({
  time: storedTime,
  activeRevision: Schema.NonEmptyString,
  addedRuleIds: ruleIds,
  removedRuleIds: ruleIds,
});
const disposition = Schema.Literal("admitted", "withheld");
const completedCheck = Schema.Struct({
  checkId: Schema.NonEmptyString,
  direction: Schema.Literal("incoming", "outgoing"),
  canonicalCandidate: bytes,
  canonicalInput: bytes,
  canonicalResult: bytes,
  admit: Schema.Boolean,
});
const storedCheck = Schema.Struct({
  direction: completedCheck.fields.direction,
  canonicalCandidate: bytes,
  canonicalInput: bytes,
  canonicalResult: bytes,
  disposition,
});
const decisionRow = Schema.Struct({
  checkId: Schema.NonEmptyString,
  canonicalResult: bytes,
  disposition,
  time: storedTime,
});
const savedSummary = Schema.parseJson(
  Schema.Struct({ summaries: Schema.Array(firewallDecisionSummarySchema) }),
);
const decoder = new TextDecoder("utf-8", { fatal: true });

type FirewallActivation = typeof activationRow.Type;
type FirewallCompletion = typeof completedCheck.Type;

type FirewallStore = Effect.Effect.Success<
  ReturnType<typeof makeFirewallStore>
>;

/* eslint-disable @typescript-eslint/naming-convention -- These properties name physical SQL tables and columns. */
interface FirewallTables {
  readonly firewall_policy: {
    readonly agent_key: string;
    readonly canonical_rules: Uint8Array;
  };
  readonly firewall_checks: {
    readonly agent_key: string;
    readonly sequence: Generated<number>;
    readonly check_id: string;
    readonly direction: typeof completedCheck.Type.direction;
    readonly canonical_candidate: Uint8Array;
    readonly canonical_input: Uint8Array;
    readonly canonical_result: Uint8Array;
    readonly decided_at: number;
    readonly disposition: typeof disposition.Type;
  };
  readonly firewall_activations: {
    readonly agent_key: string;
    readonly sequence: Generated<number>;
    readonly activated_at: number;
    readonly active_revision: string;
    readonly added_rule_ids: string;
    readonly removed_rule_ids: string;
  };
}

/* eslint-enable @typescript-eslint/naming-convention -- Restore naming outside physical SQL types. */

/** Captured ownership scopes policy, evidence and activations on every database operation. */
function makeFirewallStore(agentKey: string) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const queries = yield* makeQueries<FirewallTables>();
    const run = <A, E, R>(work: Effect.Effect<A, E, R>) =>
      withAgentTransaction(agentKey, work).pipe(
        Effect.provideService(SqlClient.SqlClient, sql),
        Effect.mapError(persistenceFailure),
      );
    const policy = queries
      .selectFrom("firewall_policy")
      .select("canonical_rules as rules")
      .where("agent_key", "=", agentKey);
    const checks = queries
      .selectFrom("firewall_checks")
      .where("agent_key", "=", agentKey);
    const activations = queries
      .selectFrom("firewall_activations")
      .where("agent_key", "=", agentKey);
    const saveDecision = (value: FirewallCompletion) =>
      Effect.gen(function* () {
        const applied = value.admit ? "admitted" : "withheld";
        yield* executeQuery(
          queries
            .insertInto("firewall_checks")
            .values({
              agent_key: agentKey,
              check_id: value.checkId,
              direction: value.direction,
              canonical_candidate: value.canonicalCandidate,
              canonical_input: value.canonicalInput,
              canonical_result: value.canonicalResult,
              disposition: applied,
              decided_at: yield* Clock.currentTimeMillis,
            })
            .onConflict((conflict) =>
              conflict.columns(["agent_key", "check_id"]).doNothing(),
            ),
        );
        const prior = yield* executeQuery(
          checks
            .select([
              "direction",
              "canonical_candidate as canonicalCandidate",
              "canonical_input as canonicalInput",
              "canonical_result as canonicalResult",
              "disposition",
            ])
            .where("check_id", "=", value.checkId),
        ).pipe(Effect.flatMap(Schema.decodeUnknown(Schema.Array(storedCheck))));
        const saved = prior[0];
        if (saved === undefined) {
          return yield* persistenceFailure();
        }
        const sameEvidence =
          sameBytes(saved.canonicalCandidate, value.canonicalCandidate) &&
          sameBytes(saved.canonicalInput, value.canonicalInput) &&
          sameBytes(saved.canonicalResult, value.canonicalResult);
        if (
          saved.direction !== value.direction ||
          saved.disposition !== applied ||
          !sameEvidence
        ) {
          return yield* persistenceFailure();
        }
      });
    return {
      readPolicy: () =>
        run(
          executeQuery(policy).pipe(
            Effect.flatMap(
              Schema.decodeUnknown(
                Schema.Array(Schema.Struct({ rules: bytes })),
              ),
            ),
            Effect.map((rows) => rows[0]?.rules),
          ),
        ),
      writePolicy: (rules: Uint8Array, activation?: FirewallActivation) =>
        run(
          Effect.gen(function* () {
            yield* executeQuery(
              queries
                .insertInto("firewall_policy")
                .values({
                  agent_key: agentKey,
                  canonical_rules: rules,
                })
                .onConflict((conflict) =>
                  conflict
                    .column("agent_key")
                    .doUpdateSet({ canonical_rules: rules }),
                ),
            );
            if (activation !== undefined) {
              yield* executeQuery(
                queries.insertInto("firewall_activations").values({
                  agent_key: agentKey,
                  activated_at: activation.time,
                  active_revision: activation.activeRevision,
                  added_rule_ids: yield* Schema.encode(ruleIds)(
                    activation.addedRuleIds,
                  ),
                  removed_rule_ids: yield* Schema.encode(ruleIds)(
                    activation.removedRuleIds,
                  ),
                }),
              );
            }
          }),
        ),
      saveDecisions: (decisions: readonly FirewallCompletion[]) =>
        run(
          Effect.forEach(decisions, saveDecision, {
            discard: true,
            concurrency: 1,
          }),
        ),
      readDecisions: (request: { readonly since?: number }) =>
        run(
          Effect.gen(function* () {
            const since = request.since ?? 0;
            const rows = yield* executeQuery(
              checks
                .select([
                  "check_id as checkId",
                  "canonical_result as canonicalResult",
                  "disposition",
                  "decided_at as time",
                ])
                .where("decided_at", ">=", since)
                .orderBy("decided_at", "desc")
                .orderBy("sequence", "desc")
                .limit(100),
            ).pipe(
              Effect.flatMap(Schema.decodeUnknown(Schema.Array(decisionRow))),
            );
            const summaries = yield* Effect.forEach(
              rows,
              decodeDecisionSummaries,
              { concurrency: 1 },
            );
            const edits = yield* executeQuery(
              activations
                .select([
                  "activated_at as time",
                  "active_revision as activeRevision",
                  "added_rule_ids as addedRuleIds",
                  "removed_rule_ids as removedRuleIds",
                ])
                .where("activated_at", ">=", since)
                .orderBy("activated_at", "desc")
                .orderBy("sequence", "desc")
                .limit(100),
            ).pipe(
              Effect.flatMap(Schema.decodeUnknown(Schema.Array(activationRow))),
            );
            return { decisions: summaries.flat(), activations: edits };
          }),
        ),
    };
  });
}

function decodeDecisionSummaries(row: typeof decisionRow.Type) {
  return Effect.try({
    try: () => decoder.decode(row.canonicalResult),
    catch: persistenceFailure,
  }).pipe(
    Effect.flatMap(Schema.decodeUnknown(savedSummary)),
    Effect.map(({ summaries }) =>
      summaries.map((summary) => ({
        ...summary,
        checkId: row.checkId,
        disposition: row.disposition,
        time: row.time,
      })),
    ),
  );
}
