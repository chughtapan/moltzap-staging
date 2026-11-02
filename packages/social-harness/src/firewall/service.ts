/** @file Applies the agent's policy to messages and returns the content it may send or read. */

import { SqlClient } from "@effect/sql";
import { Clock, Context, Data, Effect, Layer, Schema } from "effect";
import { createHash } from "node:crypto";
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
} from "../communication/index.js";
import { encodeCanonicalJson } from "../encoding/canonical-json.js";
import { exactStruct } from "../encoding/exact.js";
import { AgentCard, agentCardOf } from "../identity/agent.js";
import { Identity } from "../identity/service.js";
import {
  type Message as AssessmentText,
  type CheckedGuidance,
  checkedGuidanceSchema,
  CheckInputError,
  type HistoryEntry,
  type MessageContext,
} from "./check.js";
import {
  FirewallPolicyError,
  firewallRefusalReasonSchema,
  type LoadedMessagePolicy,
  type nextActionSchema,
} from "./rules/config.js";
import {
  type FirewallOptions,
  makeRules,
  type RuleCheckInput,
  type RuleDecision,
} from "./rules/service.js";
import { createFirewallTables, saveCheckDecision } from "./store.js";

type RuleEngine = Effect.Effect.Success<ReturnType<typeof makeRules>>;
type ReplyResult = Extract<Incoming, { readonly kind: "result" }>;
type Direction = "incoming" | "outgoing";

/** A bounded preceding history, with the read permission retained for each source. */
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

const completedDecision = Symbol("CompletedDecision");

/** Immutable completed assessment data. Pass it unchanged to saveDecisions. */
interface CompletedDecision {
  readonly checkId: string;
  readonly direction: Direction;
  /** @internal */
  readonly [completedDecision]: RuleDecision;
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

/** Policy permission applies to the immutable outgoing value supplied to the check. */
type OutgoingDecision = {
  readonly decisions: readonly CompletedDecision[];
} & (
  | { readonly kind: "allowed"; readonly guidance?: CheckedGuidance }
  | { readonly kind: "refused"; readonly refusal: FirewallRefusal }
);

/** Save this content, advice, source permissions and completed decisions together. */
export type IncomingDecision = {
  readonly decisions: readonly CompletedDecision[];
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
  /** Save returned decisions before returning refusal or submitting allowed work. */
  readonly checkOutgoing: (
    value: Outgoing,
    evidence: FirewallEvidence,
  ) => Effect.Effect<OutgoingDecision, CheckInputError>;
  /** A result uses one policy while each peer source retains its preceding context. */
  readonly checkIncoming: (
    value: Incoming,
    evidence?: FirewallEvidence | ResultEvidence,
  ) => Effect.Effect<IncomingDecision, CheckInputError>;
  /** Joins the current SQL transaction. Repeating a completed save has no effect. */
  readonly saveDecisions: (
    decisions: readonly CompletedDecision[],
  ) => Effect.Effect<void, FirewallPolicyError>;
  readonly readPolicy: RuleEngine["readPolicy"];
  readonly updatePolicy: RuleEngine["updatePolicy"];
  readonly readDecisions: RuleEngine["readDecisions"];
}

/** One local policy owns assessment, advice, disclosure and completed decisions. */
export class Firewall extends Context.Tag("SocialHarness/Firewall")<
  Firewall,
  FirewallService
>() {
  /** Policy resources are fixed for this scope. Registration supplies immutable local attribution. */
  static readonly layer = (source?: FirewallOptions | LoadedMessagePolicy) =>
    Layer.effect(
      Firewall,
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const identity = yield* Identity;
        yield* createFirewallTables();
        const rules = yield* makeRules(source);
        const localAgent = Effect.gen(function* () {
          const state = yield* identity.me();
          if (state.kind !== "registered") {
            return yield* new CheckInputError({ reason: "context" });
          }
          return agentCardOf(state.identityCard);
        });
        return Firewall.of({
          readPolicy: rules.readPolicy,
          updatePolicy: rules.updatePolicy,
          readDecisions: rules.readDecisions,
          checkOutgoing: (value, evidence) =>
            Effect.gen(function* () {
              if (rules.configuration === undefined) {
                return {
                  kind: "allowed",
                  decisions: [],
                } satisfies OutgoingDecision;
              }
              const agent = yield* localAgent;
              const [checked] = yield* assess(
                rules,
                agent,
                [{ value, evidence }],
                "outgoing",
              );
              if (checked === undefined) {
                return yield* new CheckInputError({ reason: "message" });
              }
              const decisions = [complete(checked)];
              return checked.completion.admit
                ? ({
                    kind: "allowed",
                    decisions,
                    ...(checked.guidance === undefined
                      ? {}
                      : { guidance: checked.guidance }),
                  } satisfies OutgoingDecision)
                : ({
                    kind: "refused",
                    decisions,
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
                  decisions: [],
                  visibility: [],
                } satisfies IncomingDecision;
              }
              const agent = yield* localAgent;
              if (value.kind === "result") {
                return yield* checkResult(rules, agent, value, evidence);
              }
              if (evidence === undefined || "answers" in evidence) {
                return yield* new CheckInputError({ reason: "context" });
              }
              const item: PermittedItem =
                value.kind === "request"
                  ? { kind: "collectiveRequest", message: value.message }
                  : { kind: "multicast", message: value.message };
              const reference = messageReference(value.message);
              if (rules.configuration === undefined) {
                return {
                  kind: "allowed",
                  item,
                  decisions: [],
                  visibility: [{ message: reference, visibleToAgent: true }],
                } satisfies IncomingDecision;
              }
              const [checked] = yield* assess(
                rules,
                agent,
                [{ value: value.message, evidence }],
                "incoming",
              );
              if (checked === undefined) {
                return yield* new CheckInputError({ reason: "message" });
              }
              const decisions = [complete(checked)];
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
                    decisions,
                    visibility,
                    ...(checked.guidance === undefined
                      ? {}
                      : { guidance: checked.guidance }),
                  } satisfies IncomingDecision)
                : ({
                    kind: "withheld",
                    decisions,
                    visibility,
                  } satisfies IncomingDecision);
            }),
          saveDecisions: (decisions) =>
            sql
              .withTransaction(
                Effect.forEach(
                  decisions,
                  (decision) =>
                    saveCheckDecision(decision[completedDecision].completion),
                  { discard: true, concurrency: 1 },
                ),
              )
              .pipe(
                Effect.provideService(SqlClient.SqlClient, sql),
                Effect.catchTag("SqlError", () =>
                  Effect.fail(
                    new FirewallPolicyError({ reason: "persistence-failed" }),
                  ),
                ),
              ),
        });
      }),
    );
}

interface Candidate {
  readonly value: Outgoing;
  readonly evidence: FirewallEvidence;
  readonly text?: string;
}

function assess(
  rules: RuleEngine,
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
          visibility: entry.visibleToAgent ? "runtime" : "assessor",
          admission:
            entry.visibleToAgent ||
            entry.message.author.agentId === self.agentId
              ? "admitted"
              : "withheld",
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
        facts: rules.configuration?.facts ?? [],
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
    return yield* rules.check(inputs);
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

function checkResult(
  rules: RuleEngine,
  self: AgentCard,
  incoming: ReplyResult,
  evidence?: FirewallEvidence | ResultEvidence,
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
    if (rules.configuration !== undefined) {
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
      selected.length === 0
        ? []
        : yield* assess(rules, self, selected, "incoming");
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
      decisions: checks.map(complete),
      ...(guidance === undefined ? {} : { guidance }),
    } satisfies IncomingDecision;
  });
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

function complete(check: RuleDecision): CompletedDecision {
  return Object.freeze({
    checkId: check.completion.checkId,
    direction: check.completion.direction,
    [completedDecision]: check,
  });
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

/** Construction settings and rule types are shared with the policy owner. */
export type { FirewallOptions } from "./rules/service.js";
