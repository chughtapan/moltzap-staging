/** @file Owns local message policy, assessments, permitted content and retained decisions. */

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
import {
  canonicalBytes,
  decodeCanonical,
  encodeCanonicalJson,
  exactStruct,
  MAXIMUM_RUNTIME_ITEM_BYTES,
} from "../encoding/canonical-json.js";
import { AgentCard, agentCardOf } from "../identity/agent-cards.js";
import { Identity } from "../identity/service.js";
import { exitOutcome, markRetries } from "../tracing.js";
import {
  type Chain,
  type ChainOutcome,
  chainRuleFileSchema,
  evaluateChain,
  type FirewallEvidence,
  type MatchInput,
  type MatchResult,
  validateChain,
} from "./chain.js";
import {
  type Message as AssessmentText,
  checkCapturedMessage,
  type CheckedGuidance,
  checkedGuidanceSchema,
  CheckInputError,
  decidingRules,
  type DecisionProvider,
  type DecisionProviderError,
  decodeFrozen,
  type FirewallCheck,
  freezeData,
  type HistoryEntry,
  type MessageContext,
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
  type firewallDecisionSummarySchema,
  FirewallPolicyError,
  firewallRefusalReasonSchema,
  FirewallSettings,
  type LoadedMessagePolicy,
  MAXIMUM_BEHAVIOR_BYTES,
  MAXIMUM_EDITABLE_RULES,
  type PolicyProblem,
  type policyReadSchema,
  type policyUpdateResultSchema,
  type policyUpdateSchema,
  TextPolicySettings,
  TextRule,
} from "./rules.js";
import {
  type FirewallActivation,
  type FirewallCompletion,
  type FirewallStore,
  makeFirewallStore,
  persistenceFailure,
} from "./store.js";

/**
 * Provider resources remain fixed while authorized edits change text rules.
 * Reject and hold judgments, including provider failures, refuse sends and
 * withhold incoming messages.
 */
export interface FirewallOptions {
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
type RefusalReason = typeof firewallRefusalReasonSchema.Type;

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

const refusalText: Readonly<Record<RefusalReason, string>> = {
  "rule-violation": "It breaks a rule of the agent's message policy.",
  "unresolved-evidence":
    "The policy check could not settle whether the message is allowed.",
  "check-unavailable": "The policy check could not complete.",
  "owner-approval-required": "The agent's owner has not approved it.",
};

/**
 * The sender can act on registered feedback without seeing assessment evidence.
 * `rule` names the chain rule that refused, and `detail` is that rule's matcher
 * report. The message states the reason, each deciding rule's registered
 * feedback and the detail.
 */
export class FirewallRefusal extends Data.TaggedError("FirewallRefusal")<{
  readonly reason: "firewall-refused";
  readonly checkId: string;
  readonly refusal: RefusalReason;
  readonly rule?: string;
  readonly feedback: ReadonlyArray<{ readonly text: string }>;
  readonly detail?: unknown;
}> {
  override get message(): string {
    const rule = this.rule === undefined ? "" : `, rule ${this.rule}`;
    return [
      `Message refused by local policy (${this.refusal}${rule}, check ${this.checkId}).`,
      refusalText[this.refusal],
      ...this.feedback.map((entry) => entry.text),
      ...(this.detail === undefined
        ? []
        : [`Details: ${JSON.stringify(this.detail)}`]),
      "Docs: https://docs.moltzap.xyz/guides/run-an-agent-daemon#firewall-refused",
    ].join(" ");
  }
}

/**
 * Run `save` before returning refusal or submitting this exact message.
 * Saving is repeatable and uses the caller's current SQL transaction.
 */
type OutgoingDecision = {
  readonly save: Effect.Effect<void, FirewallPolicyError>;
} & (
  | { readonly kind: "allowed" }
  | { readonly kind: "refused"; readonly refusal: FirewallRefusal }
);

/**
 * Run `save` in the same SQL transaction as the inbox write and visibility update.
 * Saving is repeatable. A withheld message with `reply` gets that text back, as
 * an ordinary message from this agent in the same conversation.
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
  | { readonly kind: "withheld"; readonly reply?: string }
);
/** Message assessment, policy edits and saved decision summaries. */
interface FirewallService {
  /** Assesses without saving. Refusals expose feedback. */
  readonly checkOutgoing: (
    value: Outgoing,
    evidence: FirewallEvidence,
  ) => Effect.Effect<OutgoingDecision, CheckInputError>;
  /**
   * Assesses each peer-controlled part under one policy with its preceding
   * evidence. An admitted part the provider was unsure about carries a
   * reminder; withheld items expose none.
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
  /**
   * Policy resources are fixed for this scope. Registration supplies immutable
   * local attribution. `chain` rules apply before the text rules of `source`;
   * a rule naming an unregistered matcher fails with `build-failed`.
   */
  static readonly layer = (
    agentKey: string,
    source?: FirewallOptions | LoadedMessagePolicy,
    chain?: Chain,
  ) =>
    Layer.effect(
      Firewall,
      Effect.gen(function* () {
        const identity = yield* Identity;
        const store = yield* makeFirewallStore(agentKey);
        const rules =
          chain === undefined ? undefined : yield* captureChain(chain);
        const policy =
          source === undefined
            ? undefined
            : yield* initialPolicy(
                store,
                source,
                rules?.rules ?? [],
                Object.keys(rules?.matchers ?? {}),
              );
        const chainRevision =
          rules === undefined ? "" : yield* chainDigest(rules);
        const localAgent = Effect.gen(function* () {
          const state = yield* identity.me();
          if (state.kind !== "registered") {
            return yield* new CheckInputError({ reason: "context" });
          }
          return agentCardOf(state.identityCard);
        });
        const installed: Installation = {
          store,
          ...(policy === undefined ? {} : { policy }),
          ...(rules === undefined ? {} : { chain: rules }),
          localAgent,
          revision: () =>
            policy === undefined
              ? chainRevision
              : policy.active.configurationDigest,
        };
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
              const [decision] = yield* decide(
                installed,
                [{ value, evidence }],
                "outgoing",
              );
              if (decision === undefined) {
                return yield* new CheckInputError({ reason: "message" });
              }
              const save = saveDecision(store, decision);
              if (decision.kind === "admitted") {
                return { kind: "allowed", save } satisfies OutgoingDecision;
              }
              const { refusal } = decision;
              return {
                kind: "refused",
                save,
                refusal: new FirewallRefusal({
                  reason: "firewall-refused",
                  checkId: decision.completion.checkId,
                  refusal: refusal.reason,
                  ...(refusal.rule === undefined ? {} : { rule: refusal.rule }),
                  feedback: refusal.feedback,
                  ...(refusal.detail === undefined
                    ? {}
                    : { detail: refusal.detail }),
                }),
              } satisfies OutgoingDecision;
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
                return yield* checkResult(installed, agent, value, evidence);
              }
              if (evidence === undefined || "answers" in evidence) {
                return yield* new CheckInputError({ reason: "context" });
              }
              const item: PermittedItem =
                value.kind === "request"
                  ? { kind: "collectiveRequest", message: value.message }
                  : { kind: "multicast", message: value.message };
              const [decision] = yield* decide(
                installed,
                [{ value: value.message, evidence }],
                "incoming",
              );
              if (decision === undefined) {
                return yield* new CheckInputError({ reason: "message" });
              }
              const save = saveDecision(store, decision);
              const visibility = [
                {
                  message: messageReference(value.message),
                  visibleToAgent: decision.kind === "admitted",
                },
              ];
              if (decision.kind === "admitted") {
                return {
                  kind: "allowed",
                  item,
                  save,
                  visibility,
                  ...(decision.guidance === undefined
                    ? {}
                    : { guidance: decision.guidance }),
                } satisfies IncomingDecision;
              }
              const { reply } = decision.refusal;
              return {
                kind: "withheld",
                save,
                visibility,
                ...(reply === undefined ? {} : { reply }),
              } satisfies IncomingDecision;
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
  /** Text rules whose matcher does not match this message, so the provider is not asked about them. */
  readonly outOfScope: readonly string[];
  /** The reply each matching rule's matcher gives, which a `reply` rule sends in place of its feedback. */
  readonly replies: Readonly<Record<string, string>>;
}

/** A refused message's reason, the chain rule that decided, and what its sender is told. */
interface Refusal {
  readonly reason: RefusalReason;
  readonly rule?: string;
  readonly feedback: ReadonlyArray<{ readonly text: string }>;
  readonly detail?: unknown;
  readonly reply?: string;
}

/**
 * One candidate's decision and the record that saves it. A message nothing
 * checked, because no chain rule matched and no text policy is installed, has
 * no record.
 */
type Decision =
  | {
      readonly kind: "admitted";
      readonly completion?: FirewallCompletion;
      readonly guidance?: CheckedGuidance;
    }
  | {
      readonly kind: "refused";
      readonly completion: FirewallCompletion;
      readonly refusal: Refusal;
    };

/** The chain and text policy one firewall applies, and the revision its records name. */
interface Installation {
  readonly store: FirewallStore;
  readonly policy?: Policy;
  readonly chain?: Chain;
  readonly localAgent: Effect.Effect<AgentCard, CheckInputError>;
  readonly revision: () => string;
}

const storedPolicyFormat = "org.social-harness/text-rules/v1";

const storedPolicySchema = exactStruct({
  format: Schema.Literal(storedPolicyFormat),
  rules: editableDocumentSchema,
});

type PolicyEditPath = NonNullable<FirewallPolicyError["path"]>;

type CapturedOptions = Effect.Effect.Success<ReturnType<typeof captureOptions>>;

type ActiveRules = Effect.Effect.Success<ReturnType<typeof buildPolicy>>;

interface RuleChange {
  readonly add: typeof editableDocumentSchema.Type;
  readonly remove: readonly string[];
}

type CapturedCheck = Effect.Effect.Success<ReturnType<typeof capturePrepared>>;

type DecisionSummary = typeof firewallDecisionSummarySchema.Type;

/** Each peer part of a result is checked as an incoming message; a `reply` rule withholds it without replying. */
function checkResult(
  installed: Installation,
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
    if (installed.policy !== undefined || installed.chain !== undefined) {
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
    const decisions =
      selected.length === 0
        ? []
        : yield* decide(installed, selected, "incoming");
    const checked = selected.map((source, index) => ({
      source,
      check: decisions[index],
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
      return check?.kind === "refused"
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
        questionCheck?.kind === "refused"
          ? withheld(questionCheck)
          : result.request.question,
      outcomes: [permittedOutcome(first), ...rest.map(permittedOutcome)],
    };
    const answers = checked.flatMap(({ source, check }) =>
      source.part === "answer" &&
      check?.kind === "admitted" &&
      check.guidance !== undefined
        ? [{ member: source.message.author, ...check.guidance }]
        : [],
    );
    const questionGuidance =
      questionCheck?.kind === "admitted" ? questionCheck.guidance : undefined;
    const guidance: LocalGuidance | undefined =
      answers.length === 0 && questionGuidance === undefined
        ? undefined
        : {
            answers,
            ...(questionGuidance === undefined
              ? {}
              : { question: questionGuidance }),
          };
    return {
      kind: "allowed",
      item,
      visibility: resultVisibility(item, sources),
      save: saveDecision(installed.store, ...decisions),
      ...(guidance === undefined ? {} : { guidance }),
    } satisfies IncomingDecision;
  });
}

/**
 * Applies the chain to each candidate, then the text policy to every candidate
 * no chain rule matched, in one assessment that shares its clock and permits.
 */
function decide(
  installed: Installation,
  candidates: readonly Candidate[],
  direction: Direction,
) {
  return Effect.gen(function* () {
    const chained = yield* Effect.forEach(
      candidates,
      (candidate) =>
        chainDecision(installed, candidate, direction).pipe(
          Effect.map((decision) => ({ candidate, decision })),
        ),
      { concurrency: 1 },
    );
    const unmatched = chained.flatMap(({ candidate, decision }) =>
      decision === undefined ? [candidate] : [],
    );
    const assessed =
      installed.policy === undefined || unmatched.length === 0
        ? []
        : yield* assess(
            installed.policy,
            yield* installed.localAgent,
            unmatched,
            direction,
            installed.chain?.matchers ?? {},
          );
    const text = assessed.values();
    return chained.map(
      ({ decision }): Decision =>
        decision ?? text.next().value ?? { kind: "admitted" },
    );
  });
}

/** Records the chain rule that decided a candidate, or returns nothing when none matched. */
function chainDecision(
  installed: Installation,
  candidate: Candidate,
  direction: Direction,
) {
  return Effect.gen(function* () {
    if (installed.chain === undefined) {
      return undefined;
    }
    const outcome = yield* evaluateChain(installed.chain, {
      direction,
      message: candidate.value,
      evidence: candidate.evidence,
      self: yield* installed.localAgent,
    });
    if (outcome.kind === "unmatched") {
      return undefined;
    }
    const activeRevision = installed.revision();
    const completion: FirewallCompletion = {
      checkId: mintCheckId(),
      direction,
      canonicalCandidate: yield* encodeCanonicalJson(candidate.value),
      canonicalInput: yield* canonicalBytes({
        activeRevision,
        rule: outcome.rule,
      }),
      canonicalResult: yield* canonicalBytes({
        result: {
          rule: outcome.rule,
          ...(outcome.kind === "refused" ? { refusal: outcome.refusal } : {}),
        },
        summaries: chainSummaries(
          candidate,
          direction,
          outcome,
          activeRevision,
        ),
      }),
      admit: outcome.kind === "allowed",
    };
    if (outcome.kind === "allowed") {
      return { kind: "admitted", completion } satisfies Decision;
    }
    return {
      kind: "refused",
      completion,
      refusal: {
        reason: outcome.refusal,
        rule: outcome.rule,
        feedback: [],
        ...(outcome.detail === undefined ? {} : { detail: outcome.detail }),
        ...(outcome.reply === undefined ? {} : { reply: outcome.reply }),
      },
    } satisfies Decision;
  }).pipe(
    Effect.catchTags({
      CanonicalJsonError: () =>
        Effect.fail(new CheckInputError({ reason: "message" })),
      ClientRepresentationError: () =>
        Effect.fail(new CheckInputError({ reason: "message" })),
    }),
  );
}

/** One summary row per peer, as for text checks, naming the deciding chain rule. */
function chainSummaries(
  candidate: Candidate,
  direction: Direction,
  outcome: Exclude<ChainOutcome, { readonly kind: "unmatched" }>,
  activeRevision: string,
): readonly DecisionSummary[] {
  const peers =
    direction === "outgoing"
      ? candidate.value.recipients
      : [candidate.value.author];
  const held =
    outcome.kind === "refused" && outcome.refusal === "owner-approval-required";
  const judged = outcome.kind === "refused" ? "reject" : "allow";
  return peers.map((peer) => ({
    direction,
    peer,
    outcome: held ? "hold" : judged,
    reason: outcome.kind === "allowed" ? "eligible" : outcome.refusal,
    decidedBy: [{ ruleId: outcome.rule, from: "chain" }],
    checkedRuleIds: [outcome.rule],
    activeRevision,
  }));
}

/** Saves the records of decisions that have one; saving none does no work. */
function saveDecision(store: FirewallStore, ...decisions: readonly Decision[]) {
  const completions = decisions.flatMap((decision) =>
    decision.completion === undefined ? [] : [decision.completion],
  );
  return completions.length === 0
    ? Effect.void
    : store.saveDecisions(completions);
}

/**
 * Checks candidates against the text rules. The classifier sees only ordered
 * history, so this agent's messages the Router has not ordered yet count as a
 * gap in it.
 */
function assess(
  policy: Policy,
  self: AgentCard,
  candidates: readonly Candidate[],
  direction: Direction,
  matchers: Chain["matchers"],
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
        gaps: [
          ...(candidate.evidence.pendingOutgoing.length === 0
            ? []
            : ["An earlier outgoing message is not yet complete."]),
          ...candidate.evidence.gaps,
        ],
      };
      const scope = yield* ruleScopes(policy.active.rules, matchers, {
        direction,
        message: candidate.value,
        evidence: candidate.evidence,
        self,
      });
      inputs.push({
        direction,
        ...scope,
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

/**
 * Each text rule's scope for the message: the rules whose matcher does not
 * match it, and the reply each matching matcher gives. A rule whose matcher is
 * missing or fails stays in scope, so it is never skipped for want of its
 * check.
 */
function ruleScopes(
  rules: readonly TextRule[],
  matchers: Chain["matchers"],
  input: MatchInput,
) {
  const inScope: MatchResult = { matched: true };
  return Effect.forEach(
    rules,
    (rule) => {
      const matcher =
        rule.match !== undefined && Object.hasOwn(matchers, rule.match)
          ? matchers[rule.match]
          : undefined;
      return (
        matcher === undefined
          ? Effect.succeed(inScope)
          : matcher
              .match(input)
              .pipe(Effect.catchAllDefect(() => Effect.succeed(inScope)))
      ).pipe(
        Effect.map((result): readonly [string, MatchResult] => [
          rule.id,
          result,
        ]),
      );
    },
    { concurrency: 1 },
  ).pipe(
    Effect.map((scoped) => ({
      outOfScope: scoped.flatMap(([id, result]) =>
        result.matched ? [] : [id],
      ),
      replies: Object.fromEntries(
        scoped.flatMap(([id, result]) =>
          result.matched && result.reply !== undefined
            ? [[id, result.reply]]
            : [],
        ),
      ),
    })),
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

function withheld(
  check: Extract<Decision, { readonly kind: "refused" }>,
): Withheld {
  return {
    kind: "withheld",
    checkId: check.completion.checkId,
    reason: check.refusal.reason,
  };
}

function refusalReason(reason: FirewallCheck["reason"]): RefusalReason {
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

/** The installation's revision covers `chain`, so a decision names the rules that applied. */
function initialPolicy(
  store: FirewallStore,
  source: FirewallOptions | LoadedMessagePolicy,
  chain: Chain["rules"],
  matchers: readonly string[],
) {
  return Effect.gen(function* () {
    const options =
      "policy" in source ? yield* configuredOptions(source) : source;
    const captured = yield* captureOptions(options, chain, matchers).pipe(
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

/** Natural-language rules without a provider fail installation instead of passing unchecked. */
function configuredOptions(loaded: LoadedMessagePolicy) {
  const { policy, provider } = loaded;
  if (policy.text !== undefined && provider === undefined) {
    return Effect.fail(new FirewallPolicyError({ reason: "build-failed" }));
  }
  return Effect.succeed({
    settings: policy.settings,
    facts: policy.facts,
    maximumHistoryRecords: policy.maximumHistoryRecords,
    maximumConcurrentChecks: policy.maximumConcurrentChecks,
    text:
      policy.text === undefined || provider === undefined
        ? undefined
        : {
            fixedRules: policy.text.fixedRules,
            initialRules: policy.text.initialRules,
            settings: policy.textSettings,
            provider: retryingTransientFailures(
              makeJevDecisionProvider(provider),
            ),
          },
  } satisfies FirewallOptions);
}

function captureOptions(
  options: FirewallOptions,
  chain: Chain["rules"],
  matchers: readonly string[],
) {
  return Effect.gen(function* () {
    const configuration = yield* decodeFrozen(firewallConfigurationSchema, {
      facts: options.facts,
      maximumHistoryRecords: options.maximumHistoryRecords ?? 256,
      maximumConcurrentChecks: options.maximumConcurrentChecks ?? 4,
    });
    const settings = yield* decodeFrozen(FirewallSettings, options.settings);
    const text =
      options.text === undefined ? undefined : yield* captureText(options.text);
    const approvePolicyUpdate =
      options.approvePolicyUpdate?.bind(options) ?? (() => Effect.void);
    return {
      configuration,
      settings,
      text,
      approvePolicyUpdate,
      chain,
      matchers,
    };
  });
}

/**
 * Validates and freezes the rules, and checks that every rule's matcher is
 * registered. A rule file that breaks the file contract fails `build-failed`.
 */
function captureChain(chain: Chain) {
  return Effect.gen(function* () {
    const { rules } = yield* decodeFrozen(chainRuleFileSchema, {
      rules: chain.rules,
    }).pipe(
      Effect.catchTag("ParseError", () =>
        Effect.fail(new FirewallPolicyError({ reason: "build-failed" })),
      ),
    );
    const captured: Chain = { ...chain, rules };
    yield* validateChain(captured);
    return captured;
  });
}

/** Names a chain-only installation, which has no text policy revision. */
function chainDigest(chain: Chain) {
  return digest({ chain: chain.rules }).pipe(
    Effect.catchTag("ClientRepresentationError", () =>
      Effect.fail(new FirewallPolicyError({ reason: "build-failed" })),
    ),
  );
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
    const projection = yield* guidanceProjection(assessed, checkId);
    const { result } = projection;
    const admit = result.judgment === "allow";
    const completion: FirewallCompletion = {
      checkId,
      direction: input.direction,
      canonicalCandidate: input.canonicalCandidate,
      canonicalInput: yield* retainedInput(active, input),
      admit,
      canonicalResult: yield* canonicalBytes({
        result: Struct.omit(result, "trace"),
        summaries: yield* decisionSummaries(active, input, result),
      }),
    };
    if (admit) {
      return {
        kind: "admitted",
        completion,
        ...(projection.guidance === undefined
          ? {}
          : { guidance: projection.guidance }),
      } satisfies Decision;
    }
    const reply =
      input.direction === "incoming"
        ? textReply(active, result, requested.replies)
        : "";
    return {
      kind: "refused",
      completion,
      refusal: {
        reason: refusalReason(result.reason),
        feedback: result.feedback,
        ...(reply === "" ? {} : { reply }),
      },
    } satisfies Decision;
  }).pipe(
    Effect.catchTag("ClientRepresentationError", () =>
      Effect.fail(new CheckInputError({ reason: "context" })),
    ),
    Effect.withSpan("assessMessage"),
  );
}

/**
 * What the sender of a withheld message is told. The first violated `reply`
 * rule whose matcher gave a reply sends that reply, such as a norm's
 * `not-understood`; otherwise each violated `reply` rule's registered
 * feedback, one per line. Holds reply nothing: the sender broke no settled
 * rule.
 */
function textReply(
  active: ActiveRules,
  result: FirewallCheck,
  replies: Readonly<Record<string, string>>,
): string {
  if (result.reason !== "rule-violation") {
    return "";
  }
  const violated = new Set(
    decidingRules(result.reason, active.rules, result).map(
      (rule) => rule.ruleId,
    ),
  );
  const replying = active.rules.filter(
    (rule) => violated.has(rule.id) && rule.action === "reply",
  );
  const matched = replying.find((rule) => Object.hasOwn(replies, rule.id));
  if (matched !== undefined) {
    return replies[matched.id] ?? "";
  }
  return replying
    .flatMap((rule) =>
      rule.feedback === undefined ? [] : [rule.feedback.text],
    )
    .join("\n");
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
          outOfScope: input.outOfScope,
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
      outOfScope: input.outOfScope,
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
    ...new Set(
      result.findings.flatMap((finding) =>
        finding.outcome === "not-applicable" ? [] : [finding.ruleId],
      ),
    ),
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

/**
 * The reminders of an admitted incoming message the provider was unsure about.
 * Reminders over the runtime item bound make the check unavailable.
 */
function guidanceProjection(result: FirewallCheck, checkId: string) {
  return Effect.gen(function* () {
    if (result.guidance.length === 0) {
      return { result };
    }
    const guidance: CheckedGuidance = {
      checkId,
      instructions: result.guidance,
    };
    const bytes = yield* canonicalBytes(guidance);
    return bytes.byteLength > MAXIMUM_RUNTIME_ITEM_BYTES
      ? { result: { ...unavailable(), findings: result.findings } }
      : { result, guidance };
  });
}

/** Missing or invalid assessment input holds without inventing a deciding rule. */
function unavailable(): FirewallCheck {
  return {
    judgment: "hold",
    reason: "check-unavailable",
    feedback: [],
    guidance: [],
    findings: [],
  };
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
    const unmatched = documents.find(
      (rule) =>
        rule.match !== undefined && !options.matchers.includes(rule.match),
    );
    if (unmatched !== undefined) {
      return yield* new FirewallPolicyError({
        reason: "build-failed",
        ruleId: unmatched.id,
      });
    }
    const metadata = {
      configuration: options.configuration,
      settings: options.settings,
      questionEdition: textQuestionEdition,
      chain: options.chain,
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
