/** @file Owns one agent's message policy: its checks, the content they permit, its edits and its saved decisions. */

import {
  Clock,
  Context,
  Effect,
  Exit,
  Layer,
  Option,
  Schedule,
  Schema,
  Struct,
} from "effect";
import { createHash, randomBytes } from "node:crypto";
import type {
  CollectiveOutcome,
  CollectiveRequest,
  CollectiveResultSources,
  Message,
  MessageReference,
} from "../communication/index.js";
import type { Incoming, Outgoing } from "../communication/service.js";
import {
  decodeJson,
  exactStruct,
  MAXIMUM_RUNTIME_ITEM_BYTES,
} from "../encoding.js";
import { type AgentCard, Identity } from "../identity.js";
import {
  type Chain,
  type ChainDecision,
  chainRuleFileSchema,
  evaluateChain,
  type FirewallEvidence,
  type Match,
  type MessageToMatch,
  validateChain,
} from "./chain.js";
import {
  type AssessmentText,
  checkCapturedMessage,
  type CheckedGuidance,
  type CheckEvidence,
  type CollectiveResult,
  decidingRules,
  type DecisionProvider,
  type DecisionProviderError,
  decodeFrozen,
  deepFreeze,
  type FirewallCheck,
  FirewallRefusal,
  type HistoryEntry,
  InvalidCheckError,
  type LocalGuidance,
  type PermittedItem,
  textQuestionEdition,
  validateChecks,
  type Withheld,
} from "./check.js";
import {
  type CompiledNorms,
  editedNorms,
  editedRules,
  type HeldNorm,
  invalidRule,
  type NormDocuments,
  resolvedChange,
} from "./editable-policy.js";
import { makeJevDecisionProvider } from "./jev.js";
import {
  CheckLimits,
  type Direction,
  editableRulesSchema,
  type FirewallConfiguration,
  firewallConfigurationSchema,
  type firewallDecisionsSchema,
  type firewallDecisionSummarySchema,
  FirewallPolicyError,
  type FirewallRefusalReason,
  type LoadedMessagePolicy,
  type policyReadSchema,
  type policyRevisionSchema,
  type policyUpdateSchema,
  ProviderRequestSettings,
  TextRule,
} from "./rules.js";
import {
  type CheckRecord,
  type FirewallActivation,
  type FirewallStore,
  makeFirewallStore,
  persistenceFailure,
} from "./store.js";

/**
 * A message policy composed in code: its limits, owner facts and text rules
 * with their decision provider. The provider stays fixed while authorized
 * edits change text rules. Reject and hold judgments, including provider
 * failures, refuse sends and withhold incoming messages.
 */
export interface ComposedMessagePolicy {
  readonly limits: CheckLimits;
  readonly facts: CheckEvidence["facts"];
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
    readonly providerSettings: ProviderRequestSettings;
  };
}

type InstalledPolicy = Effect.Effect.Success<ReturnType<typeof initialPolicy>>;

type IncomingCollectiveResult = Extract<
  Incoming,
  { readonly kind: "collectiveResult" }
>;

/**
 * The evidence a check of a collective result reads. A peer's question and
 * each peer's collective response keep their own preceding history.
 */
export interface CollectiveResultEvidence {
  readonly question?: FirewallEvidence;
  readonly responses: ReadonlyArray<{
    readonly message: MessageReference;
    readonly evidence: FirewallEvidence;
  }>;
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
  | {
      readonly kind: "withheld";
      /** Why the message was withheld. */
      readonly reason: FirewallRefusalReason;
      readonly reply?: string;
    }
);
/** Message checks, policy edits and saved decision summaries. */
interface FirewallService {
  /** Checks without saving. Refusals expose feedback. */
  readonly checkOutgoing: (
    message: Outgoing,
    evidence: FirewallEvidence,
  ) => Effect.Effect<OutgoingDecision, InvalidCheckError>;
  /**
   * Checks each part a peer wrote under one message policy with its preceding
   * evidence. An allowed part the provider was unsure about carries guidance;
   * withheld parts expose none.
   */
  readonly checkIncoming: (
    incoming: Incoming,
    evidence?: FirewallEvidence | CollectiveResultEvidence,
  ) => Effect.Effect<IncomingDecision, InvalidCheckError>;
  readonly readPolicy: () => Effect.Effect<
    typeof policyReadSchema.Type,
    FirewallPolicyError
  >;
  /** The documents of the norms the agent holds in its active revision. */
  readonly readNorms: () => Effect.Effect<
    readonly string[],
    FirewallPolicyError
  >;
  /**
   * Validates and approves each edit before saving and activating it.
   * Retries validate again and may repeat approval.
   */
  readonly updatePolicy: (
    request: typeof policyUpdateSchema.Type,
  ) => Effect.Effect<typeof policyRevisionSchema.Type, FirewallPolicyError>;
  readonly readDecisions: (request: {
    readonly since?: number;
  }) => Effect.Effect<typeof firewallDecisionsSchema.Type, FirewallPolicyError>;
}

/**
 * A decision provider that replaces the one a message policy configures.
 * Tests provide it around the daemon's own composition, so a process built
 * exactly as `sharnessd` judges text rules without network credentials.
 */
export class DecisionProviderOverride extends Context.Tag(
  "SocialHarness/DecisionProviderOverride",
)<DecisionProviderOverride, DecisionProvider>() {}

/** One agent's firewall: it checks messages against the message policy, edits that policy and reads its saved decisions. */
export class Firewall extends Context.Tag("SocialHarness/Firewall")<
  Firewall,
  FirewallService
>() {
  /**
   * Policy resources are fixed for this scope. Identity supplies this agent's
   * card. The chain rules of `chain` apply before the text rules of `source`;
   * a rule naming an unregistered matcher fails with `build-failed`. With
   * `norms`, the agent holds norm documents in its editable policy, and each
   * activation adds the matchers and fixed rules they compile to.
   */
  static readonly layer = (
    agentKey: string,
    source?: ComposedMessagePolicy | LoadedMessagePolicy,
    chain?: Chain,
    norms?: NormDocuments,
  ) =>
    Layer.effect(
      Firewall,
      Effect.gen(function* () {
        const identity = yield* Identity;
        const store = yield* makeFirewallStore(agentKey);
        const capturedChain =
          chain === undefined ? undefined : yield* captureChain(chain);
        const policy =
          source === undefined
            ? undefined
            : yield* initialPolicy(store, source, capturedChain, norms);
        if (policy === undefined && capturedChain !== undefined) {
          yield* validateChain(capturedChain);
        }
        const chainRevision =
          capturedChain === undefined
            ? ""
            : digest({ chain: capturedChain.rules });
        const localAgent = identity.me;
        const installed: Installation = {
          store,
          ...(policy === undefined ? {} : { policy }),
          ...(capturedChain === undefined ? {} : { chain: capturedChain }),
          localAgent,
          revision: () =>
            policy === undefined ? chainRevision : policy.active.revision,
        };
        return Firewall.of({
          readPolicy: () =>
            policy === undefined
              ? Effect.fail(
                  new FirewallPolicyError({ reason: "not-configured" }),
                )
              : Effect.sync(() => ({
                  rules: policy.active.editableRules,
                  fixedRules: policy.active.fixedRules,
                  norms: policy.active.norms.map((norm) => norm.name),
                  activeRevision: policy.active.revision,
                })),
          readNorms: () =>
            policy === undefined
              ? Effect.fail(
                  new FirewallPolicyError({ reason: "not-configured" }),
                )
              : Effect.sync(() =>
                  policy.active.norms.map((norm) => norm.document),
                ),
          updatePolicy: (request) =>
            policy === undefined
              ? Effect.fail(
                  new FirewallPolicyError({ reason: "not-configured" }),
                )
              : updatePolicy(policy, request),
          readDecisions: (request) => store.readDecisions(request),
          checkOutgoing: (message, evidence) =>
            Effect.gen(function* () {
              const [decision] = yield* decide(
                installed,
                [{ message, evidence }],
                "outgoing",
              );
              if (decision === undefined) {
                return yield* new InvalidCheckError({ reason: "message" });
              }
              const save = saveRecords(store, decision);
              if (decision.kind === "allowed") {
                return { kind: "allowed", save } satisfies OutgoingDecision;
              }
              const { refusal } = decision;
              return {
                kind: "refused",
                save,
                refusal: new FirewallRefusal({
                  reason: "firewall-refused",
                  checkId: decision.record.checkId,
                  refusal: refusal.reason,
                  ...(refusal.ruleId === undefined
                    ? {}
                    : { rule: refusal.ruleId }),
                  feedback: refusal.feedback,
                  ...(refusal.detail === undefined
                    ? {}
                    : { detail: refusal.detail }),
                }),
              } satisfies OutgoingDecision;
            }),
          checkIncoming: (incoming, evidence) =>
            Effect.gen(function* () {
              const agent = localAgent;
              if (incoming.kind === "collectiveResult") {
                return yield* checkCollectiveResult(
                  installed,
                  agent,
                  incoming,
                  evidence,
                );
              }
              if (evidence === undefined || "responses" in evidence) {
                return yield* new InvalidCheckError({ reason: "evidence" });
              }
              const item: PermittedItem = incoming;
              const [decision] = yield* decide(
                installed,
                [{ message: incoming.message, evidence }],
                "incoming",
              );
              if (decision === undefined) {
                return yield* new InvalidCheckError({ reason: "message" });
              }
              const save = saveRecords(store, decision);
              const visibility = [
                {
                  message: messageReference(incoming.message),
                  visibleToAgent: decision.kind === "allowed",
                },
              ];
              if (decision.kind === "allowed") {
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
                reason: decision.refusal.reason,
                save,
                visibility,
                ...(reply === undefined ? {} : { reply }),
              } satisfies IncomingDecision;
            }),
        });
      }),
    );
}

/**
 * One message under check with its evidence. `textOverride` replaces the text
 * the text rules read, for a part of a collective result.
 */
interface Candidate {
  readonly message: Outgoing;
  readonly evidence: FirewallEvidence;
  readonly textOverride?: string;
}

/** One candidate prepared for a check of the text rules. */
interface PreparedTextCheck {
  readonly direction: Direction;
  readonly messageJson: string;
  readonly message: AssessmentText;
  readonly evidence: CheckEvidence;
  readonly agents: readonly AgentCard[];
  /** Text rules whose matcher does not match this message, so the provider is not asked about them. */
  readonly outOfScope: readonly string[];
  /** The reply each matching rule's matcher gives, which a `reply` rule sends in place of its feedback. */
  readonly replies: Readonly<Record<string, string>>;
}

/** A refused or withheld message's reason, the chain rule that decided, and what its sender is told. */
interface Refusal {
  readonly reason: FirewallRefusalReason;
  readonly ruleId?: string;
  readonly feedback: ReadonlyArray<{ readonly text: string }>;
  readonly detail?: unknown;
  readonly reply?: string;
}

/**
 * One candidate's decision and the check record that saves it. `refused`
 * refuses a message the agent sends and withholds one it receives. A message
 * nothing checked, because no chain rule matched and no text rules are
 * installed, has no record.
 */
type Decision =
  | {
      readonly kind: "allowed";
      readonly record?: CheckRecord;
      readonly guidance?: CheckedGuidance;
    }
  | {
      readonly kind: "refused";
      readonly record: CheckRecord;
      readonly refusal: Refusal;
    };

/** The message policy one firewall applies, and the revision its check records name. */
interface Installation {
  readonly store: FirewallStore;
  readonly policy?: InstalledPolicy;
  readonly chain?: Chain;
  readonly localAgent: AgentCard;
  readonly revision: () => string;
}

const storedPolicyFormat = "org.social-harness/editable-policy/v1";

const storedPolicySchema = exactStruct({
  format: Schema.Literal(storedPolicyFormat),
  rules: editableRulesSchema,
  norms: Schema.Array(Schema.String),
});

type CapturedMessagePolicy = Effect.Effect.Success<
  ReturnType<typeof capturePolicy>
>;

type ActivePolicy = Effect.Effect.Success<ReturnType<typeof buildPolicy>>;

const noNorms: CompiledNorms = { matchers: {}, fixedRules: [] };

type CapturedTextCheck = Effect.Effect.Success<
  ReturnType<typeof capturePrepared>
>;

type DecisionSummary = typeof firewallDecisionSummarySchema.Type;

/**
 * Each part of a collective result that a peer wrote is checked as an incoming
 * message; a `reply` rule withholds it without replying.
 */
function checkCollectiveResult(
  installed: Installation,
  self: AgentCard,
  incoming: IncomingCollectiveResult,
  evidence?: FirewallEvidence | CollectiveResultEvidence,
) {
  return Effect.gen(function* () {
    const { result, sources } = incoming;
    const peerResponses = sources.responses.filter(
      (message) => message.author.agentId !== self.agentId,
    );
    const peerRequest =
      sources.request?.author.agentId === self.agentId
        ? undefined
        : sources.request;
    const parts: Array<
      Candidate & {
        readonly message: Message;
        readonly part: "question" | "response";
      }
    > = [];
    if (installed.policy !== undefined || installed.chain !== undefined) {
      if (
        evidence === undefined ||
        !("responses" in evidence) ||
        evidence.responses.length !== peerResponses.length
      ) {
        return yield* new InvalidCheckError({ reason: "evidence" });
      }
      const seenResponses = new Set<string>();
      for (const message of peerResponses) {
        const responseEvidence = evidence.responses.find((entry) =>
          sameReference(entry.message, messageReference(message)),
        );
        const key = `${message.author.agentId}:${message.msgId}`;
        if (responseEvidence === undefined || seenResponses.has(key)) {
          return yield* new InvalidCheckError({ reason: "evidence" });
        }
        seenResponses.add(key);
        const outcome = result.outcomes.find(
          (entry) => entry.recipient.agentId === message.author.agentId,
        );
        if (outcome === undefined) {
          return yield* new InvalidCheckError({ reason: "message" });
        }
        parts.push({
          message,
          part: "response",
          evidence: responseEvidence.evidence,
          textOverride: outcomeText(outcome.outcome),
        });
      }
      if (peerRequest !== undefined) {
        if (evidence.question === undefined) {
          return yield* new InvalidCheckError({ reason: "evidence" });
        }
        parts.push({
          message: peerRequest,
          part: "question",
          evidence: evidence.question,
          textOverride: JSON.stringify(result.request.question),
        });
      }
    }
    const decisions =
      parts.length === 0 ? [] : yield* decide(installed, parts, "incoming");
    const checked = parts.map((source, index) => ({
      source,
      decision: decisions[index],
    }));
    if (checked.some((entry) => entry.decision === undefined)) {
      return yield* new InvalidCheckError({ reason: "evidence" });
    }
    const questionDecision = checked.find(
      (entry) => entry.source.part === "question",
    )?.decision;
    const permittedOutcome = (
      outcome: (typeof result.outcomes)[number],
    ): CollectiveResult["outcomes"][number] => {
      const decision = checked.find(
        (entry) =>
          entry.source.part === "response" &&
          entry.source.message.author.agentId === outcome.recipient.agentId,
      )?.decision;
      return decision?.kind === "refused"
        ? { recipient: outcome.recipient, outcome: withheld(decision) }
        : outcome;
    };
    const [first, ...rest] = result.outcomes;
    const item: CollectiveResult = {
      kind: "collectiveResult",
      op: result.request.op,
      requestId: result.request.msgId,
      closedAt: result.closedAt,
      closedBy: result.closedBy,
      question:
        questionDecision?.kind === "refused"
          ? withheld(questionDecision)
          : result.request.question,
      outcomes: [permittedOutcome(first), ...rest.map(permittedOutcome)],
    };
    const responseGuidance = checked.flatMap(({ source, decision }) =>
      source.part === "response" &&
      decision?.kind === "allowed" &&
      decision.guidance !== undefined
        ? [{ recipient: source.message.author, ...decision.guidance }]
        : [],
    );
    const questionGuidance =
      questionDecision?.kind === "allowed"
        ? questionDecision.guidance
        : undefined;
    const guidance: LocalGuidance | undefined =
      responseGuidance.length === 0 && questionGuidance === undefined
        ? undefined
        : {
            responses: responseGuidance,
            ...(questionGuidance === undefined
              ? {}
              : { question: questionGuidance }),
          };
    return {
      kind: "allowed",
      item,
      visibility: resultVisibility(item, sources),
      save: saveRecords(installed.store, ...decisions),
      ...(guidance === undefined ? {} : { guidance }),
    } satisfies IncomingDecision;
  });
}

/**
 * Applies the chain to each candidate, then the text rules to every candidate
 * no chain rule matched, in one pass that shares its clock and permits.
 */
function decide(
  installed: Installation,
  candidates: readonly Candidate[],
  direction: Direction,
) {
  return Effect.gen(function* () {
    const chain = installed.policy?.active.chain ?? installed.chain;
    const chained = yield* Effect.forEach(
      candidates,
      (candidate) =>
        chainDecision(installed, candidate, direction, chain).pipe(
          Effect.map((decision) => ({ candidate, decision })),
        ),
      { concurrency: 1 },
    );
    const unmatched = chained.flatMap(({ candidate, decision }) =>
      decision === undefined ? [candidate] : [],
    );
    const textDecisions =
      installed.policy === undefined || unmatched.length === 0
        ? []
        : yield* checkTextRules(
            installed.policy,
            installed.localAgent,
            unmatched,
            direction,
          );
    const remaining = textDecisions.values();
    return chained.map(
      ({ decision }): Decision =>
        decision ?? remaining.next().value ?? { kind: "allowed" },
    );
  });
}

/** The decision of the chain rule that decided a candidate, with its check record, or nothing when none matched. */
function chainDecision(
  installed: Installation,
  candidate: Candidate,
  direction: Direction,
  chain?: Chain,
) {
  return Effect.gen(function* () {
    if (chain === undefined) {
      return undefined;
    }
    const decided = yield* evaluateChain(chain, {
      direction,
      message: candidate.message,
      evidence: candidate.evidence,
      self: installed.localAgent,
    });
    if (decided.kind === "unmatched") {
      return undefined;
    }
    const revision = installed.revision();
    const record: CheckRecord = {
      checkId: mintCheckId(),
      direction,
      messageJson: JSON.stringify(candidate.message),
      inputJson: JSON.stringify({ revision, ruleId: decided.ruleId }),
      resultJson: JSON.stringify({
        check: {
          ruleId: decided.ruleId,
          ...(decided.kind === "refused" ? { reason: decided.reason } : {}),
        },
        summaries: chainSummaries(candidate, direction, decided, revision),
      }),
      allowed: decided.kind === "allowed",
    };
    if (decided.kind === "allowed") {
      return { kind: "allowed", record } satisfies Decision;
    }
    return {
      kind: "refused",
      record,
      refusal: {
        reason: decided.reason,
        ruleId: decided.ruleId,
        feedback: [],
        ...(decided.detail === undefined ? {} : { detail: decided.detail }),
        ...(decided.reply === undefined ? {} : { reply: decided.reply }),
      },
    } satisfies Decision;
  });
}

/** One summary row per peer, as for a check of the text rules, naming the deciding chain rule. */
function chainSummaries(
  candidate: Candidate,
  direction: Direction,
  decided: Exclude<ChainDecision, { readonly kind: "unmatched" }>,
  revision: string,
): readonly DecisionSummary[] {
  const peers =
    direction === "outgoing"
      ? candidate.message.recipients
      : [candidate.message.author];
  const held =
    decided.kind === "refused" && decided.reason === "owner-approval-required";
  const judged = decided.kind === "refused" ? "reject" : "allow";
  return peers.map((peer) => ({
    direction,
    peer,
    judgment: held ? "hold" : judged,
    reason: decided.kind === "allowed" ? "eligible" : decided.reason,
    decidedBy: [{ ruleId: decided.ruleId, from: "chain" }],
    checkedRuleIds: [decided.ruleId],
    activeRevision: revision,
  }));
}

/** Saves the check records of decisions that have one; saving none does no work. */
function saveRecords(store: FirewallStore, ...decisions: readonly Decision[]) {
  const records = decisions.flatMap((decision) =>
    decision.record === undefined ? [] : [decision.record],
  );
  return records.length === 0 ? Effect.void : store.saveCheckRecords(records);
}

/** Checks candidates against the text rules, each with the history before it. */
function checkTextRules(
  policy: InstalledPolicy,
  self: AgentCard,
  candidates: readonly Candidate[],
  direction: Direction,
) {
  return Effect.gen(function* () {
    const now = yield* Clock.currentTimeMillis;
    const active = policy.active;
    const prepared: PreparedTextCheck[] = [];
    for (const candidate of candidates) {
      const cards = new Map<string, AgentCard>();
      for (const card of [
        self,
        candidate.message.author,
        ...candidate.message.recipients,
        ...candidate.evidence.history.flatMap(({ message }) => [
          message.author,
          ...message.recipients,
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
      const checkEvidence: CheckEvidence = {
        agentId: self.agentId,
        principal: self.principalId,
        participants: [...cards.values()].map((card) => ({
          agentId: card.agentId,
          principal: card.principalId,
        })),
        history,
        earlierHistoryOmitted: candidate.evidence.earlierHistoryOmitted,
        now,
        facts: policy?.captured.configuration.facts ?? [],
        gaps: candidate.evidence.gaps,
      };
      const scope = yield* ruleScopes(active.textRules, active.matchers, {
        direction,
        message: candidate.message,
        evidence: candidate.evidence,
        self,
      });
      prepared.push({
        direction,
        ...scope,
        messageJson: JSON.stringify(candidate.message),
        message: {
          // eslint-disable-next-line @typescript-eslint/no-misused-spread -- assessmentText returns plain fields with no prototype methods.
          ...assessmentText(candidate.message),
          ...(candidate.textOverride === undefined
            ? {}
            : { text: candidate.textOverride }),
        },
        evidence: checkEvidence,
        agents: [...cards.values()],
      });
    }
    return yield* Effect.forEach(
      prepared,
      (check) =>
        policy.permits.withPermits(1)(
          checkMessageText(policy.captured, active, check),
        ),
      { concurrency: policy.captured.configuration.maximumConcurrentChecks },
    );
  });
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
  toMatch: MessageToMatch,
) {
  const inScope: Match = { matched: true };
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
              .match(toMatch)
              .pipe(Effect.catchAllDefect(() => Effect.succeed(inScope)))
      ).pipe(Effect.map((match): readonly [string, Match] => [rule.id, match]));
    },
    { concurrency: 1 },
  ).pipe(
    Effect.map((scoped) => ({
      outOfScope: scoped.flatMap(([id, match]) => (match.matched ? [] : [id])),
      replies: Object.fromEntries(
        scoped.flatMap(([id, match]) =>
          match.matched && match.reply !== undefined ? [[id, match.reply]] : [],
        ),
      ),
    })),
  );
}

function assessmentText(message: Outgoing): AssessmentText {
  const [first, ...rest] = message.recipients;
  const participants: Pick<AssessmentText, "sender" | "recipients"> = {
    sender: message.author.agentId,
    recipients: [first.agentId, ...rest.map((card) => card.agentId)],
  };
  const base = {
    id: referenceId(`${message.author.agentId}:${message.msgId}`),
    ...participants,
  };
  const requestText = (
    request: Parameters<typeof questionText>[0],
  ): AssessmentText => ({
    ...base,
    text: questionText(request),
    operation: {
      kind: "request",
      op: request.op,
      requestId: message.msgId,
      deadlineAt: request.deadlineAt,
    },
  });
  if ("kind" in message) {
    return requestText(message);
  }
  if (message.collectiveRequest !== undefined) {
    return requestText({ ...message, ...message.collectiveRequest });
  }
  const response = message.collectiveResponse;
  if (response !== undefined) {
    return {
      ...base,
      text:
        response.action === "accept"
          ? `answered ${JSON.stringify(response.content)}`
          : "declined",
      operation: {
        kind: "answer",
        op: message.recipients.length === 1 ? "gather" : "all_gather",
        requestId: response.requestId,
      },
    };
  }
  return {
    ...base,
    text: message.content
      .map((part) =>
        part.type === "text" ? part.text : JSON.stringify(part.value),
      )
      .join("\n"),
  };
}

function questionText(
  request: Pick<
    CollectiveRequest,
    | "author"
    | "recipients"
    | "question"
    | "op"
    | "deadlineAt"
    | "requestedSchema"
  >,
) {
  return [
    `${request.op} from ${request.author.agentName} to ${request.recipients.map((agent) => agent.agentName).join(", ")}, open until ${new Date(request.deadlineAt).toISOString()}.`,
    `Question: ${request.question}`,
    `Form: ${JSON.stringify(request.requestedSchema)}`,
  ].join("\n");
}

function resultVisibility(
  item: CollectiveResult,
  sources: CollectiveResultSources,
): IncomingDecision["visibility"] {
  return [
    ...sources.responses.map((message) => ({
      message: messageReference(message),
      visibleToAgent: item.outcomes.some(
        (entry) =>
          entry.recipient.agentId === message.author.agentId &&
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

function outcomeText(outcome: typeof CollectiveOutcome.Type): string {
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
  decision: Extract<Decision, { readonly kind: "refused" }>,
): Withheld {
  return {
    kind: "withheld",
    checkId: decision.record.checkId,
    reason: decision.refusal.reason,
  };
}

function refusalReason(reason: FirewallCheck["reason"]): FirewallRefusalReason {
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

function referenceId(reference: string): string {
  return `msg_${createHash("sha256").update(reference).digest("hex")}`;
}

/**
 * Installs the message policy `source` gives. Its revision covers `chain` and
 * the held norms, so a decision names the rules that applied.
 */
function initialPolicy(
  store: FirewallStore,
  source: ComposedMessagePolicy | LoadedMessagePolicy,
  chain?: Chain,
  norms?: NormDocuments,
) {
  return Effect.gen(function* () {
    const composed =
      "policy" in source ? yield* composedPolicy(source) : source;
    const captured = yield* capturePolicy(composed, chain, norms).pipe(
      Effect.catchTag("ParseError", () =>
        Effect.fail(new FirewallPolicyError({ reason: "build-failed" })),
      ),
      Effect.catchAllDefect(() =>
        Effect.fail(new FirewallPolicyError({ reason: "build-failed" })),
      ),
    );
    const { saved, editableRules, normDocuments } = yield* loadEditablePolicy(
      store,
      captured,
    );
    const held = yield* heldNorms(captured, normDocuments);
    const active = yield* buildPolicy(captured, editableRules, held);
    if (saved === undefined) {
      yield* saveEditablePolicy(store, editableRules, held);
    }
    return {
      store,
      captured,
      active,
      edits: yield* Effect.makeSemaphore(1),
      permits: yield* Effect.makeSemaphore(
        captured.configuration.maximumConcurrentChecks,
      ),
    };
  });
}

/** The saved editable rules and norm documents, or the initial ones for an agent with none saved. */
function loadEditablePolicy(
  store: FirewallStore,
  capturedPolicy: CapturedMessagePolicy,
) {
  return Effect.gen(function* () {
    const savedJson = yield* store.readEditablePolicy();
    const saved =
      savedJson === undefined
        ? undefined
        : yield* decodeJson(storedPolicySchema, savedJson).pipe(
            Effect.mapError(persistenceFailure),
          );
    const editableRules =
      saved?.rules ?? capturedPolicy.text?.initialRules ?? [];
    const normDocuments = saved?.norms ?? capturedPolicy.norms?.initial ?? [];
    deepFreeze(editableRules);
    return { saved, editableRules, normDocuments };
  });
}

/** Names each held norm document; one the daemon's norms cannot read fails `build-failed`. */
function heldNorms(
  capturedPolicy: CapturedMessagePolicy,
  normDocuments: readonly string[],
) {
  return Effect.forEach(
    normDocuments,
    (document) =>
      Option.match(
        Option.flatMap(Option.fromNullable(capturedPolicy.norms), (norms) =>
          norms.name(document),
        ),
        {
          onNone: () =>
            Effect.fail(new FirewallPolicyError({ reason: "build-failed" })),
          onSome: (name) => Effect.succeed({ name, document }),
        },
      ),
    { concurrency: 1 },
  );
}

/** Text rules without a decision provider fail to install instead of passing unchecked. */
function composedPolicy(loaded: LoadedMessagePolicy) {
  return Effect.gen(function* () {
    const { policy: messagePolicy, provider } = loaded;
    if (messagePolicy.text !== undefined && provider === undefined) {
      return yield* new FirewallPolicyError({ reason: "build-failed" });
    }
    const override = yield* Effect.serviceOption(DecisionProviderOverride);
    return {
      limits: messagePolicy.settings,
      facts: messagePolicy.facts,
      maximumHistoryRecords: messagePolicy.maximumHistoryRecords,
      maximumConcurrentChecks: messagePolicy.maximumConcurrentChecks,
      text:
        provider === undefined
          ? undefined
          : {
              fixedRules: messagePolicy.text?.fixedRules ?? [],
              initialRules: messagePolicy.text?.initialRules ?? [],
              providerSettings: messagePolicy.textSettings,
              provider: Option.getOrElse(override, () =>
                retryingTransientFailures(makeJevDecisionProvider(provider)),
              ),
            },
    } satisfies ComposedMessagePolicy;
  });
}

function capturePolicy(
  composed: ComposedMessagePolicy,
  chain?: Chain,
  norms?: NormDocuments,
) {
  return Effect.gen(function* () {
    const configuration = yield* decodeFrozen(firewallConfigurationSchema, {
      facts: composed.facts,
      maximumHistoryRecords: composed.maximumHistoryRecords ?? 256,
      maximumConcurrentChecks: composed.maximumConcurrentChecks ?? 4,
    });
    const limits = yield* decodeFrozen(CheckLimits, composed.limits);
    const text =
      composed.text === undefined
        ? undefined
        : yield* captureText(composed.text);
    const approvePolicyUpdate =
      composed.approvePolicyUpdate?.bind(composed) ?? (() => Effect.void);
    return {
      configuration,
      limits,
      text,
      approvePolicyUpdate,
      chain,
      norms,
    };
  });
}

/**
 * Validates and freezes the chain rules. Chain rules that break the file
 * contract fail `build-failed`. Each activation checks the rules' matchers,
 * since held norms add matchers to a policy's chain.
 */
function captureChain(chain: Chain) {
  return decodeFrozen(chainRuleFileSchema, { rules: chain.rules }).pipe(
    Effect.map(({ rules }): Chain => ({ ...chain, rules })),
    Effect.catchTag("ParseError", () =>
      Effect.fail(new FirewallPolicyError({ reason: "build-failed" })),
    ),
  );
}

function captureText(selected: NonNullable<ComposedMessagePolicy["text"]>) {
  return Effect.gen(function* () {
    return {
      fixedRules: yield* decodeFrozen(
        Schema.Array(TextRule),
        selected.fixedRules ?? [],
      ),
      initialRules: yield* decodeFrozen(
        editableRulesSchema,
        selected.initialRules ?? [],
      ),
      providerSettings: yield* decodeFrozen(
        ProviderRequestSettings,
        selected.providerSettings,
      ),
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
  editableRules: readonly TextRule[],
  fixedRuleIds: readonly string[],
) {
  const collision = editableRules.find((rule) =>
    fixedRuleIds.includes(rule.id),
  );
  if (collision !== undefined) {
    return Effect.fail(
      new FirewallPolicyError({ reason: "fixed-rule", ruleId: collision.id }),
    );
  }
  const repeated = editableRules.find(
    (rule, index) =>
      editableRules.findIndex(({ id }) => id === rule.id) !== index,
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

/**
 * One check of one message against the text rules. Its record carries the
 * exact revision and evidence that delivery commits.
 */
function checkMessageText(
  capturedPolicy: CapturedMessagePolicy,
  active: ActivePolicy,
  prepared: PreparedTextCheck,
) {
  return Effect.gen(function* () {
    const captured = yield* capturePrepared(
      capturedPolicy.configuration,
      prepared,
    );
    const checkId = mintCheckId();
    const concluded = yield* runTextCheck(
      capturedPolicy,
      active,
      captured,
    ).pipe(markCheckActivity(captured.direction));
    const projection = guidanceProjection(concluded, checkId);
    const { check } = projection;
    const allowed = check.judgment === "allow";
    const record: CheckRecord = {
      checkId,
      direction: captured.direction,
      messageJson: captured.messageJson,
      inputJson: recordInputJson(active, captured),
      allowed,
      resultJson: JSON.stringify({
        check: Struct.omit(check, "trace"),
        summaries: yield* decisionSummaries(active, captured, check),
      }),
    };
    if (allowed) {
      return {
        kind: "allowed",
        record,
        ...(projection.guidance === undefined
          ? {}
          : { guidance: projection.guidance }),
      } satisfies Decision;
    }
    const reply =
      captured.direction === "incoming"
        ? textReply(active, check, prepared.replies)
        : "";
    return {
      kind: "refused",
      record,
      refusal: {
        reason: refusalReason(check.reason),
        feedback: check.feedback,
        ...(reply === "" ? {} : { reply }),
      },
    } satisfies Decision;
  }).pipe(Effect.withSpan("assessMessage"));
}

/**
 * What the sender of a withheld message is told. The first violated `reply`
 * rule whose matcher gave a reply sends that reply, such as a norm's
 * `not-understood`; otherwise each violated `reply` rule's registered
 * feedback, one per line. Holds reply nothing: the sender broke no settled
 * rule.
 */
function textReply(
  active: ActivePolicy,
  check: FirewallCheck,
  replies: Readonly<Record<string, string>>,
): string {
  if (check.reason !== "rule-violation") {
    return "";
  }
  const violated = new Set(
    decidingRules(check.reason, active.textRules, check).map(
      (rule) => rule.ruleId,
    ),
  );
  const replying = active.textRules.filter(
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

/** The check and its saved record read the same detached message and evidence. */
function capturePrepared(
  configuration: FirewallConfiguration,
  prepared: PreparedTextCheck,
) {
  return Effect.gen(function* () {
    const captured = yield* Effect.try({
      try: () =>
        structuredClone({
          direction: prepared.direction,
          messageJson: prepared.messageJson,
          event: { direction: prepared.direction, message: prepared.message },
          evidence: Object.assign({}, prepared.evidence, {
            facts: configuration.facts,
          }),
          agents: prepared.agents,
          outOfScope: prepared.outOfScope,
        }),
      catch: () => new InvalidCheckError({ reason: "evidence" }),
    });
    deepFreeze(captured.event);
    deepFreeze(captured.evidence);
    deepFreeze(captured.agents);
    return captured;
  });
}

function runTextCheck(
  capturedPolicy: CapturedMessagePolicy,
  active: ActivePolicy,
  captured: CapturedTextCheck,
) {
  return checkCapturedMessage(
    {
      textRules: active.textRules,
      outOfScope: captured.outOfScope,
      limits: capturedPolicy.limits,
      revision: active.revision,
      ...(capturedPolicy.text === undefined
        ? {}
        : {
            providerSettings: capturedPolicy.text.providerSettings,
            provider: capturedPolicy.text.provider,
          }),
    },
    { event: captured.event, evidence: captured.evidence },
  ).pipe(
    Effect.catchTag("InvalidCheckError", () => Effect.succeed(unavailable())),
  );
}

/** The revision, its contents and the evidence a check applied, as its record saves them. */
function recordInputJson(active: ActivePolicy, captured: CapturedTextCheck) {
  return JSON.stringify({
    revision: active.revision,
    revisionContents: active.revisionContents,
    evidence: captured.evidence,
    agents: captured.agents,
    message: captured.event.message,
  });
}

/** The summaries a host reads name the same captured rules and check as the decision. */
function decisionSummaries(
  active: ActivePolicy,
  captured: CapturedTextCheck,
  check: FirewallCheck,
): Effect.Effect<readonly DecisionSummary[], InvalidCheckError> {
  const deciding = decidingRules(check.reason, active.textRules, check);
  const decidedBy = deciding.map(
    (rule): DecisionSummary["decidedBy"][number] => ({
      ruleId: rule.ruleId,
      from: active.fixedRuleIds.includes(rule.ruleId) ? "fixed" : "editable",
      ...(rule.advice === undefined ? {} : { advice: rule.advice }),
    }),
  );
  const checkedRuleIds = [
    ...new Set(
      check.findings.flatMap((finding) =>
        finding.kind === "not-applicable" ? [] : [finding.ruleId],
      ),
    ),
  ];
  const message = captured.event.message;
  const peers =
    captured.direction === "outgoing" ? message.recipients : [message.sender];
  return Effect.forEach(
    peers,
    (identity) => {
      const peer = captured.agents.find((agent) => agent.agentId === identity);
      if (peer === undefined) {
        return Effect.fail(new InvalidCheckError({ reason: "evidence" }));
      }
      return Effect.succeed({
        direction: captured.direction,
        peer,
        judgment: check.judgment,
        reason: check.reason,
        decidedBy,
        checkedRuleIds,
        activeRevision: active.revision,
      });
    },
    { concurrency: 1 },
  );
}

/** A check id correlates a check without granting lookup authority. */
function mintCheckId() {
  return `chk_${randomBytes(32).toString("hex")}`;
}

/**
 * The guidance of an allowed incoming message the provider was unsure about.
 * Guidance over `MAXIMUM_RUNTIME_ITEM_BYTES` makes the check unavailable.
 */
function guidanceProjection(check: FirewallCheck, checkId: string) {
  if (check.instructions.length === 0) {
    return { check };
  }
  const guidance: CheckedGuidance = {
    checkId,
    instructions: check.instructions,
  };
  return Buffer.byteLength(JSON.stringify(guidance)) >
    MAXIMUM_RUNTIME_ITEM_BYTES
    ? { check: { ...unavailable(), findings: check.findings } }
    : { check, guidance };
}

/** A check that could not run holds without inventing a deciding rule. */
function unavailable(): FirewallCheck {
  return {
    judgment: "hold",
    reason: "check-unavailable",
    feedback: [],
    instructions: [],
    findings: [],
  };
}

function updatePolicy(
  policy: InstalledPolicy,
  request: typeof policyUpdateSchema.Type,
) {
  return policy.edits.withPermits(1)(
    Effect.gen(function* () {
      const { request: frozenRequest, change } = yield* resolvedChange(
        request,
        policy.captured.norms,
      );
      const editableRules = yield* editedRules(
        policy.active.editableRules,
        policy.active.fixedRuleIds,
        change,
      );
      const norms = yield* editedNorms(policy.active.norms, change);
      const replacement = yield* buildPolicy(
        policy.captured,
        editableRules,
        norms,
      );
      yield* policy.captured.approvePolicyUpdate(frozenRequest);
      return yield* Effect.uninterruptible(
        Effect.gen(function* () {
          yield* saveEditablePolicy(policy.store, editableRules, norms, {
            time: yield* Clock.currentTimeMillis,
            activeRevision: replacement.revision,
            addedRuleIds: change.addRules.map(({ rule }) => rule.id),
            removedRuleIds: change.removeRules.map(({ ruleId }) => ruleId),
          });
          yield* Effect.sync(() => {
            policy.active = replacement;
          });
          return { activeRevision: replacement.revision };
        }),
      );
    }),
  );
}

/**
 * One activation of the message policy: the configured fixed rules, the held
 * norms' rules and the editable rules, with the chain's matchers and the held
 * norms' own.
 */
function buildPolicy(
  capturedPolicy: CapturedMessagePolicy,
  editableRules: readonly TextRule[],
  norms: readonly HeldNorm[],
) {
  return Effect.gen(function* () {
    const compiled = yield* compiledNorms(capturedPolicy, norms);
    const fixed = [
      ...(capturedPolicy.text?.fixedRules ?? []),
      ...compiled.fixedRules,
    ];
    yield* validateIds(
      editableRules,
      fixed.map((rule) => rule.id),
    );
    const textRules = [...fixed, ...editableRules];
    if (textRules.length > 0 && capturedPolicy.text === undefined) {
      return yield* new FirewallPolicyError({ reason: "not-configured" });
    }
    const matchers = {
      ...(capturedPolicy.chain?.matchers ?? {}),
      ...compiled.matchers,
    };
    const chain =
      capturedPolicy.chain === undefined
        ? undefined
        : { ...capturedPolicy.chain, matchers };
    if (chain !== undefined) {
      yield* validateChain(chain);
    }
    const unmatched = textRules.find(
      (rule) =>
        rule.match !== undefined && !Object.hasOwn(matchers, rule.match),
    );
    if (unmatched !== undefined) {
      return yield* new FirewallPolicyError({
        reason: "build-failed",
        ruleId: unmatched.id,
      });
    }
    const revisionContents = {
      configuration: capturedPolicy.configuration,
      limits: capturedPolicy.limits,
      questionEdition: textQuestionEdition,
      chain: capturedPolicy.chain?.rules ?? [],
      textRules,
      fixedRuleIds: fixed.map((rule) => rule.id),
      ...(capturedPolicy.norms === undefined
        ? {}
        : { norms: norms.map((norm) => norm.document) }),
      ...(capturedPolicy.text === undefined
        ? {}
        : {
            providerSettings: capturedPolicy.text.providerSettings,
            provider: capturedPolicy.text.provider.provider ?? null,
            model: capturedPolicy.text.provider.model ?? null,
          }),
    };
    const revision = digest(revisionContents);
    yield* validateChecks({
      textRules,
      limits: capturedPolicy.limits,
      revision,
      providerSettings: capturedPolicy.text?.providerSettings,
    });
    return {
      revisionContents,
      editableRules,
      norms,
      revision,
      textRules,
      fixedRules: fixed,
      fixedRuleIds: revisionContents.fixedRuleIds,
      chain,
      matchers,
    };
  }).pipe(
    Effect.catchTag("InvalidCheckError", () =>
      Effect.fail(new FirewallPolicyError({ reason: "build-failed" })),
    ),
    Effect.catchAllDefect(() =>
      Effect.fail(new FirewallPolicyError({ reason: "build-failed" })),
    ),
  );
}

/**
 * Hashes the JSON text of what it is given. Every part of that is decoded
 * through a struct schema, which fixes the order of its members.
 */
function digest(hashed: unknown) {
  return createHash("sha256").update(JSON.stringify(hashed)).digest("hex");
}

/**
 * What the held norms add, frozen like configured rules. Norms the daemon's
 * norms cannot compile together fail `build-failed`.
 */
function compiledNorms(
  capturedPolicy: CapturedMessagePolicy,
  norms: readonly HeldNorm[],
) {
  if (capturedPolicy.norms === undefined) {
    return norms.length === 0
      ? Effect.succeed(noNorms)
      : Effect.fail(new FirewallPolicyError({ reason: "build-failed" }));
  }
  return Option.match(
    capturedPolicy.norms.compile(norms.map((norm) => norm.document)),
    {
      onNone: () =>
        Effect.fail(new FirewallPolicyError({ reason: "build-failed" })),
      onSome: (compiled) =>
        decodeFrozen(Schema.Array(TextRule), compiled.fixedRules).pipe(
          Effect.map((fixedRules) => ({ ...compiled, fixedRules })),
          Effect.catchTag("ParseError", () =>
            Effect.fail(new FirewallPolicyError({ reason: "build-failed" })),
          ),
        ),
    },
  );
}

function saveEditablePolicy(
  store: FirewallStore,
  editableRules: readonly TextRule[],
  norms: readonly HeldNorm[],
  activation?: FirewallActivation,
) {
  return Effect.sync(() =>
    JSON.stringify({
      format: storedPolicyFormat,
      rules: editableRules,
      norms: norms.map((norm) => norm.document),
    }),
  ).pipe(
    Effect.flatMap((editablePolicyJson) =>
      store.writeEditablePolicy(editablePolicyJson, activation),
    ),
  );
}

const activityKind = "firewall-check";
/** How a marked check ended. */
type ActivityEnding = "checked" | "interrupted" | "failed";

/** Pairs a pending and a settled span around one check, even when it fails or is cancelled. */
function markCheckActivity(direction: Direction) {
  return <A, E, R>(work: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
    Effect.uninterruptibleMask((restore) =>
      checkPending(direction).pipe(
        Effect.zipRight(restore(work)),
        Effect.onExit((exit) => {
          if (Exit.isSuccess(exit)) {
            return checkSettled(direction, "checked");
          }
          return checkSettled(
            direction,
            Exit.isInterrupted(exit) ? "interrupted" : "failed",
          );
        }),
      ),
    );
}

/** Opens the span that `checkSettled` closes for the same direction. */
function checkPending(direction: Direction): Effect.Effect<void> {
  return Effect.withSpan(Effect.void, "retry.pending", {
    captureStackTrace: false,
    attributes: { kind: activityKind, id: direction },
  });
}

/** Closes the span `checkPending` opened. */
function checkSettled(
  direction: Direction,
  ending: ActivityEnding,
): Effect.Effect<void> {
  return Effect.withSpan(Effect.void, "retry.settled", {
    captureStackTrace: false,
    attributes: { kind: activityKind, id: direction, outcome: ending },
  });
}
