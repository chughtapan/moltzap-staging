/** @file Recovers conversation history and ordering when starting or replacing the Router message feed. */

import type * as SqlClient from "@effect/sql/SqlClient";
import {
  Duration,
  Effect,
  ExecutionStrategy,
  Exit,
  FiberMap,
  Schedule,
  Scope,
} from "effect";
import type { SignedMessage } from "../../../identity/envelope/signed-message.js";
import type {
  EndpointRecovery,
  StoredOutboundMessage,
} from "../../store/index.js";
import {
  ClientRepresentationError,
  decodeCanonical,
} from "../../../encoding/canonical-json.js";
import {
  type ConversationId as ConversationIdValue,
  type DecodedOuterBody,
  type DirectPacket,
  EvidenceStatement,
  type EvidenceStatement as EvidenceStatementValue,
  hashAnchor,
  memberCard,
  type VerifiedMembership,
  verifyDeliveredEvidence,
} from "../../records/index.js";
import {
  type RouterIngressDisposition,
  type RouterWorkerIngress,
  RouterWorkerPersistenceError,
  type RouterWorkerRecovery,
  RouterWorkerRecoveryError,
} from "../../router/index.js";
import * as Store from "../../store/index.js";
import { type EngineRuntime, evidenceTarget } from "../engine-context.js";
import {
  durableRouterInstanceId,
  verifyRecoveredHistory,
  verifyStoredMemberships,
  verifyStoredOutbounds,
} from "../history/index.js";
import {
  acceptCompletedReanchor,
  acceptReanchorVote,
  positionReady as reanchorPositionReady,
  type ReanchorRun,
  startReanchorRun,
} from "../reanchor/index.js";
import {
  exitOutcome,
  markRetries,
  type RetryOutcome,
  retryPending,
  retrySettled,
} from "../retry-markers.js";
import {
  completeRecoveryBarrier,
  currentRecoveryBarrier,
  fenceConversation,
  pendingRecoveryFence,
  releaseConversation,
} from "./barrier.js";
import {
  acceptCatchUpIncomplete,
  acceptCatchUpPage,
  acceptCatchUpRequest,
  type CatchUpResponder,
  type CatchUpRun,
  makeCatchUpState,
  queueStagedSuccessor,
  requestCertifiedHistory,
  resendCertifiedHistoryRequest,
  sentByOtherMember,
  settleOnQuorum,
  waitBehindEarlierReanchor,
} from "./catch-up.js";
import {
  acceptSuccessorRecord,
  acceptSuccessorVote,
  type SuccessorRun,
} from "./successor.js";

/**
 * The recovery fences the engine installs at a discontinuity and a send waits
 * behind.
 */
export { installRecoveryBarrier, pendingRecoveryFence } from "./barrier.js";

/**
 * Delay before a conversation's first catch-up retry. One second is well past
 * a Router round trip, so a retry follows a request that went unanswered
 * rather than racing its answers.
 */
const catchUpRetryBase = Duration.seconds(1);

/**
 * Catch-up retries after a conversation's first request. With the doubling
 * delay, eight retries span about four minutes (1 s + 2 s + ... + 128 s,
 * each jittered). After the last one the conversation stays paused until a
 * member's traffic for it, a local message into it, the Router worker
 * reattaching, or the next recovery run arms it again.
 */
export const catchUpRetryAttempts = 8;

/**
 * A conversation's catch-up retries, and an author's resends of a proposal
 * whose action certificate is incomplete: doubling jittered delays, at most
 * {@link catchUpRetryAttempts} of them. Recovering the conversation removes
 * its catch-up retries from the run, which ends them early.
 */
export const catchUpRetrySchedule = Schedule.exponential(catchUpRetryBase).pipe(
  Schedule.jittered,
  Schedule.intersect(Schedule.recurs(catchUpRetryAttempts)),
);

/**
 * One authenticated recovery run. It holds each conversation that has not
 * recovered behind its own fence; the others carry on. Its catch-up and
 * re-anchor reach it only through the ports built here.
 */
interface RecoveryRun {
  readonly runtime: EngineRuntime;
  readonly recovery: RouterWorkerRecovery;
  /**
   * Conversations whose durable anchor names a Router instance other than the
   * recovery anchor. Only these re-anchor; the rest recover by catch-up alone.
   */
  readonly reanchoring: ReadonlySet<string>;
  readonly memberships: Map<ConversationIdValue, VerifiedMembership>;
  /**
   * Outbox ids of each conversation's retained envelopes, resumed and dropped
   * once it recovers.
   */
  readonly retainedOutbounds: Map<string, readonly string[]>;
  readonly completedConversations: Set<ConversationIdValue>;
  /**
   * Restarted conversations whose re-anchor `retry.pending` is open: their
   * position is ready and they wait for a quorum to certify the re-anchor.
   * Recovering the conversation, or closing the run's scope, settles each.
   */
  readonly reanchorWaits: Set<ConversationIdValue>;
  /**
   * The run's lifetime inside the engine's. Closing it ends every catch-up
   * retry the run started.
   */
  readonly scope: Scope.CloseableScope;
  /**
   * Each conversation's running catch-up retries. A pending conversation
   * missing here has used up its retries and waits for traffic to re-arm it.
   */
  readonly retries: FiberMap.FiberMap<ConversationIdValue>;
  /** Staged successors members have sent that this endpoint has not staged. */
  readonly successor: SuccessorRun;
  readonly catchUp: CatchUpRun;
  readonly reanchor: ReanchorRun;
}

const activeRuns = new WeakMap<EngineRuntime, RecoveryRun>();

const ignoredDisposition: RouterIngressDisposition = "ignored";

/**
 * Accept one verified Router delivery, whether the Router worker polls it
 * while it recovers or while it is active. Catch-up and re-anchor traffic
 * goes to the recovery run, and a member's catch-up request is always
 * answered. Every other value goes to the protocol phases unless its
 * conversation is fenced: while a discontinuity has no recovery run, every
 * conversation is, and afterwards each one the run has not recovered. A
 * fenced conversation's certified records are applied through recovery, and
 * its action traffic is ignored. A member's traffic for a fenced conversation
 * whose catch-up retries ran out arms them again.
 * @param runtime Engine whose state receives the ingress.
 * @param ingress Verified Router delivery and decoded private payload.
 * @returns Whether the payload was accepted or safely ignored.
 */
export function acceptEngineIngressWithRecovery(
  runtime: EngineRuntime,
  ingress: RouterWorkerIngress<DecodedOuterBody>,
): Effect.Effect<
  RouterIngressDisposition,
  RouterWorkerPersistenceError,
  SqlClient.SqlClient
> {
  const { payload } = ingress;
  return (
    payload.kind === "evidence"
      ? acceptEvidence(runtime, ingress, payload.message)
      : acceptPacket(runtime, ingress, payload.packet)
  ).pipe(Effect.withSpan("acceptEngineIngressWithRecovery"));
}

/**
 * Start catch-up again for paused conversations: those the recovery run
 * still holds whose capped retries ran out. A local send to such a
 * conversation arms its catch-up, and a Router worker that reattaches after
 * an outage arms every one, so a paused conversation always has a way back to
 * recovery. The requests go out on a fiber in the run's scope, so neither the
 * send nor the reattaching worker waits on the store reads they take. A store
 * failure while asking ends the new retries the same way.
 * @param runtime Engine whose recovery run holds the conversations.
 * @param conversationId The one conversation to arm; every paused
 *     conversation when omitted.
 * @returns Completion once the arming fiber is started.
 */
export function rearmPausedCatchUp(
  runtime: EngineRuntime,
  conversationId?: ConversationIdValue,
): Effect.Effect<void, never, SqlClient.SqlClient> {
  return Effect.suspend(() => {
    const run = activeRuns.get(runtime);
    if (run === undefined) {
      return Effect.void;
    }
    const conversations =
      conversationId === undefined
        ? [...run.memberships.keys()]
        : [conversationId];
    return Effect.forEach(
      conversations,
      (paused) => rearmIfPaused(runtime, paused),
      { concurrency: 1, discard: true },
    ).pipe(
      Effect.catchAll(() =>
        Effect.logWarning(
          "Catch-up was not armed again: the endpoint store could not be read",
        ),
      ),
      Effect.withSpan("rearmPausedCatchUp"),
      Effect.forkIn(run.scope),
      Effect.asVoid,
    );
  });
}

/**
 * Restart catch-up from a newly promoted head while its conversation is fenced.
 * Before a run starts, its initial request will use the durable head instead.
 * @param runtime Engine whose protocol phases promoted the record.
 * @param conversationId Conversation whose head advanced.
 * @returns Completion once a fresh request and retry schedule are installed.
 */
export function recoveryHeadAdvanced(
  runtime: EngineRuntime,
  conversationId: ConversationIdValue,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient.SqlClient> {
  const run = activeRuns.get(runtime);
  return run !== undefined && isRecovering(runtime, conversationId)
    ? armCatchUp(run, conversationId)
    : Effect.void;
}

/**
 * Whether a conversation is still fenced: by the engine fence before a run
 * has fenced each conversation, or by its own fence until it recovers.
 * @param runtime Engine whose recovery fences are read.
 * @param conversationId Conversation to check.
 * @returns True while the conversation's traffic waits for its recovery.
 */
export function isRecovering(
  runtime: EngineRuntime,
  conversationId: ConversationIdValue,
): boolean {
  return pendingRecoveryFence(runtime, conversationId) !== undefined;
}

/**
 * Reconcile every certified chain and threshold-anchor a restarted Router.
 * It verifies durable history, fences each conversation, asks its members
 * for catch-up, and returns: the Router worker resumes normal ingress while
 * each conversation recovers on its own, so one that cannot recover holds no
 * other. A run that starts replaces the engine's previous one.
 *
 * Re-anchoring is reserved for a conversation whose durable anchor names a
 * Router instance other than the recovery anchor. A daemon cold start reports
 * `router_restarted` because it has no prior instance in memory; conversations
 * still anchored to the polled instance recover by catch-up alone.
 * @param runtime Engine whose durable histories require reconciliation.
 * @param recoveryInput Authenticated Router recovery reason and new anchor.
 * @returns Completion once the run holds and has asked for every conversation.
 */
export const recoverCertifiedHistory = (
  runtime: EngineRuntime,
  recoveryInput: RouterWorkerRecovery,
): Effect.Effect<void, RouterWorkerRecoveryError, SqlClient.SqlClient> =>
  Effect.gen(function* () {
    const barrier = currentRecoveryBarrier(runtime);
    if (barrier === undefined) {
      return yield* Effect.fail(recoveryFailure());
    }
    yield* endRun(runtime);
    const recovered = yield* Store.recover().pipe(
      Effect.mapError(recoveryFailure),
    );
    const memberships = yield* recoverMemberships(runtime, recovered);
    yield* verifyRecoveredHistory(recovered, memberships);
    const reanchoring = yield* reanchoringConversations(
      recoveryInput,
      recovered,
      memberships,
    );
    const retained = yield* prepareRecoveryOutbox(
      runtime,
      recovered,
      memberships,
      reanchoring,
    );
    const run = yield* makeRecoveryRun(runtime, recoveryInput, {
      memberships,
      reanchoring,
      retainedOutbounds: groupByConversation(retained),
    });
    yield* Effect.sync(() => {
      activeRuns.set(runtime, run);
    });
    yield* Effect.forEach(
      memberships.keys(),
      (conversationId) => fenceConversation(runtime, conversationId),
      { concurrency: 1, discard: true },
    );
    yield* Effect.forEach(
      memberships.keys(),
      (conversationId) => armCatchUp(run, conversationId),
      { concurrency: 1, discard: true },
    ).pipe(Effect.mapError(recoveryFailure));
    yield* endRunOnceRecovered(run);
    yield* completeRecoveryBarrier(runtime, barrier);
  }).pipe(Effect.withSpan("recoverCertifiedHistory"));

function acceptPacket(
  runtime: EngineRuntime,
  ingress: RouterWorkerIngress<DecodedOuterBody>,
  packet: DirectPacket,
): Effect.Effect<
  RouterIngressDisposition,
  RouterWorkerPersistenceError,
  SqlClient.SqlClient
> {
  return routePacket(runtime, ingress, packet).pipe(
    Effect.tap(() =>
      rearmOnMemberTraffic(runtime, ingress, packetConversation(packet)),
    ),
  );
}

function routePacket(
  runtime: EngineRuntime,
  ingress: RouterWorkerIngress<DecodedOuterBody>,
  packet: DirectPacket,
): Effect.Effect<
  RouterIngressDisposition,
  RouterWorkerPersistenceError,
  SqlClient.SqlClient
> {
  switch (packet.kind) {
    case "catch_up_request":
      return acceptCatchUpRequest(catchUpResponder(runtime), ingress, packet);
    case "catch_up_page":
    case "catch_up_incomplete":
    case "completed_reanchor":
      return acceptRunPacket(runtime, ingress, packet);
    case "certified_record":
    case "action_proposal":
    case "action_certified_record":
      return acceptHistoryPacket(runtime, ingress, packet);
    default: {
      const exhaustive: never = packet;
      return exhaustive;
    }
  }
}

/**
 * Route a history or action packet by its conversation's fence. A fenced
 * conversation's certified records are applied through recovery, an
 * action-certified record extending its head goes to the run's staged
 * successors, and its proposals are ignored; an open conversation's go to
 * the protocol phases.
 * @param runtime Engine whose state receives the packet.
 * @param ingress Verified Router delivery carrying the packet.
 * @param packet The certified record or action packet.
 * @returns Whether the packet was accepted or safely ignored.
 */
function acceptHistoryPacket(
  runtime: EngineRuntime,
  ingress: RouterWorkerIngress<DecodedOuterBody>,
  packet: Extract<
    DirectPacket,
    {
      readonly kind:
        | "certified_record"
        | "action_proposal"
        | "action_certified_record";
    }
  >,
): Effect.Effect<
  RouterIngressDisposition,
  RouterWorkerPersistenceError,
  SqlClient.SqlClient
> {
  if (!isRecovering(runtime, packetConversation(packet))) {
    return runtime.phases.acceptIngress(runtime, ingress);
  }
  const run = activeRuns.get(runtime);
  switch (packet.kind) {
    case "certified_record":
      return runtime.phases.acceptRecoveryIngress(runtime, ingress);
    case "action_certified_record":
      return run === undefined
        ? Effect.succeed(ignoredDisposition)
        : acceptSuccessorRecord(run.successor, ingress, packet);
    case "action_proposal":
      return Effect.succeed(ignoredDisposition);
    default: {
      const exhaustive: never = packet;
      return exhaustive;
    }
  }
}

/**
 * Route one evidence message by the conversation its statement names. A
 * re-anchor vote goes to the recovery run. A durability vote for a fenced
 * conversation goes to the run's staged successors, and its action
 * signatures are ignored, as its proposals are; an action signature for a
 * proposal this endpoint has not seen names no conversation yet and goes to
 * the protocol phases, which ignore it.
 * @param runtime Engine whose state receives the evidence.
 * @param ingress Verified Router delivery carrying the evidence.
 * @param message The evidence message from the outer envelope.
 * @returns Whether the evidence was accepted or safely ignored.
 */
function acceptEvidence(
  runtime: EngineRuntime,
  ingress: RouterWorkerIngress<DecodedOuterBody>,
  message: SignedMessage,
): Effect.Effect<
  RouterIngressDisposition,
  RouterWorkerPersistenceError,
  SqlClient.SqlClient
> {
  if (
    !activeRuns.has(runtime) &&
    currentRecoveryBarrier(runtime) === undefined
  ) {
    return runtime.phases.acceptIngress(runtime, ingress);
  }
  return decodeCanonical(EvidenceStatement, message.body).pipe(
    Effect.flatMap((statement) => {
      const conversationId = evidenceTarget(runtime, statement).conversationId;
      return routeEvidence(runtime, ingress, {
        message,
        statement,
        fenced:
          conversationId !== undefined && isRecovering(runtime, conversationId),
      }).pipe(
        Effect.tap(() =>
          conversationId === undefined
            ? Effect.void
            : rearmOnMemberTraffic(runtime, ingress, conversationId),
        ),
      );
    }),
    Effect.catchTag("ClientRepresentationError", () =>
      Effect.succeed(ignoredDisposition),
    ),
  );
}

function routeEvidence(
  runtime: EngineRuntime,
  ingress: RouterWorkerIngress<DecodedOuterBody>,
  evidence: Readonly<{
    message: SignedMessage;
    statement: EvidenceStatementValue;
    fenced: boolean;
  }>,
): Effect.Effect<
  RouterIngressDisposition,
  RouterWorkerPersistenceError,
  SqlClient.SqlClient
> {
  const { message, statement } = evidence;
  const run = activeRuns.get(runtime);
  if (statement.kind === "reanchor_vote") {
    return run !== undefined &&
      statement.reanchor.routerInstanceId !==
        run.recovery.anchor.routerInstanceId
      ? noteEarlierReanchor(run, ingress, { message, statement })
      : acceptRunVote(runtime, ingress, message, statement);
  }
  if (!evidence.fenced) {
    return runtime.phases.acceptIngress(runtime, ingress, statement);
  }
  return run !== undefined && statement.kind === "durability_vote"
    ? acceptSuccessorVote(run.successor, ingress, { message, statement })
    : Effect.succeed(ignoredDisposition);
}

/**
 * Take a member's re-anchor vote for a Router instance other than the run's.
 * The run's re-anchor cannot use it, but a pending position it selects waits
 * for every member: that re-anchor may have completed at a member that has
 * not answered yet. Only a vote that verifies as the member's, names the
 * conversation's membership, and hashes to the anchor it signs counts.
 * @param run The active recovery run.
 * @param ingress Router delivery carrying the vote.
 * @param vote The vote's evidence message and its decoded statement.
 * @param vote.message The vote's evidence message.
 * @param vote.statement The vote's decoded statement.
 * @returns Ignored, since the vote counts toward no re-anchor of this run.
 */
function noteEarlierReanchor(
  run: RecoveryRun,
  ingress: RouterWorkerIngress<DecodedOuterBody>,
  vote: Readonly<{
    message: SignedMessage;
    statement: Extract<
      EvidenceStatementValue,
      { readonly kind: "reanchor_vote" }
    >;
  }>,
): Effect.Effect<RouterIngressDisposition, never, SqlClient.SqlClient> {
  const body = vote.statement.reanchor;
  const membership = run.memberships.get(body.conversationId);
  if (membership === undefined || body.membershipHash !== membership.hash) {
    return Effect.succeed(ignoredDisposition);
  }
  return waitBehindEarlierReanchor(
    run.catchUp,
    body,
    verifyDeliveredEvidence({
      outer: ingress.message,
      evidence: vote.message,
      statement: vote.statement,
      membership,
    }).pipe(
      Effect.zipRight(hashAnchor(body)),
      Effect.filterOrFail(
        (anchorHash) => anchorHash === vote.statement.anchorHash,
        () => new ClientRepresentationError(),
      ),
    ),
  ).pipe(
    Effect.catchTag("ClientRepresentationError", () => Effect.void),
    Effect.as(ignoredDisposition),
  );
}

function acceptRunVote(
  runtime: EngineRuntime,
  ingress: RouterWorkerIngress<DecodedOuterBody>,
  message: SignedMessage,
  statement: EvidenceStatementValue,
): Effect.Effect<
  RouterIngressDisposition,
  RouterWorkerPersistenceError,
  SqlClient.SqlClient
> {
  const run = activeRuns.get(runtime);
  return run === undefined
    ? Effect.succeed(ignoredDisposition)
    : acceptReanchorVote(run.reanchor, ingress, message, statement);
}

function acceptRunPacket(
  runtime: EngineRuntime,
  ingress: RouterWorkerIngress<DecodedOuterBody>,
  packet: Extract<
    DirectPacket,
    {
      readonly kind:
        | "catch_up_page"
        | "catch_up_incomplete"
        | "completed_reanchor";
    }
  >,
): Effect.Effect<
  RouterIngressDisposition,
  RouterWorkerPersistenceError,
  SqlClient.SqlClient
> {
  const run = activeRuns.get(runtime);
  if (run === undefined) {
    return Effect.succeed(ignoredDisposition);
  }
  switch (packet.kind) {
    case "catch_up_page":
      return acceptCatchUpPage(run.catchUp, ingress, packet);
    case "catch_up_incomplete":
      return acceptCatchUpIncomplete(run.catchUp, ingress, packet);
    case "completed_reanchor":
      return acceptCompletedReanchor(run.reanchor, ingress, packet);
    default: {
      const exhaustive: never = packet;
      return exhaustive;
    }
  }
}

/**
 * Arm a conversation's catch-up again after its retries ran out, on traffic
 * one of its other fixed members sent for it.
 * @param runtime Engine whose run holds the conversation.
 * @param ingress Verified Router delivery the member sent.
 * @param conversationId Conversation the delivery names.
 * @returns Completion once the conversation's catch-up is armed again.
 */
function rearmOnMemberTraffic(
  runtime: EngineRuntime,
  ingress: RouterWorkerIngress<DecodedOuterBody>,
  conversationId: ConversationIdValue,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient.SqlClient> {
  const membership = activeRuns.get(runtime)?.memberships.get(conversationId);
  return membership !== undefined &&
    sentByOtherMember(runtime, membership, ingress.message.senderAgentId)
    ? rearmIfPaused(runtime, conversationId)
    : Effect.void;
}

/**
 * Arm catch-up again for a conversation the active run still holds whose
 * retries ran out. A conversation the run has recovered, or one whose retries
 * are still running, needs nothing.
 * @param runtime Engine whose run holds the conversation.
 * @param conversationId Conversation to arm.
 * @returns Completion once the conversation's catch-up is armed again.
 */
function rearmIfPaused(
  runtime: EngineRuntime,
  conversationId: ConversationIdValue,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient.SqlClient> {
  const run = activeRuns.get(runtime);
  if (
    run === undefined ||
    !run.memberships.has(conversationId) ||
    run.completedConversations.has(conversationId)
  ) {
    return Effect.void;
  }
  return FiberMap.has(run.retries, conversationId).pipe(
    Effect.flatMap((retrying) =>
      retrying ? Effect.void : armCatchUp(run, conversationId),
    ),
  );
}

/**
 * Ask a conversation's members for its history now, then retry on
 * {@link catchUpRetrySchedule} until it recovers or the retries run out. The
 * retries start only if the conversation has not recovered while the first
 * request was being queued, since a re-arm runs beside the ingress that can
 * recover it. A retry whose store read fails ends the retries; the
 * conversation waits for traffic to arm them again.
 * @param run Recovery run that holds the conversation.
 * @param conversationId Conversation to catch up.
 * @returns Completion once the first request is queued and the retries run.
 */
function armCatchUp(
  run: RecoveryRun,
  conversationId: ConversationIdValue,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient.SqlClient> {
  return requestCertifiedHistory(run.catchUp, conversationId).pipe(
    Effect.zipRight(
      Effect.suspend(() =>
        run.completedConversations.has(conversationId)
          ? Effect.void
          : FiberMap.run(
              run.retries,
              conversationId,
              retryCatchUp(run, conversationId),
            ),
      ),
    ),
    Effect.asVoid,
  );
}

/**
 * Resend a conversation's request on {@link catchUpRetrySchedule}, then let a
 * quorum of answers settle a position that waited for every member. A
 * position a quorum already settles recovers on its own fiber in the run's
 * scope: recovering the conversation removes this retry fiber, which it could
 * not do from inside it. That recovery is bracketed as catch-up of its own,
 * opened before the retries' bracket closes, so a trace reader keeps the
 * conversation busy until it recovers the conversation or hands it on to a
 * re-anchor round.
 * @param run Recovery run that holds the conversation.
 * @param conversationId Conversation to catch up.
 * @returns The retries, ending once any settled position's recovery started.
 */
function retryCatchUp(
  run: RecoveryRun,
  conversationId: ConversationIdValue,
): Effect.Effect<void, never, SqlClient.SqlClient> {
  return Effect.schedule(
    resendCertifiedHistoryRequest(run.catchUp, conversationId),
    catchUpRetrySchedule,
  ).pipe(
    Effect.zipRight(
      Effect.sync(() => settleOnQuorum(run.catchUp, conversationId)),
    ),
    Effect.tap((settled) =>
      settled ? recoverOnQuorum(run, conversationId) : Effect.void,
    ),
    markRetries("catch-up", conversationId, (exit) =>
      catchUpOutcome(
        run,
        conversationId,
        exit,
        Exit.isSuccess(exit) && exit.value ? "superseded" : "gave-up",
      ),
    ),
    Effect.asVoid,
    Effect.catchAll(() =>
      Effect.logWarning(
        "Catch-up retries stopped: the endpoint store could not be read",
      ),
    ),
  );
}

/**
 * Recover a position a quorum settled, on its own fiber in the run's scope.
 * Its `retry.pending` is recorded here, before the fork and so before the
 * retries it takes over from settle, so a trace reader never sees the
 * conversation idle between them.
 * @param run Recovery run that holds the conversation.
 * @param conversationId Conversation whose position a quorum settled.
 * @returns Completion once the recovery is forked.
 */
function recoverOnQuorum(
  run: RecoveryRun,
  conversationId: ConversationIdValue,
): Effect.Effect<void, never, SqlClient.SqlClient> {
  return Effect.uninterruptibleMask((restore) =>
    retryPending("catch-up", conversationId).pipe(
      Effect.zipRight(
        restore(run.catchUp.onPositionReady(conversationId)).pipe(
          Effect.onExit((exit) =>
            retrySettled(
              "catch-up",
              conversationId,
              catchUpOutcome(run, conversationId, exit, "superseded"),
            ),
          ),
          Effect.catchAll(() =>
            Effect.logWarning(
              "Catch-up did not settle on a quorum: the endpoint store failed",
            ),
          ),
          Effect.forkIn(run.scope),
        ),
      ),
      Effect.asVoid,
    ),
  );
}

/**
 * How a conversation's catch-up retries, or the quorum recovery that took
 * over from them, ended.
 * @param run Recovery run that held the conversation.
 * @param conversationId Conversation being caught up.
 * @param exit How the bracketed fiber ended.
 * @param returned The outcome when it returned without recovering the
 *     conversation: `gave-up` for retries that ran out, `superseded` for a
 *     quorum recovery that handed the conversation to a re-anchor round.
 * @returns `recovered` once the conversation recovered, which removes its
 *     retries; `returned`; `failed` when a store read or defect ended it; or
 *     `interrupted` when the run, or a fresh arm of the same conversation,
 *     replaced it.
 */
function catchUpOutcome(
  run: RecoveryRun,
  conversationId: ConversationIdValue,
  exit: Exit.Exit<unknown, unknown>,
  returned: RetryOutcome,
): RetryOutcome {
  return run.completedConversations.has(conversationId)
    ? "recovered"
    : exitOutcome(exit, returned);
}

/**
 * Select conversations whose durable anchor names another Router instance.
 * @param recovery Router discontinuity and the polled recovery anchor.
 * @param snapshot Exact durable state captured before recovery starts.
 * @param memberships Verified membership for every retained conversation.
 * @returns Conversations that must re-anchor; empty unless the reason is a restart.
 */
function reanchoringConversations(
  recovery: RouterWorkerRecovery,
  snapshot: EndpointRecovery,
  memberships: ReadonlyMap<ConversationIdValue, VerifiedMembership>,
): Effect.Effect<
  ReadonlySet<string>,
  RouterWorkerRecoveryError,
  SqlClient.SqlClient
> {
  if (recovery.reason !== "router_restarted") {
    return Effect.succeed(new Set<string>());
  }
  return Effect.filter(
    memberships.values(),
    (membership) =>
      durableRouterInstanceId(membership, snapshot).pipe(
        Effect.map((instance) => instance !== recovery.anchor.routerInstanceId),
      ),
    { concurrency: 1 },
  ).pipe(
    Effect.map(
      (changed) =>
        new Set<string>(
          changed.map((membership) => membership.descriptor.conversationId),
        ),
    ),
    Effect.withSpan("reanchoringConversations"),
  );
}

/**
 * Verify retained outer envelopes and invalidate only rows bound to an old Router.
 * @param runtime Engine whose local identity authored the retained envelopes.
 * @param snapshot Exact durable state captured before recovery starts.
 * @param memberships Verified membership for every retained conversation.
 * @param reanchoring Conversations anchored to another Router instance.
 * @returns Same-instance rows that must resume with their stable outbox identity.
 */
function prepareRecoveryOutbox(
  runtime: EngineRuntime,
  snapshot: EndpointRecovery,
  memberships: ReadonlyMap<ConversationIdValue, VerifiedMembership>,
  reanchoring: ReadonlySet<string>,
): Effect.Effect<
  readonly StoredOutboundMessage[],
  RouterWorkerRecoveryError,
  SqlClient.SqlClient
> {
  return verifyStoredOutbounds(
    runtime.input,
    snapshot.outboundMessages,
    memberships,
  ).pipe(
    Effect.mapError(recoveryFailure),
    Effect.flatMap((outbounds) =>
      discardRestartedOutbounds(
        outbounds.filter((outbound) =>
          reanchoring.has(outbound.conversationId),
        ),
      ).pipe(
        Effect.as(
          outbounds.filter(
            (outbound) => !reanchoring.has(outbound.conversationId),
          ),
        ),
      ),
    ),
    Effect.withSpan("prepareRecoveryOutbox"),
  );
}

/**
 * Retire verified envelopes whose conversations require a new Router anchor.
 * @param outbounds Envelopes bound to the replaced Router instance.
 * @returns Completion once the stale envelopes are inactive.
 */
function discardRestartedOutbounds(
  outbounds: readonly StoredOutboundMessage[],
): Effect.Effect<void, RouterWorkerRecoveryError, SqlClient.SqlClient> {
  return Store.discardOutbound(outbounds).pipe(
    Effect.mapError(recoveryFailure),
    Effect.asVoid,
  );
}

function groupByConversation(
  outbounds: readonly StoredOutboundMessage[],
): Map<string, readonly string[]> {
  const grouped = new Map<string, string[]>();
  for (const { conversationId, envelopeId } of outbounds) {
    const held = grouped.get(conversationId) ?? [];
    held.push(envelopeId);
    grouped.set(conversationId, held);
  }
  return grouped;
}

/**
 * Allocate one run's scope, retries and completion accounting, and build the
 * catch-up and re-anchor ports its phases run against.
 * @param runtime Engine the run recovers.
 * @param recovery Router discontinuity reason and anchor.
 * @param history Verified memberships that must be reconciled, the
 *     conversations among them anchored to a different Router instance, and
 *     each conversation's retained envelopes.
 * @returns The run, not yet installed.
 */
function makeRecoveryRun(
  runtime: EngineRuntime,
  recovery: RouterWorkerRecovery,
  history: Pick<
    RecoveryRun,
    "memberships" | "reanchoring" | "retainedOutbounds"
  >,
): Effect.Effect<RecoveryRun, never, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const scope = yield* Scope.fork(
      runtime.scope,
      ExecutionStrategy.sequential,
    );
    const retries = yield* FiberMap.make<ConversationIdValue>().pipe(
      Scope.extend(scope),
    );
    const reanchorWaits = new Set<ConversationIdValue>();
    yield* Effect.addFinalizer(() => settleReanchorWaits(reanchorWaits)).pipe(
      Scope.extend(scope),
    );
    const isActive = () => activeRuns.get(runtime)?.scope === scope;
    const run: RecoveryRun = {
      runtime,
      recovery,
      ...history,
      completedConversations: new Set(),
      reanchorWaits,
      scope,
      retries,
      successor: successorPort(runtime, history),
      catchUp: catchUpPort(
        runtime,
        { recovery, memberships: history.memberships },
        isActive,
        () => run,
      ),
      reanchor: reanchorPort(
        runtime,
        { recovery, ...history },
        isActive,
        () => run,
      ),
    };
    return run;
  }).pipe(Effect.withSpan("makeRecoveryState"));
}

/**
 * Settle every re-anchor wait still open as `interrupted`, when its run ends
 * or the engine stops.
 * @param waits The run's open re-anchor waits, emptied here.
 * @returns Completion once each is settled.
 */
function settleReanchorWaits(
  waits: Set<ConversationIdValue>,
): Effect.Effect<void, never, SqlClient.SqlClient> {
  return Effect.suspend(() => {
    const open = [...waits];
    waits.clear();
    return Effect.forEach(
      open,
      (conversationId) =>
        retrySettled("reanchor", conversationId, "interrupted"),
      { concurrency: 1, discard: true },
    );
  });
}

/**
 * Build a run's catch-up port.
 * @param runtime Engine the run recovers.
 * @param context The run's discontinuity and the memberships it reconciles.
 * @param isActive Whether the run is still the engine's active recovery.
 * @param currentRun The run the port belongs to, once it is built.
 * @returns The port the run's catch-up requests, answers, and readiness use.
 */
function catchUpPort(
  runtime: EngineRuntime,
  context: Pick<RecoveryRun, "recovery" | "memberships">,
  isActive: () => boolean,
  currentRun: () => RecoveryRun,
): CatchUpRun {
  return {
    ...catchUpResponder(runtime),
    state: makeCatchUpState(),
    isActive,
    routerInstanceId: context.recovery.anchor.routerInstanceId,
    membership: (conversationId) => context.memberships.get(conversationId),
    onPositionReady: (conversationId) =>
      positionReady(currentRun(), conversationId).pipe(
        Effect.withSpan("positionReady"),
      ),
  };
}

/**
 * Build a run's re-anchor port.
 * @param runtime Engine the run recovers.
 * @param context The run's discontinuity, memberships, and the
 *     conversations it re-anchors.
 * @param isActive Whether the run is still the engine's active recovery.
 * @param currentRun The run the port belongs to, once it is built.
 * @returns The run's re-anchor.
 */
function reanchorPort(
  runtime: EngineRuntime,
  context: Pick<RecoveryRun, "recovery" | "memberships" | "reanchoring">,
  isActive: () => boolean,
  currentRun: () => RecoveryRun,
): ReanchorRun {
  const sender = catchUpResponder(runtime);
  return startReanchorRun({
    runtime,
    reason: context.recovery.reason,
    routerInstanceId: context.recovery.anchor.routerInstanceId,
    reanchoring: context.reanchoring,
    isActive,
    membership: (conversationId) => context.memberships.get(conversationId),
    isRecovered: (conversationId) =>
      currentRun().completedConversations.has(conversationId),
    markRecovered: (conversationId) =>
      markRecovered(currentRun(), conversationId),
    queue: sender.queue,
    requestCatchUp: (conversationId) =>
      requestCertifiedHistory(currentRun().catchUp, conversationId),
    resendStagedSuccessor: (membership, staged) =>
      queueStagedSuccessor(sender, membership, staged),
  });
}

/**
 * The catch-up responder. It answers from the active run's memberships, or
 * from the engine's conversations when no run is active.
 * @param runtime Engine that answers.
 * @returns The responder port.
 */
function catchUpResponder(runtime: EngineRuntime): CatchUpResponder {
  return {
    runtime,
    membership: (conversationId) =>
      activeRuns.get(runtime)?.memberships.get(conversationId) ??
      runtime.conversations.get(conversationId)?.membership,
    queue: (membership, body) =>
      queueRecoveryEnvelope(runtime, membership, body),
  };
}

/**
 * Build a run's staged-successor port. A newly certified head restarts the
 * conversation's catch-up from it, with fresh retries.
 * @param runtime Engine the run recovers.
 * @param context The memberships the run reconciles and the conversations it
 *     re-anchors.
 * @returns The run's staged-successor port.
 */
function successorPort(
  runtime: EngineRuntime,
  context: Pick<RecoveryRun, "memberships" | "reanchoring">,
): SuccessorRun {
  return {
    runtime,
    pending: new Map(),
    membership: (conversationId) => context.memberships.get(conversationId),
    reanchoring: (conversationId) => context.reanchoring.has(conversationId),
  };
}

/**
 * Route a conversation whose catch-up position is ready: to re-anchor after a
 * Router restart, and straight to recovered for any other reason. A member's
 * answer can complete a position after the run has ended; nothing is left to
 * route then.
 * @param run Recovery run that asked for the position.
 * @param conversationId Conversation whose position is ready.
 * @returns Completion once re-anchor has taken the position or it is recovered.
 */
function positionReady(
  run: RecoveryRun,
  conversationId: ConversationIdValue,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient.SqlClient> {
  const membership = run.memberships.get(conversationId);
  if (!run.catchUp.isActive() || membership === undefined) {
    return Effect.void;
  }
  return run.recovery.reason === "router_restarted"
    ? reanchorPositionReady(run.reanchor, membership).pipe(
        Effect.zipRight(awaitReanchor(run, conversationId)),
      )
    : markRecovered(run, conversationId);
}

/**
 * Open a restarted conversation's re-anchor `retry.pending` once its
 * position has proposed a re-anchor, or holds behind a staged successor,
 * without recovering. The round has no timeout: it waits for a quorum of
 * members, so the conversation reads busy until it recovers or the run ends.
 * The wait is recorded and its span emitted together, so an interruption
 * cannot leave the run's finalizer settling a wait that never opened.
 * @param run Recovery run that holds the conversation.
 * @param conversationId Conversation whose position is ready.
 * @returns Completion once the wait is marked, or at once when nothing waits.
 */
function awaitReanchor(
  run: RecoveryRun,
  conversationId: ConversationIdValue,
): Effect.Effect<void, never, SqlClient.SqlClient> {
  return Effect.uninterruptible(
    Effect.suspend(() => {
      if (
        !run.catchUp.isActive() ||
        run.completedConversations.has(conversationId) ||
        run.reanchorWaits.has(conversationId)
      ) {
        return Effect.void;
      }
      run.reanchorWaits.add(conversationId);
      return retryPending("reanchor", conversationId);
    }),
  );
}

/**
 * Mark one conversation recovered: stop its catch-up retries, resume its
 * retained envelopes, dissemination, folds and pending messages, and then
 * release the sends waiting on its fence. The run ends once every
 * conversation it holds has recovered. The steps run uninterruptibly: the
 * conversation counts as recovered from the first of them, so nothing would
 * release a fence an interruption left held.
 * @param run Recovery run that reconciled the conversation.
 * @param conversationId Conversation whose verified position is complete.
 * @returns Completion after the conversation's held work has resumed.
 */
function markRecovered(
  run: RecoveryRun,
  conversationId: ConversationIdValue,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient.SqlClient> {
  return Effect.suspend(() => {
    if (
      !run.catchUp.isActive() ||
      run.completedConversations.has(conversationId)
    ) {
      return Effect.void;
    }
    run.completedConversations.add(conversationId);
    const settleReanchor = run.reanchorWaits.delete(conversationId)
      ? retrySettled("reanchor", conversationId, "recovered")
      : Effect.void;
    return FiberMap.remove(run.retries, conversationId).pipe(
      Effect.zipRight(settleReanchor),
      Effect.zipRight(resumeConversation(run, conversationId)),
      Effect.zipRight(releaseConversation(run.runtime, conversationId)),
      Effect.zipRight(endRunOnceRecovered(run)),
    );
  }).pipe(Effect.uninterruptible);
}

/**
 * Resume one recovered conversation's held work: its retained envelopes in
 * durable order, its dissemination obligations, its folds' evidence, and its
 * messages not yet certified, which a re-anchor sends again at the new anchor.
 * @param run Recovery run that recovered the conversation.
 * @param conversationId Conversation that recovered.
 * @returns Completion after its held work is queued.
 */
function resumeConversation(
  run: RecoveryRun,
  conversationId: ConversationIdValue,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient.SqlClient> {
  const { runtime } = run;
  return Effect.suspend(() => {
    const retained = run.retainedOutbounds.get(conversationId) ?? [];
    run.retainedOutbounds.delete(conversationId);
    return runtime.outbox.resume(retained);
  }).pipe(
    Effect.zipRight(runtime.phases.resumeFolds(runtime, conversationId)),
    Effect.zipRight(
      runtime.outbox.serialized(resumeIntents(run, conversationId)),
    ),
  );
}

/**
 * Propose a recovered conversation's messages that have not certified. A message
 * proposed under a replaced anchor forgets that proposal first, so it
 * proposes again at the new one.
 * @param run Recovery run that recovered the conversation.
 * @param conversationId Conversation whose messages resume.
 * @returns Completion after each pending message is proposed.
 */
function resumeIntents(
  run: RecoveryRun,
  conversationId: ConversationIdValue,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient.SqlClient> {
  const { runtime } = run;
  return Effect.forEach(
    [...runtime.intents.values()].filter(
      (intent) => intent.intent.conversationId === conversationId,
    ),
    (intent) =>
      Effect.suspend(() => {
        if (run.reanchoring.has(conversationId)) {
          intent.proposedActionHash = undefined;
        }
        return runtime.phases
          .proposeIntent(runtime, intent)
          .pipe(Effect.mapError(persistenceFailure));
      }),
    { concurrency: 1, discard: true },
  );
}

/**
 * End the run once every conversation it holds has recovered.
 * @param run Recovery run to check.
 * @returns Completion after a finished run is uninstalled.
 */
function endRunOnceRecovered(
  run: RecoveryRun,
): Effect.Effect<void, never, SqlClient.SqlClient> {
  return Effect.suspend(() =>
    run.catchUp.isActive() &&
    run.completedConversations.size === run.memberships.size
      ? endRun(run.runtime)
      : Effect.void,
  );
}

/**
 * Uninstall the engine's recovery run and end its catch-up retries. The
 * scope closes uninterruptibly: a close cut short leaves the scope closed
 * with its retries running and its re-anchor waits never settled.
 * @param runtime Engine whose run ends.
 * @returns Completion once no run is installed.
 */
function endRun(
  runtime: EngineRuntime,
): Effect.Effect<void, never, SqlClient.SqlClient> {
  return Effect.suspend(() => {
    const run = activeRuns.get(runtime);
    if (run === undefined) {
      return Effect.void;
    }
    activeRuns.delete(runtime);
    return Effect.uninterruptible(Scope.close(run.scope, Exit.void));
  });
}

/**
 * Sign one outer envelope and queue it in the durable outbox. The Router
 * worker sends the outbox once it is active, which is as soon as the run has
 * started, so recovery traffic and the messages of recovered conversations go
 * out in one durable order.
 * @param runtime Engine whose outbox signs and retains the envelope.
 * @param membership Verified fixed membership for the outer envelope.
 * @param body Recovery packet or relayed evidence the envelope carries.
 * @returns Completion after the envelope is retained.
 */
function queueRecoveryEnvelope(
  runtime: EngineRuntime,
  membership: VerifiedMembership,
  body: DecodedOuterBody,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient.SqlClient> {
  return runtime.outbox.sign(membership, body).pipe(
    Effect.flatMap((message) =>
      runtime.outbox.enqueueSigned(
        membership.descriptor.conversationId,
        message,
      ),
    ),
    Effect.mapError(persistenceFailure),
  );
}

function packetConversation(packet: DirectPacket): ConversationIdValue {
  switch (packet.kind) {
    case "catch_up_request":
      return packet.conversationId;
    case "catch_up_page":
    case "catch_up_incomplete":
      return packet.request.conversationId;
    case "completed_reanchor":
      return packet.reanchor.conversationId;
    case "certified_record":
      return packet.actionCertifiedRecord.recordCore.action.conversationId;
    case "action_proposal":
      return packet.action.conversationId;
    case "action_certified_record":
      return packet.recordCore.action.conversationId;
    default: {
      const exhaustive: never = packet;
      return exhaustive;
    }
  }
}

/**
 * Validate every stored fixed membership against current engine state.
 * @param runtime Engine whose recovered conversations are authoritative.
 * @param recovery Store snapshot to validate.
 * @returns One unique verified membership per recovered conversation.
 */
function recoverMemberships(
  runtime: EngineRuntime,
  recovery: EndpointRecovery,
): Effect.Effect<
  Map<ConversationIdValue, VerifiedMembership>,
  RouterWorkerRecoveryError,
  SqlClient.SqlClient
> {
  return verifyStoredMemberships(
    recovery.memberships,
    runtime.input.registrySignerPublicKey,
  ).pipe(
    Effect.mapError(recoveryFailure),
    Effect.filterOrFail(
      (memberships) =>
        memberships.size === recovery.memberships.length &&
        [...memberships.values()].every((membership) =>
          recoveredMembershipMatches(runtime, membership),
        ),
      recoveryFailure,
    ),
    Effect.withSpan("recoverMemberships"),
  );
}

function recoveredMembershipMatches(
  runtime: EngineRuntime,
  membership: VerifiedMembership,
): boolean {
  const retained = runtime.conversations.get(
    membership.descriptor.conversationId,
  );
  return (
    memberCard(membership, runtime.input.localAgentCard.agentId) !==
      undefined && retained?.membership.hash === membership.hash
  );
}

function recoveryFailure(): RouterWorkerRecoveryError {
  return new RouterWorkerRecoveryError();
}

function persistenceFailure(): RouterWorkerPersistenceError {
  return new RouterWorkerPersistenceError();
}
