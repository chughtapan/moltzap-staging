/** @file Recovers missing ancestry and coordinates progress after Router interruption. */

import type { SqlClient } from "@effect/sql/SqlClient";
import {
  Deferred,
  Duration,
  Effect,
  ExecutionStrategy,
  Exit,
  FiberMap,
  Schedule,
  Scope,
} from "effect";
import type { AgentId } from "../../identity/agent-cards.js";
import type { SignedMessage } from "../../identity/message-security.js";
import type {
  EngineActionFold,
  EngineConversation,
  EngineRuntime,
} from "./conversation.js";
import {
  ClientRepresentationError,
  decodeCanonical,
} from "../../encoding/canonical-json.js";
import {
  exitOutcome,
  markRetries,
  type RetryOutcome,
  retryPending,
  retrySettled,
} from "../../tracing.js";
import {
  type ActionCertifiedRecord,
  type ActionCore,
  type AnchorHash,
  type ConversationId,
  type ConversationId as ConversationIdValue,
  type DecodedOuterBody,
  type DirectPacket,
  EvidenceStatement,
  type EvidenceStatement as EvidenceStatementValue,
  hashAnchor,
  memberCard,
  quorumThreshold,
  type RecordHash as RecordHashValue,
  type VerifiedMembership,
  verifyActionCertifiedRecord,
  verifyDeliveredEvidence,
  verifyOuterMessage,
} from "../records.js";
import {
  type RouterIngressDisposition,
  type RouterWorkerIngress,
  RouterWorkerPersistenceError,
  type RouterWorkerRecovery,
  RouterWorkerRecoveryError,
} from "../router/connection.js";
import {
  discardOutbound,
  type EndpointRecovery,
  recoverStoredState,
  type StoredOutboundMessage,
} from "../store.js";
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
} from "./history-sync.js";
import {
  anchorHashAtHead,
  durableRouterInstanceId,
  evidenceTarget,
  verifyRecoveredHistory,
  verifyStoredMemberships,
  verifyStoredOutbounds,
} from "./history.js";
import {
  acceptCompletedReanchor,
  acceptReanchorVote,
  positionReady as reanchorPositionReady,
  type ReanchorRun,
  startReanchorRun,
} from "./reanchor.js";

/**
 * Delay before a conversation's first catch-up retry. One second is well past
 * a Router round trip, so a retry follows a request that went unanswered
 * rather than racing its answers.
 */
const catchUpRetryBase = Duration.seconds(1);

/**
 * Eight jittered, doubling retries span about four minutes. Exhaustion leaves
 * the conversation paused until member traffic, a local send, Router
 * reattachment or another recovery run re-arms it.
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
 * Holds each unrecovered conversation behind its own fence so other
 * conversations can continue. Catch-up and re-anchoring use the operations
 * this run supplies.
 */
interface RecoveryRun {
  readonly runtime: EngineRuntime;
  readonly recovery: RouterWorkerRecovery;
  /**
   * Only conversations anchored to another Router instance need re-anchoring.
   * The others recover by catch-up alone.
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
 * The engine fence blocks all sends until verified recovery installs each
 * conversation's fence. Each conversation then blocks only its own sends.
 */
interface RecoveryFences {
  engine?: Deferred.Deferred<undefined>;
  readonly conversations: Map<ConversationId, Deferred.Deferred<undefined>>;
}

const recoveryFences = new WeakMap<EngineRuntime, RecoveryFences>();

/** A conversation's certified head and the anchor its next record binds. */
interface HeadPosition {
  readonly head: RecordHashValue;
  readonly anchorHash: AnchorHash;
}

/** A held delivery and the record it carries or votes for. */
interface HeldDelivery {
  readonly recordHash: RecordHashValue;
  readonly ingress: RouterWorkerIngress<DecodedOuterBody>;
}

/**
 * Holds one verified successor and each member's latest durability vote.
 * Later votes may use a replacement anchor. Keeping one vote per member
 * bounds faulty input. Moving the head or anchor discards the hold.
 */
interface PendingSuccessor {
  readonly position: HeadPosition;
  record?: HeldDelivery;
  readonly votes: Map<
    AgentId,
    HeldDelivery & { readonly statement: EvidenceStatement }
  >;
  /** Set once the endpoint has tried to stage the held record. */
  settled: boolean;
}

/** What staged-successor handling needs from the recovery run it belongs to. */
interface SuccessorRun {
  readonly runtime: EngineRuntime;
  /** The successor each re-anchoring conversation holds toward converting. */
  readonly pending: Map<ConversationIdValue, PendingSuccessor>;
  /** The run's verified membership of a conversation. */
  readonly membership: (
    conversationId: ConversationIdValue,
  ) => VerifiedMembership | undefined;
  /** Whether the run re-anchors the conversation after a Router restart. */
  readonly reanchoring: (conversationId: ConversationIdValue) => boolean;
}

const acceptedDisposition: RouterIngressDisposition = "accepted";

/**
 * Always answers member catch-up requests. Fenced conversations apply
 * certified records through recovery and ignore action traffic. Member traffic
 * re-arms exhausted catch-up retries.
 */
export function acceptEngineIngressWithRecovery(
  runtime: EngineRuntime,
  ingress: RouterWorkerIngress<DecodedOuterBody>,
): Effect.Effect<
  RouterIngressDisposition,
  RouterWorkerPersistenceError,
  SqlClient
> {
  const { payload } = ingress;
  return (
    payload.kind === "evidence"
      ? acceptEvidence(runtime, ingress, payload.message)
      : acceptPacket(runtime, ingress, payload.packet)
  ).pipe(Effect.withSpan("acceptEngineIngressWithRecovery"));
}

/**
 * Re-arms exhausted catch-up retries for one conversation, or all when omitted.
 * Runs in the recovery scope so local sends and Router reattachment need not
 * wait for its store reads. Another store failure ends these retries.
 */
export function rearmPausedCatchUp(
  runtime: EngineRuntime,
  conversationId?: ConversationIdValue,
): Effect.Effect<void, never, SqlClient> {
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
 * Restarts catch-up at a newly certified head while fenced. Before a run
 * starts, its initial request reads the durable head instead.
 */
export function recoveryHeadAdvanced(
  runtime: EngineRuntime,
  conversationId: ConversationIdValue,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient> {
  const run = activeRuns.get(runtime);
  return run !== undefined && isRecovering(runtime, conversationId)
    ? armCatchUp(run, conversationId)
    : Effect.void;
}

/** Includes the engine-wide fence until per-conversation fences are installed. */
export function isRecovering(
  runtime: EngineRuntime,
  conversationId: ConversationIdValue,
): boolean {
  return pendingRecoveryFence(runtime, conversationId) !== undefined;
}

/**
 * Verifies durable history and starts independent catch-up for each
 * conversation, replacing any previous run. Returns after requests are queued.
 * Only conversations anchored to another Router instance need re-anchoring.
 * A daemon starting against the same Router uses catch-up alone.
 */
export function recoverCertifiedHistory(
  runtime: EngineRuntime,
  recoveryInput: RouterWorkerRecovery,
): Effect.Effect<void, RouterWorkerRecoveryError, SqlClient> {
  const agentKey = runtime.input.agentKey;

  return Effect.gen(function* () {
    const barrier = currentRecoveryBarrier(runtime);
    if (barrier === undefined) {
      return yield* Effect.fail(recoveryFailure());
    }
    yield* endRun(runtime);
    const recovered = yield* recoverStoredState(agentKey).pipe(
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
}

/**
 * Install the one engine fence shared by all retries of a discontinuity.
 * @param runtime Engine entering Router recovery under its protocol gate.
 * @returns Completion after an engine fence exists.
 */
export function installRecoveryBarrier(
  runtime: EngineRuntime,
): Effect.Effect<void, never, SqlClient> {
  return Effect.suspend(() => {
    if (fencesOf(runtime).engine !== undefined) {
      return Effect.void;
    }
    return Deferred.make<undefined>().pipe(
      Effect.flatMap((barrier) =>
        Effect.sync(() => {
          fencesOf(runtime).engine ??= barrier;
        }),
      ),
    );
  });
}

/**
 * Returns the engine fence before a run installs conversation fences, then
 * the conversation fence. An open conversation has none.
 */
export function pendingRecoveryFence(
  runtime: EngineRuntime,
  conversationId: ConversationId,
): Deferred.Deferred<undefined> | undefined {
  const fences = recoveryFences.get(runtime);
  return fences?.engine ?? fences?.conversations.get(conversationId);
}

/** Releases only the engine fence captured by this recovery attempt. */
function completeRecoveryBarrier(
  runtime: EngineRuntime,
  barrier: Deferred.Deferred<undefined>,
): Effect.Effect<void, never, SqlClient> {
  return Effect.suspend(() => {
    const fences = recoveryFences.get(runtime);
    if (fences?.engine !== barrier) {
      return Effect.void;
    }
    delete fences.engine;
    return Deferred.succeed(barrier, undefined).pipe(Effect.asVoid);
  });
}

/** Preserves an existing fence and its send waiters. */
function fenceConversation(
  runtime: EngineRuntime,
  conversationId: ConversationId,
): Effect.Effect<void, never, SqlClient> {
  return Deferred.make<undefined>().pipe(
    Effect.flatMap((fence) =>
      Effect.sync(() => {
        const { conversations } = fencesOf(runtime);
        if (!conversations.has(conversationId)) {
          conversations.set(conversationId, fence);
        }
      }),
    ),
  );
}

function acceptPacket(
  runtime: EngineRuntime,
  ingress: RouterWorkerIngress<DecodedOuterBody>,
  packet: DirectPacket,
): Effect.Effect<
  RouterIngressDisposition,
  RouterWorkerPersistenceError,
  SqlClient
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
  SqlClient
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
 * Fenced conversations apply certified records through recovery, hold
 * action-certified successors and ignore proposals. Open conversations use
 * normal certification.
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
  SqlClient
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
 * During re-anchoring, holds a verified successor until more than `n − q(n)`
 * members vote it durable. Other recovery can stage and vote immediately.
 */
function acceptSuccessorRecord(
  run: SuccessorRun,
  ingress: RouterWorkerIngress<DecodedOuterBody>,
  record: ActionCertifiedRecord,
): Effect.Effect<
  RouterIngressDisposition,
  RouterWorkerPersistenceError,
  SqlClient
> {
  const { conversationId } = record.recordCore.action;
  const extended = headExtendedBy(run, record.recordCore.action);
  if (
    extended === undefined ||
    run.runtime.recordFolds.has(record.recordHash)
  ) {
    return Effect.succeed(ignoredDisposition);
  }
  if (!run.reanchoring(conversationId)) {
    return run.runtime.phases.acceptIngress(run.runtime, ingress);
  }
  const { position, membership } = extended;
  const pending = pendingFor(run.pending, conversationId, position);
  if (pending.settled || pending.record?.recordHash === record.recordHash) {
    return Effect.succeed(ignoredDisposition);
  }
  return holdTowardConversion(
    run,
    membership,
    verifyHeldRecord(membership, ingress, record),
    () => {
      pending.record = { recordHash: record.recordHash, ingress };
    },
  ).pipe(Effect.withSpan("acceptSuccessorRecord"));
}

/**
 * Fenced conversations hold durability evidence and ignore action signatures.
 * Unknown action signatures reach normal certification, which ignores them.
 */
function acceptEvidence(
  runtime: EngineRuntime,
  ingress: RouterWorkerIngress<DecodedOuterBody>,
  message: SignedMessage,
): Effect.Effect<
  RouterIngressDisposition,
  RouterWorkerPersistenceError,
  SqlClient
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

/**
 * Read the engine fence a discontinuity installed.
 * @param runtime Engine whose recovery state guards action activation.
 * @returns The pending engine fence, or absence once a recovery run holds
 *     each conversation's own fence.
 */
function currentRecoveryBarrier(
  runtime: EngineRuntime,
): Deferred.Deferred<undefined> | undefined {
  return recoveryFences.get(runtime)?.engine;
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
  SqlClient
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
 * A locally staged successor takes votes through normal certification. During
 * re-anchoring, an unstaged successor holds votes toward conversion.
 */
function acceptSuccessorVote(
  run: SuccessorRun,
  ingress: RouterWorkerIngress<DecodedOuterBody>,
  vote: Readonly<{
    message: SignedMessage;
    statement: Extract<EvidenceStatement, { readonly kind: "durability_vote" }>;
  }>,
): Effect.Effect<
  RouterIngressDisposition,
  RouterWorkerPersistenceError,
  SqlClient
> {
  const { conversationId, recordHash, signerAgentId } = vote.statement;
  const conversation = run.runtime.conversations.get(conversationId);
  const membership = run.membership(conversationId);
  if (conversation === undefined || membership === undefined) {
    return Effect.succeed(ignoredDisposition);
  }
  const fold = run.runtime.recordFolds.get(recordHash);
  if (fold !== undefined) {
    return acceptStagedSuccessorVote(
      run,
      { conversation, fold },
      ingress,
      vote.statement,
    );
  }
  const position = headPosition(conversation);
  if (!run.reanchoring(conversationId) || position === undefined) {
    return Effect.succeed(ignoredDisposition);
  }
  const pending = pendingFor(run.pending, conversationId, position);
  if (!takesVote(pending, signerAgentId, recordHash)) {
    return Effect.succeed(ignoredDisposition);
  }
  return holdTowardConversion(
    run,
    membership,
    verifyHeldVote(run, membership, ingress, vote),
    () => {
      pending.votes.set(signerAgentId, {
        recordHash,
        ingress,
        statement: vote.statement,
      });
    },
  ).pipe(Effect.withSpan("acceptSuccessorVote"));
}

/**
 * A vote for another Router instance contributes no vote to this run. Its verified
 * selected position waits for every member because an unanswered member may
 * hold its completed anchor.
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
): Effect.Effect<RouterIngressDisposition, never, SqlClient> {
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
  SqlClient
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
  SqlClient
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

/** Only traffic from another fixed member re-arms its conversation's catch-up. */
function rearmOnMemberTraffic(
  runtime: EngineRuntime,
  ingress: RouterWorkerIngress<DecodedOuterBody>,
  conversationId: ConversationIdValue,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient> {
  const membership = activeRuns.get(runtime)?.memberships.get(conversationId);
  return membership !== undefined &&
    sentByOtherMember(runtime, membership, ingress.message.senderAgentId)
    ? rearmIfPaused(runtime, conversationId)
    : Effect.void;
}

/** Leaves recovered conversations and active retries alone. */
function rearmIfPaused(
  runtime: EngineRuntime,
  conversationId: ConversationIdValue,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient> {
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
 * Installs retries only if the conversation is still unrecovered after
 * queueing the first request. A failed store read ends retries until new
 * traffic re-arms them.
 */
function armCatchUp(
  run: RecoveryRun,
  conversationId: ConversationIdValue,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient> {
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
 * Exhausted retries permit quorum settlement. Recovery runs on a separate
 * fiber because marking recovered removes the retry fiber. Overlapping trace
 * brackets keep the conversation busy through that handoff.
 */
function retryCatchUp(
  run: RecoveryRun,
  conversationId: ConversationIdValue,
): Effect.Effect<void, never, SqlClient> {
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
 * Records the recovery wait before forking so traces show no idle gap when
 * it takes over from exhausted retries.
 */
function recoverOnQuorum(
  run: RecoveryRun,
  conversationId: ConversationIdValue,
): Effect.Effect<void, never, SqlClient> {
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
 * Reports recovered, exhausted, handed to re-anchoring, failed or interrupted
 * work. A replacement run or re-arm interrupts the previous fiber.
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

/** Only a Router restart requires conversations on another instance to re-anchor. */
function reanchoringConversations(
  recovery: RouterWorkerRecovery,
  snapshot: EndpointRecovery,
  memberships: ReadonlyMap<ConversationIdValue, VerifiedMembership>,
): Effect.Effect<ReadonlySet<string>, RouterWorkerRecoveryError, SqlClient> {
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
 * Verifies retained envelopes before invalidating old-Router rows. Same-instance
 * rows preserve their retry identities.
 */
function prepareRecoveryOutbox(
  runtime: EngineRuntime,
  snapshot: EndpointRecovery,
  memberships: ReadonlyMap<ConversationIdValue, VerifiedMembership>,
  reanchoring: ReadonlySet<string>,
): Effect.Effect<
  readonly StoredOutboundMessage[],
  RouterWorkerRecoveryError,
  SqlClient
> {
  const agentKey = runtime.input.agentKey;

  return verifyStoredOutbounds(
    runtime.input,
    snapshot.outboundMessages,
    memberships,
  ).pipe(
    Effect.mapError(recoveryFailure),
    Effect.flatMap((outbounds) =>
      discardRestartedOutbounds(
        agentKey,
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
 *@param agentKey Immutable storage owner for this operation.
 *  @param outbounds Envelopes bound to the replaced Router instance.
 * @returns Completion once the stale envelopes are inactive.
 */
function discardRestartedOutbounds(
  agentKey: string,
  outbounds: readonly StoredOutboundMessage[],
): Effect.Effect<void, RouterWorkerRecoveryError, SqlClient> {
  return discardOutbound(agentKey, outbounds).pipe(
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

/** Allocates a run's scope and accounting without installing it. */
function makeRecoveryRun(
  runtime: EngineRuntime,
  recovery: RouterWorkerRecovery,
  history: Pick<
    RecoveryRun,
    "memberships" | "reanchoring" | "retainedOutbounds"
  >,
): Effect.Effect<RecoveryRun, never, SqlClient> {
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
): Effect.Effect<void, never, SqlClient> {
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

/** A newly certified successor restarts catch-up with fresh retries. */
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
 * An ended run routes nothing. A ready position re-anchors on Router restart
 * and otherwise completes recovery.
 */
function positionReady(
  run: RecoveryRun,
  conversationId: ConversationIdValue,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient> {
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
 * Re-anchor waits have no timeout: they require a member quorum. Recording
 * the wait and its trace share an interruption boundary so finalization cannot
 * settle a wait that never opened.
 */
function awaitReanchor(
  run: RecoveryRun,
  conversationId: ConversationIdValue,
): Effect.Effect<void, never, SqlClient> {
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
 * Resumes retained work before releasing the conversation's send fence. The
 * steps share an interruption boundary so a conversation marked recovered
 * cannot retain a fence. The run ends when all conversations recover.
 */
function markRecovered(
  run: RecoveryRun,
  conversationId: ConversationIdValue,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient> {
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
 * Release a recovered conversation's fence, resuming the sends that wait on
 * it.
 * @param runtime Engine whose recovery run recovered the conversation.
 * @param conversationId Conversation that recovered.
 * @returns Completion after its waiters resume.
 */
function releaseConversation(
  runtime: EngineRuntime,
  conversationId: ConversationId,
): Effect.Effect<void, never, SqlClient> {
  return Effect.suspend(() => {
    const fences = recoveryFences.get(runtime);
    const fence = fences?.conversations.get(conversationId);
    if (fences === undefined || fence === undefined) {
      return Effect.void;
    }
    fences.conversations.delete(conversationId);
    return Deferred.succeed(fence, undefined).pipe(Effect.asVoid);
  });
}

/** Resumes envelopes in durable order, dissemination, evidence and uncertified messages. */
function resumeConversation(
  run: RecoveryRun,
  conversationId: ConversationIdValue,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient> {
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

/** Discards a previous-anchor proposal before reproposing the message. */
function resumeIntents(
  run: RecoveryRun,
  conversationId: ConversationIdValue,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient> {
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
): Effect.Effect<void, never, SqlClient> {
  return Effect.suspend(() =>
    run.catchUp.isActive() &&
    run.completedConversations.size === run.memberships.size
      ? endRun(run.runtime)
      : Effect.void,
  );
}

/** Uninterruptible scope closure stops retries and settles re-anchor waits. */
function endRun(runtime: EngineRuntime): Effect.Effect<void, never, SqlClient> {
  return Effect.suspend(() => {
    const run = activeRuns.get(runtime);
    if (run === undefined) {
      return Effect.void;
    }
    activeRuns.delete(runtime);
    return Effect.uninterruptible(Scope.close(run.scope, Exit.void));
  });
}

/** Queues recovery traffic and recovered conversations' messages in one durable order. */
function queueRecoveryEnvelope(
  runtime: EngineRuntime,
  membership: VerifiedMembership,
  body: DecodedOuterBody,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient> {
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
  SqlClient
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

function fencesOf(runtime: EngineRuntime): RecoveryFences {
  const retained = recoveryFences.get(runtime);
  if (retained !== undefined) {
    return retained;
  }
  const created: RecoveryFences = { conversations: new Map() };
  recoveryFences.set(runtime, created);
  return created;
}

/** Forwards votes for locally staged successors to normal certification at `q(n)`. */
function acceptStagedSuccessorVote(
  run: SuccessorRun,
  target: Readonly<{
    conversation: EngineConversation;
    fold: EngineActionFold;
  }>,
  ingress: RouterWorkerIngress<DecodedOuterBody>,
  statement: Extract<EvidenceStatement, { readonly kind: "durability_vote" }>,
): Effect.Effect<
  RouterIngressDisposition,
  RouterWorkerPersistenceError,
  SqlClient
> {
  const { conversation, fold } = target;
  const position = headPosition(conversation);
  const staged =
    !fold.certified &&
    position !== undefined &&
    extendsHead(position, fold.action);
  const { signerAgentId } = statement;
  return staged && !fold.durabilityEvidence.has(signerAgentId)
    ? run.runtime.phases.acceptIngress(run.runtime, ingress, statement)
    : Effect.succeed(ignoredDisposition);
}

/** Holds verified input and attempts conversion. Returns ignored when verification fails. */
function holdTowardConversion(
  run: SuccessorRun,
  membership: VerifiedMembership,
  verified: Effect.Effect<boolean, never, SqlClient>,
  hold: () => void,
): Effect.Effect<
  RouterIngressDisposition,
  RouterWorkerPersistenceError,
  SqlClient
> {
  return verified.pipe(
    Effect.flatMap((held) => {
      if (!held) {
        return Effect.succeed(ignoredDisposition);
      }
      hold();
      return maybeConvert(run, membership).pipe(Effect.as(acceptedDisposition));
    }),
  );
}

function verifyHeldRecord(
  membership: VerifiedMembership,
  ingress: RouterWorkerIngress<DecodedOuterBody>,
  record: ActionCertifiedRecord,
): Effect.Effect<boolean, never, SqlClient> {
  return verifyOuterMessage({ message: ingress.message, membership }).pipe(
    Effect.zipRight(
      verifyActionCertifiedRecord({
        record,
        membership,
      }),
    ),
    Effect.as(true),
    Effect.catchTag("ClientRepresentationError", () => Effect.succeed(false)),
  );
}

function verifyHeldVote(
  run: SuccessorRun,
  membership: VerifiedMembership,
  ingress: RouterWorkerIngress<DecodedOuterBody>,
  vote: Readonly<{ message: SignedMessage; statement: EvidenceStatement }>,
): Effect.Effect<boolean, never, SqlClient> {
  return verifyDeliveredEvidence({
    outer: ingress.message,
    evidence: vote.message,
    statement: vote.statement,
    membership,
  }).pipe(
    Effect.map(
      ({ statement }) =>
        statement.kind === "durability_vote" &&
        statement.membershipHash === membership.hash &&
        statement.signerAgentId !== run.runtime.input.localAgentCard.agentId,
    ),
    Effect.catchTag("ClientRepresentationError", () => Effect.succeed(false)),
  );
}

/**
 * When more than `n − q(n)` other members vote for the successor, at least
 * one honest member holds a staged copy. Try conversion once because the
 * store can refuse it after a re-anchor is staged away from its anchor.
 */
function maybeConvert(
  run: SuccessorRun,
  membership: VerifiedMembership,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient> {
  return Effect.suspend(() => {
    const conversationId = membership.descriptor.conversationId;
    const pending = run.pending.get(conversationId);
    const record = pending?.record;
    if (pending === undefined || record === undefined || pending.settled) {
      return Effect.void;
    }
    const votes = [...pending.votes.values()].filter(
      (vote) => vote.recordHash === record.recordHash,
    );
    const memberCount = membership.members.length;
    if (votes.length < memberCount - quorumThreshold(memberCount) + 1) {
      return Effect.void;
    }
    pending.settled = true;
    return run.runtime.phases
      .acceptIngress(run.runtime, record.ingress)
      .pipe(
        Effect.zipRight(
          Effect.forEach(
            votes,
            (vote) =>
              run.runtime.phases.acceptIngress(
                run.runtime,
                vote.ingress,
                vote.statement,
              ),
            { concurrency: 1, discard: true },
          ),
        ),
      );
  });
}

/** Finds a held conversation whose head the action extends under its current anchor. */
function headExtendedBy(
  run: SuccessorRun,
  action: ActionCore,
):
  | Readonly<{ position: HeadPosition; membership: VerifiedMembership }>
  | undefined {
  const conversation = run.runtime.conversations.get(action.conversationId);
  const position =
    conversation === undefined ? undefined : headPosition(conversation);
  const membership = run.membership(action.conversationId);
  return position !== undefined &&
    membership !== undefined &&
    extendsHead(position, action)
    ? { position, membership }
    : undefined;
}

function headPosition(
  conversation: EngineConversation,
): HeadPosition | undefined {
  const { head } = conversation;
  return head === undefined
    ? undefined
    : {
        head: head.recordHash,
        anchorHash: anchorHashAtHead(conversation, head),
      };
}

/** A successor naming a departed anchor cannot be staged. */
function extendsHead(position: HeadPosition, action: ActionCore): boolean {
  return (
    action.kind === "MESSAGE" &&
    action.previousRecordHash === position.head &&
    action.anchorHash === position.anchorHash
  );
}

/** Keeps one vote per member for the held record, or any record before one is held. */
function takesVote(
  pending: PendingSuccessor,
  signerAgentId: AgentId,
  recordHash: RecordHashValue,
): boolean {
  const heldRecord = pending.record?.recordHash ?? recordHash;
  return (
    !pending.settled &&
    heldRecord === recordHash &&
    pending.votes.get(signerAgentId)?.recordHash !== recordHash
  );
}

function pendingFor(
  pending: SuccessorRun["pending"],
  conversationId: ConversationIdValue,
  position: HeadPosition,
): PendingSuccessor {
  const held = pending.get(conversationId);
  if (
    held?.position.head === position.head &&
    held.position.anchorHash === position.anchorHash
  ) {
    return held;
  }
  const created: PendingSuccessor = {
    position,
    votes: new Map(),
    settled: false,
  };
  pending.set(conversationId, created);
  return created;
}
