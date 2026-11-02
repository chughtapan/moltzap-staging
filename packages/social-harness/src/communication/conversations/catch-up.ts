/**
 * @file Catch-up: a recovery run asks every member for the certified history
 * after its durable position, and any endpoint answers such requests.
 */

import type { SqlClient } from "@effect/sql/SqlClient";

import { Effect, Schema } from "effect";

import type { AgentId } from "../../identity/agent.js";
import type { EndpointRecovery, StagedRecord } from "../store/types.js";
import type { EngineActionFold, EngineRuntime } from "./state.js";

import {
  type ClientRepresentationError,
  decodeCanonical,
} from "../../encoding/canonical-json.js";
import { SignedMessage } from "../../identity/envelope/signed-message.js";
import {
  type DecodedOuterBody,
  hashAnchor,
  signEvidenceMessage,
} from "../records/encoding.js";
import {
  AnchorHash,
  type AnchorHash as AnchorHashValue,
  type CatchUpIncomplete,
  type CatchUpPage,
  CatchUpRequest,
  type CatchUpRequest as CatchUpRequestValue,
  type CertifiedRecord,
  type CompletedReanchor as CompletedReanchorValue,
  type ConversationId as ConversationIdValue,
  type ReanchorBody,
  RecordHash,
  type RecordHash as RecordHashValue,
} from "../records/schemas.js";
import {
  memberCard,
  quorumThreshold,
  type VerifiedMembership,
  verifyCatchUpIncomplete,
  verifyCatchUpPage,
  verifyOuterMessage,
} from "../records/verification.js";
import {
  type RouterIngressDisposition,
  type RouterWorkerIngress,
  RouterWorkerPersistenceError,
  type RouterWorkerRecovery,
} from "../router/connection.js";
import { recoverStoredState } from "../store/reads.js";
import {
  decodeStoredAnchor,
  durablePosition,
  makeActionCertifiedRecord,
  readStoredRecord,
  recordAnchorHash,
  stagedSuccessor,
} from "./log.js";
import { applyCompletedReanchor } from "./reanchor.js";

/**
 * How a pending position becomes ready. With no entry, `q(n) − 1` other
 * members' `incomplete` answers make it ready.
 * - `every-member`: this endpoint holds, or a member sent, a re-anchor vote
 *   at the position for an earlier Router instance. That re-anchor may have
 *   completed at a member that has not answered yet, and settling without it
 *   could re-anchor apart from that member, so the position waits for every
 *   other member while its retries last.
 * - `retries-spent`: the retries ran out, and `q(n) − 1` answers settle it.
 * - `ready`: the request has made its position ready once.
 */
type Readiness = "every-member" | "retries-spent" | "ready";

/**
 * One run's catch-up bookkeeping: the request in flight per conversation, the
 * members that answered it as incomplete, how its position becomes ready, and
 * the successor accepted for each request.
 */
export interface CatchUpState {
  readonly pendingRequests: Map<ConversationIdValue, CatchUpRequestValue>;
  readonly incompleteResponders: Map<ConversationIdValue, Set<AgentId>>;
  readonly readiness: Map<ConversationIdValue, Readiness>;
  readonly acceptedSuccessors: Map<string, RecordHashValue | AnchorHashValue>;
}

/** What catch-up needs from the recovery run it belongs to; the run builds it. */
export interface CatchUpRun {
  readonly runtime: EngineRuntime;
  readonly state: CatchUpState;
  /** Whether the run is still the engine's active recovery. */
  readonly isActive: () => boolean;
  /** The run's verified membership of a conversation. */
  readonly membership: (
    conversationId: ConversationIdValue,
  ) => VerifiedMembership | undefined;
  /** Sign a body and queue it for every member of `membership`. */
  readonly queue: (
    membership: VerifiedMembership,
    body: DecodedOuterBody,
  ) => Effect.Effect<void, RouterWorkerPersistenceError, SqlClient>;
  /** The Router instance the run recovers at. */
  readonly routerInstanceId: RouterWorkerRecovery["anchor"]["routerInstanceId"];
  /**
   * A quorum of members, counting this endpoint, has attested that it holds
   * no later history.
   */
  readonly onPositionReady: (
    conversationId: ConversationIdValue,
  ) => Effect.Effect<void, RouterWorkerPersistenceError, SqlClient>;
}

/**
 * What answering a catch-up request needs. An endpoint answers inside and
 * outside its own recovery runs.
 */
export type CatchUpResponder = Pick<
  CatchUpRun,
  "runtime" | "membership" | "queue"
>;

const acceptedDisposition: RouterIngressDisposition = "accepted";
const ignoredDisposition: RouterIngressDisposition = "ignored";

/**
 * Start a run's catch-up bookkeeping with nothing in flight.
 * @returns Empty catch-up state.
 */
export function makeCatchUpState(): CatchUpState {
  return {
    pendingRequests: new Map(),
    incompleteResponders: new Map(),
    readiness: new Map(),
    acceptedSuccessors: new Map(),
  };
}

/**
 * Queue a signed catch-up request at the engine's current durable position.
 * A run that has ended, or that does not hold the conversation, such as one a
 * member created during recovery, asks for nothing.
 * @param run Recovery run that owns the request.
 * @param conversationId Private conversation identity to reconcile.
 * @returns Completion after the request is stored in the recovery queue.
 */
export const requestCertifiedHistory = (
  run: CatchUpRun,
  conversationId: ConversationIdValue,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient> =>
  queueCatchUpRequest(run, conversationId, "restart").pipe(
    Effect.withSpan("requestCertifiedHistory"),
  );

/**
 * Ask the members again for the history after the conversation's durable
 * position. The attestations already held for that same position still
 * count, so a retry only collects the members that have not answered yet.
 * @param run Recovery run that owns the request.
 * @param conversationId Private conversation identity to reconcile.
 * @returns Completion after the request is stored in the recovery queue.
 */
export const resendCertifiedHistoryRequest = (
  run: CatchUpRun,
  conversationId: ConversationIdValue,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient> =>
  queueCatchUpRequest(run, conversationId, "keep").pipe(
    Effect.withSpan("resendCertifiedHistoryRequest"),
  );

/**
 * Let a quorum of answers settle a conversation's position again once its
 * request has used up its retries. The caller runs this on the retry fiber
 * as the retries end, so the switch applies to the request those retries
 * sent and not to one a re-arm starts afterwards.
 * @param run Recovery run that owns the request.
 * @param conversationId Conversation whose retries ran out.
 * @returns Whether a quorum has already answered, so the position is ready.
 */
export function settleOnQuorum(
  run: CatchUpRun,
  conversationId: ConversationIdValue,
): boolean {
  const membership = run.membership(conversationId);
  if (!run.isActive() || membership === undefined) {
    return false;
  }
  if (run.state.readiness.get(conversationId) !== "ready") {
    run.state.readiness.set(conversationId, "retries-spent");
  }
  return claimReadyPosition(run.state, membership, conversationId);
}

/**
 * Make a conversation's pending position wait for every other member while
 * its retries last, once a member's re-anchor vote for an earlier Router
 * instance verifies and selects that position. A vote for any other
 * position, or for a position whose readiness is already decided, is not
 * verified at all. The position is checked again once the vote verifies,
 * since a retry can replace the pending request meanwhile.
 * @param run Recovery run that owns the request.
 * @param vote The vote's re-anchor body.
 * @param verified Verifies the vote as the member's.
 * @returns Completion once the vote is checked.
 */
export function waitBehindEarlierReanchor<E>(
  run: Pick<CatchUpRun, "state">,
  vote: Pick<
    ReanchorBody,
    "conversationId" | "previousAnchorHash" | "selectedRecordHash"
  >,
  verified: Effect.Effect<unknown, E, SqlClient>,
): Effect.Effect<void, E, SqlClient> {
  const { conversationId } = vote;
  const undecided = () => {
    const pending = run.state.pendingRequests.get(conversationId);
    return (
      pending !== undefined &&
      selectsPosition(vote, pending) &&
      !run.state.readiness.has(conversationId)
    );
  };
  if (!undecided()) {
    return Effect.void;
  }
  return verified.pipe(
    Effect.zipRight(
      Effect.sync(() => {
        if (undecided()) {
          run.state.readiness.set(conversationId, "every-member");
        }
      }),
    ),
  );
}

/**
 * Send members the staged, uncertified successor this endpoint holds and its
 * durability vote for it, both taken from the record's fold. A holder sends
 * them in place of an `incomplete` catch-up answer at the successor's
 * predecessor, and again when its own position is ready, since it votes for
 * no re-anchor there.
 * @param sender Engine whose fold holds the record, and how it sends.
 * @param membership Verified membership of the record's conversation.
 * @param staged The staged record row.
 * @returns Completion once the record and the vote are queued.
 */
export function queueStagedSuccessor(
  sender: Pick<CatchUpResponder, "runtime" | "queue">,
  membership: VerifiedMembership,
  staged: StagedRecord,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient> {
  return Schema.decodeUnknown(RecordHash)(staged.recordHash).pipe(
    Effect.mapError(persistenceFailure),
    Effect.flatMap((recordHash) => {
      const fold = sender.runtime.recordFolds.get(recordHash);
      return fold === undefined
        ? Effect.succeed([])
        : successorBodies(sender.runtime, fold).pipe(
            Effect.mapError(persistenceFailure),
          );
    }),
    Effect.flatMap((bodies) =>
      Effect.forEach(bodies, (body) => sender.queue(membership, body), {
        concurrency: 1,
        discard: true,
      }),
    ),
    Effect.withSpan("queueStagedSuccessor"),
  );
}

/**
 * Answer an authenticated catch-up request with the requester's next certified
 * history item, or with an attestation that there is none.
 * @param responder Engine and membership lookup, inside or outside a run.
 * @param ingress Verified Router delivery carrying the request.
 * @param request Catch-up request from a fixed member.
 * @returns Whether the request was answered or safely ignored.
 */
export function acceptCatchUpRequest(
  responder: CatchUpResponder,
  ingress: RouterWorkerIngress<DecodedOuterBody>,
  request: CatchUpRequestValue,
): Effect.Effect<
  RouterIngressDisposition,
  RouterWorkerPersistenceError,
  SqlClient
> {
  const membership = responder.membership(request.conversationId);
  if (membership === undefined) {
    return Effect.succeed(ignoredDisposition);
  }
  const senderAgentId = ingress.message.senderAgentId;
  const requestMatchesMembership =
    sentByOtherMember(responder.runtime, membership, senderAgentId) &&
    senderAgentId === request.requesterAgentId &&
    request.membershipHash === membership.hash;
  if (!requestMatchesMembership) {
    return Effect.succeed(ignoredDisposition);
  }
  return verifyOuterMessage({
    message: ingress.message,
    membership,
  }).pipe(
    Effect.flatMap(() =>
      respondToCatchUp(responder, membership, {
        request,
        routerInstanceId: ingress.routerInstanceId,
      }),
    ),
    Effect.as(acceptedDisposition),
    Effect.catchTag("ClientRepresentationError", () =>
      Effect.succeed(ignoredDisposition),
    ),
  );
}

/**
 * Whether a delivery came from one of the conversation's other fixed members,
 * the only senders whose catch-up traffic counts.
 * @param runtime Engine whose local identity is excluded.
 * @param membership Verified fixed membership of the conversation.
 * @param senderAgentId Outer sender of the delivery.
 * @returns True for a fixed member other than this endpoint.
 */
export function sentByOtherMember(
  runtime: EngineRuntime,
  membership: VerifiedMembership,
  senderAgentId: AgentId,
): boolean {
  return (
    senderAgentId !== runtime.input.localAgentCard.agentId &&
    memberCard(membership, senderAgentId) !== undefined
  );
}

/**
 * Apply the certified history item a member answered the run's pending
 * request with, then ask for the next one. A page whose item does not extend
 * the conversation, or whose successor differs from the one already applied
 * for that request, does not count: it is ignored, and at most that
 * conversation's catch-up waits on the other members.
 * @param run Recovery run that sent the request.
 * @param ingress Verified Router delivery carrying the page.
 * @param page Catch-up page from a fixed member.
 * @returns Whether the page was applied or safely ignored.
 */
export function acceptCatchUpPage(
  run: CatchUpRun,
  ingress: RouterWorkerIngress<DecodedOuterBody>,
  page: CatchUpPage,
): Effect.Effect<
  RouterIngressDisposition,
  RouterWorkerPersistenceError,
  SqlClient
> {
  const context = pendingCatchUpContext(
    run,
    page.request,
    ingress.message.senderAgentId,
  );
  if (context === undefined) {
    return Effect.succeed(ignoredDisposition);
  }
  return Effect.gen(function* () {
    yield* verifyOuterMessage({
      message: ingress.message,
      membership: context.membership,
    });
    yield* verifyCatchUpPage({
      page,
      membership: context.membership,
      responseSenderAgentId: ingress.message.senderAgentId,
    });
    const key = requestKey(page.request);
    const successor = pageSuccessorHash(page);
    const retained = run.state.acceptedSuccessors.get(key);
    if (retained !== undefined) {
      return retained === successor ? acceptedDisposition : ignoredDisposition;
    }
    if (!sameRequest(context.pending, page.request)) {
      return ignoredDisposition;
    }
    const applied = yield* applyCatchUpPage(run.runtime, ingress, page);
    if (applied === ignoredDisposition) {
      return ignoredDisposition;
    }
    yield* continueAfterPage(run, page, key, successor);
    return acceptedDisposition;
  }).pipe(
    Effect.catchTag("ClientRepresentationError", () =>
      Effect.succeed(ignoredDisposition),
    ),
    Effect.withSpan("acceptCatchUpPage"),
  );
}

/**
 * Record a member's attestation that it holds no later history; once a
 * quorum of members, counting this endpoint, has attested, the
 * conversation's position is ready.
 * @param run Recovery run that sent the request.
 * @param ingress Verified Router delivery carrying the attestation.
 * @param incomplete Catch-up incomplete attestation from a fixed member.
 * @returns Whether the attestation was taken or safely ignored.
 */
export function acceptCatchUpIncomplete(
  run: CatchUpRun,
  ingress: RouterWorkerIngress<DecodedOuterBody>,
  incomplete: CatchUpIncomplete,
): Effect.Effect<
  RouterIngressDisposition,
  RouterWorkerPersistenceError,
  SqlClient
> {
  const context = pendingCatchUpContext(
    run,
    incomplete.request,
    ingress.message.senderAgentId,
  );
  if (context === undefined) {
    return Effect.succeed(ignoredDisposition);
  }
  return Effect.gen(function* () {
    yield* verifyOuterMessage({
      message: ingress.message,
      membership: context.membership,
    });
    yield* verifyCatchUpIncomplete({
      incomplete,
      membership: context.membership,
      responseSenderAgentId: ingress.message.senderAgentId,
    });
    if (!sameRequest(context.pending, incomplete.request)) {
      return ignoredDisposition;
    }
    const positionIsReady = yield* Effect.sync(() =>
      recordIncompleteResponder(
        run.state,
        context.membership,
        incomplete.request.conversationId,
        ingress.message.senderAgentId,
      ),
    );
    if (!positionIsReady) {
      return acceptedDisposition;
    }
    yield* run.onPositionReady(incomplete.request.conversationId);
    return acceptedDisposition;
  }).pipe(
    Effect.catchTag("ClientRepresentationError", () =>
      Effect.succeed(ignoredDisposition),
    ),
    Effect.withSpan("acceptCatchUpIncomplete"),
  );
}

/**
 * Record a page's applied successor and ask for the history after it.
 * @param run Recovery run that sent the request.
 * @param page The applied page.
 * @param key The page's request key.
 * @param successor Hash of the page's applied item.
 * @returns Completion after the next request is queued.
 */
function continueAfterPage(
  run: CatchUpRun,
  page: CatchUpPage,
  key: string,
  successor: RecordHashValue | AnchorHashValue,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient> {
  return Effect.sync(() => {
    run.state.acceptedSuccessors.set(key, successor);
  }).pipe(
    Effect.zipRight(
      page.item.kind === "completed_reanchor"
        ? requestCertifiedHistory(run, page.request.conversationId)
        : Effect.void,
    ),
  );
}

/**
 * Whether a request at an unchanged position starts its attestations over
 * (`restart`) or keeps the ones already held (`keep`).
 */
type HeldAttestations = "restart" | "keep";

function queueCatchUpRequest(
  run: CatchUpRun,
  conversationId: ConversationIdValue,
  held: HeldAttestations,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient> {
  return Effect.gen(function* () {
    const membership = run.membership(conversationId);
    if (!run.isActive() || membership === undefined) {
      return;
    }
    const pending = run.state.pendingRequests.get(conversationId);
    if (
      held === "keep" &&
      pending !== undefined &&
      (yield* requestMatchesCurrentPosition(run.runtime, pending))
    ) {
      return yield* run.queue(membership, { kind: "direct", packet: pending });
    }
    const { position, recovery } = yield* durablePosition(conversationId);
    if (position === undefined) {
      return yield* Effect.fail(persistenceFailure());
    }
    const knownRecordHash = yield* decodeKnownRecordHash(position);
    const knownAnchorHash = yield* decodeKnownAnchorHash(position);
    const request = yield* makeCatchUpRequest(
      run.runtime,
      membership,
      knownRecordHash,
      knownAnchorHash,
    );
    yield* Effect.sync(() => {
      const pending = run.state.pendingRequests.get(conversationId);
      run.state.pendingRequests.set(conversationId, request);
      if (
        held === "restart" ||
        pending === undefined ||
        !sameRequest(pending, request)
      ) {
        restartAttestations(run, recovery, request);
      }
    });
    yield* run.queue(membership, { kind: "direct", packet: request });
  });
}

/**
 * In-memory positions follow durable promotions and completed re-anchors.
 * An unchanged retry can retain its request and attestations without loading
 * the conversation's retained records and evidence from the store.
 * @param runtime Engine whose installed position reflects durable writes.
 * @param request Pending request to compare with that position.
 * @returns Whether the pending request still names the installed position.
 */
function requestMatchesCurrentPosition(
  runtime: EngineRuntime,
  request: CatchUpRequestValue,
): Effect.Effect<boolean, RouterWorkerPersistenceError, SqlClient> {
  return Effect.gen(function* () {
    const conversation = runtime.conversations.get(request.conversationId);
    if (conversation === undefined) {
      return false;
    }
    const head = conversation.head?.recordHash ?? null;
    const anchor = conversation.currentAnchor;
    if (head === null) {
      return (
        request.knownRecordHash === null && request.knownAnchorHash === null
      );
    }
    const anchorHash =
      anchor.kind === "initial_anchor_body"
        ? yield* hashAnchor(anchor).pipe(Effect.mapError(persistenceFailure))
        : anchor.anchorHash;
    return (
      request.knownRecordHash === head && request.knownAnchorHash === anchorHash
    );
  });
}

/**
 * Answer a member's request with the certified successor of its position, or
 * say none is held. A responder, recovering or not, that holds a staged,
 * uncertified successor there sends that successor and its durability vote
 * instead of `incomplete`: an `incomplete` answer from a holder would let a
 * requester settle at the head the successor extends. An `incomplete` answer
 * follows this endpoint's votes for uncompleted re-anchors at the position
 * for Router instances earlier than the one delivering the request, so a
 * requester at that later instance waits for the member that may have
 * completed one.
 * @param responder The endpoint answering.
 * @param membership Verified membership of the request's conversation.
 * @param delivered The member's verified catch-up request and the Router
 *     instance that delivered it.
 * @param delivered.request The member's verified catch-up request.
 * @param delivered.routerInstanceId The Router instance that delivered it.
 * @returns Completion once the answer is queued.
 */
function respondToCatchUp(
  responder: CatchUpResponder,
  membership: VerifiedMembership,
  delivered: Readonly<{
    request: CatchUpRequestValue;
    routerInstanceId: RouterWorkerIngress<DecodedOuterBody>["routerInstanceId"];
  }>,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient> {
  const { request } = delivered;
  return Effect.gen(function* () {
    const recovery = yield* recoverStoredState(request.conversationId).pipe(
      Effect.mapError(persistenceFailure),
    );
    const successor = yield* decodeCatchUpSuccessor(
      membership,
      recovery,
      request,
    );
    if (successor !== undefined) {
      return yield* sendCatchUpPage(responder, membership, request, successor);
    }
    const staged =
      request.knownRecordHash === null || request.knownAnchorHash === null
        ? undefined
        : stagedSuccessor(
            recovery,
            request.conversationId,
            request.knownRecordHash,
            request.knownAnchorHash,
          );
    if (staged !== undefined) {
      return yield* queueStagedSuccessor(responder, membership, staged);
    }
    const votes = yield* heldReanchorVotes(
      responder.runtime,
      recovery,
      delivered,
    );
    return yield* sendCatchUpIncomplete(responder, membership, request, votes);
  });
}

/**
 * Start counting attestations for a request at a new position. The position
 * waits for every other member when this endpoint holds a re-anchor
 * candidate there for an earlier Router instance.
 * @param run Recovery run that owns the request.
 * @param recovery Complete verified recovery snapshot the request was made from.
 * @param request The request now pending.
 */
function restartAttestations(
  run: CatchUpRun,
  recovery: EndpointRecovery,
  request: CatchUpRequestValue,
): void {
  const { conversationId } = request;
  run.state.incompleteResponders.set(conversationId, new Set<AgentId>());
  const earlier = heldReanchorCandidates(recovery, request).some(
    (candidate) => candidate.routerInstanceId !== run.routerInstanceId,
  );
  if (earlier) {
    run.state.readiness.set(conversationId, "every-member");
  } else {
    run.state.readiness.delete(conversationId);
  }
}

/**
 * This endpoint's own votes for its uncompleted re-anchor candidates at a
 * request's position for Router instances other than the one delivering the
 * request. An `incomplete` answer carries them, so a requester learns that a
 * re-anchor from that position may already have completed for an earlier
 * Router instance. A vote for the delivering instance is re-anchor traffic
 * of that instance's own, not partial evidence of an answer.
 * @param runtime Engine whose identity signed the votes.
 * @param recovery Complete verified recovery snapshot.
 * @param delivered The catch-up request being answered and the Router
 *     instance that delivered it.
 * @param delivered.request The catch-up request being answered.
 * @param delivered.routerInstanceId The Router instance that delivered it.
 * @returns The votes, in stored order.
 */
function heldReanchorVotes(
  runtime: EngineRuntime,
  recovery: EndpointRecovery,
  delivered: Readonly<{
    request: CatchUpRequestValue;
    routerInstanceId: RouterWorkerIngress<DecodedOuterBody>["routerInstanceId"];
  }>,
): Effect.Effect<
  readonly SignedMessage[],
  RouterWorkerPersistenceError,
  SqlClient
> {
  const { request, routerInstanceId } = delivered;
  const candidates = new Set(
    heldReanchorCandidates(recovery, request)
      .filter((candidate) => candidate.routerInstanceId !== routerInstanceId)
      .map((candidate) => candidate.anchorHash),
  );
  const localAgentId = runtime.input.localAgentCard.agentId;
  return Effect.forEach(
    recovery.evidence.filter(
      (evidence) =>
        evidence.kind === "reanchor" &&
        evidence.conversationId === request.conversationId &&
        evidence.evidenceKey === localAgentId &&
        candidates.has(evidence.subjectId),
    ),
    (evidence) =>
      decodeCanonical(SignedMessage, evidence.canonicalEvidence).pipe(
        Effect.mapError(persistenceFailure),
      ),
    { concurrency: 1 },
  );
}

/**
 * This endpoint's uncompleted re-anchor candidates at a request's position:
 * candidates that select the requested record from the requested anchor,
 * for any Router instance.
 * @param recovery Complete verified recovery snapshot.
 * @param request The catch-up request whose position is examined.
 * @returns The candidates, completed ones excluded.
 */
function heldReanchorCandidates(
  recovery: EndpointRecovery,
  request: CatchUpRequestValue,
): EndpointRecovery["stagedReanchors"] {
  return recovery.stagedReanchors.filter(
    (candidate) =>
      candidate.conversationId === request.conversationId &&
      candidate.previousAnchorHash === request.knownAnchorHash &&
      candidate.selectedRecordHash === request.knownRecordHash &&
      candidate.canonicalCompletedReanchor === undefined,
  );
}

function decodeCatchUpSuccessor(
  membership: VerifiedMembership,
  recovery: EndpointRecovery,
  request: CatchUpRequestValue,
): Effect.Effect<
  CatchUpSuccessor | undefined,
  RouterWorkerPersistenceError,
  SqlClient
> {
  return Effect.gen(function* () {
    const rows = successorRows(recovery, request);
    if (rows.length > 1) {
      return yield* Effect.fail(persistenceFailure());
    }
    const row = rows[0];
    if (row === undefined) {
      return undefined;
    }
    const item = yield* decodeSuccessorRow(membership, recovery, row);
    const later = successorRows(recovery, nextRequest(request, item));
    if (later.length > 1) {
      return yield* Effect.fail(persistenceFailure());
    }
    return { item, hasMore: later.length === 1 };
  }).pipe(Effect.mapError(persistenceFailure));
}

function decodeSuccessorRow(
  membership: VerifiedMembership,
  recovery: EndpointRecovery,
  row: CatchUpSuccessorRow,
) {
  if (row.kind === "record") {
    return readStoredRecord(membership, recovery, row.value);
  }
  return decodeStoredAnchor(membership, row.value).pipe(
    Effect.flatMap((anchor) =>
      anchor.kind === "completed_reanchor"
        ? Effect.succeed(anchor)
        : Effect.fail(persistenceFailure()),
    ),
  );
}

function successorRows(
  recovery: EndpointRecovery,
  request: CatchUpRequestValue,
): readonly CatchUpSuccessorRow[] {
  if (request.knownRecordHash === null) {
    return recovery.certifiedRecords
      .filter(
        (record) =>
          record.conversationId === request.conversationId &&
          record.previousRecordHash === undefined,
      )
      .map((value): CatchUpSuccessorRow => ({ kind: "record", value }));
  }
  const reanchors = recovery.anchors
    .filter(
      (anchor) =>
        anchor.conversationId === request.conversationId &&
        anchor.previousAnchorHash === request.knownAnchorHash &&
        anchor.selectedRecordHash === request.knownRecordHash,
    )
    .map((value): CatchUpSuccessorRow => ({ kind: "reanchor", value }));
  const records = recovery.certifiedRecords
    .filter(
      (record) =>
        record.conversationId === request.conversationId &&
        record.previousRecordHash === request.knownRecordHash &&
        record.anchorHash === request.knownAnchorHash,
    )
    .map((value): CatchUpSuccessorRow => ({ kind: "record", value }));
  return [...reanchors, ...records];
}

function nextRequest(
  request: CatchUpRequestValue,
  item: CertifiedRecord | CompletedReanchorValue,
): CatchUpRequestValue {
  if (item.kind === "completed_reanchor") {
    return { ...request, knownAnchorHash: item.anchorHash };
  }
  return {
    ...request,
    knownRecordHash: item.actionCertifiedRecord.recordHash,
    knownAnchorHash: item.actionCertifiedRecord.recordCore.anchorHash,
  };
}

function successorBodies(
  runtime: EngineRuntime,
  fold: EngineActionFold,
): Effect.Effect<
  readonly DecodedOuterBody[],
  ClientRepresentationError,
  SqlClient
> {
  return recordAnchorHash(fold).pipe(
    Effect.flatMap((anchorHash) => makeActionCertifiedRecord(fold, anchorHash)),
    Effect.map((record): readonly DecodedOuterBody[] => {
      const vote = fold.durabilityEvidence.get(
        runtime.input.localAgentCard.agentId,
      );
      return [
        { kind: "direct", packet: record },
        ...(vote === undefined
          ? []
          : [{ kind: "evidence" as const, message: vote }]),
      ];
    }),
  );
}

/**
 * Answer that no next item is held, after this endpoint's own re-anchor votes
 * at the requested position. The votes go first, so a requester marks the
 * position as waiting for every member before it counts this answer.
 * @param responder The endpoint answering.
 * @param membership Verified membership of the request's conversation.
 * @param request The member's verified catch-up request.
 * @param votes This endpoint's votes for its re-anchor candidates there.
 * @returns Completion once the votes and the answer are queued.
 */
function sendCatchUpIncomplete(
  responder: CatchUpResponder,
  membership: VerifiedMembership,
  request: CatchUpRequestValue,
  votes: readonly SignedMessage[],
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient> {
  return Effect.gen(function* () {
    const attestation = yield* signCatchUpAttestation(
      responder.runtime,
      request,
      { kind: "incomplete", hash: null, hasMore: false },
    );
    yield* Effect.forEach(
      votes,
      (message) => responder.queue(membership, { kind: "evidence", message }),
      { concurrency: 1, discard: true },
    );
    yield* responder.queue(membership, {
      kind: "direct",
      packet: {
        sharnessVersion: responder.runtime.wireVersion,
        kind: "catch_up_incomplete",
        request,
        attestation,
      },
    });
  });
}

function sendCatchUpPage(
  responder: CatchUpResponder,
  membership: VerifiedMembership,
  request: CatchUpRequestValue,
  successor: CatchUpSuccessor,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient> {
  return Effect.gen(function* () {
    const hash =
      successor.item.kind === "certified_record"
        ? successor.item.actionCertifiedRecord.recordHash
        : successor.item.anchorHash;
    const attestation = yield* signCatchUpAttestation(
      responder.runtime,
      request,
      { kind: successor.item.kind, hash, hasMore: successor.hasMore },
    );
    yield* responder.queue(membership, {
      kind: "direct",
      packet: {
        sharnessVersion: responder.runtime.wireVersion,
        kind: "catch_up_page",
        request,
        item: successor.item,
        hasMore: successor.hasMore,
        attestation,
      },
    });
  });
}

function signCatchUpAttestation(
  runtime: EngineRuntime,
  request: CatchUpRequestValue,
  item: Readonly<{
    kind: "certified_record" | "completed_reanchor" | "incomplete";
    hash: RecordHashValue | AnchorHashValue | null;
    hasMore: boolean;
  }>,
) {
  return signEvidenceMessage({
    statement: {
      sharnessVersion: runtime.wireVersion,
      kind: "catch_up_attestation",
      signerAgentId: runtime.input.localAgentCard.agentId,
      request,
      itemKind: item.kind,
      itemHash: item.hash,
      hasMore: item.hasMore,
    },
    identityCard: runtime.input.localAgentCard,
    signingAuthority: runtime.input.signingAuthority,
  }).pipe(
    Effect.flatMap((message) => Schema.encode(SignedMessage)(message)),
    Effect.mapError(persistenceFailure),
  );
}

function pageSuccessorHash(
  page: CatchUpPage,
): RecordHashValue | AnchorHashValue {
  return page.item.kind === "certified_record"
    ? page.item.actionCertifiedRecord.recordHash
    : page.item.anchorHash;
}

/**
 * Apply a verified page's item. A record item goes through certification's
 * recovery path, which ignores a record that does not extend the
 * conversation, and a completed re-anchor the store refuses is ignored too.
 * @param runtime Engine whose store and conversations take the item.
 * @param ingress Verified Router delivery carrying the page.
 * @param page Verified catch-up page for the pending request.
 * @returns Whether the item was applied or ignored.
 */
function applyCatchUpPage(
  runtime: EngineRuntime,
  ingress: RouterWorkerIngress<DecodedOuterBody>,
  page: CatchUpPage,
): Effect.Effect<
  RouterIngressDisposition,
  RouterWorkerPersistenceError,
  SqlClient
> {
  if (page.item.kind === "completed_reanchor") {
    return applyCompletedReanchor(runtime, page.item).pipe(
      Effect.map((applied) =>
        applied ? acceptedDisposition : ignoredDisposition,
      ),
    );
  }
  const recordIngress: RouterWorkerIngress<DecodedOuterBody> = {
    ...ingress,
    payload: { kind: "direct", packet: page.item },
  };
  return runtime.phases.acceptRecoveryIngress(runtime, recordIngress);
}

function persistenceFailure(): RouterWorkerPersistenceError {
  return new RouterWorkerPersistenceError();
}

/**
 * Count a member's attestation for the pending request. The request stays
 * pending once the position is ready: a member that had not answered may
 * still send a page with later history, which then moves the position on.
 * @param state The run's catch-up bookkeeping.
 * @param membership Verified membership of the conversation.
 * @param conversationId Conversation the attestation is for.
 * @param senderAgentId Member that attested.
 * @returns Whether this attestation made the position ready.
 */
function recordIncompleteResponder(
  state: CatchUpState,
  membership: VerifiedMembership,
  conversationId: ConversationIdValue,
  senderAgentId: AgentId,
): boolean {
  const responders =
    state.incompleteResponders.get(conversationId) ?? new Set<AgentId>();
  if (responders.has(senderAgentId)) {
    return false;
  }
  responders.add(senderAgentId);
  state.incompleteResponders.set(conversationId, responders);
  return claimReadyPosition(state, membership, conversationId);
}

/**
 * Mark a position ready the first time its attestations suffice: a quorum
 * counting this endpoint, or every other member while the position waits
 * behind an earlier Router instance's re-anchor vote and its retries last.
 * @param state The run's catch-up bookkeeping.
 * @param membership Verified membership of the conversation.
 * @param conversationId Conversation whose attestations are counted.
 * @returns Whether the position became ready now.
 */
function claimReadyPosition(
  state: CatchUpState,
  membership: VerifiedMembership,
  conversationId: ConversationIdValue,
): boolean {
  const readiness = state.readiness.get(conversationId);
  const answered = state.incompleteResponders.get(conversationId)?.size ?? 0;
  const memberCount = membership.members.length;
  const needed =
    readiness === "every-member"
      ? memberCount - 1
      : quorumThreshold(memberCount) - 1;
  if (readiness === "ready" || answered < needed) {
    return false;
  }
  state.readiness.set(conversationId, "ready");
  return true;
}

function selectsPosition(
  vote: Pick<
    ReanchorBody,
    "conversationId" | "previousAnchorHash" | "selectedRecordHash"
  >,
  request: CatchUpRequestValue,
): boolean {
  return (
    request.knownAnchorHash === vote.previousAnchorHash &&
    request.knownRecordHash === vote.selectedRecordHash
  );
}

function sameRequest(
  left: CatchUpRequestValue,
  right: CatchUpRequestValue,
): boolean {
  return requestKey(left) === requestKey(right);
}

function requestKey(request: CatchUpRequestValue): string {
  return [
    request.conversationId,
    request.membershipHash,
    request.requesterAgentId,
    request.knownRecordHash ?? "null",
    request.knownAnchorHash ?? "null",
  ].join("\u0000");
}

function pendingCatchUpContext(
  run: CatchUpRun,
  request: CatchUpRequestValue,
  senderAgentId: AgentId,
):
  | Readonly<{ membership: VerifiedMembership; pending: CatchUpRequestValue }>
  | undefined {
  if (!run.isActive()) {
    return undefined;
  }
  const membership = run.membership(request.conversationId);
  const pending = run.state.pendingRequests.get(request.conversationId);
  if (membership === undefined || pending === undefined) {
    return undefined;
  }
  if (!sentByOtherMember(run.runtime, membership, senderAgentId)) {
    return undefined;
  }
  return { membership, pending };
}

interface CatchUpSuccessor {
  readonly item: CertifiedRecord | CompletedReanchorValue;
  readonly hasMore: boolean;
}

type CatchUpSuccessorRow =
  | Readonly<{
      kind: "record";
      value: EndpointRecovery["certifiedRecords"][number];
    }>
  | Readonly<{
      kind: "reanchor";
      value: EndpointRecovery["anchors"][number];
    }>;

function makeCatchUpRequest(
  runtime: EngineRuntime,
  membership: VerifiedMembership,
  knownRecordHash: RecordHashValue | null,
  knownAnchorHash: AnchorHashValue | null,
): Effect.Effect<CatchUpRequestValue, RouterWorkerPersistenceError, SqlClient> {
  return Schema.decodeUnknown(CatchUpRequest)({
    sharnessVersion: runtime.wireVersion,
    kind: "catch_up_request",
    conversationId: membership.descriptor.conversationId,
    membershipHash: membership.hash,
    requesterAgentId: runtime.input.localAgentCard.agentId,
    knownRecordHash,
    knownAnchorHash,
  }).pipe(Effect.mapError(persistenceFailure));
}

function decodeKnownRecordHash(
  position: EndpointRecovery["positions"][number],
): Effect.Effect<
  RecordHashValue | null,
  RouterWorkerPersistenceError,
  SqlClient
> {
  if (position.headRecordHash === undefined) {
    return Effect.succeed(null);
  }
  return Schema.decodeUnknown(RecordHash)(position.headRecordHash).pipe(
    Effect.mapError(persistenceFailure),
  );
}

function decodeKnownAnchorHash(
  position: EndpointRecovery["positions"][number],
): Effect.Effect<
  AnchorHashValue | null,
  RouterWorkerPersistenceError,
  SqlClient
> {
  if (position.headRecordHash === undefined) {
    return Effect.succeed(null);
  }
  return Schema.decodeUnknown(AnchorHash)(position.currentAnchorHash).pipe(
    Effect.mapError(persistenceFailure),
  );
}
