/**
 * @file Catch-up: a recovery run asks every member for the certified history
 * after its durable position, and any endpoint answers such requests.
 */

import type { SqlClient } from "@effect/sql/SqlClient";
import { Effect, Schema } from "effect";
import type { AgentId } from "../../identity/agent-cards.js";
import type { EngineActionFold, EngineRuntime } from "./conversation.js";
import {
  type ClientRepresentationError,
  decodeCanonical,
} from "../../encoding/canonical-json.js";
import { SignedMessage } from "../../identity/message-security.js";
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
  type DecodedOuterBody,
  hashAnchor,
  memberCard,
  quorumThreshold,
  type ReanchorBody,
  RecordHash,
  type RecordHash as RecordHashValue,
  signEvidenceMessage,
  type VerifiedMembership,
  verifyCatchUpIncomplete,
  verifyCatchUpPage,
  verifyOuterMessage,
} from "../records.js";
import {
  type RouterIngressDisposition,
  type RouterWorkerIngress,
  RouterWorkerPersistenceError,
  type RouterWorkerRecovery,
} from "../router/connection.js";
import {
  type EndpointRecovery,
  recoverStoredState,
  type StagedRecord,
} from "../store.js";
import {
  decodeStoredAnchor,
  durablePosition,
  makeActionCertifiedRecord,
  readStoredRecord,
  recordAnchorHash,
  stagedSuccessor,
} from "./history.js";
import { applyCompletedReanchor } from "./reanchor.js";

/**
 * A position normally needs `q(n) − 1` other members' incomplete answers.
 * A vote to re-anchor for an earlier Router instance requires answers from
 * every member until retries expire. An unanswered member may hold its
 * completed anchor. `ready` records that readiness was already claimed.
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

/** The recovery run supplies these operations to catch-up. */
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
 * Queues a request at the durable position. An ended run or a conversation
 * absent from the engine queues nothing.
 */
export const requestCertifiedHistory = (
  run: CatchUpRun,
  conversationId: ConversationIdValue,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient> =>
  queueCatchUpRequest(run, conversationId, "restart").pipe(
    Effect.withSpan("requestCertifiedHistory"),
  );

/**
 * Retries at the current durable position. Answers already held for that
 * position still count.
 */
export const resendCertifiedHistoryRequest = (
  run: CatchUpRun,
  conversationId: ConversationIdValue,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient> =>
  queueCatchUpRequest(run, conversationId, "keep").pipe(
    Effect.withSpan("resendCertifiedHistoryRequest"),
  );

/**
 * Allows quorum settlement when retries expire. Call from the ending retry
 * fiber so a re-armed request cannot inherit this relaxation. Returns whether
 * the position is ready.
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
 * A verified vote to re-anchor for an earlier Router instance makes its
 * pending position wait for every member until retries expire. Skip unrelated
 * or settled positions and recheck the pending position after verification.
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
 * Sends the held staged successor and local durability vote instead of an
 * incomplete answer. A holder cannot re-anchor away from that successor.
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
 * Answers a fixed member with its next certified item or an incomplete
 * attestation. Returns whether the request was answered or safely ignored.
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

/** Excludes the local agent and non-members from counted catch-up traffic. */
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
 * Applies the requested certified successor and asks for the next item.
 * Ignores pages that do not extend history or conflict with a successor already
 * applied for this request.
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

/** Counts an incomplete attestation toward readiness at the pending position. */
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
 * An unchanged position retains its request and attestations without reloading
 * durable history.
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
 * A staged successor and its vote replace an incomplete answer, which could
 * otherwise let the requester re-anchor away from that successor. First send
 * held votes to re-anchor for earlier Router instances so the requester waits
 * for any member that may hold a completed anchor.
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
 * A new position waits for every member if this endpoint holds a candidate
 * for an earlier Router instance. Another member may have completed it.
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
 * Returns local votes for uncompleted anchors at the requested position,
 * excluding the delivering Router instance. These warn that an earlier anchor
 * may have completed at another member.
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
 * Selects uncompleted candidates at the requested record and anchor across
 * Router instances.
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
 * Queues re-anchor votes before the incomplete answer so they change the
 * requester's readiness rule before that answer counts.
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

/** Ignores records that do not extend history and re-anchors the store refuses. */
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
 * Keeps the request pending after readiness so a late page can still advance
 * the position.
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
 * Claims readiness once: at quorum normally, or every member while earlier
 * re-anchor evidence requires it and retries remain.
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
