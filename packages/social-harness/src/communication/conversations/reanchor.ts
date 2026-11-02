/** @file Agrees on a new Router anchor without discarding certified history. */

import type { SqlClient } from "@effect/sql/SqlClient";

import { Effect, Schema } from "effect";

import type { AgentId } from "../../identity/agent.js";
import type { EngineActionFold, EngineRuntime } from "./state.js";

import {
  type ClientRepresentationError,
  decodeCanonical,
  encodeCanonical,
  sameBytes,
} from "../../encoding/canonical-json.js";
import {
  SignedMessage,
  type SignedMessage as SignedMessageValue,
} from "../../identity/envelope/signed-message.js";
import {
  type DecodedOuterBody,
  hashAnchor,
  signEvidenceMessage,
} from "../records/encoding.js";
import {
  AnchorHash,
  type AnchorHash as AnchorHashValue,
  CompletedReanchor,
  type CompletedReanchor as CompletedReanchorValue,
  type ConversationId as ConversationIdValue,
  type EvidenceStatement,
  type EvidenceStatement as EvidenceStatementValue,
  InitialAnchorBody,
  type InitialAnchorBody as InitialAnchorBodyValue,
  ReanchorBody,
  type ReanchorBody as ReanchorBodyValue,
  type ReanchorVoteStatement as ReanchorVoteStatementValue,
  RecordHash,
  type RecordHash as RecordHashValue,
  type RouterAnchor,
} from "../records/schemas.js";
import {
  quorumThreshold,
  type VerifiedMembership,
  verifyCompletedReanchor,
  verifyDeliveredEvidence,
  verifyOuterMessage,
  verifyStableEvidence,
} from "../records/verification.js";
import {
  type RouterDiscontinuityReason,
  type RouterIngressDisposition,
  type RouterWorkerIngress,
  RouterWorkerPersistenceError,
  type RouterWorkerRecovery,
} from "../router/connection.js";
import {
  applyCatchUpReanchor,
  completeReanchor,
  restartEmptyConversation,
  stageReanchor,
} from "../store/anchors.js";
import { recoverStoredState } from "../store/reads.js";
import { mergeEvidence } from "../store/records.js";
import {
  type ConversationFoundation,
  type EndpointRecovery,
  isSemanticStoreRejection,
  type StagedRecord,
  type MessageIntent as StoredMessageIntent,
} from "../store/types.js";
import {
  anchorRouterInstanceId,
  durablePosition,
  observedAnchorIsResolved,
  observedHeadIsResolved,
  orderedSignatures,
  protocolEvidence,
  stagedSuccessor,
} from "./log.js";

const acceptedDisposition: RouterIngressDisposition = "accepted";

const ignoredDisposition: RouterIngressDisposition = "ignored";

/**
 * What a re-anchor needs from the recovery run it belongs to. The run builds
 * it, so re-anchor never reads the run's state directly.
 */
export interface ReanchorRunPort {
  readonly runtime: EngineRuntime;
  readonly reason: RouterDiscontinuityReason;
  /** The Router instance the run polls, which a re-anchor moves conversations to. */
  readonly routerInstanceId: RouterWorkerRecovery["anchor"]["routerInstanceId"];
  /** Conversations anchored to another Router instance; only these re-anchor. */
  readonly reanchoring: ReadonlySet<string>;
  /** Whether the run is still the engine's active recovery. */
  readonly isActive: () => boolean;
  /** The run's verified membership of a conversation. */
  readonly membership: (
    conversationId: ConversationIdValue,
  ) => VerifiedMembership | undefined;
  readonly isRecovered: (conversationId: ConversationIdValue) => boolean;
  /** Finish a conversation and resume its held work. */
  readonly markRecovered: (
    conversationId: ConversationIdValue,
  ) => Effect.Effect<void, RouterWorkerPersistenceError, SqlClient>;
  /** Sign `body` and route it as the run routes its traffic. */
  readonly queue: (
    membership: VerifiedMembership,
    body: DecodedOuterBody,
  ) => Effect.Effect<void, RouterWorkerPersistenceError, SqlClient>;
  /** Ask members again for the history after the conversation's position. */
  readonly requestCatchUp: (
    conversationId: ConversationIdValue,
  ) => Effect.Effect<void, RouterWorkerPersistenceError, SqlClient>;
  /**
   * Send members again the staged, uncertified successor this endpoint
   * holds and its durability vote for it, so the successor can certify.
   */
  readonly resendStagedSuccessor: (
    membership: VerifiedMembership,
    staged: StagedRecord,
  ) => Effect.Effect<void, RouterWorkerPersistenceError, SqlClient>;
}

/**
 * One recovery run's re-anchor: the run's port, its vote memory, and the
 * conversations whose position is ready to take votes.
 */
export interface ReanchorRun extends ReanchorRunPort {
  readonly votes: ReanchorVotes;
  readonly positionsReady: Set<ConversationIdValue>;
}

interface RestartedPositionInput {
  readonly run: ReanchorRun;
  readonly membership: VerifiedMembership;
  readonly recovery: EndpointRecovery;
  readonly position: EndpointRecovery["positions"][number];
  readonly head: RecordHashValue;
}

interface CandidatePositionInput {
  readonly run: ReanchorRun;
  readonly recovery: EndpointRecovery;
  readonly body: ReanchorBodyValue;
  readonly head: RecordHashValue;
  readonly anchor: AnchorHashValue;
}

/**
 * What a ready position does with a member's verified vote: `certify` stages
 * and counts it now, `hold` keeps it without certifying it, and `decline`
 * drops it because it names an anchor or record this endpoint cannot resolve.
 * A declined vote does not count; at most it leaves that conversation
 * unrecovered.
 */
type ReadyVoteAction = "certify" | "hold" | "decline";

const certifyVote: ReadyVoteAction = "certify";

const holdVote: ReadyVoteAction = "hold";

const declineVote: ReadyVoteAction = "decline";

interface EmptyPositionRestartInput {
  readonly runtime: EngineRuntime;
  /** The Router instance the replacement initial anchor binds. */
  readonly routerInstanceId: InitialAnchorBodyValue["routerInstanceId"];
  readonly membership: VerifiedMembership;
  readonly recovery: EndpointRecovery;
  readonly position: EndpointRecovery["positions"][number];
}

interface ReplacementEmptyFoundation {
  readonly anchor: InitialAnchorBodyValue;
  readonly foundation: ConversationFoundation;
}

interface RestartedEmptyState {
  readonly runtime: EngineRuntime;
  readonly conversationId: ConversationIdValue;
  readonly expected: ConversationFoundation;
  readonly retained: ConversationFoundation;
  readonly messageIntents: readonly StoredMessageIntent[];
}

/** One verified re-anchor vote and its signed message, held for the rest of the run. */
interface PendingReanchorVote {
  readonly message: SignedMessageValue;
  readonly statement: ReanchorVoteStatementValue;
}

/**
 * One recovery run's re-anchor vote memory: the votes it holds per
 * conversation and candidate anchor.
 */
type ReanchorVotes = Map<
  ConversationIdValue,
  Map<AnchorHashValue, Map<AgentId, PendingReanchorVote>>
>;

type WriteReanchor = typeof completeReanchor | typeof applyCatchUpReanchor;

/**
 * Start one recovery run's re-anchor with empty vote memory and no position
 * ready yet.
 * @param port What the run exposes to its re-anchor.
 * @returns The run's re-anchor, which the run passes to every operation.
 */
export function startReanchorRun(port: ReanchorRunPort): ReanchorRun {
  return { ...port, votes: new Map(), positionsReady: new Set() };
}

/**
 * Reconcile one restarted conversation position and start or finish its
 * re-anchor.
 * @param run Recovery run whose position is ready.
 * @param membership Fixed membership of the conversation.
 * @returns Completion after re-anchor progress is durably queued.
 */
export function positionReady(
  run: ReanchorRun,
  membership: VerifiedMembership,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient> {
  return Effect.sync(() => {
    run.positionsReady.add(membership.descriptor.conversationId);
  }).pipe(Effect.zipRight(finishRestartedPosition(run, membership)));
}

/**
 * Accept one stable re-anchor vote from a verified outer member envelope.
 * @param run Recovery run the vote targets.
 * @param ingress Authenticated Router delivery containing the outer envelope.
 * @param message Stable self-addressed vote evidence.
 * @param statement Statement decoded from the vote's body by ingress.
 * @returns Whether the vote was accepted or safely ignored.
 */
export function acceptReanchorVote(
  run: ReanchorRun,
  ingress: RouterWorkerIngress<DecodedOuterBody>,
  message: SignedMessageValue,
  statement: EvidenceStatement,
): Effect.Effect<
  RouterIngressDisposition,
  RouterWorkerPersistenceError,
  SqlClient
> {
  return acceptReanchorVoteEffect(run, ingress, message, statement).pipe(
    Effect.withSpan("acceptReanchorVote"),
  );
}

/**
 * Accept one completed threshold re-anchor from a fixed conversation member.
 * @param run Recovery run the completion targets.
 * @param ingress Authenticated Router delivery containing the completion.
 * @param completed Complete re-anchor and signer-attributed certificate.
 * @returns Whether the completion was accepted or safely ignored.
 */
export function acceptCompletedReanchor(
  run: ReanchorRun,
  ingress: RouterWorkerIngress<DecodedOuterBody>,
  completed: CompletedReanchorValue,
): Effect.Effect<
  RouterIngressDisposition,
  RouterWorkerPersistenceError,
  SqlClient
> {
  return acceptCompletedReanchorEffect(run, ingress, completed).pipe(
    Effect.withSpan("acceptCompletedReanchor"),
  );
}

/**
 * Make a member's verified completed re-anchor durable and the
 * conversation's current anchor. It supersedes a different candidate this
 * endpoint staged for the same anchor and Router instance: its quorum
 * certificate shows that candidate can never be certified, because two
 * certificates for one scope would need an honest member to vote twice. The
 * store refuses a completion that does not extend this endpoint's durable
 * position; that refusal comes from the member's input, not a failed store,
 * so the completion does not count.
 * @param runtime Engine whose store and conversation take the anchor.
 * @param completed Verified completed re-anchor from a member.
 * @returns Whether the anchor was applied; false when the store refused it.
 */
export function applyCompletedReanchor(
  runtime: EngineRuntime,
  completed: CompletedReanchorValue,
): Effect.Effect<boolean, RouterWorkerPersistenceError, SqlClient> {
  return storeAndAdoptReanchor(runtime, completed, applyCatchUpReanchor).pipe(
    Effect.as(true),
    Effect.catchTag("EndpointStoreError", (error) =>
      isSemanticStoreRejection(error)
        ? Effect.succeed(false)
        : Effect.fail(persistenceFailure()),
    ),
    Effect.withSpan("applyCompletedReanchor"),
  );
}

function finishRestartedPosition(
  run: ReanchorRun,
  membership: VerifiedMembership,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient> {
  const conversationId = membership.descriptor.conversationId;
  if (!run.reanchoring.has(conversationId)) {
    return finishAnchoredPosition(run, membership);
  }
  return durablePosition(conversationId).pipe(
    Effect.flatMap(({ recovery, position }) => {
      if (position === undefined) {
        return Effect.fail(persistenceFailure());
      }
      if (position.headRecordHash === undefined) {
        if (currentAnchorForRecovery(run, conversationId) !== undefined) {
          return run.markRecovered(conversationId);
        }
        return restartEmptyPosition({
          runtime: run.runtime,
          routerInstanceId: run.routerInstanceId,
          membership,
          recovery,
          position,
        }).pipe(Effect.zipRight(run.markRecovered(conversationId)));
      }
      return Schema.decodeUnknown(RecordHash)(position.headRecordHash).pipe(
        Effect.mapError(persistenceFailure),
        Effect.flatMap((head) =>
          advanceRestartedPosition({
            run,
            membership,
            recovery,
            position,
            head,
          }),
        ),
      );
    }),
  );
}

/**
 * Replace an initial foundation only while its certified history is empty. The
 * caller marks the conversation recovered. The durable replacement and the
 * engine's adoption of its anchor are one uninterruptible step: a retry reads
 * the durable anchor as already restarted and never resets the engine, so an
 * interruption between them would leave actions binding the old anchor.
 * @param input Durable and recovered state for one empty conversation.
 * @returns Completion after the new foundation is durable and active.
 */
function restartEmptyPosition(
  input: EmptyPositionRestartInput,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient> {
  const { membership, position, recovery, routerInstanceId, runtime } = input;
  return Effect.gen(function* () {
    const expectedFoundation = yield* emptyConversationFoundation(
      recovery,
      membership,
      position,
    );
    const replacement = yield* replacementEmptyFoundation(
      routerInstanceId,
      membership,
      expectedFoundation,
    );
    yield* Effect.uninterruptible(
      Effect.gen(function* () {
        const restarted = yield* restartEmptyConversation({
          expectedFoundation,
          replacementFoundation: replacement.foundation,
        }).pipe(Effect.mapError(persistenceFailure));
        if (
          !restartedEmptyStateMatches({
            runtime,
            conversationId: membership.descriptor.conversationId,
            expected: replacement.foundation,
            retained: restarted.foundation,
            messageIntents: restarted.messageIntents,
          })
        ) {
          return yield* Effect.fail(persistenceFailure());
        }
        yield* resetEmptyConversationRuntime(
          runtime,
          membership.descriptor.conversationId,
          replacement.anchor,
          restarted.messageIntents,
        );
      }),
    );
  }).pipe(Effect.withSpan("restartEmptyPosition"));
}

/**
 * Move a restart-recovered position toward the new Router instance: finish it
 * when it is already anchored there, hold behind a staged successor, or
 * replay the peer votes held while catch-up ran and then propose this
 * endpoint's own re-anchor. A holder of a staged successor votes for no
 * re-anchor at its head; it sends the successor and its durability vote
 * again, so members can certify it. The replay can complete the re-anchor, so
 * whether to propose is decided only after it has run.
 * @param input Recovery run, membership, durable position and its head.
 * @returns Completion once the position is finished, waiting, or proposed.
 */
function advanceRestartedPosition(
  input: RestartedPositionInput,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient> {
  const { head, membership, position, recovery, run } = input;
  const conversationId = membership.descriptor.conversationId;
  if (currentAnchorForRecovery(run, conversationId) !== undefined) {
    return finishAnchoredPosition(run, membership);
  }
  const staged = stagedSuccessor(recovery, conversationId, head);
  if (staged !== undefined) {
    return run.resendStagedSuccessor(membership, staged);
  }
  return replayReanchorVotes(run, membership, conversationId, head).pipe(
    Effect.zipRight(
      Effect.suspend(() =>
        run.isRecovered(conversationId)
          ? Effect.void
          : proposeReanchor(run, membership, position),
      ),
    ),
  );
}

/**
 * Finish a conversation already anchored to the recovery Router instance.
 *
 * Catch-up alone reconciles it. A retained completed re-anchor for that
 * instance is relayed again so members still recovering can finish.
 * @param run Recovery run reconciling the conversation.
 * @param membership Fixed membership of the reconciled conversation.
 * @returns Completion after the conversation is marked recovered.
 */
function finishAnchoredPosition(
  run: ReanchorRun,
  membership: VerifiedMembership,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient> {
  const conversationId = membership.descriptor.conversationId;
  const anchor = currentAnchorForRecovery(run, conversationId);
  const relay =
    anchor?.kind === "completed_reanchor"
      ? run.queue(membership, { kind: "direct", packet: anchor })
      : Effect.void;
  return relay.pipe(Effect.zipRight(run.markRecovered(conversationId)));
}

function acceptReanchorVoteEffect(
  run: ReanchorRun,
  ingress: RouterWorkerIngress<DecodedOuterBody>,
  message: SignedMessageValue,
  statement: EvidenceStatement,
): Effect.Effect<
  RouterIngressDisposition,
  RouterWorkerPersistenceError,
  SqlClient
> {
  if (statement.kind !== "reanchor_vote") {
    return Effect.succeed(ignoredDisposition);
  }
  const membership = run.membership(statement.reanchor.conversationId);
  return membership === undefined
    ? Effect.succeed(ignoredDisposition)
    : verifyInboundVote(run, ingress, membership, { message, statement }).pipe(
        Effect.catchTag("ClientRepresentationError", () =>
          Effect.succeed(ignoredDisposition),
        ),
        Effect.mapError(persistenceFailure),
      );
}

/**
 * Verify a re-anchor vote's outer envelope and evidence against the
 * conversation's membership, then offer the vote to the active recovery run.
 * A vote that fails either check fails with a representation error, which
 * the caller reports as ignored like any other unusable input.
 * @param run Recovery run that receives the vote.
 * @param ingress Router delivery whose outer envelope carries the vote.
 * @param membership Recovered membership of the vote's conversation.
 * @param vote The signed evidence and its decoded statement.
 * @returns Accepted when the run took the vote, ignored when it did not.
 */
function verifyInboundVote(
  run: ReanchorRun,
  ingress: RouterWorkerIngress<DecodedOuterBody>,
  membership: VerifiedMembership,
  vote: Readonly<{ message: SignedMessageValue; statement: EvidenceStatement }>,
): Effect.Effect<
  RouterIngressDisposition,
  ClientRepresentationError | RouterWorkerPersistenceError,
  SqlClient
> {
  return verifyDeliveredEvidence({
    outer: ingress.message,
    evidence: vote.message,
    statement: vote.statement,
    membership,
  }).pipe(
    Effect.flatMap((verified) => {
      if (verified.statement.kind !== "reanchor_vote") {
        return Effect.succeed(ignoredDisposition);
      }
      return processReanchorVote(run, membership, {
        message: verified.message,
        statement: verified.statement,
      }).pipe(
        Effect.map((taken) =>
          taken ? acceptedDisposition : ignoredDisposition,
        ),
      );
    }),
  );
}

function acceptCompletedReanchorEffect(
  run: ReanchorRun,
  ingress: RouterWorkerIngress<DecodedOuterBody>,
  completed: CompletedReanchorValue,
): Effect.Effect<
  RouterIngressDisposition,
  RouterWorkerPersistenceError,
  SqlClient
> {
  const conversationId = completed.reanchor.conversationId;
  const membership = run.membership(conversationId);
  if (
    !run.isActive() ||
    membership === undefined ||
    !completionTargetsRecovery(run, membership, completed)
  ) {
    return Effect.succeed(ignoredDisposition);
  }
  return verifyOuterMessage({ message: ingress.message, membership }).pipe(
    Effect.zipRight(verifyCompletedReanchor({ completed, membership })),
    Effect.zipRight(applyCompletedReanchor(run.runtime, completed)),
    Effect.flatMap((applied) =>
      run
        .requestCatchUp(conversationId)
        .pipe(Effect.as(applied ? acceptedDisposition : ignoredDisposition)),
    ),
    Effect.catchTag("ClientRepresentationError", () =>
      Effect.succeed(ignoredDisposition),
    ),
    Effect.mapError(persistenceFailure),
  );
}

/**
 * Whether a relayed completion targets this run's re-anchor: a conversation
 * the run re-anchors after a Router restart, not yet anchored to the run's
 * Router instance, under the conversation's membership. The run applies a
 * verified completion directly: its quorum certificate settles the position,
 * even behind a staged successor this endpoint holds, which the certificate
 * shows can never be certified. Either way the conversation goes back to
 * catch-up: after an applied completion, to fetch what members certified
 * under the new anchor before it recovers; after one this endpoint cannot
 * apply, because its position lacks the selected record, to fetch the
 * history the completion extends, with which it arrives again.
 * @param run Recovery run the completion arrived in.
 * @param membership Verified membership of the completion's conversation.
 * @param completed The relayed completed re-anchor.
 * @returns Whether the run applies the completion.
 */
function completionTargetsRecovery(
  run: ReanchorRun,
  membership: VerifiedMembership,
  completed: CompletedReanchorValue,
): boolean {
  const body = completed.reanchor;
  if (
    run.reason !== "router_restarted" ||
    !run.reanchoring.has(body.conversationId) ||
    currentAnchorForRecovery(run, body.conversationId) !== undefined
  ) {
    return false;
  }
  return (
    body.membershipHash === membership.hash &&
    body.routerInstanceId === run.routerInstanceId
  );
}

/**
 * Propose this endpoint's re-anchor at its durable head. The run can end
 * while the position is being advanced, such as during the vote replay; an
 * ended run proposes nothing.
 * @param run Recovery run the proposal belongs to.
 * @param membership Fixed membership of the conversation.
 * @param position Durable position with a certified head.
 * @returns Completion once the candidate is voted for, or left unproposed.
 */
function proposeReanchor(
  run: ReanchorRun,
  membership: VerifiedMembership,
  position: EndpointRecovery["positions"][number],
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient> {
  if (!run.isActive()) {
    return Effect.void;
  }
  if (
    run.reason !== "router_restarted" ||
    position.headRecordHash === undefined
  ) {
    return Effect.fail(persistenceFailure());
  }
  return makeReanchorBody(
    run,
    membership,
    position,
    position.headRecordHash,
  ).pipe(
    Effect.flatMap((body) =>
      hashAnchor(body).pipe(
        Effect.mapError(persistenceFailure),
        Effect.flatMap((anchorHash) =>
          proposeReanchorCandidate(run, membership, body, anchorHash),
        ),
      ),
    ),
  );
}

function makeReanchorBody(
  run: ReanchorRun,
  membership: VerifiedMembership,
  position: EndpointRecovery["positions"][number],
  selectedRecordHash: string,
): Effect.Effect<ReanchorBodyValue, RouterWorkerPersistenceError, SqlClient> {
  const conversation = run.runtime.conversations.get(
    membership.descriptor.conversationId,
  );
  if (conversation?.head === undefined) {
    return Effect.fail(persistenceFailure());
  }
  return Schema.decodeUnknown(ReanchorBody)({
    sharnessVersion:
      conversation.head.record.actionCertifiedRecord.recordCore.sharnessVersion,
    kind: "reanchor_body",
    conversationId: membership.descriptor.conversationId,
    membershipHash: membership.hash,
    previousAnchorHash: position.currentAnchorHash,
    selectedRecordHash,
    routerInstanceId: run.routerInstanceId,
  }).pipe(Effect.mapError(persistenceFailure));
}

/**
 * Stage this endpoint's own candidate and vote for it. A candidate the
 * endpoint cannot stage gets no vote, and the conversation waits.
 * @param run Recovery run proposing the candidate.
 * @param membership Fixed membership of the conversation.
 * @param body This endpoint's re-anchor body at its durable position.
 * @param anchorHash Hash of `body`.
 * @returns Completion once the candidate is voted for, or left unproposed.
 */
function proposeReanchorCandidate(
  run: ReanchorRun,
  membership: VerifiedMembership,
  body: ReanchorBodyValue,
  anchorHash: AnchorHashValue,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient> {
  return stageReanchorCandidate(run, membership, body, anchorHash).pipe(
    Effect.flatMap((staged) =>
      staged ? voteAndComplete(run, membership, body, anchorHash) : Effect.void,
    ),
  );
}

/**
 * Replay the votes held for a conversation before its position was ready.
 * Only votes that select this endpoint's head are replayed; a held vote for a
 * record that catch-up never supplied does not count.
 * @param run Recovery run holding the votes.
 * @param membership Fixed membership of the conversation.
 * @param conversationId Conversation whose position is ready.
 * @param headRecordHash This endpoint's certified head.
 * @returns Completion once every matching held vote is replayed.
 */
function replayReanchorVotes(
  run: ReanchorRun,
  membership: VerifiedMembership,
  conversationId: ConversationIdValue,
  headRecordHash: RecordHashValue,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient> {
  const candidates = run.isActive() ? run.votes.get(conversationId) : undefined;
  if (candidates === undefined) {
    return Effect.void;
  }
  return Effect.forEach(
    candidates.values(),
    (votes) => replayCandidateVotes(run, membership, votes, headRecordHash),
    { concurrency: 1, discard: true },
  );
}

function replayCandidateVotes(
  run: ReanchorRun,
  membership: VerifiedMembership,
  votes: ReadonlyMap<
    PendingReanchorVote["statement"]["signerAgentId"],
    PendingReanchorVote
  >,
  headRecordHash: RecordHashValue,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient> {
  return Effect.forEach(
    votes.values(),
    (vote) =>
      vote.statement.reanchor.selectedRecordHash === headRecordHash
        ? processReanchorVote(run, membership, vote).pipe(Effect.asVoid)
        : Effect.void,
    { concurrency: 1, discard: true },
  );
}

/**
 * Take a vote into the active recovery run when it targets that run's Router
 * restart and names the anchor its body hashes to. Before the position is
 * ready the vote is held; once it is ready the position judges it. A vote
 * outside a restart, after the run has ended, for a conversation already
 * anchored to the run's Router instance, or one the ready position declines
 * changes nothing, so the caller reports it as ignored.
 * @param run Recovery run the vote targets.
 * @param membership Verified membership of the vote's conversation.
 * @param vote Verified re-anchor vote and its signed message.
 * @returns Whether the run took the vote.
 */
function processReanchorVote(
  run: ReanchorRun,
  membership: VerifiedMembership,
  vote: PendingReanchorVote,
): Effect.Effect<boolean, RouterWorkerPersistenceError, SqlClient> {
  if (!run.isActive() || !voteTargetsRecovery(run, membership, vote)) {
    return Effect.succeed(false);
  }
  return hashAnchor(vote.statement.reanchor).pipe(
    Effect.mapError(persistenceFailure),
    Effect.flatMap((expectedHash) => {
      if (expectedHash !== vote.statement.anchorHash) {
        return Effect.succeed(false);
      }
      return rememberReanchorVote(run.votes, vote).pipe(
        Effect.zipRight(
          run.positionsReady.has(vote.statement.reanchor.conversationId)
            ? processReadyReanchorVote(run, membership, vote)
            : Effect.succeed(true),
        ),
      );
    }),
  );
}

/**
 * Whether a vote targets this run's re-anchor. Only a conversation the run
 * re-anchors after a Router restart takes votes, and only until it is
 * anchored to the run's Router instance: a vote from that new anchor would
 * start a second re-anchor at the same instance, which no restart calls for.
 * @param run Recovery run the vote arrived in.
 * @param membership Verified membership of the vote's conversation.
 * @param vote Verified re-anchor vote.
 * @returns Whether the run takes the vote.
 */
function voteTargetsRecovery(
  run: ReanchorRun,
  membership: VerifiedMembership,
  vote: PendingReanchorVote,
): boolean {
  const body = vote.statement.reanchor;
  if (
    run.reason !== "router_restarted" ||
    !run.reanchoring.has(body.conversationId) ||
    currentAnchorForRecovery(run, body.conversationId) !== undefined
  ) {
    return false;
  }
  return (
    body.membershipHash === membership.hash &&
    body.routerInstanceId === run.routerInstanceId
  );
}

/**
 * A daemon restart does not replace a verified anchor for the same Router.
 * @param run Recovery with an authenticated Router instance.
 * @param conversationId Fixed conversation whose anchor is being reconciled.
 * @returns The retained anchor only when it already names the current Router.
 */
function currentAnchorForRecovery(
  run: ReanchorRun,
  conversationId: ConversationIdValue,
): RouterAnchor | undefined {
  const anchor = run.runtime.conversations.get(conversationId)?.currentAnchor;
  if (anchor === undefined) {
    return undefined;
  }
  return anchorRouterInstanceId(anchor) === run.routerInstanceId
    ? anchor
    : undefined;
}

/**
 * Apply a vote to a conversation whose position is ready. A conversation with
 * no head has nothing to re-anchor, so the vote is declined.
 * @param run Recovery run the vote targets.
 * @param membership Verified membership of the vote's conversation.
 * @param vote Verified re-anchor vote and its signed message.
 * @returns Whether the vote counts.
 */
function processReadyReanchorVote(
  run: ReanchorRun,
  membership: VerifiedMembership,
  vote: PendingReanchorVote,
): Effect.Effect<boolean, RouterWorkerPersistenceError, SqlClient> {
  const body = vote.statement.reanchor;
  return durablePosition(body.conversationId).pipe(
    Effect.flatMap(({ recovery, position }) => {
      if (position === undefined) {
        return Effect.fail(persistenceFailure());
      }
      if (position.headRecordHash === undefined) {
        return Effect.succeed(false);
      }
      return decodePosition(
        position.headRecordHash,
        position.currentAnchorHash,
      ).pipe(
        Effect.flatMap(({ head, anchor }) =>
          reconcileCandidatePosition({
            run,
            recovery,
            body,
            head,
            anchor,
          }),
        ),
        Effect.flatMap((action) => {
          switch (action) {
            case "certify":
              return certifyReanchorVote(run, membership, vote);
            case "hold":
              return Effect.succeed(true);
            case "decline":
              return Effect.succeed(false);
            default: {
              const exhaustive: never = action;
              return exhaustive;
            }
          }
        }),
      );
    }),
  );
}

function decodePosition(
  headRecordHash: string,
  currentAnchorHash: string,
): Effect.Effect<
  Readonly<{ head: RecordHashValue; anchor: AnchorHashValue }>,
  RouterWorkerPersistenceError,
  SqlClient
> {
  return Effect.all({
    head: Schema.decodeUnknown(RecordHash)(headRecordHash),
    anchor: Schema.decodeUnknown(AnchorHash)(currentAnchorHash),
  }).pipe(Effect.mapError(persistenceFailure));
}

function reconcileCandidatePosition(
  input: CandidatePositionInput,
): Effect.Effect<ReadyVoteAction, RouterWorkerPersistenceError, SqlClient> {
  return selectedHeadAction(input).pipe(
    Effect.map((action) =>
      action === certifyVote
        ? previousAnchorAction(input.recovery, input.body, input.anchor)
        : action,
    ),
  );
}

/**
 * Judge a vote by the record it selects. A vote for this endpoint's head is
 * certified unless a staged successor of that head is pending, and a vote for
 * an ancestor is held. A vote for a record this endpoint does not hold sends
 * the position back to catch-up and is held, unless this endpoint has already
 * staged a candidate for the Router instance: it stages no other then, so the
 * vote is declined.
 * @param input Recovery run, durable snapshot, vote body, and this endpoint's
 *     head and anchor.
 * @returns What the position does with the vote.
 */
function selectedHeadAction(
  input: CandidatePositionInput,
): Effect.Effect<ReadyVoteAction, RouterWorkerPersistenceError, SqlClient> {
  const { body, head, recovery, run } = input;
  if (body.selectedRecordHash === head) {
    return Effect.succeed(
      stagedSuccessor(recovery, body.conversationId, head) === undefined
        ? certifyVote
        : holdVote,
    );
  }
  if (
    observedHeadIsResolved(
      recovery,
      body.conversationId,
      body.selectedRecordHash,
      head,
    )
  ) {
    return Effect.succeed(holdVote);
  }
  if (hasStagedReanchor(recovery, body)) {
    return Effect.succeed(declineVote);
  }
  return Effect.sync(() => {
    run.positionsReady.delete(body.conversationId);
  }).pipe(
    Effect.zipRight(run.requestCatchUp(body.conversationId)),
    Effect.as(holdVote),
  );
}

/**
 * Judge a vote for this endpoint's head by the anchor it re-anchors from: the
 * current anchor is certified, an ancestor is held, and an anchor outside
 * this endpoint's anchor chain is declined.
 * @param recovery Durable snapshot holding the anchor chain.
 * @param body The vote's re-anchor body.
 * @param anchor This endpoint's current anchor.
 * @returns What the position does with the vote.
 */
function previousAnchorAction(
  recovery: EndpointRecovery,
  body: ReanchorBodyValue,
  anchor: AnchorHashValue,
): ReadyVoteAction {
  if (body.previousAnchorHash === anchor) {
    return certifyVote;
  }
  return observedAnchorIsResolved(
    recovery,
    body.conversationId,
    body.previousAnchorHash,
    anchor,
  )
    ? holdVote
    : declineVote;
}

function hasStagedReanchor(
  recovery: EndpointRecovery,
  body: ReanchorBodyValue,
): boolean {
  return recovery.stagedReanchors.some(
    (candidate) =>
      candidate.conversationId === body.conversationId &&
      candidate.routerInstanceId === body.routerInstanceId,
  );
}

/**
 * Stage a member's vote's candidate, persist the vote, and complete the
 * re-anchor once a quorum has voted. A vote for a candidate this endpoint
 * cannot stage, or one the store refuses, does not count.
 * @param run Recovery run the vote targets.
 * @param membership Verified membership of the vote's conversation.
 * @param vote Verified vote for this endpoint's head and anchor.
 * @returns Whether the vote counts.
 */
function certifyReanchorVote(
  run: ReanchorRun,
  membership: VerifiedMembership,
  vote: PendingReanchorVote,
): Effect.Effect<boolean, RouterWorkerPersistenceError, SqlClient> {
  const body = vote.statement.reanchor;
  const anchorHash = vote.statement.anchorHash;
  return stageReanchorCandidate(run, membership, body, anchorHash).pipe(
    Effect.flatMap((staged) =>
      staged ? persistReanchorVote(vote) : Effect.succeed(false),
    ),
    Effect.flatMap((counts) =>
      counts
        ? voteAndComplete(run, membership, body, anchorHash).pipe(
            Effect.as(true),
          )
        : Effect.succeed(false),
    ),
  );
}

function voteAndComplete(
  run: ReanchorRun,
  membership: VerifiedMembership,
  body: ReanchorBodyValue,
  anchorHash: AnchorHashValue,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient> {
  return ensureLocalReanchorVote(run, membership, body, anchorHash).pipe(
    Effect.zipRight(
      completeReanchorAtThreshold(run, membership, body, anchorHash),
    ),
  );
}

/**
 * Stage `body` as this endpoint's candidate when it still matches the durable
 * position and no other candidate holds its anchor and Router instance. An
 * endpoint stages at most one candidate per anchor and Router instance. A
 * certified successor can still move the head after a candidate is staged;
 * the endpoint then keeps that candidate and stages no other.
 * @param run Recovery run staging the candidate.
 * @param membership Fixed membership of the conversation.
 * @param body Re-anchor body to stage.
 * @param anchorHash Hash of `body`.
 * @returns Whether the candidate is staged.
 */
function stageReanchorCandidate(
  run: ReanchorRun,
  membership: VerifiedMembership,
  body: ReanchorBodyValue,
  anchorHash: AnchorHashValue,
): Effect.Effect<boolean, RouterWorkerPersistenceError, SqlClient> {
  return durablePosition(body.conversationId).pipe(
    Effect.flatMap(({ recovery, position }) => {
      if (
        !run.isActive() ||
        position === undefined ||
        position.headRecordHash === undefined ||
        !candidateMatchesPosition(run, membership, position, body)
      ) {
        return Effect.succeed(false);
      }
      if (
        stagedSuccessor(
          recovery,
          body.conversationId,
          body.selectedRecordHash,
        ) !== undefined
      ) {
        return Effect.succeed(false);
      }
      const staged = stagedCandidate(recovery, body);
      if (staged !== undefined && staged.anchorHash !== anchorHash) {
        return Effect.succeed(false);
      }
      return persistReanchorCandidate(body, anchorHash);
    }),
    Effect.mapError(persistenceFailure),
  );
}

function candidateMatchesPosition(
  run: ReanchorRun,
  membership: VerifiedMembership,
  position: EndpointRecovery["positions"][number],
  body: ReanchorBodyValue,
): boolean {
  if (
    run.reason !== "router_restarted" ||
    position.headRecordHash === undefined
  ) {
    return false;
  }
  return [
    run.positionsReady.has(body.conversationId),
    body.membershipHash === membership.hash,
    body.previousAnchorHash === position.currentAnchorHash,
    body.selectedRecordHash === position.headRecordHash,
    body.routerInstanceId === run.routerInstanceId,
  ].every((matches) => matches);
}

function stagedCandidate(
  recovery: EndpointRecovery,
  body: ReanchorBodyValue,
): EndpointRecovery["stagedReanchors"][number] | undefined {
  return recovery.stagedReanchors.find(
    (candidate) =>
      candidate.conversationId === body.conversationId &&
      candidate.previousAnchorHash === body.previousAnchorHash &&
      candidate.routerInstanceId === body.routerInstanceId,
  );
}

function persistReanchorCandidate(
  body: ReanchorBodyValue,
  anchorHash: AnchorHashValue,
): Effect.Effect<boolean, RouterWorkerPersistenceError, SqlClient> {
  return encodeCanonical(ReanchorBody, body).pipe(
    Effect.mapError(persistenceFailure),
    Effect.flatMap((canonicalBody) =>
      stageReanchor({
        conversationId: body.conversationId,
        anchorHash,
        previousAnchorHash: body.previousAnchorHash,
        routerInstanceId: body.routerInstanceId,
        selectedRecordHash: body.selectedRecordHash,
        canonicalBody,
      }),
    ),
    Effect.as(true),
    Effect.catchTag("EndpointStoreError", (error) =>
      isSemanticStoreRejection(error)
        ? Effect.succeed(false)
        : Effect.fail(persistenceFailure()),
    ),
    Effect.mapError(persistenceFailure),
  );
}

/**
 * Make sure this endpoint's own vote for a staged candidate is stored, sent,
 * and held. A run that has ended signs and sends nothing.
 * @param run Recovery run the vote belongs to.
 * @param membership Fixed membership of the conversation.
 * @param body Staged re-anchor body.
 * @param anchorHash Hash of `body`.
 * @returns Completion once the vote is stored, sent, and held.
 */
function ensureLocalReanchorVote(
  run: ReanchorRun,
  membership: VerifiedMembership,
  body: ReanchorBodyValue,
  anchorHash: AnchorHashValue,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient> {
  if (!run.isActive()) {
    return Effect.void;
  }
  const localAgentId = run.runtime.input.localAgentCard.agentId;
  const statement = localReanchorVoteStatement(run.runtime, body, anchorHash);
  return decodeReanchorVotes(membership, body, anchorHash).pipe(
    Effect.flatMap((votes) => {
      const message = votes.find((vote) => vote.senderAgentId === localAgentId);
      if (message === undefined) {
        return createLocalReanchorVote(run, membership, statement);
      }
      const vote: PendingReanchorVote = { message, statement };
      return reanchorVoteIsRemembered(run.votes, vote)
        ? Effect.void
        : run
            .queue(membership, { kind: "evidence", message })
            .pipe(Effect.zipRight(rememberReanchorVote(run.votes, vote)));
    }),
  );
}

/**
 * Whether the run already holds this signer's vote for this candidate anchor.
 * @param votes The run's vote memory.
 * @param vote Vote to look up.
 * @returns Whether the vote is held.
 */
function reanchorVoteIsRemembered(
  votes: ReanchorVotes,
  vote: PendingReanchorVote,
): boolean {
  return (
    votes
      .get(vote.statement.reanchor.conversationId)
      ?.get(vote.statement.anchorHash)
      ?.has(vote.statement.signerAgentId) === true
  );
}

function localReanchorVoteStatement(
  runtime: EngineRuntime,
  body: ReanchorBodyValue,
  anchorHash: AnchorHashValue,
): PendingReanchorVote["statement"] {
  return {
    sharnessVersion: body.sharnessVersion,
    kind: "reanchor_vote",
    signerAgentId: runtime.input.localAgentCard.agentId,
    anchorHash,
    reanchor: body,
  };
}

/**
 * Sign, persist, send, and hold this endpoint's own vote. The endpoint signs
 * only when no vote of its own is stored for the candidate, so a store
 * refusal of that vote is a local inconsistency.
 * @param run Recovery run the vote belongs to.
 * @param membership Fixed membership of the conversation.
 * @param statement This endpoint's vote for the staged candidate.
 * @returns Completion once the vote is durable, queued, and held.
 */
function createLocalReanchorVote(
  run: ReanchorRun,
  membership: VerifiedMembership,
  statement: PendingReanchorVote["statement"],
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient> {
  return signEvidenceMessage({
    statement,
    identityCard: run.runtime.input.localAgentCard,
    signingAuthority: run.runtime.input.signingAuthority,
  }).pipe(
    Effect.mapError(persistenceFailure),
    Effect.flatMap((message) => {
      const vote: PendingReanchorVote = { message, statement };
      return persistReanchorVote(vote).pipe(
        Effect.filterOrFail(
          (persisted) => persisted,
          () => persistenceFailure(),
        ),
        Effect.zipRight(run.queue(membership, { kind: "evidence", message })),
        Effect.zipRight(rememberReanchorVote(run.votes, vote)),
      );
    }),
  );
}

/**
 * Hold a verified vote for the rest of the run.
 * @param votes The run's vote memory.
 * @param vote Verified vote the run took.
 * @returns Completion once the vote is held.
 */
function rememberReanchorVote(
  votes: ReanchorVotes,
  vote: PendingReanchorVote,
): Effect.Effect<void, never, SqlClient> {
  return Effect.sync(() => {
    const conversationId = vote.statement.reanchor.conversationId;
    const candidates =
      votes.get(conversationId) ??
      new Map<
        AnchorHashValue,
        Map<
          PendingReanchorVote["statement"]["signerAgentId"],
          PendingReanchorVote
        >
      >();
    votes.set(conversationId, candidates);
    const signers =
      candidates.get(vote.statement.anchorHash) ??
      new Map<
        PendingReanchorVote["statement"]["signerAgentId"],
        PendingReanchorVote
      >();
    candidates.set(vote.statement.anchorHash, signers);
    signers.set(vote.statement.signerAgentId, vote);
  });
}

/**
 * Merge one vote into the store's re-anchor evidence for its candidate anchor.
 * One statement has more than one valid signature, so a member can send a
 * second, differently signed copy of a vote the store already holds. The
 * store refuses that copy, and it does not count.
 * @param vote Verified vote to persist.
 * @returns Whether the vote is durable; false when the store refused it.
 */
function persistReanchorVote(
  vote: PendingReanchorVote,
): Effect.Effect<boolean, RouterWorkerPersistenceError, SqlClient> {
  return protocolEvidence(
    vote.statement.reanchor.conversationId,
    "reanchor",
    vote.statement.anchorHash,
    vote.message,
  ).pipe(
    Effect.flatMap((evidence) => mergeEvidence(evidence)),
    Effect.as(true),
    Effect.catchTag("EndpointStoreError", (error) =>
      isSemanticStoreRejection(error)
        ? Effect.succeed(false)
        : Effect.fail(persistenceFailure()),
    ),
    Effect.mapError(persistenceFailure),
  );
}

function completeReanchorAtThreshold(
  run: ReanchorRun,
  membership: VerifiedMembership,
  body: ReanchorBodyValue,
  anchorHash: AnchorHashValue,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient> {
  if (!run.isActive() || run.isRecovered(body.conversationId)) {
    return Effect.void;
  }
  return decodeReanchorVotes(membership, body, anchorHash).pipe(
    Effect.flatMap((votes) =>
      votes.length < quorumThreshold(membership.members.length)
        ? Effect.succeed(undefined)
        : assembleCompletedReanchor(body, anchorHash, votes),
    ),
    Effect.flatMap((completed) =>
      completed === undefined
        ? Effect.void
        : finalizeCompletedReanchor(run, membership, completed),
    ),
  );
}

/**
 * Read and verify every stored vote for one candidate anchor.
 * @param membership Verified membership the votes must verify under.
 * @param body Re-anchor body every stored vote must name.
 * @param anchorHash Candidate anchor the votes are for.
 * @returns The stored votes, one per signer.
 */
function decodeReanchorVotes(
  membership: VerifiedMembership,
  body: ReanchorBodyValue,
  anchorHash: AnchorHashValue,
): Effect.Effect<
  readonly SignedMessageValue[],
  RouterWorkerPersistenceError,
  SqlClient
> {
  return recoverStoredState(body.conversationId).pipe(
    Effect.mapError(persistenceFailure),
    Effect.flatMap((recovery) =>
      Effect.forEach(
        recovery.evidence.filter((evidence) =>
          evidenceTargetsAnchor(evidence, body.conversationId, anchorHash),
        ),
        (evidence) =>
          verifyStoredReanchorVote(evidence, membership, body, anchorHash),
        { concurrency: 1 },
      ),
    ),
    Effect.flatMap((votes) =>
      votesHaveUniqueSigners(votes)
        ? Effect.succeed(votes)
        : Effect.fail(persistenceFailure()),
    ),
  );
}

/**
 * Assemble a completed re-anchor whose certificate holds `votes` in
 * decoded-AgentId order.
 * @param body Re-anchor body the votes name.
 * @param anchorHash Hash of `body`.
 * @param votes Stored votes for the anchor, at least one.
 * @returns The completed re-anchor.
 */
function assembleCompletedReanchor(
  body: ReanchorBodyValue,
  anchorHash: AnchorHashValue,
  votes: readonly SignedMessageValue[],
): Effect.Effect<
  CompletedReanchorValue,
  RouterWorkerPersistenceError,
  SqlClient
> {
  return orderedSignatures(votes).pipe(
    Effect.mapError(persistenceFailure),
    Effect.flatMap((ordered) => {
      if (ordered === undefined) {
        return Effect.fail(persistenceFailure());
      }
      const completed: CompletedReanchorValue = {
        sharnessVersion: body.sharnessVersion,
        kind: "completed_reanchor",
        anchorHash,
        reanchor: body,
        certificate: {
          sharnessVersion: body.sharnessVersion,
          kind: "reanchor_certificate",
          anchorHash,
          votes: ordered,
        },
      };
      return Effect.succeed(completed);
    }),
  );
}

function finalizeCompletedReanchor(
  run: ReanchorRun,
  membership: VerifiedMembership,
  completed: CompletedReanchorValue,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient> {
  return verifyCompletedReanchor({ completed, membership }).pipe(
    Effect.mapError(persistenceFailure),
    Effect.zipRight(persistCompletedReanchor(run.runtime, completed)),
    Effect.zipRight(
      run.queue(membership, { kind: "direct", packet: completed }),
    ),
    Effect.zipRight(run.markRecovered(completed.reanchor.conversationId)),
  );
}

/**
 * Make a completed re-anchor durable and the conversation's current anchor.
 * @param runtime Engine whose store and conversation receive the anchor.
 * @param completed Verified completed re-anchor.
 * @returns Completion once the anchor is durable and current.
 */
function persistCompletedReanchor(
  runtime: EngineRuntime,
  completed: CompletedReanchorValue,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient> {
  return storeAndAdoptReanchor(runtime, completed, completeReanchor).pipe(
    Effect.mapError(persistenceFailure),
  );
}

function emptyConversationFoundation(
  recovery: EndpointRecovery,
  membership: VerifiedMembership,
  position: EndpointRecovery["positions"][number],
): Effect.Effect<
  ConversationFoundation,
  RouterWorkerPersistenceError,
  SqlClient
> {
  const conversationId = membership.descriptor.conversationId;
  const storedMemberships = recovery.memberships.filter(
    (stored) => stored.conversationId === conversationId,
  );
  const storedAnchors = recovery.anchors.filter(
    (stored) => stored.conversationId === conversationId,
  );
  const storedMembership = storedMemberships[0];
  const storedAnchor = storedAnchors[0];
  if (
    storedMemberships.length !== 1 ||
    storedAnchors.length !== 1 ||
    storedMembership === undefined ||
    storedAnchor === undefined
  ) {
    return Effect.fail(persistenceFailure());
  }
  const foundationMatches = [
    position.conversationId === conversationId,
    position.membershipHash === membership.hash,
    position.currentAnchorHash === storedAnchor.anchorHash,
    storedMembership.membershipHash === membership.hash,
    storedAnchor.previousAnchorHash === undefined,
    storedAnchor.selectedRecordHash === undefined,
  ].every((matches) => matches);
  if (!foundationMatches) {
    return Effect.fail(persistenceFailure());
  }
  return Effect.succeed({
    conversationId,
    membershipHash: membership.hash,
    canonicalMembership: storedMembership.canonicalMembership,
    anchorHash: storedAnchor.anchorHash,
    canonicalAnchor: storedAnchor.canonicalAnchor,
  });
}

function replacementEmptyFoundation(
  routerInstanceId: InitialAnchorBodyValue["routerInstanceId"],
  membership: VerifiedMembership,
  expected: ConversationFoundation,
): Effect.Effect<
  ReplacementEmptyFoundation,
  RouterWorkerPersistenceError,
  SqlClient
> {
  return Effect.gen(function* () {
    const preceding = yield* decodeCanonical(
      InitialAnchorBody,
      expected.canonicalAnchor,
    ).pipe(Effect.mapError(persistenceFailure));
    const anchor: InitialAnchorBodyValue = {
      sharnessVersion: preceding.sharnessVersion,
      kind: "initial_anchor_body",
      conversationId: membership.descriptor.conversationId,
      membershipHash: membership.hash,
      routerInstanceId,
    };
    return yield* Effect.all({
      anchorHash: hashAnchor(anchor),
      canonicalAnchor: encodeCanonical(InitialAnchorBody, anchor),
    }).pipe(
      Effect.mapError(persistenceFailure),
      Effect.map(({ anchorHash, canonicalAnchor }) => ({
        anchor,
        foundation: {
          conversationId: expected.conversationId,
          membershipHash: expected.membershipHash,
          canonicalMembership: expected.canonicalMembership,
          anchorHash,
          canonicalAnchor,
        },
      })),
    );
  });
}

function restartedEmptyStateMatches(input: RestartedEmptyState): boolean {
  const { conversationId, expected, messageIntents, retained, runtime } = input;
  const conversation = runtime.conversations.get(conversationId);
  const runtimeIntents = [...runtime.intents.values()].filter(
    (pending) => pending.intent.conversationId === expected.conversationId,
  );
  if (conversation === undefined || conversation.head !== undefined) {
    return false;
  }
  const foundationMatches = [
    expected.conversationId === retained.conversationId,
    expected.membershipHash === retained.membershipHash,
    expected.anchorHash === retained.anchorHash,
    sameBytes(expected.canonicalMembership, retained.canonicalMembership),
    sameBytes(expected.canonicalAnchor, retained.canonicalAnchor),
  ].every((matches) => matches);
  return (
    foundationMatches &&
    runtimeIntents.length === messageIntents.length &&
    messageIntents.every((stored) => storedIntentMatches(runtime, stored))
  );
}

function storedIntentMatches(
  runtime: EngineRuntime,
  stored: StoredMessageIntent,
): boolean {
  const pending = runtime.intents.get(stored.msgId);
  if (pending === undefined || stored.completedRecordHash !== undefined) {
    return false;
  }
  return [
    pending.intent.conversationId === stored.conversationId,
    pending.intent.membershipHash === stored.membershipHash,
    pending.intent.authorAgentId === stored.authorAgentId,
    pending.intent.msgId === stored.msgId,
    sameBytes(pending.canonicalIntent, stored.canonicalIntent),
  ].every((matches) => matches);
}

function resetEmptyConversationRuntime(
  runtime: EngineRuntime,
  conversationId: ConversationIdValue,
  anchor: InitialAnchorBodyValue,
  messageIntents: readonly StoredMessageIntent[],
): Effect.Effect<void, never, SqlClient> {
  return Effect.sync(() => {
    replaceRuntimeAnchor(runtime, conversationId, anchor);
    discardConversationFolds(runtime, conversationId);
    clearIntentProposals(runtime, messageIntents);
  });
}

function replaceRuntimeAnchor(
  runtime: EngineRuntime,
  conversationId: ConversationIdValue,
  anchor: InitialAnchorBodyValue,
): void {
  const conversation = runtime.conversations.get(conversationId);
  if (conversation !== undefined) {
    conversation.currentAnchor = anchor;
    delete conversation.head;
  }
}

function discardConversationFolds(
  runtime: EngineRuntime,
  conversationId: ConversationIdValue,
): void {
  for (const [actionHash, fold] of runtime.actionFolds) {
    if (fold.conversation.conversationId === conversationId) {
      runtime.actionFolds.delete(actionHash);
    }
  }
  for (const [recordHash, fold] of runtime.recordFolds) {
    if (fold.conversation.conversationId === conversationId) {
      runtime.recordFolds.delete(recordHash);
    }
  }
}

function clearIntentProposals(
  runtime: EngineRuntime,
  messageIntents: readonly StoredMessageIntent[],
): void {
  for (const stored of messageIntents) {
    const pending = runtime.intents.get(stored.msgId);
    if (pending !== undefined) {
      pending.proposedActionHash = undefined;
    }
  }
}

/**
 * The durable row and its in-memory adoption form one interruption boundary,
 * so interruption cannot leave the store past an anchor the engine still holds.
 */
function storeAndAdoptReanchor(
  runtime: EngineRuntime,
  completed: CompletedReanchorValue,
  write: WriteReanchor,
) {
  const body = completed.reanchor;
  return Effect.all({
    canonicalBody: encodeCanonical(ReanchorBody, body),
    canonicalCompletedReanchor: encodeCanonical(CompletedReanchor, completed),
  }).pipe(
    Effect.mapError(persistenceFailure),
    Effect.flatMap((canonical) =>
      write({
        conversationId: body.conversationId,
        anchorHash: completed.anchorHash,
        previousAnchorHash: body.previousAnchorHash,
        routerInstanceId: body.routerInstanceId,
        selectedRecordHash: body.selectedRecordHash,
        ...canonical,
      }).pipe(
        Effect.zipRight(
          Effect.sync(() => {
            adoptCompletedReanchor(runtime, completed);
          }),
        ),
        Effect.uninterruptible,
      ),
    ),
  );
}

/**
 * Make a durable completed re-anchor the conversation's current anchor in
 * memory, and drop the fold of the proposal it supersedes at the selected
 * head, staged or not. That proposal binds the previous anchor, so it is no
 * longer gap-free and can never certify; the store has retired its lock,
 * signatures and staged record, and resuming its fold would only resend dead
 * evidence.
 * @param runtime Engine whose conversation and folds change.
 * @param completed Completed re-anchor the store has made current.
 */
function adoptCompletedReanchor(
  runtime: EngineRuntime,
  completed: CompletedReanchorValue,
): void {
  const body = completed.reanchor;
  const conversation = runtime.conversations.get(body.conversationId);
  if (conversation !== undefined) {
    conversation.currentAnchor = completed;
  }
  for (const [actionHash, fold] of runtime.actionFolds) {
    if (isSupersededProposal(fold, completed)) {
      runtime.actionFolds.delete(actionHash);
      if (fold.recordHash !== undefined) {
        runtime.recordFolds.delete(fold.recordHash);
      }
    }
  }
}

function isSupersededProposal(
  fold: EngineActionFold,
  completed: CompletedReanchorValue,
): boolean {
  const body = completed.reanchor;
  if (
    fold.conversation.conversationId !== body.conversationId ||
    fold.action.kind !== "MESSAGE"
  ) {
    return false;
  }
  return (
    fold.action.previousRecordHash === body.selectedRecordHash &&
    fold.action.anchorHash !== completed.anchorHash
  );
}

function evidenceTargetsAnchor(
  evidence: EndpointRecovery["evidence"][number],
  conversationId: ConversationIdValue,
  anchorHash: AnchorHashValue,
): boolean {
  return (
    evidence.kind === "reanchor" &&
    evidence.conversationId === conversationId &&
    evidence.subjectId === anchorHash
  );
}

function verifyStoredReanchorVote(
  evidence: EndpointRecovery["evidence"][number],
  membership: VerifiedMembership,
  body: ReanchorBodyValue,
  anchorHash: AnchorHashValue,
): Effect.Effect<SignedMessageValue, RouterWorkerPersistenceError, SqlClient> {
  return decodeCanonical(SignedMessage, evidence.canonicalEvidence).pipe(
    Effect.flatMap((message) => Schema.encode(SignedMessage)(message)),
    Effect.flatMap((representation) =>
      verifyStableEvidence({ representation, membership }),
    ),
    Effect.flatMap((verified) =>
      storedVoteMatches(verified.statement, body, anchorHash)
        ? Effect.succeed(verified.message)
        : Effect.fail(persistenceFailure()),
    ),
    Effect.mapError(persistenceFailure),
  );
}

function persistenceFailure(): RouterWorkerPersistenceError {
  return new RouterWorkerPersistenceError();
}

function storedVoteMatches(
  statement: EvidenceStatementValue,
  body: ReanchorBodyValue,
  anchorHash: AnchorHashValue,
): boolean {
  return (
    statement.kind === "reanchor_vote" &&
    statement.anchorHash === anchorHash &&
    sameReanchorBody(statement.reanchor, body)
  );
}

function votesHaveUniqueSigners(votes: readonly SignedMessageValue[]): boolean {
  return (
    new Set<PendingReanchorVote["statement"]["signerAgentId"]>(
      votes.map((vote) => vote.senderAgentId),
    ).size === votes.length
  );
}

function sameReanchorBody(
  left: ReanchorBodyValue,
  right: ReanchorBodyValue,
): boolean {
  return [
    left.conversationId === right.conversationId,
    left.membershipHash === right.membershipHash,
    left.previousAnchorHash === right.previousAnchorHash,
    left.selectedRecordHash === right.selectedRecordHash,
    left.routerInstanceId === right.routerInstanceId,
  ].every((matches) => matches);
}
