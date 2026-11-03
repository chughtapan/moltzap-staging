/** @file Agrees on a new Router anchor without discarding certified history. */

import type { SqlClient } from "@effect/sql/SqlClient";
import { Effect, Schema } from "effect";
import type { AgentId } from "../../identity/agent-cards.js";
import type { EngineActionFold, EngineRuntime } from "./conversation.js";
import {
  type ClientRepresentationError,
  decodeCanonical,
  encodeCanonical,
  sameBytes,
} from "../../encoding/canonical-json.js";
import {
  SignedMessage,
  type SignedMessage as SignedMessageValue,
} from "../../identity/message-security.js";
import {
  AnchorHash,
  type AnchorHash as AnchorHashValue,
  CompletedReanchor,
  type CompletedReanchor as CompletedReanchorValue,
  type ConversationId as ConversationIdValue,
  type DecodedOuterBody,
  type EvidenceStatement,
  type EvidenceStatement as EvidenceStatementValue,
  hashAnchor,
  InitialAnchorBody,
  type InitialAnchorBody as InitialAnchorBodyValue,
  quorumThreshold,
  ReanchorBody,
  type ReanchorBody as ReanchorBodyValue,
  type ReanchorVoteStatement as ReanchorVoteStatementValue,
  RecordHash,
  type RecordHash as RecordHashValue,
  type RouterAnchor,
  signEvidenceMessage,
  type VerifiedMembership,
  verifyCompletedReanchor,
  verifyDeliveredEvidence,
  verifyOuterMessage,
  verifyStableEvidence,
} from "../records.js";
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
  type ConversationFoundation,
  type EndpointRecovery,
  isSemanticStoreRejection,
  mergeEvidence,
  recoverStoredState,
  restartEmptyConversation,
  type StagedRecord,
  stageReanchor,
  type MessageIntent as StoredMessageIntent,
} from "../store.js";
import {
  anchorRouterInstanceId,
  durablePosition,
  observedAnchorIsResolved,
  observedHeadIsResolved,
  orderedSignatures,
  protocolEvidence,
  stagedSuccessor,
} from "./history.js";

const acceptedDisposition: RouterIngressDisposition = "accepted";

const ignoredDisposition: RouterIngressDisposition = "ignored";

/** Limits re-anchoring to the recovery operations exposed by its run. */
export interface ReanchorRunPort {
  readonly runtime: EngineRuntime;
  readonly reason: RouterDiscontinuityReason;
  /** The Router instance the run polls, which a re-anchor moves conversations to. */
  readonly routerInstanceId: RouterWorkerRecovery["anchor"]["routerInstanceId"];
  /** Only conversations anchored to another Router instance need re-anchoring. */
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

/** Holds votes and positions ready for re-anchoring during one recovery run. */
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
 * A ready position certifies a usable vote, holds one awaiting history, or
 * declines one outside its known ancestry. Declined votes do not count.
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

type ReanchorVotes = Map<
  ConversationIdValue,
  Map<AnchorHashValue, Map<AgentId, PendingReanchorVote>>
>;

type WriteReanchor = typeof completeReanchor | typeof applyCatchUpReanchor;

/** Starts with no votes or ready positions. */
export function startReanchorRun(port: ReanchorRunPort): ReanchorRun {
  return { ...port, votes: new Map(), positionsReady: new Set() };
}

/** Advances a reconciled position toward a durable replacement anchor. */
export function positionReady(
  run: ReanchorRun,
  membership: VerifiedMembership,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient> {
  return Effect.sync(() => {
    run.positionsReady.add(membership.descriptor.conversationId);
  }).pipe(Effect.zipRight(finishRestartedPosition(run, membership)));
}

/** Accepts a verified member vote or safely ignores it. */
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

/** Accepts a completed member anchor or safely ignores it. */
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
 * Makes a verified completed anchor current, superseding a competing staged
 * candidate. Two certificates in one scope would require an honest member to
 * vote twice. Returns false if the anchor does not extend durable history.
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
  const agentKey = run.runtime.input.agentKey;

  const conversationId = membership.descriptor.conversationId;
  if (!run.reanchoring.has(conversationId)) {
    return finishAnchoredPosition(run, membership);
  }
  return durablePosition(agentKey, conversationId).pipe(
    Effect.flatMap(({ recovery, position }) => {
      if (position === undefined) {
        return Effect.fail(persistenceFailure());
      }
      if (position.headRecordHash === undefined) {
        if (currentAnchorForRecovery(run, conversationId) !== undefined) {
          return run.markRecovered(conversationId);
        }
        return restartEmptyPosition(agentKey, {
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
 * Replaces an initial foundation only before any certified history exists.
 * Persisting and adopting it share an interruption boundary: retries already
 * see the new foundation and would otherwise leave memory on the old one.
 */
function restartEmptyPosition(
  agentKey: string,
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
        const restarted = yield* restartEmptyConversation(agentKey, {
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
 * A staged successor blocks re-anchoring at its head. Resend it and the local
 * vote instead. Replay held votes before proposing because they may complete
 * the anchor.
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
 * Catch-up suffices when the retained anchor already names this Router. Relay
 * its completed re-anchor again for members still recovering.
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
    : verifyInboundVote(run, ingress, membership, {
        message,
        statement,
      }).pipe(
        Effect.catchTag("ClientRepresentationError", () =>
          Effect.succeed(ignoredDisposition),
        ),
        Effect.mapError(persistenceFailure),
      );
}

/**
 * Unusable member envelopes or vote evidence fail as representation errors,
 * which the caller ignores.
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

/** Only restarted conversations awaiting this run's Router instance accept completion. */
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

/** An ended run proposes nothing, including one ended during vote replay. */
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

/** Votes only after staging the candidate. A refusal leaves the conversation waiting. */
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

/** Replays only votes selecting the head catch-up supplied. */
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
 * Holds votes until the position is ready to judge them. Ignores votes outside
 * an active Router restart or after the conversation reaches the new Router.
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
 * Only restarted conversations awaiting this Router instance take votes.
 * Voting again after reaching it would start an unneeded second re-anchor.
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

/** A daemon restart preserves an anchor already naming the current Router. */
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

/** A position without a certified head has nothing to re-anchor and declines votes. */
function processReadyReanchorVote(
  run: ReanchorRun,
  membership: VerifiedMembership,
  vote: PendingReanchorVote,
): Effect.Effect<boolean, RouterWorkerPersistenceError, SqlClient> {
  const agentKey = run.runtime.input.agentKey;

  const body = vote.statement.reanchor;
  return durablePosition(agentKey, body.conversationId).pipe(
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
 * Holds votes for ancestors. An unknown selected record restarts catch-up
 * unless a candidate is already staged for this Router instance, when the
 * vote is declined. A staged successor blocks certification at its predecessor.
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
 * Certifies from the current anchor, holds from ancestors and declines
 * anchors outside retained ancestry.
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

/** A candidate the endpoint cannot stage or the store refuses contributes no vote. */
function certifyReanchorVote(
  run: ReanchorRun,
  membership: VerifiedMembership,
  vote: PendingReanchorVote,
): Effect.Effect<boolean, RouterWorkerPersistenceError, SqlClient> {
  const agentKey = run.runtime.input.agentKey;

  const body = vote.statement.reanchor;
  const anchorHash = vote.statement.anchorHash;
  return stageReanchorCandidate(run, membership, body, anchorHash).pipe(
    Effect.flatMap((staged) =>
      staged ? persistReanchorVote(agentKey, vote) : Effect.succeed(false),
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
 * Stages at most one candidate per anchor and Router instance, only at the
 * durable position. A later certified head preserves that candidate rather
 * than allowing a competing one.
 */
function stageReanchorCandidate(
  run: ReanchorRun,
  membership: VerifiedMembership,
  body: ReanchorBodyValue,
  anchorHash: AnchorHashValue,
): Effect.Effect<boolean, RouterWorkerPersistenceError, SqlClient> {
  const agentKey = run.runtime.input.agentKey;

  return durablePosition(agentKey, body.conversationId).pipe(
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
      return persistReanchorCandidate(agentKey, body, anchorHash);
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
  agentKey: string,
  body: ReanchorBodyValue,
  anchorHash: AnchorHashValue,
): Effect.Effect<boolean, RouterWorkerPersistenceError, SqlClient> {
  return encodeCanonical(ReanchorBody, body).pipe(
    Effect.mapError(persistenceFailure),
    Effect.flatMap((canonicalBody) =>
      stageReanchor(agentKey, {
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

/** An ended run signs and sends nothing. */
function ensureLocalReanchorVote(
  run: ReanchorRun,
  membership: VerifiedMembership,
  body: ReanchorBodyValue,
  anchorHash: AnchorHashValue,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient> {
  const agentKey = run.runtime.input.agentKey;

  if (!run.isActive()) {
    return Effect.void;
  }
  const localAgentId = run.runtime.input.localAgentCard.agentId;
  const statement = localReanchorVoteStatement(run.runtime, body, anchorHash);
  return decodeReanchorVotes(agentKey, membership, body, anchorHash).pipe(
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
 * Signs only when no local vote is stored. Refusal of the new local vote
 * therefore indicates inconsistent state.
 */
function createLocalReanchorVote(
  run: ReanchorRun,
  membership: VerifiedMembership,
  statement: PendingReanchorVote["statement"],
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient> {
  const agentKey = run.runtime.input.agentKey;

  return signEvidenceMessage({
    statement,
    identityCard: run.runtime.input.localAgentCard,
    signingAuthority: run.runtime.input.signingAuthority,
  }).pipe(
    Effect.mapError(persistenceFailure),
    Effect.flatMap((message) => {
      const vote: PendingReanchorVote = { message, statement };
      return persistReanchorVote(agentKey, vote).pipe(
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
 * A differently signed copy of one member's retained vote conflicts with its
 * stored bytes and does not count.
 */
function persistReanchorVote(
  agentKey: string,
  vote: PendingReanchorVote,
): Effect.Effect<boolean, RouterWorkerPersistenceError, SqlClient> {
  return protocolEvidence(
    vote.statement.reanchor.conversationId,
    "reanchor",
    vote.statement.anchorHash,
    vote.message,
  ).pipe(
    Effect.flatMap((evidence) => mergeEvidence(agentKey, evidence)),
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
  const agentKey = run.runtime.input.agentKey;

  if (!run.isActive() || run.isRecovered(body.conversationId)) {
    return Effect.void;
  }
  return decodeReanchorVotes(agentKey, membership, body, anchorHash).pipe(
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

/** Verifies retained votes and returns one per signer. */
function decodeReanchorVotes(
  agentKey: string,
  membership: VerifiedMembership,
  body: ReanchorBodyValue,
  anchorHash: AnchorHashValue,
): Effect.Effect<
  readonly SignedMessageValue[],
  RouterWorkerPersistenceError,
  SqlClient
> {
  return recoverStoredState(agentKey, body.conversationId).pipe(
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

/** Orders certificate votes by decoded AgentId bytes. Requires at least one vote. */
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
      write(runtime.input.agentKey, {
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
 * Drops the superseded action's fold when adopting the durable anchor. Its
 * previous-anchor evidence can no longer certify, and the store has retired
 * its lock and staged record.
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
