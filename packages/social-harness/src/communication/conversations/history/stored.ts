/**
 * @file Stored history: decoding and verifying the rows the endpoint store
 * keeps, verifying each conversation's record and anchor chain, reading
 * certified history back for the owner tools, and the snapshot queries
 * recovery asks of it.
 */

import { Effect, type ParseResult, Schema } from "effect";
import type { Ed25519PublicKey } from "../../../identity/keys.js";
import type {
  EndpointEngineInput,
  EngineConversation,
} from "../engine-context.js";
import {
  type ClientRepresentationError,
  decodeCanonical,
  encodeCanonical,
  sameBytes,
} from "../../../encoding/canonical-json.js";
import { agentViewOf } from "../../../identity/cards.js";
import { SignedMessage } from "../../../identity/envelope/signed-message.js";
import { Message, messageFromIntent } from "../../message.js";
import {
  type AnchorHash as AnchorHashValue,
  type CertifiedRecord,
  CompletedReanchor,
  type CompletedReanchor as CompletedReanchorValue,
  ConversationId,
  type ConversationId as ConversationIdValue,
  hashAnchor,
  InitialAnchorBody,
  type InitialAnchorBody as InitialAnchorBodyValue,
  MembershipDescriptor,
  MessageIntent,
  type RecordCore,
  RecordCore as RecordCoreSchema,
  RecordHash,
  type RecordHash as RecordHashValue,
  type RouterAnchor,
  type VerifiedMembership,
  verifyCertifiedRecord,
  verifyCompletedReanchor,
  verifyMembershipDescriptor,
  verifyOuterMessage,
} from "../../records/index.js";
import {
  RouterWorkerPersistenceError,
  RouterWorkerRecoveryError,
} from "../../router/index.js";
import * as Store from "../../store/index.js";
import {
  type EndpointRecovery,
  OutboxTooOldError,
  type ProtocolEvidence,
  type StagedRecord,
  type CertifiedRecord as StoredCertifiedRecord,
  type StoredMembership,
  type StoredOutboundMessage,
} from "../../store/index.js";
import {
  actionCertifiedRecord,
  type CertificateSignatures,
  certifiedRecord,
  orderedSignatures,
} from "./certificates.js";

/** Why a stored row failed to decode or to match what it claims to hold. */
export type StoredRowError =
  | ClientRepresentationError
  | ParseResult.ParseError
  | RouterWorkerPersistenceError;

/**
 * Decode one stored membership, verify it against the Registry signer, and
 * check that the row's conversation and membership hash name it.
 * @param stored Durable membership row.
 * @param registrySignerPublicKey Registry key the member cards verify under.
 * @returns The verified membership. A descriptor that fails to decode or
 *   verify is a representation error; a row whose columns disagree with its
 *   own descriptor is a persistence error.
 */
export function verifyStoredMembership(
  stored: StoredMembership,
  registrySignerPublicKey: Ed25519PublicKey,
): Effect.Effect<
  VerifiedMembership,
  ClientRepresentationError | RouterWorkerPersistenceError
> {
  return decodeCanonical(MembershipDescriptor, stored.canonicalMembership).pipe(
    Effect.flatMap((descriptor) =>
      verifyMembershipDescriptor(descriptor, registrySignerPublicKey),
    ),
    Effect.flatMap((membership) =>
      stored.conversationId === membership.descriptor.conversationId &&
      stored.membershipHash === membership.hash
        ? Effect.succeed(membership)
        : Effect.fail(persistenceFailure()),
    ),
  );
}

/**
 * Verify every stored membership row with `verifyStoredMembership`.
 * @param rows Durable membership rows.
 * @param registrySignerPublicKey Registry key the member cards verify under.
 * @returns Each verified membership, keyed by its conversation.
 */
export function verifyStoredMemberships(
  rows: readonly StoredMembership[],
  registrySignerPublicKey: Ed25519PublicKey,
): Effect.Effect<
  Map<ConversationIdValue, VerifiedMembership>,
  ClientRepresentationError | RouterWorkerPersistenceError
> {
  return Effect.forEach(
    rows,
    (stored) => verifyStoredMembership(stored, registrySignerPublicKey),
    { concurrency: 1 },
  ).pipe(
    Effect.map(
      (memberships) =>
        new Map(
          memberships.map((membership) => [
            membership.descriptor.conversationId,
            membership,
          ]),
        ),
    ),
  );
}

/**
 * Reconstruct one complete certified record from separately retained
 * evidence. Each evidence row must name the record's conversation, the kind
 * and subject its certificate covers, and its own signer; the signatures are
 * verified once, with the record as a whole.
 * @param membership Verified membership of the record's conversation, which a
 *     MESSAGE record names only by its hash.
 * @param stored Durable record core and signer-attributed evidence rows.
 * @param routerAnchor Verified anchor named by the durable record core.
 * @returns The complete record after all hashes and store projections match.
 */
export const recordFromStore = (
  membership: VerifiedMembership,
  stored: StoredCertifiedRecord,
  routerAnchor: EngineConversation["currentAnchor"],
): Effect.Effect<CertifiedRecord, StoredRowError> =>
  assembleStoredRecord(stored, routerAnchor).pipe(
    Effect.tap((record) => verifyCertifiedRecord({ record, membership })),
    Effect.filterOrFail(
      (record) =>
        storedRowMatchesCore(
          stored,
          record.actionCertifiedRecord.recordCore,
          membership,
        ),
      persistenceFailure,
    ),
    Effect.withSpan("recordFromStore"),
  );

/**
 * Read one stored certified record with the anchor row it names. The anchor
 * row's columns must match the anchor it holds, and the record row must pass
 * `recordFromStore`.
 * @param membership Verified membership of the record's conversation.
 * @param history Retained anchors needed to verify this record.
 * @param stored Durable record row.
 * @returns The complete verified record.
 */
export function readStoredRecord(
  membership: VerifiedMembership,
  history: Pick<EndpointRecovery, "anchors">,
  stored: StoredCertifiedRecord,
): Effect.Effect<CertifiedRecord, StoredRowError> {
  const anchor = history.anchors.find(
    (candidate) =>
      candidate.conversationId === stored.conversationId &&
      candidate.anchorHash === stored.anchorHash,
  );
  if (anchor === undefined) {
    return Effect.fail(persistenceFailure());
  }
  return decodeStoredAnchor(membership, anchor).pipe(
    Effect.flatMap((routerAnchor) =>
      recordFromStore(membership, stored, routerAnchor),
    ),
  );
}

/**
 * Read a page of stored certified history for the owner tools, with the same
 * row checks startup and recovery apply. Each membership the page names is
 * verified once, however many of its records the page holds.
 * @param registrySignerPublicKey Registry key the member cards verify under.
 * @param history Retained memberships and anchors named by these records.
 * @param records Durable record rows, in page order.
 * @returns The verified messages and their history positions, in page order.
 */
export function readStoredHistory(
  registrySignerPublicKey: Ed25519PublicKey,
  history: Pick<EndpointRecovery, "memberships" | "anchors">,
  records: readonly StoredCertifiedRecord[],
): Effect.Effect<
  ReadonlyArray<{
    readonly recordHash: RecordHashValue;
    readonly message: Message;
  }>,
  StoredRowError
> {
  const named = new Set(records.map((stored) => stored.conversationId));
  return Effect.forEach(
    history.memberships.filter((row) => named.has(row.conversationId)),
    (row) => verifyStoredMembership(row, registrySignerPublicKey),
    { concurrency: 1 },
  ).pipe(
    Effect.flatMap((memberships) =>
      Effect.forEach(
        records,
        (stored) => {
          const membership = memberships.find(
            (candidate) =>
              candidate.descriptor.conversationId === stored.conversationId,
          );
          return membership === undefined
            ? Effect.fail(persistenceFailure())
            : readStoredRecord(membership, history, stored).pipe(
                Effect.flatMap((record) => withMessage(membership, record)),
              );
        },
        { concurrency: 1 },
      ),
    ),
    Effect.withSpan("readStoredHistory"),
  );
}

/**
 * Resolve the Router instance named by one conversation's durable current anchor.
 * @param membership Fixed membership that owns the conversation.
 * @param recovery Complete verified store snapshot.
 * @returns The instance bound by the initial or completed re-anchor at the position.
 */
export function durableRouterInstanceId(
  membership: VerifiedMembership,
  recovery: EndpointRecovery,
): Effect.Effect<
  InitialAnchorBodyValue["routerInstanceId"],
  RouterWorkerRecoveryError
> {
  const conversationId = membership.descriptor.conversationId;
  const position = recovery.positions.find(
    (candidate) => candidate.conversationId === conversationId,
  );
  const stored = recovery.anchors.find(
    (candidate) =>
      candidate.conversationId === conversationId &&
      candidate.anchorHash === position?.currentAnchorHash,
  );
  if (stored === undefined) {
    return Effect.fail(recoveryFailure());
  }
  return decodeStoredAnchor(membership, stored).pipe(
    Effect.map(anchorRouterInstanceId),
    Effect.mapError(recoveryFailure),
  );
}

/**
 * Name the Router instance an anchor binds: the initial anchor's instance, or
 * the instance a completed re-anchor moved the conversation to.
 * @param anchor Verified initial or completed re-anchor.
 * @returns The bound Router instance.
 */
export function anchorRouterInstanceId(
  anchor: RouterAnchor,
): InitialAnchorBodyValue["routerInstanceId"] {
  return anchor.kind === "initial_anchor_body"
    ? anchor.routerInstanceId
    : anchor.reanchor.routerInstanceId;
}

/**
 * Decode and verify one durable initial or completed re-anchor row.
 * @param membership Fixed membership that owns the anchor.
 * @param stored Durable anchor row and canonical representation.
 * @returns A verified anchor usable for record reconstruction.
 */
export function decodeStoredAnchor(
  membership: VerifiedMembership,
  stored: EndpointRecovery["anchors"][number],
): Effect.Effect<
  EngineConversation["currentAnchor"],
  RouterWorkerPersistenceError
> {
  const decoded =
    stored.previousAnchorHash === undefined
      ? decodeStoredInitialAnchor(membership, stored)
      : decodeStoredReanchor(membership, stored);
  return decoded.pipe(Effect.withSpan("decodeStoredAnchor"));
}

/**
 * Verify every retained current outer envelope before Router resumption.
 * @param input Engine identity and signature-verification dependencies.
 * @param outbounds Exact pending rows recovered in durable insertion order.
 * @param memberships Verified membership for each retained conversation.
 * @returns The unchanged rows after canonical bytes and attribution verify.
 */
export function verifyStoredOutbounds(
  input: EndpointEngineInput,
  outbounds: readonly StoredOutboundMessage[],
  memberships: ReadonlyMap<ConversationIdValue, VerifiedMembership>,
): Effect.Effect<
  readonly StoredOutboundMessage[],
  StoredRowError | OutboxTooOldError
> {
  return Effect.forEach(
    outbounds,
    (outbound) => verifyStoredOutbound(input, memberships, outbound),
    { concurrency: 1 },
  );
}

/**
 * Verify that every recovered record and anchor forms one gap-free history.
 * @param recovery Complete store snapshot to verify.
 * @param memberships Verified fixed memberships keyed by conversation.
 * @returns Completion only when every retained history is complete and unique.
 */
export function verifyRecoveredHistory(
  recovery: EndpointRecovery,
  memberships: Map<ConversationIdValue, VerifiedMembership>,
): Effect.Effect<void, RouterWorkerRecoveryError> {
  if (!recoveryOnlyContainsMemberships(recovery, memberships)) {
    return Effect.fail(recoveryFailure());
  }
  return Effect.forEach(
    memberships.values(),
    (membership) => verifyConversationHistory(recovery, membership),
    { concurrency: 1, discard: true },
  ).pipe(Effect.withSpan("verifyRecoveredHistory"));
}

/**
 * Check that a stored record row's columns name the record core it holds.
 * @param row Staged or certified record row.
 * @param core The row's decoded record core.
 * @param membership Verified membership of the row's conversation.
 * @returns Whether every projected column matches.
 */
export function storedRowMatchesCore(
  row: StagedRecord,
  core: RecordCore,
  membership: VerifiedMembership,
): boolean {
  return [
    row.conversationId === membership.descriptor.conversationId,
    row.membershipHash === membership.hash,
    row.previousRecordHash === (core.action.previousRecordHash ?? undefined),
    row.anchorHash === core.anchorHash,
    row.actionHash === core.actionHash,
    row.authorAgentId === core.action.messageIntent.authorAgentId,
    row.msgId === core.action.messageIntent.msgId,
  ].every((matches) => matches);
}

/**
 * Resolve the latest durable position for one fixed conversation.
 * @param conversationId Private conversation identity to locate.
 * @returns The conversation's recovery snapshot and its matching position row.
 */
export const durablePosition = (conversationId: ConversationIdValue) =>
  Store.recover(conversationId).pipe(
    Effect.mapError(persistenceFailure),
    Effect.map((recovery) => ({
      recovery,
      position: recovery.positions.find(
        (candidate) => candidate.conversationId === conversationId,
      ),
    })),
  );

/**
 * The staged record that extends a position but holds no durability
 * certificate yet. At most one exists: its action certificate and this
 * endpoint's proposal lock select one successor per predecessor and anchor.
 * @param recovery Complete verified recovery snapshot.
 * @param conversationId Conversation whose position is examined.
 * @param head Record the successor extends.
 * @param anchorHash Anchor the successor binds; any anchor when omitted.
 * @returns The staged, uncertified successor, when this endpoint holds one.
 */
export function stagedSuccessor(
  recovery: EndpointRecovery,
  conversationId: ConversationIdValue,
  head: RecordHashValue,
  anchorHash?: AnchorHashValue,
): EndpointRecovery["stagedRecords"][number] | undefined {
  return recovery.stagedRecords.find(
    (record) =>
      record.conversationId === conversationId &&
      record.previousRecordHash === head &&
      bindsAnchor(record, anchorHash) &&
      !isCertified(recovery, record),
  );
}

/**
 * Determine whether one observed record belongs to the retained head ancestry.
 * @param recovery Complete verified recovery snapshot.
 * @param conversationId Conversation whose record chain is examined.
 * @param observed Record hash reported by a fixed member.
 * @param head Locally retained certified head.
 * @returns Whether the observed record is the head or one of its ancestors.
 */
export function observedHeadIsResolved(
  recovery: EndpointRecovery,
  conversationId: ConversationIdValue,
  observed: RecordHashValue,
  head: RecordHashValue,
): boolean {
  return chainContains(
    head,
    observed,
    (recordHash) =>
      recovery.certifiedRecords.find(
        (candidate) =>
          candidate.conversationId === conversationId &&
          candidate.recordHash === recordHash,
      )?.previousRecordHash,
  );
}

/**
 * Determine whether one observed anchor belongs to the retained anchor chain.
 * @param recovery Complete verified recovery snapshot.
 * @param conversationId Conversation whose anchor chain is examined.
 * @param observed Anchor hash reported by a fixed member.
 * @param current Locally retained current anchor.
 * @returns Whether the observed anchor is current or one of its ancestors.
 */
export function observedAnchorIsResolved(
  recovery: EndpointRecovery,
  conversationId: ConversationIdValue,
  observed: AnchorHashValue,
  current: AnchorHashValue,
): boolean {
  return chainContains(
    current,
    observed,
    (anchorHash) =>
      recovery.anchors.find(
        (candidate) =>
          candidate.conversationId === conversationId &&
          candidate.anchorHash === anchorHash,
      )?.previousAnchorHash,
  );
}

/** Decoded intent identity and completion are validated once by the protocol's stored reader. */
export function readStoredMessageIntent(
  stored: Store.MessageIntent,
  memberships: ReadonlyMap<ConversationIdValue, VerifiedMembership>,
) {
  return Effect.gen(function* () {
    const intent = yield* decodeCanonical(
      MessageIntent,
      stored.canonicalIntent,
    );
    if (
      intent.conversationId !== stored.conversationId ||
      intent.membershipHash !== stored.membershipHash ||
      intent.authorAgentId !== stored.authorAgentId ||
      intent.msgId !== stored.msgId
    ) {
      return yield* persistenceFailure();
    }
    const membership = memberships.get(intent.conversationId);
    if (membership === undefined) {
      return yield* persistenceFailure();
    }
    const message = yield* messageFromIntent(
      membership.members.map(agentViewOf),
      intent,
    );
    if (
      !sameBytes(
        yield* encodeCanonical(Message, message),
        stored.canonicalMessage,
      )
    ) {
      return yield* persistenceFailure();
    }
    const completedRecordHash =
      stored.completedRecordHash === undefined
        ? undefined
        : yield* Schema.decodeUnknown(RecordHash)(stored.completedRecordHash);
    return {
      intent,
      canonicalIntent: stored.canonicalIntent,
      completedRecordHash,
    };
  }).pipe(Effect.withSpan("readStoredMessageIntent"));
}

function recoveryOnlyContainsMemberships(
  recovery: EndpointRecovery,
  memberships: ReadonlyMap<ConversationIdValue, VerifiedMembership>,
): boolean {
  const known = new Set<string>(memberships.keys());
  const validCollections = [
    recovery.positions.length === memberships.size,
    recovery.anchors.every((anchor) => known.has(anchor.conversationId)),
    recovery.certifiedRecords.every((record) =>
      known.has(record.conversationId),
    ),
  ];
  return validCollections.every((valid) => valid);
}

interface HistoryCursor {
  readonly currentAnchor: EngineConversation["currentAnchor"];
  readonly currentAnchorHash: string;
  readonly currentRecordHash: string | null;
  readonly anchorIndex: number;
}

interface ConversationHistory {
  readonly position: EndpointRecovery["positions"][number];
  readonly anchors: ReadonlyArray<EndpointRecovery["anchors"][number]>;
  readonly records: ReadonlyArray<EndpointRecovery["certifiedRecords"][number]>;
  readonly initial: HistoryCursor;
}

function verifyConversationHistory(
  recovery: EndpointRecovery,
  membership: VerifiedMembership,
): Effect.Effect<void, RouterWorkerRecoveryError> {
  return Effect.gen(function* () {
    const history = yield* conversationHistory(recovery, membership);
    let cursor = history.initial;
    for (const stored of history.records) {
      if (!recordExtendsCursor(stored, membership, cursor)) {
        return yield* Effect.fail(recoveryFailure());
      }
      yield* recordFromStore(membership, stored, cursor.currentAnchor);
      cursor = { ...cursor, currentRecordHash: stored.recordHash };
      let nextAnchor = history.anchors[cursor.anchorIndex];
      while (
        nextAnchor !== undefined &&
        anchorExtendsCursor(nextAnchor, cursor)
      ) {
        const currentAnchor = yield* decodeStoredAnchor(membership, nextAnchor);
        cursor = {
          currentAnchor,
          currentAnchorHash: nextAnchor.anchorHash,
          currentRecordHash: cursor.currentRecordHash,
          anchorIndex: cursor.anchorIndex + 1,
        };
        nextAnchor = history.anchors[cursor.anchorIndex];
      }
    }
    if (!historyCursorMatchesPosition(history, cursor)) {
      return yield* Effect.fail(recoveryFailure());
    }
  }).pipe(Effect.mapError(recoveryFailure));
}

function conversationHistory(
  recovery: EndpointRecovery,
  membership: VerifiedMembership,
): Effect.Effect<ConversationHistory, RouterWorkerRecoveryError> {
  const conversationId = membership.descriptor.conversationId;
  const positions = recovery.positions.filter(
    (candidate) => candidate.conversationId === conversationId,
  );
  const anchors = recovery.anchors.filter(
    (candidate) => candidate.conversationId === conversationId,
  );
  const position = positions[0];
  const firstAnchor = anchors[0];
  if (
    position === undefined ||
    firstAnchor === undefined ||
    !validHistoryFoundation(positions, position, firstAnchor, membership)
  ) {
    return Effect.fail(recoveryFailure());
  }
  return decodeStoredAnchor(membership, firstAnchor).pipe(
    Effect.map((currentAnchor) => ({
      position,
      anchors,
      records: recovery.certifiedRecords.filter(
        (candidate) => candidate.conversationId === conversationId,
      ),
      initial: {
        currentAnchor,
        currentAnchorHash: firstAnchor.anchorHash,
        currentRecordHash: null,
        anchorIndex: 1,
      },
    })),
    Effect.mapError(recoveryFailure),
  );
}

function validHistoryFoundation(
  positions: ReadonlyArray<EndpointRecovery["positions"][number]>,
  position: EndpointRecovery["positions"][number],
  firstAnchor: EndpointRecovery["anchors"][number],
  membership: VerifiedMembership,
): boolean {
  return [
    positions.length === 1,
    position.membershipHash === membership.hash,
    firstAnchor.previousAnchorHash === undefined,
  ].every((matches) => matches);
}

function recordExtendsCursor(
  stored: EndpointRecovery["certifiedRecords"][number],
  membership: VerifiedMembership,
  cursor: HistoryCursor,
): boolean {
  const previous = stored.previousRecordHash ?? null;
  return [
    stored.membershipHash === membership.hash,
    previous === cursor.currentRecordHash,
    stored.anchorHash === cursor.currentAnchorHash,
  ].every((matches) => matches);
}

function anchorExtendsCursor(
  anchor: EndpointRecovery["anchors"][number],
  cursor: HistoryCursor,
): boolean {
  return (
    anchor.previousAnchorHash === cursor.currentAnchorHash &&
    anchor.selectedRecordHash === cursor.currentRecordHash
  );
}

function historyCursorMatchesPosition(
  history: ConversationHistory,
  cursor: HistoryCursor,
): boolean {
  return [
    cursor.anchorIndex === history.anchors.length,
    (history.position.headRecordHash ?? null) === cursor.currentRecordHash,
    history.position.currentAnchorHash === cursor.currentAnchorHash,
  ].every((matches) => matches);
}

function verifyStoredOutbound(
  input: EndpointEngineInput,
  memberships: ReadonlyMap<ConversationIdValue, VerifiedMembership>,
  outbound: StoredOutboundMessage,
): Effect.Effect<StoredOutboundMessage, StoredRowError | OutboxTooOldError> {
  return Effect.gen(function* () {
    const retained = yield* decodeCanonical(
      SignedMessage,
      outbound.canonicalSignedMessage,
    );
    if (SignedMessage.wireVersion(retained) !== outbound.wireVersion) {
      return yield* Effect.fail(persistenceFailure());
    }
    const representation = yield* Schema.encode(SignedMessage)(retained);
    const message = yield* Schema.decodeUnknown(SignedMessage.live)(
      representation,
    ).pipe(
      Effect.catchTag("ParseError", () => Effect.fail(new OutboxTooOldError())),
    );
    const conversationId = yield* Schema.decodeUnknown(ConversationId)(
      outbound.conversationId,
    );
    const membership = memberships.get(conversationId);
    if (membership === undefined) {
      return yield* Effect.fail(persistenceFailure());
    }
    if (
      message.envelopeId !== outbound.envelopeId ||
      message.senderAgentId !== input.localAgentCard.agentId
    ) {
      return yield* Effect.fail(persistenceFailure());
    }
    yield* verifyOuterMessage({ message, membership });
    return outbound;
  });
}

function assembleStoredRecord(
  stored: StoredCertifiedRecord,
  routerAnchor: EngineConversation["currentAnchor"],
): Effect.Effect<CertifiedRecord, StoredRowError> {
  return Effect.gen(function* () {
    const recordCore = yield* decodeCanonical(
      RecordCoreSchema,
      stored.canonicalRecordCore,
    );
    const signatures = yield* restoreEvidence(stored.actionEvidence, {
      conversationId: stored.conversationId,
      kind: "action",
      subjectId: stored.actionHash,
    });
    const votes = yield* restoreEvidence(stored.durabilityEvidence, {
      conversationId: stored.conversationId,
      kind: "durability",
      subjectId: stored.recordHash,
    });
    const recordHash = yield* Schema.decodeUnknown(RecordHash)(
      stored.recordHash,
    );
    return certifiedRecord(
      actionCertifiedRecord(recordCore, recordHash, routerAnchor, signatures),
      votes,
    );
  });
}

/**
 * Decode one certificate's evidence rows. A row that names another
 * conversation, kind or subject, or whose key is not its message's signer,
 * is a persistence error.
 */
function restoreEvidence(
  rows: readonly ProtocolEvidence[],
  expected: Pick<ProtocolEvidence, "conversationId" | "kind" | "subjectId">,
): Effect.Effect<CertificateSignatures, StoredRowError> {
  return Effect.forEach(
    rows,
    (row) =>
      row.conversationId === expected.conversationId &&
      row.kind === expected.kind &&
      row.subjectId === expected.subjectId
        ? decodeCanonical(SignedMessage, row.canonicalEvidence).pipe(
            Effect.filterOrFail(
              (message) => message.senderAgentId === row.evidenceKey,
              persistenceFailure,
            ),
          )
        : Effect.fail(persistenceFailure()),
    { concurrency: 1 },
  ).pipe(
    Effect.flatMap(orderedSignatures),
    Effect.flatMap((signatures) =>
      signatures === undefined
        ? Effect.fail(persistenceFailure())
        : Effect.succeed(signatures),
    ),
  );
}

function decodeStoredInitialAnchor(
  membership: VerifiedMembership,
  stored: EndpointRecovery["anchors"][number],
): Effect.Effect<
  EngineConversation["currentAnchor"],
  RouterWorkerPersistenceError
> {
  return decodeCanonical(InitialAnchorBody, stored.canonicalAnchor).pipe(
    Effect.flatMap((anchor) =>
      hashAnchor(anchor).pipe(
        Effect.flatMap((anchorHash) =>
          storedInitialAnchorMatches(membership, stored, anchor, anchorHash)
            ? Effect.succeed(anchor)
            : Effect.fail(persistenceFailure()),
        ),
      ),
    ),
    Effect.mapError(persistenceFailure),
  );
}

function storedInitialAnchorMatches(
  membership: VerifiedMembership,
  stored: EndpointRecovery["anchors"][number],
  anchor: InitialAnchorBodyValue,
  anchorHash: AnchorHashValue,
): boolean {
  return [
    anchor.conversationId === stored.conversationId,
    anchor.membershipHash === membership.hash,
    stored.selectedRecordHash === undefined,
    anchorHash === stored.anchorHash,
  ].every((matches) => matches);
}

function decodeStoredReanchor(
  membership: VerifiedMembership,
  stored: EndpointRecovery["anchors"][number],
): Effect.Effect<
  EngineConversation["currentAnchor"],
  RouterWorkerPersistenceError
> {
  return decodeCanonical(CompletedReanchor, stored.canonicalAnchor).pipe(
    Effect.flatMap((completed) =>
      verifyCompletedReanchor({ completed, membership }).pipe(
        Effect.flatMap((anchorHash) =>
          storedReanchorMatches(stored, completed, anchorHash)
            ? Effect.succeed(completed)
            : Effect.fail(persistenceFailure()),
        ),
      ),
    ),
    Effect.mapError(persistenceFailure),
  );
}

function storedReanchorMatches(
  stored: EndpointRecovery["anchors"][number],
  completed: CompletedReanchorValue,
  anchorHash: AnchorHashValue,
): boolean {
  return [
    anchorHash === stored.anchorHash,
    completed.reanchor.previousAnchorHash === stored.previousAnchorHash,
    completed.reanchor.selectedRecordHash === stored.selectedRecordHash,
  ].every((matches) => matches);
}

function chainContains(
  start: string,
  target: string,
  parentOf: (hash: string) => string | undefined,
): boolean {
  for (
    let cursor: string | undefined = start;
    cursor !== undefined;
    cursor = parentOf(cursor)
  ) {
    if (cursor === target) {
      return true;
    }
  }
  return false;
}

/** Keeps record verification and message construction inside communication. */
function withMessage(membership: VerifiedMembership, record: CertifiedRecord) {
  return messageFromIntent(
    membership.members.map(agentViewOf),
    record.actionCertifiedRecord.recordCore.action.messageIntent,
  ).pipe(
    Effect.map((message) => ({
      recordHash: record.actionCertifiedRecord.recordHash,
      message,
    })),
    Effect.mapError(persistenceFailure),
  );
}

function persistenceFailure(): RouterWorkerPersistenceError {
  return new RouterWorkerPersistenceError();
}

function recoveryFailure(): RouterWorkerRecoveryError {
  return new RouterWorkerRecoveryError();
}

function bindsAnchor(
  record: EndpointRecovery["stagedRecords"][number],
  anchorHash?: AnchorHashValue,
): boolean {
  return anchorHash === undefined || record.anchorHash === anchorHash;
}

function isCertified(
  recovery: EndpointRecovery,
  record: EndpointRecovery["stagedRecords"][number],
): boolean {
  return recovery.certifiedRecords.some(
    (certified) =>
      certified.conversationId === record.conversationId &&
      certified.recordHash === record.recordHash,
  );
}
