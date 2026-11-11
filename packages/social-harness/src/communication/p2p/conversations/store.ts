/** @file Retains peer certificates, anchors, prepared messages and outbound recovery state. */

import { SqlClient } from "@effect/sql/SqlClient";
import { Data, Effect, Schema } from "effect";

import { decodeCanonical } from "../../../encoding/index.js";
import { AgentId, AgentIdentityCard } from "../../../identity/index.js";
import {
  conversationSortKey,
  messageFromIntent,
  MessageReference,
} from "../../messages.js";
import { MembershipDescriptor, RecordCore } from "../../records.js";
import {
  acknowledgeMessage,
  agentOperation,
  completeMessage,
  copyBytes,
  EndpointStoreError,
  readAcceptedRow,
  readBytes,
  readInteger,
  readOptionalBytes,
  readOptionalText,
  readStoredIdentity,
  readText,
  requireEqual,
  requireSameBytes,
  requireText,
  type StoreMutation,
  storeOperation,
  StoreSignal,
  transaction,
} from "../../store.js";

/** Pending signed bytes require an earlier release to drain the outbox. */
export class OutboxTooOldError extends Data.TaggedError("OutboxTooOldError") {}

/** One locally authored immutable message intent and its completion. */
export interface MessageIntent {
  readonly conversationId: string;
  readonly membershipHash: string;
  readonly authorAgentId: string;
  readonly msgId: string;
  readonly canonicalIntent: Uint8Array;
  readonly canonicalMessage: Uint8Array;
  readonly completedRecordHash?: string;
}

/** Atomic binding for a message in either retained or newly founded history. */
export type MessageIntentBinding =
  | Readonly<{
      kind: "existing-conversation";
      intent: MessageIntent;
    }>
  | Readonly<{
      kind: "new-conversation";
      foundation: ConversationFoundation;
      intent: MessageIntent;
    }>;

/** Immutable membership and initial Router anchor for one conversation. */
export interface ConversationFoundation {
  readonly conversationId: string;
  readonly membershipHash: string;
  readonly canonicalMembership: Uint8Array;
  readonly anchorHash: string;
  readonly canonicalAnchor: Uint8Array;
}

/** One immutable membership descriptor recovered independently of anchors. */
export interface StoredMembership {
  readonly conversationId: string;
  readonly membershipHash: string;
  readonly canonicalMembership: Uint8Array;
}

/** One initial or completed Router-epoch anchor in the retained chain. */
export interface StoredAnchor {
  readonly conversationId: string;
  readonly anchorHash: string;
  readonly previousAnchorHash?: string;
  readonly selectedRecordHash?: string;
  readonly canonicalAnchor: Uint8Array;
}

/** The first Router-ordered action selected for one predecessor. */
export interface ProposalLock {
  readonly conversationId: string;
  readonly previousRecordHash?: string;
  readonly actionHash: string;
  readonly canonicalActionCore: Uint8Array;
}

/** One exact record core staged before a durability vote. */
export interface StagedRecord {
  readonly conversationId: string;
  readonly recordHash: string;
  readonly previousRecordHash?: string;
  readonly membershipHash: string;
  readonly anchorHash: string;
  readonly actionHash: string;
  readonly authorAgentId: string;
  readonly msgId: string;
  readonly canonicalRecordCore: Uint8Array;
}

/** Mergeable protocol evidence addressed by its stable private identity. */
export interface ProtocolEvidence {
  readonly conversationId: string;
  readonly kind: "action" | "durability" | "catch-up" | "reanchor";
  readonly subjectId: string;
  readonly evidenceKey: string;
  readonly canonicalEvidence: Uint8Array;
}

/** One certified record with action and durability evidence kept separate. */
export interface CertifiedRecord extends StagedRecord {
  readonly actionEvidence: readonly ProtocolEvidence[];
  readonly durabilityEvidence: readonly ProtocolEvidence[];
}

/** Canonical complete outer message before it receives a stable outbox key. */
export interface OutboundMessageInput {
  readonly recordHash?: string;
  readonly wireVersion: string;
  readonly conversationId: string;
  readonly envelopeId: string;
  readonly canonicalSignedMessage: Uint8Array;
}

/** One immutable signed envelope retained across transport retries. */
export type StoredOutboundMessage = OutboundMessageInput;

/** Durable disposition chosen immediately before one Router attempt. */
export type OutboundAttempt =
  | Readonly<{
      kind: "inactive";
    }>
  | Readonly<{
      kind: "pending";
      mode: "initial" | "retry";
      outbound: StoredOutboundMessage;
    }>;

/** Exact empty-history Router-anchor replacement guarded by the old foundation. */
export interface EmptyConversationRestart {
  readonly expectedFoundation: ConversationFoundation;
  readonly replacementFoundation: ConversationFoundation;
}

/** Durable state retained after an empty conversation changes Router instance. */
export interface RestartedEmptyConversation {
  readonly foundation: ConversationFoundation;
  readonly messageIntents: readonly MessageIntent[];
}

/** One staged Router-epoch proposal. */
export interface StagedReanchor {
  readonly conversationId: string;
  readonly anchorHash: string;
  readonly previousAnchorHash: string;
  readonly routerInstanceId: string;
  readonly selectedRecordHash: string;
  readonly canonicalBody: Uint8Array;
}

/** One completed Router-epoch proposal and its threshold evidence. */
export interface CompletedReanchor extends StagedReanchor {
  readonly canonicalCompletedReanchor: Uint8Array;
}

/** One recoverable re-anchor candidate, complete only when evidence is present. */
interface RecoveredReanchor extends StagedReanchor {
  readonly canonicalCompletedReanchor?: Uint8Array;
}

/** One durable current position for a conversation. */
interface ConversationPosition {
  readonly conversationId: string;
  readonly membershipHash: string;
  readonly currentAnchorHash: string;
  readonly headRecordHash?: string;
}

/** Complete private state recovered after opening the endpoint database. */
export interface EndpointRecovery {
  readonly identity?: { readonly agentId: string };
  readonly messageIntents: readonly MessageIntent[];
  readonly memberships: readonly StoredMembership[];
  readonly anchors: readonly StoredAnchor[];
  readonly positions: readonly ConversationPosition[];
  readonly proposalLocks: readonly ProposalLock[];
  readonly stagedRecords: readonly StagedRecord[];
  readonly evidence: readonly ProtocolEvidence[];
  readonly certifiedRecords: readonly CertifiedRecord[];
  readonly stagedReanchors: readonly RecoveredReanchor[];
  readonly outboundMessages: readonly StoredOutboundMessage[];
}

/** The predecessor key of a conversation's first record. */
const FIRST_MESSAGE_PREDECESSOR = "";

/** Durable position plus the local indexing aid that carries no authority. */
interface InternalPosition extends ConversationPosition {
  readonly headOrdinal: number;
}

const historySelect = (sql: SqlClient) => sql`
  SELECT certified.sequence AS sequence, staged.conversation_id, staged.record_hash,
         staged.previous_record_hash, staged.membership_hash,
         staged.anchor_hash, staged.action_hash, staged.author_agent_id,
         staged.msg_id, staged.canonical_record_core
  FROM certified_records AS certified
  JOIN staged_records AS staged
    ON staged.agent_key=certified.agent_key AND staged.conversation_id = certified.conversation_id
   AND staged.record_hash = certified.record_hash
`;

type OutboundDisposition = "accepted" | "discarded" | "pending";

interface RetainedOutbound {
  readonly outbound: StoredOutboundMessage;
  readonly attempted: boolean;
  readonly disposition: OutboundDisposition;
}

/**
 * Durably binds one author-scoped message to its immutable canonical intent.
 *
 * @param agentKey Immutable storage owner for this operation.
 * @param binding Canonical message recovery identity and optional new foundation.
 * @returns Whether the same binding was inserted or already durable.
 */
export function bindMessageIntent(
  agentKey: string,
  binding: MessageIntentBinding,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    validateMessageIntentBinding(binding);
    return yield* transaction(
      agentKey,
      bindIntentWithFoundation(agentKey, binding),
    );
  }).pipe(agentOperation(agentKey), Effect.withSpan("bindMessageIntent"));
}

/**
 * Atomically persists the immutable membership and initial Router anchor.
 *
 * @param agentKey Immutable storage owner for this operation.
 * @param foundation Verified immutable conversation foundation.
 * @returns Whether the same foundation was inserted or already durable.
 */
export function putConversationFoundation(
  agentKey: string,
  foundation: ConversationFoundation,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient> {
  return transaction(
    agentKey,
    putConversationFoundationInTransaction(agentKey, foundation),
  ).pipe(agentOperation(agentKey));
}

/**
 * Selects the first valid Router-ordered action durably. Sign only after
 * success to prevent conflicting signatures across restart. Returns inserted
 * or an identical existing lock.
 */
export function lockProposal(
  agentKey: string,
  proposal: ProposalLock,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    validateProposalLock(proposal);
    return yield* transaction(
      agentKey,
      lockProposalInTransaction(agentKey, proposal),
    );
  }).pipe(agentOperation(agentKey), Effect.withSpan("lockProposal"));
}

/**
 * Atomically retains a verified FIRST_MESSAGE foundation and its first-candidate lock.
 *
 * @param agentKey Immutable storage owner for this operation.
 * @param foundation Verified immutable membership and Router anchor.
 * @param proposal Verified gap-free FIRST_MESSAGE action selected in Router order.
 * @returns Whether either exact durable component was inserted.
 */
export function lockFirstMessageProposal(
  agentKey: string,
  foundation: ConversationFoundation,
  proposal: ProposalLock,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    validateProposalLock(proposal);
    if (
      proposal.previousRecordHash !== undefined ||
      proposal.conversationId !== foundation.conversationId
    ) {
      return yield* new EndpointStoreError({ reason: "invalid-input" });
    }
    return yield* transaction(
      agentKey,
      Effect.gen(function* () {
        const retainedFoundation =
          yield* putConversationFoundationInTransaction(agentKey, foundation);
        const retainedProposal = yield* lockProposalInTransaction(
          agentKey,
          proposal,
        );
        return retainedFoundation === "inserted" ||
          retainedProposal === "inserted"
          ? "inserted"
          : "existing";
      }),
    );
  }).pipe(
    agentOperation(agentKey),
    Effect.withSpan("lockFirstMessageProposal"),
  );
}

/**
 * Requires the exact old foundation and an empty certified head, protecting
 * committed history and concurrent anchor changes. Returns the replacement
 * and retained immutable intents.
 */
export function restartEmptyConversation(
  agentKey: string,
  restart: EmptyConversationRestart,
): Effect.Effect<RestartedEmptyConversation, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    validateEmptyConversationRestart(restart);
    return yield* transaction(
      agentKey,
      Effect.gen(function* () {
        yield* requireEmptyConversationPosition(
          agentKey,
          restart.expectedFoundation,
        );
        yield* clearIncompleteConversationState(
          agentKey,
          restart.expectedFoundation,
        );
        yield* installReplacementFoundation(
          agentKey,
          restart.replacementFoundation,
        );
        return Object.freeze({
          foundation: copyFoundation(restart.replacementFoundation),
          messageIntents: yield* readConversationMessageIntents(
            agentKey,
            restart.replacementFoundation.conversationId,
          ),
        });
      }),
    );
  }).pipe(
    agentOperation(agentKey),
    Effect.withSpan("restartEmptyConversation"),
  );
}

/**
 * Stages one candidate per Router-instance scope for a local vote. Refuses
 * to leave a head with a staged successor under that anchor.
 */
export function stageReanchor(
  agentKey: string,
  reanchor: StagedReanchor,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    validateStagedReanchor(reanchor);
    return yield* transaction(
      agentKey,
      stageReanchorInTransaction(agentKey, reanchor, "vote"),
    );
  }).pipe(agentOperation(agentKey), Effect.withSpan("stageReanchor"));
}

/**
 * Atomically makes a staged threshold-complete Router anchor current.
 *
 * @param agentKey Immutable storage owner for this operation.
 * @param reanchor Verified completed re-anchor and threshold evidence.
 * @returns Whether completion advanced the current anchor or was idempotent.
 */
export function completeReanchor(
  agentKey: string,
  reanchor: CompletedReanchor,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    validateCompletedReanchor(reanchor);
    return yield* transaction(
      agentKey,
      completeReanchorInTransaction(agentKey, reanchor),
    );
  }).pipe(agentOperation(agentKey), Effect.withSpan("completeReanchor"));
}

/**
 * Adopts a quorum-certified anchor even over a competing staged candidate.
 * The rule requiring one candidate limits local votes because two certificates
 * in one scope would require an honest member to vote twice.
 */
export function applyCatchUpReanchor(
  agentKey: string,
  reanchor: CompletedReanchor,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    validateCompletedReanchor(reanchor);
    return yield* transaction(
      agentKey,
      Effect.gen(function* () {
        yield* retireSupersededCandidates(agentKey, reanchor);
        const staged = yield* stageReanchorInTransaction(
          agentKey,
          reanchor,
          "adopted",
        );
        const completed = yield* completeReanchorInTransaction(
          agentKey,
          reanchor,
        );
        return staged === "inserted" || completed === "inserted"
          ? "inserted"
          : "existing";
      }),
    );
  }).pipe(agentOperation(agentKey), Effect.withSpan("applyCatchUpReanchor"));
}

/**
 * Recovers all durable endpoint state needed to resume protocol work.
 *
 * @param agentKey Immutable storage owner for this operation.
 * @param conversationId Restrict protocol rows to one conversation when provided.
 * @returns A detached snapshot of the selected durable endpoint protocol state.
 */
export function recoverStoredState(
  agentKey: string,
  conversationId?: string,
): Effect.Effect<EndpointRecovery, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    const identity = yield* readStoredIdentity(agentKey);
    const conversation =
      conversationId === undefined
        ? database``
        : database`AND conversation_id=${conversationId}`;
    return Object.freeze({
      ...(identity === undefined ? {} : { identity }),
      messageIntents: yield* readMessageIntents(agentKey, conversationId),
      memberships: yield* readMemberships(agentKey, conversationId),
      anchors: yield* readAnchors(agentKey, conversationId),
      positions: yield* readPositions(agentKey, conversationId),
      proposalLocks: Object.freeze(
        (yield* database<
          Readonly<Record<string, unknown>>
        >`SELECT conversation_id,previous_record_hash,action_hash,canonical_action_core FROM proposal_locks WHERE agent_key=${agentKey} ${conversation} ORDER BY conversation_id,predecessor_key`).map(
          readProposalLock,
        ),
      ),
      stagedRecords: Object.freeze(
        (yield* database<
          Readonly<Record<string, unknown>>
        >`SELECT conversation_id,record_hash,previous_record_hash,membership_hash,anchor_hash,action_hash,author_agent_id,msg_id,canonical_record_core FROM staged_records WHERE agent_key=${agentKey} ${conversation} ORDER BY conversation_id,sequence`).map(
          readStagedRecord,
        ),
      ),
      evidence: yield* readEvidence(agentKey, conversationId),
      certifiedRecords: yield* readAllCertifiedRecords(
        agentKey,
        conversationId,
      ),
      stagedReanchors: yield* readReanchors(agentKey, conversationId),
      outboundMessages: yield* readPendingOutbound(agentKey, conversationId),
    });
  }).pipe(agentOperation(agentKey), Effect.withSpan("recoverStoredState"));
}

/** Recent certified rows and their verification dependencies share the caller's snapshot. */
export function readRecentConversation(
  agentKey: string,
  input: {
    readonly conversationId: string;
    readonly maximumRecords: number;
    readonly candidateRecordHash?: string;
  },
) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    const rows = yield* sql<
      Readonly<Record<string, unknown>>
    >`${historySelect(sql)} WHERE certified.agent_key=${agentKey} AND certified.conversation_id=${input.conversationId} AND (${input.candidateRecordHash ?? ""}='' OR certified.ordinal < (SELECT ordinal FROM certified_records WHERE agent_key=${agentKey} AND conversation_id=${input.conversationId} AND record_hash=${input.candidateRecordHash ?? ""})) ORDER BY certified.ordinal DESC LIMIT ${input.maximumRecords + 1}`;
    const candidateRows =
      input.candidateRecordHash === undefined
        ? []
        : yield* sql<
            Readonly<Record<string, unknown>>
          >`${historySelect(sql)} WHERE certified.agent_key=${agentKey} AND certified.conversation_id=${input.conversationId} AND certified.record_hash=${input.candidateRecordHash}`;
    const records = yield* Effect.forEach(
      [...rows, ...candidateRows],
      (row) => readCertifiedRecord(agentKey, row),
      { concurrency: 1 },
    );
    return {
      records,
      memberships: yield* readMemberships(agentKey, input.conversationId),
      anchors: yield* readAnchors(
        agentKey,
        input.conversationId,
        records.map((record) => record.anchorHash),
      ),
      positions: [...rows, ...candidateRows].map((row) => ({
        recordHash: readText(row, "record_hash"),
        sequence: readInteger(row, "sequence"),
      })),
    };
  }).pipe(agentOperation(agentKey), Effect.withSpan("readRecentConversation"));
}

/** Reads remote certified records in their durable local commit order until consumed. */
export function readUnconsumedRecords(agentKey: string) {
  return Effect.gen(function* () {
    yield* checkPublishedRecords(agentKey);
    const sql = yield* SqlClient;
    const records = yield* Effect.forEach(
      yield* sql<
        Readonly<Record<string, unknown>>
      >`${historySelect(sql)} WHERE certified.agent_key=${agentKey} AND EXISTS(SELECT 1 FROM message_log WHERE message_log.agent_key=staged.agent_key AND message_log.author_agent_id=staged.author_agent_id AND message_log.msg_id=staged.msg_id AND message_log.consumed=0) AND staged.author_agent_id<>(SELECT agent_id FROM communication_metadata WHERE agent_key=${agentKey}) ORDER BY certified.sequence`,
      (row) => readCertifiedRecord(agentKey, row),
      { concurrency: 1 },
    );
    return {
      records,
      memberships: yield* readMemberships(agentKey),
      anchors: yield* readAnchors(agentKey),
    };
  }).pipe(agentOperation(agentKey), Effect.withSpan("readUnconsumedRecords"));
}

/** Every certified record must have its atomically published semantic view before it can be read or delivered. */
export function checkPublishedRecords(agentKey: string) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    const [missing] = yield* sql`SELECT 1 FROM certified_records AS certified
      JOIN staged_records AS staged USING(agent_key,conversation_id,record_hash)
      LEFT JOIN message_log AS message ON message.agent_key=staged.agent_key AND message.author_agent_id=staged.author_agent_id AND message.msg_id=staged.msg_id
      WHERE certified.agent_key=${agentKey} AND (message.sequence IS NULL OR message.conversation_id<>certified.conversation_id OR message.ordinal<>certified.ordinal) LIMIT 1`;
    if (missing !== undefined) {
      return yield* new EndpointStoreError({ reason: "corrupt" });
    }
  }).pipe(agentOperation(agentKey), Effect.withSpan("checkPublishedRecords"));
}

/** A successful handler retires only its exact certified record, retaining history. */
export function consumeRecord(agentKey: string, recordHash: string) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    const [row] = yield* sql<
      Readonly<Record<string, unknown>>
    >`SELECT author_agent_id,msg_id FROM staged_records WHERE agent_key=${agentKey} AND record_hash=${recordHash}`;
    if (row === undefined) {
      return yield* new EndpointStoreError({ reason: "not-found" });
    }
    yield* acknowledgeMessage(agentKey, {
      authorAgentId: yield* Schema.decodeUnknown(AgentId)(row.author_agent_id),
      msgId: yield* Schema.decodeUnknown(MessageReference.fields.msgId)(
        row.msg_id,
      ),
    });
  }).pipe(agentOperation(agentKey), Effect.withSpan("consumeRecord"));
}

/** Protocol startup needs each retained membership, including conversations without a certified head. */
export function readMemberships(
  agentKey: string,
  conversationId?: string,
): Effect.Effect<readonly StoredMembership[], EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    const conversation =
      conversationId === undefined
        ? database``
        : database`AND conversation_id=${conversationId}`;
    return Object.freeze(
      (yield* database<
        Readonly<Record<string, unknown>>
      >`SELECT conversation_id,membership_hash,canonical_membership FROM memberships WHERE agent_key=${agentKey} ${conversation} ORDER BY conversation_id`).map(
        readMembership,
      ),
    );
  }).pipe(agentOperation(agentKey), Effect.withSpan("readMemberships"));
}

/** Each conversation prepares its earliest accepted message before offering the next. */
export function readUnpreparedMessages(agentKey: string) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    return yield* Effect.forEach(
      yield* sql<
        Readonly<Record<string, unknown>>
      >`SELECT author_agent_id,msg_id,canonical_message,completed_record_hash,failure_reason, CASE WHEN EXISTS(SELECT 1 FROM message_log WHERE message_log.agent_key=message_intents.agent_key AND message_log.author_agent_id=message_intents.author_agent_id AND message_log.msg_id=message_intents.msg_id) THEN 1 ELSE 0 END AS completed FROM message_intents LEFT JOIN prepared_messages USING(agent_key,conversation_id,author_agent_id,msg_id) WHERE agent_key=${agentKey} AND sequence IN (SELECT MIN(sequence) FROM message_intents LEFT JOIN prepared_messages USING(agent_key,conversation_id,author_agent_id,msg_id) WHERE agent_key=${agentKey} AND canonical_intent IS NULL AND NOT EXISTS(SELECT 1 FROM message_log WHERE message_log.agent_key=message_intents.agent_key AND message_log.author_agent_id=message_intents.author_agent_id AND message_log.msg_id=message_intents.msg_id) AND failure_reason IS NULL GROUP BY conversation_id) ORDER BY sequence`,
      readAcceptedRow,
      { concurrency: 1 },
    );
  }).pipe(agentOperation(agentKey), Effect.withSpan("readUnpreparedMessages"));
}

/** A record and its exact signed dissemination envelope commit before a durability vote. */
export function stageRecordAndOutbound(
  agentKey: string,
  record: StagedRecord,
  outbound: OutboundMessageInput,
): Effect.Effect<StoredOutboundMessage, EndpointStoreError, SqlClient> {
  return transaction(
    agentKey,
    Effect.gen(function* () {
      validateStagedRecord(record);
      requireEqual(record.conversationId, outbound.conversationId);
      if (
        (yield* findStagedRecord(
          agentKey,
          record.conversationId,
          record.recordHash,
        )) === undefined
      ) {
        yield* requireNoReanchorAwayFrom(agentKey, record);
      }
      yield* stageRecordInTransaction(agentKey, record);
      const retained = yield* readRecordOutbound(
        agentKey,
        record.conversationId,
        record.recordHash,
      );
      return retained !== undefined && retained.disposition !== "discarded"
        ? retained.outbound
        : yield* enqueueOutboundInTransaction(agentKey, {
            ...outbound,
            recordHash: record.recordHash,
          });
    }),
  ).pipe(agentOperation(agentKey), Effect.withSpan("stageRecordAndOutbound"));
}

/**
 * Merges one stable evidence item without replacing a conflicting value.
 *
 * @param agentKey Immutable storage owner for this operation.
 * @param evidence Verified evidence with its canonical merge key.
 * @returns Whether the evidence was inserted or already durable.
 */
export function mergeEvidence(
  agentKey: string,
  evidence: ProtocolEvidence,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    validateEvidence(evidence);
    return yield* transaction(
      agentKey,
      Effect.gen(function* () {
        yield* requireEvidenceSubject(agentKey, evidence);
        const existing = yield* findEvidence(agentKey, evidence);
        if (existing !== undefined) {
          requireSameBytes(existing, evidence.canonicalEvidence);
          return "existing";
        }
        yield* requireLocalEvidenceLock(agentKey, evidence);
        return yield* mergeEvidenceInTransaction(agentKey, evidence);
      }),
    );
  }).pipe(agentOperation(agentKey), Effect.withSpan("mergeEvidence"));
}

/**
 * Replaces a competing lock and retains the verified `q(n)` action certificate
 * atomically. After restart that certificate prevents another local signature.
 * Refuses a staged competitor: its own certificate would prove conflicting
 * quorums beyond the fault bound.
 */
export function supersedeProposalLock(
  agentKey: string,
  lock: ProposalLock,
  certificate: readonly ProtocolEvidence[],
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    validateCertificateEvidence(certificate, {
      conversationId: lock.conversationId,
      kind: "action",
      subjectId: lock.actionHash,
    });
    return yield* transaction(
      agentKey,
      Effect.gen(function* () {
        const held = yield* findProposalLock(
          agentKey,
          lock.conversationId,
          lock.previousRecordHash ?? FIRST_MESSAGE_PREDECESSOR,
        );
        if (held !== undefined && held.actionHash !== lock.actionHash) {
          if (yield* hasStagedAction(agentKey, held)) {
            return yield* new EndpointStoreError({ reason: "conflict" });
          }
          yield* releaseProposalLock(agentKey, held);
        }
        const mutation = yield* lockProposalInTransaction(agentKey, lock);
        for (const evidence of certificate) {
          yield* mergeEvidenceInTransaction(agentKey, evidence);
        }
        return mutation;
      }),
    );
  }).pipe(agentOperation(agentKey), Effect.withSpan("supersedeProposalLock"));
}

/**
 * Atomically promotes a complete record, advances its head, and retains its
 * local message completion.
 *
 * @param agentKey Immutable storage owner for this operation.
 * @param record Verified complete certified record.
 * @returns Whether any durable state was inserted.
 */
export function promoteRecord(
  agentKey: string,
  record: CertifiedRecord,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    validateCertifiedRecord(record);
    return yield* transaction(
      agentKey,
      promoteRecordInTransaction(agentKey, record),
    );
  }).pipe(agentOperation(agentKey), Effect.withSpan("promoteRecord"));
}

/**
 * Stages and promotes a complete certified record atomically with its evidence
 * and local completion. Recovery must never mistake it for a locally staged
 * record needing a vote. Adoption casts no new vote, including when retaining
 * an earlier local signature, and may cross a pending re-anchor.
 */
export function applyCertifiedRecord(
  agentKey: string,
  record: CertifiedRecord,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    validateCertifiedRecord(record);
    return yield* transaction(
      agentKey,
      Effect.gen(function* () {
        const staged = yield* stageRecordInTransaction(agentKey, record);
        const promoted = yield* promoteRecordInTransaction(agentKey, record);
        return staged === "inserted" || promoted === "inserted"
          ? "inserted"
          : "existing";
      }),
    );
  }).pipe(agentOperation(agentKey), Effect.withSpan("applyCertifiedRecord"));
}

/**
 * Stages one complete outer envelope before it can reach Router transport.
 *
 * @param agentKey Immutable storage owner for this operation.
 * @param message Canonical complete signed envelope.
 * @returns The retained envelope under its envelope identity.
 */
export function enqueueOutbound(
  agentKey: string,
  message: OutboundMessageInput,
): Effect.Effect<StoredOutboundMessage, EndpointStoreError, SqlClient> {
  return transaction(
    agentKey,
    enqueueOutboundInTransaction(agentKey, message),
  ).pipe(storeOperation, Effect.withSpan("enqueueOutbound"));
}

/**
 * Selects the exact durable envelope and retry mode for one Router attempt.
 *
 * @param agentKey Immutable storage owner for this operation.
 * @param envelopeId Stable identity of the signed envelope.
 * @returns The current stored bytes, or an inactive terminal disposition.
 */
export function beginOutbound(
  agentKey: string,
  envelopeId: string,
): Effect.Effect<OutboundAttempt, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    requireText(envelopeId);
    return yield* transaction(
      agentKey,
      Effect.gen(function* () {
        const retained = yield* requireOutbound(agentKey, envelopeId);
        if (retained.disposition !== "pending") {
          return Object.freeze({ kind: "inactive" });
        }
        const mode = retained.attempted ? "retry" : "initial";
        if (!retained.attempted) {
          yield* database<
            Readonly<Record<string, unknown>>
          >`UPDATE outbound_messages SET attempted = 1
           WHERE agent_key=${agentKey} AND envelope_id = ${envelopeId} AND disposition = 'pending'`;
        }
        return Object.freeze({
          kind: "pending",
          mode,
          outbound: retained.outbound,
        });
      }),
    );
  }).pipe(agentOperation(agentKey), Effect.withSpan("beginOutbound"));
}

/**
 * Marks one exact stored envelope complete after Router accepts its bytes.
 *
 * @param agentKey Immutable storage owner for this operation.
 * @param outbound Exact envelope whose Router digest was verified.
 * @returns Whether this call recorded acceptance or observed it already.
 */
export function completeOutbound(
  agentKey: string,
  outbound: StoredOutboundMessage,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    validateMessageInput(outbound);
    return yield* transaction(
      agentKey,
      Effect.gen(function* () {
        const retained = yield* requireOutbound(agentKey, outbound.envelopeId);
        requireSameOutbound(retained.outbound, outbound);
        switch (retained.disposition) {
          case "accepted":
            return "existing";
          case "discarded":
            return yield* new EndpointStoreError({ reason: "conflict" });
          case "pending":
            yield* database<
              Readonly<Record<string, unknown>>
            >`UPDATE outbound_messages SET disposition = 'accepted'
             WHERE agent_key=${agentKey} AND envelope_id = ${outbound.envelopeId} AND disposition = 'pending'`;
            return "inserted";
          default: {
            const exhaustive: never = retained.disposition;
            return exhaustive;
          }
        }
      }),
    );
  }).pipe(agentOperation(agentKey), Effect.withSpan("completeOutbound"));
}

/**
 * Atomically invalidates exact pending envelopes before protocol regeneration.
 *
 * @param agentKey Immutable storage owner for this operation.
 * @param outbounds Complete expected envelopes to invalidate.
 * @returns Whether at least one pending envelope became inactive.
 */
export function discardOutbound(
  agentKey: string,
  outbounds: readonly StoredOutboundMessage[],
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    const distinctIds = new Set<string>();
    for (const outbound of outbounds) {
      validateMessageInput(outbound);
      if (distinctIds.has(outbound.envelopeId)) {
        return yield* new EndpointStoreError({ reason: "invalid-input" });
      }
      distinctIds.add(outbound.envelopeId);
    }
    return yield* transaction(
      agentKey,
      Effect.gen(function* () {
        const retained = [];
        for (const outbound of outbounds) {
          const row = yield* requireOutbound(agentKey, outbound.envelopeId);
          requireSameOutbound(row.outbound, outbound);
          if (row.disposition === "accepted") {
            return yield* new EndpointStoreError({ reason: "conflict" });
          }
          retained.push(row);
        }
        const inserted = retained.some(
          (outbound) => outbound.disposition === "pending",
        );
        for (const outbound of retained) {
          if (outbound.disposition === "pending") {
            yield* database<
              Readonly<Record<string, unknown>>
            >`UPDATE outbound_messages SET disposition = 'discarded'
             WHERE agent_key=${agentKey} AND envelope_id = ${outbound.outbound.envelopeId} AND disposition = 'pending'`;
          }
        }
        return inserted ? "inserted" : "existing";
      }),
    );
  }).pipe(agentOperation(agentKey), Effect.withSpan("discardOutbound"));
}

/** The outbox itself owns the exact signed envelope for one staged record. */
export function readRecordOutbound(
  agentKey: string,
  conversationId: string,
  recordHash: string,
) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    const [row] = yield* sql<
      Readonly<Record<string, unknown>>
    >`SELECT conversation_id,envelope_id,canonical_signed_message,wire_version,record_hash,attempted,disposition
       FROM outbound_messages WHERE agent_key=${agentKey} AND conversation_id=${conversationId} AND record_hash=${recordHash}
       ORDER BY disposition='discarded',outbound_sequence DESC LIMIT 1`;
    return row === undefined ? undefined : readOutbound(row);
  }).pipe(agentOperation(agentKey), Effect.withSpan("readRecordOutbound"));
}

/**
 * Inserts one first-candidate lock inside a caller-owned transaction, or
 * confirms the same lock is already durable.
 * @param agentKey Immutable storage owner for this operation.
 * @param proposal Verified gap-free action selected at its predecessor.
 * @returns Whether the lock was inserted or already durable.
 */
function lockProposalInTransaction(
  agentKey: string,
  proposal: ProposalLock,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    const predecessorKey =
      proposal.previousRecordHash ?? FIRST_MESSAGE_PREDECESSOR;
    const existing = yield* findProposalLock(
      agentKey,
      proposal.conversationId,
      predecessorKey,
    );
    if (existing !== undefined) {
      requireSameProposalLock(existing, proposal);
      return "existing";
    }
    const position = yield* readStoredPosition(
      agentKey,
      proposal.conversationId,
    );
    requireEqual(position.headRecordHash, proposal.previousRecordHash);
    yield* database<
      Readonly<Record<string, unknown>>
    >`INSERT INTO proposal_locks
        (agent_key,conversation_id, predecessor_key, previous_record_hash,
         action_hash, canonical_action_core)
       VALUES (${agentKey},${proposal.conversationId}, ${predecessorKey}, ${proposal.previousRecordHash ?? null}, ${proposal.actionHash}, ${copyBytes(proposal.canonicalActionCore)})`;
    return "inserted";
  }).pipe(
    agentOperation(agentKey),
    Effect.withSpan("lockProposalInTransaction"),
  );
}

/**
 * Projects one stable evidence value.
 *
 * @param row SQL result row.
 * @returns The detached protocol evidence.
 */
function readProtocolEvidence(
  row: Readonly<Record<string, unknown>>,
): ProtocolEvidence {
  return Object.freeze({
    conversationId: readText(row, "conversation_id"),
    kind: readEvidenceKind(row),
    subjectId: readText(row, "subject_id"),
    evidenceKey: readText(row, "evidence_key"),
    canonicalEvidence: readBytes(row, "canonical_evidence"),
  });
}

/**
 * Projects one staged or completed recoverable re-anchor.
 *
 * @param row SQL result row.
 * @returns The detached recoverable re-anchor.
 */
function readRecoveredReanchor(
  row: Readonly<Record<string, unknown>>,
): RecoveredReanchor {
  const canonicalCompletedReanchor = readOptionalBytes(
    row,
    "canonical_completed_reanchor",
  );
  return Object.freeze({
    ...readStagedReanchor(row),
    ...(canonicalCompletedReanchor === undefined
      ? {}
      : { canonicalCompletedReanchor }),
  });
}

function requireSameProposalLock(
  left: ProposalLock,
  right: ProposalLock,
): void {
  requireEqual(left.conversationId, right.conversationId);
  requireEqual(left.previousRecordHash, right.previousRecordHash);
  requireEqual(left.actionHash, right.actionHash);
  requireSameBytes(left.canonicalActionCore, right.canonicalActionCore);
}

function readEvidenceKind(
  row: Readonly<Record<string, unknown>>,
): ProtocolEvidence["kind"] {
  const kind = readText(row, "evidence_kind");
  if (
    kind !== "action" &&
    kind !== "durability" &&
    kind !== "catch-up" &&
    kind !== "reanchor"
  ) {
    throw new StoreSignal("corrupt");
  }
  return kind;
}

function validateMessageIntentBinding(binding: MessageIntentBinding): void {
  const intent = binding.intent;
  requireText(intent.conversationId);
  requireText(intent.membershipHash);
  requireText(intent.authorAgentId);
  requireText(intent.msgId);
  requireBytes(intent.canonicalIntent);
  if (intent.completedRecordHash !== undefined) {
    throw new StoreSignal("invalid-input");
  }
  if (binding.kind === "new-conversation") {
    validateFoundation(binding.foundation);
    requireEqual(binding.foundation.conversationId, intent.conversationId);
    requireEqual(binding.foundation.membershipHash, intent.membershipHash);
  }
}

function bindIntentWithFoundation(
  agentKey: string,
  binding: MessageIntentBinding,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const intent = binding.intent;
    const foundation =
      binding.kind === "new-conversation"
        ? yield* putConversationFoundationInTransaction(
            agentKey,
            binding.foundation,
          )
        : yield* requireConversationFoundation(agentKey, intent);
    const messageIntent = yield* bindMessageIntentInTransaction(
      agentKey,
      intent,
    );
    return foundation === "inserted" || messageIntent === "inserted"
      ? "inserted"
      : "existing";
  }).pipe(agentOperation(agentKey));
}

function validateProposalLock(proposal: ProposalLock): void {
  requireText(proposal.conversationId);
  if (proposal.previousRecordHash !== undefined) {
    requireText(proposal.previousRecordHash);
  }
  requireText(proposal.actionHash);
  requireBytes(proposal.canonicalActionCore);
}

function bindMessageIntentInTransaction(
  agentKey: string,
  intent: MessageIntent,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    const identity = yield* readStoredIdentity(agentKey);
    if (identity === undefined) {
      return yield* new EndpointStoreError({ reason: "not-found" });
    }
    requireEqual(identity.agentId, intent.authorAgentId);
    const existing = (yield* database<
      Readonly<Record<string, unknown>>
    >`SELECT conversation_id, membership_hash, canonical_intent, canonical_message
       FROM message_intents LEFT JOIN prepared_messages USING(agent_key,conversation_id,author_agent_id,msg_id) WHERE agent_key=${agentKey} AND author_agent_id = ${intent.authorAgentId} AND msg_id = ${intent.msgId}`)[0];
    if (existing === undefined) {
      return yield* new EndpointStoreError({ reason: "not-found" });
    }
    requireEqual(readText(existing, "conversation_id"), intent.conversationId);
    requireSameBytes(
      readBytes(existing, "canonical_message"),
      intent.canonicalMessage,
    );
    if (readOptionalText(existing, "membership_hash") !== undefined) {
      requireEqual(
        readText(existing, "membership_hash"),
        intent.membershipHash,
      );
      requireSameBytes(
        readBytes(existing, "canonical_intent"),
        intent.canonicalIntent,
      );
      return "existing";
    }
    yield* database`INSERT INTO prepared_messages(agent_key,conversation_id,author_agent_id,msg_id,membership_hash,canonical_intent) VALUES(${agentKey},${intent.conversationId},${intent.authorAgentId},${intent.msgId},${intent.membershipHash},${intent.canonicalIntent})`;
    return "inserted";
  }).pipe(agentOperation(agentKey));
}

/**
 * Inserts or validates one complete membership and initial anchor in an
 * existing transaction.
 *
 * @param agentKey Immutable storage owner for this operation.
 * @param foundation Verified immutable conversation foundation.
 * @returns Whether this transaction inserted the foundation.
 */
function putConversationFoundationInTransaction(
  agentKey: string,
  foundation: ConversationFoundation,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    validateFoundation(foundation);
    const existing = yield* readFoundation(agentKey, foundation.conversationId);
    if (existing !== undefined) {
      requireSameFoundation(existing, foundation);
      return "existing";
    }
    yield* insertFoundation(agentKey, foundation);
    return "inserted";
  }).pipe(agentOperation(agentKey));
}

function requireConversationFoundation(
  agentKey: string,
  intent: MessageIntent,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const foundation = yield* readFoundation(agentKey, intent.conversationId);
    if (foundation === undefined) {
      return yield* new EndpointStoreError({ reason: "not-found" });
    }
    requireEqual(foundation.membershipHash, intent.membershipHash);
    return yield* Effect.succeed<StoreMutation>("existing");
  }).pipe(agentOperation(agentKey));
}

function requireEmptyConversationPosition(
  agentKey: string,
  expected: ConversationFoundation,
): Effect.Effect<void, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    const retained = yield* readFoundation(agentKey, expected.conversationId);
    if (retained === undefined) {
      return yield* new EndpointStoreError({ reason: "not-found" });
    }
    requireSameFoundation(retained, expected);
    const position = yield* readStoredPosition(
      agentKey,
      expected.conversationId,
    );
    requireEqual(position.membershipHash, expected.membershipHash);
    requireEqual(position.currentAnchorHash, expected.anchorHash);
    requireEqual(position.headRecordHash, undefined);
    requireEqual(position.headOrdinal, -1);
    if (
      (yield* database<
        Readonly<Record<string, unknown>>
      >`SELECT 1 AS retained FROM certified_records
         WHERE agent_key=${agentKey} AND conversation_id = ${expected.conversationId} LIMIT 1`)[0] !==
      undefined
    ) {
      return yield* new EndpointStoreError({ reason: "conflict" });
    }
  }).pipe(agentOperation(agentKey));
}

function readFoundation(
  agentKey: string,
  conversationId: string,
): Effect.Effect<
  ConversationFoundation | undefined,
  EndpointStoreError,
  SqlClient
> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    const row = (yield* database<
      Readonly<Record<string, unknown>>
    >`SELECT membership.membership_hash,membership.canonical_membership,anchor.anchor_hash,anchor.canonical_anchor FROM memberships AS membership JOIN anchors AS anchor ON anchor.agent_key=membership.agent_key AND anchor.conversation_id=membership.conversation_id AND anchor.previous_anchor_hash IS NULL WHERE membership.agent_key=${agentKey} AND membership.conversation_id=${conversationId}`)[0];
    return row === undefined
      ? undefined
      : Object.freeze({
          conversationId,
          membershipHash: readText(row, "membership_hash"),
          canonicalMembership: readBytes(row, "canonical_membership"),
          anchorHash: readText(row, "anchor_hash"),
          canonicalAnchor: readBytes(row, "canonical_anchor"),
        });
  }).pipe(agentOperation(agentKey));
}

function insertFoundation(
  agentKey: string,
  foundation: ConversationFoundation,
): Effect.Effect<void, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    const descriptor = yield* decodeCanonical(
      MembershipDescriptor,
      foundation.canonicalMembership,
    );
    const members = yield* Effect.forEach(
      descriptor.members,
      (card) =>
        Schema.decodeUnknown(AgentIdentityCard)(card).pipe(
          Effect.map((card) => ({
            agentId: card.agentId,
            agentName: card.agentName,
            principalId: card.principalId,
          })),
        ),
      { concurrency: 1 },
    );
    const local = yield* readStoredIdentity(agentKey);
    if (local === undefined) {
      return yield* new EndpointStoreError({ reason: "not-found" });
    }
    yield* database<Readonly<Record<string, unknown>>>`INSERT INTO memberships
        (agent_key,conversation_id, membership_hash, canonical_membership, sort_key)
       VALUES (${agentKey},${foundation.conversationId}, ${foundation.membershipHash}, ${copyBytes(foundation.canonicalMembership)}, ${conversationSortKey(members, local.agentId)})`;
    yield* database<Readonly<Record<string, unknown>>>`INSERT INTO anchors
        (agent_key,conversation_id, anchor_hash, canonical_anchor) VALUES (${agentKey},${foundation.conversationId}, ${foundation.anchorHash}, ${copyBytes(foundation.canonicalAnchor)})`;
    yield* database<
      Readonly<Record<string, unknown>>
    >`INSERT INTO conversation_state
        (agent_key,conversation_id, membership_hash, current_anchor_hash, head_ordinal)
       VALUES (${agentKey},${foundation.conversationId}, ${foundation.membershipHash}, ${foundation.anchorHash}, -1)`;
  }).pipe(agentOperation(agentKey));
}

/**
 * Local voting cannot leave a head with a staged successor. Adopting a
 * completed member anchor casts no local vote and may do so.
 */
function stageReanchorInTransaction(
  agentKey: string,
  reanchor: StagedReanchor,
  purpose: "vote" | "adopted",
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const existing = yield* findStagedReanchor(
      agentKey,
      reanchor.conversationId,
      reanchor.anchorHash,
    );
    if (existing !== undefined) {
      requireSameReanchor(existing, reanchor);
      return "existing";
    }
    yield* requireUnclaimedReanchorScope(agentKey, reanchor);
    yield* requireReanchorPosition(agentKey, reanchor);
    if (purpose === "vote") {
      yield* requireNoStagedSuccessor(agentKey, reanchor);
    }
    yield* insertStagedReanchor(agentKey, reanchor);
    return "inserted";
  }).pipe(agentOperation(agentKey));
}

function completeReanchorInTransaction(
  agentKey: string,
  reanchor: CompletedReanchor,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const staged = yield* findStagedReanchor(
      agentKey,
      reanchor.conversationId,
      reanchor.anchorHash,
    );
    if (staged === undefined) {
      return yield* new EndpointStoreError({ reason: "not-found" });
    }
    requireSameReanchor(staged, reanchor);
    if (
      yield* hasAnchor(agentKey, reanchor.conversationId, reanchor.anchorHash)
    ) {
      return "existing";
    }
    yield* requireReanchorPosition(agentKey, reanchor);
    yield* insertCompletedReanchor(agentKey, reanchor);
    yield* releaseSupersededProposal(agentKey, reanchor);
    return "inserted";
  }).pipe(agentOperation(agentKey));
}

/**
 * Requires two re-anchors with one hash to have identical body bindings.
 *
 * @param left Previously retained re-anchor.
 * @param right Candidate re-anchor with the same private hash.
 */
function requireSameReanchor(
  left: StagedReanchor,
  right: StagedReanchor,
): void {
  requireEqual(left.conversationId, right.conversationId);
  requireEqual(left.anchorHash, right.anchorHash);
  requireEqual(left.previousAnchorHash, right.previousAnchorHash);
  requireEqual(left.routerInstanceId, right.routerInstanceId);
  requireEqual(left.selectedRecordHash, right.selectedRecordHash);
  requireSameBytes(left.canonicalBody, right.canonicalBody);
}

/**
 * Retires the superseded proposal and staged successor when re-anchoring.
 * Actions under the previous anchor cannot certify, and their lock would
 * block proposals under the new anchor. A holder never votes to re-anchor behind its staged successor, so
 * the completed anchor's quorum leaves too few members to certify it.
 */
function releaseSupersededProposal(
  agentKey: string,
  reanchor: CompletedReanchor,
): Effect.Effect<void, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const lock = yield* findProposalLock(
      agentKey,
      reanchor.conversationId,
      reanchor.selectedRecordHash,
    );
    if (lock !== undefined) {
      yield* releaseProposalLock(agentKey, lock);
    }
  }).pipe(agentOperation(agentKey));
}

/**
 * Removes the lock, evidence, staged records and outbox associations in the
 * caller's transaction. The caller must establish that the action cannot
 * certify because orphaned rows would fail startup verification.
 */
function releaseProposalLock(
  agentKey: string,
  lock: ProposalLock,
): Effect.Effect<void, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    const staged = database`SELECT record_hash FROM staged_records
      WHERE agent_key=${agentKey} AND conversation_id=${lock.conversationId} AND action_hash=${lock.actionHash}`;
    for (const statement of [
      database`UPDATE outbound_messages SET record_hash=NULL,
        disposition=CASE WHEN disposition='pending' THEN 'discarded' ELSE disposition END
        WHERE agent_key=${agentKey} AND conversation_id=${lock.conversationId} AND record_hash IN (${staged})`,
      database`DELETE FROM protocol_evidence WHERE agent_key=${agentKey} AND conversation_id=${lock.conversationId}
        AND evidence_kind='durability' AND subject_id IN (${staged})`,
      database`DELETE FROM staged_records WHERE agent_key=${agentKey} AND conversation_id=${lock.conversationId} AND action_hash=${lock.actionHash}`,
      database`DELETE FROM protocol_evidence WHERE agent_key=${agentKey} AND conversation_id=${lock.conversationId}
        AND evidence_kind='action' AND subject_id=${lock.actionHash}`,
      database`DELETE FROM proposal_locks WHERE agent_key=${agentKey} AND conversation_id=${lock.conversationId} AND action_hash=${lock.actionHash}`,
    ]) {
      yield* statement;
    }
  }).pipe(agentOperation(agentKey), Effect.withSpan("releaseProposalLock"));
}

/**
 * Delete this endpoint's uncompleted candidates in a completed re-anchor's
 * scope that the completion does not select, with their re-anchor votes.
 * @param agentKey Immutable storage owner for this operation.
 * @param reanchor Verified completed re-anchor that supersedes them.
 */
function retireSupersededCandidates(
  agentKey: string,
  reanchor: CompletedReanchor,
): Effect.Effect<void, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    const superseded = (yield* database<
      Readonly<Record<string, unknown>>
    >`SELECT anchor_hash FROM reanchors WHERE agent_key=${agentKey} AND conversation_id=${reanchor.conversationId} AND previous_anchor_hash=${reanchor.previousAnchorHash} AND router_instance_id=${reanchor.routerInstanceId} AND anchor_hash<>${reanchor.anchorHash} AND canonical_completed_reanchor IS NULL`).map(
      (row) => readText(row, "anchor_hash"),
    );
    for (const anchorHash of superseded) {
      yield* database<
        Readonly<Record<string, unknown>>
      >`DELETE FROM protocol_evidence
         WHERE agent_key=${agentKey} AND conversation_id = ${reanchor.conversationId} AND evidence_kind = 'reanchor'
           AND subject_id = ${anchorHash}`;
    }
    yield* database<
      Readonly<Record<string, unknown>>
    >`DELETE FROM reanchors WHERE agent_key=${agentKey} AND conversation_id=${reanchor.conversationId} AND previous_anchor_hash=${reanchor.previousAnchorHash} AND router_instance_id=${reanchor.routerInstanceId} AND anchor_hash<>${reanchor.anchorHash} AND canonical_completed_reanchor IS NULL`;
  }).pipe(agentOperation(agentKey));
}

function requireUnclaimedReanchorScope(
  agentKey: string,
  reanchor: StagedReanchor,
): Effect.Effect<void, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    const scoped = (yield* database<
      Readonly<Record<string, unknown>>
    >`SELECT anchor_hash FROM reanchors
       WHERE agent_key=${agentKey} AND conversation_id = ${reanchor.conversationId} AND previous_anchor_hash = ${reanchor.previousAnchorHash}
         AND router_instance_id = ${reanchor.routerInstanceId}`)[0];
    if (scoped !== undefined) {
      return yield* new EndpointStoreError({ reason: "conflict" });
    }
  }).pipe(agentOperation(agentKey));
}

/**
 * A member that staged a successor cannot vote to re-anchor behind it under
 * the same anchor.
 */
function requireNoStagedSuccessor(
  agentKey: string,
  reanchor: StagedReanchor,
): Effect.Effect<void, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    const successor = (yield* database<
      Readonly<Record<string, unknown>>
    >`SELECT 1 AS staged FROM staged_records
       WHERE agent_key=${agentKey} AND conversation_id = ${reanchor.conversationId} AND anchor_hash = ${reanchor.previousAnchorHash}
         AND previous_record_hash = ${reanchor.selectedRecordHash} LIMIT 1`)[0];
    if (successor !== undefined) {
      return yield* new EndpointStoreError({ reason: "conflict" });
    }
  }).pipe(agentOperation(agentKey));
}

function requireReanchorPosition(
  agentKey: string,
  reanchor: StagedReanchor,
): Effect.Effect<void, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const position = yield* readStoredPosition(
      agentKey,
      reanchor.conversationId,
    );
    requireEqual(position.currentAnchorHash, reanchor.previousAnchorHash);
    requireEqual(position.headRecordHash, reanchor.selectedRecordHash);
  }).pipe(agentOperation(agentKey));
}

function insertStagedReanchor(
  agentKey: string,
  reanchor: StagedReanchor,
): Effect.Effect<void, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    yield* database<Readonly<Record<string, unknown>>>`INSERT INTO reanchors
        (agent_key,conversation_id, anchor_hash, previous_anchor_hash,
         router_instance_id, selected_record_hash, canonical_body)
       VALUES (${agentKey},${reanchor.conversationId}, ${reanchor.anchorHash}, ${reanchor.previousAnchorHash}, ${reanchor.routerInstanceId}, ${reanchor.selectedRecordHash}, ${copyBytes(reanchor.canonicalBody)})`;
  }).pipe(agentOperation(agentKey));
}

function insertCompletedReanchor(
  agentKey: string,
  reanchor: CompletedReanchor,
): Effect.Effect<void, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    yield* database<Readonly<Record<string, unknown>>>`INSERT INTO anchors
        (agent_key,conversation_id, anchor_hash, previous_anchor_hash,
         selected_record_hash, canonical_anchor)
       VALUES (${agentKey},${reanchor.conversationId}, ${reanchor.anchorHash}, ${reanchor.previousAnchorHash}, ${reanchor.selectedRecordHash}, ${copyBytes(reanchor.canonicalCompletedReanchor)})`;
    yield* database<
      Readonly<Record<string, unknown>>
    >`UPDATE reanchors SET canonical_completed_reanchor = ${copyBytes(reanchor.canonicalCompletedReanchor)}
       WHERE agent_key=${agentKey} AND conversation_id = ${reanchor.conversationId} AND anchor_hash = ${reanchor.anchorHash}`;
    yield* database<
      Readonly<Record<string, unknown>>
    >`UPDATE conversation_state SET current_anchor_hash = ${reanchor.anchorHash}
       WHERE agent_key=${agentKey} AND conversation_id = ${reanchor.conversationId}`;
  }).pipe(agentOperation(agentKey));
}

function hasAnchor(
  agentKey: string,
  conversationId: string,
  anchorHash: string,
): Effect.Effect<boolean, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    return (
      (yield* database<
        Readonly<Record<string, unknown>>
      >`SELECT 1 AS retained FROM anchors
         WHERE agent_key=${agentKey} AND conversation_id = ${conversationId} AND anchor_hash = ${anchorHash}`)[0] !==
      undefined
    );
  }).pipe(agentOperation(agentKey));
}

function validateEmptyConversationRestart(
  restart: EmptyConversationRestart,
): void {
  validateFoundation(restart.expectedFoundation);
  validateFoundation(restart.replacementFoundation);
  requireEqual(
    restart.expectedFoundation.conversationId,
    restart.replacementFoundation.conversationId,
  );
  requireEqual(
    restart.expectedFoundation.membershipHash,
    restart.replacementFoundation.membershipHash,
  );
  requireSameBytes(
    restart.expectedFoundation.canonicalMembership,
    restart.replacementFoundation.canonicalMembership,
  );
  if (
    restart.expectedFoundation.anchorHash ===
    restart.replacementFoundation.anchorHash
  ) {
    throw new StoreSignal("invalid-input");
  }
}

function validateFoundation(foundation: ConversationFoundation): void {
  requireText(foundation.conversationId);
  requireText(foundation.membershipHash);
  requireBytes(foundation.canonicalMembership);
  requireText(foundation.anchorHash);
  requireBytes(foundation.canonicalAnchor);
}

function clearIncompleteConversationState(
  agentKey: string,
  expected: ConversationFoundation,
): Effect.Effect<void, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    const conversationId = expected.conversationId;
    yield* database<
      Readonly<Record<string, unknown>>
    >`UPDATE outbound_messages SET record_hash=NULL, disposition=CASE WHEN disposition='pending' THEN 'discarded' ELSE disposition END WHERE agent_key=${agentKey} AND conversation_id=${conversationId}`;
    yield* database<
      Readonly<Record<string, unknown>>
    >`DELETE FROM protocol_evidence WHERE agent_key=${agentKey} AND conversation_id = ${conversationId}`;
    yield* database<
      Readonly<Record<string, unknown>>
    >`DELETE FROM proposal_locks WHERE agent_key=${agentKey} AND conversation_id = ${conversationId}`;
    yield* database<
      Readonly<Record<string, unknown>>
    >`DELETE FROM staged_records WHERE agent_key=${agentKey} AND conversation_id = ${conversationId}`;
    yield* database<
      Readonly<Record<string, unknown>>
    >`DELETE FROM reanchors WHERE agent_key=${agentKey} AND conversation_id = ${conversationId}`;
    yield* database<
      Readonly<Record<string, unknown>>
    >`UPDATE outbound_messages SET disposition = 'discarded'
       WHERE agent_key=${agentKey} AND conversation_id = ${conversationId} AND disposition = 'pending'`;
  }).pipe(agentOperation(agentKey));
}

function installReplacementFoundation(
  agentKey: string,
  replacement: ConversationFoundation,
): Effect.Effect<void, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    const retainedReplacement = (yield* database<
      Readonly<Record<string, unknown>>
    >`SELECT 1 AS retained FROM anchors
       WHERE agent_key=${agentKey} AND conversation_id = ${replacement.conversationId} AND anchor_hash = ${replacement.anchorHash}`)[0];
    if (retainedReplacement !== undefined) {
      return yield* new EndpointStoreError({ reason: "conflict" });
    }
    yield* database<Readonly<Record<string, unknown>>>`INSERT INTO anchors
        (agent_key,conversation_id, anchor_hash, canonical_anchor) VALUES (${agentKey},${replacement.conversationId}, ${replacement.anchorHash}, ${copyBytes(replacement.canonicalAnchor)})`;
    yield* database<Readonly<Record<string, unknown>>>`UPDATE conversation_state
       SET current_anchor_hash = ${replacement.anchorHash}, head_record_hash = NULL, head_ordinal = -1
       WHERE agent_key=${agentKey} AND conversation_id = ${replacement.conversationId}`;
    yield* database<Readonly<Record<string, unknown>>>`DELETE FROM anchors
       WHERE agent_key=${agentKey} AND conversation_id = ${replacement.conversationId} AND anchor_hash <> ${replacement.anchorHash}`;
  }).pipe(agentOperation(agentKey));
}

function readConversationMessageIntents(
  agentKey: string,
  conversationId: string,
): Effect.Effect<readonly MessageIntent[], EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    return Object.freeze(
      (yield* database<
        Readonly<Record<string, unknown>>
      >`SELECT conversation_id, membership_hash, author_agent_id, msg_id,
                canonical_intent, canonical_message, completed_record_hash
         FROM message_intents LEFT JOIN prepared_messages USING(agent_key,conversation_id,author_agent_id,msg_id) WHERE agent_key=${agentKey} AND conversation_id = ${conversationId} AND canonical_intent IS NOT NULL AND failure_reason IS NULL
         ORDER BY sequence`).map((row) => {
        const completedRecordHash = readOptionalText(
          row,
          "completed_record_hash",
        );
        return Object.freeze({
          conversationId: readText(row, "conversation_id"),
          membershipHash: readText(row, "membership_hash"),
          authorAgentId: readText(row, "author_agent_id"),
          msgId: readText(row, "msg_id"),
          canonicalIntent: readBytes(row, "canonical_intent"),
          canonicalMessage: readBytes(row, "canonical_message"),
          ...(completedRecordHash === undefined ? {} : { completedRecordHash }),
        });
      }),
    );
  }).pipe(agentOperation(agentKey));
}

function copyFoundation(
  foundation: ConversationFoundation,
): ConversationFoundation {
  return Object.freeze({
    ...foundation,
    canonicalMembership: copyBytes(foundation.canonicalMembership),
    canonicalAnchor: copyBytes(foundation.canonicalAnchor),
  });
}

function validateCompletedReanchor(reanchor: CompletedReanchor): void {
  validateStagedReanchor(reanchor);
  requireBytes(reanchor.canonicalCompletedReanchor);
}

function validateStagedReanchor(reanchor: StagedReanchor): void {
  requireText(reanchor.conversationId);
  requireText(reanchor.anchorHash);
  requireText(reanchor.previousAnchorHash);
  requireText(reanchor.routerInstanceId);
  requireText(reanchor.selectedRecordHash);
  requireBytes(reanchor.canonicalBody);
}

function requireSameFoundation(
  left: ConversationFoundation,
  right: ConversationFoundation,
): void {
  requireEqual(left.conversationId, right.conversationId);
  requireEqual(left.membershipHash, right.membershipHash);
  requireSameBytes(left.canonicalMembership, right.canonicalMembership);
  requireEqual(left.anchorHash, right.anchorHash);
  requireSameBytes(left.canonicalAnchor, right.canonicalAnchor);
}

function readPositions(agentKey: string, conversationId?: string) {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    const conversation =
      conversationId === undefined
        ? database``
        : database`AND conversation_id=${conversationId}`;
    return Object.freeze(
      (yield* database<
        Readonly<Record<string, unknown>>
      >`SELECT conversation_id,membership_hash,current_anchor_hash,head_record_hash FROM conversation_state WHERE agent_key=${agentKey} ${conversation} ORDER BY conversation_id`).map(
        readConversationPosition,
      ),
    );
  }).pipe(agentOperation(agentKey));
}

function readMessageIntents(
  agentKey: string,
  conversationId?: string,
): Effect.Effect<readonly MessageIntent[], EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    const conversation =
      conversationId === undefined
        ? database``
        : database`AND conversation_id=${conversationId}`;
    return Object.freeze(
      (yield* database<
        Readonly<Record<string, unknown>>
      >`SELECT conversation_id,membership_hash,author_agent_id,msg_id,canonical_intent,canonical_message,completed_record_hash FROM message_intents LEFT JOIN prepared_messages USING(agent_key,conversation_id,author_agent_id,msg_id) WHERE agent_key=${agentKey} AND canonical_intent IS NOT NULL AND failure_reason IS NULL ${conversation} ORDER BY sequence`).map(
        (row) => {
          const completedRecordHash = readOptionalText(
            row,
            "completed_record_hash",
          );
          return Object.freeze({
            conversationId: readText(row, "conversation_id"),
            membershipHash: readText(row, "membership_hash"),
            authorAgentId: readText(row, "author_agent_id"),
            msgId: readText(row, "msg_id"),
            canonicalIntent: readBytes(row, "canonical_intent"),
            canonicalMessage: readBytes(row, "canonical_message"),
            ...(completedRecordHash === undefined
              ? {}
              : { completedRecordHash }),
          });
        },
      ),
    );
  }).pipe(agentOperation(agentKey));
}

function readMembership(
  row: Readonly<Record<string, unknown>>,
): StoredMembership {
  return Object.freeze({
    conversationId: readText(row, "conversation_id"),
    membershipHash: readText(row, "membership_hash"),
    canonicalMembership: readBytes(row, "canonical_membership"),
  });
}

function readAnchors(
  agentKey: string,
  conversationId?: string,
  anchorHashes?: readonly string[],
): Effect.Effect<readonly StoredAnchor[], EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    if (anchorHashes?.length === 0) {
      return Object.freeze([]);
    }
    const database = yield* SqlClient;
    const conversation =
      conversationId === undefined
        ? database``
        : database`AND conversation_id=${conversationId}`;
    const anchors =
      anchorHashes === undefined
        ? database``
        : database`AND anchor_hash IN ${database.in(anchorHashes)}`;
    return Object.freeze(
      (yield* database<
        Readonly<Record<string, unknown>>
      >`SELECT conversation_id,anchor_hash,previous_anchor_hash,selected_record_hash,canonical_anchor FROM anchors WHERE agent_key=${agentKey} ${conversation} ${anchors} ORDER BY conversation_id,sequence`).map(
        (row) => {
          const previousAnchorHash = readOptionalText(
            row,
            "previous_anchor_hash",
          );
          const selectedRecordHash = readOptionalText(
            row,
            "selected_record_hash",
          );
          return Object.freeze({
            conversationId: readText(row, "conversation_id"),
            anchorHash: readText(row, "anchor_hash"),
            ...(previousAnchorHash === undefined ? {} : { previousAnchorHash }),
            ...(selectedRecordHash === undefined ? {} : { selectedRecordHash }),
            canonicalAnchor: readBytes(row, "canonical_anchor"),
          });
        },
      ),
    );
  }).pipe(agentOperation(agentKey));
}

function readEvidence(agentKey: string, conversationId?: string) {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    const conversation =
      conversationId === undefined
        ? database``
        : database`AND conversation_id=${conversationId}`;
    return Object.freeze(
      (yield* database<
        Readonly<Record<string, unknown>>
      >`SELECT conversation_id,evidence_kind,subject_id,evidence_key,canonical_evidence FROM protocol_evidence WHERE agent_key=${agentKey} ${conversation} ORDER BY conversation_id,evidence_kind,subject_id,evidence_key`).map(
        readProtocolEvidence,
      ),
    );
  }).pipe(agentOperation(agentKey));
}

function readAllCertifiedRecords(
  agentKey: string,
  conversationId?: string,
): Effect.Effect<readonly CertifiedRecord[], EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    const conversation =
      conversationId === undefined
        ? database``
        : database`AND certified.conversation_id=${conversationId}`;
    return Object.freeze(
      yield* Effect.forEach(
        yield* database<
          Readonly<Record<string, unknown>>
        >`${historySelect(database)} WHERE certified.agent_key=${agentKey} ${conversation} ORDER BY certified.conversation_id,certified.ordinal`,
        (row) => readCertifiedRecord(agentKey, row),
        { concurrency: 1 },
      ),
    );
  }).pipe(agentOperation(agentKey));
}

/**
 * Projects one complete certified record and its separate evidence maps.
 *
 * @param agentKey Immutable storage owner for this operation.
 * @param row SQL result row for the certified record core.
 * @returns The detached complete certified record.
 */
function readCertifiedRecord(
  agentKey: string,
  row: Readonly<Record<string, unknown>>,
): Effect.Effect<CertifiedRecord, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const staged = readStagedRecord(row);
    const actionEvidence = yield* readEvidenceForSubject(
      agentKey,
      staged.conversationId,
      "action",
      staged.actionHash,
    );
    const durabilityEvidence = yield* readEvidenceForSubject(
      agentKey,
      staged.conversationId,
      "durability",
      staged.recordHash,
    );
    if (actionEvidence.length === 0 || durabilityEvidence.length === 0) {
      return yield* new EndpointStoreError({ reason: "corrupt" });
    }
    return Object.freeze({
      ...staged,
      actionEvidence,
      durabilityEvidence,
    });
  }).pipe(agentOperation(agentKey), Effect.withSpan("readCertifiedRecord"));
}

/** Returns detached signer evidence in stored signer-key order. */
function readEvidenceForSubject(
  agentKey: string,
  conversationId: string,
  kind: ProtocolEvidence["kind"],
  subjectId: string,
): Effect.Effect<readonly ProtocolEvidence[], EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    const collation = database.onDialectOrElse({
      pg: () => database`COLLATE "C"`,
      orElse: () => database`COLLATE BINARY`,
    });
    return Object.freeze(
      (yield* database<
        Readonly<Record<string, unknown>>
      >`SELECT conversation_id, evidence_kind, subject_id,
                evidence_key, canonical_evidence
         FROM protocol_evidence
         WHERE agent_key=${agentKey} AND conversation_id = ${conversationId} AND evidence_kind = ${kind} AND subject_id = ${subjectId}
         ORDER BY evidence_key ${collation}`).map(readProtocolEvidence),
    );
  }).pipe(agentOperation(agentKey));
}

function readReanchors(agentKey: string, conversationId?: string) {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    const conversation =
      conversationId === undefined
        ? database``
        : database`AND conversation_id=${conversationId}`;
    return Object.freeze(
      (yield* database<
        Readonly<Record<string, unknown>>
      >`SELECT conversation_id,anchor_hash,previous_anchor_hash,router_instance_id,selected_record_hash,canonical_body,canonical_completed_reanchor FROM reanchors WHERE agent_key=${agentKey} ${conversation} ORDER BY conversation_id,sequence`).map(
        readRecoveredReanchor,
      ),
    );
  }).pipe(agentOperation(agentKey));
}

/**
 * Retain one verified record core without authorizing a local signature.
 * @param agentKey Immutable storage owner for this operation.
 * @param record Verified record core and private bindings.
 * @returns Whether the record was inserted or already staged.
 */
function stageRecordInTransaction(
  agentKey: string,
  record: StagedRecord,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    const existing = yield* findStagedRecord(
      agentKey,
      record.conversationId,
      record.recordHash,
    );
    if (existing !== undefined) {
      requireSameRecord(existing, record);
      return "existing";
    }
    yield* requireRecordPosition(agentKey, record);
    yield* database<
      Readonly<Record<string, unknown>>
    >`INSERT INTO staged_records
        (agent_key,conversation_id, record_hash, previous_record_hash,
         membership_hash, anchor_hash, action_hash, author_agent_id, msg_id,
         canonical_record_core)
       VALUES (${agentKey},${record.conversationId}, ${record.recordHash}, ${record.previousRecordHash ?? null}, ${record.membershipHash}, ${record.anchorHash}, ${record.actionHash}, ${record.authorAgentId}, ${record.msgId}, ${copyBytes(record.canonicalRecordCore)})`;
    return "inserted";
  }).pipe(agentOperation(agentKey));
}

function promoteRecordInTransaction(
  agentKey: string,
  record: CertifiedRecord,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const staged = yield* findStagedRecord(
      agentKey,
      record.conversationId,
      record.recordHash,
    );
    if (staged === undefined) {
      return yield* new EndpointStoreError({ reason: "not-found" });
    }
    requireSameRecord(staged, record);
    const evidenceMutation = yield* mergeCertifiedEvidence(agentKey, record);
    if (
      yield* hasCertifiedRecord(
        agentKey,
        record.conversationId,
        record.recordHash,
      )
    ) {
      return evidenceMutation;
    }
    const position = yield* readStoredPosition(agentKey, record.conversationId);
    requireEqual(position.currentAnchorHash, record.anchorHash);
    requireEqual(position.headRecordHash, record.previousRecordHash);
    yield* insertCertifiedRecord(agentKey, record, position.headOrdinal + 1);
    yield* completeLocalMessageIntent(agentKey, record);
    return "inserted";
  }).pipe(agentOperation(agentKey));
}

/**
 * Requires two records with one stable hash to have identical bindings.
 *
 * @param left Previously retained staged record.
 * @param right Candidate record with the same private hash.
 */
function requireSameRecord(left: StagedRecord, right: StagedRecord): void {
  requireEqual(left.conversationId, right.conversationId);
  requireEqual(left.recordHash, right.recordHash);
  requireEqual(left.previousRecordHash, right.previousRecordHash);
  requireEqual(left.membershipHash, right.membershipHash);
  requireEqual(left.anchorHash, right.anchorHash);
  requireEqual(left.actionHash, right.actionHash);
  requireEqual(left.authorAgentId, right.authorAgentId);
  requireEqual(left.msgId, right.msgId);
  requireSameBytes(left.canonicalRecordCore, right.canonicalRecordCore);
}

function mergeCertifiedEvidence(
  agentKey: string,
  record: CertifiedRecord,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    let mutation: StoreMutation = "existing";
    for (const evidence of [
      ...record.actionEvidence,
      ...record.durabilityEvidence,
    ]) {
      if (
        (yield* mergeEvidenceInTransaction(agentKey, evidence)) === "inserted"
      ) {
        mutation = "inserted";
      }
    }
    return mutation;
  }).pipe(agentOperation(agentKey));
}

function mergeEvidenceInTransaction(
  agentKey: string,
  evidence: ProtocolEvidence,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    yield* requireEvidenceSubject(agentKey, evidence);
    const existing = yield* findEvidence(agentKey, evidence);
    if (existing !== undefined) {
      requireSameBytes(existing, evidence.canonicalEvidence);
      return "existing";
    }
    yield* database<
      Readonly<Record<string, unknown>>
    >`INSERT INTO protocol_evidence
        (agent_key,conversation_id, evidence_kind, subject_id,
         evidence_key, canonical_evidence)
       VALUES (${agentKey},${evidence.conversationId}, ${evidence.kind}, ${evidence.subjectId}, ${evidence.evidenceKey}, ${copyBytes(evidence.canonicalEvidence)})`;
    return "inserted";
  }).pipe(agentOperation(agentKey));
}

function requireRecordPosition(
  agentKey: string,
  record: StagedRecord,
): Effect.Effect<void, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const position = yield* readStoredPosition(agentKey, record.conversationId);
    requireEqual(position.membershipHash, record.membershipHash);
    requireEqual(position.currentAnchorHash, record.anchorHash);
    requireEqual(position.headRecordHash, record.previousRecordHash);
    const proposalLock = yield* findProposalLock(
      agentKey,
      record.conversationId,
      record.previousRecordHash ?? FIRST_MESSAGE_PREDECESSOR,
    );
    if (proposalLock !== undefined) {
      requireEqual(proposalLock.actionHash, record.actionHash);
    }
  }).pipe(agentOperation(agentKey));
}

/**
 * Reads a required conversation position.
 *
 * @param agentKey Immutable storage owner for this operation.
 * @param conversationId Opaque conversation identity.
 * @returns Its durable anchor, head, and local ordinal.
 */
function readStoredPosition(
  agentKey: string,
  conversationId: string,
): Effect.Effect<InternalPosition, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    const row = (yield* database<
      Readonly<Record<string, unknown>>
    >`SELECT conversation_id, membership_hash, current_anchor_hash,
              head_record_hash, head_ordinal
       FROM conversation_state WHERE agent_key=${agentKey} AND conversation_id = ${conversationId}`)[0];
    if (row === undefined) {
      return yield* new EndpointStoreError({ reason: "not-found" });
    }
    return Object.freeze({
      ...readConversationPosition(row),
      headOrdinal: readInteger(row, "head_ordinal"),
    });
  }).pipe(agentOperation(agentKey), Effect.withSpan("readStoredPosition"));
}

/**
 * Projects one durable position row without its local ordinal.
 *
 * @param row SQL result row.
 * @returns The detached conversation position.
 */
function readConversationPosition(
  row: Readonly<Record<string, unknown>>,
): ConversationPosition {
  const headRecordHash = readOptionalText(row, "head_record_hash");
  return Object.freeze({
    conversationId: readText(row, "conversation_id"),
    membershipHash: readText(row, "membership_hash"),
    currentAnchorHash: readText(row, "current_anchor_hash"),
    ...(headRecordHash === undefined ? {} : { headRecordHash }),
  });
}

function findEvidence(
  agentKey: string,
  evidence: ProtocolEvidence,
): Effect.Effect<Uint8Array | undefined, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    const row = (yield* database<
      Readonly<Record<string, unknown>>
    >`SELECT canonical_evidence FROM protocol_evidence
       WHERE agent_key=${agentKey} AND conversation_id = ${evidence.conversationId} AND evidence_kind = ${evidence.kind}
         AND subject_id = ${evidence.subjectId} AND evidence_key = ${evidence.evidenceKey}`)[0];
    return row === undefined ? undefined : readBytes(row, "canonical_evidence");
  }).pipe(agentOperation(agentKey));
}

function requireEvidenceSubject(
  agentKey: string,
  evidence: ProtocolEvidence,
): Effect.Effect<void, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    yield* requireMembership(agentKey, evidence.conversationId);
    if (evidence.kind === "action") {
      if (!(yield* hasActionSubject(agentKey, evidence))) {
        return yield* new EndpointStoreError({ reason: "not-found" });
      }
    }
    if (
      evidence.kind === "durability" &&
      (yield* findStagedRecord(
        agentKey,
        evidence.conversationId,
        evidence.subjectId,
      )) === undefined
    ) {
      return yield* new EndpointStoreError({ reason: "not-found" });
    }
    if (
      evidence.kind === "reanchor" &&
      (yield* findStagedReanchor(
        agentKey,
        evidence.conversationId,
        evidence.subjectId,
      )) === undefined
    ) {
      return yield* new EndpointStoreError({ reason: "not-found" });
    }
  }).pipe(agentOperation(agentKey));
}

/**
 * Reads an optional staged re-anchor by stable body hash.
 *
 * @param agentKey Immutable storage owner for this operation.
 * @param conversationId Opaque conversation identity.
 * @param anchorHash Stable private anchor hash.
 * @returns The detached staged re-anchor when present.
 */
function findStagedReanchor(
  agentKey: string,
  conversationId: string,
  anchorHash: string,
): Effect.Effect<StagedReanchor | undefined, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    const row = (yield* database<
      Readonly<Record<string, unknown>>
    >`SELECT conversation_id, anchor_hash, previous_anchor_hash,
              router_instance_id, selected_record_hash, canonical_body
       FROM reanchors WHERE agent_key=${agentKey} AND conversation_id = ${conversationId} AND anchor_hash = ${anchorHash}`)[0];
    return row === undefined ? undefined : readStagedReanchor(row);
  }).pipe(agentOperation(agentKey), Effect.withSpan("findStagedReanchor"));
}

/**
 * Projects one staged re-anchor body.
 *
 * @param row SQL result row.
 * @returns The detached staged re-anchor.
 */
function readStagedReanchor(
  row: Readonly<Record<string, unknown>>,
): StagedReanchor {
  return Object.freeze({
    conversationId: readText(row, "conversation_id"),
    anchorHash: readText(row, "anchor_hash"),
    previousAnchorHash: readText(row, "previous_anchor_hash"),
    routerInstanceId: readText(row, "router_instance_id"),
    selectedRecordHash: readText(row, "selected_record_hash"),
    canonicalBody: readBytes(row, "canonical_body"),
  });
}

function requireLocalEvidenceLock(
  agentKey: string,
  evidence: ProtocolEvidence,
): Effect.Effect<void, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const identity = yield* readStoredIdentity(agentKey);
    if (identity === undefined || evidence.evidenceKey !== identity.agentId) {
      return;
    }
    if (evidence.kind === "action") {
      if (!(yield* hasProposalAction(agentKey, evidence))) {
        return yield* new EndpointStoreError({ reason: "not-found" });
      }
      return;
    }
    if (evidence.kind === "durability") {
      yield* requireLocalDurabilityEvidenceLock(agentKey, evidence);
    }
  }).pipe(agentOperation(agentKey));
}

function requireLocalDurabilityEvidenceLock(
  agentKey: string,
  evidence: ProtocolEvidence,
): Effect.Effect<void, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const record = yield* findStagedRecord(
      agentKey,
      evidence.conversationId,
      evidence.subjectId,
    );
    if (record === undefined) {
      return yield* new EndpointStoreError({ reason: "not-found" });
    }
    const proposalLock = yield* findProposalLock(
      agentKey,
      record.conversationId,
      record.previousRecordHash ?? FIRST_MESSAGE_PREDECESSOR,
    );
    if (
      proposalLock === undefined ||
      proposalLock.actionHash !== record.actionHash
    ) {
      return yield* new EndpointStoreError({ reason: "not-found" });
    }
    yield* requireNoReanchorAwayFrom(agentKey, record);
  }).pipe(agentOperation(agentKey));
}

/**
 * Reads an optional proposal lock for one predecessor.
 *
 * @param agentKey Immutable storage owner for this operation.
 * @param conversationId Opaque conversation identity.
 * @param predecessorKey Exact predecessor or the first message sentinel.
 * @returns The detached proposal lock when present.
 */
function findProposalLock(
  agentKey: string,
  conversationId: string,
  predecessorKey: string,
): Effect.Effect<ProposalLock | undefined, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    const row = (yield* database<
      Readonly<Record<string, unknown>>
    >`SELECT conversation_id, previous_record_hash, action_hash,
              canonical_action_core
       FROM proposal_locks
       WHERE agent_key=${agentKey} AND conversation_id = ${conversationId} AND predecessor_key = ${predecessorKey}`)[0];
    return row === undefined ? undefined : readProposalLock(row);
  }).pipe(agentOperation(agentKey), Effect.withSpan("findProposalLock"));
}

/**
 * Projects one durable proposal lock.
 *
 * @param row SQL result row.
 * @returns The detached proposal lock.
 */
function readProposalLock(
  row: Readonly<Record<string, unknown>>,
): ProposalLock {
  const previousRecordHash = readOptionalText(row, "previous_record_hash");
  return Object.freeze({
    conversationId: readText(row, "conversation_id"),
    ...(previousRecordHash === undefined ? {} : { previousRecordHash }),
    actionHash: readText(row, "action_hash"),
    canonicalActionCore: readBytes(row, "canonical_action_core"),
  });
}

/**
 * Reads an optional staged record by stable private hash.
 *
 * @param agentKey Immutable storage owner for this operation.
 * @param conversationId Opaque conversation identity.
 * @param recordHash Stable private record hash.
 * @returns The detached staged record when present.
 */
function findStagedRecord(
  agentKey: string,
  conversationId: string,
  recordHash: string,
): Effect.Effect<StagedRecord | undefined, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    const row = (yield* database<
      Readonly<Record<string, unknown>>
    >`SELECT conversation_id, record_hash, previous_record_hash,
              membership_hash, anchor_hash, action_hash, author_agent_id,
              msg_id, canonical_record_core
       FROM staged_records WHERE agent_key=${agentKey} AND conversation_id = ${conversationId} AND record_hash = ${recordHash}`)[0];
    return row === undefined ? undefined : readStagedRecord(row);
  }).pipe(agentOperation(agentKey), Effect.withSpan("findStagedRecord"));
}

/**
 * Projects one exact staged record core.
 *
 * @param row SQL result row.
 * @returns The detached staged record.
 */
function readStagedRecord(
  row: Readonly<Record<string, unknown>>,
): StagedRecord {
  const previousRecordHash = readOptionalText(row, "previous_record_hash");
  return Object.freeze({
    conversationId: readText(row, "conversation_id"),
    recordHash: readText(row, "record_hash"),
    ...(previousRecordHash === undefined ? {} : { previousRecordHash }),
    membershipHash: readText(row, "membership_hash"),
    anchorHash: readText(row, "anchor_hash"),
    actionHash: readText(row, "action_hash"),
    authorAgentId: readText(row, "author_agent_id"),
    msgId: readText(row, "msg_id"),
    canonicalRecordCore: readBytes(row, "canonical_record_core"),
  });
}

/**
 * After staging a re-anchor away from an anchor, refuses both staging and
 * retaining new local durability votes under that anchor. This member cannot
 * sign both leaving a head and certifying its successor.
 */
function requireNoReanchorAwayFrom(
  agentKey: string,
  record: StagedRecord,
): Effect.Effect<void, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    const left = (yield* database<
      Readonly<Record<string, unknown>>
    >`SELECT 1 AS staged FROM reanchors
       WHERE agent_key=${agentKey} AND conversation_id = ${record.conversationId} AND previous_anchor_hash = ${record.anchorHash} LIMIT 1`)[0];
    if (left !== undefined) {
      return yield* new EndpointStoreError({ reason: "conflict" });
    }
  }).pipe(agentOperation(agentKey));
}

function hasProposalAction(
  agentKey: string,
  evidence: ProtocolEvidence,
): Effect.Effect<boolean, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    return (
      (yield* database<
        Readonly<Record<string, unknown>>
      >`SELECT 1 AS retained FROM proposal_locks
         WHERE agent_key=${agentKey} AND conversation_id = ${evidence.conversationId} AND action_hash = ${evidence.subjectId} LIMIT 1`)[0] !==
      undefined
    );
  }).pipe(agentOperation(agentKey));
}

function hasStagedAction(
  agentKey: string,
  lock: ProposalLock,
): Effect.Effect<boolean, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    return (
      (yield* database<
        Readonly<Record<string, unknown>>
      >`SELECT 1 AS staged FROM staged_records
         WHERE agent_key=${agentKey} AND conversation_id = ${lock.conversationId} AND action_hash = ${lock.actionHash} LIMIT 1`)[0] !==
      undefined
    );
  }).pipe(agentOperation(agentKey));
}

function hasActionSubject(
  agentKey: string,
  evidence: ProtocolEvidence,
): Effect.Effect<boolean, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    return (
      (yield* database<
        Readonly<Record<string, unknown>>
      >`SELECT 1 AS retained FROM proposal_locks WHERE agent_key=${agentKey} AND conversation_id=${evidence.conversationId} AND action_hash=${evidence.subjectId} UNION ALL SELECT 1 AS retained FROM staged_records WHERE agent_key=${agentKey} AND conversation_id=${evidence.conversationId} AND action_hash=${evidence.subjectId} LIMIT 1`)[0] !==
      undefined
    );
  }).pipe(agentOperation(agentKey));
}

function insertCertifiedRecord(
  agentKey: string,
  record: CertifiedRecord,
  ordinal: number,
): Effect.Effect<void, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    yield* database<
      Readonly<Record<string, unknown>>
    >`INSERT INTO certified_records (agent_key,conversation_id, record_hash, ordinal)
       VALUES (${agentKey},${record.conversationId}, ${record.recordHash}, ${ordinal})`;
    const core = yield* decodeCanonical(RecordCore, record.canonicalRecordCore);
    const membership = yield* requireMembershipDescriptor(
      agentKey,
      record.conversationId,
    );
    const cards = yield* Effect.forEach(
      membership.members,
      (card) => Schema.decodeUnknown(AgentIdentityCard)(card),
      { concurrency: 1 },
    );
    const message = yield* messageFromIntent(
      cards.map(({ agentId, agentName, principalId }) => ({
        agentId,
        agentName,
        principalId,
      })),
      core.action.messageIntent,
    );
    yield* completeMessage(agentKey, record.conversationId, ordinal, message);
    yield* database<Readonly<Record<string, unknown>>>`UPDATE conversation_state
       SET head_record_hash = ${record.recordHash}, head_ordinal = ${ordinal} WHERE agent_key=${agentKey} AND conversation_id = ${record.conversationId}`;
  }).pipe(agentOperation(agentKey));
}

function completeLocalMessageIntent(
  agentKey: string,
  record: CertifiedRecord,
): Effect.Effect<void, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    const identity = yield* readStoredIdentity(agentKey);
    if (identity === undefined) {
      return yield* new EndpointStoreError({ reason: "not-found" });
    }
    if (record.authorAgentId !== identity.agentId) {
      return;
    }
    const row = (yield* database<
      Readonly<Record<string, unknown>>
    >`SELECT conversation_id, membership_hash, completed_record_hash
       FROM message_intents LEFT JOIN prepared_messages USING(agent_key,conversation_id,author_agent_id,msg_id) WHERE agent_key=${agentKey} AND author_agent_id = ${record.authorAgentId} AND msg_id = ${record.msgId}`)[0];
    if (row === undefined) {
      return yield* new EndpointStoreError({ reason: "not-found" });
    }
    requireEqual(readText(row, "conversation_id"), record.conversationId);
    requireEqual(readText(row, "membership_hash"), record.membershipHash);
    const completed = readOptionalText(row, "completed_record_hash");
    if (completed !== undefined && completed !== record.recordHash) {
      return yield* new EndpointStoreError({ reason: "conflict" });
    }
    yield* database<
      Readonly<Record<string, unknown>>
    >`UPDATE prepared_messages SET completed_record_hash = ${record.recordHash}
       WHERE agent_key=${agentKey} AND author_agent_id = ${record.authorAgentId} AND msg_id = ${record.msgId}`;
  }).pipe(agentOperation(agentKey));
}

function requireMembership(
  agentKey: string,
  conversationId: string,
): Effect.Effect<void, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    if (
      (yield* database<
        Readonly<Record<string, unknown>>
      >`SELECT 1 AS retained FROM memberships WHERE agent_key=${agentKey} AND conversation_id = ${conversationId}`)[0] ===
      undefined
    ) {
      return yield* new EndpointStoreError({ reason: "not-found" });
    }
  }).pipe(agentOperation(agentKey));
}

function hasCertifiedRecord(
  agentKey: string,
  conversationId: string,
  recordHash: string,
): Effect.Effect<boolean, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    return (
      (yield* database<
        Readonly<Record<string, unknown>>
      >`SELECT 1 AS retained FROM certified_records
         WHERE agent_key=${agentKey} AND conversation_id = ${conversationId} AND record_hash = ${recordHash}`)[0] !==
      undefined
    );
  }).pipe(agentOperation(agentKey));
}

function validateCertifiedRecord(record: CertifiedRecord): void {
  validateStagedRecord(record);
  validateCertificateEvidence(record.actionEvidence, {
    conversationId: record.conversationId,
    kind: "action",
    subjectId: record.actionHash,
  });
  validateCertificateEvidence(record.durabilityEvidence, {
    conversationId: record.conversationId,
    kind: "durability",
    subjectId: record.recordHash,
  });
}

function validateCertificateEvidence(
  evidenceItems: readonly ProtocolEvidence[],
  expected: Readonly<{
    conversationId: string;
    kind: ProtocolEvidence["kind"];
    subjectId: string;
  }>,
): void {
  if (evidenceItems.length === 0) {
    throw new StoreSignal("invalid-input");
  }
  const keys = new Set<string>();
  for (const evidence of evidenceItems) {
    validateEvidence(evidence);
    if (
      evidence.conversationId !== expected.conversationId ||
      evidence.kind !== expected.kind ||
      evidence.subjectId !== expected.subjectId ||
      keys.has(evidence.evidenceKey)
    ) {
      throw new StoreSignal("invalid-input");
    }
    keys.add(evidence.evidenceKey);
  }
}

function validateStagedRecord(record: StagedRecord): void {
  requireText(record.conversationId);
  requireText(record.recordHash);
  if (record.previousRecordHash !== undefined) {
    requireText(record.previousRecordHash);
  }
  requireText(record.membershipHash);
  requireText(record.anchorHash);
  requireText(record.actionHash);
  requireText(record.authorAgentId);
  requireText(record.msgId);
  requireBytes(record.canonicalRecordCore);
}

function validateEvidence(evidence: ProtocolEvidence): void {
  requireText(evidence.conversationId);
  requireText(evidence.subjectId);
  requireText(evidence.evidenceKey);
  requireBytes(evidence.canonicalEvidence);
}

/**
 * The caller's transaction keeps lookup and insertion on one leased connection.
 * Repeated envelope identities retain the same complete bytes and conversation.
 * @param agentKey Immutable storage owner for this operation.
 * @param message Canonical complete signed envelope.
 * @returns The retained envelope under its envelope identity.
 */
function enqueueOutboundInTransaction(
  agentKey: string,
  message: OutboundMessageInput,
): Effect.Effect<StoredOutboundMessage, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    validateMessageInput(message);
    yield* requireConversation(agentKey, message.conversationId);
    const existing = yield* findOutbound(agentKey, message.envelopeId);
    if (existing !== undefined) {
      requireEqual(existing.outbound.conversationId, message.conversationId);
      requireEqual(existing.outbound.wireVersion, message.wireVersion);
      requireSameBytes(
        existing.outbound.canonicalSignedMessage,
        message.canonicalSignedMessage,
      );
      return existing.outbound;
    }
    yield* database<
      Readonly<Record<string, unknown>>
    >`INSERT INTO outbound_messages
        (agent_key,conversation_id, envelope_id,
         canonical_signed_message, wire_version, record_hash, attempted, disposition)
       VALUES (${agentKey},${message.conversationId}, ${message.envelopeId}, ${copyBytes(message.canonicalSignedMessage)}, ${message.wireVersion}, ${message.recordHash ?? null}, 0, 'pending')`;
    return copyOutbound(message);
  }).pipe(
    agentOperation(agentKey),
    Effect.withSpan("enqueueOutboundInTransaction"),
  );
}

/**
 * Reads every pending stored envelope in durable insertion order.
 *
 * @param agentKey Immutable storage owner for this operation.
 * @param conversationId Restrict the read to this conversation when provided.
 * @returns Detached pending envelopes, excluding accepted and discarded rows.
 */
function readPendingOutbound(
  agentKey: string,
  conversationId?: string,
): Effect.Effect<
  readonly StoredOutboundMessage[],
  EndpointStoreError,
  SqlClient
> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    const conversation =
      conversationId === undefined
        ? database``
        : database`AND conversation_id=${conversationId}`;
    return Object.freeze(
      (yield* database<
        Readonly<Record<string, unknown>>
      >`SELECT conversation_id,envelope_id,canonical_signed_message,wire_version,record_hash,attempted,disposition FROM outbound_messages WHERE agent_key=${agentKey} AND disposition='pending' ${conversation} ORDER BY outbound_sequence`).map(
        (row) => readOutbound(row).outbound,
      ),
    );
  }).pipe(agentOperation(agentKey), Effect.withSpan("readPendingOutbound"));
}

function requireOutbound(
  agentKey: string,
  envelopeId: string,
): Effect.Effect<RetainedOutbound, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const retained = yield* findOutbound(agentKey, envelopeId);
    if (retained === undefined) {
      return yield* new EndpointStoreError({ reason: "not-found" });
    }
    return retained;
  }).pipe(agentOperation(agentKey));
}

function findOutbound(
  agentKey: string,
  envelopeId: string,
): Effect.Effect<RetainedOutbound | undefined, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    const row = (yield* database<
      Readonly<Record<string, unknown>>
    >`SELECT conversation_id, envelope_id,
              canonical_signed_message, wire_version, record_hash, attempted, disposition
       FROM outbound_messages WHERE agent_key=${agentKey} AND envelope_id = ${envelopeId}`)[0];
    return row === undefined ? undefined : readOutbound(row);
  }).pipe(agentOperation(agentKey));
}

function readOutbound(
  row: Readonly<Record<string, unknown>>,
): RetainedOutbound {
  const attempted = readInteger(row, "attempted");
  if (attempted !== 0 && attempted !== 1) {
    throw new StoreSignal("corrupt");
  }
  const recordHash = readOptionalText(row, "record_hash");
  return Object.freeze({
    outbound: Object.freeze({
      conversationId: readText(row, "conversation_id"),
      envelopeId: readText(row, "envelope_id"),
      wireVersion: readText(row, "wire_version"),
      ...(recordHash === undefined ? {} : { recordHash }),
      canonicalSignedMessage: readBytes(row, "canonical_signed_message"),
    }),
    attempted: attempted === 1,
    disposition: readDisposition(row),
  });
}

function readDisposition(
  row: Readonly<Record<string, unknown>>,
): OutboundDisposition {
  const disposition = readText(row, "disposition");
  switch (disposition) {
    case "accepted":
    case "discarded":
    case "pending":
      return disposition;
    default:
      throw new StoreSignal("corrupt");
  }
}

function requireConversation(
  agentKey: string,
  conversationId: string,
): Effect.Effect<void, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    if (
      (yield* database<
        Readonly<Record<string, unknown>>
      >`SELECT 1 AS retained FROM conversation_state
         WHERE agent_key=${agentKey} AND conversation_id = ${conversationId}`)[0] ===
      undefined
    ) {
      return yield* new EndpointStoreError({ reason: "not-found" });
    }
  }).pipe(agentOperation(agentKey));
}

function validateMessageInput(message: OutboundMessageInput): void {
  requireText(message.wireVersion);
  requireText(message.conversationId);
  requireText(message.envelopeId);
  requireBytes(message.canonicalSignedMessage);
}

/**
 * Rejects empty or non-byte canonical values before persistence.
 *
 * @param value Canonical representation bytes.
 */
function requireBytes(value: Uint8Array): void {
  if (!(value instanceof Uint8Array) || value.byteLength === 0) {
    throw new StoreSignal("invalid-input");
  }
}

function requireSameOutbound(
  left: StoredOutboundMessage,
  right: StoredOutboundMessage,
): void {
  requireEqual(left.conversationId, right.conversationId);
  requireEqual(left.envelopeId, right.envelopeId);
  requireEqual(left.wireVersion, right.wireVersion);
  requireSameBytes(left.canonicalSignedMessage, right.canonicalSignedMessage);
}

function copyOutbound(outbound: StoredOutboundMessage): StoredOutboundMessage {
  return Object.freeze({
    ...outbound,
    canonicalSignedMessage: copyBytes(outbound.canonicalSignedMessage),
  });
}

/** The protocol owns membership verification before publishing its message. */
function requireMembershipDescriptor(agentKey: string, conversationId: string) {
  return Effect.gen(function* () {
    const [membership] = yield* readMemberships(agentKey, conversationId);
    if (membership === undefined) {
      return yield* new EndpointStoreError({ reason: "not-found" });
    }
    return yield* decodeCanonical(
      MembershipDescriptor,
      membership.canonicalMembership,
    );
  }).pipe(storeOperation);
}
