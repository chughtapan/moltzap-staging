/** @file Decodes retained protocol rows and checks idempotent transitions. */

import { SqlClient } from "@effect/sql/SqlClient";
import { Effect, Schema } from "effect";

import type {
  CertifiedRecord,
  ConversationPosition,
  ProposalLock,
  ProtocolEvidence,
  RecoveredReanchor,
  StagedReanchor,
  StagedRecord,
  StoreMutation,
} from "./types.js";

import { AgentId } from "../../identity/agent.js";
import {
  copyBytes,
  EndpointStoreError,
  readBytes,
  readInteger,
  readOptionalBytes,
  readOptionalText,
  readText,
  requireEqual,
  requireSameBytes,
  storeOperation,
  StoreSignal,
} from "./database.js";

/** The predecessor key of a conversation's first record. */
export const FIRST_MESSAGE_PREDECESSOR = "";

/** Durable position plus the local indexing aid that carries no authority. */
export interface InternalPosition extends ConversationPosition {
  readonly headOrdinal: number;
}

/** The communication owner binds one immutable agent identity to its protocol state. */
export function readStoredIdentity() {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    const [row] = yield* sql.unsafe<{ readonly agentId: string }>(
      'SELECT agent_id AS "agentId" FROM communication_metadata WHERE singleton=1',
    );
    return row === undefined
      ? undefined
      : {
          agentId: yield* Schema.decodeUnknown(AgentId)(row.agentId).pipe(
            Effect.catchTag(
              "ParseError",
              () => new EndpointStoreError({ reason: "corrupt" }),
            ),
          ),
        };
  }).pipe(
    Effect.withSpan("readStoredIdentity"),
    Effect.catchTag(
      "SqlError",
      () => new EndpointStoreError({ reason: "persistence" }),
    ),
  );
}

/**
 * Inserts one first-candidate lock inside a caller-owned transaction, or
 * confirms the same lock is already durable.
 * @param proposal Verified gap-free action selected at its predecessor.
 * @returns Whether the lock was inserted or already durable.
 */
export function lockProposalInTransaction(
  proposal: ProposalLock,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    const predecessorKey =
      proposal.previousRecordHash ?? FIRST_MESSAGE_PREDECESSOR;
    const existing = yield* findProposalLock(
      proposal.conversationId,
      predecessorKey,
    );
    if (existing !== undefined) {
      requireSameProposalLock(existing, proposal);
      return "existing";
    }
    const position = yield* readStoredPosition(proposal.conversationId);
    requireEqual(position.headRecordHash, proposal.previousRecordHash);
    yield* database.unsafe<Readonly<Record<string, unknown>>>(
      `INSERT INTO proposal_locks
        (conversation_id, predecessor_key, previous_record_hash,
         action_hash, canonical_action_core)
       VALUES (?, ?, ?, ?, ?)`,
      [
        proposal.conversationId,
        predecessorKey,
        proposal.previousRecordHash ?? null,
        proposal.actionHash,
        copyBytes(proposal.canonicalActionCore),
      ],
    );
    return "inserted";
  }).pipe(storeOperation, Effect.withSpan("lockProposalInTransaction"));
}

/**
 * Deletes one proposal lock inside a caller-owned transaction, with the
 * action signatures held for its action and every record staged for that
 * action, including the record's durability votes and signed outbox association. The caller shows the action can never be certified; kept
 * without the lock, any of these rows would name an action no durable lock
 * selects, which startup refuses.
 * @param lock The durable lock to release.
 */
export function releaseProposalLock(
  lock: ProposalLock,
): Effect.Effect<void, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    const staged = `SELECT record_hash FROM staged_records
    WHERE conversation_id = ?1 AND action_hash = ?2`;
    for (const statement of [
      `UPDATE outbound_messages SET record_hash=NULL,
       disposition=CASE WHEN disposition='pending' THEN 'discarded' ELSE disposition END
       WHERE conversation_id=?1 AND record_hash IN (${staged})`,
      `DELETE FROM protocol_evidence
     WHERE conversation_id = ?1 AND evidence_kind = 'durability'
       AND subject_id IN (${staged})`,
      `DELETE FROM staged_records
     WHERE conversation_id = ?1 AND action_hash = ?2`,
      `DELETE FROM protocol_evidence
     WHERE conversation_id = ?1 AND evidence_kind = 'action'
       AND subject_id = ?2`,
      `DELETE FROM proposal_locks
     WHERE conversation_id = ?1 AND action_hash = ?2`,
    ]) {
      yield* database.unsafe<Readonly<Record<string, unknown>>>(statement, [
        { 1: lock.conversationId, 2: lock.actionHash },
      ]);
    }
  }).pipe(storeOperation, Effect.withSpan("releaseProposalLock"));
}

/**
 * Reads an optional proposal lock for one predecessor.
 *
 * @param conversationId Opaque conversation identity.
 * @param predecessorKey Exact predecessor or the first message sentinel.
 * @returns The detached proposal lock when present.
 */
export function findProposalLock(
  conversationId: string,
  predecessorKey: string,
): Effect.Effect<ProposalLock | undefined, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    const row = (yield* database.unsafe<Readonly<Record<string, unknown>>>(
      `SELECT conversation_id, previous_record_hash, action_hash,
              canonical_action_core
       FROM proposal_locks
       WHERE conversation_id = ? AND predecessor_key = ?`,
      [conversationId, predecessorKey],
    ))[0];
    return row === undefined ? undefined : readProposalLock(row);
  }).pipe(storeOperation, Effect.withSpan("findProposalLock"));
}

/**
 * Projects one durable proposal lock.
 *
 * @param row SQLite result row.
 * @returns The detached proposal lock.
 */
export function readProposalLock(
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
 * Reads a required conversation position.
 *
 * @param conversationId Opaque conversation identity.
 * @returns Its durable anchor, head, and local ordinal.
 */
export function readStoredPosition(
  conversationId: string,
): Effect.Effect<InternalPosition, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    const row = (yield* database.unsafe<Readonly<Record<string, unknown>>>(
      `SELECT conversation_id, membership_hash, current_anchor_hash,
              head_record_hash, head_ordinal
       FROM conversation_state WHERE conversation_id = ?`,
      [conversationId],
    ))[0];
    if (row === undefined) {
      return yield* new EndpointStoreError({ reason: "not-found" });
    }
    return Object.freeze({
      ...readConversationPosition(row),
      headOrdinal: readInteger(row, "head_ordinal"),
    });
  }).pipe(storeOperation, Effect.withSpan("readStoredPosition"));
}

/**
 * Projects one durable position row without its local ordinal.
 *
 * @param row SQLite result row.
 * @returns The detached conversation position.
 */
export function readConversationPosition(
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

/**
 * Projects one complete certified record and its separate evidence maps.
 *
 * @param row SQLite result row for the certified record core.
 * @returns The detached complete certified record.
 */
export function readCertifiedRecord(
  row: Readonly<Record<string, unknown>>,
): Effect.Effect<CertifiedRecord, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const staged = readStagedRecord(row);
    const actionEvidence = yield* readEvidenceForSubject(
      staged.conversationId,
      "action",
      staged.actionHash,
    );
    const durabilityEvidence = yield* readEvidenceForSubject(
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
  }).pipe(storeOperation, Effect.withSpan("readCertifiedRecord"));
}

/**
 * Reads an optional staged record by stable private hash.
 *
 * @param conversationId Opaque conversation identity.
 * @param recordHash Stable private record hash.
 * @returns The detached staged record when present.
 */
export function findStagedRecord(
  conversationId: string,
  recordHash: string,
): Effect.Effect<StagedRecord | undefined, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    const row = (yield* database.unsafe<Readonly<Record<string, unknown>>>(
      `SELECT conversation_id, record_hash, previous_record_hash,
              membership_hash, anchor_hash, action_hash, author_agent_id,
              msg_id, canonical_record_core
       FROM staged_records WHERE conversation_id = ? AND record_hash = ?`,
      [conversationId, recordHash],
    ))[0];
    return row === undefined ? undefined : readStagedRecord(row);
  }).pipe(storeOperation, Effect.withSpan("findStagedRecord"));
}

/**
 * Projects one exact staged record core.
 *
 * @param row SQLite result row.
 * @returns The detached staged record.
 */
export function readStagedRecord(
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
 * Requires two records with one stable hash to have identical bindings.
 *
 * @param left Previously retained staged record.
 * @param right Candidate record with the same private hash.
 */
export function requireSameRecord(
  left: StagedRecord,
  right: StagedRecord,
): void {
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

/**
 * Projects one stable evidence value.
 *
 * @param row SQLite result row.
 * @returns The detached protocol evidence.
 */
export function readProtocolEvidence(
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
 * @param row SQLite result row.
 * @returns The detached recoverable re-anchor.
 */
export function readRecoveredReanchor(
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

/**
 * Reads an optional staged re-anchor by stable body hash.
 *
 * @param conversationId Opaque conversation identity.
 * @param anchorHash Stable private anchor hash.
 * @returns The detached staged re-anchor when present.
 */
export function findStagedReanchor(
  conversationId: string,
  anchorHash: string,
): Effect.Effect<StagedReanchor | undefined, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    const row = (yield* database.unsafe<Readonly<Record<string, unknown>>>(
      `SELECT conversation_id, anchor_hash, previous_anchor_hash,
              router_instance_id, selected_record_hash, canonical_body
       FROM reanchors WHERE conversation_id = ? AND anchor_hash = ?`,
      [conversationId, anchorHash],
    ))[0];
    return row === undefined ? undefined : readStagedReanchor(row);
  }).pipe(storeOperation, Effect.withSpan("findStagedReanchor"));
}

/**
 * Requires two re-anchors with one hash to have identical body bindings.
 *
 * @param left Previously retained re-anchor.
 * @param right Candidate re-anchor with the same private hash.
 */
export function requireSameReanchor(
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

/**
 * Reads one ordered signer evidence map from its separate store rows.
 *
 * @param conversationId Conversation whose evidence is retained.
 * @param kind Separate evidence domain to read.
 * @param subjectId Action, record, request, or anchor identity.
 * @returns Detached evidence ordered by signer key.
 */
function readEvidenceForSubject(
  conversationId: string,
  kind: ProtocolEvidence["kind"],
  subjectId: string,
): Effect.Effect<readonly ProtocolEvidence[], EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    return Object.freeze(
      (yield* database.unsafe<Readonly<Record<string, unknown>>>(
        `SELECT conversation_id, evidence_kind, subject_id,
                evidence_key, canonical_evidence
         FROM protocol_evidence
         WHERE conversation_id = ? AND evidence_kind = ? AND subject_id = ?
         ORDER BY evidence_key COLLATE BINARY`,
        [conversationId, kind, subjectId],
      )).map(readProtocolEvidence),
    );
  }).pipe(storeOperation);
}

/**
 * Projects one staged re-anchor body.
 *
 * @param row SQLite result row.
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
