/** @file Durable candidate locks and the conversation positions they protect. */

import * as SqlClient from "@effect/sql/SqlClient";
import { Effect } from "effect";
import type {
  ConversationPosition,
  ProposalLock,
  StoreMutation,
} from "../types.js";

import {
  copyBytes,
  EndpointStoreError,
  readBytes,
  readInteger,
  readOptionalText,
  readText,
  requireEqual,
  requireSameBytes,
  storeOperation,
} from "../database/index.js";
/** The predecessor key of a conversation's first record. */
export const FIRST_MESSAGE_PREDECESSOR = "";
/**
 * Inserts one first-candidate lock inside a caller-owned transaction, or
 * confirms the same lock is already durable.
 * @param proposal Verified gap-free action selected at its predecessor.
 * @returns Whether the lock was inserted or already durable.
 */
export function lockProposalInTransaction(
  proposal: ProposalLock,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
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
): Effect.Effect<void, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
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
): Effect.Effect<
  ProposalLock | undefined,
  EndpointStoreError,
  SqlClient.SqlClient
> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
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
/** Durable position plus the local indexing aid that carries no authority. */
export interface InternalPosition extends ConversationPosition {
  readonly headOrdinal: number;
}
/**
 * Reads a required conversation position.
 *
 * @param conversationId Opaque conversation identity.
 * @returns Its durable anchor, head, and local ordinal.
 */
export function readStoredPosition(
  conversationId: string,
): Effect.Effect<InternalPosition, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
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
function requireSameProposalLock(
  left: ProposalLock,
  right: ProposalLock,
): void {
  requireEqual(left.conversationId, right.conversationId);
  requireEqual(left.previousRecordHash, right.previousRecordHash);
  requireEqual(left.actionHash, right.actionHash);
  requireSameBytes(left.canonicalActionCore, right.canonicalActionCore);
}
