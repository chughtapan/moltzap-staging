/** @file Atomic record-core, evidence, certification, and delivery transitions. */

import * as SqlClient from "@effect/sql/SqlClient";
import { Effect } from "effect";
import type {
  CertifiedRecord,
  OutboundMessageInput,
  ProposalLock,
  ProtocolEvidence,
  StagedRecord,
  StoredOutboundMessage,
  StoreMutation,
} from "./types.js";

import {
  copyBytes,
  EndpointStoreError,
  readBytes,
  readOptionalText,
  readText,
  requireBytes,
  requireEqual,
  requireSameBytes,
  requireText,
  storeOperation,
  StoreSignal,
  transaction,
} from "./database/index.js";
import {
  enqueueOutboundInTransaction,
  readRecordOutbound,
} from "./outbound.js";
import {
  findProposalLock,
  findStagedReanchor,
  findStagedRecord,
  FIRST_MESSAGE_PREDECESSOR,
  lockProposalInTransaction,
  readStoredIdentity,
  readStoredPosition,
  releaseProposalLock,
  requireSameRecord,
} from "./rows/index.js";
/** A record and its exact signed dissemination envelope commit before a durability vote. */
export function stageRecordAndOutbound(
  record: StagedRecord,
  outbound: OutboundMessageInput,
): Effect.Effect<
  StoredOutboundMessage,
  EndpointStoreError,
  SqlClient.SqlClient
> {
  return transaction(
    Effect.gen(function* () {
      validateStagedRecord(record);
      requireEqual(record.conversationId, outbound.conversationId);
      if (
        (yield* findStagedRecord(record.conversationId, record.recordHash)) ===
        undefined
      ) {
        yield* requireNoReanchorAwayFrom(record);
      }
      yield* stageRecordInTransaction(record);
      const retained = yield* readRecordOutbound(
        record.conversationId,
        record.recordHash,
      );
      return retained !== undefined && retained.disposition !== "discarded"
        ? retained.outbound
        : yield* enqueueOutboundInTransaction({
            ...outbound,
            recordHash: record.recordHash,
          });
    }),
  ).pipe(storeOperation, Effect.withSpan("stageRecordAndOutbound"));
}
/**
 * Merges one stable evidence item without replacing a conflicting value.
 *
 * @param evidence Verified evidence with its canonical merge key.
 * @returns Whether the evidence was inserted or already durable.
 */
export function mergeEvidence(
  evidence: ProtocolEvidence,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    validateEvidence(evidence);
    return yield* transaction(
      Effect.gen(function* () {
        yield* requireEvidenceSubject(evidence);
        const existing = yield* findEvidence(evidence);
        if (existing !== undefined) {
          requireSameBytes(existing, evidence.canonicalEvidence);
          return "existing";
        }
        yield* requireLocalEvidenceLock(evidence);
        return yield* mergeEvidenceInTransaction(evidence);
      }),
    );
  }).pipe(storeOperation, Effect.withSpan("mergeEvidence"));
}
/**
 * Atomically replaces the lock this endpoint holds on another action at a
 * predecessor with the lock on an action whose `q(n)` action certificate it
 * verified, releasing everything held for the other action and retaining the
 * certificate's signatures. They are written with the new lock so that a
 * restart never finds the certified action locked without the certificate
 * that keeps this endpoint from signing it.
 *
 * A held action this endpoint already staged refuses the replacement as a
 * conflict. Staging it took its own `q(n)` action certificate, so a second one
 * at the same predecessor shows more than `f` faulty members, and this
 * endpoint, which may have voted for the staged record, must never vote for
 * another successor of that head under the same anchor.
 *
 * @param lock The lock on the certified action.
 * @param certificate The action certificate's signatures as evidence rows.
 * @returns Whether the lock on the certified action was inserted.
 */
export function supersedeProposalLock(
  lock: ProposalLock,
  certificate: readonly ProtocolEvidence[],
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    validateCertificateEvidence(certificate, {
      conversationId: lock.conversationId,
      kind: "action",
      subjectId: lock.actionHash,
    });
    return yield* transaction(
      Effect.gen(function* () {
        const held = yield* findProposalLock(
          lock.conversationId,
          lock.previousRecordHash ?? FIRST_MESSAGE_PREDECESSOR,
        );
        if (held !== undefined && held.actionHash !== lock.actionHash) {
          if (yield* hasStagedAction(held)) {
            return yield* new EndpointStoreError({ reason: "conflict" });
          }
          yield* releaseProposalLock(held);
        }
        const mutation = yield* lockProposalInTransaction(lock);
        for (const evidence of certificate) {
          yield* mergeEvidenceInTransaction(evidence);
        }
        return mutation;
      }),
    );
  }).pipe(storeOperation, Effect.withSpan("supersedeProposalLock"));
}
/**
 * Atomically promotes a complete record, advances its head, and retains its
 * local message completion.
 *
 * @param record Verified complete certified record.
 * @returns Whether any durable state was inserted.
 */
export function promoteRecord(
  record: CertifiedRecord,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    validateCertifiedRecord(record);
    return yield* transaction(promoteRecordInTransaction(record));
  }).pipe(storeOperation, Effect.withSpan("promoteRecord"));
}
/**
 * Atomically stages and promotes a record received whole, with its
 * certificates' evidence and its local message completion. A crash therefore never leaves its core staged without its
 * certification, where a restart could not tell it from a record this
 * endpoint staged for its own durability vote. This endpoint casts no vote
 * for it, so its core is staged even under an anchor this endpoint has
 * staged a re-anchor candidate away from. Retaining an already verified
 * signature, including this endpoint's earlier signature, casts no new vote.
 *
 * @param record Verified complete certified record.
 * @returns Whether any durable state was inserted.
 */
export function applyCertifiedRecord(
  record: CertifiedRecord,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    validateCertifiedRecord(record);
    return yield* transaction(
      Effect.gen(function* () {
        const staged = yield* stageRecordInTransaction(record);
        const promoted = yield* promoteRecordInTransaction(record);
        return staged === "inserted" || promoted === "inserted"
          ? "inserted"
          : "existing";
      }),
    );
  }).pipe(storeOperation, Effect.withSpan("applyCertifiedRecord"));
}
/**
 * Retain one verified record core without authorizing a local signature.
 * @param record Verified record core and private bindings.
 * @returns Whether the record was inserted or already staged.
 */
function stageRecordInTransaction(
  record: StagedRecord,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    const existing = yield* findStagedRecord(
      record.conversationId,
      record.recordHash,
    );
    if (existing !== undefined) {
      requireSameRecord(existing, record);
      return "existing";
    }
    yield* requireRecordPosition(record);
    yield* database.unsafe<Readonly<Record<string, unknown>>>(
      `INSERT INTO staged_records
        (conversation_id, record_hash, previous_record_hash,
         membership_hash, anchor_hash, action_hash, author_agent_id, msg_id,
         canonical_record_core)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        record.conversationId,
        record.recordHash,
        record.previousRecordHash ?? null,
        record.membershipHash,
        record.anchorHash,
        record.actionHash,
        record.authorAgentId,
        record.msgId,
        copyBytes(record.canonicalRecordCore),
      ],
    );
    return "inserted";
  }).pipe(storeOperation);
}
function promoteRecordInTransaction(
  record: CertifiedRecord,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const staged = yield* findStagedRecord(
      record.conversationId,
      record.recordHash,
    );
    if (staged === undefined) {
      return yield* new EndpointStoreError({ reason: "not-found" });
    }
    requireSameRecord(staged, record);
    const evidenceMutation = yield* mergeCertifiedEvidence(record);
    if (yield* hasCertifiedRecord(record.conversationId, record.recordHash)) {
      return evidenceMutation;
    }
    const position = yield* readStoredPosition(record.conversationId);
    requireEqual(position.currentAnchorHash, record.anchorHash);
    requireEqual(position.headRecordHash, record.previousRecordHash);
    yield* insertCertifiedRecord(record, position.headOrdinal + 1);
    yield* completeLocalMessageIntent(record);
    return "inserted";
  }).pipe(storeOperation);
}
function mergeCertifiedEvidence(
  record: CertifiedRecord,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    let mutation: StoreMutation = "existing";
    for (const evidence of [
      ...record.actionEvidence,
      ...record.durabilityEvidence,
    ]) {
      if ((yield* mergeEvidenceInTransaction(evidence)) === "inserted") {
        mutation = "inserted";
      }
    }
    return mutation;
  }).pipe(storeOperation);
}
function mergeEvidenceInTransaction(
  evidence: ProtocolEvidence,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    yield* requireEvidenceSubject(evidence);
    const existing = yield* findEvidence(evidence);
    if (existing !== undefined) {
      requireSameBytes(existing, evidence.canonicalEvidence);
      return "existing";
    }
    yield* database.unsafe<Readonly<Record<string, unknown>>>(
      `INSERT INTO protocol_evidence
        (conversation_id, evidence_kind, subject_id,
         evidence_key, canonical_evidence)
       VALUES (?, ?, ?, ?, ?)`,
      [
        evidence.conversationId,
        evidence.kind,
        evidence.subjectId,
        evidence.evidenceKey,
        copyBytes(evidence.canonicalEvidence),
      ],
    );
    return "inserted";
  }).pipe(storeOperation);
}
function requireRecordPosition(
  record: StagedRecord,
): Effect.Effect<void, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const position = yield* readStoredPosition(record.conversationId);
    requireEqual(position.membershipHash, record.membershipHash);
    requireEqual(position.currentAnchorHash, record.anchorHash);
    requireEqual(position.headRecordHash, record.previousRecordHash);
    const proposalLock = yield* findProposalLock(
      record.conversationId,
      record.previousRecordHash ?? FIRST_MESSAGE_PREDECESSOR,
    );
    if (proposalLock !== undefined) {
      requireEqual(proposalLock.actionHash, record.actionHash);
    }
  }).pipe(storeOperation);
}
function findEvidence(
  evidence: ProtocolEvidence,
): Effect.Effect<
  Uint8Array | undefined,
  EndpointStoreError,
  SqlClient.SqlClient
> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    const row = (yield* database.unsafe<Readonly<Record<string, unknown>>>(
      `SELECT canonical_evidence FROM protocol_evidence
       WHERE conversation_id = ? AND evidence_kind = ?
         AND subject_id = ? AND evidence_key = ?`,
      [
        evidence.conversationId,
        evidence.kind,
        evidence.subjectId,
        evidence.evidenceKey,
      ],
    ))[0];
    return row === undefined ? undefined : readBytes(row, "canonical_evidence");
  }).pipe(storeOperation);
}
function requireEvidenceSubject(
  evidence: ProtocolEvidence,
): Effect.Effect<void, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    yield* requireMembership(evidence.conversationId);
    if (evidence.kind === "action") {
      if (!(yield* hasActionSubject(evidence))) {
        return yield* new EndpointStoreError({ reason: "not-found" });
      }
    }
    if (
      evidence.kind === "durability" &&
      (yield* findStagedRecord(evidence.conversationId, evidence.subjectId)) ===
        undefined
    ) {
      return yield* new EndpointStoreError({ reason: "not-found" });
    }
    if (
      evidence.kind === "reanchor" &&
      (yield* findStagedReanchor(
        evidence.conversationId,
        evidence.subjectId,
      )) === undefined
    ) {
      return yield* new EndpointStoreError({ reason: "not-found" });
    }
  }).pipe(storeOperation);
}
function requireLocalEvidenceLock(
  evidence: ProtocolEvidence,
): Effect.Effect<void, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const identity = yield* readStoredIdentity();
    if (identity === undefined || evidence.evidenceKey !== identity.agentId) {
      return;
    }
    if (evidence.kind === "action") {
      if (!(yield* hasProposalAction(evidence))) {
        return yield* new EndpointStoreError({ reason: "not-found" });
      }
      return;
    }
    if (evidence.kind === "durability") {
      yield* requireLocalDurabilityEvidenceLock(evidence);
    }
  }).pipe(storeOperation);
}
function requireLocalDurabilityEvidenceLock(
  evidence: ProtocolEvidence,
): Effect.Effect<void, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const record = yield* findStagedRecord(
      evidence.conversationId,
      evidence.subjectId,
    );
    if (record === undefined) {
      return yield* new EndpointStoreError({ reason: "not-found" });
    }
    const proposalLock = yield* findProposalLock(
      record.conversationId,
      record.previousRecordHash ?? FIRST_MESSAGE_PREDECESSOR,
    );
    if (
      proposalLock === undefined ||
      proposalLock.actionHash !== record.actionHash
    ) {
      return yield* new EndpointStoreError({ reason: "not-found" });
    }
    yield* requireNoReanchorAwayFrom(record);
  }).pipe(storeOperation);
}
/**
 * Refuse a record under an anchor this endpoint has staged a re-anchor
 * candidate away from, wherever this endpoint would vote for it: as it stages
 * the record for its vote, and as it stores its own vote for any staged
 * record. A member that has voted to leave an anchor signs nothing more under
 * it, so a re-anchor away from a head and a durability certificate extending
 * that head never both collect this endpoint's signature.
 * @param record Record about to be staged for this endpoint's vote, or a
 *     staged record this endpoint's own vote is about to be stored for.
 */
function requireNoReanchorAwayFrom(
  record: StagedRecord,
): Effect.Effect<void, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    const left = (yield* database.unsafe<Readonly<Record<string, unknown>>>(
      `SELECT 1 AS staged FROM reanchors
       WHERE conversation_id = ? AND previous_anchor_hash = ? LIMIT 1`,
      [record.conversationId, record.anchorHash],
    ))[0];
    if (left !== undefined) {
      return yield* new EndpointStoreError({ reason: "conflict" });
    }
  }).pipe(storeOperation);
}
function hasProposalAction(
  evidence: ProtocolEvidence,
): Effect.Effect<boolean, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    return (
      (yield* database.unsafe<Readonly<Record<string, unknown>>>(
        `SELECT 1 AS retained FROM proposal_locks
         WHERE conversation_id = ? AND action_hash = ? LIMIT 1`,
        [evidence.conversationId, evidence.subjectId],
      ))[0] !== undefined
    );
  }).pipe(storeOperation);
}
function hasStagedAction(
  lock: ProposalLock,
): Effect.Effect<boolean, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    return (
      (yield* database.unsafe<Readonly<Record<string, unknown>>>(
        `SELECT 1 AS staged FROM staged_records
         WHERE conversation_id = ? AND action_hash = ? LIMIT 1`,
        [lock.conversationId, lock.actionHash],
      ))[0] !== undefined
    );
  }).pipe(storeOperation);
}
function hasActionSubject(
  evidence: ProtocolEvidence,
): Effect.Effect<boolean, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    return (
      (yield* database.unsafe<Readonly<Record<string, unknown>>>(
        `SELECT 1 AS retained FROM proposal_locks
         WHERE conversation_id = ? AND action_hash = ?
         UNION ALL
         SELECT 1 AS retained FROM staged_records
         WHERE conversation_id = ? AND action_hash = ?
         LIMIT 1`,
        [
          evidence.conversationId,
          evidence.subjectId,
          evidence.conversationId,
          evidence.subjectId,
        ],
      ))[0] !== undefined
    );
  }).pipe(storeOperation);
}
function insertCertifiedRecord(
  record: CertifiedRecord,
  ordinal: number,
): Effect.Effect<void, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    yield* database.unsafe<Readonly<Record<string, unknown>>>(
      `INSERT INTO certified_records (conversation_id, record_hash, ordinal)
       VALUES (?, ?, ?)`,
      [record.conversationId, record.recordHash, ordinal],
    );
    yield* database.unsafe<Readonly<Record<string, unknown>>>(
      `UPDATE conversation_state
       SET head_record_hash = ?, head_ordinal = ? WHERE conversation_id = ?`,
      [record.recordHash, ordinal, record.conversationId],
    );
  }).pipe(storeOperation);
}
function completeLocalMessageIntent(
  record: CertifiedRecord,
): Effect.Effect<void, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    const identity = yield* readStoredIdentity();
    if (identity === undefined) {
      return yield* new EndpointStoreError({ reason: "not-found" });
    }
    if (record.authorAgentId !== identity.agentId) {
      return;
    }
    const row = (yield* database.unsafe<Readonly<Record<string, unknown>>>(
      `SELECT conversation_id, membership_hash, completed_record_hash
       FROM message_intents WHERE author_agent_id = ? AND msg_id = ?`,
      [record.authorAgentId, record.msgId],
    ))[0];
    if (row === undefined) {
      return yield* new EndpointStoreError({ reason: "not-found" });
    }
    requireEqual(readText(row, "conversation_id"), record.conversationId);
    requireEqual(readText(row, "membership_hash"), record.membershipHash);
    const completed = readOptionalText(row, "completed_record_hash");
    if (completed !== undefined && completed !== record.recordHash) {
      return yield* new EndpointStoreError({ reason: "conflict" });
    }
    yield* database.unsafe<Readonly<Record<string, unknown>>>(
      `UPDATE message_intents SET completed_record_hash = ?
       WHERE author_agent_id = ? AND msg_id = ?`,
      [record.recordHash, record.authorAgentId, record.msgId],
    );
  }).pipe(storeOperation);
}
function requireMembership(
  conversationId: string,
): Effect.Effect<void, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    if (
      (yield* database.unsafe<Readonly<Record<string, unknown>>>(
        "SELECT 1 AS retained FROM memberships WHERE conversation_id = ?",
        [conversationId],
      ))[0] === undefined
    ) {
      return yield* new EndpointStoreError({ reason: "not-found" });
    }
  }).pipe(storeOperation);
}
function hasCertifiedRecord(
  conversationId: string,
  recordHash: string,
): Effect.Effect<boolean, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    return (
      (yield* database.unsafe<Readonly<Record<string, unknown>>>(
        `SELECT 1 AS retained FROM certified_records
         WHERE conversation_id = ? AND record_hash = ?`,
        [conversationId, recordHash],
      ))[0] !== undefined
    );
  }).pipe(storeOperation);
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
