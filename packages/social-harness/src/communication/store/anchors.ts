/** @file Atomic identity, message-intent, foundation, and Router-anchor transitions. */

import * as SqlClient from "@effect/sql/SqlClient";
import { Effect } from "effect";
import type {
  CompletedReanchor,
  ConversationFoundation,
  EmptyConversationRestart,
  MessageIntent,
  MessageIntentBinding,
  ProposalLock,
  RestartedEmptyConversation,
  StagedReanchor,
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
  findProposalLock,
  findStagedReanchor,
  lockProposalInTransaction,
  readStoredIdentity,
  readStoredPosition,
  releaseProposalLock,
  requireSameReanchor,
} from "./rows/index.js";
/**
 * Durably binds one author-scoped message to its immutable canonical intent.
 *
 * @param binding Canonical message recovery identity and optional new foundation.
 * @returns Whether the same binding was inserted or already durable.
 */
export function bindMessageIntent(
  binding: MessageIntentBinding,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    validateMessageIntentBinding(binding);
    return yield* transaction(bindIntentWithFoundation(binding));
  }).pipe(storeOperation, Effect.withSpan("bindMessageIntent"));
}
/**
 * Atomically persists the immutable membership and initial Router anchor.
 *
 * @param foundation Verified immutable conversation foundation.
 * @returns Whether the same foundation was inserted or already durable.
 */
export function putConversationFoundation(
  foundation: ConversationFoundation,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient.SqlClient> {
  return transaction(putConversationFoundationInTransaction(foundation)).pipe(
    storeOperation,
  );
}
/**
 * Commits the first valid Router-ordered action for one predecessor.
 *
 * A caller signs only after this operation succeeds, so an endpoint cannot
 * sign two actions for the same predecessor across crashes or restarts.
 *
 * @param proposal Verified action core selected in Router order.
 * @returns Whether the lock was inserted or the same lock already existed.
 */
export function lockProposal(
  proposal: ProposalLock,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    validateProposalLock(proposal);
    return yield* transaction(lockProposalInTransaction(proposal));
  }).pipe(storeOperation, Effect.withSpan("lockProposal"));
}
/**
 * Atomically retains a verified FIRST_MESSAGE foundation and its first-candidate lock.
 *
 * @param foundation Verified immutable membership and Router anchor.
 * @param proposal Verified gap-free FIRST_MESSAGE action selected in Router order.
 * @returns Whether either exact durable component was inserted.
 */
export function lockFirstMessageProposal(
  foundation: ConversationFoundation,
  proposal: ProposalLock,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    validateProposalLock(proposal);
    if (
      proposal.previousRecordHash !== undefined ||
      proposal.conversationId !== foundation.conversationId
    ) {
      return yield* new EndpointStoreError({ reason: "invalid-input" });
    }
    return yield* transaction(
      Effect.gen(function* () {
        const retainedFoundation =
          yield* putConversationFoundationInTransaction(foundation);
        const retainedProposal = yield* lockProposalInTransaction(proposal);
        return retainedFoundation === "inserted" ||
          retainedProposal === "inserted"
          ? "inserted"
          : "existing";
      }),
    );
  }).pipe(storeOperation, Effect.withSpan("lockFirstMessageProposal"));
}
/**
 * Replaces an uncommitted conversation's Router-instance foundation.
 *
 * The exact old foundation and empty certified head prevent this cleanup from
 * erasing committed history or a concurrently advanced anchor.
 *
 * @param restart Expected old and verified replacement initial foundations.
 * @returns The replacement foundation and immutable retained message intents.
 */
export function restartEmptyConversation(
  restart: EmptyConversationRestart,
): Effect.Effect<
  RestartedEmptyConversation,
  EndpointStoreError,
  SqlClient.SqlClient
> {
  return Effect.gen(function* () {
    validateEmptyConversationRestart(restart);
    return yield* transaction(
      Effect.gen(function* () {
        yield* requireEmptyConversationPosition(restart.expectedFoundation);
        yield* clearIncompleteConversationState(restart.expectedFoundation);
        yield* installReplacementFoundation(restart.replacementFoundation);
        return Object.freeze({
          foundation: copyFoundation(restart.replacementFoundation),
          messageIntents: yield* readConversationMessageIntents(
            restart.replacementFoundation.conversationId,
          ),
        });
      }),
    );
  }).pipe(storeOperation, Effect.withSpan("restartEmptyConversation"));
}
/**
 * Durably stages at most one re-anchor proposal for one Router-instance scope,
 * for this endpoint's vote. It refuses a proposal away from a head this
 * endpoint holds a staged successor of under the same anchor, since it has
 * voted that successor durable.
 *
 * @param reanchor Verified proposal body and stable hash.
 * @returns Whether the same proposal was inserted or already durable.
 */
export function stageReanchor(
  reanchor: StagedReanchor,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    validateStagedReanchor(reanchor);
    return yield* transaction(stageReanchorInTransaction(reanchor, "vote"));
  }).pipe(storeOperation, Effect.withSpan("stageReanchor"));
}
/**
 * Atomically makes a staged threshold-complete Router anchor current.
 *
 * @param reanchor Verified completed re-anchor and threshold evidence.
 * @returns Whether completion advanced the current anchor or was idempotent.
 */
export function completeReanchor(
  reanchor: CompletedReanchor,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    validateCompletedReanchor(reanchor);
    return yield* transaction(completeReanchorInTransaction(reanchor));
  }).pipe(storeOperation, Effect.withSpan("completeReanchor"));
}
/**
 * Atomically stages and completes one member's verified completed re-anchor.
 *
 * The completion replaces a different candidate this endpoint staged for the
 * same previous anchor and Router instance. The one-candidate rule limits
 * what this endpoint signs, not which quorum-certified anchor it accepts:
 * two certificates in one scope would need an honest member to vote twice,
 * so the replaced candidate can never be certified.
 *
 * @param reanchor One verified completed re-anchor from a member.
 * @returns Whether any durable state was added.
 */
export function applyCatchUpReanchor(
  reanchor: CompletedReanchor,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    validateCompletedReanchor(reanchor);
    return yield* transaction(
      Effect.gen(function* () {
        yield* retireSupersededCandidates(reanchor);
        const staged = yield* stageReanchorInTransaction(reanchor, "adopted");
        const completed = yield* completeReanchorInTransaction(reanchor);
        return staged === "inserted" || completed === "inserted"
          ? "inserted"
          : "existing";
      }),
    );
  }).pipe(storeOperation, Effect.withSpan("applyCatchUpReanchor"));
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
  binding: MessageIntentBinding,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const intent = binding.intent;
    const foundation =
      binding.kind === "new-conversation"
        ? yield* putConversationFoundationInTransaction(binding.foundation)
        : yield* requireConversationFoundation(intent);
    const messageIntent = yield* bindMessageIntentInTransaction(intent);
    return foundation === "inserted" || messageIntent === "inserted"
      ? "inserted"
      : "existing";
  }).pipe(storeOperation);
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
  intent: MessageIntent,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    const identity = yield* readStoredIdentity();
    if (identity === undefined) {
      return yield* new EndpointStoreError({ reason: "not-found" });
    }
    requireEqual(identity.agentId, intent.authorAgentId);
    const existing = (yield* database.unsafe<Readonly<Record<string, unknown>>>(
      `SELECT conversation_id, membership_hash, canonical_intent, canonical_message
       FROM message_intents WHERE author_agent_id = ? AND msg_id = ?`,
      [intent.authorAgentId, intent.msgId],
    ))[0];
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
    yield* database.unsafe(
      `UPDATE message_intents SET membership_hash=?,canonical_intent=?
       WHERE author_agent_id=? AND msg_id=?`,
      [
        intent.membershipHash,
        intent.canonicalIntent,
        intent.authorAgentId,
        intent.msgId,
      ],
    );
    return "inserted";
  }).pipe(storeOperation);
}
/**
 * Inserts or validates one complete membership and initial anchor in an
 * existing transaction.
 *
 * @param foundation Verified immutable conversation foundation.
 * @returns Whether this transaction inserted the foundation.
 */
function putConversationFoundationInTransaction(
  foundation: ConversationFoundation,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    validateFoundation(foundation);
    const existing = yield* readFoundation(foundation.conversationId);
    if (existing !== undefined) {
      requireSameFoundation(existing, foundation);
      return "existing";
    }
    yield* insertFoundation(foundation);
    return "inserted";
  }).pipe(storeOperation);
}
function requireConversationFoundation(
  intent: MessageIntent,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const foundation = yield* readFoundation(intent.conversationId);
    if (foundation === undefined) {
      return yield* new EndpointStoreError({ reason: "not-found" });
    }
    requireEqual(foundation.membershipHash, intent.membershipHash);
    return yield* Effect.succeed<StoreMutation>("existing");
  }).pipe(storeOperation);
}
function requireEmptyConversationPosition(
  expected: ConversationFoundation,
): Effect.Effect<void, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    const retained = yield* readFoundation(expected.conversationId);
    if (retained === undefined) {
      return yield* new EndpointStoreError({ reason: "not-found" });
    }
    requireSameFoundation(retained, expected);
    const position = yield* readStoredPosition(expected.conversationId);
    requireEqual(position.membershipHash, expected.membershipHash);
    requireEqual(position.currentAnchorHash, expected.anchorHash);
    requireEqual(position.headRecordHash, undefined);
    requireEqual(position.headOrdinal, -1);
    if (
      (yield* database.unsafe<Readonly<Record<string, unknown>>>(
        `SELECT 1 AS retained FROM certified_records
         WHERE conversation_id = ? LIMIT 1`,
        [expected.conversationId],
      ))[0] !== undefined
    ) {
      return yield* new EndpointStoreError({ reason: "conflict" });
    }
  }).pipe(storeOperation);
}
function readFoundation(
  conversationId: string,
): Effect.Effect<
  ConversationFoundation | undefined,
  EndpointStoreError,
  SqlClient.SqlClient
> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    const row = (yield* database.unsafe<Readonly<Record<string, unknown>>>(
      `SELECT membership.membership_hash, membership.canonical_membership,
              anchor.anchor_hash, anchor.canonical_anchor
       FROM memberships AS membership
       JOIN anchors AS anchor
         ON anchor.conversation_id = membership.conversation_id
        AND anchor.previous_anchor_hash IS NULL
       WHERE membership.conversation_id = ?`,
      [conversationId],
    ))[0];
    return row === undefined
      ? undefined
      : Object.freeze({
          conversationId,
          membershipHash: readText(row, "membership_hash"),
          canonicalMembership: readBytes(row, "canonical_membership"),
          anchorHash: readText(row, "anchor_hash"),
          canonicalAnchor: readBytes(row, "canonical_anchor"),
        });
  }).pipe(storeOperation);
}
function insertFoundation(
  foundation: ConversationFoundation,
): Effect.Effect<void, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    yield* database.unsafe<Readonly<Record<string, unknown>>>(
      `INSERT INTO memberships
        (conversation_id, membership_hash, canonical_membership)
       VALUES (?, ?, ?)`,
      [
        foundation.conversationId,
        foundation.membershipHash,
        copyBytes(foundation.canonicalMembership),
      ],
    );
    yield* database.unsafe<Readonly<Record<string, unknown>>>(
      `INSERT INTO anchors
        (conversation_id, anchor_hash, canonical_anchor) VALUES (?, ?, ?)`,
      [
        foundation.conversationId,
        foundation.anchorHash,
        copyBytes(foundation.canonicalAnchor),
      ],
    );
    yield* database.unsafe<Readonly<Record<string, unknown>>>(
      `INSERT INTO conversation_state
        (conversation_id, membership_hash, current_anchor_hash, head_ordinal)
       VALUES (?, ?, ?, -1)`,
      [
        foundation.conversationId,
        foundation.membershipHash,
        foundation.anchorHash,
      ],
    );
  }).pipe(storeOperation);
}
/**
 * Stage a re-anchor proposal. A proposal staged for this endpoint's vote
 * must not leave a head it holds a staged successor of; one adopted complete
 * from members carries no vote of this endpoint's, so it may.
 * @param reanchor Verified proposal body and stable hash.
 * @param purpose Whether this endpoint votes for it or adopts it complete.
 * @returns Whether the proposal was inserted or already durable.
 */
function stageReanchorInTransaction(
  reanchor: StagedReanchor,
  purpose: "vote" | "adopted",
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const existing = yield* findStagedReanchor(
      reanchor.conversationId,
      reanchor.anchorHash,
    );
    if (existing !== undefined) {
      requireSameReanchor(existing, reanchor);
      return "existing";
    }
    yield* requireUnclaimedReanchorScope(reanchor);
    yield* requireReanchorPosition(reanchor);
    if (purpose === "vote") {
      yield* requireNoStagedSuccessor(reanchor);
    }
    yield* insertStagedReanchor(reanchor);
    return "inserted";
  }).pipe(storeOperation);
}
function completeReanchorInTransaction(
  reanchor: CompletedReanchor,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const staged = yield* findStagedReanchor(
      reanchor.conversationId,
      reanchor.anchorHash,
    );
    if (staged === undefined) {
      return yield* new EndpointStoreError({ reason: "not-found" });
    }
    requireSameReanchor(staged, reanchor);
    if (yield* hasAnchor(reanchor.conversationId, reanchor.anchorHash)) {
      return "existing";
    }
    yield* requireReanchorPosition(reanchor);
    yield* insertCompletedReanchor(reanchor);
    yield* releaseSupersededProposal(reanchor);
    return "inserted";
  }).pipe(storeOperation);
}
/**
 * Retires the proposal locked at the head a completed re-anchor selects, with
 * everything held for it.
 *
 * An action binds its anchor, so once the new anchor is current no action
 * under the previous one is gap-free, and the old proposal can never be
 * signed, staged or certified here again. Its lock, kept, would refuse every
 * candidate at that head under the new anchor and stall the conversation.
 *
 * The staged record can be retired because the re-anchor's quorum
 * certificate shows it can never be certified anywhere: a member holding a
 * staged successor does not vote to re-anchor behind it, so the q(n) members
 * that selected this head leave at most 2f members that can attest the
 * successor's durability, fewer than q(n).
 *
 * @param reanchor Completed re-anchor that just became current.
 */
function releaseSupersededProposal(
  reanchor: CompletedReanchor,
): Effect.Effect<void, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const lock = yield* findProposalLock(
      reanchor.conversationId,
      reanchor.selectedRecordHash,
    );
    if (lock !== undefined) {
      yield* releaseProposalLock(lock);
    }
  }).pipe(storeOperation);
}
/**
 * Delete this endpoint's uncompleted candidates in a completed re-anchor's
 * scope that the completion does not select, with their re-anchor votes.
 * @param reanchor Verified completed re-anchor that supersedes them.
 */
function retireSupersededCandidates(
  reanchor: CompletedReanchor,
): Effect.Effect<void, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    const scope = [
      reanchor.conversationId,
      reanchor.previousAnchorHash,
      reanchor.routerInstanceId,
      reanchor.anchorHash,
    ] as const;
    const superseded = (yield* database.unsafe<
      Readonly<Record<string, unknown>>
    >(
      `SELECT anchor_hash FROM reanchors
       WHERE conversation_id = ? AND previous_anchor_hash = ?
         AND router_instance_id = ? AND anchor_hash <> ?
         AND canonical_completed_reanchor IS NULL`,
      [...scope],
    )).map((row) => readText(row, "anchor_hash"));
    for (const anchorHash of superseded) {
      yield* database.unsafe<Readonly<Record<string, unknown>>>(
        `DELETE FROM protocol_evidence
         WHERE conversation_id = ? AND evidence_kind = 'reanchor'
           AND subject_id = ?`,
        [reanchor.conversationId, anchorHash],
      );
    }
    yield* database.unsafe<Readonly<Record<string, unknown>>>(
      `DELETE FROM reanchors
       WHERE conversation_id = ? AND previous_anchor_hash = ?
         AND router_instance_id = ? AND anchor_hash <> ?
         AND canonical_completed_reanchor IS NULL`,
      [...scope],
    );
  }).pipe(storeOperation);
}
function requireUnclaimedReanchorScope(
  reanchor: StagedReanchor,
): Effect.Effect<void, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    const scoped = (yield* database.unsafe<Readonly<Record<string, unknown>>>(
      `SELECT anchor_hash FROM reanchors
       WHERE conversation_id = ? AND previous_anchor_hash = ?
         AND router_instance_id = ?`,
      [
        reanchor.conversationId,
        reanchor.previousAnchorHash,
        reanchor.routerInstanceId,
      ],
    ))[0];
    if (scoped !== undefined) {
      return yield* new EndpointStoreError({ reason: "conflict" });
    }
  }).pipe(storeOperation);
}
/**
 * Refuse a re-anchor away from a head this endpoint holds a staged successor
 * of under the same anchor. It voted that successor durable, so a re-anchor
 * away from the head and a durability certificate extending it never both
 * collect this endpoint's signature.
 * @param reanchor Proposal about to be staged.
 */
function requireNoStagedSuccessor(
  reanchor: StagedReanchor,
): Effect.Effect<void, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    const successor = (yield* database.unsafe<
      Readonly<Record<string, unknown>>
    >(
      `SELECT 1 AS staged FROM staged_records
       WHERE conversation_id = ? AND anchor_hash = ?
         AND previous_record_hash = ? LIMIT 1`,
      [
        reanchor.conversationId,
        reanchor.previousAnchorHash,
        reanchor.selectedRecordHash,
      ],
    ))[0];
    if (successor !== undefined) {
      return yield* new EndpointStoreError({ reason: "conflict" });
    }
  }).pipe(storeOperation);
}
function requireReanchorPosition(
  reanchor: StagedReanchor,
): Effect.Effect<void, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const position = yield* readStoredPosition(reanchor.conversationId);
    requireEqual(position.currentAnchorHash, reanchor.previousAnchorHash);
    requireEqual(position.headRecordHash, reanchor.selectedRecordHash);
  }).pipe(storeOperation);
}
function insertStagedReanchor(
  reanchor: StagedReanchor,
): Effect.Effect<void, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    yield* database.unsafe<Readonly<Record<string, unknown>>>(
      `INSERT INTO reanchors
        (conversation_id, anchor_hash, previous_anchor_hash,
         router_instance_id, selected_record_hash, canonical_body)
       VALUES (?, ?, ?, ?, ?, ?)`,
      [
        reanchor.conversationId,
        reanchor.anchorHash,
        reanchor.previousAnchorHash,
        reanchor.routerInstanceId,
        reanchor.selectedRecordHash,
        copyBytes(reanchor.canonicalBody),
      ],
    );
  }).pipe(storeOperation);
}
function insertCompletedReanchor(
  reanchor: CompletedReanchor,
): Effect.Effect<void, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    yield* database.unsafe<Readonly<Record<string, unknown>>>(
      `INSERT INTO anchors
        (conversation_id, anchor_hash, previous_anchor_hash,
         selected_record_hash, canonical_anchor)
       VALUES (?, ?, ?, ?, ?)`,
      [
        reanchor.conversationId,
        reanchor.anchorHash,
        reanchor.previousAnchorHash,
        reanchor.selectedRecordHash,
        copyBytes(reanchor.canonicalCompletedReanchor),
      ],
    );
    yield* database.unsafe<Readonly<Record<string, unknown>>>(
      `UPDATE reanchors SET canonical_completed_reanchor = ?
       WHERE conversation_id = ? AND anchor_hash = ?`,
      [
        copyBytes(reanchor.canonicalCompletedReanchor),
        reanchor.conversationId,
        reanchor.anchorHash,
      ],
    );
    yield* database.unsafe<Readonly<Record<string, unknown>>>(
      `UPDATE conversation_state SET current_anchor_hash = ?
       WHERE conversation_id = ?`,
      [reanchor.anchorHash, reanchor.conversationId],
    );
  }).pipe(storeOperation);
}
function hasAnchor(
  conversationId: string,
  anchorHash: string,
): Effect.Effect<boolean, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    return (
      (yield* database.unsafe<Readonly<Record<string, unknown>>>(
        `SELECT 1 AS retained FROM anchors
         WHERE conversation_id = ? AND anchor_hash = ?`,
        [conversationId, anchorHash],
      ))[0] !== undefined
    );
  }).pipe(storeOperation);
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
  expected: ConversationFoundation,
): Effect.Effect<void, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    const conversationId = expected.conversationId;
    yield* database.unsafe<Readonly<Record<string, unknown>>>(
      "UPDATE outbound_messages SET record_hash=NULL, disposition=CASE WHEN disposition='pending' THEN 'discarded' ELSE disposition END WHERE conversation_id=?",
      [conversationId],
    );
    yield* database.unsafe<Readonly<Record<string, unknown>>>(
      "DELETE FROM protocol_evidence WHERE conversation_id = ?",
      [conversationId],
    );
    yield* database.unsafe<Readonly<Record<string, unknown>>>(
      "DELETE FROM proposal_locks WHERE conversation_id = ?",
      [conversationId],
    );
    yield* database.unsafe<Readonly<Record<string, unknown>>>(
      "DELETE FROM staged_records WHERE conversation_id = ?",
      [conversationId],
    );
    yield* database.unsafe<Readonly<Record<string, unknown>>>(
      "DELETE FROM reanchors WHERE conversation_id = ?",
      [conversationId],
    );
    yield* database.unsafe<Readonly<Record<string, unknown>>>(
      `UPDATE outbound_messages SET disposition = 'discarded'
       WHERE conversation_id = ? AND disposition = 'pending'`,
      [conversationId],
    );
  }).pipe(storeOperation);
}
function installReplacementFoundation(
  replacement: ConversationFoundation,
): Effect.Effect<void, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    const retainedReplacement = (yield* database.unsafe<
      Readonly<Record<string, unknown>>
    >(
      `SELECT 1 AS retained FROM anchors
       WHERE conversation_id = ? AND anchor_hash = ?`,
      [replacement.conversationId, replacement.anchorHash],
    ))[0];
    if (retainedReplacement !== undefined) {
      return yield* new EndpointStoreError({ reason: "conflict" });
    }
    yield* database.unsafe<Readonly<Record<string, unknown>>>(
      `INSERT INTO anchors
        (conversation_id, anchor_hash, canonical_anchor) VALUES (?, ?, ?)`,
      [
        replacement.conversationId,
        replacement.anchorHash,
        copyBytes(replacement.canonicalAnchor),
      ],
    );
    yield* database.unsafe<Readonly<Record<string, unknown>>>(
      `UPDATE conversation_state
       SET current_anchor_hash = ?, head_record_hash = NULL, head_ordinal = -1
       WHERE conversation_id = ?`,
      [replacement.anchorHash, replacement.conversationId],
    );
    yield* database.unsafe<Readonly<Record<string, unknown>>>(
      `DELETE FROM anchors
       WHERE conversation_id = ? AND anchor_hash <> ?`,
      [replacement.conversationId, replacement.anchorHash],
    );
  }).pipe(storeOperation);
}
function readConversationMessageIntents(
  conversationId: string,
): Effect.Effect<
  readonly MessageIntent[],
  EndpointStoreError,
  SqlClient.SqlClient
> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    return Object.freeze(
      (yield* database.unsafe<Readonly<Record<string, unknown>>>(
        `SELECT conversation_id, membership_hash, author_agent_id, msg_id,
                canonical_intent, canonical_message, completed_record_hash
         FROM message_intents WHERE conversation_id = ? AND canonical_intent IS NOT NULL AND failure_reason IS NULL
         ORDER BY rowid`,
        [conversationId],
      )).map((row) => {
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
  }).pipe(storeOperation);
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
