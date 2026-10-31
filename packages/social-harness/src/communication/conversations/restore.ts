/** @file Restores the conversation engine from verified local history when the daemon starts. */

import { Effect, type ParseResult, Schema } from "effect";
import type {
  EndpointRecovery,
  OutboxTooOldError,
  ProtocolEvidence,
  StoredOutboundMessage,
} from "../store/index.js";
import { decodeCanonical } from "../../encoding/canonical-json.js";
import {
  SignedMessage,
  type SignedMessage as SignedMessageValue,
} from "../../identity/envelope/signed-message.js";
import {
  ActionCore,
  type ActionHash,
  ActionHash as ActionHashSchema,
  AnchorHash,
  type AnchorHash as AnchorHashValue,
  type CertifiedRecord,
  ConversationId,
  type ConversationId as ConversationIdValue,
  type RecordCore,
  RecordCore as RecordCoreSchema,
  RecordHash,
  type RecordHash as RecordHashValue,
  type RouterAnchor,
  type VerifiedEvidence,
  type VerifiedMembership,
  verifyActionCore,
  verifyRecordCore,
  verifyStableEvidence,
} from "../records/index.js";
import { RouterWorkerPersistenceError } from "../router/index.js";
import {
  type EndpointEngineInput,
  type EngineActionFold,
  type EngineConversation,
  evidenceMatchesFold,
  type EvidenceRoute,
  makeActionFold,
} from "./engine-context.js";
import {
  decodeStoredAnchor,
  readStoredMessageIntent,
  recordFromStore,
  type StoredRowError,
  storedRowMatchesCore,
  verifyStoredMemberships,
  verifyStoredOutbounds,
} from "./history/index.js";

/** Complete process-local projections recovered before protocol work resumes. */
interface RecoveredEngineState {
  readonly conversations: Map<ConversationIdValue, EngineConversation>;
  readonly actionFolds: Map<ActionHash, EngineActionFold>;
  readonly recordFolds: Map<RecordHashValue, EngineActionFold>;
  readonly completedMessages: Map<string, RecordHashValue>;
  readonly messageIntents: ReadonlyArray<
    Effect.Effect.Success<ReturnType<typeof readStoredMessageIntent>>
  >;
  readonly outboundMessages: readonly StoredOutboundMessage[];
}

/**
 * Rebuild only state whose hashes, signatures, and cross-fields still verify.
 * @param input Engine dependencies used to verify persisted representation.
 * @param recovery Complete store snapshot captured before engine startup.
 * @returns Verified conversations, folds, and durable message intents.
 */
export const recoverEngineState = (
  input: EndpointEngineInput,
  recovery: EndpointRecovery,
): Effect.Effect<RecoveredEngineState, StoredRowError | OutboxTooOldError> =>
  Effect.gen(function* () {
    const verifiedMemberships = yield* verifyStoredMemberships(
      recovery.memberships,
      input.registrySignerPublicKey,
    );
    const anchors = yield* recoverEngineAnchors(recovery, verifiedMemberships);
    const conversations = yield* recoverConversations(
      recovery,
      verifiedMemberships,
      anchors.current,
    );
    const actionFolds = yield* recoverActionFolds(
      recovery,
      conversations,
      anchors.byHash,
    );
    const recordFolds = yield* recoverStagedFolds(recovery, actionFolds);
    const outboundMessages = yield* verifyStoredOutbounds(
      input,
      recovery.outboundMessages,
      verifiedMemberships,
    );
    yield* recoverFoldEvidence(recovery, actionFolds, recordFolds);
    yield* recoverCertifiedFolds({
      input,
      recovery,
      conversations,
      anchorsByHash: anchors.byHash,
      actionFolds,
      recordFolds,
    });
    const messageIntents = yield* Effect.forEach(
      recovery.messageIntents,
      (stored) => readStoredMessageIntent(stored, verifiedMemberships),
      { concurrency: 1 },
    );
    const completedMessages = new Map(
      messageIntents.flatMap((stored) =>
        stored.completedRecordHash === undefined
          ? []
          : [[stored.intent.msgId, stored.completedRecordHash] as const],
      ),
    );
    return {
      conversations,
      actionFolds,
      recordFolds,
      completedMessages,
      messageIntents,
      outboundMessages,
    };
  }).pipe(Effect.withSpan("recoverEngineState"));

interface RecoveredAnchors {
  readonly current: Map<
    ConversationIdValue,
    EngineConversation["currentAnchor"]
  >;
  readonly byHash: Map<AnchorHashValue, EngineConversation["currentAnchor"]>;
}

function recoverEngineAnchors(
  recovery: EndpointRecovery,
  memberships: ReadonlyMap<ConversationIdValue, VerifiedMembership>,
): Effect.Effect<RecoveredAnchors, StoredRowError> {
  return Effect.forEach(
    recovery.anchors,
    (stored) =>
      Schema.decodeUnknown(ConversationId)(stored.conversationId).pipe(
        Effect.flatMap((conversationId) => {
          const membership = memberships.get(conversationId);
          if (membership === undefined) {
            return Effect.fail(persistenceFailure());
          }
          return decodeStoredAnchor(membership, stored).pipe(
            Effect.zipWith(
              Schema.decodeUnknown(AnchorHash)(stored.anchorHash),
              (anchor, anchorHash) => ({
                stored,
                conversationId,
                anchorHash,
                anchor,
              }),
            ),
          );
        }),
      ),
    { concurrency: 1 },
  ).pipe(
    Effect.map((entries) => {
      const current = new Map<
        ConversationIdValue,
        EngineConversation["currentAnchor"]
      >();
      const byHash = new Map<
        AnchorHashValue,
        EngineConversation["currentAnchor"]
      >();
      for (const { stored, conversationId, anchorHash, anchor } of entries) {
        byHash.set(anchorHash, anchor);
        if (positionUsesAnchor(recovery, stored)) {
          current.set(conversationId, anchor);
        }
      }
      return { current, byHash };
    }),
  );
}

function positionUsesAnchor(
  recovery: EndpointRecovery,
  stored: EndpointRecovery["anchors"][number],
): boolean {
  return recovery.positions.some(
    (position) =>
      position.conversationId === stored.conversationId &&
      position.currentAnchorHash === stored.anchorHash,
  );
}

function recoverConversations(
  recovery: EndpointRecovery,
  memberships: ReadonlyMap<ConversationIdValue, VerifiedMembership>,
  anchors: ReadonlyMap<
    ConversationIdValue,
    EngineConversation["currentAnchor"]
  >,
) {
  return Effect.forEach(
    recovery.positions,
    (position) =>
      Schema.decodeUnknown(ConversationId)(position.conversationId).pipe(
        Effect.flatMap((conversationId) => {
          const membership = memberships.get(conversationId);
          const anchor = anchors.get(conversationId);
          if (membership === undefined || anchor === undefined) {
            return Effect.fail(persistenceFailure());
          }
          const conversation: EngineConversation = {
            conversationId,
            membership,
            currentAnchor: anchor,
          };
          const entry: readonly [ConversationIdValue, EngineConversation] = [
            conversationId,
            conversation,
          ];
          return Effect.succeed(entry);
        }),
      ),
    { concurrency: 1 },
  ).pipe(
    Effect.map(
      (entries) => new Map<ConversationIdValue, EngineConversation>(entries),
    ),
  );
}

function recoverActionFolds(
  recovery: EndpointRecovery,
  conversations: ReadonlyMap<ConversationIdValue, EngineConversation>,
  anchorsByHash: ReadonlyMap<AnchorHashValue, RouterAnchor>,
) {
  return Effect.forEach(
    recovery.proposalLocks,
    (lock) =>
      Effect.gen(function* () {
        const conversationId = yield* Schema.decodeUnknown(ConversationId)(
          lock.conversationId,
        );
        const conversation = conversations.get(conversationId);
        if (conversation === undefined) {
          return yield* Effect.fail(persistenceFailure());
        }
        const action = yield* decodeCanonical(
          ActionCore,
          lock.canonicalActionCore,
        );
        const actionHash = yield* Schema.decodeUnknown(ActionHashSchema)(
          lock.actionHash,
        );
        const verified = yield* verifyActionCore({
          action,
          membership: conversation.membership,
        });
        if (verified.actionHash !== actionHash) {
          return yield* Effect.fail(persistenceFailure());
        }
        const routerAnchor = anchorsByHash.get(verified.anchorHash);
        if (routerAnchor === undefined) {
          return yield* Effect.fail(persistenceFailure());
        }
        const entry: readonly [ActionHash, EngineActionFold] = [
          actionHash,
          makeActionFold(conversation, action, actionHash, routerAnchor),
        ];
        return entry;
      }),
    { concurrency: 1 },
  ).pipe(
    Effect.map((entries) => new Map<ActionHash, EngineActionFold>(entries)),
  );
}

function recoverStagedFolds(
  recovery: EndpointRecovery,
  actionFolds: Map<ActionHash, EngineActionFold>,
): Effect.Effect<Map<RecordHashValue, EngineActionFold>, StoredRowError> {
  return Effect.forEach(
    recovery.stagedRecords,
    (staged) =>
      Effect.gen(function* () {
        const actionHash = yield* Schema.decodeUnknown(ActionHashSchema)(
          staged.actionHash,
        );
        const recordHash = yield* Schema.decodeUnknown(RecordHash)(
          staged.recordHash,
        );
        const fold = actionFolds.get(actionHash);
        if (fold === undefined) {
          return yield* Effect.fail(persistenceFailure());
        }
        const recordCore = yield* decodeCanonical(
          RecordCoreSchema,
          staged.canonicalRecordCore,
        );
        const verifiedHash = yield* verifyRecordCore({
          recordCore,
          membership: fold.conversation.membership,
        });
        if (!stagedRecordMatches(staged, fold, recordCore, verifiedHash)) {
          return yield* Effect.fail(persistenceFailure());
        }
        yield* Effect.sync(() => {
          fold.recordHash = recordHash;
        });
        const entry: readonly [RecordHashValue, EngineActionFold] = [
          recordHash,
          fold,
        ];
        return entry;
      }),
    { concurrency: 1 },
  ).pipe(
    Effect.map(
      (entries) => new Map<RecordHashValue, EngineActionFold>(entries),
    ),
  );
}

function stagedRecordMatches(
  staged: EndpointRecovery["stagedRecords"][number],
  fold: EngineActionFold,
  recordCore: RecordCore,
  recordHash: RecordHashValue,
): boolean {
  return (
    storedRowMatchesCore(staged, recordCore, fold.conversation.membership) &&
    recordCore.actionHash === fold.actionHash &&
    staged.recordHash === recordHash
  );
}

interface CertifiedFoldRecoveryInput {
  readonly input: EndpointEngineInput;
  readonly recovery: EndpointRecovery;
  readonly conversations: Map<ConversationIdValue, EngineConversation>;
  readonly anchorsByHash: ReadonlyMap<
    AnchorHashValue,
    EngineConversation["currentAnchor"]
  >;
  readonly actionFolds: Map<ActionHash, EngineActionFold>;
  readonly recordFolds: Map<RecordHashValue, EngineActionFold>;
}

function recoverCertifiedFolds(
  input: CertifiedFoldRecoveryInput,
): Effect.Effect<void, StoredRowError> {
  return Effect.forEach(
    input.recovery.certifiedRecords,
    (stored) => recoverCertifiedFold(input, stored),
    { concurrency: 1, discard: true },
  );
}

function recoverCertifiedFold(
  input: CertifiedFoldRecoveryInput,
  stored: EndpointRecovery["certifiedRecords"][number],
): Effect.Effect<void, StoredRowError> {
  return Effect.gen(function* () {
    const context = yield* certifiedFoldContext(input, stored);
    const record = yield* recordFromStore(
      context.conversation.membership,
      stored,
      context.routerAnchor,
    );
    const actionHash = record.actionCertifiedRecord.recordCore.actionHash;
    const actionEvidence = yield* certificateMessages(
      record.actionCertifiedRecord.actionCertificate.signatures,
    );
    const durabilityEvidence = yield* certificateMessages(
      record.durabilityCertificate.votes,
    );
    yield* Effect.sync(() => {
      const fold =
        input.actionFolds.get(actionHash) ??
        makeActionFold(
          context.conversation,
          record.actionCertifiedRecord.recordCore.action,
          actionHash,
          context.routerAnchor,
        );
      input.actionFolds.set(actionHash, fold);
      completeRecoveredFold(fold, record, actionEvidence, durabilityEvidence);
      input.recordFolds.set(record.actionCertifiedRecord.recordHash, fold);
      context.conversation.head = {
        recordHash: record.actionCertifiedRecord.recordHash,
        record,
      };
    });
  });
}

function certificateMessages(
  representations: readonly unknown[],
): Effect.Effect<readonly SignedMessageValue[], ParseResult.ParseError> {
  return Effect.forEach(
    representations,
    (representation) => Schema.decodeUnknown(SignedMessage)(representation),
    { concurrency: 1 },
  );
}

function certifiedFoldContext(
  input: CertifiedFoldRecoveryInput,
  stored: EndpointRecovery["certifiedRecords"][number],
) {
  return Effect.all({
    conversationId: Schema.decodeUnknown(ConversationId)(stored.conversationId),
    anchorHash: Schema.decodeUnknown(AnchorHash)(stored.anchorHash),
  }).pipe(
    Effect.flatMap(({ conversationId, anchorHash }) => {
      const conversation = input.conversations.get(conversationId);
      const routerAnchor = input.anchorsByHash.get(anchorHash);
      if (conversation === undefined || routerAnchor === undefined) {
        return Effect.fail(persistenceFailure());
      }
      return Effect.succeed({ conversation, routerAnchor });
    }),
  );
}

function completeRecoveredFold(
  fold: EngineActionFold,
  record: CertifiedRecord,
  actionEvidence: readonly SignedMessageValue[],
  durabilityEvidence: readonly SignedMessageValue[],
): void {
  for (const message of actionEvidence) {
    fold.actionEvidence.set(message.senderAgentId, message);
  }
  for (const message of durabilityEvidence) {
    fold.durabilityEvidence.set(message.senderAgentId, message);
  }
  fold.recordHash = record.actionCertifiedRecord.recordHash;
  fold.certified = true;
}

/**
 * Restore only evidence whose signer, subject, and conversation still match.
 * @param recovery Complete endpoint-store snapshot.
 * @param actionFolds Active folds indexed by verified action hash.
 * @param recordFolds Active folds indexed by verified record hash.
 * @returns Completion after every relevant row is verified and installed.
 */
function recoverFoldEvidence(
  recovery: EndpointRecovery,
  actionFolds: Map<ActionHash, EngineActionFold>,
  recordFolds: Map<RecordHashValue, EngineActionFold>,
): Effect.Effect<void, StoredRowError> {
  return Effect.forEach(
    recovery.evidence,
    (row) => {
      if (row.kind === "catch-up" || row.kind === "reanchor") {
        return Effect.void;
      }
      return foldForEvidence(row, actionFolds, recordFolds).pipe(
        Effect.flatMap((target) =>
          target === undefined
            ? Effect.fail(persistenceFailure())
            : restoreFoldEvidence(row, target),
        ),
      );
    },
    { concurrency: 1, discard: true },
  );
}

function foldForEvidence(
  row: ProtocolEvidence,
  actionFolds: Map<ActionHash, EngineActionFold>,
  recordFolds: Map<RecordHashValue, EngineActionFold>,
): Effect.Effect<EvidenceRoute | undefined, StoredRowError> {
  switch (row.kind) {
    case "action":
      return Schema.decodeUnknown(ActionHashSchema)(row.subjectId).pipe(
        Effect.map((hash) => actionFolds.get(hash)),
        Effect.map((fold) =>
          fold === undefined ? undefined : { fold, kind: "action" },
        ),
      );
    case "durability":
      return Schema.decodeUnknown(RecordHash)(row.subjectId).pipe(
        Effect.map((hash) => recordFolds.get(hash)),
        Effect.map((fold) =>
          fold === undefined ? undefined : { fold, kind: "durability" },
        ),
      );
    case "catch-up":
    case "reanchor":
      return Effect.succeed(undefined);
    default: {
      const exhaustive: never = row.kind;
      return exhaustive;
    }
  }
}

function restoreFoldEvidence(
  row: ProtocolEvidence,
  target: EvidenceRoute,
): Effect.Effect<void, StoredRowError> {
  return decodeCanonical(SignedMessage, row.canonicalEvidence).pipe(
    Effect.flatMap((message) => Schema.encode(SignedMessage)(message)),
    Effect.flatMap((representation) =>
      verifyStableEvidence({
        representation,
        membership: target.fold.conversation.membership,
      }),
    ),
    Effect.flatMap((verified) =>
      foldEvidenceMatches(row, target, verified.statement)
        ? Effect.sync(() => {
            const evidence =
              target.kind === "action"
                ? target.fold.actionEvidence
                : target.fold.durabilityEvidence;
            evidence.set(verified.message.senderAgentId, verified.message);
          })
        : Effect.fail(persistenceFailure()),
    ),
  );
}

function foldEvidenceMatches(
  row: ProtocolEvidence,
  target: EvidenceRoute,
  statement: VerifiedEvidence["statement"],
): boolean {
  const subjectId =
    target.kind === "action" ? target.fold.actionHash : target.fold.recordHash;
  return (
    row.conversationId === target.fold.conversation.conversationId &&
    row.evidenceKey === statement.signerAgentId &&
    row.subjectId === subjectId &&
    evidenceMatchesFold(target, statement)
  );
}

function persistenceFailure(): RouterWorkerPersistenceError {
  return new RouterWorkerPersistenceError();
}
