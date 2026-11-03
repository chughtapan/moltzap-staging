/** @file Verified conversation records restore the engine and supply retained history. */

import type { SqlClient } from "@effect/sql/SqlClient";
import { Effect, type ParseResult, Schema } from "effect";
import type { AgentPublicKey } from "../../identity/keys.js";
import type {
  EndpointEngineInput,
  EngineActionFold,
  EngineCertifiedHead,
  EngineConversation,
  EngineRuntime,
} from "./conversation.js";
import {
  type ClientRepresentationError,
  decodeCanonical,
  encodeCanonical,
  representationFailure,
  sameBytes,
} from "../../encoding/canonical-json.js";
import { agentCardOf, type AgentId } from "../../identity/agent-cards.js";
import {
  SignedMessage,
  type SignedMessage as SignedMessageValue,
} from "../../identity/message-security.js";
import { compareAgentIds, Message, messageFromIntent } from "../messages.js";
import {
  type ActionCertifiedRecord,
  ActionCore,
  type ActionHash,
  ActionHash as ActionHashSchema,
  AnchorHash,
  type AnchorHash as AnchorHashValue,
  type CertifiedRecord,
  CompletedReanchor,
  type CompletedReanchor as CompletedReanchorValue,
  ConversationId,
  type ConversationId as ConversationIdValue,
  type EvidenceStatement,
  hashAnchor,
  hashRecord,
  InitialAnchorBody,
  type InitialAnchorBody as InitialAnchorBodyValue,
  MembershipDescriptor,
  MessageIntent,
  type RecordCore,
  RecordCore as RecordCoreSchema,
  RecordHash,
  type RecordHash as RecordHashValue,
  type RouterAnchor,
  type VerifiedEvidence,
  type VerifiedMembership,
  verifyActionCore,
  verifyCertifiedRecord,
  verifyCompletedReanchor,
  verifyMembershipDescriptor,
  verifyOuterMessage,
  verifyRecordCore,
  verifyStableEvidence,
} from "../records.js";
import {
  RouterWorkerPersistenceError,
  RouterWorkerRecoveryError,
} from "../router/connection.js";
import {
  type EndpointRecovery,
  OutboxTooOldError,
  type ProtocolEvidence,
  recoverStoredState,
  type StagedRecord,
  type CertifiedRecord as StoredCertifiedRecord,
  type StoredMembership,
  type MessageIntent as storedMessageIntent,
  type StoredOutboundMessage,
} from "../store.js";

/** A protocol fold selected by an evidence statement's signed subject. */
export type EvidenceRoute =
  | Readonly<{ fold: EngineActionFold; kind: "action" }>
  | Readonly<{ fold: EngineActionFold; kind: "durability" }>;

/**
 * The certified head binds the current anchor hash. Before certification,
 * the initial anchor is retained as an unhashed body.
 */
export const anchorHashAtHead = (
  conversation: EngineConversation,
  head: EngineCertifiedHead,
): AnchorHash =>
  conversation.currentAnchor.kind === "initial_anchor_body"
    ? head.record.actionCertifiedRecord.recordCore.anchorHash
    : conversation.currentAnchor.anchorHash;

/** Uses the immutable anchor captured when the action fold was created. */
export const recordAnchorHash = (
  fold: EngineActionFold,
): Effect.Effect<AnchorHash, ClientRepresentationError> =>
  fold.action.kind === "FIRST_MESSAGE"
    ? hashAnchor(fold.action.anchor)
    : Effect.succeed(fold.action.anchorHash);

const encodeOrderedEvidence = (
  evidence: ReadonlyMap<AgentId, SignedMessage>,
): Effect.Effect<CertificateSignatures, ClientRepresentationError> =>
  orderedSignatures(evidence.values()).pipe(
    Effect.mapError(representationFailure),
    Effect.flatMap((signatures) =>
      signatures === undefined
        ? Effect.fail(representationFailure())
        : Effect.succeed(signatures),
    ),
  );

/** Retains the inner signature's exact bytes as signer-attributed evidence. */
export const protocolEvidence = (
  conversationId: string,
  kind: ProtocolEvidence["kind"],
  subjectId: string,
  message: SignedMessage,
): Effect.Effect<ProtocolEvidence, ClientRepresentationError> =>
  encodeCanonical(SignedMessage, message).pipe(
    Effect.map((canonicalEvidence) => ({
      conversationId,
      kind,
      subjectId,
      evidenceKey: message.senderAgentId,
      canonicalEvidence,
    })),
  );

/** Builds the record core and action certificate from threshold-complete evidence. */
export const makeActionCertifiedRecord = (
  fold: EngineActionFold,
  anchorHash: RecordCore["anchorHash"],
): Effect.Effect<ActionCertifiedRecord, ClientRepresentationError> =>
  Effect.gen(function* () {
    const signatures = yield* encodeOrderedEvidence(fold.actionEvidence);
    const recordCore: RecordCore =
      fold.action.kind === "FIRST_MESSAGE"
        ? {
            sharnessVersion: fold.action.messageIntent.sharnessVersion,
            kind: "record_core",
            membership: fold.conversation.membership.descriptor,
            anchorHash,
            action: fold.action,
            actionHash: fold.actionHash,
          }
        : {
            sharnessVersion: fold.action.messageIntent.sharnessVersion,
            kind: "record_core",
            membershipHash: fold.conversation.membership.hash,
            anchorHash,
            action: fold.action,
            actionHash: fold.actionHash,
          };
    return actionCertifiedRecord(
      recordCore,
      yield* hashRecord(recordCore),
      fold.routerAnchor,
      signatures,
    );
  }).pipe(Effect.withSpan("makeActionCertifiedRecord"));

/** Adds durability evidence without changing record identity. */
export const makeCertifiedRecord = (
  record: ActionCertifiedRecord,
  fold: EngineActionFold,
): Effect.Effect<CertifiedRecord, ClientRepresentationError> =>
  encodeOrderedEvidence(fold.durabilityEvidence).pipe(
    Effect.map((votes) => certifiedRecord(record, votes)),
  );

/**
 * Convert one verified record core to the store's evidence-free staging row.
 * @param record Verified action-certified record from the protocol fold.
 * @returns Canonical evidence-free row used for atomic staging.
 */
export const stagedRecord = (
  record: ActionCertifiedRecord,
): Effect.Effect<StagedRecord, ClientRepresentationError> => {
  const action = record.recordCore.action;
  return encodeCanonical(RecordCoreSchema, record.recordCore).pipe(
    Effect.map((canonicalRecordCore) => ({
      conversationId: action.conversationId,
      recordHash: record.recordHash,
      ...(action.previousRecordHash === null
        ? {}
        : { previousRecordHash: action.previousRecordHash }),
      membershipHash:
        action.kind === "FIRST_MESSAGE"
          ? action.messageIntent.membershipHash
          : action.membershipHash,
      anchorHash: record.recordCore.anchorHash,
      actionHash: record.recordCore.actionHash,
      authorAgentId: action.messageIntent.authorAgentId,
      msgId: action.messageIntent.msgId,
      canonicalRecordCore,
    })),
  );
};

/** Keeps action and durability evidence separate in the promotion row. */
export const storedCertifiedRecord = (
  record: CertifiedRecord,
  actionSignatures: Iterable<SignedMessage>,
  durabilityVotes: Iterable<SignedMessage>,
): Effect.Effect<StoredCertifiedRecord, ClientRepresentationError> =>
  Effect.gen(function* () {
    const staged = yield* stagedRecord(record.actionCertifiedRecord);
    const actionEvidence = yield* Effect.forEach(
      actionSignatures,
      (message) =>
        protocolEvidence(
          staged.conversationId,
          "action",
          staged.actionHash,
          message,
        ),
      { concurrency: 1 },
    );
    const durabilityEvidence = yield* Effect.forEach(
      durabilityVotes,
      (message) =>
        protocolEvidence(
          staged.conversationId,
          "durability",
          staged.recordHash,
          message,
        ),
      { concurrency: 1 },
    );
    return { ...staged, actionEvidence, durabilityEvidence };
  }).pipe(Effect.withSpan("storedCertifiedRecord"));

/** The nonempty encoded signer messages one certificate carries. */
export type CertificateSignatures =
  ActionCertifiedRecord["actionCertificate"]["signatures"];

/** Why a stored row failed to decode or to match what it claims to hold. */
export type StoredRowError =
  | ClientRepresentationError
  | ParseResult.ParseError
  | RouterWorkerPersistenceError;

/**
 * Resolve the latest durable position for one fixed conversation.
 * @param conversationId Private conversation identity to locate.
 * @returns The conversation's recovery snapshot and its matching position row.
 */
export const durablePosition = (
  conversationId: ConversationIdValue,
): Effect.Effect<
  {
    readonly recovery: EndpointRecovery;
    readonly position?: EndpointRecovery["positions"][number];
  },
  RouterWorkerPersistenceError,
  SqlClient
> =>
  recoverStoredState(conversationId).pipe(
    Effect.mapError(persistenceFailure),
    Effect.map((recovery) => ({
      recovery,
      position: recovery.positions.find(
        (candidate) => candidate.conversationId === conversationId,
      ),
    })),
  );

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

/** Complete process-local projections recovered before protocol work resumes. */
interface RecoveredEngineState {
  readonly conversations: Map<ConversationIdValue, EngineConversation>;
  readonly actionFolds: Map<ActionHash, EngineActionFold>;
  readonly recordFolds: Map<RecordHashValue, EngineActionFold>;
  readonly completedMessages: Set<string>;
  /** Verified pending intents retain their durable acceptance order. */
  readonly messageIntents: ReadonlyArray<
    Effect.Effect.Success<ReturnType<typeof readStoredMessageIntent>>
  >;
  readonly outboundMessages: readonly StoredOutboundMessage[];
}

/** Reconstructs only state whose hashes, signatures and projected fields agree. */
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
    const completedMessages = new Set(
      messageIntents.flatMap((stored) =>
        stored.completedRecordHash === undefined ? [] : [stored.intent.msgId],
      ),
    );
    return {
      conversations,
      actionFolds,
      recordFolds,
      completedMessages,
      messageIntents: messageIntents.filter(
        (stored) => stored.completedRecordHash === undefined,
      ),
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

/**
 * Starts without evidence or queued signatures. The caller registers the
 * fold in `EngineRuntime.actionFolds` so certification can choose local evidence.
 */
export function makeActionFold(
  conversation: EngineConversation,
  action: ActionCore,
  actionHash: ActionHash,
  routerAnchor: RouterAnchor,
): EngineActionFold {
  return {
    conversation,
    action,
    actionHash,
    routerAnchor,
    actionEvidence: new Map(),
    durabilityEvidence: new Map(),
    localActionEvidenceQueued: false,
    localDurabilityEvidenceQueued: false,
    certified: false,
  };
}

/** Finds the named conversation and fold before verifying the statement's signature. */
export function evidenceTarget(
  runtime: EngineRuntime,
  statement: EvidenceStatement,
): {
  readonly conversationId?: ConversationId;
  readonly route?: EvidenceRoute;
} {
  switch (statement.kind) {
    case "action_signature": {
      const fold = runtime.actionFolds.get(statement.actionHash);
      return fold === undefined
        ? {}
        : {
            conversationId: fold.conversation.conversationId,
            route: { fold, kind: "action" },
          };
    }
    case "durability_vote": {
      const fold = runtime.recordFolds.get(statement.recordHash);
      return {
        conversationId: statement.conversationId,
        ...(fold === undefined
          ? {}
          : { route: { fold, kind: "durability" as const } }),
      };
    }
    case "reanchor_vote":
      return { conversationId: statement.reanchor.conversationId };
    case "catch_up_attestation":
      return { conversationId: statement.request.conversationId };
    default: {
      const exhaustive: never = statement;
      return exhaustive;
    }
  }
}

/** Checks fold-specific bindings after cryptographic verification. */
export function evidenceMatchesFold(
  route: EvidenceRoute,
  statement: VerifiedEvidence["statement"],
): boolean {
  const { fold } = route;
  switch (route.kind) {
    case "action":
      return (
        statement.kind === "action_signature" &&
        statement.actionHash === fold.actionHash
      );
    case "durability":
      if (
        statement.kind !== "durability_vote" ||
        fold.recordHash === undefined
      ) {
        return false;
      }
      return (
        statement.recordHash === fold.recordHash &&
        statement.conversationId === fold.conversation.conversationId &&
        statement.membershipHash === fold.conversation.membership.hash
      );
    default: {
      const exhaustive: never = route;
      return exhaustive;
    }
  }
}

/**
 * Orders signatures by decoded AgentId bytes, independent of map or store
 * order. Returns undefined when empty so callers choose the error.
 */
export function orderedSignatures(
  messages: Iterable<SignedMessage>,
): Effect.Effect<CertificateSignatures | undefined, ParseResult.ParseError> {
  const sorted = [...messages].sort((left, right) =>
    compareAgentIds(left.senderAgentId, right.senderAgentId),
  );
  return Effect.forEach(
    sorted,
    (message) => Schema.encode(SignedMessage)(message),
    { concurrency: 1 },
  ).pipe(
    Effect.map((encoded) => {
      const first = encoded[0];
      return first === undefined ? undefined : [first, ...encoded.slice(1)];
    }),
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
  registrySignerPublicKey: AgentPublicKey,
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
 * Applies startup's retained-history checks and verifies each membership once
 * per page. Preserves page order.
 */
export function readStoredHistory(
  registrySignerPublicKey: AgentPublicKey,
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
 * Verifies the descriptor against the Registry signer and its projected row
 * fields. Invalid representation differs from a persistence error caused by
 * columns disagreeing with their descriptor.
 */
export function verifyStoredMembership(
  stored: StoredMembership,
  registrySignerPublicKey: AgentPublicKey,
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

/** Verifies the record and its anchor, including agreement with projected row fields. */
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

/** Resolves the Router instance from the durable current anchor. */
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

/** Reads the instance bound by an initial or completed anchor. */
export function anchorRouterInstanceId(
  anchor: RouterAnchor,
): InitialAnchorBodyValue["routerInstanceId"] {
  return anchor.kind === "initial_anchor_body"
    ? anchor.routerInstanceId
    : anchor.reanchor.routerInstanceId;
}

/** Verifies a retained anchor before record reconstruction. */
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
 * Verifies canonical bytes and attribution before resuming unchanged outbox
 * rows in durable order.
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

/** Requires one complete, gap-free chain of records and anchors per conversation. */
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
 * Returns the uncertified staged successor. A proposal lock selects at most
 * one per predecessor and anchor. Omit `anchorHash` to search any anchor.
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

/** Includes the head itself when testing retained record ancestry. */
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

/** Includes the current anchor itself when testing retained anchor ancestry. */
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
function readStoredMessageIntent(
  stored: storedMessageIntent,
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
      membership.members.map(agentCardOf),
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
    membership.members.map(agentCardOf),
    record.actionCertifiedRecord.recordCore.action.messageIntent,
  ).pipe(
    Effect.map((message) => ({
      recordHash: record.actionCertifiedRecord.recordHash,
      message,
    })),
    Effect.mapError(persistenceFailure),
  );
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
            return Effect.fail(restorePersistenceFailure());
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
            return Effect.fail(restorePersistenceFailure());
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
          return yield* Effect.fail(restorePersistenceFailure());
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
          return yield* Effect.fail(restorePersistenceFailure());
        }
        const routerAnchor = anchorsByHash.get(verified.anchorHash);
        if (routerAnchor === undefined) {
          return yield* Effect.fail(restorePersistenceFailure());
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
          return yield* Effect.fail(restorePersistenceFailure());
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
          return yield* Effect.fail(restorePersistenceFailure());
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

/**
 * Requires evidence to name the record's conversation, subject and signer.
 * Verifies the assembled record's signatures once and checks projected store
 * fields against the record core.
 */
function recordFromStore(
  membership: VerifiedMembership,
  stored: StoredCertifiedRecord,
  routerAnchor: EngineConversation["currentAnchor"],
): Effect.Effect<CertifiedRecord, StoredRowError> {
  return assembleStoredRecord(stored, routerAnchor).pipe(
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
}

function storedRowMatchesCore(
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

function actionCertifiedRecord(
  recordCore: RecordCore,
  recordHash: RecordHash,
  routerAnchor: RouterAnchor,
  signatures: CertificateSignatures,
): ActionCertifiedRecord {
  return {
    sharnessVersion: recordCore.sharnessVersion,
    kind: "action_certified_record",
    recordHash,
    recordCore,
    routerAnchor,
    actionCertificate: {
      sharnessVersion: recordCore.sharnessVersion,
      kind: "action_certificate",
      actionHash: recordCore.actionHash,
      signatures,
    },
  };
}

/**
 * Add a durability certificate without changing the record hash.
 * @param record Action-certified record being completed.
 * @param votes Durability votes in certificate order.
 * @returns The complete certified record.
 */
function certifiedRecord(
  record: ActionCertifiedRecord,
  votes: CertificateSignatures,
): CertifiedRecord {
  return {
    sharnessVersion: record.recordCore.sharnessVersion,
    kind: "certified_record",
    actionCertifiedRecord: record,
    durabilityCertificate: {
      sharnessVersion: record.recordCore.sharnessVersion,
      kind: "durability_certificate",
      recordHash: record.recordHash,
      votes,
    },
  };
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

function persistenceFailure(): RouterWorkerPersistenceError {
  return new RouterWorkerPersistenceError();
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
        return Effect.fail(restorePersistenceFailure());
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

/** Restores only evidence whose signer, subject and conversation agree. */
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
            ? Effect.fail(restorePersistenceFailure())
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
        : Effect.fail(restorePersistenceFailure()),
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

function restorePersistenceFailure(): RouterWorkerPersistenceError {
  return new RouterWorkerPersistenceError();
}
