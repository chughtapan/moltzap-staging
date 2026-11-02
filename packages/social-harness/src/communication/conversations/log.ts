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
} from "./service.js";
import {
  type ClientRepresentationError,
  decodeCanonical,
  encodeCanonical,
  representationFailure,
  sameBytes,
} from "../../encoding/canonical-json.js";
import { agentCardOf, type AgentId } from "../../identity/agent.js";
import {
  SignedMessage,
  type SignedMessage as SignedMessageValue,
} from "../../identity/envelope.js";
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
} from "../router/worker.js";
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
 * The hash of a conversation's current anchor, which its next record binds.
 * A conversation still on its initial anchor holds the anchor body unhashed,
 * and every record since that anchor, the head included, binds its hash.
 * @param conversation The conversation whose anchor is current.
 * @param head The conversation's certified head.
 * @returns The current anchor's hash.
 */
export const anchorHashAtHead = (
  conversation: EngineConversation,
  head: EngineCertifiedHead,
): AnchorHash =>
  conversation.currentAnchor.kind === "initial_anchor_body"
    ? head.record.actionCertifiedRecord.recordCore.anchorHash
    : conversation.currentAnchor.anchorHash;

/**
 * Resolve the immutable Router anchor bound when one action fold is created.
 * @param fold Selected action and its action-specific Router anchor.
 * @returns The canonical anchor hash committed by the record core.
 */
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

/**
 * Convert one verified inner signature into its durable evidence row.
 * @param conversationId Private conversation that owns the evidence.
 * @param kind Evidence statement family retained by the store.
 * @param subjectId Hash named by the evidence statement.
 * @param message Verified self-addressed evidence message.
 * @returns Canonical durable evidence without changing its signer bytes.
 */
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

/**
 * Build the action-certified record after its threshold is reached.
 * @param fold In-memory fold containing verified action evidence.
 * @param anchorHash Completed anchor bound by the record core.
 * @returns One evidence-independent record core with its action certificate.
 */
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

/**
 * Add a durability certificate without changing the certified record hash.
 * @param record Action-certified core being finalized.
 * @param fold In-memory fold containing verified durability evidence.
 * @returns One complete certified record with mergeable durability votes.
 */
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

/**
 * Convert a complete wire record into one complete store promotion row.
 * @param record Complete certified wire record.
 * @param actionSignatures Verified action signatures stored with the record.
 * @param durabilityVotes Verified durability votes stored with the record.
 * @returns Store promotion data with separate action and durability evidence.
 */
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
 * Open an empty fold for one action: no evidence, and no local signature
 * queued yet, so certification decides what to sign and send.
 * @param conversation Conversation whose next position the action proposes.
 * @param action Selected action the fold collects evidence for.
 * @param actionHash Hash that action and durability evidence name.
 * @param routerAnchor Anchor the action's record certifies against.
 * @returns A fold the caller registers in `EngineRuntime.actionFolds`.
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

/**
 * Resolve evidence once for both recovery fencing and active certification.
 * @param runtime Engine holding the active folds.
 * @param statement Decoded statement whose signature has not yet been checked.
 * @returns Its named conversation and active fold, when known.
 */
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

/**
 * Bind verified evidence to the active fold selected by its signed subject.
 * @param route Active fold and the certificate role the statement targets.
 * @param statement Cryptographically verified evidence statement.
 * @returns Whether all fold-specific bindings match.
 */
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
 * Encode signer messages in decoded-AgentId byte order, which a certificate
 * requires and neither a fold's map nor the store's key order gives.
 * @param messages Verified signer messages, in any order.
 * @returns The encoded messages, or `undefined` when there are none; each
 *   caller reports an empty certificate in its own error.
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
 * Read a page of stored certified history for the owner tools, with the same
 * row checks startup and recovery apply. Each membership the page names is
 * verified once, however many of its records the page holds.
 * @param registrySignerPublicKey Registry key the member cards verify under.
 * @param history Retained memberships and anchors named by these records.
 * @param records Durable record rows, in page order.
 * @returns The verified messages and their history positions, in page order.
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

/**
 * Check that a stored record row's columns name the record core it holds.
 * @param row Staged or certified record row.
 * @param core The row's decoded record core.
 * @param membership Verified membership of the row's conversation.
 * @returns Whether every projected column matches.
 */
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

/**
 * Assemble an action-certified record from its verified parts.
 * @param recordCore Record core the action certificate certifies.
 * @param recordHash Hash of `recordCore`.
 * @param routerAnchor Anchor the record core commits to.
 * @param signatures Action signatures in certificate order.
 * @returns The record, whose certificate names the core's action hash.
 */
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
