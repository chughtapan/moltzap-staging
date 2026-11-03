/** @file Retains conversation protocol state, exact outbound envelopes and request results atomically. */

import { SqlClient } from "@effect/sql/SqlClient";
import { Cause, Data, Effect, Option, Schema } from "effect";
import {
  canonicalBytes,
  decodeCanonical,
  encodeCanonical,
  sameBytes,
} from "../encoding/canonical-json.js";
import { AgentId, AgentIdentityCard } from "../identity/agent-cards.js";
import { type SendFailure, sendFailureReasons } from "./errors.js";
import {
  CollectiveId,
  type CollectiveValue,
  conversationSortKey,
  maximumMembers,
  Message,
  messageInterpretation,
  MessageReference,
  Request,
  ResultDelivery,
} from "./messages.js";
import { MembershipDescriptor } from "./records.js";

/** One closed endpoint-store failure. */
export class EndpointStoreError extends Data.TaggedError("EndpointStoreError")<{
  readonly reason: EndpointStoreFailure;
}> {}

/** Pending signed bytes require an earlier release to drain the outbox. */
export class OutboxTooOldError extends Data.TaggedError("OutboxTooOldError") {}

/** Internal control-flow signal mapped to the closed store error. */
class StoreSignal extends Error {
  readonly reason: EndpointStoreFailure;
  constructor(reason: EndpointStoreFailure) {
    super(reason);
    this.reason = reason;
  }
}

/** Only an empty database or the current stored format can be opened. */
export type StoreOpening = "create" | "reopen";

const SCHEMA_VERSION = 22;

const collectiveSchemaSql = `
  CREATE TABLE collective_operations (
    operation_id TEXT PRIMARY KEY,
    creator_agent_id TEXT NOT NULL,
    origin TEXT NOT NULL CHECK(origin IN ('local','remote')),
    canonical_request BLOB NOT NULL,
    deadline_at INTEGER NOT NULL,
    phase TEXT NOT NULL DEFAULT 'collecting' CHECK(phase IN ('collecting','finalizing','completed')),
    canonical_close BLOB,
    canonical_result BLOB,
    canonical_answer_refs BLOB,
    delivered INTEGER NOT NULL DEFAULT 0 CHECK(delivered IN (0,1))
  ) STRICT;
  CREATE TABLE collective_messages (
    operation_id TEXT NOT NULL REFERENCES collective_operations(operation_id),
    role TEXT NOT NULL CHECK(role IN ('request','response','close')),
    destination TEXT NOT NULL,
    author_agent_id TEXT NOT NULL,
    msg_id TEXT NOT NULL,
    PRIMARY KEY(operation_id,role,destination),
    UNIQUE(author_agent_id,msg_id)
  ) STRICT;
  CREATE UNIQUE INDEX collective_one_response ON collective_messages(operation_id) WHERE role='response';
  CREATE UNIQUE INDEX collective_one_close ON collective_messages(operation_id) WHERE role='close';
  CREATE INDEX collective_pending_results ON collective_operations(delivered) WHERE phase='completed' AND delivered=0;
  CREATE TABLE collective_answers (
    operation_id TEXT NOT NULL REFERENCES collective_operations(operation_id),
    member TEXT NOT NULL,
    canonical_message BLOB NOT NULL,
    PRIMARY KEY(operation_id,member)
  ) STRICT;
`;

const schemaSql = `
  CREATE TABLE message_intents (
    author_agent_id TEXT NOT NULL,
    msg_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    membership_hash TEXT,
    canonical_message BLOB NOT NULL,
    canonical_intent BLOB,
    completed_record_hash TEXT,
    failure_reason TEXT,
    PRIMARY KEY (author_agent_id, msg_id),
    FOREIGN KEY (conversation_id, membership_hash)
      REFERENCES memberships(conversation_id, membership_hash)
  ) STRICT;
  CREATE INDEX message_intents_unresolved ON message_intents(conversation_id)
    WHERE completed_record_hash IS NULL;
  CREATE TABLE memberships (
    conversation_id TEXT PRIMARY KEY,
    membership_hash TEXT NOT NULL,
    canonical_membership BLOB NOT NULL,
    sort_key TEXT NOT NULL UNIQUE,
    UNIQUE (conversation_id, membership_hash)
  ) STRICT;
  CREATE TABLE anchors (
    conversation_id TEXT NOT NULL REFERENCES memberships(conversation_id),
    anchor_hash TEXT NOT NULL,
    previous_anchor_hash TEXT,
    selected_record_hash TEXT,
    canonical_anchor BLOB NOT NULL,
    PRIMARY KEY (conversation_id, anchor_hash)
  ) STRICT;
  CREATE TABLE conversation_state (
    conversation_id TEXT PRIMARY KEY REFERENCES memberships(conversation_id),
    membership_hash TEXT NOT NULL,
    current_anchor_hash TEXT NOT NULL,
    head_record_hash TEXT,
    head_ordinal INTEGER NOT NULL CHECK (head_ordinal >= -1),
    FOREIGN KEY (conversation_id, current_anchor_hash)
      REFERENCES anchors(conversation_id, anchor_hash)
  ) STRICT;
  CREATE TABLE proposal_locks (
    conversation_id TEXT NOT NULL REFERENCES memberships(conversation_id),
    predecessor_key TEXT NOT NULL,
    previous_record_hash TEXT,
    action_hash TEXT NOT NULL,
    canonical_action_core BLOB NOT NULL,
    PRIMARY KEY (conversation_id, predecessor_key),
    CHECK (
      (previous_record_hash IS NULL AND predecessor_key = '') OR
      (previous_record_hash IS NOT NULL AND predecessor_key = previous_record_hash)
    )
  ) STRICT;
  CREATE TABLE staged_records (
    conversation_id TEXT NOT NULL REFERENCES memberships(conversation_id),
    record_hash TEXT NOT NULL,
    previous_record_hash TEXT,
    membership_hash TEXT NOT NULL,
    anchor_hash TEXT NOT NULL,
    action_hash TEXT NOT NULL,
    author_agent_id TEXT NOT NULL,
    msg_id TEXT NOT NULL,
    canonical_record_core BLOB NOT NULL,
    PRIMARY KEY (conversation_id, record_hash),
    FOREIGN KEY (conversation_id, anchor_hash)
      REFERENCES anchors(conversation_id, anchor_hash)
  ) STRICT;
  CREATE TABLE protocol_evidence (
    conversation_id TEXT NOT NULL REFERENCES memberships(conversation_id),
    evidence_kind TEXT NOT NULL CHECK (
      evidence_kind IN ('action', 'durability', 'catch-up', 'reanchor')
    ),
    subject_id TEXT NOT NULL,
    evidence_key TEXT NOT NULL,
    canonical_evidence BLOB NOT NULL,
    PRIMARY KEY (conversation_id, evidence_kind, subject_id, evidence_key)
  ) STRICT;
  CREATE TABLE certified_records (
    conversation_id TEXT NOT NULL,
    record_hash TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    consumed INTEGER NOT NULL DEFAULT 0 CHECK (consumed IN (0,1)),
    PRIMARY KEY (conversation_id, record_hash),
    UNIQUE (conversation_id, ordinal),
    FOREIGN KEY (conversation_id, record_hash)
      REFERENCES staged_records(conversation_id, record_hash)
  ) STRICT;
  CREATE INDEX certified_records_by_hash ON certified_records(record_hash);
  CREATE TABLE reanchors (
    conversation_id TEXT NOT NULL REFERENCES memberships(conversation_id),
    anchor_hash TEXT NOT NULL,
    previous_anchor_hash TEXT NOT NULL,
    router_instance_id TEXT NOT NULL,
    selected_record_hash TEXT NOT NULL,
    canonical_body BLOB NOT NULL,
    canonical_completed_reanchor BLOB,
    PRIMARY KEY (conversation_id, anchor_hash),
    UNIQUE (conversation_id, previous_anchor_hash, router_instance_id),
    FOREIGN KEY (conversation_id, previous_anchor_hash)
      REFERENCES anchors(conversation_id, anchor_hash),
    FOREIGN KEY (conversation_id, selected_record_hash)
      REFERENCES certified_records(conversation_id, record_hash)
  ) STRICT;
  CREATE TABLE outbound_messages (
    outbound_sequence INTEGER PRIMARY KEY,
    conversation_id TEXT NOT NULL REFERENCES memberships(conversation_id),
    envelope_id TEXT NOT NULL UNIQUE,
    record_hash TEXT,
    canonical_signed_message BLOB NOT NULL,
    wire_version TEXT NOT NULL,
    attempted INTEGER NOT NULL CHECK (attempted IN (0, 1)),
    disposition TEXT NOT NULL CHECK (
      disposition IN ('pending', 'accepted', 'discarded')
    ),
    FOREIGN KEY (conversation_id, record_hash) REFERENCES staged_records(conversation_id, record_hash)
  ) STRICT;
  CREATE UNIQUE INDEX outbound_current_record ON outbound_messages(conversation_id,record_hash)
    WHERE record_hash IS NOT NULL AND disposition<>'discarded';
`;

/** Closed storage failures that never expose SQLite diagnostics. */
export type EndpointStoreFailure =
  | "closed"
  | "conflict"
  | "corrupt"
  | "incompatible"
  | "invalid-continuation"
  | "invalid-input"
  | "not-found"
  | "persistence";

/** Whether an idempotent mutation inserted state or observed the same state. */
export type StoreMutation = "inserted" | "existing";

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

/** Certified records bounded by the conversation head captured for the first page. */
export interface HistoryPage {
  readonly records: readonly CertifiedRecord[];
  readonly memberships: readonly StoredMembership[];
  readonly anchors: readonly StoredAnchor[];
  readonly continuation: string | null;
}

/** The predecessor key of a conversation's first record. */
const FIRST_MESSAGE_PREDECESSOR = "";

/** Durable position plus the local indexing aid that carries no authority. */
interface InternalPosition extends ConversationPosition {
  readonly headOrdinal: number;
}

const decoder = new TextDecoder("utf-8", { fatal: true });

/**
 * Encode immutable runtime values before they enter the endpoint store.
 * @param value Validated runtime value without process resources.
 * @returns Canonical bytes or a closed persistence failure.
 */
export const encodeRuntimeValue = (
  value: unknown,
): Effect.Effect<Uint8Array, EndpointStoreError> =>
  canonicalBytes(value).pipe(
    Effect.catchTag("ClientRepresentationError", () =>
      Effect.fail(new EndpointStoreError({ reason: "invalid-input" })),
    ),
  );

/**
 * Validate stored bytes at their owning runtime boundary before reuse.
 * @param schema Closed schema of the stored runtime value.
 * @param bytes Retained canonical UTF-8 bytes.
 * @returns The decoded value or a closed corruption failure.
 */
const decodeRuntimeValue = <A, I>(
  schema: Schema.Schema<A, I>,
  bytes: Uint8Array,
): Effect.Effect<A, EndpointStoreError> =>
  Effect.try({
    try: () => decoder.decode(bytes),
    catch: () => new EndpointStoreError({ reason: "corrupt" }),
  }).pipe(
    Effect.flatMap(Schema.decodeUnknown(Schema.parseJson(schema))),
    Effect.catchTag("ParseError", () =>
      Effect.fail(new EndpointStoreError({ reason: "corrupt" })),
    ),
  );

const HISTORY_PAGE_SIZE = 50;

const historySelect = `
  SELECT certified.rowid AS sequence, staged.conversation_id, staged.record_hash,
         staged.previous_record_hash, staged.membership_hash,
         staged.anchor_hash, staged.action_hash, staged.author_agent_id,
         staged.msg_id, staged.canonical_record_core
  FROM certified_records AS certified
  JOIN staged_records AS staged
    ON staged.conversation_id = certified.conversation_id
   AND staged.record_hash = certified.record_hash
`;

const historyCursor = Schema.Struct({
  conversationId: Schema.NonEmptyString,
  afterRecordHash: Schema.NonEmptyString,
  throughRecordHash: Schema.NonEmptyString,
});

const encodeCursor = Schema.encodeSync(Schema.parseJson(historyCursor));

const decodeCursor = Schema.decodeUnknown(Schema.parseJson(historyCursor), {
  onExcessProperty: "error",
});

type OutboundDisposition = "accepted" | "discarded" | "pending";
interface RetainedOutbound {
  readonly outbound: StoredOutboundMessage;
  readonly attempted: boolean;
  readonly disposition: OutboundDisposition;
}

type OperationValue = Extract<CollectiveValue, { readonly kind: "operation" }>;
type CloseValue = Extract<CollectiveValue, { readonly kind: "close" }>;

interface OperationState {
  readonly operationId: CollectiveId;
  readonly deadlineAt: number;
  readonly phase: "collecting" | "finalizing" | "completed";
  readonly close?: CloseValue;
  readonly result?: {
    readonly item: ResultDelivery["item"];
    readonly answers: readonly MessageReference[];
  };
}

/** Disk readers distinguish local captured questions from recognized peer requests. */
export type StoredCollectiveOperation = OperationState &
  (
    | { readonly origin: "local"; readonly request: Request }
    | {
        readonly origin: "remote";
        readonly request: {
          readonly message: Message;
          readonly value: OperationValue;
        };
      }
  );

/**
 * Communication retains the accepted Message. A collective retains its role
 * and identity.
 */
export interface StoredCollectiveMessage {
  readonly operationId: CollectiveId;
  readonly role: "request" | "response" | "close";
  readonly destination: string;
  readonly reference: MessageReference;
}

/** Results become available only after their required close certifies. */
export const readNextCollectiveResult = () =>
  readOperations(
    " WHERE phase='completed' AND delivered=0 AND canonical_result IS NOT NULL ORDER BY rowid LIMIT 1",
    [],
  ).pipe(
    Effect.map((rows) => rows[0]),
    Effect.withSpan("readNextCollectiveResult"),
  );

/** Each construction checks the captured agent before touching retained protocol work. */
export function initializeCommunication(identity: {
  readonly agentId: string;
}) {
  return transaction(
    Effect.gen(function* () {
      const opening = yield* inspectStoreState();
      const sql = yield* SqlClient;
      if (opening === "create") {
        yield* createCommunicationTables();
        yield* sql.unsafe(
          "CREATE TABLE communication_metadata(singleton INTEGER PRIMARY KEY CHECK(singleton=1), version INTEGER NOT NULL, agent_id TEXT NOT NULL) STRICT",
        );
        yield* sql.unsafe(
          "INSERT INTO communication_metadata(singleton,version,agent_id) VALUES(1,?,?)",
          [SCHEMA_VERSION, identity.agentId],
        );
      } else {
        const [row] = yield* sql.unsafe<{ readonly agentId: string }>(
          'SELECT agent_id AS "agentId" FROM communication_metadata WHERE singleton=1',
        );
        if (row?.agentId !== identity.agentId) {
          return yield* new EndpointStoreError({ reason: "conflict" });
        }
      }
    }),
  ).pipe(Effect.withSpan("initializeCommunication"));
}

/**
 * Whether a store error refuses the input itself, because it conflicts with
 * durable state or is invalid, rather than reporting a failed store.
 * @param error A closed endpoint-store error.
 * @returns Whether the store refused the input.
 */
export function isSemanticStoreRejection(error: EndpointStoreError): boolean {
  return error.reason === "conflict" || error.reason === "invalid-input";
}

/**
 * Durably binds one author-scoped message to its immutable canonical intent.
 *
 * @param binding Canonical message recovery identity and optional new foundation.
 * @returns Whether the same binding was inserted or already durable.
 */
export function bindMessageIntent(
  binding: MessageIntentBinding,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient> {
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
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient> {
  return transaction(putConversationFoundationInTransaction(foundation)).pipe(
    storeOperation,
  );
}

/**
 * Selects the first valid Router-ordered action durably. Sign only after
 * success to prevent conflicting signatures across restart. Returns inserted
 * or an identical existing lock.
 */
export function lockProposal(
  proposal: ProposalLock,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient> {
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
 * Requires the exact old foundation and an empty certified head, protecting
 * committed history and concurrent anchor changes. Returns the replacement
 * and retained immutable intents.
 */
export function restartEmptyConversation(
  restart: EmptyConversationRestart,
): Effect.Effect<RestartedEmptyConversation, EndpointStoreError, SqlClient> {
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
 * Stages one candidate per Router-instance scope for a local vote. Refuses
 * to leave a head with a staged successor under that anchor.
 */
export function stageReanchor(
  reanchor: StagedReanchor,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient> {
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
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    validateCompletedReanchor(reanchor);
    return yield* transaction(completeReanchorInTransaction(reanchor));
  }).pipe(storeOperation, Effect.withSpan("completeReanchor"));
}

/**
 * Adopts a quorum-certified anchor even over a competing staged candidate.
 * The rule requiring one candidate limits local votes because two certificates
 * in one scope would require an honest member to vote twice.
 */
export function applyCatchUpReanchor(
  reanchor: CompletedReanchor,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient> {
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

/**
 * Recovers all durable endpoint state needed to resume protocol work.
 *
 * @param conversationId Restrict protocol rows to one conversation when provided.
 * @returns A detached snapshot of the selected durable endpoint protocol state.
 */
export function recoverStoredState(
  conversationId?: string,
): Effect.Effect<EndpointRecovery, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    const identity = yield* readStoredIdentity();
    return Object.freeze({
      ...(identity === undefined ? {} : { identity }),
      messageIntents: yield* readMessageIntents(conversationId),
      memberships: yield* readMemberships(conversationId),
      anchors: yield* readAnchors(conversationId),
      positions: yield* readPositions(conversationId),
      proposalLocks: Object.freeze(
        (yield* database.unsafe<Readonly<Record<string, unknown>>>(
          `SELECT conversation_id, previous_record_hash, action_hash,
                  canonical_action_core
           FROM proposal_locks ${conversationId === undefined ? "" : "WHERE conversation_id = ?"} ORDER BY conversation_id, predecessor_key`,
          [...(conversationId === undefined ? [] : [conversationId])],
        )).map(readProposalLock),
      ),
      stagedRecords: Object.freeze(
        (yield* database.unsafe<Readonly<Record<string, unknown>>>(
          `SELECT conversation_id, record_hash, previous_record_hash,
                  membership_hash, anchor_hash, action_hash, author_agent_id,
                  msg_id, canonical_record_core
           FROM staged_records ${conversationId === undefined ? "" : "WHERE conversation_id = ?"} ORDER BY conversation_id, rowid`,
          [...(conversationId === undefined ? [] : [conversationId])],
        )).map(readStagedRecord),
      ),
      evidence: yield* readEvidence(conversationId),
      certifiedRecords: yield* readAllCertifiedRecords(conversationId),
      stagedReanchors: yield* readReanchors(conversationId),
      outboundMessages: yield* readPendingOutbound(conversationId),
    });
  }).pipe(storeOperation, Effect.withSpan("recoverStoredState"));
}

/** Pages stop at the original certified head and can be replayed after restart. */
export function readStoredConversation(
  request:
    | Readonly<{
        conversationId: string;
        afterRecordHash?: string;
        maximumRecords?: number;
      }>
    | Readonly<{
        continuation: string;
        maximumRecords?: number;
      }>,
): Effect.Effect<HistoryPage, EndpointStoreError, SqlClient> {
  return transaction(
    Effect.gen(function* () {
      const sql = yield* SqlClient;
      const { conversationId, after, through, throughRecordHash } =
        yield* resolveHistoryWindow(request);
      const maximumRecords = request.maximumRecords ?? HISTORY_PAGE_SIZE;
      const rows = yield* sql.unsafe<Readonly<Record<string, unknown>>>(
        historySelect +
          " WHERE certified.conversation_id=? AND certified.ordinal>? AND certified.ordinal<=? ORDER BY certified.ordinal LIMIT ?",
        [conversationId, after, through, maximumRecords + 1],
      );
      const selected = rows.slice(0, maximumRecords);
      const records = yield* Effect.forEach(selected, readCertifiedRecord, {
        concurrency: 1,
      });
      const last = selected.at(-1);
      const continuation =
        rows.length > maximumRecords && last !== undefined
          ? Buffer.from(
              encodeCursor({
                conversationId,
                afterRecordHash: readText(last, "record_hash"),
                throughRecordHash,
              }),
            ).toString("base64url")
          : null;
      return {
        records,
        memberships: yield* readMemberships(conversationId),
        anchors: yield* readAnchors(conversationId, [
          ...new Set(records.map((record) => record.anchorHash)),
        ]),
        continuation,
      };
    }),
  ).pipe(Effect.withSpan("readStoredConversation"));
}

/** Recent certified rows and their verification dependencies share the caller's snapshot. */
export function readRecentConversation(input: {
  readonly conversationId: string;
  readonly maximumRecords: number;
  readonly candidateRecordHash?: string;
}) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    const rows = yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      historySelect +
        " WHERE certified.conversation_id=? AND (? = '' OR certified.ordinal < (SELECT ordinal FROM certified_records WHERE conversation_id=? AND record_hash=?)) ORDER BY certified.ordinal DESC LIMIT ?",
      [
        input.conversationId,
        input.candidateRecordHash ?? "",
        input.conversationId,
        input.candidateRecordHash ?? "",
        input.maximumRecords + 1,
      ],
    );
    const candidateRows =
      input.candidateRecordHash === undefined
        ? []
        : yield* sql.unsafe<Readonly<Record<string, unknown>>>(
            historySelect +
              " WHERE certified.conversation_id=? AND certified.record_hash=?",
            [input.conversationId, input.candidateRecordHash],
          );
    const records = yield* Effect.forEach(
      [...rows, ...candidateRows],
      readCertifiedRecord,
      { concurrency: 1 },
    );
    return {
      records,
      memberships: yield* readMemberships(input.conversationId),
      anchors: yield* readAnchors(
        input.conversationId,
        records.map((record) => record.anchorHash),
      ),
      positions: [...rows, ...candidateRows].map((row) => ({
        recordHash: readText(row, "record_hash"),
        sequence: readInteger(row, "sequence"),
      })),
    };
  }).pipe(storeOperation, Effect.withSpan("readRecentConversation"));
}

/** Reads remote certified records in their durable local commit order until consumed. */
export function readUnconsumedRecords() {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    const records = yield* Effect.forEach(
      yield* sql.unsafe<Readonly<Record<string, unknown>>>(
        historySelect +
          ` WHERE certified.consumed=0
          AND staged.author_agent_id<>(SELECT agent_id FROM communication_metadata WHERE singleton=1)
          ORDER BY certified.rowid`,
      ),
      readCertifiedRecord,
      { concurrency: 1 },
    );
    return {
      records,
      memberships: yield* readMemberships(),
      anchors: yield* readAnchors(),
    };
  }).pipe(storeOperation, Effect.withSpan("readUnconsumedRecords"));
}

/** A successful handler retires only its exact certified record, retaining history. */
export function consumeRecord(recordHash: string) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    const rows = yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      "UPDATE certified_records SET consumed=1 WHERE record_hash=? RETURNING record_hash",
      [recordHash],
    );
    if (rows.length !== 1) {
      return yield* new EndpointStoreError({ reason: "not-found" });
    }
  }).pipe(storeOperation, Effect.withSpan("consumeRecord"));
}

/** Protocol startup needs each retained membership, including conversations without a certified head. */
export function readMemberships(
  conversationId?: string,
): Effect.Effect<readonly StoredMembership[], EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    return Object.freeze(
      (yield* database.unsafe<Readonly<Record<string, unknown>>>(
        `SELECT conversation_id, membership_hash, canonical_membership
         FROM memberships ${conversationId === undefined ? "" : "WHERE conversation_id = ?"} ORDER BY conversation_id`,
        [...(conversationId === undefined ? [] : [conversationId])],
      )).map(readMembership),
    );
  }).pipe(storeOperation, Effect.withSpan("readMemberships"));
}

/** Acceptance stays in the caller's SQL transaction and performs no network work. */
export function acceptMessage(input: {
  readonly authorAgentId: string;
  readonly msgId: string;
  readonly conversationId: string;
  readonly canonicalMessage: Uint8Array;
}) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    const identity = yield* readStoredIdentity();
    if (identity === undefined) {
      return yield* new EndpointStoreError({ reason: "not-found" });
    }
    requireEqual(identity.agentId, input.authorAgentId);
    yield* sql.unsafe(
      `INSERT INTO message_intents(author_agent_id,msg_id,conversation_id,canonical_message)
       VALUES(?,?,?,?) ON CONFLICT(author_agent_id,msg_id) DO NOTHING`,
      [
        input.authorAgentId,
        input.msgId,
        input.conversationId,
        input.canonicalMessage,
      ],
    );
    const [row] = yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      "SELECT conversation_id,canonical_message FROM message_intents WHERE author_agent_id=? AND msg_id=?",
      [input.authorAgentId, input.msgId],
    );
    if (row === undefined) {
      return yield* new EndpointStoreError({ reason: "corrupt" });
    }
    requireEqual(readText(row, "conversation_id"), input.conversationId);
    requireSameBytes(
      readBytes(row, "canonical_message"),
      input.canonicalMessage,
    );
  }).pipe(storeOperation, Effect.withSpan("acceptMessage"));
}

/** The owning reader returns the retained Message and durable terminal outcome. */
export function readAcceptedMessage(reference: MessageReference) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    const [row] = yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      "SELECT author_agent_id,msg_id,canonical_message,completed_record_hash,failure_reason FROM message_intents WHERE author_agent_id=? AND msg_id=?",
      [reference.authorAgentId, reference.msgId],
    );
    return row === undefined ? undefined : yield* readAcceptedRow(row);
  }).pipe(storeOperation, Effect.withSpan("readAcceptedMessage"));
}

/** Each conversation prepares its earliest accepted message before offering the next. */
export function readUnpreparedMessages() {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    return yield* Effect.forEach(
      yield* sql.unsafe<Readonly<Record<string, unknown>>>(
        `SELECT author_agent_id,msg_id,canonical_message,completed_record_hash,failure_reason
         FROM message_intents WHERE rowid IN (
           SELECT MIN(rowid) FROM message_intents
           WHERE canonical_intent IS NULL AND completed_record_hash IS NULL AND failure_reason IS NULL
           GROUP BY conversation_id
         ) ORDER BY rowid`,
      ),
      readAcceptedRow,
      { concurrency: 1 },
    );
  }).pipe(storeOperation, Effect.withSpan("readUnpreparedMessages"));
}

/** Terminal preparation failures remain observable after the process restarts. */
export function failMessage(reference: MessageReference, reason: SendFailure) {
  return Effect.gen(function* () {
    requireText(reason);
    const sql = yield* SqlClient;
    yield* sql.unsafe(
      `UPDATE message_intents SET failure_reason=COALESCE(failure_reason,?)
       WHERE author_agent_id=? AND msg_id=? AND completed_record_hash IS NULL`,
      [reason, reference.authorAgentId, reference.msgId],
    );
  }).pipe(storeOperation, Effect.withSpan("failMessage"));
}

/** Pending status includes accepted messages whose membership is not prepared yet. */
export function hasPendingMessages(conversationId: string) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    const rows = yield* sql.unsafe(
      "SELECT 1 FROM message_intents WHERE conversation_id=? AND completed_record_hash IS NULL AND failure_reason IS NULL LIMIT 1",
      [conversationId],
    );
    return rows.length > 0;
  }).pipe(storeOperation, Effect.withSpan("hasPendingMessages"));
}

/** A record and its exact signed dissemination envelope commit before a durability vote. */
export function stageRecordAndOutbound(
  record: StagedRecord,
  outbound: OutboundMessageInput,
): Effect.Effect<StoredOutboundMessage, EndpointStoreError, SqlClient> {
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
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient> {
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
 * Replaces a competing lock and retains the verified `q(n)` action certificate
 * atomically. After restart that certificate prevents another local signature.
 * Refuses a staged competitor: its own certificate would prove conflicting
 * quorums beyond the fault bound.
 */
export function supersedeProposalLock(
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
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    validateCertifiedRecord(record);
    return yield* transaction(promoteRecordInTransaction(record));
  }).pipe(storeOperation, Effect.withSpan("promoteRecord"));
}

/**
 * Stages and promotes a complete certified record atomically with its evidence
 * and local completion. Recovery must never mistake it for a locally staged
 * record needing a vote. Adoption casts no new vote, including when retaining
 * an earlier local signature, and may cross a pending re-anchor.
 */
export function applyCertifiedRecord(
  record: CertifiedRecord,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient> {
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
 * Stages one complete outer envelope before it can reach Router transport.
 *
 * @param message Canonical complete signed envelope.
 * @returns The retained envelope under its envelope identity.
 */
export function enqueueOutbound(
  message: OutboundMessageInput,
): Effect.Effect<StoredOutboundMessage, EndpointStoreError, SqlClient> {
  return transaction(enqueueOutboundInTransaction(message)).pipe(
    storeOperation,
    Effect.withSpan("enqueueOutbound"),
  );
}

/**
 * Selects the exact durable envelope and retry mode for one Router attempt.
 *
 * @param envelopeId Stable identity of the signed envelope.
 * @returns The current stored bytes, or an inactive terminal disposition.
 */
export function beginOutbound(
  envelopeId: string,
): Effect.Effect<OutboundAttempt, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    requireText(envelopeId);
    return yield* transaction(
      Effect.gen(function* () {
        const retained = yield* requireOutbound(envelopeId);
        if (retained.disposition !== "pending") {
          return Object.freeze({ kind: "inactive" });
        }
        const mode = retained.attempted ? "retry" : "initial";
        if (!retained.attempted) {
          yield* database.unsafe<Readonly<Record<string, unknown>>>(
            `UPDATE outbound_messages SET attempted = 1
           WHERE envelope_id = ? AND disposition = 'pending'`,
            [envelopeId],
          );
        }
        return Object.freeze({
          kind: "pending",
          mode,
          outbound: retained.outbound,
        });
      }),
    );
  }).pipe(storeOperation, Effect.withSpan("beginOutbound"));
}

/**
 * Marks one exact stored envelope complete after Router accepts its bytes.
 *
 * @param outbound Exact envelope whose Router digest was verified.
 * @returns Whether this call recorded acceptance or observed it already.
 */
export function completeOutbound(
  outbound: StoredOutboundMessage,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    validateMessageInput(outbound);
    return yield* transaction(
      Effect.gen(function* () {
        const retained = yield* requireOutbound(outbound.envelopeId);
        requireSameOutbound(retained.outbound, outbound);
        switch (retained.disposition) {
          case "accepted":
            return "existing";
          case "discarded":
            return yield* new EndpointStoreError({ reason: "conflict" });
          case "pending":
            yield* database.unsafe<Readonly<Record<string, unknown>>>(
              `UPDATE outbound_messages SET disposition = 'accepted'
             WHERE envelope_id = ? AND disposition = 'pending'`,
              [outbound.envelopeId],
            );
            return "inserted";
          default: {
            const exhaustive: never = retained.disposition;
            return exhaustive;
          }
        }
      }),
    );
  }).pipe(storeOperation, Effect.withSpan("completeOutbound"));
}

/**
 * Atomically invalidates exact pending envelopes before protocol regeneration.
 *
 * @param outbounds Complete expected envelopes to invalidate.
 * @returns Whether at least one pending envelope became inactive.
 */
export function discardOutbound(
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
      Effect.gen(function* () {
        const retained = [];
        for (const outbound of outbounds) {
          const row = yield* requireOutbound(outbound.envelopeId);
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
            yield* database.unsafe<Readonly<Record<string, unknown>>>(
              `UPDATE outbound_messages SET disposition = 'discarded'
             WHERE envelope_id = ? AND disposition = 'pending'`,
              [outbound.outbound.envelopeId],
            );
          }
        }
        return inserted ? "inserted" : "existing";
      }),
    );
  }).pipe(storeOperation, Effect.withSpan("discardOutbound"));
}

/** The outbox itself owns the exact signed envelope for one staged record. */
export function readRecordOutbound(conversationId: string, recordHash: string) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    const [row] = yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      `SELECT conversation_id,envelope_id,canonical_signed_message,wire_version,record_hash,attempted,disposition
       FROM outbound_messages WHERE conversation_id=? AND record_hash=?
       ORDER BY disposition='discarded',outbound_sequence DESC LIMIT 1`,
      [conversationId, recordHash],
    );
    return row === undefined ? undefined : readOutbound(row);
  }).pipe(storeOperation, Effect.withSpan("readRecordOutbound"));
}

/** A repeated operation must preserve its original request and authority. */
export function putCollectiveOperation(
  input:
    | { readonly origin: "local"; readonly request: Request }
    | {
        readonly origin: "remote";
        readonly request: Message;
        readonly value: OperationValue;
      },
) {
  return transaction(
    Effect.gen(function* () {
      const sql = yield* SqlClient;
      const operationId =
        input.origin === "local"
          ? input.request.operationId
          : input.value.operationId;
      const deadlineAt =
        input.origin === "local"
          ? input.request.deadlineAt
          : input.value.deadlineAt;
      const canonicalRequest = yield* input.origin === "local"
        ? encodeCanonical(Request, input.request)
        : encodeCanonical(Message, input.request);
      const [existing] = yield* sql.unsafe<Readonly<Record<string, unknown>>>(
        "SELECT canonical_request,origin,creator_agent_id,deadline_at FROM collective_operations WHERE operation_id=?",
        [operationId],
      );
      if (existing !== undefined) {
        requireSameBytes(
          readBytes(existing, "canonical_request"),
          canonicalRequest,
        );
        if (
          readText(existing, "origin") !== input.origin ||
          readText(existing, "creator_agent_id") !==
            input.request.author.agentId ||
          readInteger(existing, "deadline_at") !== deadlineAt
        ) {
          return yield* new EndpointStoreError({ reason: "conflict" });
        }
        return;
      }
      yield* sql.unsafe(
        "INSERT INTO collective_operations(operation_id,creator_agent_id,origin,canonical_request,deadline_at) VALUES(?,?,?,?,?)",
        [
          operationId,
          input.request.author.agentId,
          input.origin,
          canonicalRequest,
          deadlineAt,
        ],
      );
    }),
  ).pipe(storeOperation, Effect.withSpan("putCollectiveOperation"));
}

/** Workers load each operation's content inside its completion transaction. */
export function readCollectiveOperationIds(
  origin: StoredCollectiveOperation["origin"],
  phase: StoredCollectiveOperation["phase"],
) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    const rows = yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      "SELECT operation_id FROM collective_operations WHERE origin=? AND phase=? ORDER BY rowid",
      [origin, phase],
    );
    return yield* Effect.forEach(
      rows,
      (row) => decodeValue(CollectiveId, readText(row, "operation_id")),
      { concurrency: 1 },
    );
  }).pipe(storeOperation, Effect.withSpan("readCollectiveOperationIds"));
}

/** Role uniqueness and Communication acceptance commit in the same caller transaction. */
export function putCollectiveMessage(input: StoredCollectiveMessage) {
  return transaction(
    Effect.gen(function* () {
      const sql = yield* SqlClient;
      const prior = (yield* sql.unsafe<Readonly<Record<string, unknown>>>(
        "SELECT author_agent_id,msg_id FROM collective_messages WHERE operation_id=? AND role=? AND destination=?",
        [input.operationId, input.role, input.destination],
      ))[0];
      if (prior !== undefined) {
        if (
          readText(prior, "author_agent_id") !==
            input.reference.authorAgentId ||
          readText(prior, "msg_id") !== input.reference.msgId
        ) {
          return yield* new EndpointStoreError({ reason: "conflict" });
        }
        return false;
      }
      yield* sql.unsafe(
        "INSERT INTO collective_messages(operation_id,role,destination,author_agent_id,msg_id) VALUES(?,?,?,?,?)",
        [
          input.operationId,
          input.role,
          input.destination,
          input.reference.authorAgentId,
          input.reference.msgId,
        ],
      );
      return true;
    }),
  ).pipe(storeOperation, Effect.withSpan("putCollectiveMessage"));
}

/** An operation's accepted messages retain their role and fixed identity. */
export function readCollectiveMessages(operationId: CollectiveId) {
  return readMessageRows(
    "WHERE message.operation_id=? ORDER BY message.rowid",
    [operationId],
  );
}

/** One bounded worker observes only durable completion or terminal failure. */
export function readReadyCollectiveMessages() {
  return readMessageRows(
    `JOIN collective_operations AS operation ON operation.operation_id=message.operation_id
   JOIN message_intents AS intent ON intent.author_agent_id=message.author_agent_id AND intent.msg_id=message.msg_id
   WHERE operation.phase!='completed' AND message.role IN ('response','close')
    AND (intent.completed_record_hash IS NOT NULL OR (intent.failure_reason IS NOT NULL AND (message.role='close' OR operation.phase='finalizing')))
    AND (message.role='close' OR NOT EXISTS (SELECT 1 FROM collective_answers AS answer WHERE answer.operation_id=message.operation_id AND answer.member=message.author_agent_id))
   ORDER BY message.rowid`,
    [],
  );
}

/** Only the collector's first accepted answer before its deadline is selectable. */
export function recordCollectiveAnswer(
  operationId: CollectiveId,
  message: Message,
  now: number,
) {
  return transaction(
    Effect.gen(function* () {
      const sql = yield* SqlClient;
      const operation = yield* readCollectiveOperation(operationId);
      if (operation === undefined || operation.phase === "completed") {
        return false;
      }
      if (
        operation.origin === "local" &&
        (operation.phase !== "collecting" || now >= operation.deadlineAt)
      ) {
        return false;
      }
      const member = message.author.agentId;
      const prior = yield* sql.unsafe(
        "SELECT 1 FROM collective_answers WHERE operation_id=? AND member=?",
        [operationId, member],
      );
      if (prior.length > 0) {
        return false;
      }
      yield* sql.unsafe(
        "INSERT INTO collective_answers(operation_id,member,canonical_message) VALUES(?,?,?)",
        [operationId, member, yield* encodeCanonical(Message, message)],
      );
      return true;
    }),
  ).pipe(storeOperation, Effect.withSpan("recordCollectiveAnswer"));
}

/** Related domain writes use the driver's connection and transaction context. */
export function transaction<A, E, R>(
  operation: Effect.Effect<A, E, R>,
): Effect.Effect<A, EndpointStoreError, R | SqlClient> {
  return Effect.flatMap(SqlClient, (sql) =>
    sql.withTransaction(operation),
  ).pipe(storeOperation);
}

/** Stored requests and their identifiers are validated before business operations receive them. */
export function readCollectiveOperation(operationId: CollectiveId) {
  return readOperations(" WHERE operation_id=?", [operationId]).pipe(
    Effect.map((rows) => rows[0]),
    Effect.withSpan("readCollectiveOperation"),
  );
}

/** First-answer messages are decoded once at their persistence boundary. */
export function readCollectiveAnswers(operationId: CollectiveId) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    const rows = yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      "SELECT canonical_message FROM collective_answers WHERE operation_id=? ORDER BY rowid",
      [operationId],
    );
    return yield* Effect.forEach(
      rows,
      (row) => decodeRuntimeValue(Message, readBytes(row, "canonical_message")),
      { concurrency: 1 },
    );
  }).pipe(storeOperation, Effect.withSpan("readCollectiveAnswers"));
}

/** The first authenticated remote close remains available while an own answer certifies. */
export function saveRemoteClose(operationId: CollectiveId, message: Message) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    yield* sql.unsafe(
      "UPDATE collective_operations SET phase='finalizing',canonical_close=? WHERE operation_id=? AND origin='remote' AND phase='collecting'",
      [yield* encodeCanonical(Message, message), operationId],
    );
  }).pipe(storeOperation, Effect.withSpan("saveRemoteClose"));
}

/**
 * Freezes final bytes and exact source identities. Close certification
 * changes only their visibility.
 */
export function saveCollectiveResult(input: {
  readonly operationId: CollectiveId;
  readonly item: ResultDelivery["item"];
  readonly answers: readonly MessageReference[];
  readonly complete: boolean;
}) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    yield* sql.unsafe(
      "UPDATE collective_operations SET phase=?,canonical_result=?,canonical_answer_refs=? WHERE operation_id=? AND phase<>'completed' AND canonical_result IS NULL",
      [
        input.complete ? "completed" : "finalizing",
        yield* encodeCanonical(ResultDelivery.fields.item, input.item),
        yield* encodeRuntimeValue(input.answers),
        input.operationId,
      ],
    );
  }).pipe(storeOperation, Effect.withSpan("saveCollectiveResult"));
}

/** A selected result becomes deliverable without rebuilding its content or sources. */
export function completeCollectiveOperation(operationId: CollectiveId) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    yield* sql.unsafe(
      "UPDATE collective_operations SET phase='completed' WHERE operation_id=? AND canonical_result IS NOT NULL",
      [operationId],
    );
  }).pipe(storeOperation, Effect.withSpan("completeCollectiveOperation"));
}

/** A terminal close failure replaces only an unpublished result. */
export function failCollectiveClose(
  operationId: CollectiveId,
  item: ResultDelivery["item"],
) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    yield* sql.unsafe(
      "UPDATE collective_operations SET phase='completed',canonical_result=?,canonical_answer_refs=? WHERE operation_id=? AND phase='finalizing' AND delivered=0",
      [
        yield* encodeCanonical(ResultDelivery.fields.item, item),
        yield* encodeRuntimeValue([]),
        operationId,
      ],
    );
  }).pipe(storeOperation, Effect.withSpan("failCollectiveClose"));
}

/** Successful handling retires the same result while keeping its content readable. */
export function retireCollectiveResult(operationId: CollectiveId) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    yield* sql.unsafe(
      "UPDATE collective_operations SET delivered=1 WHERE operation_id=? AND phase='completed'",
      [operationId],
    );
  }).pipe(storeOperation, Effect.withSpan("retireCollectiveResult"));
}

/** An invalid close consumes protocol state without inventing a result. */
export function retireCollectiveOperation(operationId: CollectiveId) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    yield* sql.unsafe(
      "UPDATE collective_operations SET phase='completed',delivered=1 WHERE operation_id=?",
      [operationId],
    );
  }).pipe(storeOperation, Effect.withSpan("retireCollectiveOperation"));
}

/** Driver failures and invalid stored values stay inside the closed persistence boundary. */
function storeOperation<A, E, R>(
  operation: Effect.Effect<A, E, R>,
): Effect.Effect<A, EndpointStoreError, R> {
  return operation.pipe(
    Effect.catchAllCause((cause) => {
      if (Cause.isInterruptedOnly(cause)) {
        return Effect.failCause(Cause.stripFailures(cause));
      }
      const failure = Cause.failureOption(cause);
      const defect = Cause.dieOption(cause);
      return Effect.fail(
        mapStoreFailure(
          Option.getOrElse(failure, () =>
            Option.getOrElse(defect, () => undefined),
          ),
          "persistence",
        ),
      );
    }),
  );
}

/**
 * Inserts one first-candidate lock inside a caller-owned transaction, or
 * confirms the same lock is already durable.
 * @param proposal Verified gap-free action selected at its predecessor.
 * @returns Whether the lock was inserted or already durable.
 */
function lockProposalInTransaction(
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
 * Projects one stable evidence value.
 *
 * @param row SQLite result row.
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
 * @param row SQLite result row.
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

/** Protocol metadata allows other owners to initialize their own tables independently. */
function inspectStoreState(): Effect.Effect<
  StoreOpening,
  EndpointStoreError,
  SqlClient
> {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    const tables = yield* sql.unsafe<{
      readonly name: string;
    }>(
      "SELECT name FROM sqlite_schema WHERE type='table' AND name IN ('communication_metadata','memberships')",
    );
    if (!tables.some((row) => row.name === "communication_metadata")) {
      if (tables.length !== 0) {
        return yield* new EndpointStoreError({ reason: "incompatible" });
      }
      return "create";
    }
    const [metadata] = yield* sql.unsafe<{
      readonly version: number;
    }>("SELECT version FROM communication_metadata WHERE singleton=1");
    if (metadata?.version !== SCHEMA_VERSION) {
      return yield* new EndpointStoreError({ reason: "incompatible" });
    }
    const checks =
      yield* sql.unsafe<Readonly<Record<string, unknown>>>(
        "PRAGMA quick_check",
      );
    if (
      checks.length !== 1 ||
      checks[0] === undefined ||
      readText(checks[0], "quick_check") !== "ok"
    ) {
      return yield* new EndpointStoreError({ reason: "corrupt" });
    }
    return "reopen";
  }).pipe(storeOperation, Effect.withSpan("inspectCommunicationState"));
}

/** Fresh databases receive the current protocol layout inside daemon startup's transaction. */
function createCommunicationTables(): Effect.Effect<
  void,
  EndpointStoreError,
  SqlClient
> {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    for (const statement of (schemaSql + collectiveSchemaSql)
      .split(";")
      .map((part) => part.trim())
      .filter((part) => part.length > 0)) {
      yield* sql.unsafe<Readonly<Record<string, unknown>>>(statement);
    }
  }).pipe(storeOperation, Effect.withSpan("createCommunicationTables"));
}

/**
 * Maps every internal failure to the store's closed error channel.
 *
 * @param failure Internal failure or closed control-flow signal.
 * @param fallback Closed category for unexpected implementation failures.
 * @returns A non-diagnostic endpoint-store error.
 */
function mapStoreFailure(
  failure: unknown,
  fallback: EndpointStoreFailure,
): EndpointStoreError {
  if (failure instanceof EndpointStoreError) {
    return failure;
  }
  return new EndpointStoreError({
    reason: failure instanceof StoreSignal ? failure.reason : fallback,
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
  binding: MessageIntentBinding,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient> {
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
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
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
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient> {
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
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient> {
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
): Effect.Effect<void, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
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
  SqlClient
> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
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
    const local = yield* readStoredIdentity();
    if (local === undefined) {
      return yield* new EndpointStoreError({ reason: "not-found" });
    }
    yield* database.unsafe<Readonly<Record<string, unknown>>>(
      `INSERT INTO memberships
        (conversation_id, membership_hash, canonical_membership, sort_key)
       VALUES (?, ?, ?, ?)`,
      [
        foundation.conversationId,
        foundation.membershipHash,
        copyBytes(foundation.canonicalMembership),
        conversationSortKey(members, local.agentId),
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
 * Local voting cannot leave a head with a staged successor. Adopting a
 * completed member anchor casts no local vote and may do so.
 */
function stageReanchorInTransaction(
  reanchor: StagedReanchor,
  purpose: "vote" | "adopted",
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient> {
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
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient> {
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
  reanchor: CompletedReanchor,
): Effect.Effect<void, EndpointStoreError, SqlClient> {
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
 * Removes the lock, evidence, staged records and outbox associations in the
 * caller's transaction. The caller must establish that the action cannot
 * certify because orphaned rows would fail startup verification.
 */
function releaseProposalLock(
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
 * Delete this endpoint's uncompleted candidates in a completed re-anchor's
 * scope that the completion does not select, with their re-anchor votes.
 * @param reanchor Verified completed re-anchor that supersedes them.
 */
function retireSupersededCandidates(
  reanchor: CompletedReanchor,
): Effect.Effect<void, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
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
): Effect.Effect<void, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
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
 * A member that staged a successor cannot vote to re-anchor behind it under
 * the same anchor.
 */
function requireNoStagedSuccessor(
  reanchor: StagedReanchor,
): Effect.Effect<void, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
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
): Effect.Effect<void, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const position = yield* readStoredPosition(reanchor.conversationId);
    requireEqual(position.currentAnchorHash, reanchor.previousAnchorHash);
    requireEqual(position.headRecordHash, reanchor.selectedRecordHash);
  }).pipe(storeOperation);
}

function insertStagedReanchor(
  reanchor: StagedReanchor,
): Effect.Effect<void, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
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
): Effect.Effect<void, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
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
): Effect.Effect<boolean, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
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
): Effect.Effect<void, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
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
): Effect.Effect<void, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
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
): Effect.Effect<readonly MessageIntent[], EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
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

function readPositions(conversationId?: string) {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    return Object.freeze(
      (yield* database.unsafe<Readonly<Record<string, unknown>>>(
        `SELECT conversation_id, membership_hash, current_anchor_hash,
                  head_record_hash FROM conversation_state
           ${conversationId === undefined ? "" : "WHERE conversation_id = ?"}
           ORDER BY conversation_id`,
        [...(conversationId === undefined ? [] : [conversationId])],
      )).map(readConversationPosition),
    );
  }).pipe(storeOperation);
}

function readMessageIntents(
  conversationId?: string,
): Effect.Effect<readonly MessageIntent[], EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    return Object.freeze(
      (yield* database.unsafe<Readonly<Record<string, unknown>>>(
        `SELECT conversation_id, membership_hash, author_agent_id, msg_id,
                canonical_intent, canonical_message, completed_record_hash
         FROM message_intents WHERE canonical_intent IS NOT NULL AND failure_reason IS NULL ${conversationId === undefined ? "" : "AND conversation_id = ?"} ORDER BY rowid`,
        [...(conversationId === undefined ? [] : [conversationId])],
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
  conversationId?: string,
  anchorHashes?: readonly string[],
): Effect.Effect<readonly StoredAnchor[], EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    if (anchorHashes?.length === 0) {
      return Object.freeze([]);
    }
    const database = yield* SqlClient;
    return Object.freeze(
      (yield* database.unsafe<Readonly<Record<string, unknown>>>(
        `SELECT conversation_id, anchor_hash, previous_anchor_hash,
                selected_record_hash, canonical_anchor
         FROM anchors WHERE ${conversationId === undefined ? "1" : "conversation_id = ?"}
         ${anchorHashes === undefined ? "" : `AND anchor_hash IN (${anchorHashes.map(() => "?").join(", ")})`}
         ORDER BY conversation_id, rowid`,
        [
          ...(conversationId === undefined ? [] : [conversationId]),
          ...(anchorHashes ?? []),
        ],
      )).map((row) => {
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
      }),
    );
  }).pipe(storeOperation);
}

function readEvidence(conversationId?: string) {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    return Object.freeze(
      (yield* database.unsafe<Readonly<Record<string, unknown>>>(
        `SELECT conversation_id, evidence_kind, subject_id,
                evidence_key, canonical_evidence
         FROM protocol_evidence
         ${conversationId === undefined ? "" : "WHERE conversation_id = ?"}
         ORDER BY conversation_id, evidence_kind, subject_id, evidence_key`,
        [...(conversationId === undefined ? [] : [conversationId])],
      )).map(readProtocolEvidence),
    );
  }).pipe(storeOperation);
}

function readAllCertifiedRecords(
  conversationId?: string,
): Effect.Effect<readonly CertifiedRecord[], EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    return Object.freeze(
      yield* Effect.forEach(
        yield* database.unsafe<Readonly<Record<string, unknown>>>(
          `${historySelect}
         ${conversationId === undefined ? "" : "WHERE certified.conversation_id = ?"}
         ORDER BY certified.conversation_id, certified.ordinal`,
          [...(conversationId === undefined ? [] : [conversationId])],
        ),
        (row) => readCertifiedRecord(row),
        { concurrency: 1 },
      ),
    );
  }).pipe(storeOperation);
}

/**
 * Projects one complete certified record and its separate evidence maps.
 *
 * @param row SQLite result row for the certified record core.
 * @returns The detached complete certified record.
 */
function readCertifiedRecord(
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

/** Returns detached signer evidence in stored signer-key order. */
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

function readReanchors(conversationId?: string) {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    return Object.freeze(
      (yield* database.unsafe<Readonly<Record<string, unknown>>>(
        `SELECT conversation_id, anchor_hash, previous_anchor_hash,
                router_instance_id, selected_record_hash, canonical_body,
                canonical_completed_reanchor
         FROM reanchors ${conversationId === undefined ? "" : "WHERE conversation_id = ?"} ORDER BY conversation_id, rowid`,
        [...(conversationId === undefined ? [] : [conversationId])],
      )).map(readRecoveredReanchor),
    );
  }).pipe(storeOperation);
}

function resolveHistoryWindow(
  request: Parameters<typeof readStoredConversation>[0],
) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    const selected = yield* resolveHistoryRequest(request);
    const conversationId = selected.conversationId;
    if (conversationId.length === 0) {
      return yield* new EndpointStoreError({ reason: "invalid-input" });
    }
    const [head] = yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      "SELECT record_hash,ordinal FROM certified_records WHERE conversation_id=? ORDER BY ordinal DESC LIMIT 1",
      [conversationId],
    );
    if (head === undefined) {
      return yield* new EndpointStoreError({
        reason: selected.resumed ? "invalid-continuation" : "not-found",
      });
    }
    const throughRecordHash =
      selected.throughRecordHash ?? readText(head, "record_hash");
    const through = yield* historyPosition(
      conversationId,
      throughRecordHash,
      selected.resumed,
    );
    const after =
      selected.afterRecordHash === undefined
        ? -1
        : yield* historyPosition(
            conversationId,
            selected.afterRecordHash,
            selected.resumed,
          );
    if (after > through) {
      return yield* new EndpointStoreError({ reason: "invalid-continuation" });
    }
    return { conversationId, after, through, throughRecordHash };
  });
}

function resolveHistoryRequest(
  request: Parameters<typeof readStoredConversation>[0],
): Effect.Effect<
  {
    readonly conversationId: string;
    readonly afterRecordHash?: string;
    readonly throughRecordHash?: string;
    readonly resumed: boolean;
  },
  EndpointStoreError
> {
  if ("continuation" in request) {
    return decodeHistoryCursor(request.continuation).pipe(
      Effect.map((cursor) => ({ ...cursor, resumed: true })),
    );
  }
  return Effect.succeed({ ...request, resumed: false });
}

function decodeHistoryCursor(value: string) {
  return Effect.try({
    try: () => {
      if (value.length > 4096 || !/^[A-Za-z0-9_-]+$/u.test(value)) {
        throw new StoreSignal("invalid-continuation");
      }
      const bytes = Buffer.from(value, "base64url");
      if (bytes.toString("base64url") !== value) {
        throw new StoreSignal("invalid-continuation");
      }
      return new TextDecoder("utf-8", { fatal: true }).decode(bytes);
    },
    catch: () => new EndpointStoreError({ reason: "invalid-continuation" }),
  }).pipe(
    Effect.flatMap(decodeCursor),
    Effect.catchTag("ParseError", () =>
      Effect.fail(new EndpointStoreError({ reason: "invalid-continuation" })),
    ),
  );
}

function historyPosition(
  conversationId: string,
  recordHash: string,
  continuation: boolean,
) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    const row = (yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      "SELECT ordinal FROM certified_records WHERE conversation_id=? AND record_hash=?",
      [conversationId, recordHash],
    ))[0];
    if (row === undefined) {
      return yield* new EndpointStoreError({
        reason: continuation ? "invalid-continuation" : "not-found",
      });
    }
    return readInteger(row, "ordinal");
  }).pipe(storeOperation);
}

function readAcceptedRow(row: Readonly<Record<string, unknown>>) {
  return Effect.gen(function* () {
    const canonicalMessage = readBytes(row, "canonical_message");
    const message = yield* decodeCanonical(Message, canonicalMessage);
    requireEqual(message.author.agentId, readText(row, "author_agent_id"));
    requireEqual(message.msgId, readText(row, "msg_id"));
    const savedFailure = readOptionalText(row, "failure_reason");
    const failure =
      savedFailure === undefined
        ? undefined
        : yield* Schema.decodeUnknown(Schema.Literal(...sendFailureReasons))(
            savedFailure,
          );
    return {
      message,
      canonicalMessage,
      completed: readOptionalText(row, "completed_record_hash") !== undefined,
      failure,
    };
  }).pipe(storeOperation);
}

/**
 * Retain one verified record core without authorizing a local signature.
 * @param record Verified record core and private bindings.
 * @returns Whether the record was inserted or already staged.
 */
function stageRecordInTransaction(
  record: StagedRecord,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
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
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient> {
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
  record: CertifiedRecord,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient> {
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
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
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
): Effect.Effect<void, EndpointStoreError, SqlClient> {
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

/**
 * Reads a required conversation position.
 *
 * @param conversationId Opaque conversation identity.
 * @returns Its durable anchor, head, and local ordinal.
 */
function readStoredPosition(
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
  evidence: ProtocolEvidence,
): Effect.Effect<Uint8Array | undefined, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
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
): Effect.Effect<void, EndpointStoreError, SqlClient> {
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

/**
 * Reads an optional staged re-anchor by stable body hash.
 *
 * @param conversationId Opaque conversation identity.
 * @param anchorHash Stable private anchor hash.
 * @returns The detached staged re-anchor when present.
 */
function findStagedReanchor(
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

function requireLocalEvidenceLock(
  evidence: ProtocolEvidence,
): Effect.Effect<void, EndpointStoreError, SqlClient> {
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
): Effect.Effect<void, EndpointStoreError, SqlClient> {
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
 * Reads an optional proposal lock for one predecessor.
 *
 * @param conversationId Opaque conversation identity.
 * @param predecessorKey Exact predecessor or the first message sentinel.
 * @returns The detached proposal lock when present.
 */
function findProposalLock(
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
 * @param conversationId Opaque conversation identity.
 * @param recordHash Stable private record hash.
 * @returns The detached staged record when present.
 */
function findStagedRecord(
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
  record: StagedRecord,
): Effect.Effect<void, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
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
): Effect.Effect<boolean, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
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
): Effect.Effect<boolean, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
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
): Effect.Effect<boolean, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
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
): Effect.Effect<void, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
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
): Effect.Effect<void, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
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

/** The communication owner binds one immutable agent identity to its protocol state. */
function readStoredIdentity() {
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

function requireMembership(
  conversationId: string,
): Effect.Effect<void, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
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
): Effect.Effect<boolean, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
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

/**
 * The caller's transaction keeps lookup and insertion on one leased connection.
 * Repeated envelope identities retain the same complete bytes and conversation.
 * @param message Canonical complete signed envelope.
 * @returns The retained envelope under its envelope identity.
 */
function enqueueOutboundInTransaction(
  message: OutboundMessageInput,
): Effect.Effect<StoredOutboundMessage, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    validateMessageInput(message);
    yield* requireConversation(message.conversationId);
    const existing = yield* findOutbound(message.envelopeId);
    if (existing !== undefined) {
      requireEqual(existing.outbound.conversationId, message.conversationId);
      requireEqual(existing.outbound.wireVersion, message.wireVersion);
      requireSameBytes(
        existing.outbound.canonicalSignedMessage,
        message.canonicalSignedMessage,
      );
      return existing.outbound;
    }
    yield* database.unsafe<Readonly<Record<string, unknown>>>(
      `INSERT INTO outbound_messages
        (conversation_id, envelope_id,
         canonical_signed_message, wire_version, record_hash, attempted, disposition)
       VALUES (?, ?, ?, ?, ?, 0, 'pending')`,
      [
        message.conversationId,
        message.envelopeId,
        copyBytes(message.canonicalSignedMessage),
        message.wireVersion,
        message.recordHash ?? null,
      ],
    );
    return copyOutbound(message);
  }).pipe(storeOperation, Effect.withSpan("enqueueOutboundInTransaction"));
}

/**
 * Reads every pending stored envelope in durable insertion order.
 *
 * @param conversationId Restrict the read to this conversation when provided.
 * @returns Detached pending envelopes, excluding accepted and discarded rows.
 */
function readPendingOutbound(
  conversationId?: string,
): Effect.Effect<
  readonly StoredOutboundMessage[],
  EndpointStoreError,
  SqlClient
> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    return Object.freeze(
      (yield* database.unsafe<Readonly<Record<string, unknown>>>(
        `SELECT conversation_id, envelope_id,
                canonical_signed_message, wire_version, record_hash, attempted, disposition
         FROM outbound_messages WHERE disposition = 'pending' ${conversationId === undefined ? "" : "AND conversation_id = ?"}
         ORDER BY outbound_sequence`,
        [...(conversationId === undefined ? [] : [conversationId])],
      )).map((row) => readOutbound(row).outbound),
    );
  }).pipe(storeOperation, Effect.withSpan("readPendingOutbound"));
}

function requireOutbound(
  envelopeId: string,
): Effect.Effect<RetainedOutbound, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const retained = yield* findOutbound(envelopeId);
    if (retained === undefined) {
      return yield* new EndpointStoreError({ reason: "not-found" });
    }
    return retained;
  }).pipe(storeOperation);
}

function findOutbound(
  envelopeId: string,
): Effect.Effect<RetainedOutbound | undefined, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    const row = (yield* database.unsafe<Readonly<Record<string, unknown>>>(
      `SELECT conversation_id, envelope_id,
              canonical_signed_message, wire_version, record_hash, attempted, disposition
       FROM outbound_messages WHERE envelope_id = ?`,
      [envelopeId],
    ))[0];
    return row === undefined ? undefined : readOutbound(row);
  }).pipe(storeOperation);
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

/**
 * Reads one nullable nonempty SQLite TEXT.
 *
 * @param row SQLite result row.
 * @param key Selected column name.
 * @returns The exact text, or undefined for SQL NULL.
 */
function readOptionalText(
  row: Readonly<Record<string, unknown>>,
  key: string,
): string | undefined {
  const value = row[key];
  if (value === null) {
    return undefined;
  }
  return readText(row, key);
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
  conversationId: string,
): Effect.Effect<void, EndpointStoreError, SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient;
    if (
      (yield* database.unsafe<Readonly<Record<string, unknown>>>(
        `SELECT 1 AS retained FROM conversation_state
         WHERE conversation_id = ?`,
        [conversationId],
      ))[0] === undefined
    ) {
      return yield* new EndpointStoreError({ reason: "not-found" });
    }
  }).pipe(storeOperation);
}

function validateMessageInput(message: OutboundMessageInput): void {
  requireText(message.wireVersion);
  requireText(message.conversationId);
  requireText(message.envelopeId);
  requireBytes(message.canonicalSignedMessage);
}

/**
 * Rejects empty and NUL-bearing values before they reach SQLite.
 *
 * @param value Opaque private identifier or hash.
 */
function requireText(value: string): void {
  if (value.length === 0 || value.includes("\u0000")) {
    throw new StoreSignal("invalid-input");
  }
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

/**
 * Requires canonical byte equality for idempotent state.
 *
 * @param left Previously retained canonical bytes.
 * @param right Candidate canonical bytes.
 */
function requireSameBytes(left: Uint8Array, right: Uint8Array): void {
  if (!sameBytes(left, right)) {
    throw new StoreSignal("conflict");
  }
}

/**
 * Requires two exact bindings, including optional bindings, to agree.
 *
 * @param left Previously retained binding.
 * @param right Candidate binding.
 */
function requireEqual<Value>(left: Value, right: Value): void {
  if (left !== right) {
    throw new StoreSignal("conflict");
  }
}

function copyOutbound(outbound: StoredOutboundMessage): StoredOutboundMessage {
  return Object.freeze({
    ...outbound,
    canonicalSignedMessage: copyBytes(outbound.canonicalSignedMessage),
  });
}

function readMessageRows(suffix: string, parameters: readonly string[]) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    const rows = yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      "SELECT message.operation_id,message.role,message.destination,message.author_agent_id,message.msg_id FROM collective_messages AS message " +
        suffix,
      parameters,
    );
    return yield* Effect.forEach(
      rows,
      (row) =>
        Effect.gen(function* () {
          const role = readText(row, "role");
          if (role !== "request" && role !== "response" && role !== "close") {
            return yield* new EndpointStoreError({ reason: "corrupt" });
          }
          return {
            operationId: yield* decodeValue(
              CollectiveId,
              readText(row, "operation_id"),
            ),
            role,
            destination: yield* decodeValue(
              Schema.String,
              readText(row, "destination"),
            ),
            reference: yield* decodeValue(MessageReference, {
              authorAgentId: readText(row, "author_agent_id"),
              msgId: readText(row, "msg_id"),
            }),
          } satisfies StoredCollectiveMessage;
        }),
      { concurrency: 1 },
    );
  }).pipe(storeOperation, Effect.withSpan("readCollectiveMessages"));
}

function readOperations(where: string, parameters: readonly string[]) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    const rows = yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      "SELECT * FROM collective_operations" + where,
      parameters,
    );
    return yield* Effect.forEach(rows, readOperation, { concurrency: 1 });
  }).pipe(storeOperation);
}

function readOperation(
  row: Readonly<Record<string, unknown>>,
): Effect.Effect<StoredCollectiveOperation, EndpointStoreError> {
  return Effect.gen(function* () {
    const operationId = yield* decodeValue(
      CollectiveId,
      readText(row, "operation_id"),
    );
    const creatorAgentId = yield* decodeValue(
      AgentId,
      readText(row, "creator_agent_id"),
    );
    const origin = yield* decodeValue(
      Schema.Literal("local", "remote"),
      readText(row, "origin"),
    );
    const phase = yield* decodeValue(
      Schema.Literal("collecting", "finalizing", "completed"),
      readText(row, "phase"),
    );
    const closeBytes = readOptionalBytes(row, "canonical_close");
    const close =
      closeBytes === undefined
        ? undefined
        : yield* readPart(closeBytes, "close");
    const result = yield* readResult(row, operationId);
    if (close !== undefined) {
      if (
        origin !== "remote" ||
        close.value.operationId !== operationId ||
        close.message.author.agentId !== creatorAgentId
      ) {
        return yield* new EndpointStoreError({ reason: "corrupt" });
      }
    }
    const state: OperationState = {
      operationId,
      phase,
      deadlineAt: readInteger(row, "deadline_at"),
      ...(close === undefined ? {} : { close: close.value }),
      ...(result === undefined ? {} : { result }),
    };
    if (origin === "local") {
      const request = yield* decodeRuntimeValue(
        Request,
        readBytes(row, "canonical_request"),
      );
      if (
        request.operationId !== operationId ||
        request.author.agentId !== creatorAgentId ||
        request.deadlineAt !== state.deadlineAt
      ) {
        return yield* new EndpointStoreError({ reason: "corrupt" });
      }
      return { ...state, origin, request };
    }
    const request = yield* readPart(
      readBytes(row, "canonical_request"),
      "operation",
    );
    if (
      request.value.operationId !== operationId ||
      request.message.author.agentId !== creatorAgentId ||
      request.value.deadlineAt !== state.deadlineAt
    ) {
      return yield* new EndpointStoreError({ reason: "corrupt" });
    }
    return { ...state, origin, request };
  });
}

/**
 * Reads one safe SQLite INTEGER.
 *
 * @param row SQLite result row when one was selected.
 * @param key Selected column name.
 * @returns The exact safe integer.
 */
function readInteger(
  row: Readonly<Record<string, unknown>>,
  key: string,
): number {
  const value = row[key];
  if (typeof value !== "number" || !Number.isSafeInteger(value)) {
    throw new StoreSignal("corrupt");
  }
  return value;
}

/**
 * Reads one required nonempty SQLite TEXT.
 *
 * @param row SQLite result row when one was selected.
 * @param key Selected column name.
 * @returns The exact nonempty text.
 */
function readText(row: Readonly<Record<string, unknown>>, key: string): string {
  const value = row[key];
  if (typeof value !== "string" || value.length === 0) {
    throw new StoreSignal("corrupt");
  }
  return value;
}

function readResult(
  row: Readonly<Record<string, unknown>>,
  operationId: CollectiveId,
) {
  return Effect.gen(function* () {
    const bytes = readOptionalBytes(row, "canonical_result");
    const refs = readOptionalBytes(row, "canonical_answer_refs");
    if (bytes === undefined && refs === undefined) {
      return undefined;
    }
    if (bytes === undefined || refs === undefined) {
      return yield* new EndpointStoreError({ reason: "corrupt" });
    }
    const item = yield* decodeRuntimeValue(ResultDelivery.fields.item, bytes);
    if (
      (item.kind === "result"
        ? item.result.request.operationId
        : item.request.operationId) !== operationId
    ) {
      return yield* new EndpointStoreError({ reason: "corrupt" });
    }
    return {
      item,
      answers: yield* decodeRuntimeValue(
        Schema.Array(MessageReference).pipe(
          Schema.maxItems(maximumMembers - 1),
        ),
        refs,
      ),
    };
  });
}

/**
 * Reads and copies one nullable SQLite BLOB.
 *
 * @param row SQLite result row.
 * @param key Selected column name.
 * @returns An isolated byte copy, or undefined for SQL NULL.
 */
function readOptionalBytes(
  row: Readonly<Record<string, unknown>>,
  key: string,
): Uint8Array | undefined {
  const value = row[key];
  if (value === null) {
    return undefined;
  }
  return readBytes(row, key);
}

/**
 * Reads and copies one required SQLite BLOB.
 *
 * @param row SQLite result row.
 * @param key Selected column name.
 * @returns An isolated nonempty byte copy.
 */
function readBytes(
  row: Readonly<Record<string, unknown>>,
  key: string,
): Uint8Array {
  const value = row[key];
  if (!(value instanceof Uint8Array) || value.byteLength === 0) {
    throw new StoreSignal("corrupt");
  }
  return copyBytes(value);
}

/**
 * Returns an isolated copy of canonical bytes.
 *
 * @param value Canonical bytes crossing the storage boundary.
 * @returns A detached copy suitable for SQLite or a caller.
 */
function copyBytes(value: Uint8Array): Uint8Array {
  return new Uint8Array(value);
}

function readPart(
  bytes: Uint8Array,
  kind: "operation",
): Effect.Effect<
  { readonly message: Message; readonly value: OperationValue },
  EndpointStoreError
>;

function readPart(
  bytes: Uint8Array,
  kind: "close",
): Effect.Effect<
  { readonly message: Message; readonly value: CloseValue },
  EndpointStoreError
>;

function readPart(bytes: Uint8Array, kind: "operation" | "close") {
  return Effect.gen(function* () {
    const message = yield* decodeRuntimeValue(Message, bytes);
    const value = yield* messageInterpretation(message).pipe(
      Effect.catchTag(
        "CollectivePartInvalidError",
        () => new EndpointStoreError({ reason: "corrupt" }),
      ),
    );
    if (Option.isNone(value) || value.value.kind !== kind) {
      return yield* new EndpointStoreError({ reason: "corrupt" });
    }
    return { message, value: value.value };
  });
}

function decodeValue<A, I>(schema: Schema.Schema<A, I>, value: unknown) {
  return Schema.decodeUnknown(schema)(value).pipe(
    Effect.catchTag(
      "ParseError",
      () => new EndpointStoreError({ reason: "corrupt" }),
    ),
  );
}
