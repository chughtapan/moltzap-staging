/** @file Owns protocol tables, stored values and the SQL failure boundary. */

import { SqlClient } from "@effect/sql/SqlClient";
import { Cause, Data, Effect, Option, Schema } from "effect";
import { canonicalBytes, sameBytes } from "../../encoding/canonical-json.js";
import { AgentId } from "../../identity/agent.js";

/** One closed endpoint-store failure. */
export class EndpointStoreError extends Data.TaggedError("EndpointStoreError")<{
  readonly reason: EndpointStoreFailure;
}> {}

/** Pending signed bytes require an earlier release to drain the outbox. */
export class OutboxTooOldError extends Data.TaggedError("OutboxTooOldError") {}

/** Internal control-flow signal mapped to the closed store error. */
export class StoreSignal extends Error {
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
export interface RecoveredReanchor extends StagedReanchor {
  readonly canonicalCompletedReanchor?: Uint8Array;
}

/** One durable current position for a conversation. */
export interface ConversationPosition {
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
export const FIRST_MESSAGE_PREDECESSOR = "";

/** Durable position plus the local indexing aid that carries no authority. */
export interface InternalPosition extends ConversationPosition {
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
export const decodeRuntimeValue = <A, I>(
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

/** Driver failures and invalid stored values stay inside the closed persistence boundary. */
export function storeOperation<A, E, R>(
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

/** Related domain writes use the driver's connection and transaction context. */
export function transaction<A, E, R>(
  operation: Effect.Effect<A, E, R>,
): Effect.Effect<A, EndpointStoreError, R | SqlClient> {
  return Effect.flatMap(SqlClient, (sql) =>
    sql.withTransaction(operation),
  ).pipe(storeOperation);
}

/**
 * Rejects empty and NUL-bearing values before they reach SQLite.
 *
 * @param value Opaque private identifier or hash.
 */
export function requireText(value: string): void {
  if (value.length === 0 || value.includes("\u0000")) {
    throw new StoreSignal("invalid-input");
  }
}

/**
 * Rejects empty or non-byte canonical values before persistence.
 *
 * @param value Canonical representation bytes.
 */
export function requireBytes(value: Uint8Array): void {
  if (!(value instanceof Uint8Array) || value.byteLength === 0) {
    throw new StoreSignal("invalid-input");
  }
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
 * Reads one safe SQLite INTEGER.
 *
 * @param row SQLite result row when one was selected.
 * @param key Selected column name.
 * @returns The exact safe integer.
 */
export function readInteger(
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
 * Reads one nullable nonempty SQLite TEXT.
 *
 * @param row SQLite result row.
 * @param key Selected column name.
 * @returns The exact text, or undefined for SQL NULL.
 */
export function readOptionalText(
  row: Readonly<Record<string, unknown>>,
  key: string,
): string | undefined {
  const value = row[key];
  if (value === null) {
    return undefined;
  }
  return readText(row, key);
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
 * Reads one required nonempty SQLite TEXT.
 *
 * @param row SQLite result row when one was selected.
 * @param key Selected column name.
 * @returns The exact nonempty text.
 */
export function readText(
  row: Readonly<Record<string, unknown>>,
  key: string,
): string {
  const value = row[key];
  if (typeof value !== "string" || value.length === 0) {
    throw new StoreSignal("corrupt");
  }
  return value;
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
 * Reads and copies one nullable SQLite BLOB.
 *
 * @param row SQLite result row.
 * @param key Selected column name.
 * @returns An isolated byte copy, or undefined for SQL NULL.
 */
export function readOptionalBytes(
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
export function readBytes(
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
export function copyBytes(value: Uint8Array): Uint8Array {
  return new Uint8Array(value);
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

/**
 * Requires canonical byte equality for idempotent state.
 *
 * @param left Previously retained canonical bytes.
 * @param right Candidate canonical bytes.
 */
export function requireSameBytes(left: Uint8Array, right: Uint8Array): void {
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
export function requireEqual<Value>(left: Value, right: Value): void {
  if (left !== right) {
    throw new StoreSignal("conflict");
  }
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
