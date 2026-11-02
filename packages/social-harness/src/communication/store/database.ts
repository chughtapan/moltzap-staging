/** @file Initializes protocol tables and isolates SQL failures and stored values. */

import { SqlClient } from "@effect/sql/SqlClient";
import { Cause, Data, Effect, Option } from "effect";
import { randomBytes } from "node:crypto";

import { sameBytes } from "../../encoding/canonical-json.js";

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

const SCHEMA_VERSION = 21;

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
    delivery_token TEXT NOT NULL UNIQUE,
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

/** Protocol metadata allows other owners to initialize their own tables independently. */
export function inspectStoreState(): Effect.Effect<
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
export function createCommunicationTables(): Effect.Effect<
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
export function mapStoreFailure(
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

/** Random durable identities are checked against retained bindings before use. */
export function mintDeliveryToken<E, R>(
  isRetained: (token: string) => Effect.Effect<boolean, E, R>,
): Effect.Effect<string, E | EndpointStoreError, R> {
  return Effect.gen(function* () {
    for (let attempt = 0; attempt < 8; attempt += 1) {
      const token = `dlv_${randomBytes(32).toString("base64url")}`;
      if (!(yield* isRetained(token))) {
        return token;
      }
    }
    return yield* new EndpointStoreError({ reason: "persistence" });
  }).pipe(Effect.withSpan("mintDeliveryToken"));
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
 * Requires two optional byte bindings to be both absent or byte-identical.
 *
 * @param prior Previously retained bytes, if any.
 * @param next Candidate bytes, if any.
 */
export function requireSameOptionalBytes(
  prior?: Uint8Array,
  next?: Uint8Array,
): void {
  if (prior === undefined || next === undefined) {
    if (prior !== next) {
      throw new StoreSignal("conflict");
    }
    return;
  }
  requireSameBytes(prior, next);
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
