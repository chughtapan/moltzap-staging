/** @file Protocol tables and SQL transaction boundaries for the agent database. */

import * as SqlClient from "@effect/sql/SqlClient";
import { Cause, Effect, Option } from "effect";
import {
  EndpointStoreError,
  mapStoreFailure,
  readInteger,
  readText,
} from "./values.js";
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
  CREATE TABLE identity_binding (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    agent_id TEXT NOT NULL,
    canonical_agent_card BLOB NOT NULL
  ) STRICT;
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
/** Driver failures and invalid stored values stay inside the closed persistence boundary. */
export const storeOperation = <A, E, R>(
  operation: Effect.Effect<A, E, R>,
): Effect.Effect<A, EndpointStoreError, R> =>
  operation.pipe(
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
/** Related domain writes use the driver's connection and transaction context. */
export const transaction = <A, E, R>(
  operation: Effect.Effect<A, E, R>,
): Effect.Effect<A, EndpointStoreError, R | SqlClient.SqlClient> =>
  Effect.flatMap(SqlClient.SqlClient, (sql) =>
    sql.withTransaction(operation),
  ).pipe(storeOperation);
/** Reads the existing schema before startup can change retained state. */
export const inspectStoreState = (): Effect.Effect<
  StoreOpening,
  EndpointStoreError,
  SqlClient.SqlClient
> =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      "PRAGMA user_version",
    );
    const row = rows[0];
    if (row === undefined) {
      return yield* new EndpointStoreError({ reason: "corrupt" });
    }
    const version = readInteger(row, "user_version");
    if (version === SCHEMA_VERSION) {
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
      return "reopen" as const;
    }
    const objects = yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      "SELECT 1 FROM sqlite_schema WHERE type IN ('table','index','view','trigger') AND name NOT LIKE 'sqlite_%' LIMIT 1",
    );
    if (version !== 0 || objects.length !== 0) {
      return yield* new EndpointStoreError({ reason: "incompatible" });
    }
    return "create" as const;
  }).pipe(storeOperation, Effect.withSpan("inspectStoreState"));
/** Fresh databases receive the current protocol layout inside daemon startup's transaction. */
export const createCommunicationTables = (): Effect.Effect<
  void,
  EndpointStoreError,
  SqlClient.SqlClient
> =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    for (const statement of (schemaSql + collectiveSchemaSql)
      .split(";")
      .map((part) => part.trim())
      .filter((part) => part.length > 0)) {
      yield* sql.unsafe<Readonly<Record<string, unknown>>>(statement);
    }
    yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      `PRAGMA user_version = ${SCHEMA_VERSION}`,
    );
  }).pipe(storeOperation, Effect.withSpan("createCommunicationTables"));
