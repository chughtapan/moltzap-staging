/** @file Prepares versioned daemon storage for deployment and checks its runtime compatibility. */

import { Migrator, SqlClient } from "@effect/sql";
import { Data, Effect, type Redacted } from "effect";
import { initializeDatabaseKey, verifyDatabaseKey } from "./secrets.js";
import {
  databaseAccess,
  databaseConnection,
  prepareDatabaseDirectory,
} from "./sql.js";

/** Identifies the table layout. A database with another number is not opened. */
const daemonDatabaseFormat = 6;

/** Rows of these tables are visible only to the agent whose key they carry. */
const agentTables = [
  "harness_items",
  "harness_visibility",
  "mcp_webhook_state",
  "firewall_checks",
  "firewall_policy",
  "firewall_activations",
  "message_log",
  "collective_operations",
  "collective_answers",
];

/** Encryption check, owners and the agents the daemon has created for them. */
const agentSchema = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE database_format (singleton INTEGER PRIMARY KEY CHECK(singleton=1), version INTEGER NOT NULL)`;
  yield* sql`INSERT INTO database_format(singleton,version) VALUES(1,${daemonDatabaseFormat})`;
  yield* sql`CREATE TABLE database_encryption (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1), verifier TEXT NOT NULL
  )`;
  yield* sql`CREATE TABLE owner_bindings (
    agent_key TEXT PRIMARY KEY,
    issuer TEXT NOT NULL,
    subject TEXT NOT NULL,
    UNIQUE(issuer, subject)
  )`;
  yield* sql`CREATE TABLE agent_profiles (
    agent_key TEXT PRIMARY KEY REFERENCES owner_bindings(agent_key),
    agent_id TEXT NOT NULL UNIQUE,
    agent_name TEXT NOT NULL UNIQUE,
    principal_id TEXT NOT NULL
  )`;
});

/** Each agent's inbox, webhook subscription and firewall. */
const hostSchema = (sequence: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql.unsafe(`CREATE TABLE harness_items (
    agent_key TEXT NOT NULL,
    sequence ${sequence},
    identity TEXT NOT NULL,
    token TEXT NOT NULL,
    address TEXT NOT NULL,
    kind TEXT NOT NULL,
    author TEXT,
    message_id TEXT,
    operation_id TEXT,
    incoming TEXT,
    view TEXT,
    status TEXT NOT NULL CHECK(status IN ('checking','withheld','pending','acknowledged')),
    UNIQUE(agent_key, identity),
    UNIQUE(agent_key, token)
  )`);
    yield* sql`CREATE INDEX harness_pending ON harness_items(agent_key,status,sequence)`;
    yield* sql`CREATE INDEX harness_conversation ON harness_items(agent_key,address,sequence)`;
    yield* sql`CREATE INDEX harness_message ON harness_items(agent_key,author,message_id)`;
    yield* sql`CREATE INDEX harness_request ON harness_items(agent_key,operation_id)`;
    yield* sql`CREATE TABLE harness_visibility (
    agent_key TEXT NOT NULL, author TEXT NOT NULL, message_id TEXT NOT NULL,
    visible INTEGER NOT NULL, PRIMARY KEY(agent_key, author, message_id)
  )`;
    yield* sql`CREATE TABLE mcp_webhook_state (agent_key TEXT PRIMARY KEY, state TEXT NOT NULL)`;
    yield* sql.unsafe(`CREATE TABLE firewall_checks (
    agent_key TEXT NOT NULL,
    sequence ${sequence},
    check_id TEXT NOT NULL,
    direction TEXT NOT NULL CHECK(direction IN ('incoming','outgoing')),
    candidate TEXT NOT NULL,
    input TEXT NOT NULL,
    result TEXT NOT NULL,
    decided_at BIGINT NOT NULL,
    disposition TEXT NOT NULL CHECK(disposition IN ('admitted','withheld')),
    UNIQUE(agent_key, check_id)
  )`);
    yield* sql`CREATE TABLE firewall_policy (
    agent_key TEXT PRIMARY KEY,
    policy TEXT NOT NULL
  )`;
    yield* sql.unsafe(`CREATE TABLE firewall_activations (
    agent_key TEXT NOT NULL,
    sequence ${sequence},
    activated_at BIGINT NOT NULL,
    active_revision TEXT NOT NULL,
    added_rule_ids TEXT NOT NULL,
    removed_rule_ids TEXT NOT NULL
  )`);
    yield* sql`CREATE INDEX firewall_activations_time ON firewall_activations(agent_key,activated_at,sequence)`;
    yield* sql`CREATE INDEX firewall_decisions_time ON firewall_checks(agent_key,decided_at,sequence)`;
  });

/** Each conversation's next position, each agent's messages, and the questions and answers built on them. */
const messageSchema = (sequence: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`CREATE TABLE communication_mode(singleton INTEGER PRIMARY KEY CHECK(singleton=1), kind TEXT NOT NULL)`;
    yield* sql`CREATE TABLE message_sequences(conversation_id TEXT PRIMARY KEY, next_ordinal INTEGER NOT NULL)`;
    yield* sql.unsafe(`CREATE TABLE message_log (
    agent_key TEXT NOT NULL, sequence ${sequence}, conversation_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK(ordinal >= 0), author_agent_id TEXT NOT NULL,
    msg_id TEXT NOT NULL, message TEXT NOT NULL,
    consumed INTEGER NOT NULL DEFAULT 0 CHECK(consumed IN (0,1)),
    UNIQUE(agent_key,author_agent_id,msg_id), UNIQUE(agent_key,conversation_id,ordinal)
  )`);
    yield* sql`CREATE INDEX message_log_delivery ON message_log(agent_key,consumed,sequence)`;
    yield* sql.unsafe(`CREATE TABLE collective_operations (
    agent_key TEXT NOT NULL,
    sequence ${sequence},
    operation_id TEXT NOT NULL,
    creator_agent_id TEXT NOT NULL,
    origin TEXT NOT NULL CHECK(origin IN ('local','remote')),
    request TEXT NOT NULL,
    deadline_at BIGINT NOT NULL,
    phase TEXT NOT NULL DEFAULT 'collecting' CHECK(phase IN ('collecting','completed')),
    result TEXT,
    answer_refs TEXT,
    delivered INTEGER NOT NULL DEFAULT 0 CHECK(delivered IN (0,1)),
    UNIQUE (agent_key, operation_id)
  )`);
    yield* sql.unsafe(`CREATE TABLE collective_answers (
    agent_key TEXT NOT NULL,
    sequence ${sequence},
    operation_id TEXT NOT NULL,
    member TEXT NOT NULL,
    message TEXT NOT NULL,
    FOREIGN KEY (agent_key,operation_id) REFERENCES collective_operations(agent_key,operation_id),
    UNIQUE (agent_key, operation_id,member)
  )`);
    yield* sql`CREATE INDEX collective_pending_results ON collective_operations(agent_key,delivered) WHERE phase='completed' AND delivered=0`;
  });

/** Every table the daemon uses, created in one step. */
const daemonSchema = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const sequence = sql.onDialectOrElse({
    pg: () => "INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY",
    orElse: () => "INTEGER PRIMARY KEY AUTOINCREMENT",
  });
  yield* agentSchema;
  yield* hostSchema(sequence);
  yield* messageSchema(sequence);
  yield* sql.onDialectOrElse({
    pg: () =>
      Effect.forEach(
        agentTables,
        (table) =>
          Effect.gen(function* () {
            yield* sql`ALTER TABLE ${sql(table)} ENABLE ROW LEVEL SECURITY`;
            yield* sql`ALTER TABLE ${sql(table)} FORCE ROW LEVEL SECURITY`;
            yield* sql`CREATE POLICY agent_owner ON ${sql(table)}
          USING (agent_key = current_setting('social_harness.agent_key', true))
          WITH CHECK (agent_key = current_setting('social_harness.agent_key', true))`;
          }),
        { concurrency: 1, discard: true },
      ),
    orElse: () => Effect.void,
  });
});

const migrationsTable = "effect_sql_migrations";
const migrationLoader = Migrator.fromRecord({
  "0001_daemon": daemonSchema,
});
const runMigrations = Migrator.make({})({
  loader: migrationLoader,
  table: migrationsTable,
});
/** PostgreSQL's missing-relation probe must finish before the atomic domain transaction begins. */
const initializeMigrationLedger = Migrator.make({})({
  loader: Effect.succeed([]),
  table: migrationsTable,
});

/**
 * Schema failures identify the operator action without disclosing SQL or unlock material.
 * Exported so the public preparation operation's inferred failure can be named.
 * @public
 */
export class DatabasePreparationError extends Data.TaggedError(
  "DatabasePreparationError",
)<{
  readonly reason: "schema" | "key" | "preparation";
}> {
  override get message(): string {
    return `Database ${this.reason} check failed; prepare a compatible database with its existing unlock key before starting the daemon`;
  }
}

/** SQLite's reset guard requires ledger creation to roll back with a failed first migration. */
export const migrateDaemon = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql.onDialectOrElse({
    sqlite: () => sql.withTransaction(runMigrations),
    orElse: () => runMigrations,
  });
}).pipe(Effect.withSpan("migrateDaemon"));

/** Fails unless the database has this format and `unlockKey` opens it. Changes nothing. */
export const checkDatabase = Effect.fn("checkDatabase")(
  function* (unlockKey: Redacted.Redacted<Uint8Array>) {
    const sql = yield* SqlClient.SqlClient;
    const [format] = yield* sql<{
      version: number;
    }>`SELECT version FROM database_format WHERE singleton=1`;
    if (format?.version !== daemonDatabaseFormat) {
      return yield* new DatabasePreparationError({ reason: "schema" });
    }
    yield* verifyDatabaseKey(unlockKey);
  },
  Effect.catchTags({
    DatabaseKeyError: () => new DatabasePreparationError({ reason: "key" }),
    SqlError: () => new DatabasePreparationError({ reason: "schema" }),
  }),
  Effect.withSpan("checkDatabase"),
);

/**
 * Deployment commits domain migrations and encryption metadata atomically.
 * Failed first PostgreSQL preparation can retain an empty Migrator bookkeeping table.
 */
export const prepareDatabase = Effect.gen(function* () {
  const access = yield* databaseAccess;
  yield* prepareDatabaseDirectory(access.locator);
  yield* Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* compatibleDatabase();
    yield* sql.onDialectOrElse({
      pg: () => initializeMigrationLedger,
      orElse: () => Effect.void,
    });
    yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* migrateDaemon;
        yield* initializeDatabaseKey(access.unlockKey).pipe(
          Effect.catchTags({
            DatabaseKeyError: () =>
              new DatabasePreparationError({ reason: "key" }),
            StoredCredentialError: () =>
              new DatabasePreparationError({ reason: "key" }),
          }),
        );
        yield* checkDatabase(access.unlockKey);
      }),
    );
  }).pipe(Effect.provide(databaseConnection(access.locator)));
}).pipe(
  Effect.scoped,
  Effect.catchTags({
    DatabaseAccessError: () =>
      new DatabasePreparationError({ reason: "preparation" }),
    DatabaseLocatorError: () =>
      new DatabasePreparationError({ reason: "preparation" }),
    ConfigError: () => new DatabasePreparationError({ reason: "preparation" }),
    BadArgument: () => new DatabasePreparationError({ reason: "preparation" }),
    SystemError: () => new DatabasePreparationError({ reason: "preparation" }),
    MigrationError: () => new DatabasePreparationError({ reason: "schema" }),
    SqlError: () => new DatabasePreparationError({ reason: "preparation" }),
  }),
  Effect.catchAllDefect(
    (defect) =>
      new DatabasePreparationError({
        reason:
          defect instanceof Migrator.MigrationError ? "schema" : "preparation",
      }),
  ),
  Effect.withSpan("prepareDatabase"),
);

/**
 * Only an empty database or one already in this format can be prepared. An
 * empty database may hold the migration ledger table with no rows.
 */
function compatibleDatabase() {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const tables = yield* sql.onDialectOrElse({
      pg: () =>
        sql<{
          name: string;
        }>`SELECT tablename AS name FROM pg_tables WHERE schemaname=current_schema()`,
      orElse: () =>
        sql<{
          name: string;
        }>`SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'`,
    });
    if (tables.some((table) => table.name === "database_format")) {
      const [format] = yield* sql<{
        version: number;
      }>`SELECT version FROM database_format WHERE singleton=1`;
      if (format?.version === daemonDatabaseFormat) {
        return;
      }
      return yield* new DatabasePreparationError({ reason: "schema" });
    }
    if (tables.some((table) => table.name !== migrationsTable)) {
      return yield* new DatabasePreparationError({ reason: "schema" });
    }
    if (tables.length > 0) {
      const rows = yield* sql`SELECT 1 FROM ${sql(migrationsTable)} LIMIT 1`;
      if (rows.length > 0) {
        return yield* new DatabasePreparationError({ reason: "schema" });
      }
    }
  });
}
