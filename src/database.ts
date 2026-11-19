/**
 * @file SQL access for every domain store: the PostgreSQL connection, typed
 * queries, transactions with work to run once one commits, and the migration
 * that prepares the daemon's database for deployment.
 */
import type { SqlError } from "@effect/sql/SqlError";
import { Migrator, SqlClient } from "@effect/sql";
import * as PgClient from "@effect/sql-pg/PgClient";
import {
  Config,
  type ConfigError,
  Context,
  Data,
  Effect,
  Layer,
  Option,
  Redacted,
} from "effect";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- Kysely only compiles SQL; Effect SqlClient owns every connection and execution.
import {
  type Compilable,
  DummyDriver,
  Kysely,
  PostgresAdapter,
  PostgresIntrospector,
  PostgresQueryCompiler,
} from "kysely";

/** Builders have no driver resources; compiled queries run through the current SqlClient. */
export const makeQueries = <Tables>() =>
  Effect.sync(
    () =>
      new Kysely<Tables>({
        dialect: {
          createAdapter: () => new PostgresAdapter(),
          createDriver: () => new DummyDriver(),
          createIntrospector: (db: Kysely<Tables>) =>
            new PostgresIntrospector(db),
          createQueryCompiler: () => new PostgresQueryCompiler(),
        },
      }),
  );

/** Compiled parameters use the active transaction connection, including its RLS context. */
export const executeQuery = <Row extends object>(query: Compilable<Row>) =>
  Effect.flatMap(SqlClient.SqlClient, (sql) => {
    const compiled = query.compile();
    return sql.unsafe<Row>(compiled.sql, compiled.parameters);
  });

/**
 * The rejected database URL remains private because PostgreSQL URLs can contain credentials.
 * Exported so dependent declarations can name the inferred Effect failure.
 * @public
 */
export class DatabaseUrlError extends Data.TaggedError("DatabaseUrlError") {}

/**
 * The environment names no database URL.
 * Exported so the daemon layer's inferred failure can be named.
 * @public
 */
export class DatabaseUrlMissingError extends Data.TaggedError(
  "DatabaseUrlMissingError",
) {
  override get message(): string {
    return "Set SHARNESS_DATABASE_URL to the database the daemon uses";
  }
}

/** The database URL that preparation and the daemon share. */
export const databaseUrl = Config.redacted("SHARNESS_DATABASE_URL").pipe(
  Effect.catchTag("ConfigError", () => new DatabaseUrlMissingError()),
  Effect.withSpan("databaseUrl"),
);

/** Every domain store reaches PostgreSQL through this SqlClient. */
export const databaseConnection = (url: Redacted.Redacted) =>
  Layer.unwrapEffect(
    Effect.try({
      try: () => new URL(Redacted.value(url)).protocol,
      catch: () => new DatabaseUrlError(),
    }).pipe(
      Effect.filterOrFail(
        (protocol) => protocol === "postgres:" || protocol === "postgresql:",
        () => new DatabaseUrlError(),
      ),
      Effect.map(
        (): Layer.Layer<
          SqlClient.SqlClient,
          SqlError | ConfigError.ConfigError
        > => PgClient.layer({ url }),
      ),
      Effect.withSpan("databaseConnection"),
    ),
  );

/** The work to run once the transaction a fiber is in commits. */
class AfterCommitWork extends Context.Tag("SocialHarness/AfterCommitWork")<
  AfterCommitWork,
  Array<Effect.Effect<void>>
>() {}

/**
 * Runs `work` in a transaction, or in a savepoint of the one already open.
 * Work handed to `afterCommit` inside it runs once the outermost transaction
 * has committed, and never when that transaction rolls back.
 */
export const transaction = Effect.fn("transaction")(function* <A, E, R>(
  work: Effect.Effect<A, E, R>,
) {
  const sql = yield* SqlClient.SqlClient;
  if (Option.isSome(yield* Effect.serviceOption(AfterCommitWork))) {
    return yield* sql.withTransaction(work);
  }
  const afterCommitWork: Array<Effect.Effect<void>> = [];
  return yield* Effect.uninterruptibleMask((restore) =>
    sql
      .withTransaction(restore(work))
      .pipe(
        Effect.provideService(AfterCommitWork, afterCommitWork),
        Effect.zipLeft(
          Effect.suspend(() => Effect.all(afterCommitWork, { discard: true })),
        ),
      ),
  );
});

/** Runs `work` once the open transaction commits, or at once when none is open. */
export const afterCommit = (work: Effect.Effect<void>) =>
  Effect.serviceOption(AfterCommitWork).pipe(
    Effect.flatMap(
      Option.match({
        onNone: () => work,
        onSome: (afterCommitWork) =>
          Effect.sync(() => afterCommitWork.push(work)),
      }),
    ),
    Effect.asVoid,
  );

/**
 * Runs `work` in a transaction that reads and writes only the rows stored
 * under `agentKey`. It sets `social_harness.agent_key`, which every row-level
 * policy compares. A nested call runs in a savepoint, which rolls back a
 * failed query, and restores its caller's agent key once the savepoint has
 * closed.
 */
export const withAgentRows = Effect.fn("withAgentRows")(
  <A, E, R>(agentKey: string, work: Effect.Effect<A, E, R>) =>
    Effect.flatMap(SqlClient.SqlClient, (sql) =>
      sql.withTransaction(
        Effect.gen(function* () {
          const [row] = yield* sql<{
            previous: string | null;
          }>`SELECT current_setting('social_harness.agent_key', true) AS previous`;
          return yield* sql`SELECT set_config('social_harness.agent_key', ${agentKey}, true)`.pipe(
            Effect.zipRight(sql.withTransaction(work)),
            Effect.ensuring(
              sql`SELECT set_config('social_harness.agent_key', ${row?.previous ?? ""}, true)`.pipe(
                Effect.orDie,
              ),
            ),
          );
        }),
      ),
    ),
);

/** Rows of these tables are visible only to the agent whose key they carry. */
const rowSecuredTables = [
  "inbox_entries",
  "inbox_read_numbers",
  "subscriptions",
  "firewall_checks",
  "firewall_policy",
  "firewall_activations",
  "message_log",
];

/** Owners and the cards of the agents the daemon has created for them. */
const identitySchema = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE owner_bindings (
    agent_key TEXT PRIMARY KEY,
    issuer TEXT NOT NULL,
    subject TEXT NOT NULL,
    UNIQUE(issuer, subject)
  )`;
  yield* sql`CREATE TABLE agent_cards (
    agent_key TEXT PRIMARY KEY REFERENCES owner_bindings(agent_key),
    agent_id TEXT NOT NULL UNIQUE,
    agent_name TEXT NOT NULL UNIQUE,
    principal_id TEXT NOT NULL
  )`;
});

/** Each agent's inbox, subscription and firewall. */
const inboxAndFirewallSchema = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE inbox_entries (
    agent_key TEXT NOT NULL,
    sequence INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    read_number INTEGER,
    address TEXT NOT NULL,
    author TEXT NOT NULL,
    message_id TEXT NOT NULL,
    unchecked TEXT,
    entry TEXT,
    status TEXT NOT NULL CHECK(status IN ('unchecked','withheld','allowed')),
    UNIQUE(agent_key, author, message_id),
    UNIQUE(agent_key, read_number)
  )`;
  yield* sql`CREATE INDEX inbox_entries_status ON inbox_entries(agent_key,status,sequence)`;
  yield* sql`CREATE INDEX inbox_entries_address ON inbox_entries(agent_key,address,sequence)`;
  yield* sql`CREATE TABLE inbox_read_numbers (agent_key TEXT PRIMARY KEY, newest INTEGER NOT NULL)`;
  yield* sql`CREATE TABLE subscriptions (agent_key TEXT PRIMARY KEY, subscription TEXT NOT NULL)`;
  yield* sql`CREATE TABLE firewall_checks (
    agent_key TEXT NOT NULL,
    sequence INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    check_id TEXT NOT NULL,
    message_key TEXT NOT NULL,
    direction TEXT NOT NULL CHECK(direction IN ('incoming','outgoing')),
    message TEXT NOT NULL,
    input TEXT NOT NULL,
    result TEXT NOT NULL,
    decided_at BIGINT NOT NULL,
    decision TEXT NOT NULL CHECK(decision IN ('allowed','refused','withheld')),
    UNIQUE(agent_key, check_id)
  )`;
  yield* sql`CREATE TABLE firewall_policy (
    agent_key TEXT PRIMARY KEY,
    policy TEXT NOT NULL
  )`;
  yield* sql`CREATE TABLE firewall_activations (
    agent_key TEXT NOT NULL,
    sequence INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    activated_at BIGINT NOT NULL,
    revision TEXT NOT NULL,
    added_rule_ids TEXT NOT NULL,
    removed_rule_ids TEXT NOT NULL
  )`;
  yield* sql`CREATE INDEX firewall_activations_time ON firewall_activations(agent_key,activated_at,sequence)`;
  yield* sql`CREATE INDEX firewall_checks_time ON firewall_checks(agent_key,decided_at,sequence)`;
  yield* sql`CREATE INDEX firewall_checks_message ON firewall_checks(agent_key,message_key,sequence)`;
});

/** Each conversation's next position and each agent's messages. */
const messageSchema = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE message_sequences(conversation_id TEXT PRIMARY KEY, next_ordinal INTEGER NOT NULL)`;
  yield* sql`CREATE TABLE message_log (
    agent_key TEXT NOT NULL, sequence INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, conversation_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK(ordinal >= 0), author_agent_id TEXT NOT NULL,
    msg_id TEXT NOT NULL, message TEXT NOT NULL,
    consumed INTEGER NOT NULL DEFAULT 0 CHECK(consumed IN (0,1)),
    UNIQUE(agent_key,author_agent_id,msg_id), UNIQUE(agent_key,conversation_id,ordinal)
  )`;
  yield* sql`CREATE INDEX message_log_delivery ON message_log(agent_key,consumed,sequence)`;
});

/** Every table the daemon uses, created in one step. */
const daemonSchema = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* identitySchema;
  yield* inboxAndFirewallSchema;
  yield* messageSchema;
  yield* Effect.forEach(
    rowSecuredTables,
    (table) =>
      Effect.gen(function* () {
        yield* sql`ALTER TABLE ${sql(table)} ENABLE ROW LEVEL SECURITY`;
        yield* sql`ALTER TABLE ${sql(table)} FORCE ROW LEVEL SECURITY`;
        yield* sql`CREATE POLICY agent_owner ON ${sql(table)}
          USING (agent_key = current_setting('social_harness.agent_key', true))
          WITH CHECK (agent_key = current_setting('social_harness.agent_key', true))`;
      }),
    { concurrency: 1, discard: true },
  );
});

const migrationsTable = "effect_sql_migrations";
const migrationLoader = Migrator.fromRecord({
  "0001_daemon": daemonSchema,
});
const runMigrations = Migrator.make({})({
  loader: migrationLoader,
  table: migrationsTable,
});
/** The Migrator's missing-relation probe must finish before the atomic domain transaction begins. */
const initializeMigrationLedger = Migrator.make({})({
  loader: Effect.succeed([]),
  table: migrationsTable,
});

/**
 * The daemon's tables could not be created. Messages name the operator's
 * action without disclosing SQL.
 * Exported so the daemon's inferred startup failure can be named.
 * @public
 */
export class DatabasePreparationError extends Data.TaggedError(
  "DatabasePreparationError",
)<{
  readonly reason: "schema" | "preparation";
}> {
  override get message(): string {
    return this.reason === "schema"
      ? "The database holds tables that are not this daemon's; give the daemon an empty database"
      : "The daemon could not create its tables; check that SHARNESS_DATABASE_URL names a reachable database its login may create tables in";
  }
}

/**
 * Creates the daemon's tables in an empty database, in one transaction, and
 * leaves a database that already holds them as it is, so a login that cannot
 * create tables serves a database another login prepared. A failed first
 * preparation can retain an empty Migrator bookkeeping table.
 */
export const prepareDatabase = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  if (yield* alreadyPrepared()) {
    return;
  }
  yield* initializeMigrationLedger;
  yield* sql.withTransaction(runMigrations);
}).pipe(
  Effect.catchTags({
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
 * Whether the database already holds the daemon's tables. Fails for a
 * database that holds anything else: an empty one may hold only the migration
 * ledger table, with no rows.
 */
function alreadyPrepared() {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const tables = yield* sql<{
      name: string;
    }>`SELECT tablename AS name FROM pg_tables WHERE schemaname=current_schema()`;
    if (tables.some((table) => table.name === "owner_bindings")) {
      return true;
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
    return false;
  });
}

/** Stores use Kysely column and query types without introducing another executor. */
export type { Compilable, Generated } from "kysely";
