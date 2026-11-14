/** @file Selects SQL drivers, compiles typed queries, and opens transactions, with work to run once one commits. */
import type { SqlError } from "@effect/sql/SqlError";
import { FileSystem } from "@effect/platform";
import { SqlClient } from "@effect/sql";
import * as PgClient from "@effect/sql-pg/PgClient";
import * as SqliteClient from "@effect/sql-sqlite-node/SqliteClient";
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
  SqliteAdapter,
  SqliteIntrospector,
  SqliteQueryCompiler,
} from "kysely";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";

/** Builders have no driver resources; compiled queries run through the current SqlClient. */
export const makeQueries = <Tables>() =>
  Effect.map(
    SqlClient.SqlClient,
    (sql) =>
      new Kysely<Tables>({
        dialect: sql.onDialectOrElse({
          pg: () => ({
            createAdapter: () => new PostgresAdapter(),
            createDriver: () => new DummyDriver(),
            createIntrospector: (db: Kysely<Tables>) =>
              new PostgresIntrospector(db),
            createQueryCompiler: () => new PostgresQueryCompiler(),
          }),
          orElse: () => ({
            createAdapter: () => new SqliteAdapter(),
            createDriver: () => new DummyDriver(),
            createIntrospector: (db: Kysely<Tables>) =>
              new SqliteIntrospector(db),
            createQueryCompiler: () => new SqliteQueryCompiler(),
          }),
        }),
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

const parseDatabaseUrl = Effect.fn("parseDatabaseUrl")(function* (
  url: Redacted.Redacted,
) {
  const parsed = yield* Effect.try({
    try: () => new URL(Redacted.value(url)),
    catch: () => new DatabaseUrlError(),
  });
  if (parsed.protocol === "postgres:" || parsed.protocol === "postgresql:") {
    return { kind: "postgres" as const, url };
  }
  if (
    parsed.protocol !== "sqlite:" ||
    parsed.host !== "" ||
    parsed.search !== "" ||
    parsed.hash !== ""
  ) {
    return yield* new DatabaseUrlError();
  }
  const filename = yield* Effect.try({
    try: () =>
      parsed.pathname === ":memory:"
        ? ":memory:"
        : fileURLToPath(new URL(parsed.href.replace(/^sqlite:/u, "file:"))),
    catch: () => new DatabaseUrlError(),
  });
  return { kind: "sqlite" as const, filename };
});

/** SQLite and PostgreSQL expose the same SqlClient to all domain stores. */
export const databaseConnection = (url: Redacted.Redacted) =>
  Layer.unwrapEffect(
    parseDatabaseUrl(url).pipe(
      Effect.map((database) => {
        const layer: Layer.Layer<
          SqlClient.SqlClient,
          SqlError | ConfigError.ConfigError
        > =
          database.kind === "postgres"
            ? PgClient.layer({ url: database.url })
            : SqliteClient.layer({
                filename: database.filename,
                disableWAL: true,
              });
        return Layer.effectDiscard(connectionPragmas()).pipe(
          Layer.provideMerge(layer),
        );
      }),
      Effect.withSpan("databaseConnection"),
    ),
  );

/** Connection pragmas precede transactions so preparation and the daemon share SQLite durability. */
function connectionPragmas() {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql.onDialectOrElse({
      sqlite: () =>
        Effect.gen(function* () {
          yield* sql.unsafe("PRAGMA foreign_keys = ON");
          yield* sql.unsafe("PRAGMA journal_mode = WAL");
          yield* sql.unsafe("PRAGMA synchronous = FULL");
        }),
      orElse: () => Effect.void,
    });
  });
}

/** Setup creates private SQLite paths without truncating existing data; PostgreSQL and memory need no filesystem. */
export const prepareDatabaseDirectory = Effect.fn("prepareDatabaseDirectory")(
  function* (url: Redacted.Redacted) {
    const database = yield* parseDatabaseUrl(url);
    if (database.kind === "postgres" || database.filename === ":memory:") {
      return;
    }
    const filesystem = yield* FileSystem.FileSystem;
    yield* filesystem.makeDirectory(dirname(database.filename), {
      recursive: true,
      mode: 0o700,
    });
    yield* filesystem.open(database.filename, { flag: "a", mode: 0o600 });
    yield* filesystem.chmod(database.filename, 0o600);
  },
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
 * under `agentKey`. On PostgreSQL it sets `social_harness.agent_key`, which
 * every row-level policy compares; a nested call restores its caller's agent
 * key after its savepoint has closed.
 */
export const withAgentRows = Effect.fn("withAgentRows")(
  <A, E, R>(agentKey: string, work: Effect.Effect<A, E, R>) =>
    Effect.flatMap(SqlClient.SqlClient, (sql) =>
      sql.withTransaction(
        sql.onDialectOrElse({
          pg: () => postgresAgentRows(sql, agentKey, work),
          orElse: () => work,
        }),
      ),
    ),
);

/** The inner savepoint rolls back a failed query before restoring the outer transaction's agent key. */
function postgresAgentRows<A, E, R>(
  sql: SqlClient.SqlClient,
  agentKey: string,
  work: Effect.Effect<A, E, R>,
) {
  return Effect.gen(function* () {
    const [row] = yield* sql<{
      previous: string | null;
    }>`SELECT current_setting('social_harness.agent_key', true) AS previous`;
    // eslint-disable-next-line agent-code-guard/acquire-release-requires-scope -- acquireUseRelease brackets this transaction-local setting itself; no resource escapes the call.
    return yield* Effect.acquireUseRelease(
      sql`SELECT set_config('social_harness.agent_key', ${agentKey}, true)`,
      () => sql.withTransaction(work),
      () =>
        sql`SELECT set_config('social_harness.agent_key', ${row?.previous ?? ""}, true)`.pipe(
          Effect.orDie,
        ),
    );
  });
}

/** Stores use Kysely column and query types without introducing another executor. */
export type { Compilable, Generated } from "kysely";
