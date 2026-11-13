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
  Schema,
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
 * The rejected locator remains private because PostgreSQL URLs can contain credentials.
 * Exported so dependent declarations can name the inferred Effect failure.
 * @public
 */
export class DatabaseLocatorError extends Data.TaggedError(
  "DatabaseLocatorError",
) {}

/**
 * Database environment failures never include the supplied locator or unlock material.
 * Exported so the daemon layer's inferred failure can be named.
 * @public
 */
export class DatabaseAccessError extends Data.TaggedError(
  "DatabaseAccessError",
) {
  override get message(): string {
    return "Set SHARNESS_DATABASE_URL and a canonical base64url-encoded 32-byte SHARNESS_DATABASE_KEY";
  }
}

/** One database locator and canonical unlock key are shared by preparation and runtime. */
export const databaseAccess = Config.all({
  locator: Config.redacted("SHARNESS_DATABASE_URL"),
  encodedKey: Config.redacted("SHARNESS_DATABASE_KEY"),
}).pipe(
  Effect.flatMap(({ locator, encodedKey }) =>
    Schema.decodeUnknown(Schema.Uint8ArrayFromBase64Url)(
      Redacted.value(encodedKey),
    ).pipe(
      Effect.filterOrFail(
        (bytes) =>
          bytes.byteLength === 32 &&
          Buffer.from(bytes).toString("base64url") ===
            Redacted.value(encodedKey),
        () => new DatabaseAccessError(),
      ),
      Effect.map((bytes) => ({ locator, unlockKey: Redacted.make(bytes) })),
    ),
  ),
  Effect.catchTags({
    ConfigError: () => new DatabaseAccessError(),
    ParseError: () => new DatabaseAccessError(),
  }),
  Effect.withSpan("databaseAccess"),
);

const databaseLocation = Effect.fn("databaseLocation")(function* (
  locator: Redacted.Redacted,
) {
  const url = yield* Effect.try({
    try: () => new URL(Redacted.value(locator)),
    catch: () => new DatabaseLocatorError(),
  });
  if (url.protocol === "postgres:" || url.protocol === "postgresql:") {
    return { kind: "postgres" as const, locator };
  }
  if (
    url.protocol !== "sqlite:" ||
    url.host !== "" ||
    url.search !== "" ||
    url.hash !== ""
  ) {
    return yield* new DatabaseLocatorError();
  }
  const filename = yield* Effect.try({
    try: () =>
      url.pathname === ":memory:"
        ? ":memory:"
        : fileURLToPath(new URL(url.href.replace(/^sqlite:/u, "file:"))),
    catch: () => new DatabaseLocatorError(),
  });
  return { kind: "sqlite" as const, filename };
});

/** SQLite and PostgreSQL expose the same SqlClient to all domain stores. */
export const databaseConnection = (locator: Redacted.Redacted) =>
  Layer.unwrapEffect(
    databaseLocation(locator).pipe(
      Effect.map((location) => {
        const layer: Layer.Layer<
          SqlClient.SqlClient,
          SqlError | ConfigError.ConfigError
        > =
          location.kind === "postgres"
            ? PgClient.layer({ url: location.locator })
            : SqliteClient.layer({
                filename: location.filename,
                disableWAL: true,
              });
        return Layer.effectDiscard(connectionSettings()).pipe(
          Layer.provideMerge(layer),
        );
      }),
      Effect.withSpan("databaseConnection"),
    ),
  );

/** Connection pragmas precede transactions so preparation and runtime share SQLite durability. */
function connectionSettings() {
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
  function* (locator: Redacted.Redacted) {
    const location = yield* databaseLocation(locator);
    if (location.kind === "postgres" || location.filename === ":memory:") {
      return;
    }
    const filesystem = yield* FileSystem.FileSystem;
    yield* filesystem.makeDirectory(dirname(location.filename), {
      recursive: true,
      mode: 0o700,
    });
    yield* filesystem.open(location.filename, { flag: "a", mode: 0o600 });
    yield* filesystem.chmod(location.filename, 0o600);
  },
);

/** The work to run once the transaction a fiber is in commits. */
class Committed extends Context.Tag("SocialHarness/Committed")<
  Committed,
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
  if (Option.isSome(yield* Effect.serviceOption(Committed))) {
    return yield* sql.withTransaction(work);
  }
  const committed: Array<Effect.Effect<void>> = [];
  return yield* Effect.uninterruptibleMask((restore) =>
    sql
      .withTransaction(restore(work))
      .pipe(
        Effect.provideService(Committed, committed),
        Effect.zipLeft(
          Effect.suspend(() => Effect.all(committed, { discard: true })),
        ),
      ),
  );
});

/** Runs `work` once the open transaction commits, or at once when none is open. */
export const afterCommit = (work: Effect.Effect<void>) =>
  Effect.serviceOption(Committed).pipe(
    Effect.flatMap(
      Option.match({
        onNone: () => work,
        onSome: (committed) => Effect.sync(() => committed.push(work)),
      }),
    ),
    Effect.asVoid,
  );

/** A nested tenant operation restores its caller after its savepoint has closed. */
export const withAgentTransaction = Effect.fn("withAgentTransaction")(
  <A, E, R>(agentKey: string, work: Effect.Effect<A, E, R>) =>
    Effect.flatMap(SqlClient.SqlClient, (sql) =>
      sql.withTransaction(
        sql.onDialectOrElse({
          pg: () => postgresAgentTransaction(sql, agentKey, work),
          orElse: () => work,
        }),
      ),
    ),
);

/** The inner savepoint rolls back a failed query before restoring the outer transaction's tenant. */
function postgresAgentTransaction<A, E, R>(
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
