/** @file Selects SQL drivers and compiles typed queries for Effect-owned execution. */
import type { SqlError } from "@effect/sql/SqlError";
import { FileSystem } from "@effect/platform";
import { SqlClient } from "@effect/sql";
import * as PgClient from "@effect/sql-pg/PgClient";
import * as SqliteClient from "@effect/sql-sqlite-node/SqliteClient";
import { type ConfigError, Data, Effect, Layer, Redacted } from "effect";
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
        return layer;
      }),
      Effect.withSpan("databaseConnection"),
    ),
  );

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

/** Stores use Kysely column and query types without introducing another executor. */
export type { Compilable, Generated } from "kysely";
