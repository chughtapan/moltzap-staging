/** @file Selects SQL drivers and compiles typed queries for Effect-owned execution. */
import type { SqlError } from "@effect/sql/SqlError";
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

/** The rejected locator remains private because PostgreSQL URLs can contain credentials. */
export class DatabaseLocatorError extends Data.TaggedError(
  "DatabaseLocatorError",
) {}

/** SQLite and PostgreSQL expose the same SqlClient to all domain stores. */
export const databaseConnection = (locator: Redacted.Redacted) =>
  Layer.unwrapEffect(
    Effect.gen(function* () {
      const url = yield* Effect.try({
        try: () => new URL(Redacted.value(locator)),
        catch: () => new DatabaseLocatorError(),
      });
      if (url.protocol === "postgres:" || url.protocol === "postgresql:") {
        const layer: Layer.Layer<
          SqlClient.SqlClient,
          SqlError | ConfigError.ConfigError
        > = PgClient.layer({ url: locator });
        return layer;
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
      const layer: Layer.Layer<
        SqlClient.SqlClient,
        SqlError | ConfigError.ConfigError
      > = SqliteClient.layer({ filename, disableWAL: true });
      return layer;
    }).pipe(Effect.withSpan("databaseConnection")),
  );

/** Stores use Kysely column and query types without introducing another executor. */
export type { Compilable, Generated } from "kysely";
