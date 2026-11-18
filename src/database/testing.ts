/** @file Gives a test a PostgreSQL schema of its own on the server `SHARNESS_PG_TEST_URL` names. */

import { SqlClient } from "@effect/sql";
import { Config, Context, Effect, Layer, Redacted } from "effect";
import { randomBytes } from "node:crypto";
import { migrateDaemon } from "./migrations.js";
import { databaseConnection } from "./sql.js";

const serverUrl = Config.redacted("SHARNESS_PG_TEST_URL").pipe(
  Effect.catchTag("ConfigError", () =>
    Effect.dieMessage(
      "Tests that use a database need a PostgreSQL server: start one, for example with `docker run --detach --publish 127.0.0.1:5432:5432 --env POSTGRES_HOST_AUTH_METHOD=trust postgres:17`, and set SHARNESS_PG_TEST_URL to a login on it that can create schemas and roles, for example postgres://postgres@127.0.0.1:5432/postgres.",
    ),
  ),
);

/**
 * Creates a schema that only the caller's scope uses and connects to it as the
 * login of `SHARNESS_PG_TEST_URL`; closing the scope drops the schema. The
 * schema holds the daemon's tables unless `prepared` is false. `url` reaches
 * the same schema from another connection or process.
 */
export const openTestDatabase = Effect.fn("openTestDatabase")(function* (
  options: { readonly prepared?: boolean } = {},
) {
  const schema = `test_${randomBytes(8).toString("hex")}`;
  const located = new URL(Redacted.value(yield* serverUrl));
  located.searchParams.set(
    "options",
    `-c search_path=${schema} -c synchronous_commit=off`,
  );
  const url = Redacted.make(located.href);
  const sql = Context.get(
    yield* Layer.build(databaseConnection(url)),
    SqlClient.SqlClient,
  );
  yield* sql`CREATE SCHEMA ${sql(schema)}`;
  yield* Effect.addFinalizer(() =>
    sql`DROP SCHEMA ${sql(schema)} CASCADE`.pipe(Effect.orDie),
  );
  if (options.prepared !== false) {
    yield* migrateDaemon.pipe(Effect.provideService(SqlClient.SqlClient, sql));
  }
  return {
    schema,
    url,
    sql,
    run: <A, E, R>(effect: Effect.Effect<A, E, R>) =>
      effect.pipe(Effect.provideService(SqlClient.SqlClient, sql)),
  };
}, Effect.orDie);

/** A test's database. A fixture given one serves it again, as a restart does. */
export type TestDatabase = Effect.Effect.Success<
  ReturnType<typeof openTestDatabase>
>;

/**
 * Creates a login of `database`'s tables that can read and write rows but can
 * neither create objects nor bypass row policies, as the login a daemon serves
 * through cannot. Returns the URL that reaches the schema as that login;
 * closing the scope drops it.
 */
export const restrictedLogin = Effect.fn("restrictedLogin")(function* ({
  schema,
  url,
  sql,
}: TestDatabase) {
  const role = `runtime_${schema}`;
  const password = randomBytes(24).toString("hex");
  yield* sql`CREATE ROLE ${sql(role)} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS PASSWORD ${sql.literal("'" + password + "'")}`;
  yield* Effect.addFinalizer(() =>
    sql`DROP OWNED BY ${sql(role)}`.pipe(
      Effect.zipRight(sql`DROP ROLE ${sql(role)}`),
      Effect.orDie,
    ),
  );
  yield* sql`GRANT USAGE ON SCHEMA ${sql(schema)} TO ${sql(role)}`;
  yield* sql`GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA ${sql(schema)} TO ${sql(role)}`;
  yield* sql`GRANT USAGE ON ALL SEQUENCES IN SCHEMA ${sql(schema)} TO ${sql(role)}`;
  const restricted = new URL(Redacted.value(url));
  restricted.username = role;
  restricted.password = password;
  return Redacted.make(restricted.href);
}, Effect.orDie);
