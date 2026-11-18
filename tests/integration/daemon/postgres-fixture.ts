/** @file A test database and the restricted login a daemon serves it through. */
import { openTestDatabase } from "@social-harness/social-harness/test";
import { Effect, Redacted } from "effect";
import { randomBytes } from "node:crypto";

/**
 * A prepared database of the caller's scope with a login of its own, which
 * can read and write rows but cannot create objects or bypass row policies.
 * `configureLocator` and `admin` reach it as the login that prepared it,
 * `runtimeLocator` as the restricted one.
 */
export const acquirePostgres = Effect.gen(function* () {
  const { schema, url, sql } = yield* openTestDatabase();
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
  const runtime = new URL(Redacted.value(url));
  runtime.username = role;
  runtime.password = password;
  return {
    configureLocator: url,
    runtimeLocator: Redacted.make(runtime.href),
    admin: sql,
  };
}).pipe(Effect.orDie, Effect.withSpan("acquirePostgres"));
