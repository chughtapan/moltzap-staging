/** @file Disposable PostgreSQL schemas and restricted runtime logins for daemon qualification. */
import { SqlClient } from "@effect/sql";
import {
  databaseConnection,
  migrateDaemon,
} from "@moltzap/social-harness/test";
import { PostgreSqlContainer } from "@testcontainers/postgresql";
import { Config, Context, Effect, Layer, Option, Redacted } from "effect";
import { randomBytes } from "node:crypto";

const databaseLocator = Config.redacted("SHARNESS_PG_TEST_URL").pipe(
  Config.option,
  Effect.flatMap(
    Option.match({
      onSome: Effect.succeed,
      onNone: () =>
        // eslint-disable-next-line agent-code-guard/acquire-release-requires-scope -- acquirePostgres requires its caller's Scope, which also owns the schema and runtime login.
        Effect.acquireRelease(
          Effect.tryPromise(() =>
            new PostgreSqlContainer("postgres:17-alpine")
              .withDatabase("qualification")
              .withUsername("qualification")
              .withPassword(randomBytes(24).toString("hex"))
              .start(),
          ),
          (container) =>
            Effect.tryPromise(() => container.stop()).pipe(
              Effect.asVoid,
              Effect.orDie,
            ),
        ).pipe(
          Effect.map((container) =>
            Redacted.make(container.getConnectionUri()),
          ),
        ),
    }),
  ),
);

/** Each scope owns its schema and login; runtime credentials cannot create objects or bypass row policies. */
export const acquirePostgres = acquirePostgresSchema(true);

/** Preparation tests begin before either domain tables or the migration ledger exist. */
export const acquireUnpreparedPostgres = acquirePostgresSchema(false);

function acquirePostgresSchema(migrated: boolean) {
  return Effect.gen(function* () {
    const locator = yield* databaseLocator;
    const root = Context.get(
      yield* Layer.build(databaseConnection(locator)),
      SqlClient.SqlClient,
    );
    const suffix = randomBytes(8).toString("hex");
    const schema = `qualification_${suffix}`;
    const role = `runtime_${suffix}`;
    const password = randomBytes(24).toString("hex");
    // eslint-disable-next-line agent-code-guard/acquire-release-requires-scope -- The caller's Scope owns the schema/login and outlives the connections released before this finalizer.
    yield* Effect.acquireRelease(
      root.withTransaction(
        Effect.gen(function* () {
          yield* root`CREATE SCHEMA ${root(schema)}`;
          yield* root`CREATE ROLE ${root(role)} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS PASSWORD ${root.literal("'" + password + "'")}`;
        }),
      ),
      () =>
        root`DROP SCHEMA ${root(schema)} CASCADE`.pipe(
          Effect.zipRight(root`DROP ROLE ${root(role)}`),
          Effect.orDie,
        ),
    );
    const schemaUrl = new URL(Redacted.value(locator));
    schemaUrl.searchParams.set("options", `-c search_path=${schema}`);
    const configureLocator = Redacted.make(schemaUrl.href);
    const admin = Context.get(
      yield* Layer.build(databaseConnection(configureLocator)),
      SqlClient.SqlClient,
    );
    if (migrated) {
      yield* migrateDaemon.pipe(
        Effect.provideService(SqlClient.SqlClient, admin),
      );
    }
    yield* root`GRANT USAGE ON SCHEMA ${root(schema)} TO ${root(role)}`;
    yield* root`GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA ${root(schema)} TO ${root(role)}`;
    yield* root`GRANT USAGE ON ALL SEQUENCES IN SCHEMA ${root(schema)} TO ${root(role)}`;
    schemaUrl.username = role;
    schemaUrl.password = password;
    return {
      configureLocator,
      runtimeLocator: Redacted.make(schemaUrl.href),
      admin,
    };
  }).pipe(Effect.withSpan("acquirePostgres"));
}
