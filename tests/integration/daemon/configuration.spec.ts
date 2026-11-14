/** @file Deployment prepares PostgreSQL storage and refuses storage that holds other tables. */
import { NodeContext } from "@effect/platform-node";
import { Migrator, SqlClient } from "@effect/sql";
import { scopedLive as it } from "@effect/vitest";
import { prepareDatabase } from "@social-harness/social-harness/service";
import { checkDatabase } from "@social-harness/social-harness/test";
import { ConfigProvider, Effect, Redacted } from "effect";
import { expect } from "vitest";
import { acquireUnpreparedPostgres } from "./postgres-fixture.js";

const postgresFixture = Effect.gen(function* () {
  const database = yield* acquireUnpreparedPostgres;
  const provider = ConfigProvider.fromMap(
    new Map([
      ["SHARNESS_DATABASE_URL", Redacted.value(database.configureLocator)],
    ]),
  );
  return {
    sql: database.admin,
    prepare: prepareDatabase.pipe(Effect.withConfigProvider(provider)),
    check: checkDatabase.pipe(
      Effect.provideService(SqlClient.SqlClient, database.admin),
    ),
    locator: database.configureLocator,
  };
});

it(
  "prepares fresh PostgreSQL storage independently of runtime settings",
  () =>
    Effect.gen(function* () {
      const test = yield* postgresFixture;
      expect(yield* test.check.pipe(Effect.flip)).toMatchObject({
        reason: "schema",
      });
      expect(
        yield* test.sql`SELECT tablename FROM pg_tables WHERE schemaname=current_schema()`,
      ).toEqual([]);
      yield* test.prepare;
      yield* test.check;
      expect(yield* test.sql`SELECT * FROM owner_bindings`).toHaveLength(0);
    }).pipe(Effect.provide(NodeContext.layer)),
  60_000,
);

it(
  "prepares PostgreSQL storage when its framework migration ledger is already empty",
  () =>
    Effect.gen(function* () {
      const test = yield* postgresFixture;
      yield* Migrator.make({})({ loader: Effect.succeed([]) }).pipe(
        Effect.provideService(SqlClient.SqlClient, test.sql),
      );
      yield* test.prepare;
      yield* test.check;
      expect(yield* test.sql`SELECT * FROM owner_bindings`).toHaveLength(0);
    }).pipe(Effect.provide(NodeContext.layer)),
  60_000,
);

it(
  "refuses incompatible PostgreSQL storage without modifying retained tables",
  () =>
    Effect.gen(function* () {
      const test = yield* postgresFixture;
      yield* test.sql`CREATE TABLE firewall_checks (value TEXT NOT NULL)`;
      yield* test.sql`INSERT INTO firewall_checks VALUES ('retained obstruction')`;
      expect(yield* test.prepare.pipe(Effect.flip)).toMatchObject({
        reason: "schema",
      });
      expect(yield* test.sql`SELECT * FROM firewall_checks`).toEqual([
        { value: "retained obstruction" },
      ]);
      expect(
        yield* test.sql`SELECT tablename FROM pg_tables WHERE schemaname=current_schema() ORDER BY tablename`,
      ).toEqual([{ tablename: "firewall_checks" }]);
    }).pipe(Effect.provide(NodeContext.layer)),
  60_000,
);
