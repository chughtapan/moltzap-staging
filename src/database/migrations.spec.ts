/** @file Deployment prepares a database's schema without touching what it cannot adopt, and the daemon's check changes nothing. */
import { Migrator } from "@effect/sql";
import { scopedLive as it } from "@effect/vitest";
import { ConfigProvider, Effect, Redacted } from "effect";
import { expect } from "vitest";
import { checkDatabase, prepareDatabase } from "./migrations.js";
import { openTestDatabase } from "./testing.js";

/** A database nothing has prepared, and deployment's preparation of it. */
const fixture = Effect.gen(function* () {
  const { sql, url, run } = yield* openTestDatabase({ prepared: false });
  const tables = sql<{
    name: string;
  }>`SELECT tablename AS name FROM pg_tables WHERE schemaname=current_schema()`;
  const prepare = prepareDatabase.pipe(
    Effect.withConfigProvider(
      ConfigProvider.fromMap(
        new Map([["SHARNESS_DATABASE_URL", Redacted.value(url)]]),
      ),
    ),
  );
  return { sql, run, tables, prepare };
});

it("prepares a fresh database, which the check refuses until then without creating its schema", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    expect(yield* test.run(checkDatabase).pipe(Effect.flip)).toMatchObject({
      reason: "schema",
    });
    expect(yield* test.tables).toEqual([]);
    yield* test.prepare;
    yield* test.run(checkDatabase);
    expect(yield* test.sql`SELECT * FROM owner_bindings`).toHaveLength(0);
  }));

it("prepares a fresh database whose migration ledger already exists but is empty", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    yield* test.run(Migrator.make({})({ loader: Effect.succeed([]) }));
    yield* test.prepare;
    yield* test.run(checkDatabase);
    expect(yield* test.sql`SELECT * FROM owner_bindings`).toHaveLength(0);
  }));

it("rejects a database that holds other tables without changing their records", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    yield* test.sql`CREATE TABLE retained (value TEXT NOT NULL)`;
    yield* test.sql`INSERT INTO retained VALUES ('prior installation')`;
    expect(yield* test.prepare.pipe(Effect.flip)).toMatchObject({
      reason: "schema",
    });
    expect(yield* test.sql`SELECT * FROM retained`).toEqual([
      { value: "prior installation" },
    ]);
    expect(yield* test.tables).toEqual([{ name: "retained" }]);
  }));
