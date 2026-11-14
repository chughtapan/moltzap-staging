/** @file Deployment prepares a SQLite database's schema without touching what it cannot adopt, and the runtime check changes nothing. */
import { FileSystem } from "@effect/platform";
import { NodeContext } from "@effect/platform-node";
import { Migrator, SqlClient } from "@effect/sql";
import { scopedLive as it } from "@effect/vitest";
import {
  ConfigProvider,
  Context,
  Effect,
  Exit,
  FastCheck as fc,
  Layer,
} from "effect";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { expect } from "vitest";
import { checkDatabase, migrateDaemon, prepareDatabase } from "./migrations.js";
import { databaseAccess, databaseConnection } from "./sql.js";

/** Only the owner reaches the database directory and file. */
const privateDirectoryMode = 0o700;
const privateFileMode = 0o600;

/**
 * A SQLite file nothing has prepared, in a directory of its own, with a
 * connection to it and the environment that names it.
 * Each property run builds one, so the properties below bound their runs.
 */
const fixture = Effect.gen(function* () {
  const filesystem = yield* FileSystem.FileSystem;
  const directory = yield* filesystem.makeTempDirectoryScoped({
    prefix: "sharness-database-",
  });
  const locator = pathToFileURL(join(directory, "daemon.sqlite3")).href.replace(
    /^file:/u,
    "sqlite:",
  );
  const providerFor = (database: string) =>
    ConfigProvider.fromMap(new Map([["SHARNESS_DATABASE_URL", database]]));
  const provider = providerFor(locator);
  const access = yield* databaseAccess.pipe(
    Effect.withConfigProvider(provider),
  );
  const prepare = prepareDatabase.pipe(Effect.withConfigProvider(provider));
  const sql = Context.get(
    yield* Layer.build(databaseConnection(access.locator)),
    SqlClient.SqlClient,
  );
  return { filesystem, directory, access, prepare, providerFor, sql };
});

it("checks an unprepared database without creating its schema", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    const check = checkDatabase.pipe(
      Effect.provideService(SqlClient.SqlClient, test.sql),
    );
    expect(yield* check.pipe(Effect.flip)).toMatchObject({
      reason: "schema",
    });
    expect(
      yield* test.sql`SELECT name FROM sqlite_schema WHERE type='table'`,
    ).toEqual([]);
  }).pipe(Effect.provide(NodeContext.layer)));

it("prepares a fresh database whose migration ledger already exists but is empty", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    yield* Migrator.make({})({ loader: Effect.succeed([]) }).pipe(
      Effect.provideService(SqlClient.SqlClient, test.sql),
    );
    yield* test.prepare;
    yield* checkDatabase.pipe(
      Effect.provideService(SqlClient.SqlClient, test.sql),
    );
    expect(yield* test.sql`SELECT * FROM owner_bindings`).toHaveLength(0);
  }).pipe(Effect.provide(NodeContext.layer)));

it("rejects a SQLite database that holds other tables without changing their records", () =>
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
    expect(
      yield* test.sql`SELECT name FROM sqlite_schema WHERE type='table'`,
    ).toEqual([{ name: "retained" }]);
  }).pipe(Effect.provide(NodeContext.layer)));

it("leaves no table and no migration row behind when the migration fails", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    yield* test.sql`CREATE TABLE firewall_checks (value TEXT NOT NULL)`;
    yield* test.sql`INSERT INTO firewall_checks VALUES ('retained obstruction')`;
    const migrated = yield* migrateDaemon.pipe(
      Effect.provideService(SqlClient.SqlClient, test.sql),
      Effect.exit,
    );
    expect(Exit.isFailure(migrated)).toBe(true);
    expect(yield* test.sql`SELECT * FROM firewall_checks`).toEqual([
      { value: "retained obstruction" },
    ]);
    expect(
      yield* test.sql`SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'`,
    ).toEqual([{ name: "firewall_checks" }]);
  }).pipe(Effect.provide(NodeContext.layer)));

it("creates private encoded database paths only during deployment preparation", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    const directory = join(test.directory, "private café # space");
    const filename = join(directory, "daemon.sqlite3");
    const locator = pathToFileURL(filename).href.replace(/^file:/u, "sqlite:");
    yield* prepareDatabase.pipe(
      Effect.withConfigProvider(test.providerFor(locator)),
    );
    expect((yield* test.filesystem.stat(directory)).mode & 0o777).toBe(
      privateDirectoryMode,
    );
    expect((yield* test.filesystem.stat(filename)).mode & 0o777).toBe(
      privateFileMode,
    );
  }).pipe(Effect.provide(NodeContext.layer)));

it.prop(
  "refuses a SQLite locator that carries a query or a fragment, creating nothing and repeating none of it",
  [
    fc.constantFrom("?", "#"),
    fc.stringMatching(/^[a-z=&]{0,12}$/),
    fc.stringMatching(/^[0-9a-f]{16}$/),
  ],
  ([separator, text, marker]) =>
    Effect.gen(function* () {
      const test = yield* fixture;
      const directory = join(test.directory, "uncreated");
      const locator =
        pathToFileURL(join(directory, "daemon.sqlite3")).href.replace(
          /^file:/u,
          "sqlite:",
        ) +
        separator +
        text +
        marker;

      const failure = yield* prepareDatabase.pipe(
        Effect.withConfigProvider(test.providerFor(locator)),
        Effect.flip,
      );

      expect(JSON.stringify(failure)).not.toContain(marker);
      expect(yield* test.filesystem.exists(directory)).toBe(false);
    }).pipe(Effect.provide(NodeContext.layer)),
  { fastCheck: { numRuns: 20 } },
);
