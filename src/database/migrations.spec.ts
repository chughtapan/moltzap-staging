/** @file Deployment prepares a SQLite database's schema and unlock-key verifier without touching what it cannot adopt, and the runtime check changes nothing. */
import { FileSystem } from "@effect/platform";
import { NodeContext } from "@effect/platform-node";
import { Migrator, SqlClient } from "@effect/sql";
import { scopedLive as it } from "@effect/vitest";
import { ConfigProvider, Context, Effect, Exit, Layer, Redacted } from "effect";
import { randomBytes } from "node:crypto";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { describe, expect } from "vitest";
import { createTestAgent } from "../__tests__/agents.js";
import { checkDatabase, migrateDaemon, prepareDatabase } from "./migrations.js";
import { databaseAccess, databaseConnection } from "./sql.js";

/** Only the owner reaches the database directory and file. */
const privateDirectoryMode = 0o700;
const privateFileMode = 0o600;

/**
 * A SQLite file nothing has prepared, in a directory of its own, with a
 * connection to it and the environment that names it and its unlock key.
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
  const key = randomBytes(32).toString("base64url");
  const providerFor = (database: string, unlockKey = key) =>
    ConfigProvider.fromMap(
      new Map([
        ["SHARNESS_DATABASE_URL", database],
        ["SHARNESS_DATABASE_KEY", unlockKey],
      ]),
    );
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

describe("the runtime check", () => {
  it("checks an unprepared database without creating schema or key metadata", () =>
    Effect.gen(function* () {
      const test = yield* fixture;
      const check = checkDatabase(test.access.unlockKey).pipe(
        Effect.provideService(SqlClient.SqlClient, test.sql),
      );
      expect(yield* check.pipe(Effect.flip)).toMatchObject({
        reason: "schema",
      });
      expect(
        yield* test.sql`SELECT name FROM sqlite_schema WHERE type='table'`,
      ).toEqual([]);
    }).pipe(Effect.provide(NodeContext.layer)));

  it("deployment initializes a schema-only database while runtime refuses its missing verifier", () =>
    Effect.gen(function* () {
      const test = yield* fixture;
      yield* migrateDaemon.pipe(
        Effect.provideService(SqlClient.SqlClient, test.sql),
      );
      const check = checkDatabase(test.access.unlockKey).pipe(
        Effect.provideService(SqlClient.SqlClient, test.sql),
      );
      expect(yield* check.pipe(Effect.flip)).toMatchObject({ reason: "key" });
      expect(yield* test.sql`SELECT * FROM database_encryption`).toHaveLength(
        0,
      );
      yield* test.prepare;
      yield* check;
    }).pipe(Effect.provide(NodeContext.layer)));

  it("runtime refuses the wrong key before changing retained owner state", () =>
    Effect.gen(function* () {
      const test = yield* fixture;
      yield* test.prepare;
      const agent = yield* createTestAgent("owner").pipe(
        Effect.provideService(SqlClient.SqlClient, test.sql),
      );
      const before = yield* test.sql`SELECT * FROM database_encryption`;
      expect(
        yield* checkDatabase(Redacted.make(randomBytes(32))).pipe(
          Effect.provideService(SqlClient.SqlClient, test.sql),
          Effect.flip,
        ),
      ).toMatchObject({ reason: "key" });
      expect(yield* test.sql`SELECT * FROM database_encryption`).toEqual(
        before,
      );
      expect(
        (yield* test.sql<{
          readonly agentKey: string;
        }>`SELECT agent_key AS "agentKey" FROM owner_bindings`).map(
          (row) => row.agentKey,
        ),
      ).toEqual([agent.agentKey]);
    }).pipe(Effect.provide(NodeContext.layer)));
});

describe("preparing a database that has a verifier or has lost it", () => {
  it("never overwrites an existing verifier under another key even with zero owners", () =>
    Effect.gen(function* () {
      const test = yield* fixture;
      yield* test.prepare;
      const before = yield* test.sql`SELECT * FROM database_encryption`;
      const failure = yield* prepareDatabase.pipe(
        Effect.withConfigProvider(
          test.providerFor(
            Redacted.value(test.access.locator),
            randomBytes(32).toString("base64url"),
          ),
        ),
        Effect.flip,
      );
      expect(failure).toMatchObject({ reason: "key" });
      expect(yield* test.sql`SELECT * FROM database_encryption`).toEqual(
        before,
      );
      expect(yield* test.sql`SELECT * FROM owner_bindings`).toHaveLength(0);
    }).pipe(Effect.provide(NodeContext.layer)));

  it("deployment refuses missing verifier metadata when an orphaned profile remains", () =>
    Effect.gen(function* () {
      const test = yield* fixture;
      yield* test.prepare;
      yield* createTestAgent("owner").pipe(
        Effect.provideService(SqlClient.SqlClient, test.sql),
      );
      yield* test.sql.unsafe("PRAGMA foreign_keys = OFF");
      yield* test.sql`DELETE FROM owner_bindings`;
      yield* test.sql`DELETE FROM database_encryption`;
      expect(yield* test.prepare.pipe(Effect.flip)).toMatchObject({
        reason: "key",
      });
      expect(yield* test.sql`SELECT * FROM database_encryption`).toHaveLength(
        0,
      );
      expect(yield* test.sql`SELECT * FROM agent_profiles`).toHaveLength(1);
    }).pipe(Effect.provide(NodeContext.layer)));

  it("deployment refuses to adopt missing verifier metadata for existing owners", () =>
    Effect.gen(function* () {
      const test = yield* fixture;
      yield* test.prepare;
      yield* createTestAgent("owner").pipe(
        Effect.provideService(SqlClient.SqlClient, test.sql),
      );
      yield* test.sql`DELETE FROM database_encryption`;
      expect(yield* test.prepare.pipe(Effect.flip)).toMatchObject({
        reason: "key",
      });
      expect(yield* test.sql`SELECT * FROM database_encryption`).toHaveLength(
        0,
      );
      expect(yield* test.sql`SELECT * FROM owner_bindings`).toHaveLength(1);
    }).pipe(Effect.provide(NodeContext.layer)));
});

describe("a database that already holds tables", () => {
  it("prepares a fresh database whose migration ledger already exists but is empty", () =>
    Effect.gen(function* () {
      const test = yield* fixture;
      yield* Migrator.make({})({ loader: Effect.succeed([]) }).pipe(
        Effect.provideService(SqlClient.SqlClient, test.sql),
      );
      yield* test.prepare;
      yield* checkDatabase(test.access.unlockKey).pipe(
        Effect.provideService(SqlClient.SqlClient, test.sql),
      );
      expect(yield* test.sql`SELECT * FROM database_encryption`).toHaveLength(
        1,
      );
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
});

describe("the SQLite file a locator names", () => {
  it("creates private encoded database paths only during deployment preparation", () =>
    Effect.gen(function* () {
      const test = yield* fixture;
      const directory = join(test.directory, "private café # space");
      const filename = join(directory, "daemon.sqlite3");
      const locator = pathToFileURL(filename).href.replace(
        /^file:/u,
        "sqlite:",
      );
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

  it.each(["?credential=", "#"])(
    "rejects invalid SQLite locators without creating their directories: %s",
    (suffix) =>
      Effect.gen(function* () {
        const test = yield* fixture;
        const marker = randomBytes(16).toString("hex");
        const directory = join(test.directory, "uncreated");
        const locator =
          pathToFileURL(join(directory, "daemon.sqlite3")).href.replace(
            /^file:/u,
            "sqlite:",
          ) +
          suffix +
          marker;
        const failure = yield* prepareDatabase.pipe(
          Effect.withConfigProvider(test.providerFor(locator)),
          Effect.flip,
        );
        expect(JSON.stringify(failure)).not.toContain(marker);
        expect(yield* test.filesystem.exists(directory)).toBe(false);
      }).pipe(Effect.provide(NodeContext.layer)),
  );
});
