/** @file Runtime files and deployment-prepared SQL preserve configuration and key ownership. */
import { FileSystem } from "@effect/platform";
import { NodeContext } from "@effect/platform-node";
import { Migrator, SqlClient } from "@effect/sql";
import { scopedLive as it } from "@effect/vitest";
import { prepareDatabase } from "@moltzap/social-harness/service";
import {
  checkDatabase,
  databaseAccess,
  databaseConnection,
  getOrCreateOwnedAgent,
  loadDaemonSettings,
  localOwner,
  migrateDaemon,
} from "@moltzap/social-harness/test";
import { ConfigProvider, Context, Effect, Layer, Redacted } from "effect";
import { randomBytes } from "node:crypto";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- Filesystem effects use FileSystem; Node supplies the POSIX permission constants.
import { constants } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { expect } from "vitest";
import { acquireUnpreparedPostgres } from "./postgres-fixture.js";

const signer = {
  crv: "Ed25519",
  kty: "OKP",
  x: "y1j1FUgbqjCPeQVEnllv-2euwn_s9DeDkfEh3gk_OJ0",
};
const secret = "configuration-only-test-bearer-0000000001";
const settingsVariable = "SHARNESS_CONFIG_FILE";
const settings = {
  port: 4319,
  host: "127.0.0.1",
  authentication: { kind: "local", credential: secret },
  registryOrigin: "https://registry.example",
  registrySignerPublicKey: signer,
  routerOrigin: "https://router.example",
  admissionCredential: "test-admission-credential",
};
const shippedNorms = fileURLToPath(
  new URL("../../../packages/social-harness/config/norms/", import.meta.url),
);

const fixture = Effect.gen(function* () {
  const filesystem = yield* FileSystem.FileSystem;
  const directory = yield* filesystem.makeTempDirectoryScoped({
    prefix: "sharness-settings-",
  });
  const filename = join(directory, "settings.json");
  const locator = pathToFileURL(join(directory, "daemon.sqlite3")).href.replace(
    /^file:/u,
    "sqlite:",
  );
  const key = randomBytes(32).toString("base64url");
  const providerFor = (database: string, unlockKey = key) =>
    ConfigProvider.fromMap(
      new Map([
        [settingsVariable, filename],
        ["SHARNESS_DATABASE_URL", database],
        ["SHARNESS_DATABASE_KEY", unlockKey],
      ]),
    );
  const provider = providerFor(locator);
  const write = (value: unknown) =>
    filesystem.writeFileString(filename, JSON.stringify(value));
  yield* write(settings);
  const access = yield* databaseAccess.pipe(
    Effect.withConfigProvider(provider),
  );
  const prepare = prepareDatabase.pipe(Effect.withConfigProvider(provider));
  const sql = Context.get(
    yield* Layer.build(databaseConnection(access.locator)),
    SqlClient.SqlClient,
  );
  return {
    filesystem,
    directory,
    filename,
    write,
    access,
    prepare,
    provider,
    providerFor,
    sql,
  };
});

const postgresFixture = Effect.gen(function* () {
  const database = yield* acquireUnpreparedPostgres;
  const key = randomBytes(32);
  const provider = ConfigProvider.fromMap(
    new Map([
      ["SHARNESS_DATABASE_URL", Redacted.value(database.configureLocator)],
      ["SHARNESS_DATABASE_KEY", key.toString("base64url")],
    ]),
  );
  return {
    sql: database.admin,
    prepare: prepareDatabase.pipe(Effect.withConfigProvider(provider)),
    check: checkDatabase(Redacted.make(key)).pipe(
      Effect.provideService(SqlClient.SqlClient, database.admin),
    ),
    locator: database.configureLocator,
  };
});

// @agent-code-guard/regression-only: These finite file, schema and key custody boundaries use real SQL and filesystem operations.
it("names the missing settings-file variable without opening storage", () =>
  loadDaemonSettings().pipe(
    Effect.withConfigProvider(ConfigProvider.fromMap(new Map())),
    Effect.flip,
    Effect.tap((failure) => {
      expect(failure).toMatchObject({ reason: "environment" });
      expect(failure.message).toContain(settingsVariable);
    }),
    Effect.provide(NodeContext.layer),
  ));

it("loads changed process settings from their file on every startup", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    const first = yield* loadDaemonSettings().pipe(
      Effect.withConfigProvider(test.provider),
    );
    expect(first.settings.port).toBe(settings.port);
    expect(first.settings.authentication.kind).toBe(
      settings.authentication.kind,
    );
    yield* test.write({ ...settings, port: settings.port + 1 });
    expect((yield* loadDaemonSettings(test.filename)).settings.port).toBe(
      settings.port + 1,
    );
    expect(
      yield* test.sql`SELECT name FROM sqlite_schema WHERE type='table'`,
    ).toEqual([]);
  }).pipe(Effect.provide(NodeContext.layer)));

it("requires the current settings file instead of retaining a stale SQL copy", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    yield* test.prepare;
    yield* test.filesystem.remove(test.filename);
    const failure = yield* loadDaemonSettings(test.filename).pipe(Effect.flip);
    expect(failure).toMatchObject({ reason: "document" });
    expect(yield* test.sql`SELECT * FROM owner_bindings`).toHaveLength(0);
    expect(JSON.stringify(failure)).not.toContain(secret);
  }).pipe(Effect.provide(NodeContext.layer)));

it("resolves policy references relative to the settings file", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    const policy = { format: "org.social-harness/message-policy/v2" };
    yield* test.filesystem.writeFileString(
      join(test.directory, "policy.json"),
      JSON.stringify(policy),
    );
    yield* test.write({ ...settings, messagePolicyFile: "policy.json" });
    expect(
      (yield* loadDaemonSettings(test.filename)).settings.messagePolicy,
    ).toBeDefined();
  }).pipe(Effect.provide(NodeContext.layer)));

it("resolves provider credential references relative to the settings file", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    const providerKey = "private-decision-provider-key";
    yield* test.filesystem.writeFileString(
      join(test.directory, "policy.json"),
      JSON.stringify({
        format: "org.social-harness/message-policy/v2",
        text: {},
        decisionProvider: { kind: "jev", model: "configured-model" },
      }),
    );
    yield* test.filesystem.writeFileString(
      join(test.directory, "provider.key"),
      providerKey,
    );
    yield* test.write({
      ...settings,
      messagePolicyFile: "policy.json",
      decisionProviderKeyFile: "provider.key",
    });
    const provider = (yield* loadDaemonSettings(test.filename)).settings
      .messagePolicy?.provider;
    expect(provider?.kind).toBe("jev");
    if (provider?.kind === "jev") {
      expect(Redacted.value(provider.apiKey)).toBe(providerKey);
    }
  }).pipe(Effect.provide(NodeContext.layer)));

it("resolves the norms directory relative to the settings file", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    const norms = join(test.directory, "norms");
    yield* test.filesystem.makeDirectory(norms);
    yield* test.filesystem.copyFile(
      join(shippedNorms, "chat.lisp"),
      join(norms, "chat.lisp"),
    );
    yield* test.filesystem.copyFile(
      join(shippedNorms, "rules.json"),
      join(norms, "rules.json"),
    );
    yield* test.write({ ...settings, normsDirectory: "norms" });
    const loaded = yield* loadDaemonSettings(test.filename);
    expect(loaded.settings.norms?.documents).toEqual(
      expect.arrayContaining([
        yield* test.filesystem.readFileString(join(shippedNorms, "chat.lisp")),
      ]),
    );
  }).pipe(Effect.provide(NodeContext.layer)));

/** A norms directory holding the shipped `office-hours`, whose documents give English rules. */
function normsWithRules(test: Effect.Effect.Success<typeof fixture>) {
  return Effect.gen(function* () {
    const norms = join(test.directory, "norms");
    yield* test.filesystem.makeDirectory(norms);
    yield* Effect.forEach(
      ["examples/office-hours.lisp", "rules.json"],
      (file) =>
        test.filesystem.copyFile(
          join(shippedNorms, file),
          join(norms, file.replace("examples/", "")),
        ),
      { concurrency: 1 },
    );
  });
}

it("adds the norms' English rules to the message policy as fixed rules", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    yield* normsWithRules(test);
    yield* test.filesystem.writeFileString(
      join(test.directory, "policy.json"),
      JSON.stringify({
        format: "org.social-harness/message-policy/v2",
        decisionProvider: { kind: "jev", model: "configured-model" },
      }),
    );
    yield* test.filesystem.writeFileString(
      join(test.directory, "provider.key"),
      "private-decision-provider-key",
    );
    yield* test.write({
      ...settings,
      normsDirectory: "norms",
      messagePolicyFile: "policy.json",
      decisionProviderKeyFile: "provider.key",
    });

    const loaded = yield* loadDaemonSettings(test.filename);

    expect(
      loaded.settings.messagePolicy?.policy.text?.fixedRules.map((rule) => ({
        id: rule.id,
        applies: rule.applies,
      })),
    ).toEqual([{ id: "office-hours-rule-1", applies: "both" }]);
    expect(loaded.settings.messagePolicy?.provider?.kind).toBe("jev");
  }).pipe(Effect.provide(NodeContext.layer)));

it("refuses norms with English rules when no message policy can check them", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    yield* normsWithRules(test);
    yield* test.write({ ...settings, normsDirectory: "norms" });

    const failure = yield* loadDaemonSettings(test.filename).pipe(Effect.flip);

    expect(failure).toMatchObject({ reason: "norms" });
  }).pipe(Effect.provide(NodeContext.layer)));

it("refuses a norms directory without its rule file", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    yield* test.filesystem.makeDirectory(join(test.directory, "norms"));
    yield* test.write({ ...settings, normsDirectory: "norms" });
    const failure = yield* loadDaemonSettings(test.filename).pipe(Effect.flip);
    expect(failure).toMatchObject({ reason: "norms" });
  }).pipe(Effect.provide(NodeContext.layer)));

it("accepts public OAuth settings without allocating an owner", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    const authentication = {
      kind: "oauth",
      resource: "https://mcp.example/mcp",
      issuer: "https://issuer.example/",
      scope: "use:mcp",
      allowedOrigins: ["https://owner.example"],
    };
    yield* test.write({ ...settings, host: "0.0.0.0", authentication });
    yield* test.prepare;
    const loaded = yield* loadDaemonSettings(test.filename);
    expect(loaded.settings.authentication).toEqual({
      ...authentication,
      resource: new URL(authentication.resource),
    });
    expect(yield* test.sql`SELECT * FROM owner_bindings`).toHaveLength(0);
  }).pipe(Effect.provide(NodeContext.layer)));

it.each([
  { ...settings, host: "0.0.0.0" },
  { ...settings, porrt: settings.port },
  { ...settings, port: 0 },
  { ...settings, registryOrigin: "https://registry.example/path" },
])(
  "rejects invalid settings without disclosing their credentials: %j",
  (document) =>
    Effect.gen(function* () {
      const test = yield* fixture;
      yield* test.write(document);
      const failure = yield* loadDaemonSettings(test.filename).pipe(
        Effect.flip,
      );
      expect(failure).toMatchObject({ reason: "document" });
      expect(JSON.stringify(failure)).not.toContain(secret);
      expect(JSON.stringify(failure)).not.toContain(
        settings.admissionCredential,
      );
    }).pipe(Effect.provide(NodeContext.layer)),
);

it("checks an unprepared database without creating schema or key metadata", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    const check = checkDatabase(test.access.unlockKey).pipe(
      Effect.provideService(SqlClient.SqlClient, test.sql),
    );
    expect(yield* check.pipe(Effect.flip)).toMatchObject({ reason: "schema" });
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
    expect(yield* test.sql`SELECT * FROM database_encryption`).toHaveLength(0);
    yield* test.prepare;
    yield* check;
  }).pipe(Effect.provide(NodeContext.layer)));

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
    expect(yield* test.sql`SELECT * FROM database_encryption`).toHaveLength(1);
  }).pipe(Effect.provide(NodeContext.layer)));

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
    expect(yield* test.sql`SELECT * FROM database_encryption`).toEqual(before);
    expect(yield* test.sql`SELECT * FROM owner_bindings`).toHaveLength(0);
  }).pipe(Effect.provide(NodeContext.layer)));

it("runtime refuses the wrong key before changing retained owner state", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    yield* test.prepare;
    const agent = yield* getOrCreateOwnedAgent(
      localOwner,
      test.access.unlockKey,
    ).pipe(Effect.provideService(SqlClient.SqlClient, test.sql));
    const before = yield* test.sql`SELECT * FROM database_encryption`;
    expect(
      yield* checkDatabase(Redacted.make(randomBytes(32))).pipe(
        Effect.provideService(SqlClient.SqlClient, test.sql),
        Effect.flip,
      ),
    ).toMatchObject({ reason: "key" });
    expect(yield* test.sql`SELECT * FROM database_encryption`).toEqual(before);
    expect(
      (yield* test.sql<{
        readonly agentKey: string;
      }>`SELECT agent_key AS "agentKey" FROM owner_bindings`).map(
        (row) => row.agentKey,
      ),
    ).toEqual([agent]);
  }).pipe(Effect.provide(NodeContext.layer)));

it("rolls back additive schema changes and migration rows when the retained key fails", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    yield* test.prepare;
    yield* test.sql`ALTER TABLE database_encryption RENAME TO daemon_settings`;
    yield* test.sql`ALTER TABLE daemon_settings RENAME COLUMN verifier TO settings`;
    yield* test.sql`DELETE FROM effect_sql_migrations WHERE migration_id=(SELECT MAX(migration_id) FROM effect_sql_migrations)`;
    const metadata = yield* test.sql`SELECT * FROM daemon_settings`;
    const migrations = yield* test.sql`SELECT * FROM effect_sql_migrations`;
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
    expect(yield* test.sql`SELECT * FROM daemon_settings`).toEqual(metadata);
    expect(yield* test.sql`SELECT * FROM effect_sql_migrations`).toEqual(
      migrations,
    );
    yield* test.prepare;
    yield* checkDatabase(test.access.unlockKey).pipe(
      Effect.provideService(SqlClient.SqlClient, test.sql),
    );
  }).pipe(Effect.provide(NodeContext.layer)));

it("deployment refuses missing verifier metadata when an orphaned profile remains", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    yield* test.prepare;
    yield* getOrCreateOwnedAgent(localOwner, test.access.unlockKey).pipe(
      Effect.provideService(SqlClient.SqlClient, test.sql),
    );
    yield* test.sql.unsafe("PRAGMA foreign_keys = OFF");
    yield* test.sql`DELETE FROM owner_bindings`;
    yield* test.sql`DELETE FROM database_encryption`;
    expect(yield* test.prepare.pipe(Effect.flip)).toMatchObject({
      reason: "key",
    });
    expect(yield* test.sql`SELECT * FROM database_encryption`).toHaveLength(0);
    expect(yield* test.sql`SELECT * FROM agent_profiles`).toHaveLength(1);
  }).pipe(Effect.provide(NodeContext.layer)));

it("deployment refuses to adopt missing verifier metadata for existing owners", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    yield* test.prepare;
    yield* getOrCreateOwnedAgent(localOwner, test.access.unlockKey).pipe(
      Effect.provideService(SqlClient.SqlClient, test.sql),
    );
    yield* test.sql`DELETE FROM database_encryption`;
    expect(yield* test.prepare.pipe(Effect.flip)).toMatchObject({
      reason: "key",
    });
    expect(yield* test.sql`SELECT * FROM database_encryption`).toHaveLength(0);
    expect(yield* test.sql`SELECT * FROM owner_bindings`).toHaveLength(1);
  }).pipe(Effect.provide(NodeContext.layer)));

it("rejects older SQLite state without changing its retained records", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    yield* test.sql`CREATE TABLE retained (value TEXT NOT NULL)`;
    yield* test.sql`INSERT INTO retained VALUES ('prior installation')`;
    yield* test.sql.unsafe("PRAGMA user_version = 1");
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

it("rolls back domain schema and migration rows when schema preparation fails", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    yield* test.sql`CREATE TABLE firewall_checks (value TEXT NOT NULL)`;
    yield* test.sql`INSERT INTO firewall_checks VALUES ('retained obstruction')`;
    yield* test.sql.unsafe("PRAGMA user_version = 3");
    expect(yield* test.prepare.pipe(Effect.flip)).toBeDefined();
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
    const directory = join(dirname(test.filename), "private café # space");
    const filename = join(directory, "daemon.sqlite3");
    const locator = pathToFileURL(filename).href.replace(/^file:/u, "sqlite:");
    yield* prepareDatabase.pipe(
      Effect.withConfigProvider(test.providerFor(locator)),
    );
    expect((yield* test.filesystem.stat(directory)).mode & 0o777).toBe(
      constants.S_IRWXU,
    );
    expect((yield* test.filesystem.stat(filename)).mode & 0o777).toBe(
      constants.S_IRUSR | constants.S_IWUSR,
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
      expect(yield* test.sql`SELECT * FROM database_encryption`).toHaveLength(
        1,
      );
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
      expect(yield* test.sql`SELECT * FROM database_encryption`).toHaveLength(
        1,
      );
    }).pipe(Effect.provide(NodeContext.layer)),
  60_000,
);

it(
  "rolls back failed PostgreSQL domain migrations while leaving only an empty bookkeeping ledger",
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
      expect(yield* test.sql`SELECT * FROM effect_sql_migrations`).toEqual([]);
      expect(
        yield* test.sql`SELECT tablename FROM pg_tables WHERE schemaname=current_schema() ORDER BY tablename`,
      ).toEqual([
        { tablename: "effect_sql_migrations" },
        { tablename: "firewall_checks" },
      ]);
    }).pipe(Effect.provide(NodeContext.layer)),
  60_000,
);

it(
  "rolls back pending PostgreSQL schema and ledger changes when the retained key fails",
  () =>
    Effect.gen(function* () {
      const test = yield* postgresFixture;
      yield* test.prepare;
      yield* test.sql`ALTER TABLE database_encryption RENAME TO daemon_settings`;
      yield* test.sql`ALTER TABLE daemon_settings RENAME COLUMN verifier TO settings`;
      yield* test.sql`DELETE FROM effect_sql_migrations WHERE migration_id=(SELECT MAX(migration_id) FROM effect_sql_migrations)`;
      const metadata = yield* test.sql`SELECT * FROM daemon_settings`;
      const migrations =
        yield* test.sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`;
      const provider = ConfigProvider.fromMap(
        new Map([
          ["SHARNESS_DATABASE_URL", Redacted.value(test.locator)],
          ["SHARNESS_DATABASE_KEY", randomBytes(32).toString("base64url")],
        ]),
      );
      expect(
        yield* prepareDatabase.pipe(
          Effect.withConfigProvider(provider),
          Effect.flip,
        ),
      ).toMatchObject({ reason: "key" });
      expect(yield* test.sql`SELECT * FROM daemon_settings`).toEqual(metadata);
      expect(
        yield* test.sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`,
      ).toEqual(migrations);
      yield* test.prepare;
      yield* test.check;
    }).pipe(Effect.provide(NodeContext.layer)),
  60_000,
);
