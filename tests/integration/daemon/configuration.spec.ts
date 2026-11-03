/** @file Setup stores encrypted SQL configuration independently of its source document. */
import { FileSystem } from "@effect/platform";
import { NodeContext } from "@effect/platform-node";
import { SqlClient } from "@effect/sql";
import { scopedLive as it } from "@effect/vitest";
import {
  configureDaemon,
  databaseAccess,
  databaseConnection,
  loadDaemonSettings,
} from "@moltzap/social-harness/test";
import { ConfigProvider, Context, Effect, Layer, Redacted } from "effect";
import { randomBytes } from "node:crypto";
import { join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { expect } from "vitest";

import { runProcess } from "../../../scripts/test/packed-workspace.js";

const signer = {
  crv: "Ed25519",
  kty: "OKP",
  x: "y1j1FUgbqjCPeQVEnllv-2euwn_s9DeDkfEh3gk_OJ0",
};
const secret = "configuration-only-test-bearer-0000000001";
const settings = {
  port: 4319,
  host: "127.0.0.1",
  authentication: { kind: "local", credential: secret },
  registryOrigin: "https://registry.example",
  registrySignerPublicKey: signer,
  routerOrigin: "https://router.example",
  admissionCredential: "test-admission-credential",
};

const fixture = Effect.gen(function* () {
  const filesystem = yield* FileSystem.FileSystem;
  const directory = yield* filesystem.makeTempDirectoryScoped({
    prefix: "sharness-settings-",
  });
  const filename = join(directory, "setup.json");
  const locator = pathToFileURL(join(directory, "daemon.sqlite3")).href.replace(
    /^file:/u,
    "sqlite:",
  );
  const key = randomBytes(32).toString("base64url");
  const provider = ConfigProvider.fromMap(
    new Map([
      ["SHARNESS_DATABASE_URL", locator],
      ["SHARNESS_DATABASE_KEY", key],
    ]),
  );
  const write = (value: unknown) =>
    filesystem.writeFileString(filename, JSON.stringify(value));
  yield* write(settings);
  const configure = configureDaemon(filename).pipe(
    Effect.withConfigProvider(provider),
  );
  const access = yield* databaseAccess.pipe(
    Effect.withConfigProvider(provider),
  );
  return { filesystem, filename, write, configure, access, provider };
});

// eslint-disable-next-line agent-code-guard/no-example-only-tests -- These regressions cover persistence and configuration rejection boundaries.
it("recovers encrypted settings after the setup document is removed", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    const configured = yield* runProcess(
      process.execPath,
      [
        fileURLToPath(
          new URL(
            "../../../packages/social-harness/bin/sharnessd",
            import.meta.url,
          ),
        ),
        "configure",
        test.filename,
      ],
      {
        env: {
          SHARNESS_DATABASE_URL: Redacted.value(test.access.locator),
          SHARNESS_DATABASE_KEY: Buffer.from(
            Redacted.value(test.access.unlockKey),
          ).toString("base64url"),
        },
      },
    );
    expect(configured.exitCode, configured.stderr).toBe(0);
    yield* test.filesystem.remove(test.filename);
    const sql = Context.get(
      yield* Layer.build(databaseConnection(test.access.locator)),
      SqlClient.SqlClient,
    );
    const loaded = yield* loadDaemonSettings(test.access.unlockKey).pipe(
      Effect.provideService(SqlClient.SqlClient, sql),
    );
    expect(loaded.registryOrigin.href).toBe(
      new URL(settings.registryOrigin).href,
    );
    expect(loaded.authentication.kind).toBe(settings.authentication.kind);
    if (loaded.authentication.kind !== "local") {
      return yield* Effect.dieMessage("Expected the saved local credential");
    }
    expect(Redacted.value(loaded.authentication.credential)).toBe(secret);
    const records = yield* sql<{
      readonly settings: string;
    }>`SELECT settings FROM daemon_settings`;
    expect(JSON.stringify(records)).not.toContain(secret);
    expect(JSON.stringify(records)).not.toContain(settings.admissionCredential);
  }).pipe(Effect.provide(NodeContext.layer)));

it("accepts public OAuth setup without creating an agent before authentication", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    const document = {
      ...settings,
      host: "0.0.0.0",
      authentication: {
        kind: "oauth",
        resource: "https://mcp.example/mcp",
        issuer: "https://issuer.example/",
        scope: "use:mcp",
      },
    };
    yield* test.write(document);
    yield* test.configure;
    const sql = Context.get(
      yield* Layer.build(databaseConnection(test.access.locator)),
      SqlClient.SqlClient,
    );
    expect(yield* sql`SELECT * FROM owner_bindings`).toHaveLength(0);
    const loaded = yield* loadDaemonSettings(test.access.unlockKey).pipe(
      Effect.provideService(SqlClient.SqlClient, sql),
    );
    expect(loaded.authentication.kind).toBe(document.authentication.kind);
  }).pipe(Effect.provide(NodeContext.layer)));

it.each([
  { ...settings, host: "0.0.0.0" },
  { ...settings, porrt: 4319 },
  { ...settings, port: 0 },
  { ...settings, registryOrigin: "https://registry.example/path" },
])("rejects invalid setup without replacing saved settings", (document) =>
  Effect.gen(function* () {
    const test = yield* fixture;
    yield* test.configure;
    yield* test.write(document);
    const error = yield* test.configure.pipe(Effect.flip);
    expect(error).toBeDefined();
    expect(JSON.stringify(error)).not.toContain(secret);
    const sql = Context.get(
      yield* Layer.build(databaseConnection(test.access.locator)),
      SqlClient.SqlClient,
    );
    const loaded = yield* loadDaemonSettings(test.access.unlockKey).pipe(
      Effect.provideService(SqlClient.SqlClient, sql),
    );
    expect(loaded.port).toBe(settings.port);
  }).pipe(Effect.provide(NodeContext.layer)),
);

it("rejects a different unlock key without disclosing stored plaintext", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    yield* test.configure;
    const error = yield* loadDaemonSettings(
      Redacted.make(randomBytes(32)),
    ).pipe(
      Effect.provide(databaseConnection(test.access.locator)),
      Effect.flip,
    );
    expect(error).toBeDefined();
    expect(JSON.stringify(error)).not.toContain(secret);
  }).pipe(Effect.provide(NodeContext.layer)));

it("does not replace a configured database under a different unlock key", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    yield* test.configure;
    const wrong = ConfigProvider.fromMap(
      new Map([
        ["SHARNESS_DATABASE_URL", Redacted.value(test.access.locator)],
        ["SHARNESS_DATABASE_KEY", randomBytes(32).toString("base64url")],
      ]),
    );
    const error = yield* configureDaemon(test.filename).pipe(
      Effect.withConfigProvider(wrong),
      Effect.flip,
    );
    expect(error).toBeDefined();
    const saved = yield* loadDaemonSettings(test.access.unlockKey).pipe(
      Effect.provide(databaseConnection(test.access.locator)),
    );
    expect(saved.port).toBe(settings.port);
  }).pipe(Effect.provide(NodeContext.layer)));

it("rejects an older SQLite file without migrating or replacing its contents", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TABLE retained (value TEXT NOT NULL)`;
      yield* sql`INSERT INTO retained VALUES ('prior installation')`;
      yield* sql.unsafe("PRAGMA user_version = 1");
    }).pipe(Effect.provide(databaseConnection(test.access.locator)));
    const failure = yield* test.configure.pipe(Effect.flip);
    expect(failure).toMatchObject({ reason: "stored" });
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      expect(yield* sql`SELECT * FROM retained`).toEqual([
        { value: "prior installation" },
      ]);
      expect(
        yield* sql`SELECT name FROM sqlite_schema WHERE type = 'table'`,
      ).toEqual([{ name: "retained" }]);
    }).pipe(Effect.provide(databaseConnection(test.access.locator)));
  }).pipe(Effect.provide(NodeContext.layer)));
