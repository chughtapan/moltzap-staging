/** @file The daemon loads its settings from one file, resolves the policy and norms that file names, and refuses a document it cannot serve. */
import { FileSystem } from "@effect/platform";
import { NodeContext } from "@effect/platform-node";
import { scopedLive as it } from "@effect/vitest";
import { ConfigProvider, Effect, Redacted } from "effect";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect } from "vitest";
import { loadDaemonSettings } from "./config.js";

const secret = "configuration-only-test-bearer-0000000001";
const settingsVariable = "SHARNESS_CONFIG_FILE";
const serverCredentialVariable = "SHARNESS_SERVER_AGENT_CREDENTIAL";
const rulesFile = "rules.json";
const settings = {
  port: 4319,
  host: "127.0.0.1",
  authentication: { kind: "local", credential: secret },
};
const shippedNorms = fileURLToPath(
  new URL("../../config/norms/", import.meta.url),
);

/** A valid settings file in a directory of its own, loaded through the variable that names it. */
const fixture = Effect.gen(function* () {
  const filesystem = yield* FileSystem.FileSystem;
  const directory = yield* filesystem.makeTempDirectoryScoped({
    prefix: "sharness-settings-",
  });
  const filename = join(directory, "settings.json");
  const write = (value: unknown) =>
    filesystem.writeFileString(filename, JSON.stringify(value));
  yield* write(settings);
  const load = loadDaemonSettings().pipe(
    Effect.withConfigProvider(
      ConfigProvider.fromMap(new Map([[settingsVariable, filename]])),
    ),
  );
  return { filesystem, directory, filename, write, load };
});

/** A message policy file beside the settings, with a decision provider and its key when `judged`. */
function messagePolicy(
  test: Effect.Effect.Success<typeof fixture>,
  judged: boolean,
) {
  return Effect.gen(function* () {
    yield* test.filesystem.writeFileString(
      join(test.directory, "policy.json"),
      JSON.stringify({
        format: "org.social-harness/message-policy/v2",
        ...(judged
          ? { decisionProvider: { kind: "jev", model: "configured-model" } }
          : {}),
      }),
    );
    yield* test.filesystem.writeFileString(
      join(test.directory, "provider.key"),
      "private-decision-provider-key",
    );
    return {
      messagePolicyFile: "policy.json",
      ...(judged ? { decisionProviderKeyFile: "provider.key" } : {}),
    };
  });
}

/** A norms directory holding the shipped `office-hours`, whose documents give English rules. */
function normsWithRules(test: Effect.Effect.Success<typeof fixture>) {
  return Effect.gen(function* () {
    const norms = join(test.directory, "norms");
    yield* test.filesystem.makeDirectory(norms);
    yield* Effect.forEach(
      ["examples/office-hours.lisp", rulesFile],
      (file) =>
        test.filesystem.copyFile(
          join(shippedNorms, file),
          join(norms, file.replace("examples/", "")),
        ),
      { concurrency: 1 },
    );
  });
}

describe("the settings file", () => {
  it("names the missing settings-file variable", () =>
    loadDaemonSettings().pipe(
      Effect.withConfigProvider(ConfigProvider.fromMap(new Map())),
      Effect.flip,
      Effect.tap((failure) => {
        expect(failure).toMatchObject({ reason: "environment" });
        expect(failure.message).toContain(settingsVariable);
      }),
      Effect.provide(NodeContext.layer),
    ));

  it("loads the settings file its variable names", () =>
    Effect.gen(function* () {
      const test = yield* fixture;
      const loaded = yield* test.load;
      expect(loaded.settings.port).toBe(settings.port);
      expect(loaded.settings.authentication.kind).toBe(
        settings.authentication.kind,
      );
    }).pipe(Effect.provide(NodeContext.layer)));

  it("refuses a settings file that does not exist", () =>
    Effect.gen(function* () {
      const test = yield* fixture;
      yield* test.filesystem.remove(test.filename);
      expect(yield* test.load.pipe(Effect.flip)).toMatchObject({
        reason: "document",
      });
    }).pipe(Effect.provide(NodeContext.layer)));
});

describe("files the settings name", () => {
  it("resolves policy references relative to the settings file", () =>
    Effect.gen(function* () {
      const test = yield* fixture;
      const policy = { format: "org.social-harness/message-policy/v2" };
      yield* test.filesystem.writeFileString(
        join(test.directory, "policy.json"),
        JSON.stringify(policy),
      );
      yield* test.write({ ...settings, messagePolicyFile: "policy.json" });
      expect((yield* test.load).settings.messagePolicy).toBeDefined();
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
      const provider = (yield* test.load).settings.messagePolicy?.provider;
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
        join(shippedNorms, "examples/chat.lisp"),
        join(norms, "chat.lisp"),
      );
      yield* test.filesystem.copyFile(
        join(shippedNorms, rulesFile),
        join(norms, rulesFile),
      );
      yield* test.write({
        ...settings,
        ...(yield* messagePolicy(test, false)),
        normsDirectory: "norms",
      });
      const loaded = yield* test.load;
      expect(loaded.settings.norms?.initial).toEqual([
        yield* test.filesystem.readFileString(
          join(shippedNorms, "examples/chat.lisp"),
        ),
      ]);
    }).pipe(Effect.provide(NodeContext.layer)));
});

describe("a norms directory", () => {
  it("loads the decision provider that judges initial norms' English rules, though the policy has no text rules", () =>
    Effect.gen(function* () {
      const test = yield* fixture;
      yield* normsWithRules(test);
      yield* test.write({
        ...settings,
        ...(yield* messagePolicy(test, true)),
        normsDirectory: "norms",
      });

      const loaded = yield* test.load;

      expect(loaded.settings.norms?.initial).toHaveLength(1);
      expect(loaded.settings.messagePolicy?.policy.text).toBeUndefined();
      expect(loaded.settings.messagePolicy?.provider?.kind).toBe("jev");
    }).pipe(Effect.provide(NodeContext.layer)));

  it.each([
    { what: "no message policy", policy: () => Effect.succeed({}) },
    {
      what: "a message policy without a decision provider",
      policy: (test: Effect.Effect.Success<typeof fixture>) =>
        messagePolicy(test, false),
    },
  ])("refuses initial norms with English rules under $what", ({ policy }) =>
    Effect.gen(function* () {
      const test = yield* fixture;
      yield* normsWithRules(test);
      yield* test.write({
        ...settings,
        ...(yield* policy(test)),
        normsDirectory: "norms",
      });

      const failure = yield* test.load.pipe(Effect.flip);

      expect(failure).toMatchObject({ reason: "norms" });
    }).pipe(Effect.provide(NodeContext.layer)),
  );

  it("refuses a norms directory without its rule file, naming the file", () =>
    Effect.gen(function* () {
      const test = yield* fixture;
      yield* test.filesystem.makeDirectory(join(test.directory, "norms"));
      yield* test.write({
        ...settings,
        ...(yield* messagePolicy(test, false)),
        normsDirectory: "norms",
      });
      const failure = yield* test.load.pipe(Effect.flip);
      expect(failure).toMatchObject({ reason: "norms" });
      expect(failure.message).toContain(rulesFile);
    }).pipe(Effect.provide(NodeContext.layer)));
});

describe("the listener and who may sign in", () => {
  it("accepts OAuth settings on a public listener", () =>
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
      const loaded = yield* test.load;
      expect(loaded.settings.authentication).toEqual({
        ...authentication,
        resource: new URL(authentication.resource),
      });
    }).pipe(Effect.provide(NodeContext.layer)));

  it.each([
    { ...settings, host: "0.0.0.0" },
    { ...settings, porrt: settings.port },
    { ...settings, port: 0 },
    { ...settings, serverAgentName: "not a name" },
  ])(
    "rejects invalid settings without disclosing their credentials: %j",
    (document) =>
      Effect.gen(function* () {
        const test = yield* fixture;
        yield* test.write(document);
        const failure = yield* test.load.pipe(Effect.flip);
        expect(failure).toMatchObject({ reason: "document" });
        expect(JSON.stringify(failure)).not.toContain(secret);
      }).pipe(Effect.provide(NodeContext.layer)),
  );

  it("refuses a server agent without its credential and names the variable to set", () =>
    Effect.gen(function* () {
      const test = yield* fixture;
      yield* test.write({
        ...settings,
        serverAgentName: "server-agent",
      });
      const failure = yield* test.load.pipe(Effect.flip);
      expect(failure).toMatchObject({ reason: "environment" });
      expect(failure.message).toContain(serverCredentialVariable);
    }).pipe(Effect.provide(NodeContext.layer)));
});
