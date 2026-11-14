/** @file The daemon loads its settings from one file, resolves the policy and norms that file names, and refuses a document it cannot serve. */
import { FileSystem } from "@effect/platform";
import { NodeContext } from "@effect/platform-node";
import { scopedLive as it } from "@effect/vitest";
import { ConfigProvider, Effect, FastCheck as fc, Redacted } from "effect";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { expect } from "vitest";
import { loadDaemonSettings } from "./config.js";

const secret = "configuration-only-test-bearer-0000000001";
const settingsVariable = "SHARNESS_CONFIG_FILE";
const serverCredentialVariable = "SHARNESS_SERVER_AGENT_CREDENTIAL";
const rulesFile = "rules.json";
const providerKey = "private-decision-provider-key";
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

/**
 * A message policy file in `beneath`, a directory under the settings file's
 * own, with a decision provider and its key when `judged`. Returns the
 * settings that name them.
 */
function messagePolicy(
  test: Effect.Effect.Success<typeof fixture>,
  judged: boolean,
  beneath = "",
) {
  return Effect.gen(function* () {
    const directory = join(test.directory, beneath);
    yield* test.filesystem.makeDirectory(directory, { recursive: true });
    yield* test.filesystem.writeFileString(
      join(directory, "policy.json"),
      JSON.stringify({
        format: "org.social-harness/message-policy/v2",
        ...(judged
          ? { decisionProvider: { kind: "jev", model: "configured-model" } }
          : {}),
      }),
    );
    yield* test.filesystem.writeFileString(
      join(directory, "provider.key"),
      providerKey,
    );
    return {
      messagePolicyFile: join(beneath, "policy.json"),
      ...(judged
        ? { decisionProviderKeyFile: join(beneath, "provider.key") }
        : {}),
    };
  });
}

/**
 * A norms directory in `beneath` holding one shipped norm and the shipped
 * rule file. `office-hours` gives English rules; `chat` gives none.
 */
function norms(
  test: Effect.Effect.Success<typeof fixture>,
  norm: "office-hours" | "chat",
  beneath = "",
) {
  return Effect.gen(function* () {
    const directory = join(test.directory, beneath, "norms");
    yield* test.filesystem.makeDirectory(directory, { recursive: true });
    yield* Effect.forEach(
      [`examples/${norm}.lisp`, rulesFile],
      (file) =>
        test.filesystem.copyFile(
          join(shippedNorms, file),
          join(directory, file.replace("examples/", "")),
        ),
      { concurrency: 1 },
    );
    return { normsDirectory: join(beneath, "norms") };
  });
}

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

it.prop(
  "resolves the policy, the provider key and the norms directory relative to the settings file, however deep beneath it they sit",
  [fc.array(fc.stringMatching(/^[a-z0-9]{1,8}$/), { maxLength: 3 })],
  ([segments]) =>
    Effect.gen(function* () {
      const test = yield* fixture;
      const beneath = join(...segments);
      yield* test.write({
        ...settings,
        ...(yield* messagePolicy(test, true, beneath)),
        ...(yield* norms(test, "chat", beneath)),
      });

      const loaded = (yield* test.load).settings;

      expect(
        loaded.messagePolicy?.provider?.kind === "jev" &&
          Redacted.value(loaded.messagePolicy.provider.apiKey),
      ).toBe(providerKey);
      expect(loaded.norms?.initial).toEqual([
        yield* test.filesystem.readFileString(
          join(shippedNorms, "examples/chat.lisp"),
        ),
      ]);
    }).pipe(Effect.provide(NodeContext.layer)),
);

it("loads the decision provider that judges initial norms' English rules, though the policy has no text rules", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    yield* test.write({
      ...settings,
      ...(yield* messagePolicy(test, true)),
      ...(yield* norms(test, "office-hours")),
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
    yield* test.write({
      ...settings,
      ...(yield* policy(test)),
      ...(yield* norms(test, "office-hours")),
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

const knownSettings = new Set([
  ...Object.keys(settings),
  "serverAgentName",
  "agentName",
  "hideGroups",
  "hideCollectives",
  "messagePolicyFile",
  "decisionProviderKeyFile",
  "normsDirectory",
]);

/** One setting no daemon serves: a listener beyond this machine under a local bearer, a port outside 1 to 65535, a setting the document does not have, or a server agent name that is not an agent name. */
const unservable = fc.oneof(
  fc
    .oneof(fc.ipV4(), fc.domain())
    .filter((host) => host !== settings.host)
    .map((host) => ({ host })),
  fc
    .oneof(
      fc.integer({ min: -65_535, max: 0 }),
      fc.integer({ min: 65_536, max: 1_000_000 }),
      fc.integer({ min: 1, max: 65_534 }).map((port) => port + 0.5),
    )
    .map((port) => ({ port })),
  fc
    .stringMatching(/^[a-zA-Z]{3,12}$/)
    .filter((name) => !knownSettings.has(name))
    .map((name) => ({ [name]: settings.port })),
  fc
    .oneof(
      fc.stringMatching(/^[A-Z ]{3,12}$/),
      fc.stringMatching(/^[a-z]{1,2}$/),
    )
    .map((serverAgentName) => ({ serverAgentName })),
);

it.prop(
  "refuses a document with a setting it cannot serve, without disclosing the bearer",
  [unservable, fc.stringMatching(/^[A-Za-z0-9]{32,48}$/)],
  ([setting, credential]) =>
    Effect.gen(function* () {
      const test = yield* fixture;
      yield* test.write({
        ...settings,
        authentication: { kind: "local", credential },
        ...setting,
      });

      const failure = yield* test.load.pipe(Effect.flip);

      expect(failure).toMatchObject({ reason: "document" });
      expect(JSON.stringify(failure)).not.toContain(credential);
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
