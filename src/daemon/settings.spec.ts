/** @file The daemon loads its settings from one file, resolves the policy and norms that file names, and refuses a document it cannot serve. */
import { FileSystem } from "@effect/platform";
import { NodeContext } from "@effect/platform-node";
import { scopedLive as it } from "@effect/vitest";
import { ConfigProvider, Effect, FastCheck as fc, Redacted } from "effect";
import { join } from "node:path";
import { expect } from "vitest";
import { loadDaemonSettings } from "./settings.js";

const settingsVariable = "SHARNESS_CONFIG_FILE";
const providerKey = "private-decision-provider-key";
const settings = {
  port: 4319,
  host: "127.0.0.1",
  authentication: {
    resource: "https://mcp.example/mcp",
    issuer: "https://issuer.example/",
    scope: "use:mcp",
  },
};

/**
 * A valid settings file in a directory of its own, loaded through the variable
 * that names it. Each property run builds one, so the properties below bound
 * their runs.
 */
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

/** A norm document with no English rule. */
const unruledNorm = `(norm ping :initiator Asker :participant Answerer)
(content ask :by Asker :starts true :ends true)
`;

/** The same norm with an English rule, which needs a decision provider. */
const ruledNorm = `${unruledNorm}(rule :act ask "The question is polite.")
`;

/** A norms directory in `beneath` holding `document` as its one norm document. */
function norms(
  test: Effect.Effect.Success<typeof fixture>,
  document: string,
  beneath = "",
) {
  return Effect.gen(function* () {
    const directory = join(test.directory, beneath, "norms");
    yield* test.filesystem.makeDirectory(directory, { recursive: true });
    yield* test.filesystem.writeFileString(
      join(directory, "ping.lisp"),
      document,
    );
    return { normsDirectory: join(beneath, "norms") };
  });
}

it("names the missing settings-file variable", () =>
  loadDaemonSettings().pipe(
    Effect.withConfigProvider(ConfigProvider.fromMap(new Map())),
    Effect.flip,
    Effect.tap((failure) => {
      expect(failure.message).toContain(settingsVariable);
    }),
    Effect.provide(NodeContext.layer),
  ));

it("loads the settings file its variable names", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    const loaded = yield* test.load;
    expect(loaded.port).toBe(settings.port);
    expect(loaded.authentication).toEqual({
      ...settings.authentication,
      resource: new URL(settings.authentication.resource),
    });
  }).pipe(Effect.provide(NodeContext.layer)));

it("refuses a settings file that does not exist", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    yield* test.filesystem.remove(test.filename);
    expect((yield* test.load.pipe(Effect.flip)).message).toContain(
      "settings file",
    );
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
        ...(yield* norms(test, unruledNorm, beneath)),
      });

      const loaded = yield* test.load;

      expect(
        loaded.messagePolicy?.provider?.kind === "jev" &&
          Redacted.value(loaded.messagePolicy.provider.apiKey),
      ).toBe(providerKey);
      expect(
        loaded.norms?.contracts.map((contract) => contract.source),
      ).toEqual([unruledNorm]);
    }).pipe(Effect.provide(NodeContext.layer)),
  { fastCheck: { numRuns: 12 } },
);

it("loads the decision provider that judges initial norms' English rules, though the policy has no text rules", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    yield* test.write({
      ...settings,
      ...(yield* messagePolicy(test, true)),
      ...(yield* norms(test, ruledNorm)),
    });

    const loaded = yield* test.load;

    expect(loaded.norms?.contracts).toHaveLength(1);
    expect(loaded.messagePolicy?.policy.text).toBeUndefined();
    expect(loaded.messagePolicy?.provider?.kind).toBe("jev");
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
      ...(yield* norms(test, ruledNorm)),
    });

    const failure = yield* test.load.pipe(Effect.flip);

    expect(failure.message).toContain("configured norms");
  }).pipe(Effect.provide(NodeContext.layer)),
);

it("refuses a norms directory whose document does not compile", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    yield* test.write({
      ...settings,
      ...(yield* messagePolicy(test, false)),
      ...(yield* norms(test, "(norm ping :initiator Asker)")),
    });
    const failure = yield* test.load.pipe(Effect.flip);
    expect(failure.message).toContain("norms directory");
  }).pipe(Effect.provide(NodeContext.layer)));

const knownSettings = new Set([
  ...Object.keys(settings),
  "serverAgent",
  "hideGroups",
  "messagePolicyFile",
  "decisionProviderKeyFile",
  "normsDirectory",
]);

/** One setting no daemon serves: a port outside 1 to 65535, a setting the document does not have, or a server agent name that is not an agent name. */
const unservable = fc.oneof(
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
    .map((name) => ({ serverAgent: { name, subject: "server" } })),
);

it.prop(
  "refuses a document with a setting it cannot serve",
  [unservable],
  ([setting]) =>
    Effect.gen(function* () {
      const test = yield* fixture;
      yield* test.write({ ...settings, ...setting });

      const failure = yield* test.load.pipe(Effect.flip);

      expect(failure.message).toContain("settings file");
    }).pipe(Effect.provide(NodeContext.layer)),
  { fastCheck: { numRuns: 40 } },
);
