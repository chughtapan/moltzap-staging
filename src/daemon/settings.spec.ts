/** @file The daemon loads its settings from one file, resolves the policy and norms that file names, and refuses a document it cannot serve. */
import { FileSystem } from "@effect/platform";
import { NodeContext } from "@effect/platform-node";
import { scopedLive as it } from "@effect/vitest";
import {
  ConfigProvider,
  Effect,
  Either,
  FastCheck as fc,
  Layer,
  Redacted,
} from "effect";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { expect } from "vitest";
import { loadDaemonSettings, spanExport } from "./settings.js";

const settingsVariable = "SHARNESS_CONFIG_FILE";
const serverCredentialVariable = "SHARNESS_SERVER_AGENT_CREDENTIAL";
const rulesFile = "rules.json";
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
const shippedNorms = fileURLToPath(
  new URL("../../config/norms/", import.meta.url),
);

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
    expect(loaded.settings.authentication).toEqual({
      ...settings.authentication,
      resource: new URL(settings.authentication.resource),
      allowedOrigins: [],
    });
  }).pipe(Effect.provide(NodeContext.layer)));

it("refuses a settings file that does not exist", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    yield* test.filesystem.remove(test.filename);
    expect(yield* test.load.pipe(Effect.flip)).toMatchObject({
      reason: "settings-file",
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
  { fastCheck: { numRuns: 12 } },
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

it("loads the origins its authentication allows", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    const allowedOrigins = ["https://owner.example"];
    yield* test.write({
      ...settings,
      authentication: { ...settings.authentication, allowedOrigins },
    });
    const loaded = yield* test.load;
    expect(loaded.settings.authentication.allowedOrigins).toEqual(
      allowedOrigins,
    );
  }).pipe(Effect.provide(NodeContext.layer)));

const knownSettings = new Set([
  ...Object.keys(settings),
  "serverAgentName",
  "hideGroups",
  "hideCollectives",
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
    .map((serverAgentName) => ({ serverAgentName })),
);

it.prop(
  "refuses a document with a setting it cannot serve",
  [unservable],
  ([setting]) =>
    Effect.gen(function* () {
      const test = yield* fixture;
      yield* test.write({ ...settings, ...setting });

      const failure = yield* test.load.pipe(Effect.flip);

      expect(failure).toMatchObject({ reason: "settings-file" });
    }).pipe(Effect.provide(NodeContext.layer)),
  { fastCheck: { numRuns: 40 } },
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

const collector = "http://127.0.0.1:4318";
const protocolVariables = [
  "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL",
  "OTEL_EXPORTER_OTLP_PROTOCOL",
];
const endpointVariables = [
  "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
  "OTEL_EXPORTER_OTLP_ENDPOINT",
];

/** What span export does with standard OTLP variables: the variables a refusal's message names, or none when it starts. */
const spanExportRules: ReadonlyArray<{
  readonly rule: string;
  readonly variables: Readonly<Record<string, string>>;
  readonly refusal?: readonly string[];
}> = [
  {
    rule: "refuses a general protocol other than http/protobuf",
    variables: {
      OTEL_EXPORTER_OTLP_ENDPOINT: collector,
      OTEL_EXPORTER_OTLP_PROTOCOL: "grpc",
    },
    refusal: protocolVariables,
  },
  {
    rule: "refuses a traces protocol other than http/protobuf",
    variables: {
      OTEL_EXPORTER_OTLP_ENDPOINT: collector,
      OTEL_EXPORTER_OTLP_TRACES_PROTOCOL: "grpc",
    },
    refusal: protocolVariables,
  },
  {
    rule: "lets the traces protocol replace the general one",
    variables: {
      OTEL_EXPORTER_OTLP_ENDPOINT: collector,
      OTEL_EXPORTER_OTLP_TRACES_PROTOCOL: "http/protobuf",
      OTEL_EXPORTER_OTLP_PROTOCOL: "grpc",
    },
  },
  {
    rule: "refuses an endpoint that is not an http or https URL",
    variables: { OTEL_EXPORTER_OTLP_ENDPOINT: "collector:4318" },
    refusal: endpointVariables,
  },
  {
    rule: "refuses a traces endpoint that is not an http or https URL",
    variables: {
      OTEL_EXPORTER_OTLP_ENDPOINT: collector,
      OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: "collector:4318/v1/traces",
    },
    refusal: endpointVariables,
  },
  {
    rule: "reads no other exporter variable while the endpoint is blank",
    variables: {
      OTEL_EXPORTER_OTLP_ENDPOINT: " ",
      OTEL_EXPORTER_OTLP_PROTOCOL: "grpc",
    },
  },
];

it.each(spanExportRules)("span export $rule", ({ variables, refusal }) =>
  Effect.gen(function* () {
    const started = yield* Layer.build(spanExport).pipe(
      Effect.withConfigProvider(
        ConfigProvider.fromMap(new Map(Object.entries(variables))),
      ),
      Effect.either,
    );

    expect(
      Either.match(started, {
        onLeft: ({ reason, message }) => ({
          reason,
          variablesToFix: message.match(/OTEL_[A-Z_]+/gu),
        }),
        onRight: () => undefined,
      }),
    ).toEqual(refusal && { reason: "environment", variablesToFix: refusal });
  }),
);
