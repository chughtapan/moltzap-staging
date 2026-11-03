/** @file Process configuration validates exact secret files and preserves safe startup diagnostics. */

import { scopedLive as it } from "@effect/vitest";
import { ConfigProvider, Effect, Layer, Redacted, Schema } from "effect";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- Configuration tests control exact secret bytes before platform services start.
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect } from "vitest";
import {
  AgentPublicKey,
  AgentSigningAuthority,
} from "../identity/agent-cards.js";
import {
  loadDaemonBootstrap,
  loadDaemonProcessConfiguration,
} from "./config.js";
import { daemonLayer, DaemonStartupError, withFirewall } from "./startup.js";
/* eslint-disable agent-code-guard/no-hardcoded-assertion-literals -- Exact public reasons, credential bytes and configuration diagnostics are the behavior under test. */

const privateKey = `-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIHsbmQdBGQFs1eXLEWxKDblLeG//B9s8WmWEMQHvw4f8
-----END PRIVATE KEY-----`;
const registrySigner =
  '{"crv":"Ed25519","kty":"OKP","x":"y1j1FUgbqjCPeQVEnllv-2euwn_s9DeDkfEh3gk_OJ0"}';
const mcpCredential = "mcp-local-test-credential-0000000001";
const directories: string[] = [];

afterEach(() => {
  for (const directory of directories.splice(0)) {
    rmSync(directory, { recursive: true, force: true });
  }
});

function fixture(overrides: Readonly<Record<string, string>> = {}) {
  const directory = mkdtempSync(join(tmpdir(), "sharness-config-"));
  directories.push(directory);
  const values = {
    SHARNESS_STATE_DIRECTORY: join(directory, "state"),
    SHARNESS_MCP_PORT: "4319",
    SHARNESS_MCP_CREDENTIAL_FILE: join(directory, "mcp-credential"),
    SHARNESS_REGISTRY_ORIGIN: "https://registry.example",
    SHARNESS_REGISTRY_SIGNER_PUBLIC_KEY: registrySigner,
    SHARNESS_ROUTER_ORIGIN: "http://router.example:4320",
    SHARNESS_AGENT_PRIVATE_KEY_FILE: join(directory, "agent.pem"),
    SHARNESS_ADMISSION_CREDENTIAL_FILE: join(directory, "admission"),
    ...overrides,
  };
  writeFileSync(values.SHARNESS_AGENT_PRIVATE_KEY_FILE, privateKey);
  writeFileSync(values.SHARNESS_MCP_CREDENTIAL_FILE, mcpCredential);
  writeFileSync(
    values.SHARNESS_ADMISSION_CREDENTIAL_FILE ||
      join(directory, "unused-admission"),
    "bootstrap-token=",
  );
  const provider = ConfigProvider.fromMap(new Map(Object.entries(values)));
  const load = loadDaemonProcessConfiguration.pipe(
    Effect.withConfigProvider(provider),
  );
  return { directory, values, provider, load };
}

// @agent-code-guard/regression-only: Exact process settings, secret bytes and startup diagnostics are finite boundary cases.
it("loads process settings and keeps private credentials redacted", () =>
  Effect.gen(function* () {
    const subject = fixture({
      SHARNESS_HISTORY_EXPORT: "/run/social-harness/history.ndjson",
    });
    const configuration = yield* subject.load;
    expect(configuration).toMatchObject({
      stateDirectory: subject.values.SHARNESS_STATE_DIRECTORY,
      mcpPort: 4319,
      historyExport: "/run/social-harness/history.ndjson",
      hideGroups: false,
      hideCollectives: false,
    });
    expect(configuration.registryOrigin.href).toBe("https://registry.example/");
    expect(configuration.routerOrigin.href).toBe("http://router.example:4320/");
    const bootstrap = yield* loadDaemonBootstrap(configuration);
    expect(AgentSigningAuthority.publicKey(bootstrap.signingAuthority).x).toBe(
      "3rUJ92tIP0DE4ekmET1zme6SIWTp5G0KiF3ZjL-AoKg",
    );
    expect(bootstrap.mcpCredential).toEqual(Redacted.make(mcpCredential));
    expect(Redacted.value(yield* bootstrap.admissionCredential)).toBe(
      "bootstrap-token=",
    );
    expect(JSON.stringify(bootstrap)).not.toContain(mcpCredential);
    expect(JSON.stringify(bootstrap)).not.toContain(privateKey);
  }));

it("accepts a valid Registry key with JSON whitespace and reordered members", () =>
  Effect.gen(function* () {
    const expected = yield* Schema.decodeUnknown(
      Schema.parseJson(AgentPublicKey),
    )(registrySigner);
    const reordered = JSON.stringify(
      { x: expected.x, kty: "OKP", crv: "Ed25519" },
      null,
      2,
    );
    const subject = fixture({
      SHARNESS_REGISTRY_SIGNER_PUBLIC_KEY: ` ${reordered} `,
    });
    const configuration = yield* subject.load;
    expect(configuration.registrySignerPublicKey).toEqual(expected);
  }));

const invalidSettings: ReadonlyArray<readonly [string, string]> = [
  ["SHARNESS_STATE_DIRECTORY", ""],
  ["SHARNESS_MCP_PORT", "0"],
  ["SHARNESS_MCP_PORT", "01"],
  ["SHARNESS_MCP_PORT", "65536"],
  ["SHARNESS_REGISTRY_ORIGIN", "https://registry.example/private"],
  ["SHARNESS_ROUTER_ORIGIN", "ftp://router.example"],
  ["SHARNESS_HISTORY_EXPORT", ""],
  ["SHARNESS_EXPERIMENT_HIDE_GROUPS", "sometimes"],
  ["SHARNESS_EXPERIMENT_HIDE_COLLECTIVES", "sometimes"],
];
it.each(invalidSettings)(
  "rejects invalid process setting %s",
  ([name, value]) =>
    Effect.gen(function* () {
      const subject = fixture({ [name]: value });
      const error = yield* subject.load.pipe(Effect.flip);
      expect(error.reason).toBe("environment");
      expect(error.message).toContain(name);
    }),
);

it("loads a complete startup registration and rejects partial requests", () =>
  Effect.gen(function* () {
    const registration = {
      SHARNESS_REGISTRATION_AGENT_NAME: "alice",
      SHARNESS_REGISTRATION_OPERATION_ID: "opn_AAAAAAAAAAAAAAAAAAAAAA",
      SHARNESS_REGISTRATION_PRINCIPAL_ID: "prn_AAAAAAAAAAAAAAAAAAAAAA",
    };
    expect((yield* fixture(registration).load).registration).toEqual({
      agentName: "alice",
      operationId: registration.SHARNESS_REGISTRATION_OPERATION_ID,
      principalId: registration.SHARNESS_REGISTRATION_PRINCIPAL_ID,
    });
    for (const name of Object.keys(registration)) {
      const partial = new Map(Object.entries(registration));
      partial.delete(name);
      expect(
        (yield* fixture(Object.fromEntries(partial)).load.pipe(Effect.flip))
          .reason,
      ).toBe("environment");
    }
  }));

it.each(["unset", "empty", "missing"])(
  "defers the %s admission file until registration needs it",
  (kind) =>
    Effect.gen(function* () {
      const subject = fixture(
        kind === "empty" ? { SHARNESS_ADMISSION_CREDENTIAL_FILE: "" } : {},
      );
      if (kind === "missing") {
        rmSync(subject.values.SHARNESS_ADMISSION_CREDENTIAL_FILE);
      }
      const values = new Map(Object.entries(subject.values));
      if (kind === "unset") {
        values.delete("SHARNESS_ADMISSION_CREDENTIAL_FILE");
      }
      const configuration = yield* loadDaemonProcessConfiguration.pipe(
        Effect.withConfigProvider(ConfigProvider.fromMap(values)),
      );
      const bootstrap = yield* loadDaemonBootstrap(configuration);
      expect(
        (yield* bootstrap.admissionCredential.pipe(Effect.flip)).reason,
      ).toBe("admission-credential-file");
    }),
);

it.each([
  {
    file: "agent.pem",
    bytes: "not a private key",
    reason: "agent-private-key",
  },
  { file: "mcp-credential", bytes: "short", reason: "mcp-credential" },
  {
    file: "mcp-credential",
    bytes: `${mcpCredential}\n`,
    reason: "mcp-credential",
  },
  {
    file: "mcp-credential",
    bytes: `\ufeff${mcpCredential}`,
    reason: "mcp-credential",
  },
  {
    file: "admission",
    bytes: "bootstrap-token=\n",
    reason: "admission-credential",
  },
  {
    file: "admission",
    bytes: "\ufeffbootstrap-token=",
    reason: "admission-credential",
  },
])("rejects exact secret bytes for $file", ({ file, bytes, reason }) =>
  Effect.gen(function* () {
    const subject = fixture();
    writeFileSync(join(subject.directory, file), bytes);
    const bootstrap = subject.load.pipe(Effect.flatMap(loadDaemonBootstrap));
    const error = yield* bootstrap.pipe(
      Effect.flatMap((value) =>
        file === "admission"
          ? value.admissionCredential.pipe(Effect.asVoid)
          : Effect.void,
      ),
      Effect.flip,
    );
    expect(error.reason).toBe(reason);
    expect(error.cause).toBeUndefined();
    expect(JSON.stringify(error.toJSON())).not.toContain(bytes);
  }),
);

it.each([
  { file: "agent.pem", reason: "agent-private-key-file" },
  { file: "mcp-credential", reason: "mcp-credential-file" },
])(
  "reports an unreadable $file without exposing the file path",
  ({ file, reason }) =>
    Effect.gen(function* () {
      const subject = fixture();
      const path = join(subject.directory, file);
      rmSync(path);
      const error = yield* subject.load.pipe(
        Effect.flatMap(loadDaemonBootstrap),
        Effect.flip,
      );
      expect(error.reason).toBe(reason);
      expect(JSON.stringify(error.toJSON())).not.toContain(path);
    }),
);

it("keeps configured secrets out of process startup errors", () =>
  Effect.gen(function* () {
    const rejected =
      "https://operator:private-password@registry.example/private";
    const subject = fixture({ SHARNESS_REGISTRY_ORIGIN: rejected });
    const error = yield* Layer.build(daemonLayer).pipe(
      Effect.scoped,
      Effect.withConfigProvider(subject.provider),
      Effect.flip,
    );
    expect(error).toBeInstanceOf(DaemonStartupError);
    expect(error.message).toContain("SHARNESS_REGISTRY_ORIGIN");
    expect(JSON.stringify(error.toJSON())).not.toContain(rejected);
    expect(String(error)).not.toContain(rejected);
    expect(error.cause).toBeUndefined();
  }));

it("reports an invalid policy before attempting to load the agent key", () =>
  Effect.gen(function* () {
    const subject = fixture();
    const path = join(subject.directory, "policy.json");
    writeFileSync(path, "[]");
    rmSync(subject.values.SHARNESS_AGENT_PRIVATE_KEY_FILE);
    const provider = ConfigProvider.fromMap(
      new Map([
        ...Object.entries(subject.values),
        ["SHARNESS_MESSAGE_POLICY_FILE", path],
      ]),
    );
    const error = yield* Layer.build(daemonLayer).pipe(
      Effect.scoped,
      Effect.withConfigProvider(provider),
      Effect.flip,
    );
    expect(error).toMatchObject({ phase: "configuration" });
    expect(error.message).toContain("SHARNESS_MESSAGE_POLICY_FILE");
  }));

it("requires one source when programmatic and file policy are both configured", () =>
  Effect.gen(function* () {
    const subject = fixture();
    const path = join(subject.directory, "policy.json");
    writeFileSync(
      path,
      JSON.stringify({
        format: "org.social-harness/message-policy/v2",
        mode: "guidance",
      }),
    );
    const provider = ConfigProvider.fromMap(
      new Map([
        ...Object.entries(subject.values),
        ["SHARNESS_MESSAGE_POLICY_FILE", path],
      ]),
    );
    const error = yield* Layer.build(
      withFirewall({
        mode: "guidance",
        settings: {
          timeoutMilliseconds: 1000,
          maximumInputBytes: 1000,
          maximumResultBytes: 1000,
        },
        facts: [],
      }),
    ).pipe(Effect.scoped, Effect.withConfigProvider(provider), Effect.flip);
    expect(error).toMatchObject({ phase: "configuration" });
    expect(error.message).toContain("one source");
  }));

/* eslint-enable agent-code-guard/no-hardcoded-assertion-literals -- Restore defaults after boundary contract assertions. */
