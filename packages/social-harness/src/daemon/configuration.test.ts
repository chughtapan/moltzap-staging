/** @file Exact process-input and secret-file tests for daemon bootstrap. */

import { live as it } from "@effect/vitest";
import { ConfigProvider, Effect, Redacted } from "effect";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- Tests create and remove exact temporary secret-file fixtures around the configuration boundary.
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect } from "vitest";
import type {
  DaemonConfigurationError,
  DaemonProcessConfiguration,
} from "./bootstrap.js";
import { AgentSigningAuthority } from "../identity/keys.js";
import {
  loadDaemonBootstrap,
  loadDaemonProcessConfiguration,
} from "./configuration.js";

/* eslint-disable agent-code-guard/no-hardcoded-assertion-literals -- Exact keys, spellings, redaction, and closed reasons are the configuration contract under test. */

const privateKey = `-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIHsbmQdBGQFs1eXLEWxKDblLeG//B9s8WmWEMQHvw4f8
-----END PRIVATE KEY-----`;
const registrySigner =
  '{"crv":"Ed25519","kty":"OKP","x":"y1j1FUgbqjCPeQVEnllv-2euwn_s9DeDkfEh3gk_OJ0"}';
const directories: string[] = [];
const noOverrides = new Map<string, string>();
const EXPORT_PATH = "/run/social-harness/history.ndjson";

const temporaryDirectory = (): string => {
  const directory = mkdtempSync(join(tmpdir(), "sharness-daemon-config-"));
  directories.push(directory);
  return directory;
};

const requiredConfiguration = (directory: string) =>
  new Map([
    ["SHARNESS_STATE_DIRECTORY", join(directory, "state")],
    ["SHARNESS_MCP_PORT", "4319"],
    ["SHARNESS_MCP_CREDENTIAL_FILE", join(directory, "mcp-credential")],
    ["SHARNESS_REGISTRY_ORIGIN", "https://registry.example"],
    ["SHARNESS_REGISTRY_SIGNER_PUBLIC_KEY", registrySigner],
    ["SHARNESS_ROUTER_ORIGIN", "http://router.example:4320"],
    ["SHARNESS_AGENT_PRIVATE_KEY_FILE", join(directory, "agent.pem")],
    ["SHARNESS_ADMISSION_CREDENTIAL_FILE", join(directory, "admission")],
  ]);

const loadConfiguration = (
  directory: string,
  overrides: ReadonlyMap<string, string> = noOverrides,
) =>
  loadDaemonProcessConfiguration.pipe(
    Effect.withConfigProvider(
      ConfigProvider.fromMap(
        new Map([
          ...requiredConfiguration(directory),
          ["UNRELATED_DEPLOYMENT_VALUE", "ignored"],
          ...overrides,
        ]),
      ),
    ),
  );

const failureReason = <A>(effect: Effect.Effect<A, DaemonConfigurationError>) =>
  effect.pipe(
    Effect.flip,
    Effect.map((error) => error.reason),
  );

afterEach(() => {
  for (const directory of directories.splice(0)) {
    rmSync(directory, { recursive: true, force: true });
  }
});

const loadsExactConfiguration = () =>
  Effect.gen(function* () {
    const directory = temporaryDirectory();
    const configuration = yield* loadConfiguration(directory);
    expect(configuration).toMatchObject({
      stateDirectory: join(directory, "state"),
      mcpPort: 4319,
    });
    expect(configuration.registryOrigin.href).toBe("https://registry.example/");
    expect(configuration.routerOrigin.href).toBe("http://router.example:4320/");
    expect(configuration.registrySignerPublicKey.x).toBe(
      "y1j1FUgbqjCPeQVEnllv-2euwn_s9DeDkfEh3gk_OJ0",
    );
    expect(Redacted.isRedacted(configuration.agentPrivateKeyFile)).toBe(true);
    expect(Redacted.isRedacted(configuration.admissionCredentialFile)).toBe(
      true,
    );
  });

const registrationSettings = new Map([
  ["SHARNESS_REGISTRATION_AGENT_NAME", "alice"],
  ["SHARNESS_REGISTRATION_OPERATION_ID", "opn_AAAAAAAAAAAAAAAAAAAAAA"],
  ["SHARNESS_REGISTRATION_PRINCIPAL_ID", "prn_AAAAAAAAAAAAAAAAAAAAAA"],
]);

const loadsRegistration = () =>
  Effect.gen(function* () {
    const configuration = yield* loadConfiguration(
      temporaryDirectory(),
      registrationSettings,
    );
    expect(configuration.registration).toEqual({
      agentName: "alice",
      operationId: "opn_AAAAAAAAAAAAAAAAAAAAAA",
      principalId: "prn_AAAAAAAAAAAAAAAAAAAAAA",
    });
    for (const name of registrationSettings.keys()) {
      const partial = new Map(registrationSettings);
      partial.delete(name);
      expect(
        yield* failureReason(loadConfiguration(temporaryDirectory(), partial)),
      ).toBe("environment");
      const invalid = new Map(registrationSettings);
      invalid.set(name, "");
      expect(
        yield* failureReason(loadConfiguration(temporaryDirectory(), invalid)),
      ).toBe("environment");
    }
  });

const rejectsInvalidEnvironment = () =>
  Effect.gen(function* () {
    const directory = temporaryDirectory();
    const invalidOverrides = [
      new Map([["SHARNESS_STATE_DIRECTORY", ""]]),
      new Map([["SHARNESS_MCP_PORT", "0"]]),
      new Map([["SHARNESS_MCP_PORT", "01"]]),
      new Map([["SHARNESS_MCP_PORT", "65536"]]),
      new Map([["SHARNESS_REGISTRY_ORIGIN", "https://registry.example/path"]]),
      new Map([["SHARNESS_ROUTER_ORIGIN", "ftp://router.example"]]),
      new Map([["SHARNESS_REGISTRY_SIGNER_PUBLIC_KEY", `${registrySigner} `]]),
      new Map([["SHARNESS_ADMISSION_CREDENTIAL_FILE", "/run/\u0000admission"]]),
    ];
    for (const overrides of invalidOverrides) {
      expect(
        yield* failureReason(loadConfiguration(directory, overrides)),
      ).toBe("environment");
    }
  });

const loadsExactRedactedSecrets = () =>
  Effect.gen(function* () {
    const directory = temporaryDirectory();
    writeFileSync(join(directory, "agent.pem"), privateKey);
    writeFileSync(
      join(directory, "mcp-credential"),
      "mcp-local-test-credential-0000000001",
    );
    writeFileSync(join(directory, "admission"), "bootstrap-token=");
    const configuration = yield* loadConfiguration(directory);
    const bootstrap = yield* loadDaemonBootstrap(configuration);
    expect(bootstrap.agentPublicKey.x).toBe(
      AgentSigningAuthority.publicKey(bootstrap.signingAuthority).x,
    );
    expect(bootstrap.agentPublicKey.x).toBe(
      "3rUJ92tIP0DE4ekmET1zme6SIWTp5G0KiF3ZjL-AoKg",
    );
    const admissionCredential = yield* bootstrap.admissionCredential;
    expect(Redacted.isRedacted(admissionCredential)).toBe(true);
    expect(Redacted.value(admissionCredential)).toBe("bootstrap-token=");
  });

const loadsBootstrapWithoutAdmissionFile = () =>
  Effect.gen(function* () {
    const directory = temporaryDirectory();
    writeFileSync(join(directory, "agent.pem"), privateKey);
    writeFileSync(
      join(directory, "mcp-credential"),
      "mcp-local-test-credential-0000000001",
    );
    const configuration = yield* loadConfiguration(directory);
    const bootstrap = yield* loadDaemonBootstrap(configuration);
    expect(bootstrap.agentPublicKey.x).toBe(
      "3rUJ92tIP0DE4ekmET1zme6SIWTp5G0KiF3ZjL-AoKg",
    );
    expect(yield* failureReason(bootstrap.admissionCredential)).toBe(
      "admission-credential-file",
    );
  });

const treatsEmptyAdmissionFileAsUnset = () =>
  Effect.gen(function* () {
    const directory = temporaryDirectory();
    writeFileSync(join(directory, "agent.pem"), privateKey);
    writeFileSync(
      join(directory, "mcp-credential"),
      "mcp-local-test-credential-0000000001",
    );
    const configuration = yield* loadConfiguration(
      directory,
      new Map([["SHARNESS_ADMISSION_CREDENTIAL_FILE", ""]]),
    );
    expect(configuration.admissionCredentialFile).toBeUndefined();
    const bootstrap = yield* loadDaemonBootstrap(configuration);
    expect(yield* failureReason(bootstrap.admissionCredential)).toBe(
      "admission-credential-file",
    );
  });

const loadsBootstrapWithAdmissionFileUnset = () =>
  Effect.gen(function* () {
    const directory = temporaryDirectory();
    writeFileSync(join(directory, "agent.pem"), privateKey);
    writeFileSync(
      join(directory, "mcp-credential"),
      "mcp-local-test-credential-0000000001",
    );
    const configuration = yield* loadDaemonProcessConfiguration.pipe(
      Effect.withConfigProvider(
        ConfigProvider.fromMap(
          new Map(
            [...requiredConfiguration(directory)].filter(
              ([name]) => name !== "SHARNESS_ADMISSION_CREDENTIAL_FILE",
            ),
          ),
        ),
      ),
    );
    expect(configuration.admissionCredentialFile).toBeUndefined();
    const bootstrap = yield* loadDaemonBootstrap(configuration);
    expect(yield* failureReason(bootstrap.admissionCredential)).toBe(
      "admission-credential-file",
    );
  });

const admissionFailureReason = (configuration: DaemonProcessConfiguration) =>
  Effect.gen(function* () {
    const bootstrap = yield* loadDaemonBootstrap(configuration);
    return yield* failureReason(bootstrap.admissionCredential);
  });

const rejectsSecretFileFailures = () =>
  Effect.gen(function* () {
    const directory = temporaryDirectory();
    const configuration = yield* loadConfiguration(directory);
    expect(yield* failureReason(loadDaemonBootstrap(configuration))).toBe(
      "agent-private-key-file",
    );

    writeFileSync(join(directory, "agent.pem"), "not a private key");
    expect(yield* failureReason(loadDaemonBootstrap(configuration))).toBe(
      "agent-private-key",
    );

    writeFileSync(join(directory, "agent.pem"), privateKey);
    writeFileSync(
      join(directory, "mcp-credential"),
      "mcp-local-test-credential-0000000001",
    );
    writeFileSync(join(directory, "admission"), "bootstrap-token=\n");
    expect(yield* admissionFailureReason(configuration)).toBe(
      "admission-credential",
    );

    writeFileSync(
      join(directory, "admission"),
      Buffer.from([0xef, 0xbb, 0xbf, ...Buffer.from("bootstrap-token=")]),
    );
    expect(yield* admissionFailureReason(configuration)).toBe(
      "admission-credential",
    );
  });

// @agent-code-guard/regression-only: these examples pin the daemon process inputs and exact secret-file boundary.
describe("daemon configuration", () => {
  it(
    "loads exactly the declared configuration and ignores unrelated values",
    loadsExactConfiguration,
  );
  it(
    "rejects alternate process-input spellings and ranges",
    rejectsInvalidEnvironment,
  );
  it(
    "loads the unmodified private key and redacted admission credential",
    loadsExactRedactedSecrets,
  );
  it(
    "closes file, UTF-8, key, and credential failures",
    rejectsSecretFileFailures,
  );
  it(
    "loads the bootstrap without reading a missing admission credential file",
    loadsBootstrapWithoutAdmissionFile,
  );
  it(
    "accepts an unset admission credential file until the credential is used",
    loadsBootstrapWithAdmissionFileUnset,
  );
  it(
    "treats an empty admission credential file setting as unset",
    treatsEmptyAdmissionFileAsUnset,
  );
});

describe("history export configuration", () => {
  it("leaves the export unset when the operator names no file", () =>
    Effect.gen(function* () {
      const directory = temporaryDirectory();
      const configuration = yield* loadConfiguration(directory);
      expect(configuration.historyExport).toBeUndefined();
    }));

  it("takes the export file the operator names", () =>
    Effect.gen(function* () {
      const directory = temporaryDirectory();
      const configuration = yield* loadConfiguration(
        directory,
        new Map([["SHARNESS_HISTORY_EXPORT", EXPORT_PATH]]),
      );
      expect(configuration.historyExport).toBe(EXPORT_PATH);
    }));

  it("refuses an empty export path rather than exporting nowhere", () =>
    Effect.gen(function* () {
      const directory = temporaryDirectory();
      expect(
        yield* failureReason(
          loadConfiguration(
            directory,
            new Map([["SHARNESS_HISTORY_EXPORT", ""]]),
          ),
        ),
      ).toBe("environment");
    }));
});

const mcpToken = "mcp-local-test-credential-0000000001";
const bootstrapFiles = () => {
  const directory = temporaryDirectory();
  writeFileSync(join(directory, "agent.pem"), privateKey);
  writeFileSync(
    join(directory, "mcp-credential"),
    "mcp-local-test-credential-0000000001",
  );
  writeFileSync(join(directory, "admission"), "bootstrap-token=");
  return directory;
};
const loadsBearerCredential = () =>
  Effect.gen(function* () {
    const directory = bootstrapFiles();
    writeFileSync(join(directory, "mcp-credential"), mcpToken);
    const bootstrap = yield* loadConfiguration(directory).pipe(
      Effect.flatMap(loadDaemonBootstrap),
    );
    expect(bootstrap.mcpCredential).toEqual(Redacted.make(mcpToken));
    expect(JSON.stringify(bootstrap.mcpCredential)).not.toContain(mcpToken);
  });
const rejectsInvalidCredential = () =>
  Effect.gen(function* () {
    const directory = bootstrapFiles();
    for (const token of ["short", `${mcpToken}\n`, `\ufeff${mcpToken}`]) {
      writeFileSync(join(directory, "mcp-credential"), token);
      expect(
        yield* failureReason(
          loadConfiguration(directory).pipe(
            Effect.flatMap(loadDaemonBootstrap),
          ),
        ),
      ).toBe("mcp-credential");
    }
  });
const rejectsMissingCredential = () =>
  Effect.gen(function* () {
    const directory = bootstrapFiles();
    rmSync(join(directory, "mcp-credential"));
    expect(
      yield* failureReason(
        loadConfiguration(directory).pipe(Effect.flatMap(loadDaemonBootstrap)),
      ),
    ).toBe("mcp-credential-file");
  });
describe("startup registration configuration", () => {
  it(
    "loads a complete request and rejects partial or invalid settings",
    loadsRegistration,
  );
});

// @agent-code-guard/regression-only: exact credential bytes and unreadable files fail closed.
describe("MCP credential configuration", () => {
  it("loads one redacted bearer credential", loadsBearerCredential);
  it(
    "rejects short or noncanonical credential files",
    rejectsInvalidCredential,
  );
  it("reports an unreadable credential file", rejectsMissingCredential);
});

const oauthValues = (directory: string) => {
  const values = requiredConfiguration(directory);
  values.delete("SHARNESS_MCP_CREDENTIAL_FILE");
  values.set("SHARNESS_MCP_PUBLIC_URL", "https://mcp.example.test/mcp");
  values.set("SHARNESS_MCP_BIND_HOST", "0.0.0.0");
  values.set("SHARNESS_OAUTH_ISSUER", "https://issuer.example.test/");
  values.set("SHARNESS_OAUTH_OWNER_SUBJECT", "owner-subject");
  values.set("SHARNESS_OAUTH_REQUIRED_SCOPE", "use:mcp");
  return values;
};
const loadValues = (values: Map<string, string>) =>
  loadDaemonProcessConfiguration.pipe(
    Effect.withConfigProvider(ConfigProvider.fromMap(values)),
  );
const loadsOAuth = () =>
  Effect.gen(function* () {
    const directory = temporaryDirectory();
    writeFileSync(join(directory, "agent.pem"), privateKey);
    const configuration = yield* loadValues(oauthValues(directory));
    const bootstrap = yield* loadDaemonBootstrap(configuration);
    expect(bootstrap.configuration.oauth).toMatchObject({
      issuer: "https://issuer.example.test/",
      ownerSubject: "owner-subject",
      scope: "use:mcp",
    });
    expect(bootstrap.configuration.oauth?.resource.href).toBe(
      "https://mcp.example.test/mcp",
    );
  });
const rejectsIncompleteOAuth = () =>
  Effect.gen(function* () {
    const directory = temporaryDirectory();
    const mixed = oauthValues(directory);
    mixed.set("SHARNESS_MCP_CREDENTIAL_FILE", join(directory, "credential"));
    expect(yield* failureReason(loadValues(mixed))).toBe("environment");
    const partial = oauthValues(directory);
    partial.delete("SHARNESS_OAUTH_OWNER_SUBJECT");
    expect(yield* failureReason(loadValues(partial))).toBe("environment");
    const insecure = oauthValues(directory);
    insecure.set("SHARNESS_OAUTH_ISSUER", "http://issuer.example.test/");
    expect(yield* failureReason(loadValues(insecure))).toBe("environment");
    for (const suffix of ["?", "#"]) {
      const delimited = oauthValues(directory);
      delimited.set(
        "SHARNESS_MCP_PUBLIC_URL",
        `https://mcp.example.test/mcp${suffix}`,
      );
      expect(yield* failureReason(loadValues(delimited))).toBe("environment");
    }
  });
/**
 * Value: protects=canonical HTTPS resource authority without URI credentials;
 * fails_when=wrong paths, normalization or embedded credentials are accepted;
 * why_new=earlier cases covered incomplete and insecure settings; seam=none.
 */
const rejectsNoncanonicalResources = () =>
  Effect.gen(function* () {
    const directory = temporaryDirectory();
    for (const resource of [
      "https://mcp.example.test/other",
      "https://MCP.example.test/mcp",
      "https://mcp.example.test:443/mcp",
      "https://mcp.example.test/old/../mcp",
      "https://owner:example@mcp.example.test/mcp",
    ]) {
      const values = oauthValues(directory);
      values.set("SHARNESS_MCP_PUBLIC_URL", resource);
      expect(yield* failureReason(loadValues(values))).toBe("environment");
    }
    const issuerCredentials = oauthValues(directory);
    issuerCredentials.set(
      "SHARNESS_OAUTH_ISSUER",
      "https://owner:example@issuer.example.test/",
    );
    expect(yield* failureReason(loadValues(issuerCredentials))).toBe(
      "environment",
    );
    const accepted = yield* loadValues(oauthValues(directory));
    expect(accepted.oauth?.resource.href).toBe("https://mcp.example.test/mcp");
  });
describe("public MCP OAuth configuration", () => {
  it(
    "rejects wrong paths, URI normalization and embedded credentials",
    rejectsNoncanonicalResources,
  );

  it(
    "starts with its fixed owner and issuer without a local credential file",
    loadsOAuth,
  );
  it("rejects mixed, partial and insecure authority", rejectsIncompleteOAuth);
});

/* eslint-enable agent-code-guard/no-hardcoded-assertion-literals -- Restore defaults after configuration contract assertions. */
