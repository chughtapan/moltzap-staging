/** @file Registry startup rejects invalid settings and keeps secrets out of errors. */
import { NodeContext } from "@effect/platform-node";
import { live as it } from "@effect/vitest";
import { ConfigProvider, Effect, Layer } from "effect";
import { expect } from "vitest";
import { layer, StartupError } from "./index.js";

const invalidConfigurationOverrides = [
  new Map([["SHARNESS_REGISTRY_PORT", "0"]]),
  new Map([["SHARNESS_REGISTRY_PORT", "01"]]),
  new Map([["SHARNESS_REGISTRY_PORT", "65536"]]),
  // eslint-disable-next-line sonarjs/no-clear-text-protocols -- The configuration boundary must reject this intentionally wrong scheme.
  new Map([["SHARNESS_REGISTRY_POSTGRESQL_URL", "http://db/registry"]]),
  new Map([["SHARNESS_REGISTRY_ADMISSION_CREDENTIAL", "short"]]),
  new Map([["SHARNESS_REGISTRY_ADMISSION_CREDENTIAL", "invalid!token"]]),
  new Map([["SHARNESS_REGISTRY_SIGNING_PRIVATE_KEY_PATH", "relative.pem"]]),
  new Map([["SHARNESS_REGISTRY_REQUEST_CONCURRENCY_LIMIT", "+2"]]),
  new Map([["SHARNESS_REGISTRY_LIVE_NONCE_CAPACITY", "1.5"]]),
  new Map([["SHARNESS_REGISTRY_SQL_POOL_SIZE", "0"]]),
  new Map([["SHARNESS_REGISTRY_SQL_OPERATION_TIMEOUT_MS", "1e3"]]),
];

const rejectedValue =
  "postgresql://operator:private-password@localhost/database#invalid";
const databaseVariable = "SHARNESS_REGISTRY_POSTGRESQL_URL";
const signingKeyVariable = "SHARNESS_REGISTRY_SIGNING_PRIVATE_KEY_PATH";
const environment = ConfigProvider.fromMap(
  new Map([[databaseVariable, rejectedValue]]),
);

it("removes configured values from startup error serialization and causes", () =>
  Effect.gen(function* () {
    const error = yield* Layer.build(layer).pipe(
      Effect.scoped,
      Effect.withConfigProvider(environment),
      Effect.flip,
    );
    expect(error).toBeInstanceOf(StartupError);
    expect(error.message).toContain(signingKeyVariable);
    expect(error.message).toContain(databaseVariable);
    expect(JSON.stringify(error.toJSON())).not.toContain(rejectedValue);
    expect(String(error)).not.toContain(rejectedValue);
    expect(error.cause).toBeUndefined();
  }));

it("identifies signing key file failures without exposing the path", () =>
  Effect.gen(function* () {
    const privatePath = "/missing/private-registry-signing-key.pem";
    const provider = ConfigProvider.fromMap(
      new Map([
        ["SHARNESS_REGISTRY_PORT", "4100"],
        [databaseVariable, "postgresql://localhost/registry"],
        ["SHARNESS_REGISTRY_ADMISSION_CREDENTIAL", "admission-token"],
        [signingKeyVariable, privatePath],
      ]),
    );
    const error = yield* Layer.build(layer).pipe(
      Effect.scoped,
      Effect.withConfigProvider(provider),
      Effect.provide(NodeContext.layer),
      Effect.flip,
    );
    expect(error.message).toContain(signingKeyVariable);
    expect(JSON.stringify(error.toJSON())).not.toContain(privatePath);
    expect(String(error)).not.toContain(privatePath);
    expect(error.cause).toBeUndefined();
  }));

it("rejects malformed startup settings before opening resources", () =>
  Effect.gen(function* () {
    const required = new Map([
      ["SHARNESS_REGISTRY_PORT", "4317"],
      ["SHARNESS_REGISTRY_POSTGRESQL_URL", "postgresql://localhost/registry"],
      ["SHARNESS_REGISTRY_ADMISSION_CREDENTIAL", "admission-token"],
      [
        "SHARNESS_REGISTRY_SIGNING_PRIVATE_KEY_PATH",
        "/run/secrets/registry.pem",
      ],
    ]);
    for (const overrides of invalidConfigurationOverrides) {
      const error = yield* Layer.build(layer).pipe(
        Effect.scoped,
        Effect.withConfigProvider(
          ConfigProvider.fromMap(new Map([...required, ...overrides])),
        ),
        Effect.flip,
      );
      expect(error).toBeInstanceOf(StartupError);
      // eslint-disable-next-line agent-code-guard/no-hardcoded-assertion-literals -- The closed startup phase proves malformed settings fail before opening resources.
      expect(error.phase).toBe("configuration");
    }
  }));
