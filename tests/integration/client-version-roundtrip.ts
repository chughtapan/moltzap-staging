/** @file Real-process version-mismatch round trips through the public clients. */
import type { FileSystem, HttpClient } from "@effect/platform";
import { NodeFileSystem, NodeHttpClient } from "@effect/platform-node";
import {
  AgentName,
  AgentSigningAuthority,
  LIVE_WIRE_VERSIONS,
  PrincipalId,
} from "@moltzap/social-harness";
import {
  Registry,
  registryContract,
  Router,
} from "@moltzap/social-harness/internal";
import { WireVersions } from "@moltzap/social-harness/test";
import { Duration, Effect, Layer, Redacted, Schema, type Scope } from "effect";
import { generateKeyPairSync } from "node:crypto";
import { expect, it } from "vitest";
import { processTrace } from "./process-trace.js";

/** Registry setup shared by the independent process round trips. */
export interface RegistryInfrastructure {
  readonly registryOrigin: URL;
  readonly registrySignerPublicKey: ReturnType<
    typeof AgentSigningAuthority.publicKey
  >;
}

interface Infrastructure extends RegistryInfrastructure {
  readonly routerOrigin: URL;
}

const outsideWireWindow = {
  emitted: "1900.101.0",
  live: ["1900.101.0"],
  history: ["1900.101.0"],
};

const registerVersionMismatchAgent = (
  infrastructure: RegistryInfrastructure,
  admissionCredential: Redacted.Redacted,
) =>
  Effect.gen(function* () {
    const privateKeyPem = generateKeyPairSync("ed25519").privateKey.export({
      format: "pem",
      type: "pkcs8",
    });
    const signingAuthority = yield* AgentSigningAuthority.fromPkcs8(
      Redacted.make(privateKeyPem),
    );
    const registered = yield* Registry.register({
      request: {
        operationId: Schema.decodeUnknownSync(registryContract.RegistrationId)(
          "opn_AAAAAAAAAAAAAAAAAAAAAA",
        ),
        principalId: Schema.decodeUnknownSync(PrincipalId)(
          "prn_AAAAAAAAAAAAAAAAAAAAAA",
        ),
        agentName: Schema.decodeUnknownSync(AgentName)("version-mismatch"),
        publicKey: AgentSigningAuthority.publicKey(signingAuthority),
      },
      admissionCredential,
      signingAuthority,
    }).pipe(
      Effect.provide(
        Registry.layer({
          origin: infrastructure.registryOrigin,
          registrySignerPublicKey: infrastructure.registrySignerPublicKey,
          requestTimeout: Duration.seconds(5),
        }),
      ),
    );
    if (registered.kind !== "registered") {
      return yield* Effect.dieMessage(
        `registration returned ${registered.kind}`,
      );
    }
    return { signingAuthority, identityCard: registered.identityCard };
  });

/** A signed out-of-window request preserves the real Router encoder's live set. */
const assertRouterVersionMismatch = (
  infrastructure: Infrastructure,
  admissionCredential: Redacted.Redacted,
) =>
  Effect.gen(function* () {
    const { signingAuthority, identityCard } =
      yield* registerVersionMismatchAgent(infrastructure, admissionCredential);
    const error = yield* Router.poll({
      request: {},
      callerAgentId: identityCard.agentId,
      signingAuthority,
    }).pipe(
      Effect.provide(
        Router.layer({
          origin: infrastructure.routerOrigin,
          sendTimeout: Duration.seconds(5),
          pollTimeout: Duration.seconds(5),
        }),
      ),
      Effect.provideService(WireVersions, outsideWireWindow),
      Effect.flip,
    );
    expect(error).toMatchObject({
      _tag: "VersionMismatchError",
      supported: LIVE_WIRE_VERSIONS,
    });
  });

/** The Registry client classifies a version refusal as a closed HTTP failure. */
const assertRegistryVersionMismatch = (
  infrastructure: RegistryInfrastructure,
) =>
  Effect.gen(function* () {
    const error = yield* Registry.lookup({
      agentName: Schema.decodeUnknownSync(AgentName)("version-mismatch"),
    }).pipe(
      Effect.provide(
        Registry.layer({
          origin: infrastructure.registryOrigin,
          registrySignerPublicKey: infrastructure.registrySignerPublicKey,
          requestTimeout: Duration.seconds(5),
        }),
      ),
      Effect.provideService(WireVersions, outsideWireWindow),
      Effect.flip,
    );
    expect(error).toMatchObject({
      _tag: "RegistryError",
      reason: "http-envelope",
    });
  });

/** Each real server's encoder is exercised independently through its public client. */
export const testVersionMismatchRoundTrips = (
  setups: {
    readonly registry: Effect.Effect<
      RegistryInfrastructure,
      unknown,
      FileSystem.FileSystem | Scope.Scope | HttpClient.HttpClient
    >;
    readonly router: Effect.Effect<
      Infrastructure,
      unknown,
      FileSystem.FileSystem | Scope.Scope | HttpClient.HttpClient
    >;
  },
  admissionCredential: Redacted.Redacted,
) => {
  const scenarios = [
    {
      name: "returns the live versions from the real Router through the public client",
      check: setups.router.pipe(
        Effect.flatMap((infrastructure) =>
          assertRouterVersionMismatch(infrastructure, admissionCredential),
        ),
      ),
    },
    {
      name: "reports a Registry version refusal through the closed public client error",
      check: setups.registry.pipe(
        Effect.flatMap(assertRegistryVersionMismatch),
      ),
    },
  ];
  for (const { name, check } of scenarios) {
    it(
      name,
      processTrace(
        check.pipe(
          Effect.scoped,
          Effect.provide(NodeFileSystem.layer),
          Effect.provide(NodeHttpClient.layer),
        ),
      ),
      60_000,
    );
  }
};

/** Process tests share the public Router client's production HTTP transport. */
export const makeRouterRunner = (origin: URL) => {
  const routerLayer = Router.layer({
    origin,
    sendTimeout: Duration.seconds(5),
    pollTimeout: Duration.seconds(35),
  }).pipe(Layer.provide(NodeHttpClient.layer));
  return <A, E>(effect: Effect.Effect<A, E, Router>) =>
    effect.pipe(Effect.provide(routerLayer));
};

/** Process tests share the Registry signer pin and production HTTP transport. */
export const makeRegistryRunner = (
  origin: URL,
  registrySignerPublicKey: RegistryInfrastructure["registrySignerPublicKey"],
) => {
  const registryLayer = Registry.layer({
    origin,
    registrySignerPublicKey,
    requestTimeout: Duration.seconds(5),
  }).pipe(Layer.provide(NodeHttpClient.layer));
  return <A, E>(effect: Effect.Effect<A, E, Registry>) =>
    effect.pipe(Effect.provide(registryLayer));
};
