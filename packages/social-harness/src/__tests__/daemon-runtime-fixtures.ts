/** @file Verified agent identities and public daemon configuration for runtime tests. */

import { NodeHttpServer } from "@effect/platform-node";
import {
  Client,
  StreamableHTTPClientTransport,
} from "@modelcontextprotocol/client";
import {
  Duration,
  Effect,
  Redacted,
  Schedule,
  Schema,
  type Scope,
} from "effect";
import { generateKeyPairSync } from "node:crypto";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- NodeHttpServer.make requires a native server factory for the loopback listener.
import { createServer } from "node:http";
import type { DaemonBootstrap } from "../daemon/bootstrap.js";
import { Router } from "../communication/router/index.js";
import { runDaemonRuntime } from "../daemon/lifecycle.js";
import { encodeCanonical } from "../encoding/canonical-json.js";
import { Firewall } from "../firewall/index.js";
import { managementRegisterRequestSchema } from "../host-protocol/index.js";
import { AgentCard, type VerifiedAgentCard } from "../identity/cards.js";
import { AgentSigningAuthority, Ed25519PublicKey } from "../identity/keys.js";
import { saveLocalIdentity } from "../identity/local-store.js";
import { localIdentityLayer } from "../identity/local.js";
import { Registry } from "../identity/registry/index.js";
import {
  identifier,
  issueTestCard,
  makeTestAuthority,
  type RegistryKeyPair,
} from "./agent-card-fixtures.js";
import { mcpTestCredential, mcpTestHeaders } from "./mcp-http-fixtures.js";
import {
  emptyBatch,
  pollCursor,
  routerInstanceId,
} from "./router-worker-fixtures.js";

/** One signing authority owns a verified local identity and trusted peer cards. */
export interface Fixture {
  readonly bootstrap: DaemonBootstrap;
  readonly localCard: VerifiedAgentCard;
  readonly peerCards: readonly VerifiedAgentCard[];
  readonly canonicalLocalCard: Uint8Array;
  readonly registerRequest: typeof managementRegisterRequestSchema.Type;
}

const availablePort = Effect.scoped(
  NodeHttpServer.make(createServer, { port: 0, host: "127.0.0.1" }).pipe(
    Effect.flatMap((server) =>
      server.address._tag === "TcpAddress"
        ? Effect.succeed(server.address.port)
        : Effect.dieMessage("Expected a TCP listener"),
    ),
  ),
);

const makePeerCards = (registryKeys: RegistryKeyPair) =>
  Effect.forEach(
    ["bob", "carol"],
    (name, index) =>
      Effect.gen(function* () {
        return yield* issueTestCard({
          byte: index + 2,
          name,
          authority: yield* makeTestAuthority(),
          registryKeys,
        });
      }),
    { concurrency: 1 },
  );

/** Supplies a verified local identity, trusted peers and an available loopback port. */
export const makeFixture = Effect.gen(function* () {
  const registryKeys = generateKeyPairSync("ed25519");
  const registrySignerPublicKey = yield* Schema.decodeUnknown(Ed25519PublicKey)(
    registryKeys.publicKey.export({ format: "jwk" }),
  );
  const signingAuthority = yield* makeTestAuthority();
  const localCard = yield* issueTestCard({
    byte: 1,
    name: "alice",
    authority: signingAuthority,
    registryKeys,
  });
  const bootstrap: DaemonBootstrap = Object.freeze({
    configuration: {
      stateDirectory: "/var/lib/social-harness",
      mcpPort: yield* availablePort,
      mcpCredentialFile: Redacted.make("/run/secrets/mcp-credential"),
      registryOrigin: new URL("https://registry.example"),
      registrySignerPublicKey,
      routerOrigin: new URL("https://router.example"),
      agentPrivateKeyFile: Redacted.make("/run/secrets/agent.pem"),
      admissionCredentialFile: Redacted.make("/run/secrets/admission"),
      hideGroups: false,
      hideCollectives: false,
    },
    signingAuthority,
    mcpCredential: mcpTestCredential,
    agentPublicKey: AgentSigningAuthority.publicKey(signingAuthority),
    admissionCredential: Effect.succeed(Redacted.make("bootstrap-token=")),
  });
  const registerRequest = yield* Schema.decodeUnknown(
    managementRegisterRequestSchema,
  )({
    operationId: identifier("opn_", 3),
    principalId: localCard.principalId,
    agentName: localCard.agentName,
  });
  return {
    bootstrap,
    localCard,
    peerCards: yield* makePeerCards(registryKeys),
    canonicalLocalCard: yield* encodeCanonical(AgentCard, localCard),
    registerRequest,
  } satisfies Fixture;
}).pipe(Effect.orDie, Effect.withSpan("makeFixture"));

/** A registered runtime uses real protocol state with a quiet, reachable Router. */
export const runRegisteredFixture = (fixture: Fixture) =>
  saveLocalIdentity(fixture.localCard).pipe(
    Effect.zipRight(
      runDaemonRuntime(fixture.bootstrap).pipe(
        Effect.provide(identityLayerFor(fixture.bootstrap)),
        Effect.provide(Firewall.layer()),
        Effect.provideService(Registry, {
          register: () =>
            Effect.dieMessage("Registered fixture attempted registration"),
          lookup: (request) => {
            const card = fixture.peerCards.find((peer) =>
              "agentName" in request
                ? peer.agentName === request.agentName
                : peer.agentId === request.agentId,
            );
            return Effect.succeed(
              card === undefined
                ? { kind: "not_found" as const }
                : { kind: "found" as const, agentCard: card },
            );
          },
        }),
        Effect.provideService(Router, {
          poll: (call) =>
            call.request.pollCursor === undefined
              ? Effect.succeed(emptyBatch(routerInstanceId(21), pollCursor(24)))
              : Effect.never,
          send: () => Effect.never,
        }),
      ),
    ),
  );

/** Tests compose the same saved identity service as the production daemon. */
export function identityLayerFor(bootstrap: DaemonBootstrap) {
  return localIdentityLayer({
    registrySignerPublicKey: bootstrap.configuration.registrySignerPublicKey,
    agentPublicKey: bootstrap.agentPublicKey,
    signingAuthority: bootstrap.signingAuthority,
    admissionCredential: bootstrap.admissionCredential,
  });
}

/** Startup can initialize persistence before its configured listener accepts MCP connections. */
export function acquireDaemonClient(
  fixture: Fixture,
): Effect.Effect<Client, unknown, Scope.Scope> {
  return Effect.acquireRelease(
    connectDaemonClient(fixture).pipe(
      Effect.retry(
        Schedule.spaced(Duration.millis(25)).pipe(
          Schedule.intersect(Schedule.recurs(400)),
        ),
      ),
    ),
    (client) => Effect.tryPromise(() => client.close()).pipe(Effect.ignore),
  );
}

function connectDaemonClient(fixture: Fixture) {
  return Effect.gen(function* () {
    const client = new Client(
      { name: "daemon-runtime-test", version: "1" },
      {
        capabilities: {},
        versionNegotiation: { mode: { pin: "2026-07-28" } },
      },
    );
    yield* Effect.tryPromise(() =>
      client.connect(
        new StreamableHTTPClientTransport(
          new URL(
            `http://127.0.0.1:${fixture.bootstrap.configuration.mcpPort}/mcp`,
          ),
          { requestInit: { headers: mcpTestHeaders } },
        ),
      ),
    ).pipe(
      Effect.tapError(() =>
        Effect.tryPromise(() => client.close()).pipe(Effect.ignore),
      ),
    );
    return client;
  });
}
