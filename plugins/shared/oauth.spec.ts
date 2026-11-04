/** @file The official MCP OAuth client rotates refresh tokens against real HTTP endpoints. */
import { HttpServerRequest, HttpServerResponse } from "@effect/platform";
import { NodeHttpServer } from "@effect/platform-node";
import type {
  OAuthClientProvider,
  StoredOAuthTokens,
} from "@modelcontextprotocol/client";
import { Deferred, Effect, Fiber, Redacted, Ref, Schema } from "effect";
import { createServer } from "node:http";
import { describe, expect, it } from "vitest";
import { toolDefinitions } from "@moltzap/social-harness";
import {
  mcpFixture,
  mcpTestCredential,
} from "../../packages/social-harness/src/harness/__tests__/mcp-fixtures.js";
import { acquireMcpConnection, callTool } from "./mcp-client.js";

const entry = Schema.decodeUnknownSync(toolDefinitions.read_inbox.output)({
  items: [
    {
      deliveryToken: `dlv_${"A".repeat(43)}`,
      address: "agent:bob",
      item: {
        kind: "operationFailed",
        operationId: `col_${"A".repeat(64)}`,
        error: "request failed",
      },
    },
  ],
}).items[0];
if (entry === undefined) {
  throw new Error("Missing inbox fixture");
}

const fixture = Effect.gen(function* () {
  const acknowledged = yield* Ref.make(0);
  const receipt = yield* Deferred.make<void>();
  const daemon = yield* mcpFixture({
    harness: {
      listInbox: () =>
        Ref.get(acknowledged).pipe(
          Effect.map((count) => ({ items: count === 0 ? [entry] : [] })),
        ),
      acknowledgeDelivery: () =>
        Ref.update(acknowledged, (count) => count + 1).pipe(
          Effect.zipRight(Deferred.succeed(receipt, undefined)),
          Effect.asVoid,
        ),
    },
  });
  const server = yield* NodeHttpServer.make(createServer, {
    host: "127.0.0.1",
    port: 0,
  });
  if (server.address._tag !== "TcpAddress") {
    return yield* Effect.dieMessage("Expected TCP listener");
  }
  const issuer = `http://127.0.0.1:${server.address.port}/`;
  const endpoint = new URL("mcp", issuer);
  const refreshes = yield* Ref.make(0);
  const revoked = yield* Ref.make(false);
  const invalidations = yield* Ref.make(0);
  const unauthorizedWave = yield* Ref.make<
    | {
        readonly remaining: Ref.Ref<number>;
        readonly ready: Deferred.Deferred<void>;
      }
    | undefined
  >(undefined);
  yield* server.serve(
    Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest;
      const wave = yield* Ref.get(unauthorizedWave);
      if (
        request.url === "/mcp" &&
        request.headers.authorization === "Bearer expired" &&
        wave !== undefined
      ) {
        if (
          (yield* Ref.updateAndGet(wave.remaining, (count) => count - 1)) === 0
        ) {
          yield* Deferred.succeed(wave.ready, undefined);
        }
        yield* Deferred.await(wave.ready);
      }
      if (request.url === "/.well-known/oauth-protected-resource/mcp") {
        return yield* HttpServerResponse.json({
          resource: endpoint.href,
          authorization_servers: [issuer],
          scopes_supported: ["use:mcp"],
        }).pipe(Effect.orDie);
      }
      if (
        request.url === "/.well-known/oauth-authorization-server" ||
        request.url === "/.well-known/openid-configuration"
      ) {
        return yield* HttpServerResponse.json({
          issuer,
          authorization_endpoint: `${issuer}authorize`,
          token_endpoint: `${issuer}token`,
          response_types_supported: ["code"],
          grant_types_supported: ["authorization_code", "refresh_token"],
          token_endpoint_auth_methods_supported: ["none"],
          code_challenge_methods_supported: ["S256"],
        }).pipe(Effect.orDie);
      }
      if (request.url === "/token") {
        const body = new URLSearchParams(
          yield* request.text.pipe(Effect.orDie),
        );
        const count = yield* Ref.get(refreshes);
        const valid =
          !(yield* Ref.get(revoked)) &&
          body.get("grant_type") === "refresh_token" &&
          body.get("refresh_token") === `refresh-${count}` &&
          body.get("resource") === endpoint.href;
        if (!valid) {
          return yield* HttpServerResponse.json(
            { error: "invalid_grant" },
            { status: 400 },
          ).pipe(Effect.orDie);
        }
        yield* Ref.update(refreshes, (value) => value + 1);
        return yield* HttpServerResponse.json({
          access_token: Redacted.value(mcpTestCredential),
          token_type: "Bearer",
          refresh_token: `refresh-${count + 1}`,
          expires_in: 60,
          scope: "use:mcp",
        }).pipe(Effect.orDie);
      }
      return yield* daemon.app;
    }),
  );
  const tokens = yield* Ref.make<StoredOAuthTokens>({
    access_token: "expired",
    token_type: "Bearer",
    refresh_token: "refresh-0",
    issuer,
  });
  const unexpectedConsent = () => {
    throw new Error("Interactive consent must not replace refresh");
  };
  const provider: OAuthClientProvider = {
    redirectUrl: `${issuer}callback`,
    clientMetadata: {
      redirect_uris: [`${issuer}callback`],
      grant_types: ["authorization_code", "refresh_token"],
      response_types: ["code"],
      token_endpoint_auth_method: "none",
      scope: "use:mcp",
    },
    clientInformation: () => ({
      client_id: "fixture-client",
      token_endpoint_auth_method: "none",
      issuer,
    }),
    tokens: () => Effect.runPromise(Ref.get(tokens)),
    saveTokens: (value) => Effect.runPromise(Ref.set(tokens, value)),
    invalidateCredentials: () =>
      Effect.runPromise(
        Ref.update(invalidations, (count) => count + 1).pipe(
          Effect.zipRight(
            Ref.set(tokens, {
              access_token: "invalidated",
              token_type: "Bearer",
              issuer,
            }),
          ),
        ),
      ),
    redirectToAuthorization: unexpectedConsent,
    saveCodeVerifier: unexpectedConsent,
    codeVerifier: unexpectedConsent,
  };
  const expire = Ref.update(tokens, (value) => ({
    ...value,
    access_token: "expired",
  }));
  return {
    endpoint,
    provider,
    tokens,
    expire,
    refreshes,
    revoked,
    acknowledged,
    receipt,
    invalidations,
    unauthorizedWave,
  };
});

// @agent-code-guard/regression-only: OAuth refresh and revocation are external wire contracts exercised through the stock SDK.
describe("renewable MCP authorization", () => {
  it("keeps rotating grants usable when tool calls are unauthorized concurrently", () =>
    Effect.gen(function* () {
      const test = yield* fixture;
      const connection = yield* acquireMcpConnection(
        { endpoint: test.endpoint, authProvider: test.provider },
        { name: "oauth-fixture", version: "1" },
      );
      yield* test.expire;
      yield* Ref.set(test.unauthorizedWave, {
        remaining: yield* Ref.make(2),
        ready: yield* Deferred.make<void>(),
      });
      const pages = yield* Effect.all(
        [
          callTool(connection.client, { name: "read_inbox", arguments: {} }),
          callTool(connection.client, { name: "read_inbox", arguments: {} }),
        ],
        { concurrency: 2 },
      );
      expect(pages.flatMap((page) => page.items)).toContainEqual(entry);
      expect(yield* Ref.get(test.invalidations)).toBe(0);
      expect((yield* Ref.get(test.tokens)).refresh_token).toBe(
        `refresh-${yield* Ref.get(test.refreshes)}`,
      );
    }).pipe(Effect.scoped, Effect.runPromise));

  it("refreshes at connection and again without replacing the connection", () =>
    Effect.gen(function* () {
      const test = yield* fixture;
      const connection = yield* acquireMcpConnection(
        { endpoint: test.endpoint, authProvider: test.provider },
        { name: "oauth-fixture", version: "1" },
      );
      expect(yield* Ref.get(test.refreshes)).toBe(1);
      yield* test.expire;
      expect(
        (yield* callTool(connection.client, {
          name: "read_inbox",
          arguments: {},
        })).items,
      ).toEqual([entry]);
      expect(yield* Ref.get(test.refreshes)).toBe(2);
      expect((yield* Ref.get(test.tokens)).refresh_token).toBe("refresh-2");
    }).pipe(Effect.scoped, Effect.runPromise));

  it("delivers and acknowledges after refreshing an expired receiver credential", () =>
    Effect.gen(function* () {
      const test = yield* fixture;
      const connection = yield* acquireMcpConnection(
        { endpoint: test.endpoint, authProvider: test.provider },
        { name: "oauth-fixture", version: "1" },
      );
      yield* test.expire;
      const receiver = yield* connection
        .consumeInbox({}, (received) =>
          Effect.sync(() => expect(received).toEqual(entry)),
        )
        .pipe(Effect.forkScoped);
      yield* Deferred.await(test.receipt);
      yield* Fiber.interrupt(receiver);
      expect(yield* Ref.get(test.refreshes)).toBe(2);
      expect(yield* Ref.get(test.acknowledged)).toBe(1);
    }).pipe(Effect.scoped, Effect.runPromise));

  it("leaves work pending when the refresh grant has been revoked", () =>
    Effect.gen(function* () {
      const test = yield* fixture;
      const connection = yield* acquireMcpConnection(
        { endpoint: test.endpoint, authProvider: test.provider },
        { name: "oauth-fixture", version: "1" },
      );
      yield* Ref.set(test.revoked, true);
      yield* test.expire;
      const refused = yield* connection
        .consumeInbox({}, () =>
          Effect.dieMessage("Revoked client must not receive content"),
        )
        .pipe(Effect.flip);
      expect(refused.reason).toBe("transport-failed");
      expect(yield* Ref.get(test.acknowledged)).toBe(0);
    }).pipe(Effect.scoped, Effect.runPromise));
});
