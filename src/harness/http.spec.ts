/** @file OAuth admission validates owner-bound JWTs against a real issuer key-set endpoint. */
import { HttpServerRequest, HttpServerResponse } from "@effect/platform";
import { NodeHttpServer } from "@effect/platform-node";
import { scopedLive as it } from "@effect/vitest";
import { Effect, FastCheck as fc, Ref } from "effect";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- The issuer fixture owns an actual HTTP listener for discovery and JWKS.
import { createServer } from "node:http";
import { expect } from "vitest";
import type { McpAuthentication } from "./http.js";
import { mcpFixture, mcpRequest } from "./__tests__/mcp-fixtures.js";

const resource = new URL("https://mcp.example.test/mcp");
const owner = "owner-subject";
const scope = "use:mcp";
const allowedOrigin = "https://owner.example.test";

const fixture = Effect.gen(function* () {
  const keys = yield* Effect.tryPromise(() => generateKeyPair("RS256"));
  const key = yield* Effect.tryPromise(() => exportJWK(keys.publicKey));
  const server = yield* NodeHttpServer.make(createServer, {
    host: "127.0.0.1",
    port: 0,
  });
  if (server.address._tag !== "TcpAddress") {
    return yield* Effect.dieMessage("Expected issuer listener");
  }
  const issuer = `http://127.0.0.1:${server.address.port}/`;
  yield* server.serve(
    Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest;
      const data =
        request.url === "/.well-known/openid-configuration"
          ? { issuer, jwks_uri: `${issuer}jwks` }
          : { keys: [{ ...key, kid: "issuer-key", alg: "RS256" }] };
      return yield* HttpServerResponse.json(data).pipe(Effect.orDie);
    }),
  );
  const oauth: McpAuthentication = {
    kind: "oauth",
    resource,
    issuer,
    scope,
    allowedOrigins: [allowedOrigin],
  };
  const test = yield* mcpFixture({ authentication: oauth });
  const token = (
    changes: {
      readonly audience?: string;
      readonly subject?: string;
      readonly granted?: readonly string[];
      readonly secondsLeft?: number;
    } = {},
  ) =>
    Effect.tryPromise(() =>
      new SignJWT({ scope: (changes.granted ?? [scope]).join(" ") })
        .setProtectedHeader({ alg: "RS256", kid: "issuer-key" })
        .setIssuer(issuer)
        .setAudience(changes.audience ?? resource.href)
        .setSubject(changes.subject ?? owner)
        .setExpirationTime(
          Math.floor(Date.now() / 1000) + (changes.secondsLeft ?? 300),
        )
        .sign(keys.privateKey),
    );
  const call = (bearer?: string, host?: string, origin?: string) =>
    Effect.tryPromise(() =>
      test.fetch(
        mcpRequest(
          "initialize",
          {
            protocolVersion: "2026-07-28",
            capabilities: {},
            clientInfo: { name: "oauth-test", version: "1" },
          },
          {
            headers: {
              host: host ?? resource.host,
              authorization: bearer === undefined ? "" : `Bearer ${bearer}`,
              ...(origin === undefined ? {} : { origin }),
            },
          },
        ),
      ),
    );
  return { ...test, oauth, token, call };
});

/** A scope that begins with the configured one and goes on past it. */
const longerScope = fc
  .stringMatching(/^[a-z:]{1,3}$/u)
  .map((more) => `${scope}${more}`);

/**
 * A request the resource admits: its Host in any letter case, no Origin or the
 * allowed one, and the configured scope anywhere among those granted.
 */
const admissible = fc.record({
  host: fc.mixedCase(fc.constant(resource.host)),
  origin: fc.constantFrom(undefined, allowedOrigin),
  subject: fc.string({ minLength: 1 }),
  granted: fc
    .tuple(fc.array(longerScope), fc.array(longerScope))
    .map(([before, after]) => [...before, scope, ...after]),
});

/** One claim that puts a token outside the resource's authority. */
const defect = fc.oneof(
  fc.record({ subject: fc.constant("") }),
  fc.record({ audience: fc.webUrl().filter((url) => url !== resource.href) }),
  fc.record({ granted: fc.array(longerScope) }),
  fc.record({ secondsLeft: fc.integer({ min: -3600, max: 0 }) }),
);

/**
 * The headers of a caller the resource does not trust: another Host, or the
 * resource's Host with an Origin that is not allowed.
 */
const untrusted = fc.oneof(
  fc.record({
    host: fc.domain().filter((host) => host !== resource.host),
    origin: fc.constant(undefined),
  }),
  fc.record({
    host: fc.constant(resource.host),
    origin: fc.webUrl().filter((origin) => origin !== allowedOrigin),
  }),
);

it("advertises protected-resource discovery without granting MCP access", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    const metadata = yield* Effect.tryPromise(() =>
      test.fetch(
        new Request(
          new URL("/.well-known/oauth-protected-resource/mcp", resource),
          { headers: { host: resource.host } },
        ),
      ),
    );
    expect(metadata.status).toBe(200);
    expect(yield* Effect.tryPromise(() => metadata.json())).toEqual({
      resource: resource.href,
      authorization_servers: [test.oauth.issuer],
      scopes_supported: [scope],
      bearer_methods_supported: ["header"],
    });
    const refused = yield* test.call();
    expect(refused.status).toBe(401);
    expect(refused.headers.get("www-authenticate")).toContain(
      "resource_metadata=",
    );
  }));

it("selects the subject of a token granted the scope, under the verified issuer", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    yield* Effect.tryPromise(() =>
      fc.assert(
        fc.asyncProperty(admissible, ({ host, origin, ...claims }) =>
          Effect.gen(function* () {
            yield* Ref.set(test.owners, []);
            const token = yield* test.token(claims);
            const response = yield* test.call(token, host, origin);
            expect(response.status).toBe(200);
            expect(yield* Ref.get(test.owners)).toEqual([
              { issuer: test.oauth.issuer, subject: claims.subject },
            ]);
          }).pipe(Effect.runPromise),
        ),
      ),
    );
  }));

it("refuses a token with an empty subject, another audience, no grant of the scope or no time left", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    yield* Effect.tryPromise(() =>
      fc.assert(
        fc.asyncProperty(defect, (claim) =>
          Effect.gen(function* () {
            const response = yield* test.call(yield* test.token(claim));
            expect(response.status).toBe(401);
            expect(yield* Ref.get(test.owners)).toEqual([]);
          }).pipe(Effect.runPromise),
        ),
      ),
    );
  }));

it("rejects another Host or an Origin that is not allowed, even with the owner's token", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    const token = yield* test.token();
    yield* Effect.tryPromise(() =>
      fc.assert(
        fc.asyncProperty(untrusted, ({ host, origin }) =>
          Effect.gen(function* () {
            const response = yield* test.call(token, host, origin);
            expect(response.status).toBe(403);
            expect(yield* Ref.get(test.owners)).toEqual([]);
          }).pipe(Effect.runPromise),
        ),
      ),
    );
  }));

it(
  "closes an admitted stream at token expiry without closing later requests",
  () =>
    Effect.gen(function* () {
      const test = yield* fixture;
      const token = yield* test.token({ secondsLeft: 2 });
      const response = yield* Effect.tryPromise(() =>
        test.fetch(
          mcpRequest(
            "subscriptions/listen",
            { notifications: { toolsListChanged: true } },
            {
              headers: {
                host: resource.host,
                authorization: `Bearer ${token}`,
              },
            },
          ),
        ),
      );
      expect(response.status).toBe(200);
      const body = yield* Effect.tryPromise(() => response.text());
      expect(body).toContain("notifications/subscriptions/acknowledged");
      const later = yield* test.call(yield* test.token());
      expect(later.status).toBe(200);
    }),
  15000,
);
