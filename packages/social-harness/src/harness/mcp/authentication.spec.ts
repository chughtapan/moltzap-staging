/** @file OAuth admission validates owner-bound JWTs against a real issuer key-set endpoint. */
import { HttpServerRequest, HttpServerResponse } from "@effect/platform";
import { NodeHttpServer } from "@effect/platform-node";
import { scopedLive as it } from "@effect/vitest";
import { Effect, Ref } from "effect";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- The issuer fixture owns an actual HTTP listener for discovery and JWKS.
import { createServer } from "node:http";
import { expect } from "vitest";
import type { McpAuthentication } from "./authentication.js";
import { mcpFixture, mcpRequest } from "../__tests__/mcp-fixtures.js";
import { INBOX_PENDING_EVENT } from "./contracts.js";

const resource = new URL("https://mcp.example.test/mcp");
const owner = "owner-subject";
const scope = "use:mcp";

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
    allowedOrigins: ["https://owner.example.test"],
  };
  const test = yield* mcpFixture({ authentication: oauth });
  const token = (
    changes: {
      readonly audience?: string;
      readonly subject?: string;
      readonly scope?: string;
      readonly expiresAt?: number;
    } = {},
  ) =>
    Effect.tryPromise(() =>
      new SignJWT({ scope: changes.scope ?? scope })
        .setProtectedHeader({ alg: "RS256", kid: "issuer-key" })
        .setIssuer(issuer)
        .setAudience(changes.audience ?? resource.href)
        .setSubject(changes.subject ?? owner)
        .setExpirationTime(
          changes.expiresAt ?? Math.floor(Date.now() / 1000) + 300,
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

/* eslint-disable agent-code-guard/no-hardcoded-assertion-literals -- HTTP statuses and MCP wire notifications are external conformance contracts. */
// @agent-code-guard/regression-only: These cases pin finite OAuth admission and stream-expiry contracts against a real issuer.
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

it("admits the owner through the actual MCP initialize path", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    const response = yield* test.call(yield* test.token());
    expect(response.status).toBe(200);
  }));

it("selects each authenticated subject under its verified issuer", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    const other = "another-owner";
    expect((yield* test.call(yield* test.token())).status).toBe(200);
    expect(
      (yield* test.call(yield* test.token({ subject: other }))).status,
    ).toBe(200);
    expect(yield* Ref.get(test.owners)).toEqual([
      { issuer: test.oauth.issuer, subject: owner },
      { issuer: test.oauth.issuer, subject: other },
    ]);
  }));

it.each([
  { subject: "" },
  { audience: "https://another-resource.example/mcp" },
  { scope: "another-scope" },
  { expiresAt: 1 },
])("refuses a token outside the resource authority: %j", (changes) =>
  Effect.gen(function* () {
    const test = yield* fixture;
    const response = yield* test.call(yield* test.token(changes));
    expect(response.status).toBe(401);
  }),
);

it("rejects a different Host even for the correct owner's token", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    const response = yield* test.call(
      yield* test.token(),
      "another-resource.example",
    );
    expect(response.status).toBe(403);
  }));

it(
  "closes an admitted Events stream at token expiry without closing later requests",
  () =>
    Effect.gen(function* () {
      const test = yield* fixture;
      const token = yield* test.token({
        expiresAt: Math.floor(Date.now() / 1000) + 2,
      });
      const response = yield* Effect.tryPromise(() =>
        test.fetch(
          mcpRequest(
            "events/stream",
            {
              name: INBOX_PENDING_EVENT,
              arguments: {},
              cursor: null,
            },
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
      expect(body).toContain("notifications/events/active");
      const later = yield* test.call(yield* test.token());
      expect(later.status).toBe(200);
    }),
  15000,
);

it.each([
  { origin: "https://owner.example.test", status: 200 },
  { origin: "https://another-host.example", status: 403 },
])("admits only configured browser origins: %j", ({ origin, status }) =>
  Effect.gen(function* () {
    const test = yield* fixture;
    const response = yield* test.call(yield* test.token(), undefined, origin);
    expect(response.status).toBe(status);
  }),
);

/* eslint-enable agent-code-guard/no-hardcoded-assertion-literals -- Restore the assertion policy. */
