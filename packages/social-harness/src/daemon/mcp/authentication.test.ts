/** @file Public OAuth admission through real HTTP and a locally served issuer key set. */
import { scopedLive as it } from "@effect/vitest";
import {
  CLIENT_CAPABILITIES_META_KEY,
  CLIENT_INFO_META_KEY,
  PROTOCOL_VERSION_META_KEY,
} from "@modelcontextprotocol/server";
import { Effect, Schedule, Schema } from "effect";
import {
  exportJWK,
  generateKeyPair,
  type JWK,
  type JWTPayload,
  SignJWT,
} from "jose";
import assert from "node:assert/strict";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- The issuer fixture uses a real Node HTTP listener to exercise JOSE discovery and JWKS retrieval.
import {
  createServer,
  request as httpRequest,
  type IncomingMessage,
  type Server,
} from "node:http";
import { describe, expect } from "vitest";
import type { DaemonOAuth } from "./authentication.js";
import { openInboxFixture } from "../../__tests__/inbox-fixture.js";
import { mcpIdentity } from "../../__tests__/mcp-http-fixtures.js";
import { stateDirectory } from "../../__tests__/store-schema-fixtures.js";
import {
  token as deliveryToken,
  enqueue,
} from "../../__tests__/webhook-fixtures.js";
import { decodePollResult } from "../../host-protocol/index.js";
import {
  INBOX_ITEM_EVENT,
  INBOX_PENDING_EVENT,
} from "../../host-protocol/names.js";
import { Identity } from "../../identity/local.js";
import {
  acquireHarnessMcpHttpServer,
  maximumPublicMcpRequestBytes,
} from "./http.js";
import {
  type HarnessMcpOperations,
  makeHarnessMcpHttpHandler,
} from "./index.js";

/* eslint-disable agent-code-guard/promise-type, agent-code-guard/no-hardcoded-assertion-literals -- Native HTTP fixture lifecycles and JWT signing are Promise APIs; expected statuses and claims define the OAuth boundary. */

const resource = new URL("https://mcp.example.test/mcp");
const owner = "owner-subject";
const scope = "use:mcp";
const unrelated = () => Effect.dieMessage("Unrelated daemon operation");
const operations: HarnessMcpOperations = {
  readStatus: () => Effect.succeed({ kind: "unregistered" }),
  register: unrelated,
  searchConversations: unrelated,
  readConversation: unrelated,
  readEvent: unrelated,
  send: unrelated,
  readInbox: () => Effect.succeed({ items: [] }),
  readInboxSummary: () =>
    Effect.succeed({ pendingCount: 0, newestSequence: 0 }),
  acknowledgeDelivery: unrelated,
};

const listen = (server: Server) =>
  Effect.async<undefined, Error>((resume) => {
    server.once("error", (error) => {
      resume(Effect.fail(error));
    });
    server.listen(0, "127.0.0.1", () => {
      resume(Effect.succeed(undefined));
    });
  });
const port = (server: Server) => {
  const address = server.address();
  assert(address !== null && typeof address !== "string");
  return address.port;
};
const close = (server: Server) =>
  Effect.async<undefined>((resume) => {
    server.close(() => {
      resume(Effect.succeed(undefined));
    });
    server.closeAllConnections();
  });

function issuerFixture(
  store?: Effect.Effect.Success<ReturnType<typeof openInboxFixture>>,
  behavior: {
    readonly path?: string;
    readonly status?: number;
    readonly delay?: number;
    readonly onStatus?: () => void;
  } = {},
) {
  return Effect.gen(function* () {
    const inbox = store ?? (yield* openInboxFixture(stateDirectory()));
    const keys = yield* Effect.tryPromise(() => generateKeyPair("RS256"));
    const jwk = yield* Effect.tryPromise(() => exportJWK(keys.publicKey));
    const issuerPath = behavior.path ?? "/";
    const server = issuerServer(jwk, issuerPath, behavior.status ?? 200);
    // eslint-disable-next-line agent-code-guard/acquire-release-requires-scope -- scopedLive owns this fixture and releases its listener after each test.
    yield* Effect.acquireRelease(listen(server), () => close(server));
    const issuer = `http://127.0.0.1:${port(server)}${issuerPath}`;
    const oauth: DaemonOAuth = {
      resource,
      issuer,
      ownerSubject: owner,
      scope,
      allowedOrigins: ["https://ui.example.test"],
    };
    const sign = signer(issuer, keys.privateKey);
    const token = () => sign({});
    const handler = yield* fixtureHandler(oauth, {
      store: inbox,
      delay: behavior.delay ?? 0,
      onStatus: behavior.onStatus,
    });
    const endpoint = yield* acquireHarnessMcpHttpServer({
      port: 0,
      handler,
      oauth,
    });
    return {
      endpoint: new URL(`http://127.0.0.1:${port(endpoint)}/mcp`),
      handler,
      oauth,
      token,
      sign,
    };
  });
}

function request(
  endpoint: URL,
  token: string | { readonly value: string; readonly scheme: string } = "",
  method = "tools/list",
  params: Readonly<Record<string, unknown>> = {},
) {
  return rawFetch(endpoint, {
    method: "POST",
    headers: {
      host: resource.host,
      accept: "application/json, text/event-stream",
      "content-type": "application/json",
      "mcp-method": method,
      ...(typeof params.name === "string" ? { "mcp-name": params.name } : {}),
      ...(token === "" ? {} : { authorization: authorization(token) }),
    },
    body: requestBody(method, params),
  });
}

function requestBody(
  method: string,
  params: Readonly<Record<string, unknown>>,
): string {
  return JSON.stringify({
    jsonrpc: "2.0",
    id: "oauth-request",
    method,
    params: {
      ...params,
      _meta: {
        ...(typeof params._meta === "object" ? params._meta : {}),
        [PROTOCOL_VERSION_META_KEY]: "2026-07-28",
        [CLIENT_INFO_META_KEY]: { name: "oauth-test", version: "1" },
        [CLIENT_CAPABILITIES_META_KEY]: {},
      },
    },
  });
}

function authorization(
  token: string | { readonly value: string; readonly scheme: string },
): string {
  return typeof token === "string"
    ? `Bearer ${token}`
    : `${token.scheme} ${token.value}`;
}

const discoversAndAuthenticates = () =>
  Effect.gen(function* () {
    const fixture = yield* issuerFixture();
    const discovery = yield* Effect.tryPromise(() =>
      rawFetch(
        new URL("/.well-known/oauth-protected-resource/mcp", fixture.endpoint),
        { headers: { host: resource.host } },
      ),
    );
    expect(discovery.status).toBe(200);
    expect(yield* Effect.tryPromise(() => discovery.json())).toMatchObject({
      resource: resource.href,
      authorization_servers: [fixture.oauth.issuer],
      scopes_supported: [scope],
    });
    const missing = yield* Effect.tryPromise(() => request(fixture.endpoint));
    expect(missing.status).toBe(401);
    expect(missing.headers.get("www-authenticate")).toContain(
      "https://mcp.example.test/.well-known/oauth-protected-resource/mcp",
    );
    const token = yield* Effect.tryPromise(() => fixture.token());
    const accepted = yield* Effect.tryPromise(() =>
      request(fixture.endpoint, token),
    );
    const lowercase = yield* Effect.tryPromise(() =>
      request(fixture.endpoint, { value: token, scheme: "bearer" }),
    );
    expect(lowercase.status).toBe(200);
    yield* Effect.tryPromise(
      () => lowercase.body?.cancel() ?? Promise.resolve(),
    );
    expect(accepted.status).toBe(200);
    expect(yield* Effect.tryPromise(() => accepted.json())).toHaveProperty(
      "result.tools",
    );
  });

const discoversPathIssuer = () =>
  Effect.gen(function* () {
    const fixture = yield* issuerFixture(undefined, { path: "/tenant" });
    const token = yield* Effect.tryPromise(() => fixture.token());
    const accepted = yield* Effect.tryPromise(() =>
      request(fixture.endpoint, token),
    );
    expect(accepted.status).toBe(200);
    expect(yield* Effect.tryPromise(() => accepted.json())).toHaveProperty(
      "result.tools",
    );
  });

const refusesFailedDiscovery = () =>
  Effect.gen(function* () {
    const result = yield* Effect.either(
      issuerFixture(undefined, { status: 503 }),
    );
    expect(result).toMatchObject({
      _tag: "Left",
      left: { reason: "authentication-unavailable" },
    });
  });

const completesAdmittedUnary = () =>
  Effect.gen(function* () {
    const fixture = yield* issuerFixture(undefined, { delay: 2100 });
    const exp = Math.floor(Date.now() / 1000) + 2;
    const token = yield* Effect.tryPromise(() => fixture.sign({ exp }));
    const response = yield* Effect.tryPromise(() =>
      request(fixture.endpoint, token, "tools/call", {
        name: "status",
        arguments: {},
      }),
    );
    expect(response.status).toBe(200);
    expect(yield* Effect.tryPromise(() => response.json())).toHaveProperty(
      "result.structuredContent",
    );
    expect(Date.now()).toBeGreaterThanOrEqual(exp * 1000);
  });

const boundsPublicBodies = () =>
  Effect.gen(function* () {
    const fixture = yield* issuerFixture();
    const oversized = yield* Effect.tryPromise(() =>
      rawFetch(fixture.endpoint, {
        method: "POST",
        headers: { host: resource.host, "transfer-encoding": "chunked" },
        body: "€".repeat(Math.ceil(maximumPublicMcpRequestBytes / 3) + 1),
      }),
    );
    expect(oversized.status).toBe(413);
    expect(oversized.headers.get("connection")).toBe("close");
    yield* Effect.tryPromise(() => oversized.text());
    const token = yield* Effect.tryPromise(() => fixture.token());
    const malformed = yield* Effect.tryPromise(() =>
      rawFetch(fixture.endpoint, {
        method: "POST",
        headers: {
          host: resource.host,
          authorization: `Bearer ${token}`,
          "content-type": "application/json",
          accept: "application/json, text/event-stream",
        },
        body: "{",
      }),
    );
    expect(malformed.status).toBe(400);
    expect(yield* Effect.tryPromise(() => malformed.json())).toMatchObject({
      error: { code: -32700 },
    });
    const accepted = yield* Effect.tryPromise(() =>
      request(fixture.endpoint, token),
    );
    expect(yield* Effect.tryPromise(() => accepted.json())).toHaveProperty(
      "result.tools",
    );
  });

const containsMalformedTarget = () =>
  Effect.gen(function* () {
    const fixture = yield* issuerFixture();
    const malformed = yield* Effect.tryPromise(() =>
      rawFetch(fixture.endpoint, {
        path: "//[",
        headers: { host: resource.host },
      }),
    );
    expect(malformed.status).toBe(400);
    yield* Effect.tryPromise(() => malformed.text());
    const token = yield* Effect.tryPromise(() => fixture.token());
    const accepted = yield* Effect.tryPromise(() =>
      request(fixture.endpoint, token),
    );
    expect(accepted.status).toBe(200);
    yield* Effect.tryPromise(() => accepted.text());
  });

const checksMethodEnvelope = () =>
  Effect.gen(function* () {
    const fixture = yield* issuerFixture();
    const token = yield* Effect.tryPromise(() => fixture.token());
    for (const method of ["tools/list", undefined]) {
      const response = yield* Effect.tryPromise(() =>
        rawFetch(fixture.endpoint, {
          method: "POST",
          headers: {
            host: resource.host,
            authorization: `Bearer ${token}`,
            "content-type": "application/json",
            "mcp-name": INBOX_PENDING_EVENT,
            accept: "application/json, text/event-stream",
            ...(method === undefined ? {} : { "mcp-method": method }),
          },
          body: requestBody("events/stream", {
            name: INBOX_PENDING_EVENT,
            arguments: {},
          }),
        }),
      );
      expect(response.status).toBe(400);
      yield* Effect.tryPromise(() => response.text());
    }
    const status = yield* Effect.tryPromise(() =>
      request(fixture.endpoint, token, "tools/call", {
        name: "event_subscription_status",
        arguments: {},
      }),
    );
    expect(yield* Effect.tryPromise(() => status.json())).toHaveProperty(
      "result.structuredContent.mode",
      "none",
    );
  });

/**
 * Value: protects=the specified 1 MiB public request boundary;
 * fails_when=the exact limit is rejected or one extra byte is admitted;
 * why_new=the oversized case derives its size from production; seam=none.
 */
const acceptsExactBodyBoundary = () =>
  Effect.gen(function* () {
    const fixture = yield* issuerFixture();
    const token = yield* Effect.tryPromise(() => fixture.token());
    const protocolBody = requestBody("tools/list", {});
    const limit = 1_048_576;
    const body =
      protocolBody + " ".repeat(limit - Buffer.byteLength(protocolBody));
    const headers = {
      host: resource.host,
      authorization: `Bearer ${token}`,
      "content-type": "application/json",
      accept: "application/json, text/event-stream",
      "mcp-method": "tools/list",
    };
    const accepted = yield* Effect.tryPromise(() =>
      rawFetch(fixture.endpoint, { method: "POST", headers, body }),
    );
    expect(accepted.status).toBe(200);
    expect(yield* Effect.tryPromise(() => accepted.json())).toHaveProperty(
      "result.tools",
    );
    const refused = yield* Effect.tryPromise(() =>
      rawFetch(fixture.endpoint, { method: "POST", headers, body: `${body} ` }),
    );
    expect(refused.status).toBe(413);
    expect(refused.headers.get("connection")).toBe("close");
    yield* Effect.tryPromise(() => refused.text());
  });

/**
 * Value: protects=GET-only metadata and the HTTP route whitelist;
 * fails_when=an unsupported method or route dispatches an operation;
 * why_new=discovery previously covered only successful GET; seam=none.
 */
const restrictsMetadataRoutes = () =>
  Effect.gen(function* () {
    let dispatches = 0;
    const fixture = yield* issuerFixture(undefined, {
      onStatus: () => {
        dispatches += 1;
      },
    });
    const token = yield* Effect.tryPromise(() => fixture.token());
    for (const route of [
      {
        path: "/.well-known/oauth-protected-resource/mcp",
        method: "POST",
        status: 405,
        allow: "GET",
      },
      { path: "/mcp", method: "GET", status: 405, allow: "POST" },
      { path: "/unrelated", method: "GET", status: 404, allow: null },
    ]) {
      const response = yield* Effect.tryPromise(() =>
        rawFetch(new URL(route.path, fixture.endpoint), {
          method: route.method,
          headers: { host: resource.host, authorization: `Bearer ${token}` },
        }),
      );
      expect(response.status).toBe(route.status);
      expect(response.headers.get("allow")).toBe(route.allow);
      yield* Effect.tryPromise(() => response.text());
    }
    expect(dispatches).toBe(0);
    const accepted = yield* Effect.tryPromise(() =>
      request(fixture.endpoint, token, "tools/call", {
        name: "status",
        arguments: {},
      }),
    );
    expect(accepted.status).toBe(200);
    expect(yield* Effect.tryPromise(() => accepted.json())).toHaveProperty(
      "result.structuredContent",
    );
    expect(dispatches).toBe(1);
  });

/**
 * Value: protects=consumer release after explicit OAuth stream cancellation;
 * fails_when=a canceled stream prevents a replacement from becoming active;
 * why_new=the earlier lifetime test covered token expiry; seam=none.
 */
const cancelsAndReacquiresStream = () =>
  Effect.gen(function* () {
    const store = yield* openInboxFixture(stateDirectory());
    const fixture = yield* issuerFixture(store);
    yield* enqueue(store, 1);
    const token = yield* Effect.tryPromise(() => fixture.token());
    const first = yield* Effect.tryPromise(() =>
      request(fixture.endpoint, token, "events/stream", {
        name: INBOX_PENDING_EVENT,
        arguments: {},
      }),
    );
    const reader = yield* activeReader(first);
    yield* Effect.tryPromise(() => reader.cancel());
    const replacement = yield* reconnectStream(fixture.endpoint, token);
    const replacementReader = yield* activeReader(replacement);
    yield* Effect.tryPromise(() => replacementReader.cancel());
  });

function activeReader(response: Response) {
  if (response.body === null) {
    return Effect.dieMessage("Expected an active Events response body");
  }
  const reader = response.body.getReader();
  return Effect.tryPromise(() => reader.read()).pipe(
    Effect.tap((item) =>
      Effect.sync(() => {
        const bytes = Schema.decodeUnknownSync(Schema.Uint8ArrayFromSelf)(
          item.value,
        );
        expect(new TextDecoder().decode(bytes)).toContain(
          "notifications/events/active",
        );
      }),
    ),
    Effect.as(reader),
  );
}

function reconnectStream(endpoint: URL, token: string) {
  return Effect.tryPromise(() =>
    request(endpoint, token, "events/stream", {
      name: INBOX_PENDING_EVENT,
      arguments: {},
    }),
  ).pipe(
    Effect.flatMap((response) =>
      response.headers.get("content-type")?.startsWith("text/event-stream") ===
      true
        ? Effect.succeed(response)
        : Effect.tryPromise(
            () => response.body?.cancel() ?? Promise.resolve(),
          ).pipe(Effect.zipRight(Effect.fail({ reason: "consumer-closing" }))),
    ),
    Effect.retry({ times: 20, schedule: Schedule.spaced("10 millis") }),
  );
}

const checksPublicTransport = () =>
  Effect.gen(function* () {
    const fixture = yield* issuerFixture();
    const metadata = new URL(
      "/.well-known/oauth-protected-resource/mcp",
      fixture.endpoint,
    );
    const wrongHost = yield* Effect.tryPromise(() =>
      rawFetch(metadata, { headers: { host: "wrong.example.test" } }),
    );
    expect(wrongHost.status).toBe(403);
    yield* Effect.tryPromise(
      () => wrongHost.body?.cancel() ?? Promise.resolve(),
    );
    const wrongOrigin = yield* Effect.tryPromise(() =>
      rawFetch(metadata, {
        headers: { host: resource.host, origin: "https://wrong.example.test" },
      }),
    );
    expect(wrongOrigin.status).toBe(403);
    yield* Effect.tryPromise(
      () => wrongOrigin.body?.cancel() ?? Promise.resolve(),
    );
    const allowed = yield* Effect.tryPromise(() =>
      rawFetch(metadata, {
        headers: { host: resource.host, origin: "https://ui.example.test" },
      }),
    );
    expect(allowed.status).toBe(200);
    yield* Effect.tryPromise(() => allowed.body?.cancel() ?? Promise.resolve());
  });

const rejectsWrongAuthority = () =>
  Effect.gen(function* () {
    const fixture = yield* issuerFixture();
    for (const claims of [
      { sub: "another-owner" },
      { aud: "https://other.example.test/mcp" },
      { iss: "https://other-issuer.example.test/" },
      { scope: "different:scope" },
      { exp: Math.floor(Date.now() / 1000) - 1 },
      { nbf: Math.floor(Date.now() / 1000) + 120 },
      { exp: undefined },
      { scope: undefined },
    ]) {
      const token = yield* Effect.tryPromise(() => fixture.sign(claims));
      const response = yield* Effect.tryPromise(() =>
        request(fixture.endpoint, token),
      );
      expect(response.status).toBe(401);
    }
    const token = yield* Effect.tryPromise(() =>
      fixture.sign({
        aud: [resource.href, "https://issuer.example.test/userinfo"],
      }),
    );
    const response = yield* Effect.tryPromise(() =>
      request(fixture.endpoint, token),
    );
    expect(response.status).toBe(200);
    yield* Effect.tryPromise(
      () => response.body?.cancel() ?? Promise.resolve(),
    );
  });

const expiresStream = () =>
  Effect.gen(function* () {
    const fixture = yield* issuerFixture();
    const token = yield* Effect.tryPromise(() =>
      fixture.sign({ exp: Math.floor(Date.now() / 1000) + 2 }),
    );
    const response = yield* Effect.tryPromise(() =>
      request(fixture.endpoint, token, "events/stream", {
        name: INBOX_PENDING_EVENT,
        arguments: {},
      }),
    );
    expect(response.status).toBe(200);
    const text = yield* Effect.tryPromise(() => response.text()).pipe(
      Effect.timeout("5 seconds"),
    );
    expect(text).toContain("notifications/events/active");
    yield* Effect.yieldNow();
    const fresh = yield* Effect.tryPromise(() => fixture.token());
    const status = yield* Effect.tryPromise(() =>
      request(fixture.endpoint, fresh, "tools/call", {
        name: "event_subscription_status",
        arguments: {},
      }),
    );
    expect(yield* Effect.tryPromise(() => status.json())).toHaveProperty(
      "result.structuredContent.mode",
      "none",
    );
    const replacement = yield* Effect.tryPromise(() =>
      request(fixture.endpoint, fresh, "events/stream", {
        name: INBOX_PENDING_EVENT,
        arguments: {},
      }),
    );
    expect(replacement.status).toBe(200);
    const reader = yield* activeReader(replacement);
    yield* Effect.tryPromise(() => reader.cancel());
  });

const sharesInboxCustody = () =>
  Effect.gen(function* () {
    const store = yield* openInboxFixture(stateDirectory());
    const fixture = yield* issuerFixture(store);
    yield* enqueue(store, 1);
    const token = yield* Effect.tryPromise(() => fixture.token());
    const initial = yield* Effect.tryPromise(() =>
      request(fixture.endpoint, token, "events/poll", {
        name: INBOX_ITEM_EVENT,
        arguments: {},
        cursor: null,
      }),
    );
    const initialResult = yield* Effect.tryPromise(() => initial.json()).pipe(
      Effect.flatMap(
        Schema.decodeUnknown(Schema.Struct({ result: Schema.Unknown })),
      ),
      Effect.flatMap((value) => decodePollResult(value.result)),
    );
    const stream = yield* Effect.tryPromise(() =>
      request(fixture.endpoint, token, "events/stream", {
        name: INBOX_PENDING_EVENT,
        arguments: {},
      }),
    );
    expect(yield* Effect.tryPromise(() => stream.json())).toMatchObject({
      error: { data: { reason: "already-listening" } },
    });
    yield* readAndAcknowledge(fixture.endpoint, token, initialResult._meta);
    expect((yield* store.readInboxSummary()).pendingCount).toBe(0);
  });

function readAndAcknowledge(
  endpoint: URL,
  token: string,
  metadata: Readonly<Record<string, unknown>>,
) {
  return Effect.gen(function* () {
    const read = yield* Effect.tryPromise(() =>
      request(endpoint, token, "tools/call", {
        name: "read_inbox",
        arguments: {},
        _meta: metadata,
      }),
    );
    expect(yield* Effect.tryPromise(() => read.json())).toMatchObject({
      result: {
        structuredContent: { items: [{ deliveryToken: deliveryToken(1) }] },
      },
    });
    const ack = yield* Effect.tryPromise(() =>
      request(endpoint, token, "tools/call", {
        name: "acknowledge_delivery",
        arguments: { deliveryToken: deliveryToken(1) },
        _meta: metadata,
      }),
    );
    expect(yield* Effect.tryPromise(() => ack.json())).toHaveProperty("result");
    const retained = yield* Effect.tryPromise(() =>
      request(endpoint, token, "tools/call", {
        name: "read_event",
        arguments: { eventId: deliveryToken(1) },
      }),
    );
    expect(yield* Effect.tryPromise(() => retained.json())).toHaveProperty(
      "result.structuredContent.item",
    );
  });
}

describe("public daemon OAuth boundary contracts", () => {
  it(
    "accepts exactly 1 MiB and rejects one additional byte",
    acceptsExactBodyBoundary,
  );
  it(
    "restricts metadata methods and unrelated routes before operation dispatch",
    restrictsMetadataRoutes,
  );
  it(
    "releases a canceled OAuth Events stream for a replacement consumer",
    cancelsAndReacquiresStream,
  );
});

describe("public daemon OAuth", () => {
  it(
    "fails startup on an issuer error even with otherwise valid metadata",
    refusesFailedDiscovery,
  );
  it(
    "lets admitted unary work return its complete result after token expiry",
    completesAdmittedUnary,
  );
  it(
    "bounds chunked UTF-8 public bodies and preserves SDK validation",
    boundsPublicBodies,
  );
  it(
    "contains malformed request targets and keeps serving",
    containsMalformedTarget,
  );
  it(
    "rejects missing or conflicting method headers before streaming",
    checksMethodEnvelope,
  );

  it(
    "discovers issuers with a path and no trailing slash",
    discoversPathIssuer,
  );
  it(
    "admits only the configured public Host and browser origins",
    checksPublicTransport,
  );
  it(
    "shares poll, stream ownership, receipts and retained event lookup",
    sharesInboxCustody,
  );
  it(
    "discovers its fixed resource and accepts the owner's access token",
    discoversAndAuthenticates,
  );
  it(
    "rejects wrong owner, issuer, resource, scope and token lifetime",
    rejectsWrongAuthority,
  );
  it(
    "closes expired Events authority and admits a refreshed stream",
    expiresStream,
  );
});

/** Node HTTP preserves the public Host header while dialing the fixture's loopback address. */
function rawFetch(
  url: URL,
  options: RequestInit & { readonly path?: string },
): Promise<Response> {
  return new Promise((resolve, reject) => {
    const headers = Object.fromEntries(new Headers(options.headers));
    const request = httpRequest(
      url,
      {
        method: options.method ?? "GET",
        headers,
        ...(options.path === undefined ? {} : { path: options.path }),
      },
      (incoming) => {
        resolve(incomingResponse(incoming));
      },
    );
    request.on("error", reject);
    request.end(options.body);
  });
}

function incomingResponse(incoming: IncomingMessage): Response {
  const headers = new Headers();
  for (const [name, value] of Object.entries(incoming.headers)) {
    if (typeof value === "string") {
      headers.set(name, value);
    } else if (Array.isArray(value)) {
      for (const entry of value) {
        headers.append(name, entry);
      }
    }
  }
  const body = new ReadableStream<Uint8Array>({
    start(controller) {
      incoming.on("data", (chunk: Uint8Array) => {
        controller.enqueue(chunk);
      });
      incoming.on("end", () => {
        controller.close();
      });
      incoming.on("error", (error) => {
        controller.error(error);
      });
    },
    cancel() {
      incoming.destroy();
    },
  });
  return new Response(body, { status: incoming.statusCode, headers });
}

function fixtureHandler(
  oauth: DaemonOAuth,
  input: {
    readonly store: Effect.Effect.Success<ReturnType<typeof openInboxFixture>>;
    readonly delay: number;
    readonly onStatus?: () => void;
  },
) {
  const base = {
    ...operations,
    readStatus: () =>
      Effect.sleep(input.delay).pipe(
        Effect.tap(() =>
          Effect.sync(() => {
            input.onStatus?.();
          }),
        ),
        Effect.andThen(operations.readStatus()),
      ),
  };
  return makeHarnessMcpHttpHandler({
    oauth,
    implementation: { name: "oauth-test", version: "1" },
    operations: {
      ...base,
      ...input.store.operations,
      readInboxSummary: input.store.readInboxSummary,
    },
    eventStore: input.store,
  }).pipe(Effect.provideService(Identity, mcpIdentity()));
}

function issuerServer(jwk: JWK, issuerPath: string, status: number): Server {
  const server = createServer((request, response) => {
    const issuer = `http://127.0.0.1:${port(server)}${issuerPath}`;
    const metadata =
      request.url ===
      `${issuerPath.replace(/\/$/u, "")}/.well-known/openid-configuration`
        ? { issuer, jwks_uri: `${issuer.replace(/\/$/u, "")}/jwks` }
        : { keys: [{ ...jwk, kid: "issuer-key", alg: "RS256", use: "sig" }] };
    response.statusCode = status;
    response.setHeader("content-type", "application/json");
    response.end(JSON.stringify(metadata));
  });
  return server;
}

function signer(issuer: string, privateKey: CryptoKey) {
  return (claims: JWTPayload) =>
    new SignJWT({
      scope,
      iss: issuer,
      sub: owner,
      aud: resource.href,
      exp: Math.floor(Date.now() / 1000) + 120,
      ...claims,
    })
      .setProtectedHeader({ alg: "RS256", kid: "issuer-key" })
      .sign(privateKey);
}

/* eslint-enable agent-code-guard/promise-type, agent-code-guard/no-hardcoded-assertion-literals -- Restore defaults. */
