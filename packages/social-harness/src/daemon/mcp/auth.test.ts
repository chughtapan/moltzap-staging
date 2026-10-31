/** @file Discovery and operations share one authenticated daemon connection. */

import { scopedLive as it } from "@effect/vitest";
import {
  CLIENT_CAPABILITIES_META_KEY,
  CLIENT_INFO_META_KEY,
  PROTOCOL_VERSION_META_KEY,
} from "@modelcontextprotocol/server";
import { Effect, Schema } from "effect";
import { describe, expect } from "vitest";
import { openInboxFixture } from "../../__tests__/inbox-fixture.js";
import {
  connectInProcessClient,
  mcpIdentity,
  mcpTestCredential,
  mcpTestHeaders,
} from "../../__tests__/mcp-http-fixtures.js";
import { stateDirectory } from "../../__tests__/store-schema-fixtures.js";
import { Identity } from "../../identity/local.js";
import {
  type HarnessMcpOperations,
  makeHarnessMcpHttpHandler,
} from "./index.js";

const implementation = { name: "authenticated-daemon-test", version: "1" };
const unrelated = () => Effect.dieMessage("Unrelated daemon operation");
const revision = "a".repeat(64);
const policy = { rules: [], fixedRules: [], activeRevision: revision };
const operations: HarnessMcpOperations = {
  readStatus: () => Effect.succeed({ kind: "unregistered" }),
  register: unrelated,
  searchConversations: () =>
    Effect.succeed({ kind: "page", addresses: [], hasMore: false }),
  readConversation: unrelated,
  readPolicy: () => Effect.succeed(policy),
  updatePolicy: () => Effect.succeed({ activeRevision: revision }),
  readDecisions: () => Effect.succeed({ activations: [], decisions: [] }),
  readEvent: () => Effect.fail({ reason: "unknown-event" }),
  send: unrelated,
  readInbox: () => Effect.succeed({ items: [] }),
  readInboxSummary: () =>
    Effect.succeed({ pendingCount: 0, newestSequence: 0 }),
  acknowledgeDelivery: unrelated,
};
const acquireHandler = (active = true) =>
  Effect.gen(function* () {
    const store = yield* openInboxFixture(stateDirectory());
    const handler = yield* makeHarnessMcpHttpHandler({
      eventStore: store,
      credential: mcpTestCredential,
      implementation,
      operations: operations,
    }).pipe(Effect.provideService(Identity, mcpIdentity(active)));
    yield* Effect.addFinalizer(() =>
      Effect.tryPromise(() => handler.close()).pipe(Effect.orDie),
    );
    return handler;
  });
const request = (
  method: string,
  params: Readonly<Record<string, unknown>>,
  headers: Readonly<Record<string, string>>,
) =>
  new Request("http://127.0.0.1/mcp", {
    method: "POST",
    headers: {
      ...headers,
      accept: "application/json, text/event-stream",
      "content-type": "application/json",
      "mcp-method": method,
      ...(typeof params.name === "string" ? { "mcp-name": params.name } : {}),
    },
    body: JSON.stringify({
      jsonrpc: "2.0",
      id: 1,
      method,
      params: {
        ...params,
        _meta: {
          [CLIENT_INFO_META_KEY]: implementation,
          [CLIENT_CAPABILITIES_META_KEY]: {},
          [PROTOCOL_VERSION_META_KEY]: "2026-07-28",
        },
      },
    }),
  });
const responseSchema = Schema.parseJson(
  Schema.Struct({ result: Schema.Unknown }),
);

const deniedHeaders: ReadonlyArray<Readonly<Record<string, string>>> = [
  {},
  { Authorization: "Bearer invalid-token" },
];

const requiresBearer = ({
  method,
  params,
}: {
  readonly method: string;
  readonly params: Readonly<Record<string, unknown>>;
}) =>
  Effect.gen(function* () {
    const handler = yield* acquireHandler();
    for (const headers of deniedHeaders) {
      const response = yield* Effect.tryPromise(() =>
        handler.fetch(request(method, params, headers)),
      );
      // eslint-disable-next-line agent-code-guard/no-hardcoded-assertion-literals -- HTTP authentication failure has the standard 401 status.
      expect(response.status).toBe(401);
    }
  });
const sharesAuthenticatedConnection = () =>
  Effect.gen(function* () {
    const handler = yield* acquireHandler();
    const client = yield* connectInProcessClient(handler, {
      capabilities: {},
      versionNegotiation: { mode: { pin: "2026-07-28" } },
    });
    const read = yield* Effect.tryPromise(() =>
      client.callTool({ name: "policy_read", arguments: {} }),
    );
    expect(read.structuredContent).toEqual(policy);
    const updated = yield* Effect.tryPromise(() =>
      client.callTool({ name: "policy_update", arguments: { remove: [] } }),
    );
    expect(updated.structuredContent).toEqual({ activeRevision: revision });
    const status = yield* Effect.tryPromise(() =>
      client.callTool({ name: "event_subscription_status", arguments: {} }),
    );
    expect(status.structuredContent).toEqual({ mode: "none" });
  });
const readsUnregisteredStatus = () =>
  Effect.gen(function* () {
    const handler = yield* acquireHandler(false);
    const response = yield* Effect.tryPromise(() =>
      handler.fetch(
        request(
          "tools/call",
          { name: "status", arguments: {} },
          mcpTestHeaders,
        ),
      ),
    );
    const decoded = yield* Effect.tryPromise(() => response.text()).pipe(
      Effect.flatMap(Schema.decodeUnknown(responseSchema)),
    );
    expect(decoded.result).toMatchObject({
      structuredContent: { kind: "unregistered" },
    });
  });
const supportsLegacyInitialization = () =>
  Effect.gen(function* () {
    const handler = yield* acquireHandler();
    const client = yield* connectInProcessClient(handler, {
      capabilities: {},
      versionNegotiation: { mode: "legacy" },
      supportedProtocolVersions: ["2025-11-25"],
    });
    const status = yield* Effect.tryPromise(() =>
      client.callTool({ name: "status", arguments: {} }),
    );
    expect(status.structuredContent).toEqual({ kind: "unregistered" });
  });

// @agent-code-guard/regression-only: discovery, tool calls and Events reject unauthenticated requests before dispatch.
describe("daemon authentication", () => {
  it.each([
    { method: "tools/list", params: {} },
    { method: "tools/call", params: { name: "status", arguments: {} } },
    { method: "events/list", params: {} },
  ])("requires a bearer for $method", requiresBearer);
  it(
    "serves policy and delivery diagnostics on one connection",
    sharesAuthenticatedConnection,
  );
  it(
    "keeps authenticated status available before registration",
    readsUnregisteredStatus,
  );
  it(
    "supports authenticated clients using initialization",
    supportsLegacyInitialization,
  );
});
