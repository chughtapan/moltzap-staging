/** @file Authenticated polling uses the production MCP, admitted inbox and receipt boundaries. */

import { HttpClient, HttpClientRequest } from "@effect/platform";
import { NodeHttpClient } from "@effect/platform-node";
import { scopedLive as it } from "@effect/vitest";
import {
  CLIENT_CAPABILITIES_META_KEY,
  CLIENT_INFO_META_KEY,
  PROTOCOL_VERSION_META_KEY,
} from "@modelcontextprotocol/server";
import { Deferred, Effect, Fiber, Redacted, Schema } from "effect";
import { describe, expect } from "vitest";
import { openInboxFixture } from "../../__tests__/inbox-fixture.js";
import {
  loopbackMcpEndpoint,
  mcpIdentity,
} from "../../__tests__/mcp-http-fixtures.js";
import { localGuidance } from "../../__tests__/message-policy-fixtures.js";
import { stateDirectory } from "../../__tests__/store-schema-fixtures.js";
import { enqueue, item, token } from "../../__tests__/webhook-fixtures.js";
import { encodeRuntimeValue } from "../../communication/store/index.js";
import {
  decodePollResult,
  type PollResult,
} from "../../host-protocol/index.js";
import {
  HARNESS_CONSUMER_META_KEY,
  INBOX_ITEM_EVENT,
  INBOX_PENDING_EVENT,
} from "../../host-protocol/names.js";
import { Identity } from "../../identity/local.js";
import { makeHarnessMcpHttpHandler } from "./index.js";

/* eslint-disable agent-code-guard/no-hardcoded-assertion-literals -- HTTP/JSON-RPC refusal codes and receipt counts are external behavior. */

const info = { name: "native-polling-conformance", version: "1" };
const credential = Redacted.make("poll-credential-for-local-test-0001");
const pollInput = { name: INBOX_ITEM_EVENT, arguments: {}, cursor: null };
const unreachable = () =>
  Effect.dieMessage("poll scenario reached an unrelated operation");
interface BlockedOperation {
  readonly kind: "poll" | "read" | "ack";
  readonly wait: Effect.Effect<void>;
}
const beforeReceive = <A, E>(
  kind: BlockedOperation["kind"],
  operation: Effect.Effect<A, E>,
  block?: BlockedOperation,
) =>
  block?.kind === kind
    ? block.wait.pipe(Effect.zipRight(operation))
    : operation;
const acquireFixture = (block?: BlockedOperation) =>
  Effect.gen(function* () {
    const store = yield* openInboxFixture(stateDirectory());
    const handler = yield* makeHarnessMcpHttpHandler({
      implementation: info,
      credential,
      eventStore: {
        ...store,
        readInbox: (bounds) =>
          beforeReceive("poll", store.readInbox(bounds), block),
      },
      operations: {
        ...store.operations,
        readInbox: (request) =>
          beforeReceive("read", store.operations.readInbox(request), block),
        acknowledgeDelivery: (deliveryToken) =>
          beforeReceive(
            "ack",
            store.operations.acknowledgeDelivery(deliveryToken),
            block,
          ),

        send: unreachable,
        readInboxSummary: store.readInboxSummary,
        readStatus: unreachable,
        register: unreachable,
        searchConversations: unreachable,
        readConversation: unreachable,
      },
    }).pipe(Effect.provideService(Identity, mcpIdentity(true)));
    return { store, handler, endpoint: yield* loopbackMcpEndpoint(handler) };
  });

interface RequestOptions {
  readonly metadata?: Readonly<Record<string, unknown>>;
  readonly credential?: Redacted.Redacted;
}
const request = (
  endpoint: URL,
  method: string,
  params: Readonly<Record<string, unknown>>,
  options: RequestOptions = {},
) =>
  HttpClient.execute(
    HttpClientRequest.post(endpoint).pipe(
      HttpClientRequest.setHeaders({
        "content-type": "application/json",
        accept: "application/json, text/event-stream",
        "mcp-method": method,
        ...(typeof params.name === "string" ? { "mcp-name": params.name } : {}),
        authorization: `Bearer ${Redacted.value(options.credential ?? credential)}`,
      }),
      HttpClientRequest.bodyUnsafeJson({
        jsonrpc: "2.0",
        id: 1,
        method,
        params: {
          ...params,
          _meta: {
            ...options.metadata,
            [PROTOCOL_VERSION_META_KEY]: "2026-07-28",
            [CLIENT_INFO_META_KEY]: info,
            [CLIENT_CAPABILITIES_META_KEY]: {},
          },
        },
      }),
    ),
  ).pipe(Effect.provide(NodeHttpClient.layer));

const rpc = (...args: Parameters<typeof request>) =>
  request(...args).pipe(Effect.flatMap((response) => response.json));
const poll = (endpoint: URL, previous?: PollResult) =>
  rpc(
    endpoint,
    "events/poll",
    {
      ...pollInput,
      cursor: previous?.cursor ?? null,
    },
    { metadata: previous?._meta },
  ).pipe(
    Effect.tap((response) =>
      Effect.sync(() => {
        expect(response).not.toHaveProperty("error");
      }),
    ),
    Effect.flatMap(
      Schema.decodeUnknown(Schema.Struct({ result: Schema.Unknown })),
    ),
    Effect.flatMap((response) => decodePollResult(response.result)),
  );
const call = (
  endpoint: URL,
  name: string,
  args: Readonly<Record<string, unknown>>,
  options: RequestOptions = {},
) => rpc(endpoint, "tools/call", { name, arguments: args }, options);

const deliversAdmittedItems = () =>
  Effect.gen(function* () {
    const { store, endpoint } = yield* acquireFixture();
    yield* enqueue(store, 1);
    const started = yield* poll(endpoint);
    expect(started.events).toEqual([]);
    expect(
      yield* call(endpoint, "read_inbox", {}, { metadata: started._meta }),
    ).toMatchObject({
      result: { structuredContent: { items: [{ deliveryToken: token(1) }] } },
    });
    const guidance = localGuidance(2);
    yield* store.putInboxItem({
      deliveryToken: token(2),
      canonicalItem: yield* encodeRuntimeValue(item("admitted")),
      canonicalGuidance: yield* encodeRuntimeValue(guidance),
    });
    yield* store.queueIncomingItem({
      deliveryToken: token(3),
      canonicalItem: yield* encodeRuntimeValue(item("private pending check")),
      conversation: "agent:bob",
    });
    const delivered = yield* poll(endpoint, started);
    expect(delivered.events).toMatchObject([
      {
        eventId: token(2),
        data: { kind: "item", item: item("admitted"), guidance },
      },
    ]);
    expect((yield* store.readInboxSummary()).pendingCount).toBe(2);
    expect(
      yield* call(endpoint, "read_event", { eventId: token(3) }),
    ).toMatchObject({
      error: { data: { reason: "unknown-event" } },
    });
    expect(
      yield* call(
        endpoint,
        "acknowledge_delivery",
        { deliveryToken: token(2) },
        { metadata: delivered._meta },
      ),
    ).toMatchObject({
      result: { structuredContent: {} },
    });
    expect((yield* store.readInboxSummary()).pendingCount).toBe(1);
  });

const refusesCompetingConsumers = () =>
  Effect.gen(function* () {
    const { endpoint } = yield* acquireFixture();
    yield* poll(endpoint);
    expect(yield* rpc(endpoint, "events/poll", pollInput)).toMatchObject({
      error: { data: { reason: "already-listening" } },
    });
    expect(
      yield* rpc(
        endpoint,
        "events/stream",
        {
          name: INBOX_PENDING_EVENT,
          arguments: {},
        },
        { credential: credential },
      ),
    ).toMatchObject({
      error: { data: { reason: "already-listening" } },
    });
    expect(
      yield* call(
        endpoint,
        "event_subscription_status",
        {},
        { credential: credential },
      ),
    ).toMatchObject({
      result: { structuredContent: { mode: "poll" } },
    });
  });

const fencesReplacedConsumers = () =>
  Effect.gen(function* () {
    const { store, endpoint } = yield* acquireFixture();
    const old = yield* poll(endpoint);
    yield* enqueue(store, 1);
    yield* call(
      endpoint,
      "revoke_event_subscription",
      {},
      { credential: credential },
    );
    const replacement = yield* poll(endpoint);
    expect(replacement._meta).not.toEqual(old._meta);
    expect(
      yield* call(
        endpoint,
        "acknowledge_delivery",
        { deliveryToken: token(1) },
        { metadata: old._meta },
      ),
    ).toMatchObject({
      error: { code: -32012, data: { reason: "consumer-revoked" } },
    });
    expect((yield* store.readInboxSummary()).pendingCount).toBe(1);
    expect(
      yield* call(endpoint, "read_inbox", {}, { metadata: old._meta }),
    ).toMatchObject({
      error: { data: { reason: "consumer-revoked" } },
    });
    yield* call(
      endpoint,
      "acknowledge_delivery",
      { deliveryToken: token(1) },
      { metadata: replacement._meta },
    );
    expect((yield* store.readInboxSummary()).pendingCount).toBe(0);
  });

const requiresAuthenticatedBinding = () =>
  Effect.gen(function* () {
    const { endpoint } = yield* acquireFixture();
    const denied = yield* request(endpoint, "events/poll", pollInput, {
      credential: Redacted.make("wrong"),
    });
    expect(denied.status).toBe(401);
    const started = yield* poll(endpoint);
    expect(yield* call(endpoint, "read_inbox", {})).toMatchObject({
      result: { structuredContent: { items: [] } },
    });
    expect(
      yield* call(
        endpoint,
        "read_inbox",
        {},
        {
          metadata: {
            [HARNESS_CONSUMER_META_KEY]: { generation: "malformed" },
          },
        },
      ),
    ).toMatchObject({ error: { code: -32602 } });
    expect(
      yield* rpc(
        endpoint,
        "events/poll",
        {
          ...pollInput,
          maxEvents: 0,
        },
        { metadata: started._meta },
      ),
    ).toMatchObject({ error: { code: -32602 } });
  });

const receiveRequest = (
  kind: BlockedOperation["kind"],
  endpoint: URL,
  started: PollResult,
) => {
  if (kind === "poll") {
    return poll(endpoint, started);
  }
  return call(
    endpoint,
    kind === "read" ? "read_inbox" : "acknowledge_delivery",
    kind === "read" ? {} : { deliveryToken: token(1) },
    { metadata: started._meta },
  );
};

const closesBlockedReceive = (kind: BlockedOperation["kind"]) =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<undefined>();
    const interrupted = yield* Deferred.make<undefined>();
    const wait = Deferred.succeed(entered, undefined).pipe(
      Effect.zipRight(Effect.never),
      Effect.onInterrupt(() => Deferred.succeed(interrupted, undefined)),
    );
    const { store, handler, endpoint } = yield* acquireFixture({ kind, wait });
    const started = yield* poll(endpoint);
    yield* enqueue(store, 1);
    const running = yield* receiveRequest(kind, endpoint, started).pipe(
      Effect.forkScoped,
    );
    yield* Deferred.await(entered);
    yield* Effect.tryPromise(() => handler.close()).pipe(
      Effect.timeout("2 seconds"),
    );
    yield* Deferred.await(interrupted);
    yield* Fiber.interrupt(running);
    expect((yield* store.readInboxSummary()).pendingCount).toBe(1);
  });

// @agent-code-guard/regression-only: finite protocol transcripts cover MCP interoperability, authentication and durable handoff.
describe("authenticated polling over loopback HTTP", () => {
  it.each(["poll", "read", "ack"] as const)(
    "closes a handler with a blocked %s without committing a receipt",
    closesBlockedReceive,
  );
  it("refuses a competing poll or push consumer", refusesCompetingConsumers);
  it(
    "recovers unread content and polls only admitted items with their guidance",
    deliversAdmittedItems,
  );
  it(
    "refuses competitors and fences reads and acknowledgments after replacement",
    fencesReplacedConsumers,
  );
  it(
    "authenticates requests and validates supplied consumer bindings",
    requiresAuthenticatedBinding,
  );
});

/* eslint-enable agent-code-guard/no-hardcoded-assertion-literals -- Restore repository defaults. */
