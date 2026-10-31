/** @file Pins Events discovery and failure isolation through loopback HTTP. */

import { FileSystem } from "@effect/platform";
import { NodeFileSystem } from "@effect/platform-node";
import { scopedLive as it } from "@effect/vitest";
import {
  Client,
  fromJsonSchema,
  type JsonSchemaType,
  ProtocolError,
  ProtocolErrorCode,
  StreamableHTTPClientTransport,
} from "@modelcontextprotocol/client";
import {
  CLIENT_CAPABILITIES_META_KEY,
  CLIENT_INFO_META_KEY,
  type Implementation,
  PROTOCOL_VERSION_META_KEY,
} from "@modelcontextprotocol/server";
import {
  Deferred,
  Duration,
  Effect,
  Exit,
  Fiber,
  Schema,
  Scope,
  Stream,
  TestClock,
  TestContext,
} from "effect";
import {
  Agent,
  type Dispatcher,
  getGlobalDispatcher,
  MockAgent,
  setGlobalDispatcher,
} from "undici";
import { describe, expect } from "vitest";
import { digest, testAgentView } from "../../__tests__/agent-card-fixtures.js";
import { collectiveOperationId } from "../../__tests__/collective-id-fixtures.js";
import { requestId } from "../../__tests__/collective-operation-fixtures.js";
import { openInboxFixture } from "../../__tests__/inbox-fixture.js";
import {
  mcpIdentity,
  mcpTestCredential,
  mcpTestHeaders,
} from "../../__tests__/mcp-http-fixtures.js";
import { localGuidance } from "../../__tests__/message-policy-fixtures.js";
import { stateDirectory } from "../../__tests__/store-schema-fixtures.js";
import {
  CollectiveError,
  CollectiveId,
  SendInput,
} from "../../communication/collectives/contracts.js";
import {
  ListenError,
  SendError,
} from "../../communication/conversations/errors.js";
import {
  DeliveryToken,
  encodeRuntimeValue,
} from "../../communication/store/index.js";
import { acquireHarnessEndpoint } from "../../host-client/index.js";
import { FirewallSendError } from "../../host-protocol/advice.js";
import { InboundItem } from "../../host-protocol/items.js";
import {
  HARNESS_SEND_TOOL,
  INBOX_PENDING_EVENT,
} from "../../host-protocol/names.js";
import {
  type SendResult,
  SendResultSchema,
} from "../../host-protocol/send-result.js";
import { Identity } from "../../identity/local.js";
import { readRuntimeEvent } from "../inbox/inbox.js";
import { acquireHarnessMcpHttpServer } from "./http.js";
import {
  type HarnessMcpOperations,
  makeHarnessMcpHttpHandler,
} from "./index.js";

const MODERN_PROTOCOL_VERSION = "2026-07-28";
const IMPLEMENTATION = {
  name: "harness-boundary-test",
  version: "1.0.0",
} satisfies Implementation;
const PRIVATE_STATUS_DEFECT = "private status defect";

const unusedOperation = Effect.dieMessage("operation is outside this test");
const operations: HarnessMcpOperations = {
  readEvent: () => Effect.fail({ reason: "unknown-event" }),
  readInboxSummary: () =>
    Effect.succeed({ pendingCount: 0, newestSequence: 0 }),
  readInbox: () => Effect.succeed({ items: [] }),
  readStatus: () => Effect.succeed({ kind: "unregistered" }),
  register: () => unusedOperation,
  searchConversations: () => unusedOperation,
  readConversation: () => unusedOperation,
  send: () => unusedOperation,
  acknowledgeDelivery: () => unusedOperation,
};

function acquireBoundaryServer(
  selectedOperations: HarnessMcpOperations,
  registered = true,
) {
  return Effect.gen(function* () {
    const store = yield* openInboxFixture(stateDirectory());
    const handler = yield* makeHarnessMcpHttpHandler({
      eventStore: store,
      credential: mcpTestCredential,
      implementation: IMPLEMENTATION,
      operations: selectedOperations,
    }).pipe(Effect.provideService(Identity, mcpIdentity(registered)));
    const server = yield* acquireHarnessMcpHttpServer({ port: 0, handler });
    const address = server.address();
    const port =
      typeof address === "object" && address !== null ? address.port : 0;
    return { port, server };
  });
}

function acquireProtocolClient(port: number, name: string) {
  // eslint-disable-next-line agent-code-guard/acquire-release-requires-scope -- scopedLive's `it` supplies the test scope that releases it.
  return Effect.acquireRelease(
    Effect.gen(function* () {
      const client = new Client(
        { name, version: "1.0.0" },
        {
          capabilities: {},
          versionNegotiation: { mode: { pin: MODERN_PROTOCOL_VERSION } },
        },
      );
      yield* Effect.tryPromise({
        try: (signal) =>
          client.connect(
            new StreamableHTTPClientTransport(
              new URL(`http://127.0.0.1:${port}/mcp`),
              { requestInit: { headers: mcpTestHeaders } },
            ),
            { signal },
          ),
        catch: (cause) => cause,
      });
      return client;
    }),
    (client) => Effect.tryPromise(() => client.close()).pipe(Effect.ignore),
  );
}

function capturesProtocolError(
  client: Client,
  toolArguments: Readonly<Record<string, unknown>>,
) {
  return Effect.tryPromise({
    try: (signal) =>
      client.callTool({ name: "status", arguments: toolArguments }, { signal }),
    catch: (cause) => cause,
  }).pipe(
    Effect.match({
      onFailure: (cause) => cause,
      onSuccess: () => new Error("status unexpectedly succeeded"),
    }),
  );
}

function advertisesEventsBeforeRegistration() {
  return Effect.gen(function* () {
    const { port } = yield* acquireBoundaryServer(operations, false);
    const client = yield* acquireProtocolClient(
      port,
      "harness-capability-client",
    );
    const capabilities = yield* Effect.tryPromise(() =>
      client.request(
        { method: "events/list", params: {} },
        fromJsonSchema<{ events: unknown[] }>({
          type: "object",
          properties: { events: { type: "array" } },
          required: ["events"],
        }),
      ),
    );

    const pushDelivery: unknown = expect.arrayContaining(["push"]);
    expect(capabilities.events).toContainEqual(
      expect.objectContaining({
        name: INBOX_PENDING_EVENT,
        delivery: pushDelivery,
      }),
    );
  });
}

function distinguishesProtocolAndDomainFailures() {
  return Effect.gen(function* () {
    const failingOperations: HarnessMcpOperations = {
      ...operations,
      readStatus: () => Effect.fail({ reason: "incompatible-daemon" }),
    };
    const { port } = yield* acquireBoundaryServer(failingOperations, false);
    const client = yield* acquireProtocolClient(
      port,
      "harness-boundary-client",
    );
    const privateInput = "private-status-input";
    const [malformedCause, domainCause] = yield* Effect.all(
      [
        capturesProtocolError(client, { unexpected: privateInput }),
        capturesProtocolError(client, {}),
      ],
      { concurrency: 1 },
    );

    expect(ProtocolError.isInstance(malformedCause)).toBe(true);
    expect(ProtocolError.isInstance(domainCause)).toBe(true);
    if (
      !ProtocolError.isInstance(malformedCause) ||
      !ProtocolError.isInstance(domainCause)
    ) {
      throw new Error("expected protocol errors from both calls");
    }
    expect(malformedCause).toMatchObject({
      code: ProtocolErrorCode.InvalidParams,
      data: {
        issues: [{ path: ["unexpected"], message: "Unexpected field" }],
      },
    });
    expect(JSON.stringify(malformedCause.data)).not.toContain(privateInput);
    expect(domainCause).toMatchObject({
      code: ProtocolErrorCode.InternalError,
      data: { reason: "incompatible-daemon" },
    });
  });
}

/**
 * A status failure outside the status vocabulary, such as
 * `persistence-failed`, which other owner tools admit, reaches the owner as
 * `incompatible-daemon`. A failure the vocabulary admits cannot show this,
 * because the fallback reason equals the one admitted reason.
 */
function reportsOutsideVocabularyStatusFailure() {
  return Effect.gen(function* () {
    const failingOperations: HarnessMcpOperations = {
      ...operations,
      readStatus: () => Effect.fail({ reason: "persistence-failed" }),
    };
    const { port } = yield* acquireBoundaryServer(failingOperations, false);
    const client = yield* acquireProtocolClient(
      port,
      "harness-status-vocabulary-client",
    );
    const cause = yield* capturesProtocolError(client, {});

    expect(cause).toMatchObject({
      code: ProtocolErrorCode.InternalError,
      data: { reason: "incompatible-daemon" },
    });
  });
}

function sanitizesUnexpectedOperationDefects() {
  return Effect.gen(function* () {
    const defectiveOperations: HarnessMcpOperations = {
      ...operations,
      readStatus: () => Effect.dieMessage(PRIVATE_STATUS_DEFECT),
    };
    const { port } = yield* acquireBoundaryServer(defectiveOperations, false);
    const client = yield* acquireProtocolClient(port, "harness-defect-client");
    const cause = yield* capturesProtocolError(client, {});

    expect(ProtocolError.isInstance(cause)).toBe(true);
    if (!ProtocolError.isInstance(cause)) {
      throw new Error("expected a sanitized defect response");
    }
    expect(cause).toMatchObject({
      code: ProtocolErrorCode.InternalError,
      data: { reason: "incompatible-daemon" },
    });
    expect(String(cause)).not.toContain(PRIVATE_STATUS_DEFECT);
  });
}

function reportsUnexpectedSubscriptionLoss() {
  return Effect.gen(function* () {
    const { server, receive } = yield* observeListeningSubscription();
    yield* Effect.sync(() => {
      server.closeAllConnections();
    });
    const result = yield* Fiber.join(receive).pipe(
      Effect.timeoutFail({
        duration: Duration.seconds(5),
        onTimeout: () => new Error("message stream did not observe disconnect"),
      }),
    );

    expect(result).toBeInstanceOf(ListenError);
    expect(result).toMatchObject({ reason: "transport-failed" });
  });
}

function emitsIdleHeartbeat() {
  return Effect.gen(function* () {
    const store = yield* openInboxFixture(stateDirectory());
    const handler = yield* makeHarnessMcpHttpHandler({
      eventStore: store,
      credential: mcpTestCredential,
      implementation: IMPLEMENTATION,
      operations,
    }).pipe(Effect.provideService(Identity, mcpIdentity(true)));
    yield* Effect.addFinalizer(() =>
      Effect.tryPromise({
        try: () => handler.close(),
        catch: (cause) => new Error("MCP handler closure failed", { cause }),
      }).pipe(Effect.orDie),
    );
    const active = yield* Deferred.make<undefined>();
    const heartbeat = yield* Deferred.make<string>();
    const response = yield* Effect.tryPromise({
      try: () =>
        handler.fetch(
          new Request("http://127.0.0.1/mcp", {
            method: "POST",
            headers: {
              ...mcpTestHeaders,
              "content-type": "application/json",
              accept: "text/event-stream",
              "mcp-method": "events/stream",
              "mcp-name": INBOX_PENDING_EVENT,
            },
            body: JSON.stringify({
              jsonrpc: "2.0",
              id: "heartbeat",
              method: "events/stream",
              params: {
                name: INBOX_PENDING_EVENT,
                arguments: {},
                cursor: null,
                _meta: {
                  [PROTOCOL_VERSION_META_KEY]: MODERN_PROTOCOL_VERSION,
                  [CLIENT_INFO_META_KEY]: IMPLEMENTATION,
                  [CLIENT_CAPABILITIES_META_KEY]: {},
                },
              },
            }),
          }),
        ),
      catch: (cause) => new Error("MCP event request failed", { cause }),
    });
    const body = yield* Effect.fromNullable(response.body);
    yield* Stream.fromReadableStream(
      () => body,
      (cause) => new Error("Event stream failed", { cause }),
    ).pipe(
      Stream.decodeText(),
      Stream.splitLines,
      Stream.tap((line) => {
        if (line.includes("notifications/events/active")) {
          return Deferred.succeed(active, undefined);
        }
        if (line.includes("notifications/events/heartbeat")) {
          return Deferred.succeed(heartbeat, line);
        }
        return Effect.void;
      }),
      Stream.runDrain,
      Effect.forkScoped,
    );
    yield* Deferred.await(active);
    yield* TestClock.adjust("1 minute");
    const frame = yield* Schema.decodeUnknown(Schema.parseJson())(
      (yield* Deferred.await(heartbeat)).replace(/^data:\s*/u, ""),
    );
    expect(frame).toMatchObject({
      method: "notifications/events/heartbeat",
      params: { cursor: null },
    });
  }).pipe(Effect.provide(TestContext.TestContext));
}

/** A boundary server with one endpoint listening on it, and the fiber observing that stream's end. */
function observeListeningSubscription() {
  return Effect.gen(function* () {
    const subscriptionActive = yield* Deferred.make<undefined>();
    const { port, server } = yield* acquireBoundaryServer(operations, true);
    const endpoint = yield* acquireHarnessEndpoint(
      new URL(`http://127.0.0.1:${port}/mcp`),
      {
        credential: mcpTestCredential,
        receiveMode: "push",
        onReady: Deferred.succeed(subscriptionActive, undefined),
      },
    );
    const receive = yield* endpoint.messages.pipe(
      Stream.runHead,
      Effect.match({
        onFailure: (cause) => cause,
        onSuccess: () =>
          new Error("message stream ended without a transport failure"),
      }),
      Effect.forkScoped,
    );
    yield* Deferred.await(subscriptionActive);
    return { server, receive };
  });
}

const sendInput = Schema.decodeUnknownSync(SendInput)({
  to: "agent:bob",
  text: "one semantic action",
});
const sendResult = Schema.decodeUnknownSync(SendResultSchema)({
  kind: "multicast",
  msgId: digest("msg_", 8),
  address: "agent:bob",
});

const SENDS_WITH_GUIDANCE: readonly SendResult[] = [
  { ...sendResult, guidance: localGuidance(14) },
  Schema.decodeUnknownSync(SendResultSchema)({
    kind: "response",
    msgId: digest("msg_", 15),
    address: "agent:bob",
    operationId: collectiveOperationId(
      testAgentView("agent:alice").agentId,
      16,
    ),
    guidance: localGuidance(15),
  }),
  {
    kind: "collective",
    operationId: collectiveOperationId(
      testAgentView("agent:alice").agentId,
      16,
    ),
    guidance: localGuidance(16),
  },
];

function preservesAdmittedSendGuidance(result: SendResult) {
  return Effect.gen(function* () {
    const endpoint = yield* acquireSendEndpoint({
      send: () => Effect.succeed(result),
    });

    expect(yield* endpoint.send(sendInput)).toEqual(result);
  });
}

/**
 * Bounds a hang in a boundary trace that starts servers and endpoints and
 * makes MCP requests one after another; no assertion depends on it. The
 * send-validation trace starts three servers and three endpoints and, as the
 * file's first, pays the MCP and endpoint stack's warm-up: it took 1.4 to
 * 5.2 s at a load average of 30 to 42 on 8 cores. The bookkeeping trace's
 * sequential requests took 2.6 to 4.6 s there. Both have timed out at the
 * 5 s default.
 */
const MCP_TRACE_TIMEOUT_MS = 30_000;

const distinguishesSendValidationFailures = () =>
  Effect.gen(function* () {
    const inactive = yield* acquireSendEndpoint(operations, false);
    expect(yield* inactive.send(sendInput).pipe(Effect.flip)).toMatchObject({
      reason: "outcome-unknown",
    });
    const refused = yield* acquireSendEndpoint({
      ...operations,
      send: () => Effect.fail(new SendError({ reason: "not-registered" })),
    });
    expect(yield* refused.send(sendInput).pipe(Effect.flip)).toMatchObject({
      reason: "not-registered",
    });
    const invalidOutput = yield* acquireSendEndpoint({
      ...operations,
      send: () =>
        Effect.succeed({
          kind: "collective",
          operationId: Schema.decodeUnknownSync(CollectiveId)(
            `col_${"A".repeat(64)}`,
          ),
          unexpected: true,
        }),
    });
    expect(
      yield* invalidOutput.send(sendInput).pipe(Effect.flip),
    ).toMatchObject({
      reason: "outcome-unknown",
    });
  });

const carriesARefusalDetailAcrossTheDaemonBoundary = (
  refusal: SendError | CollectiveError,
) =>
  Effect.gen(function* () {
    const endpoint = yield* acquireSendEndpoint({
      ...operations,
      send: () => Effect.fail(refusal),
    });

    const error = yield* endpoint.send(sendInput).pipe(Effect.flip);

    expect(error.message).toBe(refusal.message);
    expect(error).toStrictEqual(refusal);
  });

/** Refusals name a send's failure, the openOperationIds of an answer that named no request, or the questions open in other conversations. */
const DETAILED_REFUSALS = [
  new SendError({
    reason: "content-invalid",
    detail: "the answer does not match the requested form",
  }),
  new SendError({
    reason: "unknown-agent",
    detail: "agent:dana is not a known agent",
  }),
  new CollectiveError({
    operationId: Schema.decodeUnknownSync(CollectiveId)(
      `col_${"A".repeat(64)}`,
    ),
    failure: { kind: "request-expired" },
  }),
];

const answersOutcomeUnknownForAnUntypedSendFailure = () =>
  Effect.gen(function* () {
    const endpoint = yield* acquireSendEndpoint({
      ...operations,
      send: () => Effect.dieMessage("send defect"),
    });

    const error = yield* endpoint.send(sendInput).pipe(Effect.flip);

    expect(error).toStrictEqual(new SendError({ reason: "outcome-unknown" }));
  });

const reportsOutcomeUnknownWhenTheConnectionDropsMidSend = () =>
  Effect.gen(function* () {
    const started = yield* Deferred.make<undefined>();
    const { port, server } = yield* acquireBoundaryServer({
      ...operations,
      send: () =>
        Deferred.succeed(started, undefined).pipe(
          Effect.zipRight(Effect.never),
        ),
    });
    const endpoint = yield* acquireHarnessEndpoint(
      new URL(`http://127.0.0.1:${port}/mcp`),
      { credential: mcpTestCredential, receiveMode: "push" },
    );
    const sending = yield* Effect.fork(
      endpoint.send(sendInput).pipe(Effect.flip),
    );
    yield* Deferred.await(started);

    server.closeAllConnections();

    expect(yield* Fiber.join(sending)).toStrictEqual(
      new SendError({ reason: "outcome-unknown" }),
    );
  });

/**
 * Route this scope's requests through `dispatcher`, then restore the previous
 * global dispatcher and close this one.
 * @param dispatcher The undici dispatcher the MCP client's fetch uses.
 * @returns The scoped routing.
 */
function routeRequestsThrough(dispatcher: Dispatcher) {
  // eslint-disable-next-line agent-code-guard/acquire-release-requires-scope -- scopedLive's `it` supplies the test scope that releases it.
  return Effect.acquireRelease(
    Effect.sync(() => {
      const previous = getGlobalDispatcher();
      setGlobalDispatcher(dispatcher);
      return previous;
    }),
    (previous) =>
      Effect.sync(() => {
        setGlobalDispatcher(previous);
      }).pipe(
        Effect.zipRight(
          Effect.tryPromise({
            try: () => dispatcher.close(),
            catch: (cause) => cause,
          }),
        ),
        Effect.orDie,
      ),
  );
}

/**
 * A dispatcher that answers the next `send_message` POST with `status` before
 * it reaches the daemon and passes every other request through.
 * @param status The HTTP status the daemon's HTTP layer seems to answer.
 * @returns The dispatcher.
 */
function answerSendWithStatus(status: number): MockAgent {
  const agent = new MockAgent();
  agent.enableNetConnect();
  agent
    .get(/^http:\/\/127\.0\.0\.1:\d+$/)
    .intercept({
      path: "/mcp",
      method: "POST",
      body: (body) => body.includes(`"${HARNESS_SEND_TOOL}"`),
    })
    .reply(status, "refused");
  return agent;
}

const rpcCallSchema = Schema.parseJson(
  Schema.Struct({ id: Schema.Union(Schema.String, Schema.Number) }),
);

function answerSendWithMessage(
  message: Readonly<Record<string, unknown>>,
): MockAgent {
  const agent = new MockAgent();
  agent.enableNetConnect();
  agent
    .get(/^http:\/\/127\.0\.0\.1:\d+$/)
    .intercept({
      path: "/mcp",
      method: "POST",
      body: (body) => body.includes(`"${HARNESS_SEND_TOOL}"`),
    })
    .reply(
      200,
      ({ body }) => ({
        jsonrpc: "2.0",
        id: Schema.decodeUnknownSync(rpcCallSchema)(body).id,
        ...message,
      }),
      { headers: { "content-type": "application/json" } },
    );
  return agent;
}

function reportsAmbiguousSendAnswer(
  message: Readonly<Record<string, unknown>>,
) {
  return Effect.gen(function* () {
    const endpoint = yield* acquireSendEndpoint({
      send: () => Effect.succeed(sendResult),
    });
    yield* routeRequestsThrough(answerSendWithMessage(message));

    const error = yield* endpoint.send(sendInput).pipe(Effect.flip);

    expect(error).toStrictEqual(new SendError({ reason: "outcome-unknown" }));
  });
}

const AMBIGUOUS_SEND_ANSWERS = [
  {
    name: "a JSON-RPC error without refusal data",
    message: { error: { code: -32602, message: "Invalid completed result" } },
  },
  {
    name: "a JSON-RPC error with malformed refusal data",
    message: {
      error: { code: -32602, message: "Invalid completed result", data: false },
    },
  },
  {
    name: "a JSON-RPC error with an unknown refusal reason",
    message: {
      error: {
        code: -32603,
        message: "Unrecognized failure",
        data: { reason: "unrecognized-failure" },
      },
    },
  },
  {
    name: "an untyped tool error",
    message: { result: { content: [], isError: true } },
  },
  {
    name: "a malformed successful result",
    message: {
      result: {
        content: [],
        structuredContent: { kind: "collective", operationId: false },
      },
    },
  },
];

const classifiesAnHttpAnswerToASend = (
  status: number,
  reason: SendError["reason"],
) =>
  Effect.gen(function* () {
    const { port } = yield* acquireBoundaryServer(operations);
    const endpoint = yield* acquireHarnessEndpoint(
      new URL(`http://127.0.0.1:${port}/mcp`),
      { credential: mcpTestCredential, receiveMode: "push" },
    );
    yield* routeRequestsThrough(answerSendWithStatus(status));

    const error = yield* endpoint.send(sendInput).pipe(Effect.flip);

    expect(error).toStrictEqual(new SendError({ reason }));
  });

/**
 * A send after the server closed. Its requests open one connection each, so
 * the send meets a refused connection rather than a pooled socket the server
 * closed, which the request may race.
 */
const keepsARefusedConnectionNetworkUnavailable = () =>
  Effect.gen(function* () {
    yield* routeRequestsThrough(new Agent({ pipelining: 0 }));
    const serverScope = yield* Scope.make();
    const { port } = yield* acquireBoundaryServer(operations).pipe(
      Scope.extend(serverScope),
    );
    const endpoint = yield* acquireHarnessEndpoint(
      new URL(`http://127.0.0.1:${port}/mcp`),
      { credential: mcpTestCredential, receiveMode: "push" },
    );
    yield* Scope.close(serverScope, Exit.void);

    const error = yield* endpoint.send(sendInput).pipe(Effect.flip);

    expect(error).toStrictEqual(
      new SendError({ reason: "network-unavailable" }),
    );
  });

function acquireSendEndpoint(
  selected: Pick<HarnessMcpOperations, "send">,
  registered = true,
) {
  return Effect.gen(function* () {
    const { port } = yield* acquireBoundaryServer(
      {
        ...operations,
        ...selected,
      },
      registered,
    );
    return yield* acquireHarnessEndpoint(
      new URL(`http://127.0.0.1:${port}/mcp`),
      { credential: mcpTestCredential, receiveMode: "push" },
    );
  });
}

const invalidSendCalls = [
  { arguments: { input: sendInput, unexpected: true } },
  { arguments: { input: { to: "agent:bob", text: 1 } } },
];

const checksSemanticSendSchema = (client: Client) =>
  Effect.gen(function* () {
    const catalog = yield* Effect.tryPromise(() => client.listTools());
    const schema = catalog.tools.find(
      (tool) => tool.name === "send_message",
    )?.inputSchema;
    if (schema === undefined) {
      return yield* Effect.dieMessage("Expected the semantic send schema");
    }
    expect(schema.additionalProperties).toBe(false);
    expect(Object.keys(schema.properties ?? {})).toEqual(["input"]);
    const validator = fromJsonSchema(
      // eslint-disable-next-line agent-code-guard/require-assertion-rationale -- The real server emits JSONSchema.make output; the SDK catalog type widens its nested properties to JSON values.
      schema as JsonSchemaType,
    );
    for (const input of [
      {
        to: "group:alice,bob,carol",
        text: "Ready?",
        collective: {
          op: "gather",
          deadline: 600,
          requestedSchema: {
            type: "object",
            properties: { ready: { type: "boolean", default: false } },
            required: ["ready"],
          },
        },
      },
      {
        to: "agent:bob",
        collectiveResponse: {
          operationId: requestId,
          action: "accept",
          content: { ready: true },
        },
      },
    ]) {
      expect(
        yield* Effect.tryPromise(() =>
          Promise.resolve(validator["~standard"].validate({ input })),
        ),
      ).toHaveProperty("value");
    }
  });

/** Invalid model arguments fail before execution; unrelated SDK metadata remains transport-owned. */
function rejectsSendBookkeepingArguments() {
  return Effect.gen(function* () {
    const executed: unknown[] = [];
    const { port } = yield* acquireBoundaryServer({
      ...operations,
      send: (input) =>
        Effect.sync(() => {
          executed.push(input);
          return sendResult;
        }),
    });
    const client = yield* acquireProtocolClient(port, "semantic-send-client");
    yield* checksSemanticSendSchema(client);
    for (const call of invalidSendCalls) {
      const error = yield* Effect.tryPromise(() =>
        client.callTool({ name: "send_message", ...call }),
      ).pipe(Effect.flip);
      expect(error).toMatchObject({
        cause: { code: ProtocolErrorCode.InvalidParams },
      });
    }
    expect(executed).toEqual([]);
    yield* Effect.tryPromise(() =>
      client.callTool({
        name: "send_message",
        arguments: { input: sendInput },
        _meta: {
          "another.example/trace": { id: "independent" },
        },
      }),
    );
    expect(executed).toEqual([{ input: sendInput }]);
  });
}

function readsRetainedEventThroughSdk() {
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const store = yield* openInboxFixture(yield* fs.makeTempDirectoryScoped());
    const deliveryToken = Schema.decodeUnknownSync(DeliveryToken)(
      digest("dlv_", 7),
    );
    const item = Schema.decodeUnknownSync(InboundItem)({
      kind: "operationFailed",
      operationId: collectiveOperationId(
        testAgentView("agent:alice").agentId,
        7,
      ),
      to: "agent:bob",
      error: "retained result",
    });
    const canonicalItem = yield* encodeRuntimeValue(item);
    const guidance = localGuidance(7);
    yield* store.putInboxItem({
      deliveryToken,
      canonicalItem,
      canonicalGuidance: yield* encodeRuntimeValue(guidance),
    });
    yield* store.acknowledgeInboxItem(deliveryToken);
    const { port } = yield* acquireBoundaryServer({
      ...operations,
      readEvent: ({ eventId }) =>
        Schema.decodeUnknown(DeliveryToken)(eventId).pipe(
          Effect.catchTag("ParseError", () =>
            Effect.fail({ reason: "invalid-event" }),
          ),
          Effect.flatMap(readRuntimeEvent),
          store.run,
        ),
    });
    const client = yield* acquireProtocolClient(port, "retained-event-client");
    const result = yield* Effect.tryPromise(() =>
      client.callTool({
        name: "read_event",
        arguments: { eventId: deliveryToken },
      }),
    );
    expect(result.structuredContent).toEqual({ item, guidance });
    expect((yield* store.readInboxSummary()).pendingCount).toBe(0);
  }).pipe(Effect.provide(NodeFileSystem.layer));
}

// @agent-code-guard/regression-only: this boundary pins the exact capability and closed transport failures.
describe("Harness MCP HTTP boundary", () => {
  it.each(SENDS_WITH_GUIDANCE)(
    "preserves admitted $kind identity and local guidance through MCP",
    preservesAdmittedSendGuidance,
  );
  it(
    "keeps dispatch and invalid output distinct from rejected send input",
    distinguishesSendValidationFailures,
    MCP_TRACE_TIMEOUT_MS,
  );
  it(
    "reads an acknowledged event with its guidance without redelivery",
    readsRetainedEventThroughSdk,
  );
  it(
    "rejects bookkeeping in model arguments and validates runtime metadata",
    rejectsSendBookkeepingArguments,
    MCP_TRACE_TIMEOUT_MS,
  );
  it(
    "advertises the event descriptor before registration",
    advertisesEventsBeforeRegistration,
  );
  it(
    "keeps malformed input separate from closed domain failures",
    distinguishesProtocolAndDomainFailures,
  );
  it(
    "reports a status failure outside its vocabulary as incompatible-daemon",
    reportsOutsideVocabularyStatusFailure,
  );
  it(
    "sanitizes unexpected operation defects",
    sanitizesUnexpectedOperationDefects,
  );
  it("emits heartbeat frames while a subscription is idle", emitsIdleHeartbeat);
  it(
    "reports an unexpected subscription disconnect",
    reportsUnexpectedSubscriptionLoss,
  );
});

function preservesFirewallRefusal() {
  return Effect.gen(function* () {
    const refusal = new FirewallSendError({
      checkId: `chk_${"1f".repeat(32)}`,
      refusal: "rule-violation",
      feedback: [{ nextAction: "revise", text: "Include Carol in the reply." }],
    });
    const endpoint = yield* acquireSendEndpoint({
      send: () => Effect.fail(refusal),
    });
    const error = yield* Effect.flip(endpoint.send(sendInput));
    expect(error).toBeInstanceOf(FirewallSendError);
    expect(error.message).toBe(refusal.message);
    expect(error.message).toContain(refusal.checkId);
    expect(error).toMatchObject({
      checkId: refusal.checkId,
      feedback: refusal.feedback,
    });
  });
}

describe("a refused send across the daemon boundary", () => {
  it.each(AMBIGUOUS_SEND_ANSWERS)(
    "reports outcome-unknown for $name",
    ({ message }) => reportsAmbiguousSendAnswer(message),
  );
  it(
    "preserves the typed firewall refusal and check reference through MCP",
    preservesFirewallRefusal,
  );
  it.each(DETAILED_REFUSALS)(
    "carries the detail of a refusal: %s",
    carriesARefusalDetailAcrossTheDaemonBoundary,
  );
  it(
    "answers outcome-unknown for a send that ends without a typed failure",
    answersOutcomeUnknownForAnUntypedSendFailure,
  );
  it(
    "reports outcome-unknown when the connection drops during a send",
    reportsOutcomeUnknownWhenTheConnectionDropsMidSend,
    MCP_TRACE_TIMEOUT_MS,
  );
  it(
    "keeps a refused connection network-unavailable",
    keepsARefusedConnectionNetworkUnavailable,
    MCP_TRACE_TIMEOUT_MS,
  );
  it.each([
    [404, "network-unavailable"],
    [499, "outcome-unknown"],
    [500, "outcome-unknown"],
  ] as const)(
    "classifies an HTTP %i answer to a send as %s",
    ([status, reason]) => classifiesAnHttpAnswerToASend(status, reason),
    MCP_TRACE_TIMEOUT_MS,
  );
});
