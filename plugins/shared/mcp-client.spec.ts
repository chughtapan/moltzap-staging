/** @file Inbox reads record delivery before native host callbacks run. */
import { Client, ProtocolError } from "@modelcontextprotocol/client";
import {
  Deferred,
  Effect,
  Exit,
  Fiber,
  Redacted,
  Ref,
  Schema,
  Scope,
} from "effect";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  mcpFixture,
  loopbackMcpEndpoint,
  mcpTestCredential,
} from "../../packages/social-harness/src/harness/__tests__/mcp-fixtures.js";
import { toolDefinitions } from "@moltzap/social-harness";
import { acquireMcpConnection, callTool } from "./mcp-client.js";

const implementation = { name: "shared-mcp-test", version: "1" };
const credential = Redacted.make("private-fixture-credential");
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
if (entry === undefined) throw new Error("Fixture has no inbox entry");
afterEach(() => vi.restoreAllMocks());

const inboxFixture = Effect.gen(function* () {
  const acknowledged = yield* Ref.make(0);
  const receipt = yield* Deferred.make<void>();
  const fixture = yield* mcpFixture({
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
  const endpoint = yield* loopbackMcpEndpoint(fixture.app);
  return { acknowledged, receipt, endpoint };
});

describe("shared inbox reception", () => {
  it.each(["poll", "push"] as const)(
    "records delivery before host acceptance over %s",
    (receiveMode) =>
      Effect.gen(function* () {
        const fixture = yield* inboxFixture;
        const connection = yield* acquireMcpConnection(
          { endpoint: fixture.endpoint, credential: mcpTestCredential },
          implementation,
        );
        const entered = yield* Deferred.make<void>();
        const accepted = yield* Deferred.make<void>();
        const consume = connection.consumeInbox;
        const options = { onReady: Effect.void, receiveMode };
        const receiver = yield* consume(options, (received) => {
          expect(received).toEqual(entry);
          return Deferred.succeed(entered, undefined).pipe(
            Effect.zipRight(Deferred.await(accepted)),
          );
        }).pipe(Effect.forkScoped);
        yield* Deferred.await(entered);
        expect(yield* Ref.get(fixture.acknowledged)).toBe(1);
        yield* Deferred.succeed(accepted, undefined);
        yield* Deferred.await(fixture.receipt);
        yield* Fiber.interrupt(receiver);
        expect(yield* Ref.get(fixture.acknowledged)).toBe(1);
      }).pipe(Effect.scoped, Effect.runPromise),
  );

  it("keeps a failed callback from undoing delivery", () =>
    Effect.gen(function* () {
      const fixture = yield* inboxFixture;
      const connection = yield* acquireMcpConnection(
        { endpoint: fixture.endpoint, credential: mcpTestCredential },
        implementation,
      );
      expect(
        yield* connection
          .consumeInbox({}, () => Effect.fail("host-refused"))
          .pipe(Effect.flip),
      ).toBe("host-refused");
      expect(yield* Ref.get(fixture.acknowledged)).toBe(1);
    }).pipe(Effect.scoped, Effect.runPromise));

  it("interrupts a pending callback when its connection closes", () =>
    Effect.gen(function* () {
      const fixture = yield* inboxFixture;
      const scope = yield* Scope.make();
      const connection = yield* acquireMcpConnection(
        { endpoint: fixture.endpoint, credential: mcpTestCredential },
        implementation,
      ).pipe(Effect.provideService(Scope.Scope, scope));
      const entered = yield* Deferred.make<void>();
      const interrupted = yield* Deferred.make<void>();
      const receiver = yield* connection
        .consumeInbox({}, () =>
          Deferred.succeed(entered, undefined).pipe(
            Effect.zipRight(Effect.never),
            Effect.onInterrupt(() => Deferred.succeed(interrupted, undefined)),
          ),
        )
        .pipe(Effect.forkScoped);
      yield* Deferred.await(entered);
      yield* Scope.close(scope, Exit.void);
      yield* Deferred.await(interrupted);
      yield* Fiber.await(receiver);
      expect(yield* Ref.get(fixture.acknowledged)).toBe(1);
    }).pipe(Effect.scoped, Effect.runPromise));
});

describe("credential transport", () => {
  it.each([
    { url: "http://remote.example/mcp", allowHttp: undefined },
    { url: "http://remote.example/mcp", allowHttp: false },
    { url: "http://localhost/mcp", allowHttp: undefined },
    { url: "https://user:password@remote.example/mcp", allowHttp: true },
    { url: "https://remote.example/mcp#fragment", allowHttp: true },
    { url: "http://user:password@remote.example/mcp", allowHttp: true },
    { url: "http://remote.example/mcp#fragment", allowHttp: true },
    { url: "ftp://remote.example/mcp", allowHttp: true },
  ])(
    "refuses $url with allowHttp=$allowHttp before sending a bearer",
    ({ url, allowHttp }) => {
      const fetch = vi.spyOn(globalThis, "fetch");
      return acquireMcpConnection(
        {
          endpoint: new URL(url),
          credential,
          ...(allowHttp === undefined ? {} : { allowHttp }),
        },
        implementation,
      ).pipe(
        Effect.scoped,
        Effect.flip,
        Effect.tap((error) =>
          Effect.sync(() => {
            expect(error.reason).toBe("transport-failed");
            expect(fetch).not.toHaveBeenCalled();
          }),
        ),
        Effect.asVoid,
        Effect.runPromise,
      );
    },
  );
  it("sends the bearer to host-approved HTTP without following redirects", () => {
    const fetch = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValue(new Response(null, { status: 401 }));
    const endpoint = new URL("http://internal.example/mcp");
    return acquireMcpConnection(
      { endpoint, credential, allowHttp: true },
      implementation,
    ).pipe(
      Effect.scoped,
      Effect.flip,
      Effect.tap(() =>
        Effect.sync(() => {
          const call = fetch.mock.calls[0];
          if (call === undefined)
            return expect.unreachable("MCP did not connect");
          const request = new Request(...call);
          expect(request.url).toBe(endpoint.href);
          expect(request.headers.get("Authorization")).toBe(
            `Bearer ${Redacted.value(credential)}`,
          );
          expect(request.redirect).toBe("error");
        }),
      ),
      Effect.asVoid,
      Effect.runPromise,
    );
  });
  it("captures the URL before the caller mutates it", () => {
    const fetch = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValue(new Response(null, { status: 401 }));
    const endpoint = new URL("https://native.example/mcp");
    const original = endpoint.href;
    const connection = acquireMcpConnection(
      { endpoint, credential },
      implementation,
    );
    endpoint.hostname = "different.example";
    return connection.pipe(
      Effect.scoped,
      Effect.flip,
      Effect.tap(() =>
        Effect.sync(() => {
          const input = fetch.mock.calls[0]?.[0];
          expect(input instanceof Request ? input.url : input?.toString()).toBe(
            original,
          );
        }),
      ),
      Effect.asVoid,
      Effect.runPromise,
    );
  });
});

describe("send outcomes", () => {
  it.each([
    new Error("transport closed"),
    new ProtocolError(-32603, "private failure"),
  ])("does not retry an uncertain send", (error) => {
    const client = new Client(implementation);
    const called = vi.spyOn(client, "callTool").mockRejectedValue(error);
    const input = Schema.decodeUnknownSync(toolDefinitions.send_message.input)({
      input: { to: "agent:bob", text: "hello" },
    });
    return callTool(client, { name: "send_message", arguments: input }).pipe(
      Effect.flip,
      Effect.tap((failure) =>
        Effect.sync(() => {
          expect(failure).toMatchObject({
            _tag: "SendError",
            reason: "outcome-unknown",
          });
          expect(called).toHaveBeenCalledTimes(1);
        }),
      ),
      Effect.asVoid,
      Effect.runPromise,
    );
  });
  it("treats a malformed completed send result as uncertain", () => {
    const client = new Client(implementation);
    vi.spyOn(client, "callTool").mockResolvedValue({
      content: [],
      structuredContent: { private: "invalid-result" },
    });
    const input = Schema.decodeUnknownSync(toolDefinitions.send_message.input)({
      input: { to: "agent:bob", text: "hello" },
    });
    return callTool(client, { name: "send_message", arguments: input }).pipe(
      Effect.flip,
      Effect.tap((failure) =>
        Effect.sync(() =>
          expect(failure).toMatchObject({ reason: "outcome-unknown" }),
        ),
      ),
      Effect.asVoid,
      Effect.runPromise,
    );
  });
});

describe("management failures", () => {
  const failures: readonly {
    readonly request: import("@moltzap/social-harness").ToolRequest;
    readonly data: { readonly reason: string };
    readonly tag: string;
  }[] = [
    {
      request: {
        name: "read_event",
        arguments: { eventId: entry.deliveryToken },
      },
      data: { reason: "unknown-event" },
      tag: "InboxError",
    },
    {
      request: { name: "search_conversations", arguments: {} },
      data: { reason: "invalid-cursor" },
      tag: "HistoryError",
    },
    {
      request: {
        name: "register",
        arguments: Schema.decodeUnknownSync(toolDefinitions.register.input)({
          operationId: `opn_${"A".repeat(22)}`,
          principalId: `prn_${"A".repeat(22)}`,
          agentName: "alice",
        }),
      },
      data: { reason: "admission-refused" },
      tag: "RegistrationError",
    },
    {
      request: { name: "policy_read", arguments: {} },
      data: { reason: "approval-refused" },
      tag: "FirewallPolicyError",
    },
  ];
  it.each(failures)(
    "reconstructs $tag from the owning wire schema",
    ({ request, data, tag }) => {
      const client = new Client(implementation);
      vi.spyOn(client, "callTool").mockRejectedValue(
        new ProtocolError(-32603, "private transport detail", data),
      );
      return callTool(client, request).pipe(
        Effect.flip,
        Effect.tap((failure) =>
          Effect.sync(() => {
            expect(failure).toMatchObject({ _tag: tag, reason: data.reason });
            expect(String(failure)).not.toContain("private transport detail");
          }),
        ),
        Effect.asVoid,
        Effect.runPromise,
      );
    },
  );
});
