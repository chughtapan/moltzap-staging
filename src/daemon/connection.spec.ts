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
  acquireMcpConnection,
  connectFromEnvironment,
  McpConnectError,
  sendMessage,
} from "../mcp/index.js";
import { toolDefinitions } from "../mcp/tools.js";
import { loopbackMcpFixture } from "./__tests__/mcp-fixtures.js";

const implementation = { name: "shared-mcp-test", version: "1" };
const credential = Redacted.make("private-fixture-credential");
const entry = Schema.decodeUnknownSync(toolDefinitions.receive_messages.output)(
  {
    entries: [
      {
        address: "agent:bob",
        message: {
          author: {
            agentId: `agt_${"A".repeat(22)}`,
            agentName: "bob",
            principalId: `prn_${"A".repeat(22)}`,
          },
          recipients: [
            {
              agentId: `agt_${"B".repeat(21)}A`,
              agentName: "alice",
              principalId: `prn_${"B".repeat(21)}A`,
            },
          ],
          msgId: `msg_${"A".repeat(43)}`,
          content: [{ type: "text", text: "hello" }],
        },
      },
    ],
  },
).entries[0];
if (entry === undefined) {
  throw new Error("Fixture has no inbox entry");
}
afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
});

/**
 * A daemon whose first inbox read returns one entry with a cursor and whose
 * later reads wait. `cursors` holds the cursor each read presented, and
 * `returned` completes when a second read arrives.
 */
const inboxFixture = Effect.gen(function* () {
  const cursors = yield* Ref.make<ReadonlyArray<string | undefined>>([]);
  const returned = yield* Deferred.make<undefined>();
  const fixture = yield* loopbackMcpFixture({
    harness: {
      receive: ({ cursor }: { readonly cursor?: string }) =>
        Ref.getAndUpdate(cursors, (earlier) => [...earlier, cursor]).pipe(
          Effect.flatMap((earlier) =>
            earlier.length === 0
              ? Effect.succeed({ entries: [entry], nextCursor: "after-entry" })
              : Deferred.succeed(returned, undefined).pipe(
                  Effect.zipRight(Effect.never),
                ),
          ),
        ),
    },
  });
  return {
    cursors,
    returned,
    endpoint: fixture.endpoint,
    credential: fixture.credential,
  };
});

describe("shared inbox reception", () => {
  it("commits a page by presenting its cursor on the next read, once the host has accepted every entry in it", () =>
    Effect.gen(function* () {
      const fixture = yield* inboxFixture;
      const connection = yield* acquireMcpConnection(
        { endpoint: fixture.endpoint, credential: fixture.credential },
        implementation,
      );
      const entered = yield* Deferred.make<undefined>();
      const accepted = yield* Deferred.make<undefined>();
      const receiver = yield* connection
        .consumeInbox((received) => {
          expect(received).toEqual(entry);
          return Deferred.succeed(entered, undefined).pipe(
            Effect.zipRight(Deferred.await(accepted)),
          );
        })
        .pipe(Effect.forkScoped);
      yield* Deferred.await(entered);
      expect(yield* Ref.get(fixture.cursors)).toEqual([undefined]);
      yield* Deferred.succeed(accepted, undefined);
      yield* Deferred.await(fixture.returned);
      yield* Fiber.interrupt(receiver);
      expect(yield* Ref.get(fixture.cursors)).toEqual([
        undefined,
        "after-entry",
      ]);
    }).pipe(Effect.scoped, Effect.runPromise));

  it("stops at a failed callback without committing its page", () =>
    Effect.gen(function* () {
      const fixture = yield* inboxFixture;
      const connection = yield* acquireMcpConnection(
        { endpoint: fixture.endpoint, credential: fixture.credential },
        implementation,
      );
      const refusal = "host-refused";
      expect(
        yield* connection
          .consumeInbox(() => Effect.fail(refusal))
          .pipe(Effect.flip),
      ).toBe(refusal);
      expect(yield* Ref.get(fixture.cursors)).toEqual([undefined]);
    }).pipe(Effect.scoped, Effect.runPromise));

  it("interrupts a pending callback when its connection closes", () =>
    Effect.gen(function* () {
      const fixture = yield* inboxFixture;
      const scope = yield* Scope.make();
      const connection = yield* acquireMcpConnection(
        { endpoint: fixture.endpoint, credential: fixture.credential },
        implementation,
      ).pipe(Effect.provideService(Scope.Scope, scope));
      const entered = yield* Deferred.make<undefined>();
      const interrupted = yield* Deferred.make<undefined>();
      const receiver = yield* connection
        .consumeInbox(() =>
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
      expect(yield* Ref.get(fixture.cursors)).toEqual([undefined]);
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
            expect(error).toEqual(
              new McpConnectError({ reason: "transport-failed" }),
            );
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
          if (call === undefined) {
            return expect.unreachable("MCP did not connect");
          }
          const request = new Request(...call);
          expect(request.url).toBe(endpoint.href);
          expect(request.headers.get("Authorization")).toBe(
            `Bearer ${Redacted.value(credential)}`,
          );
          expect(request).toMatchObject({ redirect: "error" });
        }),
      ),
      Effect.asVoid,
      Effect.runPromise,
    );
  });
});

describe("the host's environment", () => {
  /** A readable file; the empty bearer it holds is not under test. */
  const credentialFile = "/dev/null";
  it.each([
    { value: undefined, requests: 0 },
    { value: "false", requests: 0 },
    { value: "true", requests: 1 },
    { value: "invalid", requests: 0 },
  ])(
    "sends the bearer over HTTP only when SHARNESS_MCP_ALLOW_HTTP is true: $value",
    ({ value, requests }) => {
      vi.stubEnv("SHARNESS_MCP_URL", "http://internal.example/mcp");
      vi.stubEnv("SHARNESS_MCP_CREDENTIAL_FILE", credentialFile);
      vi.stubEnv("SHARNESS_MCP_ALLOW_HTTP", value);
      const fetch = vi
        .spyOn(globalThis, "fetch")
        .mockResolvedValue(new Response(null, { status: 401 }));
      return connectFromEnvironment(implementation).pipe(
        Effect.scoped,
        Effect.flip,
        Effect.tap(() =>
          Effect.sync(() => {
            expect(fetch).toHaveBeenCalledTimes(requests);
          }),
        ),
        Effect.asVoid,
        Effect.runPromise,
      );
    },
  );
});

describe("send outcomes", () => {
  const hello = { to: "agent:bob", text: "hello" };
  it.each([
    {
      ending: "the transport closes",
      error: new Error("transport closed"),
      reason: "outcome-unknown",
    },
    {
      ending: "the daemon answers a protocol error",
      error: new ProtocolError(-32603, "private failure"),
      reason: "outcome-unknown",
    },
    {
      ending: "the daemon refuses the connection",
      error: new Error("fetch failed", {
        cause: Object.assign(new Error("refused"), { code: "ECONNREFUSED" }),
      }),
      reason: "network-unavailable",
    },
  ])(
    "answers $reason, without calling again, when $ending",
    ({ error, reason }) => {
      const mcp = new Client(implementation);
      const called = vi.spyOn(mcp, "callTool").mockRejectedValue(error);
      return sendMessage(mcp, hello).pipe(
        Effect.tap((result) =>
          Effect.sync(() => {
            expect(result).toMatchObject({
              isError: true,
              structuredContent: { reason },
            });
            expect(JSON.stringify(result)).not.toContain(error.message);
            expect(called).toHaveBeenCalledTimes(1);
          }),
        ),
        Effect.asVoid,
        Effect.runPromise,
      );
    },
  );
  it("treats a completed send whose result is not a send's as uncertain", () => {
    const mcp = new Client(implementation);
    vi.spyOn(mcp, "callTool").mockResolvedValue({
      content: [],
      structuredContent: { private: "invalid-result" },
    });
    return sendMessage(mcp, hello).pipe(
      Effect.tap((result) =>
        Effect.sync(() => {
          expect(result).toMatchObject({
            isError: true,
            structuredContent: { reason: "outcome-unknown" },
          });
        }),
      ),
      Effect.asVoid,
      Effect.runPromise,
    );
  });
});
