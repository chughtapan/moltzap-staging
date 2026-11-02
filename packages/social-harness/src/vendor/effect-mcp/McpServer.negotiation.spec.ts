/** @file Negotiated output schemas and payloads agree for unmodified modern and initialize clients. */
import { HttpApp } from "@effect/platform";
import { scopedLive as it } from "@effect/vitest";
import {
  Client,
  type ClientOptions,
  StreamableHTTPClientTransport,
} from "@modelcontextprotocol/client";
import { Deferred, Effect, Schema } from "effect";
import { expect } from "vitest";
import { CallToolResult, McpServerClient, Tool } from "./McpSchema.js";
import { makeHttpApp, McpServer } from "./McpServer.js";

it.each([
  {
    era: "modern",
    options: {
      capabilities: {},
      versionNegotiation: { mode: { pin: "2026-07-28" } },
    } satisfies ClientOptions,
    expected: true,
  },
  {
    era: "initialize",
    options: {
      capabilities: {},
      supportedProtocolVersions: ["2025-11-25"],
      versionNegotiation: { mode: "legacy" },
    } satisfies ClientOptions,
    expected: { result: true },
  },
])(
  "matches primitive output and root references for a $era client",
  ({ era, options, expected }) =>
    Effect.gen(function* () {
      const server = yield* McpServer.make;
      yield* server.addTool({
        tool: new Tool({
          name: "choose",
          inputSchema: {
            type: "object",
            properties: {},
            additionalProperties: false,
          },
          outputSchema: {
            anyOf: [{ $ref: "#/$defs/Word" }, { type: "boolean" }],
            $defs: { Word: { type: "string", enum: ["yes"] } },
          },
        }),
        handle: () =>
          Effect.succeed(
            new CallToolResult({
              content: [{ type: "text", text: "true" }],
              structuredContent: true,
            }),
          ),
      });
      const app = yield* makeHttpApp({
        name: "negotiated-schema-test",
        version: "1",
      }).pipe(Effect.provideService(McpServer, server));
      const handleRequest = HttpApp.toWebHandler(app);
      const client = new Client(
        { name: "unmodified-client", version: "1" },
        options,
      );
      // eslint-disable-next-line agent-code-guard/acquire-release-requires-scope -- scopedLive owns the SDK connection.
      yield* Effect.acquireRelease(
        Effect.tryPromise(() =>
          client.connect(
            new StreamableHTTPClientTransport(new URL("http://127.0.0.1/mcp"), {
              fetch: (input, init) => handleRequest(new Request(input, init)),
            }),
          ),
        ),
        () => Effect.tryPromise(() => client.close()).pipe(Effect.ignore),
      );
      const listed = yield* Effect.tryPromise(() => client.listTools());
      expect(listed.tools[0]?.outputSchema).toHaveProperty("$defs.Word");
      const response = yield* Effect.tryPromise(() =>
        client.callTool({ name: "choose", arguments: {} }),
      );
      expect(response.structuredContent).toEqual(expected);
      if (era === "modern") {
        const changed = yield* Deferred.make<undefined>();
        client.setNotificationHandler(
          "notifications/tools/list_changed",
          () => {
            Effect.runFork(Deferred.succeed(changed, undefined));
          },
        );
        // eslint-disable-next-line agent-code-guard/acquire-release-requires-scope -- scopedLive owns the listening request.
        const subscription = yield* Effect.acquireRelease(
          Effect.tryPromise(() => client.listen({ toolsListChanged: true })),
          (listening) =>
            Effect.tryPromise(() => listening.close()).pipe(Effect.ignore),
        );
        expect(subscription.honoredFilter.toolsListChanged).toBe(true);
        yield* server.toolsChanged;
        yield* Deferred.await(changed).pipe(Effect.timeout("2 seconds"));
        yield* Effect.tryPromise(() => subscription.close());

        const reason = yield* Effect.tryPromise(() => subscription.closed);
        // eslint-disable-next-line agent-code-guard/no-hardcoded-assertion-literals -- The SDK reports this reason when a client closes its subscription.
        expect(reason).toBe("local");
      }
    }),
);

it("keeps an Events stream scoped while ordinary requests complete", () =>
  Effect.gen(function* () {
    const stopped = yield* Deferred.make<undefined>();
    const activeNotification = "notifications/events/active";
    const subscriptionId = "events";
    const server = yield* McpServer.make;
    server.events.current = {
      list: () => Effect.succeed({ events: [] }),
      subscribe: () => Effect.succeed({}),
      unsubscribe: () => Effect.succeed({}),
      stream: () =>
        Effect.gen(function* () {
          const client = yield* McpServerClient;
          if (client.notify === undefined) {
            return yield* Effect.dieMessage("Expected stream notifications");
          }
          yield* client.notify(activeNotification, {
            cursor: null,
            truncated: false,
          });
          return yield* Effect.never;
        }).pipe(Effect.ensuring(Deferred.succeed(stopped, undefined))),
    };
    const app = yield* makeHttpApp({
      name: "events-lifetime-test",
      version: "1",
    }).pipe(Effect.provideService(McpServer, server));
    const handleRequest = HttpApp.toWebHandler(app);
    const abort = new AbortController();
    // eslint-disable-next-line agent-code-guard/acquire-release-requires-scope -- scopedLive owns the streaming HTTP request.
    const response = yield* Effect.acquireRelease(
      Effect.tryPromise(() =>
        handleRequest(
          new Request("http://127.0.0.1/mcp", {
            method: "POST",
            headers: { "content-type": "application/json" },
            signal: abort.signal,
            body: JSON.stringify({
              jsonrpc: "2.0",
              id: subscriptionId,
              method: "events/stream",
              params: {},
            }),
          }),
        ),
      ),
      () =>
        Effect.sync(() => {
          abort.abort();
        }),
    );
    const reader = response.body?.getReader();
    if (reader === undefined) {
      return yield* Effect.dieMessage("Expected stream body");
    }
    const first = yield* Effect.tryPromise(() => reader.read());
    const bytes = yield* Schema.decodeUnknown(Schema.Uint8ArrayFromSelf)(
      first.value,
    );
    const frame = new TextDecoder().decode(bytes);
    const notification = yield* Schema.decodeUnknown(
      Schema.parseJson(Schema.Unknown),
    )(frame.split("data: ")[1]);
    expect(notification).toMatchObject({
      method: activeNotification,
      params: {
        _meta: { "io.modelcontextprotocol/subscriptionId": subscriptionId },
      },
    });

    const ping = yield* Effect.tryPromise(() =>
      handleRequest(
        new Request("http://127.0.0.1/mcp", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ jsonrpc: "2.0", id: "ping", method: "ping" }),
        }),
      ),
    );
    expect(yield* Effect.tryPromise(() => ping.json())).toMatchObject({
      id: "ping",
      result: {},
    });
    yield* Effect.sync(() => {
      abort.abort();
    });
    yield* Effect.tryPromise(() => reader.cancel());
    yield* Deferred.await(stopped).pipe(Effect.timeout("2 seconds"));
  }));
