/** @file Negotiated output schemas and payloads agree for unmodified modern and initialize clients. */
import { HttpApp } from "@effect/platform";
import { scopedLive as it } from "@effect/vitest";
import {
  Client,
  type ClientOptions,
  StreamableHTTPClientTransport,
} from "@modelcontextprotocol/client";
import { Deferred, Effect } from "effect";
import { expect } from "vitest";
import { CallToolResult, Tool } from "./McpSchema.js";
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
        yield* server.notifications["notifications/tools/list_changed"]({});
        yield* Deferred.await(changed).pipe(Effect.timeout("2 seconds"));
        yield* Effect.tryPromise(() => subscription.close());

        const reason = yield* Effect.tryPromise(() => subscription.closed);
        // eslint-disable-next-line agent-code-guard/no-hardcoded-assertion-literals -- The SDK reports this reason when a client closes its subscription.
        expect(reason).toBe("local");
      }
    }),
);
