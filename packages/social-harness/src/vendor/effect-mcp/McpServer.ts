/** @license MIT. Effect contributors. See LICENSE and README.md in this directory. */
/** @file Serves MCP tools and optional event delivery over HTTP. */
import * as HttpApp from "@effect/platform/HttpApp";
import * as HttpServerRequest from "@effect/platform/HttpServerRequest";
import * as HttpServerResponse from "@effect/platform/HttpServerResponse";
import * as Queue from "effect/Queue";
import * as Scope from "effect/Scope";
import * as StreamValues from "effect/Stream";
import * as RpcMessageValues from "@effect/rpc/RpcMessage";
import * as Headers from "@effect/platform/Headers";
import * as RpcServer from "@effect/rpc/RpcServer";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type * as Types from "effect/Types";
import type {
  CallTool,
  ServerCapabilities,
  SubscriptionFilter,
} from "./McpSchema.js";
import {
  CallToolResult,
  ClientRpcs,
  CompleteResult,
  InternalError,
  McpError,
  ProtocolError,
  InvalidParams,
  ListPromptsResult,
  ListResourcesResult,
  ListResourceTemplatesResult,
  ListToolsResult,
  McpServerClient,
  McpServerClientMiddleware,
  Tool,
} from "./McpSchema.js";

interface ToolRegistration {
  readonly tool: Tool;
  /** Reevaluated for each listing and call. Omission makes the tool visible. */
  readonly visible?: Effect.Effect<boolean>;
  readonly handle: (
    payload: unknown,
  ) => Effect.Effect<CallToolResult, McpError, McpServerClient>;
}

/** Registers tools and optional event operations for one MCP server. */
export class McpServer extends Context.Tag("@effect/ai/McpServer")<
  McpServer,
  {
    readonly toolsChanged: Effect.Effect<void>;
    readonly toolChanges: Queue.Dequeue<undefined>;
    readonly listTools: Effect.Effect<
      ReadonlyArray<Tool>,
      never,
      McpServerClient
    >;
    readonly events: {
      current?: {
        readonly list: (
          payload: unknown,
        ) => Effect.Effect<unknown, McpError, McpServerClient>;
        readonly stream: (
          payload: unknown,
        ) => Effect.Effect<unknown, McpError, McpServerClient>;
        readonly subscribe: (
          payload: unknown,
        ) => Effect.Effect<unknown, McpError, McpServerClient>;
        readonly unsubscribe: (
          payload: unknown,
        ) => Effect.Effect<unknown, McpError, McpServerClient>;
      };
    };

    readonly tools: ReadonlyArray<Tool>;
    /** Replaces a tool with the same name and announces the catalog change. */
    readonly addTool: (options: ToolRegistration) => Effect.Effect<void>;
    readonly callTool: (
      requests: typeof CallTool.payloadSchema.Type,
    ) => Effect.Effect<CallToolResult, McpError, McpServerClient>;
  }
>() {
  /** Creates an empty tool catalog with coalesced change notifications. */
  static readonly make = Effect.gen(function* () {
    const registrations = new Map<string, ToolRegistration>();
    const events: McpServer["Type"]["events"] = {};
    const toolChanges = yield* Queue.sliding<undefined>(1);
    const toolsChanged = Queue.offer(toolChanges, undefined).pipe(
      Effect.asVoid,
    );

    return McpServer.of({
      toolsChanged,
      toolChanges,
      events,
      listTools: Effect.gen(function* () {
        const client = yield* McpServerClient;
        const selected = yield* Effect.filter(
          Array.from(registrations.values()),
          (registration) => registration.visible ?? Effect.succeed(true),
        );
        return selected.map(({ tool }) =>
          projectTool(tool, client.protocolVersion),
        );
      }),
      get tools() {
        return Array.from(registrations.values(), ({ tool }) => tool);
      },
      addTool: (options) =>
        Effect.suspend(() => {
          registrations.set(options.tool.name, options);
          return toolsChanged;
        }),
      callTool: (request) =>
        Effect.gen(function* () {
          const registration = registrations.get(request.name);
          if (
            !registration ||
            !(yield* registration.visible ?? Effect.succeed(true))
          ) {
            return yield* new InvalidParams({ message: "Tool not found" });
          }
          const client = yield* McpServerClient;
          const result = yield* registration.handle(request.arguments ?? {});
          return projectToolResult(
            result,
            registration.tool.outputSchema,
            client.protocolVersion,
          );
        }),
    });
  });
}

const LATEST_PROTOCOL_VERSION = "2026-07-28";
const SUPPORTED_PROTOCOL_VERSIONS = [
  LATEST_PROTOCOL_VERSION,
  "2025-11-25",
  "2025-03-26",
  "2024-11-05",
  "2024-10-07",
];

const layerHandlers = (serverInfo: {
  readonly name: string;
  readonly version: string;
}) =>
  ClientRpcs.toLayer(
    Effect.gen(function* () {
      const server = yield* McpServer;

      return {
        // Requests
        ping: () => Effect.succeed({}),
        "server/discover": () =>
          Effect.succeed({
            supportedVersions: SUPPORTED_PROTOCOL_VERSIONS,
            ttlMs: 0,
            cacheScope: "private",
            capabilities: {
              ...(server.tools.length > 0
                ? { tools: { listChanged: true } }
                : {}),
              ...(server.events.current ? { events: {} } : {}),
            },
          }),
        initialize(params) {
          const requestedVersion = params.protocolVersion;
          const capabilities: Types.DeepMutable<
            typeof ServerCapabilities.Type
          > = {
            completions: {},
          };
          if (server.tools.length > 0) {
            capabilities.tools = { listChanged: true };
          }
          if (server.events.current) (capabilities as any).events = {};
          return Effect.succeed({
            capabilities,
            serverInfo,
            protocolVersion: SUPPORTED_PROTOCOL_VERSIONS.includes(
              requestedVersion,
            )
              ? requestedVersion
              : LATEST_PROTOCOL_VERSION,
          });
        },
        "subscriptions/listen": ({ notifications }) =>
          Effect.gen(function* () {
            const context = yield* McpServerClient;
            if (!context.notify)
              return yield* Effect.fail(
                new ProtocolError(-32603, "Notification stream unavailable"),
              );
            yield* context.notify("notifications/subscriptions/acknowledged", {
              notifications,
            });
            return yield* Effect.never;
          }),
        "completion/complete": () => Effect.succeed(CompleteResult.empty),
        "logging/setLevel": () => InternalError.notImplemented,
        "prompts/get": ({ name }) =>
          new InvalidParams({ message: `Prompt '${name}' not found` }),
        "prompts/list": () =>
          Effect.sync(() => new ListPromptsResult({ prompts: [] })),
        "resources/list": () =>
          Effect.sync(() => new ListResourcesResult({ resources: [] })),
        "resources/read": () => Effect.succeed({ contents: [] }),
        "resources/subscribe": () => InternalError.notImplemented,
        "resources/unsubscribe": () => InternalError.notImplemented,
        "resources/templates/list": () =>
          Effect.sync(
            () =>
              new ListResourceTemplatesResult({
                resourceTemplates: [],
              }),
          ),
        "tools/call": server.callTool,
        "tools/list": () =>
          Effect.map(
            server.listTools,
            (tools) => new ListToolsResult({ tools }),
          ),
        "events/list": (payload) =>
          server.events.current?.list(payload) ??
          Effect.fail(new ProtocolError(-32601, "Events unavailable")),
        "events/stream": (payload) =>
          server.events.current?.stream(payload) ??
          Effect.fail(new ProtocolError(-32601, "Events unavailable")),
        "events/subscribe": (payload) =>
          server.events.current?.subscribe(payload) ??
          Effect.fail(new ProtocolError(-32601, "Events unavailable")),
        "events/unsubscribe": (payload) =>
          server.events.current?.unsubscribe(payload) ??
          Effect.fail(new ProtocolError(-32601, "Events unavailable")),

        // Notifications
        "notifications/cancelled": (_) => Effect.void,
        "notifications/initialized": (_) => Effect.void,
        "notifications/progress": (_) => Effect.void,
        "notifications/roots/list_changed": (_) => Effect.void,
      };
    }),
  );

const modernVersion = (version?: string) => version === "2026-07-28";

const projectTool = (tool: Tool, version?: string): Tool => {
  const schema = tool.outputSchema as Record<string, unknown> | undefined;
  if (modernVersion(version) || !schema || schema.type === "object")
    return tool;
  const { $defs, ...root } = schema;
  return new Tool({
    ...tool,
    outputSchema: {
      type: "object",
      properties: { result: root },
      required: ["result"],
      ...($defs ? { $defs } : {}),
    },
  });
};

const projectToolResult = (
  result: CallToolResult,
  schema: unknown,
  version?: string,
): CallToolResult => {
  const output = schema as Record<string, unknown> | undefined;
  return modernVersion(version) ||
    !output ||
    output.type === "object" ||
    result.structuredContent === undefined
    ? result
    : new CallToolResult({
        ...result,
        structuredContent: { result: result.structuredContent },
      });
};

type HttpRequestState = {
  readonly id: string | number;
  readonly method: string;
  readonly protocolVersion: string;
  readonly filter?: typeof SubscriptionFilter.Type;
  readonly notifications: Queue.Queue<unknown>;
};

const wireError = (error: unknown) => {
  if (
    error instanceof ProtocolError ||
    error instanceof InternalError ||
    error instanceof InvalidParams
  ) {
    return {
      code: error.code,
      message: error.message,
      ...(error.data === undefined ? {} : { data: error.data }),
    };
  }
  return { code: -32603, message: "Internal error" };
};

const requestEnvelope = Schema.Struct({
  jsonrpc: Schema.Literal("2.0"),
  id: Schema.optional(Schema.Union(Schema.String, Schema.Finite)),
  method: Schema.String,
  params: Schema.optional(Schema.Unknown),
});

/**
 * Serves requests while the caller's scope remains open. Disconnecting cancels
 * that request's handler. Responses preserve request IDs and expose protocol errors.
 */
export const makeHttpApp = Effect.fnUntraced(function* (options: {
  readonly name: string;
  readonly version: string;
}) {
  const handlers = yield* Layer.build(layerHandlers(options));
  const states = new Map<number, HttpRequestState>();
  let nextClient = 0;
  const middleware = McpServerClientMiddleware.of(({ clientId }) =>
    Effect.succeed(
      McpServerClient.of({
        clientId,
        protocolVersion: states.get(clientId)?.protocolVersion,
        requestId: states.get(clientId)?.id,
        notify: (method, params) =>
          Effect.suspend(() => {
            const state = states.get(clientId);
            return state
              ? Queue.offer(state.notifications, {
                  jsonrpc: "2.0",
                  method,
                  params: {
                    ...params,
                    _meta: {
                      "io.modelcontextprotocol/subscriptionId": state.id,
                    },
                  },
                }).pipe(Effect.asVoid)
              : Effect.void;
          }),
      }),
    ),
  );
  const engine = yield* RpcServer.makeNoSerialization(ClientRpcs, {
    disableTracing: true,
    disableSpanPropagation: true,
    disableClientAcks: true,
    disableFatalDefects: true,
    onFromServer: (response) =>
      Effect.suspend(() => {
        const state = states.get(response.clientId);
        if (!state || response._tag !== "Exit") return Effect.void;
        const message = Exit.isSuccess(response.exit)
          ? {
              jsonrpc: "2.0",
              id: state.id,
              result: modernVersion(state.protocolVersion)
                ? {
                    ...(response.exit.value as object),
                    resultType: "complete",
                    ...([
                      "tools/list",
                      "resources/list",
                      "resources/templates/list",
                      "prompts/list",
                    ].includes(state.method)
                      ? { ttlMs: 0, cacheScope: "private" }
                      : {}),
                    _meta: { "io.modelcontextprotocol/serverInfo": options },
                  }
                : response.exit.value,
            }
          : {
              jsonrpc: "2.0",
              id: state.id,
              error: wireError(
                Option.getOrUndefined(Cause.failureOption(response.exit.cause)),
              ),
            };
        return Queue.offer(state.notifications, message).pipe(Effect.asVoid);
      }),
  }).pipe(
    Effect.provide(handlers),
    Effect.provideService(McpServerClientMiddleware, middleware),
  );

  const mcpServer = yield* McpServer;
  yield* Queue.take(mcpServer.toolChanges).pipe(
    Effect.flatMap(() =>
      Effect.forEach(
        Array.from(states.values()).filter(
          (state) =>
            state.method === "events/stream" ||
            state.filter?.toolsListChanged === true,
        ),
        (state) =>
          Queue.offer(state.notifications, {
            jsonrpc: "2.0",
            method: "notifications/tools/list_changed",
            params: {
              _meta: { "io.modelcontextprotocol/subscriptionId": state.id },
            },
          }),
        { discard: true },
      ),
    ),
    Effect.forever,
    Effect.forkScoped,
  );

  const app: HttpApp.Default<never, Scope.Scope> = Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const text = yield* request.text.pipe(
      Effect.catchAll(() => Effect.succeed("")),
    );
    const parsed = yield* Schema.decodeUnknown(
      Schema.parseJson(Schema.Unknown),
    )(text).pipe(Effect.either);
    if (parsed._tag === "Left") {
      return yield* HttpServerResponse.json(
        {
          jsonrpc: "2.0",
          id: null,
          error: { code: -32700, message: "Parse error" },
        },
        { status: 400 },
      ).pipe(Effect.orDie);
    }
    const decoded = yield* Schema.decodeUnknown(requestEnvelope)(
      parsed.right,
    ).pipe(Effect.either);
    if (decoded._tag === "Left") {
      return yield* HttpServerResponse.json(
        {
          jsonrpc: "2.0",
          id: null,
          error: { code: -32600, message: "Invalid request" },
        },
        { status: 400 },
      ).pipe(Effect.orDie);
    }
    const envelope = decoded.right;
    if (envelope.id === undefined) {
      if (envelope.method === "notifications/cancelled") {
        const cancellation = Schema.decodeUnknownOption(
          Schema.Struct({
            requestId: Schema.Union(Schema.String, Schema.Finite),
          }),
        )(envelope.params);
        if (Option.isSome(cancellation)) {
          const matching = Array.from(states.entries()).filter(
            ([, state]) => state.id === cancellation.value.requestId,
          );
          if (matching.length === 1) yield* engine.disconnect(matching[0]![0]);
        }
      }
      return HttpServerResponse.empty({ status: 202 });
    }
    const id = ++nextClient;
    const rpc = ClientRpcs.requests.get(envelope.method);
    if (!rpc)
      return yield* HttpServerResponse.json({
        jsonrpc: "2.0",
        id: envelope.id,
        error: { code: -32601, message: "Method not found" },
      }).pipe(Effect.orDie);
    const params = yield* Schema.decodeUnknown(
      rpc.payloadSchema as Schema.Schema<unknown, unknown>,
    )(envelope.params ?? {}).pipe(Effect.either);
    if (params._tag === "Left")
      return yield* HttpServerResponse.json({
        jsonrpc: "2.0",
        id: envelope.id,
        error: { code: -32602, message: "Invalid params" },
      }).pipe(Effect.orDie);
    const meta =
      envelope.params &&
      typeof envelope.params === "object" &&
      "_meta" in envelope.params
        ? (envelope.params._meta as Record<string, unknown>)
        : undefined;
    const version =
      request.headers["mcp-protocol-version"] ??
      meta?.["io.modelcontextprotocol/protocolVersion"] ??
      (envelope.method === "initialize" &&
      envelope.params &&
      typeof envelope.params === "object" &&
      "protocolVersion" in envelope.params
        ? envelope.params.protocolVersion
        : "2025-11-25");
    if (
      version === "2026-07-28" &&
      envelope.method !== "initialize" &&
      (!meta ||
        typeof meta["io.modelcontextprotocol/clientCapabilities"] !==
          "object" ||
        meta["io.modelcontextprotocol/clientCapabilities"] === null)
    ) {
      return yield* HttpServerResponse.json({
        jsonrpc: "2.0",
        id: envelope.id,
        error: { code: -32602, message: "Invalid MCP request metadata" },
      }).pipe(Effect.orDie);
    }
    const state: HttpRequestState = {
      id: envelope.id,
      method: envelope.method,
      protocolVersion: typeof version === "string" ? version : "2025-11-25",
      ...(envelope.method === "subscriptions/listen"
        ? {
            filter: (
              params.right as { notifications: typeof SubscriptionFilter.Type }
            ).notifications,
          }
        : {}),
      notifications: yield* Queue.unbounded<unknown>(),
    };
    states.set(id, state);
    yield* Effect.addFinalizer(() =>
      engine
        .disconnect(id)
        .pipe(Effect.zipRight(Effect.sync(() => states.delete(id)))),
    );
    yield* engine.write(id, {
      _tag: "Request",
      id: RpcMessageValues.RequestId(BigInt(id)),
      tag: envelope.method,
      payload: params.right,
      headers: Headers.fromInput(request.headers),
      context: Context.empty(),
    } as any);
    if (
      envelope.method !== "events/stream" &&
      envelope.method !== "subscriptions/listen"
    ) {
      const result = yield* Queue.take(state.notifications);
      return yield* HttpServerResponse.json(result, {
        headers: { "mcp-protocol-version": state.protocolVersion },
      }).pipe(Effect.orDie);
    }
    const events = StreamValues.fromQueue(state.notifications).pipe(
      StreamValues.map(
        (message) => `event: message\ndata: ${JSON.stringify(message)}\n\n`,
      ),
      StreamValues.encodeText,
    );
    return HttpServerResponse.stream(events, {
      contentType: "text/event-stream",
      headers: {
        "cache-control": "no-cache",
        "mcp-protocol-version": state.protocolVersion,
      },
    });
  }).pipe(
    Effect.interruptible,
    Effect.catchAllCause(() =>
      HttpServerResponse.json({
        jsonrpc: "2.0",
        id: null,
        error: { code: -32603, message: "Internal error" },
      }).pipe(Effect.orDie),
    ),
  );
  return app;
});
