/**
 * @file Hosts one official MCP handler on a guarded Node listener and
 * binds request draining and connection teardown to the enclosing scope.
 */

import type { McpHttpHandler } from "@modelcontextprotocol/server";
import type { Socket } from "node:net";
import {
  type FetchLikeMcpHandler,
  localhostHostValidation,
  localhostOriginValidation,
  type NodeMcpRequestHandler,
  toNodeHandler,
} from "@modelcontextprotocol/node";
import {
  Deferred,
  Duration,
  Effect,
  Exit,
  Option,
  Schema,
  type Scope,
} from "effect";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- The official MCP Node adapter supplies a Node RequestListener, so this process edge hosts that exact interface.
import {
  createServer,
  type Server as NodeHttpServer,
  type RequestListener,
} from "node:http";
import { type DaemonOAuth, protectedResourcePath } from "./authentication.js";

const HARNESS_MCP_PATH = "/mcp";
const POST_METHOD = "POST";
/** Default listener address for local bearer deployments. */
export const LOOPBACK_HOST = "127.0.0.1";
/** Maximum public request representation, counted before SDK body buffering. */
export const maximumPublicMcpRequestBytes = 1024 * 1024;
const RESPONSE_DRAIN_GRACE_PERIOD = Duration.seconds(1);

interface HarnessMcpHttpServerOptions {
  readonly port: number;
  readonly handler: McpHttpHandler;
  readonly host?: string;
  readonly oauth?: DaemonOAuth;
}

interface HarnessMcpRequestListener {
  readonly listener: RequestListener;
  readonly waitForResponses: Effect.Effect<void>;
}

interface RunningHarnessMcpHttpServer {
  readonly server: NodeHttpServer;
  readonly destroyConnections: () => void;
  readonly waitForResponses: Effect.Effect<void>;
}

interface ResponseTracker {
  readonly track: (
    response: ReturnType<NodeMcpRequestHandler>,
    nodeResponse: Parameters<RequestListener>[1],
  ) => void;
  readonly waitForResponses: Effect.Effect<void>;
}

const respond = (
  status: number,
  body: string,
  response: Parameters<RequestListener>[1],
  headers: Readonly<Record<string, string>> = {},
): void => {
  response.writeHead(status, {
    "content-type": "text/plain; charset=utf-8",
    ...headers,
  });
  response.end(body);
};

const makeResponseTracker = (): ResponseTracker => {
  const inFlight = new Set<ReturnType<NodeMcpRequestHandler>>();
  const responseWaiters = new Set<() => void>();
  const finish = (response: ReturnType<NodeMcpRequestHandler>): void => {
    inFlight.delete(response);
    if (inFlight.size !== 0) {
      return;
    }
    for (const resume of responseWaiters) {
      resume();
    }
    responseWaiters.clear();
  };

  return {
    track: (response, nodeResponse) => {
      inFlight.add(response);
      Effect.runFork(
        Effect.tryPromise({
          try: () => response,
          catch: (cause) => cause,
        }).pipe(
          Effect.catchAll((cause) =>
            Effect.sync(() => {
              nodeResponse.destroy(cause instanceof Error ? cause : undefined);
            }),
          ),
          Effect.ensuring(
            Effect.sync(() => {
              finish(response);
            }),
          ),
        ),
      );
    },
    waitForResponses: Effect.async((resume) => {
      if (inFlight.size === 0) {
        resume(Effect.void);
        return;
      }
      const done = (): void => {
        resume(Effect.void);
      };
      responseWaiters.add(done);
      return Effect.sync(() => {
        responseWaiters.delete(done);
      });
    }),
  };
};

const makeHarnessMcpRequestListener = (
  handler: FetchLikeMcpHandler,
  oauth?: DaemonOAuth,
): HarnessMcpRequestListener => {
  const validateHost = localhostHostValidation();
  const validateOrigin = localhostOriginValidation();
  const nodeHandler =
    oauth === undefined ? toNodeHandler(handler) : boundedNodeHandler(handler);
  const responses = makeResponseTracker();

  const listener: RequestListener = (request, response): void => {
    if (
      !admitTransport(request, response, {
        oauth,
        validateHost,
        validateOrigin,
      })
    ) {
      return;
    }
    const pathname = requestPath(request);
    if (pathname === undefined) {
      respond(400, "Bad request target.", response);
      return;
    }
    const metadata =
      oauth !== undefined && pathname === protectedResourcePath(oauth);
    if (pathname !== HARNESS_MCP_PATH && !metadata) {
      respond(404, "Not found.", response);
      return;
    }
    const method = metadata ? "GET" : POST_METHOD;
    if (request.method !== method) {
      respond(405, "Method not allowed.", response, {
        allow: method,
      });
      return;
    }
    responses.track(nodeHandler(request, response), response);
  };

  return { listener, waitForResponses: responses.waitForResponses };
};

function requestPath(
  request: Parameters<RequestListener>[0],
): string | undefined {
  const target = request.url ?? "/";
  return URL.canParse(target, "http://localhost")
    ? new URL(target, "http://localhost").pathname
    : undefined;
}

/* eslint-disable agent-code-guard/async-keyword -- The SDK's Node request boundary is an async iterable and a Promise-returning listener. */
function boundedNodeHandler(handler: FetchLikeMcpHandler) {
  return (
    request: Parameters<RequestListener>[0],
    response: Parameters<RequestListener>[1],
  ) => {
    let overflow = false;
    const input = {
      method: request.method,
      url: request.url,
      headers: request.headers,
      async *[Symbol.asyncIterator]() {
        let bytes = 0;
        for await (const chunk of request.iterator({
          destroyOnReturn: false,
        })) {
          const data = Schema.decodeUnknownSync(Schema.Uint8ArrayFromSelf)(
            chunk,
          );
          bytes += data.byteLength;
          if (bytes > maximumPublicMcpRequestBytes) {
            overflow = true;
            return;
          }
          yield data;
        }
      },
    };
    return toNodeHandler({
      fetch: (webRequest, options) =>
        overflow
          ? Promise.resolve(
              new Response("Request body too large.", {
                status: 413,
                headers: { connection: "close" },
              }),
            )
          : handler.fetch(webRequest, options),
    })(input, response);
  };
}
/* eslint-enable agent-code-guard/async-keyword -- Restore repository defaults beyond the SDK boundary. */

function admitTransport(
  request: Parameters<RequestListener>[0],
  response: Parameters<RequestListener>[1],
  local: {
    readonly oauth?: DaemonOAuth;
    readonly validateHost: ReturnType<typeof localhostHostValidation>;
    readonly validateOrigin: ReturnType<typeof localhostOriginValidation>;
  },
): boolean {
  const { oauth } = local;
  if (oauth === undefined) {
    return (
      local.validateHost(request, response) &&
      local.validateOrigin(request, response)
    );
  }
  if (
    request.headers.host !== oauth.resource.host ||
    (request.headers.origin !== undefined &&
      !oauth.allowedOrigins.includes(request.headers.origin))
  ) {
    respond(403, "Forbidden", response);
    return false;
  }
  return true;
}

const trackConnections = (server: NodeHttpServer): (() => void) => {
  const sockets = new Set<Socket>();
  server.on("connection", (socket) => {
    sockets.add(socket);
    socket.once("close", () => sockets.delete(socket));
  });
  return () => {
    for (const socket of sockets) {
      socket.destroy();
    }
    server.closeAllConnections();
  };
};

const listen = (
  options: HarnessMcpHttpServerOptions,
): Effect.Effect<RunningHarnessMcpHttpServer, Error> =>
  Effect.async<RunningHarnessMcpHttpServer, Error>((resume) => {
    const requests = makeHarnessMcpRequestListener(
      options.handler,
      options.oauth,
    );
    const server = createServer(requests.listener);
    const destroyConnections = trackConnections(server);
    const onError = (error: Error): void => {
      resume(Effect.fail(error));
    };
    server.once("error", onError);
    server.listen(options.port, options.host ?? LOOPBACK_HOST, () => {
      server.off("error", onError);
      resume(
        Effect.succeed({
          server,
          destroyConnections,
          waitForResponses: requests.waitForResponses,
        }),
      );
    });
    return Effect.async((cancelResume) => {
      server.off("error", onError);
      const onClose = (): void => {
        cancelResume(Effect.void);
      };
      server.once("close", onClose);
      try {
        server.close();
      } catch (cause) {
        server.off("close", onClose);
        Effect.runSync(
          Effect.logWarning("MCP startup cancellation failed", cause),
        );
        cancelResume(Effect.void);
      }
    });
  });

const beginClose = (
  server: NodeHttpServer,
): Effect.Effect<Effect.Effect<void>> =>
  Effect.gen(function* () {
    const completion = yield* Deferred.make<Error | undefined>();
    yield* Effect.sync(() => {
      server.close((error) => {
        Effect.runSync(Deferred.succeed(completion, error));
      });
    });
    return Deferred.await(completion).pipe(
      Effect.flatMap((error) =>
        error === undefined ? Effect.void : Effect.logWarning(error),
      ),
    );
  });

const closeHandler = (handler: McpHttpHandler): Effect.Effect<void> =>
  Effect.tryPromise({
    try: () => handler.close(),
    catch: (cause) => cause,
  }).pipe(Effect.ignore);

/** The handler stays open through the bounded drain so a send can finish its typed reply. */
const release = (
  running: RunningHarnessMcpHttpServer,
  options: HarnessMcpHttpServerOptions,
): Effect.Effect<void> =>
  Effect.gen(function* () {
    const closeCompletion = yield* beginClose(running.server);
    const drained = yield* running.waitForResponses.pipe(
      Effect.timeoutOption(RESPONSE_DRAIN_GRACE_PERIOD),
      Effect.interruptible,
    );
    if (Option.isNone(drained)) {
      running.destroyConnections();
    } else {
      running.server.closeAllConnections();
    }
    yield* closeHandler(options.handler);
    yield* running.waitForResponses;
    yield* closeCompletion;
  });

/**
 * Acquire one guarded `/mcp` server on the explicit configured port.
 * @param options Official MCP handler, listener address and nonzero port.
 * @returns The live Node server bound to the caller's scope.
 */
export const acquireHarnessMcpHttpServer = (
  options: HarnessMcpHttpServerOptions,
): Effect.Effect<NodeHttpServer, Error, Scope.Scope> =>
  // eslint-disable-next-line agent-code-guard/acquire-release-requires-scope -- The returned Scope requirement owns listener and response teardown.
  Effect.acquireRelease(
    listen(options).pipe(
      Effect.onExit((exit) =>
        Exit.isSuccess(exit) ? Effect.void : closeHandler(options.handler),
      ),
    ),
    (running) => release(running, options),
  ).pipe(Effect.map((running) => running.server));
