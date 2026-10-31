/** @file Test-only loopback HTTP servers that stand in front of, or in place of, a real service. */

import { NodeHttpServer } from "@effect/platform-node";
import { Effect, FiberSet } from "effect";
import { Buffer } from "node:buffer";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- NodeHttpServer.make requires Node listener factories and callback types.
import {
  createServer,
  type IncomingMessage,
  type ServerResponse,
} from "node:http";
import {
  LOOPBACK_HOST,
  ProcessTestError,
} from "./daemon/daemon-process-harness.js";

/** Callback types for handlers served by the loopback Node listener. */
export type { IncomingMessage, ServerResponse };

/**
 * Reads one request body in full.
 * @param incoming The request to read.
 * @returns The body's bytes.
 */
export const readBody = (incoming: IncomingMessage) =>
  Effect.async<Buffer, ProcessTestError>((resume) => {
    const chunks: Buffer[] = [];
    incoming.on("data", (chunk: Buffer) => chunks.push(chunk));
    incoming.once("end", () => {
      resume(Effect.succeed(Buffer.concat(chunks)));
    });
    incoming.once("error", (cause) => {
      resume(
        Effect.fail(
          new ProcessTestError({ message: "loopback request failed", cause }),
        ),
      );
    });
  });

/**
 * Starts a loopback HTTP server on a free port that runs `handle` for each
 * request. The scope interrupts the running handlers and closes the server.
 * Active connections close before the listener finalizer waits, so unfinished
 * requests cannot hold teardown open.
 * @param handle Answers one request.
 * @returns The listening server and its port.
 */
export const acquireLoopbackServer = (
  handle: (
    incoming: IncomingMessage,
    response: ServerResponse,
  ) => Effect.Effect<void>,
) =>
  Effect.gen(function* () {
    const run = yield* FiberSet.makeRuntime();
    const server = createServer((incoming, response) => {
      run(handle(incoming, response));
    });
    const listener = yield* NodeHttpServer.make(() => server, {
      host: LOOPBACK_HOST,
      port: 0,
    }).pipe(
      Effect.catchTag("ServeError", (cause) =>
        Effect.fail(
          new ProcessTestError({
            message: "loopback server could not listen",
            cause,
          }),
        ),
      ),
    );
    const address = listener.address;
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        server.closeAllConnections();
      }),
    );
    if (address._tag !== "TcpAddress") {
      return yield* Effect.fail(
        new ProcessTestError({
          message: "loopback server exposed no TCP port",
        }),
      );
    }
    const port = address.port;
    return { server, port };
  }).pipe(Effect.withSpan("acquireLoopbackServer"));
