/** @file Effect HTTP resources bind the MCP application to literal loopback. */
import type { HttpApp } from "@effect/platform";
import { NodeHttpServer } from "@effect/platform-node";
import { Effect, type Scope } from "effect";
import { createServer } from "node:http";

/** The listener and advertised MCP URL use the same literal loopback address. */
export const LOOPBACK_HOST = "127.0.0.1";

/** Returns after listening. Scope closure disconnects streaming requests before the listener closes. */
export const serveMcp = Effect.fn("Mcp.serve")(function* (
  port: number,
  app: HttpApp.Default<never, Scope.Scope>,
) {
  const listener = yield* Effect.sync(createServer);
  const server = yield* NodeHttpServer.make(() => listener, {
    host: LOOPBACK_HOST,
    port,
  });
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      listener.closeAllConnections();
    }),
  );
  yield* server.serve(app);
});
