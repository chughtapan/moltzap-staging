/** @file Disposable OTLP endpoint for Client startup configuration checks. */
import { NodeHttpServer } from "@effect/platform-node";
import { Effect } from "effect";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- NodeHttpServer.make requires a Node listener factory.
import { createServer } from "node:http";

const LOOPBACK_HOST = "127.0.0.1";

/** Startup tests need a reachable collector without sharing cross-package fixtures. */
export const acquireOtlpReceiver = Effect.gen(function* () {
  const server = yield* NodeHttpServer.make(
    () =>
      createServer((request, response) => {
        request.resume();
        response.writeHead(200).end();
      }),
    { host: LOOPBACK_HOST, port: 0 },
  );
  const address = server.address;
  if (address._tag !== "TcpAddress") {
    return yield* Effect.dieMessage("OTLP receiver exposed no TCP port");
  }
  return { endpoint: new URL(`http://${LOOPBACK_HOST}:${address.port}`) };
}).pipe(Effect.withSpan("acquireOtlpReceiver"));
