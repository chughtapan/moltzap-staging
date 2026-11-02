/** @file Listener startup reports conflicts and shutdown closes active streaming connections. */
import { HttpServerResponse } from "@effect/platform";
import { NodeHttpServer } from "@effect/platform-node";
import { scopedLive as it } from "@effect/vitest";
import { Effect, Exit, Scope, Stream } from "effect";
import { createServer } from "node:http";
import { expect } from "vitest";
import { serveMcp } from "./http.js";

const boundPort = Effect.gen(function* () {
  const server = yield* NodeHttpServer.make(createServer, {
    host: "127.0.0.1",
    port: 0,
  });
  if (server.address._tag !== "TcpAddress") {
    return yield* Effect.dieMessage("Expected TCP listener");
  }
  return server.address.port;
});

it("reports an occupied port before declaring the listener ready", () =>
  Effect.gen(function* () {
    const port = yield* boundPort;
    expect(
      yield* serveMcp(port, Effect.succeed(HttpServerResponse.empty())).pipe(
        Effect.flip,
      ),
    ).toMatchObject({ _tag: "ServeError" });
  }));

it("closes an active event stream with the listener's scope", () =>
  Effect.gen(function* () {
    const port = yield* Effect.scoped(boundPort);
    const scope = yield* Scope.make();
    const app = Effect.succeed(
      HttpServerResponse.stream(
        Stream.concat(Stream.make("active"), Stream.never).pipe(
          Stream.encodeText,
        ),
      ),
    );
    yield* serveMcp(port, app).pipe(Effect.provideService(Scope.Scope, scope));
    const abort = new AbortController();
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        abort.abort();
      }),
    );
    const url = `http://127.0.0.1:${port}`;
    const response = yield* Effect.tryPromise(() =>
      fetch(url, { signal: abort.signal }),
    );
    expect(response.status).toBe(200);
    yield* Scope.close(scope, Exit.void).pipe(Effect.timeout("2 seconds"));
    expect(
      (yield* Effect.tryPromise(() => fetch(url)).pipe(Effect.either))._tag,
    ).toBe("Left");
  }));
