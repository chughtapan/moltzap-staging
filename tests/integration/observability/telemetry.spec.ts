/** @file A real daemon exports its traces over OTLP/HTTP protobuf only when it is given an endpoint. */

import {
  HttpServer,
  HttpServerRequest,
  HttpServerResponse,
} from "@effect/platform";
import { NodeHttpServer } from "@effect/platform-node";
import { Context, Duration, Effect, Layer, Ref } from "effect";
import { Buffer } from "node:buffer";
import { expect, it } from "vitest";
import { LOOPBACK_HOST, sharedDaemon } from "../daemon/daemon-fixture.js";
import { processTrace } from "../scenario-fixture.js";

/** The standard batch delay, shortened from 5 s so spans leave the daemon sooner. */
const BATCH_DELAY = Duration.millis(100);

/** How long a daemon without an endpoint must stay silent after the exporting daemon has exported. */
const SILENCE_WINDOW = Duration.times(BATCH_DELAY, 20);

/** One request a recorder received. `body` holds its bytes, one character each. */
interface RecordedRequest {
  readonly method: string;
  readonly path: string;
  readonly contentType: string;
  readonly body: string;
}

/** An HTTP server on a port of its own that accepts and records every request. */
const recorder = Effect.gen(function* () {
  const requests = yield* Ref.make<readonly RecordedRequest[]>([]);
  const server = Context.get(
    yield* Layer.build(NodeHttpServer.layerTest),
    HttpServer.HttpServer,
  );
  if (server.address._tag !== "TcpAddress") {
    return yield* Effect.dieMessage("Expected a TCP listener");
  }
  yield* server.serve(
    Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest;
      const body = Buffer.from(yield* request.arrayBuffer).toString("latin1");
      yield* Ref.update(requests, (received) => [
        ...received,
        {
          method: request.method,
          path: request.url,
          contentType: request.headers["content-type"] ?? "",
          body,
        },
      ]);
      return HttpServerResponse.empty();
    }).pipe(Effect.orDie),
  );
  return {
    origin: `http://${LOOPBACK_HOST}:${server.address.port}`,
    requests: Ref.get(requests),
  };
});

it(
  "posts protobuf traces naming its service and resource to /v1/traces under its OTLP endpoint, and sends nothing without one",
  processTrace(
    Effect.gen(function* () {
      const collector = yield* recorder;
      const unnamed = yield* recorder;
      const silent = yield* sharedDaemon();
      yield* silent.start({
        OTEL_EXPORTER_OTLP_ENDPOINT: "",
        OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: `${unnamed.origin}/v1/traces`,
        OTEL_BSP_SCHEDULE_DELAY: String(Duration.toMillis(BATCH_DELAY)),
      });
      const exporting = yield* sharedDaemon();
      yield* exporting.start({
        OTEL_EXPORTER_OTLP_ENDPOINT: collector.origin,
        OTEL_RESOURCE_ATTRIBUTES: "deployment=exporting",
        OTEL_BSP_SCHEDULE_DELAY: String(Duration.toMillis(BATCH_DELAY)),
      });

      const [exported] = yield* collector.requests.pipe(
        Effect.delay(BATCH_DELAY),
        Effect.repeat({ until: (received) => received.length > 0 }),
      );
      yield* Effect.sleep(SILENCE_WINDOW);

      expect(exported).toMatchObject({
        method: "POST",
        path: "/v1/traces",
        contentType: "application/x-protobuf",
      });
      expect(exported?.body).toContain("sharnessd");
      expect(exported?.body).toContain("deployment");
      expect(exported?.body).toContain("exporting");
      expect(yield* unnamed.requests).toEqual([]);
    }),
  ),
  120_000,
);
