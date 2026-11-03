/** @file Test-only Router proxy that parks delivery-bearing poll responses. */

import type { Scope } from "effect/Scope";
import {
  HttpBody,
  HttpClient,
  HttpClientRequest,
  HttpMethod,
} from "@effect/platform";
import { NodeHttpClient } from "@effect/platform-node";
import { Deferred, Effect, Option, Ref, Schema } from "effect";
import { Buffer } from "node:buffer";
import { LOOPBACK_HOST, ProcessTestError } from "../daemon/daemon-fixture.js";
import {
  acquireLoopbackServer,
  type IncomingMessage,
  readBody,
  type ServerResponse,
} from "../loopback-server.js";

/** Scoped control over one endpoint's Router link. */
export interface RouterHoldProxy {
  /** Origin the held endpoint daemon uses as its Router. */
  readonly origin: URL;
  /** Parks every later delivery-bearing poll response until `release`. */
  readonly hold: Effect.Effect<void>;
  /** Writes every parked response verbatim and stops parking. */
  readonly release: Effect.Effect<void>;
  /** Count of delivery-bearing poll responses parked since acquisition. */
  readonly parkedResponses: Effect.Effect<number>;
}

interface UpstreamResponse {
  readonly status: number;
  readonly headers: IncomingMessage["headers"];
  readonly body: Buffer;
}

interface HoldState {
  readonly gate: Option.Option<Deferred.Deferred<undefined>>;
  readonly parkedResponses: number;
}

const ROUTER_POLL_PATH = "/v1/messages:poll";

const HOP_BY_HOP_HEADERS = [
  "connection",
  "keep-alive",
  "proxy-connection",
  "transfer-encoding",
  "trailer",
  "upgrade",
] as const;

/**
 * A poll batch the Router has already ordered for the polling endpoint. Only
 * the fields that decide parking are read; the forwarded bytes stay verbatim.
 */
const deliveryBatch = Schema.parseJson(
  Schema.Struct({
    kind: Schema.Literal("batch"),
    signedMessages: Schema.NonEmptyArray(Schema.Unknown),
  }),
);

const withoutHopByHop = (
  headers: IncomingMessage["headers"],
): IncomingMessage["headers"] =>
  Object.fromEntries(
    Object.entries(headers).filter(
      ([name]) => !HOP_BY_HOP_HEADERS.some((hop) => hop === name),
    ),
  );

/** Preserve the signed Host header while the platform client owns cancellation and response scope. */
const forward = (upstream: URL, incoming: IncomingMessage, body: Buffer) =>
  Effect.gen(function* () {
    const method = incoming.method;
    if (!HttpMethod.isHttpMethod(method)) {
      return yield* Effect.fail(
        new ProcessTestError({
          message: "proxy received an unsupported HTTP method",
        }),
      );
    }
    const path = incoming.url;
    if (path === undefined || !path.startsWith("/") || path.startsWith("//")) {
      return yield* Effect.fail(
        new ProcessTestError({
          message: "proxy received a request target outside the Router origin",
        }),
      );
    }
    const contentType = incoming.headers["content-type"];
    const request = HttpClientRequest.make(method)(
      new URL(`${upstream.origin}${path}`),
    ).pipe(
      HttpClientRequest.setBody(
        contentType === undefined
          ? HttpBody.empty
          : HttpBody.uint8Array(body, contentType),
      ),
      HttpClientRequest.setHeaders(incoming.headers),
    );
    const response = yield* HttpClient.execute(request);
    return {
      status: response.status,
      headers: response.headers,
      body: Buffer.from(yield* response.arrayBuffer),
    };
  }).pipe(
    Effect.locally(HttpClient.currentTracerPropagation, false),
    Effect.scoped,
    Effect.provide(NodeHttpClient.layer),
  );

const carriesDeliveries = (
  incoming: IncomingMessage,
  upstream: UpstreamResponse,
): boolean =>
  incoming.method === "POST" &&
  incoming.url === ROUTER_POLL_PATH &&
  upstream.status === 200 &&
  Option.isSome(
    Schema.decodeUnknownOption(deliveryBatch)(upstream.body.toString("utf8")),
  );

const parkIfHeld = (state: Ref.Ref<HoldState>): Effect.Effect<void> =>
  Ref.modify(state, (current): readonly [HoldState["gate"], HoldState] =>
    Option.isSome(current.gate)
      ? [
          current.gate,
          { ...current, parkedResponses: current.parkedResponses + 1 },
        ]
      : [Option.none(), current],
  ).pipe(
    Effect.flatMap((gate) =>
      Option.isSome(gate) ? Deferred.await(gate.value) : Effect.void,
    ),
  );

const writeResponse = (
  response: ServerResponse,
  upstream: UpstreamResponse,
): void => {
  if (response.destroyed) {
    return;
  }
  response.writeHead(upstream.status, {
    ...withoutHopByHop(upstream.headers),
    "content-length": String(upstream.body.byteLength),
  });
  response.end(upstream.body);
};

const failResponse = (response: ServerResponse): void => {
  if (response.destroyed) {
    return;
  }
  if (!response.headersSent) {
    response.writeHead(502, { "content-length": "0" });
  }
  response.end();
};

const handle = (
  upstream: URL,
  state: Ref.Ref<HoldState>,
  incoming: IncomingMessage,
  response: ServerResponse,
) =>
  Effect.gen(function* () {
    const body = yield* readBody(incoming);
    const answer = yield* forward(upstream, incoming, body);
    if (carriesDeliveries(incoming, answer)) {
      yield* parkIfHeld(state);
    }
    writeResponse(response, answer);
  }).pipe(
    Effect.tapErrorCause((cause) =>
      Effect.logWarning("router hold proxy answered 502", cause),
    ),
    Effect.catchAll(() =>
      Effect.sync(() => {
        failResponse(response);
      }),
    ),
  );

/**
 * Starts a loopback reverse proxy in front of the real Router. Responses are
 * parked only after the Router has answered, so a parked poll proves the
 * message is already ordered and awaiting delivery. Handlers are interrupted
 * and the listener is closed when the scope ends.
 */
export const acquireRouterHoldProxy = (
  upstream: URL,
): Effect.Effect<RouterHoldProxy, ProcessTestError, Scope> =>
  Effect.gen(function* () {
    const state = yield* Ref.make<HoldState>({
      gate: Option.none(),
      parkedResponses: 0,
    });
    const { port } = yield* acquireLoopbackServer((incoming, response) =>
      handle(upstream, state, incoming, response),
    );
    const hold = Deferred.make<undefined>().pipe(
      Effect.flatMap((gate) =>
        Ref.update(state, (current) => ({
          ...current,
          gate: Option.some(gate),
        })),
      ),
    );
    const release = Ref.modify(state, (current) => [
      current.gate,
      { ...current, gate: Option.none() },
    ]).pipe(
      Effect.flatMap(
        Option.match({
          onNone: () => Effect.void,
          onSome: (gate) => Deferred.succeed(gate, undefined),
        }),
      ),
      Effect.asVoid,
    );
    return {
      origin: new URL(`http://${LOOPBACK_HOST}:${port}`),
      hold,
      release,
      parkedResponses: Ref.get(state).pipe(
        Effect.map(({ parkedResponses }) => parkedResponses),
      ),
    };
  }).pipe(Effect.withSpan("acquireRouterHoldProxy"));
