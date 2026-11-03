/** @file Test-only OTLP/HTTP receiver that decodes the protobuf trace exports daemons send it. */

import type { Buffer } from "node:buffer";
import { Cause, Duration, Effect, Ref, Schema, type Scope } from "effect";
import protobuf from "protobufjs";
import { LOOPBACK_HOST, ProcessTestError } from "../daemon/daemon-fixture.js";
import {
  acquireLoopbackServer,
  type IncomingMessage,
  readBody,
  type ServerResponse,
} from "../loopback-server.js";

/** A scalar attribute value as OTLP carries it. */
type AttributeValue = string | number | boolean;

/** One exported span: its name, start, attributes and its resource's attributes. */
export interface ExportedSpan {
  readonly name: string;
  readonly startTimeUnixNano: bigint;
  readonly attributes: Readonly<Record<string, AttributeValue>>;
  readonly resource: Readonly<Record<string, AttributeValue>>;
}

/** A running receiver and everything daemons have exported to it. */
export interface OtlpReceiver {
  /** The value for `OTEL_EXPORTER_OTLP_ENDPOINT`; traces post to `/v1/traces` under it. */
  readonly endpoint: URL;
  /** TCP connections accepted since the receiver started. */
  readonly connections: Effect.Effect<number>;
  /** Every span received, in arrival order. */
  readonly spans: Effect.Effect<readonly ExportedSpan[]>;
  /** Each request the receiver refused, as its method, path and content type, or why it failed to decode. */
  readonly rejected: Effect.Effect<readonly string[]>;
}

const TRACES_PATH = "/v1/traces";

const PROTOBUF_CONTENT_TYPE = "application/x-protobuf";

const SPAN_POLL_INTERVAL = Duration.millis(100);

/**
 * How long a wait for spans lasts before it fails naming what did arrive,
 * well inside every test's own limit.
 */
const SPAN_WAIT_LIMIT = Duration.minutes(2);

/**
 * The fields of opentelemetry-proto's `ExportTraceServiceRequest` the tests
 * read, with their wire field numbers. Decoding matches fields by number and
 * skips every other field, so the names are this file's own: a `KeyValue`'s
 * `key` is `name` here.
 */
const exportTraceServiceRequest = protobuf.Root.fromJSON({
  nested: {
    ExportTraceServiceRequest: {
      fields: {
        resourceSpans: { rule: "repeated", type: "ResourceSpans", id: 1 },
      },
    },
    ResourceSpans: {
      fields: {
        resource: { type: "Resource", id: 1 },
        scopeSpans: { rule: "repeated", type: "ScopeSpans", id: 2 },
      },
    },
    Resource: {
      fields: { attributes: { rule: "repeated", type: "KeyValue", id: 1 } },
    },
    ScopeSpans: {
      fields: { spans: { rule: "repeated", type: "Span", id: 2 } },
    },
    Span: {
      fields: {
        name: { type: "string", id: 5 },
        startTimeUnixNano: { type: "fixed64", id: 7 },
        attributes: { rule: "repeated", type: "KeyValue", id: 9 },
      },
    },
    KeyValue: {
      fields: {
        name: { type: "string", id: 1 },
        value: { type: "AnyValue", id: 2 },
      },
    },
    AnyValue: {
      fields: {
        stringValue: { type: "string", id: 1 },
        boolValue: { type: "bool", id: 2 },
        intValue: { type: "int64", id: 3 },
        doubleValue: { type: "double", id: 4 },
      },
    },
  },
}).lookupType("ExportTraceServiceRequest");

const anyValueSchema = Schema.Struct({
  stringValue: Schema.optional(Schema.String),
  boolValue: Schema.optional(Schema.Boolean),
  intValue: Schema.optional(Schema.NumberFromString),
  doubleValue: Schema.optional(Schema.Number),
});

const keyValueSchema = Schema.Struct({
  name: Schema.String,
  value: Schema.optional(anyValueSchema),
});

const spanSchema = Schema.Struct({
  name: Schema.String,
  startTimeUnixNano: Schema.BigInt,
  attributes: Schema.Array(keyValueSchema),
});

const resourceSpansSchema = Schema.Struct({
  resource: Schema.optional(
    Schema.Struct({ attributes: Schema.Array(keyValueSchema) }),
  ),
  scopeSpans: Schema.Array(Schema.Struct({ spans: Schema.Array(spanSchema) })),
});

/** The decoded request, with 64-bit integers as decimal strings. */
const decodedRequestSchema = Schema.Struct({
  resourceSpans: Schema.Array(resourceSpansSchema),
});

/**
 * Accepts only protobuf trace exports, as the evals controller does, and
 * answers each with an empty `ExportTraceServiceResponse`.
 */
const handle = (
  spans: Ref.Ref<readonly ExportedSpan[]>,
  rejected: Ref.Ref<readonly string[]>,
  incoming: IncomingMessage,
  response: ServerResponse,
) =>
  readBody(incoming).pipe(
    Effect.flatMap((body) => {
      if (
        incoming.method !== "POST" ||
        incoming.url !== TRACES_PATH ||
        incoming.headers["content-type"] !== PROTOBUF_CONTENT_TYPE
      ) {
        const request = `${incoming.method} ${incoming.url} ${incoming.headers["content-type"]}`;
        return Ref.update(rejected, (current) => [...current, request]).pipe(
          Effect.zipRight(Effect.sync(() => response.writeHead(415).end())),
        );
      }
      const received = decodeSpans(body);
      return Ref.update(spans, (current) => [...current, ...received]).pipe(
        Effect.zipRight(
          Effect.sync(() =>
            response
              .writeHead(200, { "content-type": PROTOBUF_CONTENT_TYPE })
              .end(),
          ),
        ),
      );
    }),
    Effect.catchAllCause((cause) =>
      Ref.update(rejected, (current) => [...current, Cause.pretty(cause)]).pipe(
        Effect.zipRight(
          Effect.sync(() => {
            if (!response.headersSent) {
              response.writeHead(400);
            }
            response.end();
          }),
        ),
      ),
    ),
  );

/** Starts a loopback OTLP receiver that the scope closes. */
export const acquireOtlpReceiver: Effect.Effect<
  OtlpReceiver,
  ProcessTestError,
  Scope.Scope
> = Effect.gen(function* () {
  const spans = yield* Ref.make<readonly ExportedSpan[]>([]);
  const rejected = yield* Ref.make<readonly string[]>([]);
  const connections = yield* Ref.make(0);
  const { server, port } = yield* acquireLoopbackServer((incoming, response) =>
    handle(spans, rejected, incoming, response),
  );
  server.on("connection", () => {
    Effect.runSync(Ref.update(connections, (count) => count + 1));
  });
  return {
    endpoint: new URL(`http://${LOOPBACK_HOST}:${port}`),
    connections: Ref.get(connections),
    spans: Ref.get(spans),
    rejected: Ref.get(rejected),
  };
}).pipe(Effect.withSpan("acquireOtlpReceiver"));

/**
 * Polls the receiver until `ready` holds for the spans received so far.
 * After {@link SPAN_WAIT_LIMIT} it fails, naming every span and refused
 * request that did arrive.
 * @param receiver The receiver daemons export to.
 * @param ready What the received spans must show.
 * @returns The spans received when `ready` first held.
 */
export const awaitSpans = (
  receiver: OtlpReceiver,
  ready: (spans: readonly ExportedSpan[]) => boolean,
): Effect.Effect<readonly ExportedSpan[], ProcessTestError> =>
  Effect.sleep(SPAN_POLL_INTERVAL).pipe(
    Effect.zipRight(receiver.spans),
    Effect.repeat({ until: ready }),
    Effect.timeout(SPAN_WAIT_LIMIT),
    Effect.catchTag("TimeoutException", () =>
      Effect.all([receiver.spans, receiver.rejected]).pipe(
        Effect.flatMap(([spans, rejected]) =>
          Effect.fail(
            new ProcessTestError({
              message: `the awaited spans never arrived; received ${JSON.stringify(
                spans.map(({ name, attributes }) => ({ name, attributes })),
              )}; refused ${JSON.stringify(rejected)}`,
            }),
          ),
        ),
      ),
    ),
  );

/** The spans of one protobuf export. */
function decodeSpans(body: Buffer): readonly ExportedSpan[] {
  const request = Schema.decodeUnknownSync(decodedRequestSchema)(
    exportTraceServiceRequest.toObject(exportTraceServiceRequest.decode(body), {
      arrays: true,
      longs: String,
    }),
  );
  return request.resourceSpans.flatMap(({ resource, scopeSpans }) =>
    scopeSpans.flatMap(({ spans }) =>
      spans.map((span) => ({
        name: span.name,
        startTimeUnixNano: span.startTimeUnixNano,
        attributes: attributeRecord(span.attributes),
        resource: attributeRecord(resource?.attributes ?? []),
      })),
    ),
  );
}

function attributeRecord(
  keyValues: ReadonlyArray<typeof keyValueSchema.Type>,
): Record<string, AttributeValue> {
  return Object.fromEntries(
    keyValues.flatMap(({ name, value }) => {
      const scalar = value === undefined ? undefined : scalarAttribute(value);
      return scalar === undefined ? [] : [[name, scalar]];
    }),
  );
}

function scalarAttribute(value: typeof anyValueSchema.Type) {
  return (
    value.stringValue ?? value.intValue ?? value.doubleValue ?? value.boolValue
  );
}
