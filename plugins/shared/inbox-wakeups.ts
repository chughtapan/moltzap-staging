/** @file Scoped draft Events reception through the official HTTP transport. */

import {
  CLIENT_CAPABILITIES_META_KEY,
  CLIENT_INFO_META_KEY,
  PROTOCOL_VERSION_META_KEY,
  StreamableHTTPClientTransport,
  SUBSCRIPTION_ID_META_KEY,
} from "@modelcontextprotocol/client";
import { INBOX_PENDING_EVENT, packageVersion } from "@moltzap/social-harness";
import {
  Data,
  Deferred,
  Effect,
  Queue,
  Redacted,
  Schema,
  Stream,
  Take,
} from "effect";
import { randomUUID } from "node:crypto";

/** Reception failures contain no transport body, credentials or peer content. */
export class McpReceiveError extends Data.TaggedError("McpReceiveError")<{
  readonly reason:
    | "transport-failed"
    | "decode-failed"
    | "already-listening"
    | "incompatible-daemon";
}> {}

const protocolVersion = "2026-07-28";
const metadataSchema = Schema.Struct({
  [SUBSCRIPTION_ID_META_KEY]: Schema.Union(Schema.String, Schema.Number),
});
const positionSchema = Schema.Struct({
  cursor: Schema.optionalWith(Schema.Null, { exact: true }),
  _meta: metadataSchema,
});
const eventSchema = Schema.Struct({
  eventId: Schema.NonEmptyString,
  name: Schema.Literal(INBOX_PENDING_EVENT),
  timestamp: Schema.String,
  data: Schema.Struct({ pendingCount: Schema.NonNegativeInt }),
  cursor: Schema.optionalWith(Schema.Null, { exact: true }),
  _meta: metadataSchema,
});
const activeSchema = Schema.Struct({
  ...positionSchema.fields,
  truncated: Schema.Boolean,
});
const errorSchema = Schema.Struct({
  _meta: metadataSchema,
  error: Schema.Struct({ code: Schema.Number, message: Schema.String }),
});

type FrameKind = "active" | "event" | "heartbeat" | "error" | "terminated";
/** Each method owns its decoder and the kind produced by a successful decode. */
const frame =
  <Value extends typeof positionSchema.Type, Encoded>(
    kind: FrameKind,
    schema: Schema.Schema<Value, Encoded>,
  ) =>
  (params: unknown) =>
    Schema.decodeUnknown(schema)(params).pipe(
      Effect.map((value) => ({
        kind,
        subscriptionId: value._meta[SUBSCRIPTION_ID_META_KEY],
      })),
    );
const frames: Readonly<Record<string, ReturnType<typeof frame> | undefined>> = {
  "notifications/events/active": frame("active", activeSchema),
  "notifications/events/event": frame("event", eventSchema),
  "notifications/events/heartbeat": frame("heartbeat", positionSchema),
  "notifications/events/error": frame("error", errorSchema),
  "notifications/events/terminated": frame("terminated", errorSchema),
};
const decodeFrame = (method: string, params: unknown, id: string) => {
  const decode = frames[method];
  if (decode === undefined) {
    return Effect.fail(new McpReceiveError({ reason: "decode-failed" }));
  }
  return decode(params).pipe(
    Effect.catchTag("ParseError", () =>
      Effect.fail(new McpReceiveError({ reason: "decode-failed" })),
    ),
    Effect.flatMap((value) =>
      value.subscriptionId === id
        ? Effect.succeed(value.kind)
        : Effect.fail(new McpReceiveError({ reason: "decode-failed" })),
    ),
  );
};
type Phase = "opening" | "active" | "failed" | "stopped";
interface Reception {
  readonly queue: Queue.Queue<Take.Take<boolean, McpReceiveError>>;
  readonly active: Deferred.Deferred<undefined, McpReceiveError>;
  readonly transport: StreamableHTTPClientTransport;
  readonly abort: AbortController;
  readonly id: string;
  phase: Phase;
  queued: boolean;
  lastSeen: number;
}
const fail = (state: Reception, error: McpReceiveError) =>
  Effect.gen(function* () {
    if (state.phase === "stopped" || state.phase === "failed") {
      return;
    }
    state.phase = "failed";
    yield* Deferred.fail(state.active, error);
    yield* Queue.offer(state.queue, Take.fail(error));
    state.abort.abort();
  });
const wake = (state: Reception) =>
  Effect.suspend(() => {
    if (state.queued || state.phase === "stopped" || state.phase === "failed") {
      return Effect.void;
    }
    state.queued = true;
    return Queue.offer(state.queue, Take.of(true)).pipe(Effect.asVoid);
  });
const receiveFrame = (state: Reception, kind: FrameKind) =>
  Effect.gen(function* () {
    if (state.phase === "stopped" || state.phase === "failed") {
      return;
    }
    state.lastSeen = Date.now();
    if (kind === "active") {
      state.phase = "active";
      yield* Deferred.succeed(state.active, undefined);
      yield* wake(state);
    } else if (state.phase === "opening") {
      yield* fail(state, new McpReceiveError({ reason: "decode-failed" }));
    } else if (kind === "event") {
      yield* wake(state);
    } else if (kind === "terminated") {
      yield* fail(state, new McpReceiveError({ reason: "transport-failed" }));
    }
  });
const protocolFailure = (data: unknown): McpReceiveError => {
  if (typeof data !== "object" || data === null) {
    return new McpReceiveError({ reason: "transport-failed" });
  }
  return new McpReceiveError({
    reason:
      "reason" in data && data.reason === "already-listening"
        ? "already-listening"
        : "transport-failed",
  });
};
const onMessage =
  (state: Reception): NonNullable<StreamableHTTPClientTransport["onmessage"]> =>
  (message) => {
    if (state.phase === "stopped" || state.phase === "failed") {
      return;
    }
    if ("error" in message) {
      const error =
        message.id === state.id
          ? protocolFailure(message.error.data)
          : new McpReceiveError({ reason: "decode-failed" });
      Effect.runFork(fail(state, error));
    } else if ("method" in message) {
      if (!message.method.startsWith("notifications/events/")) {
        return;
      }
      Effect.runFork(
        decodeFrame(message.method, message.params, state.id).pipe(
          Effect.flatMap((kind) => receiveFrame(state, kind)),
          Effect.catchAll((error) => fail(state, error)),
        ),
      );
    } else {
      Effect.runFork(
        fail(state, new McpReceiveError({ reason: "transport-failed" })),
      );
    }
  };
const close = (state: Reception) =>
  Effect.sync(() => {
    state.phase = "stopped";
    state.abort.abort();
  }).pipe(
    Effect.zipRight(
      Effect.tryPromise(() => state.transport.close()).pipe(Effect.ignore),
    ),
    Effect.zipRight(Queue.shutdown(state.queue)),
  );

/** The MCP client identity the inbox stream announces to the daemon. */
const INBOX_CLIENT_INFO = {
  name: "sharness-inbox",
  version: packageVersion,
} as const;

const open = (state: Reception) =>
  Effect.gen(function* () {
    state.transport.onmessage = onMessage(state);
    state.transport.onerror = () => {
      Effect.runFork(
        fail(state, new McpReceiveError({ reason: "transport-failed" })),
      );
    };
    state.transport.setProtocolVersion(protocolVersion);
    yield* Effect.tryPromise({
      try: () => state.transport.start(),
      catch: () => new McpReceiveError({ reason: "transport-failed" }),
    });
    yield* Effect.tryPromise({
      try: () =>
        state.transport.send(
          {
            jsonrpc: "2.0",
            id: state.id,
            method: "events/stream",
            params: {
              name: INBOX_PENDING_EVENT,
              arguments: {},
              cursor: null,
              _meta: {
                [PROTOCOL_VERSION_META_KEY]: protocolVersion,
                [CLIENT_INFO_META_KEY]: INBOX_CLIENT_INFO,
                [CLIENT_CAPABILITIES_META_KEY]: {},
              },
            },
          },
          {
            requestSignal: state.abort.signal,
            onRequestStreamEnd: () => {
              Effect.runFork(
                fail(
                  state,
                  new McpReceiveError({ reason: "transport-failed" }),
                ),
              );
            },
          },
        ),
      catch: () => new McpReceiveError({ reason: "transport-failed" }),
    });
    yield* Deferred.await(state.active);
  }).pipe(
    Effect.timeoutFail({
      duration: "60 seconds",
      onTimeout: () => new McpReceiveError({ reason: "transport-failed" }),
    }),
  );
const watchSilence = (state: Reception) =>
  Effect.sleep("30 seconds").pipe(
    Effect.zipRight(
      Effect.suspend(() =>
        Date.now() - state.lastSeen > 60_000
          ? fail(state, new McpReceiveError({ reason: "transport-failed" }))
          : Effect.void,
      ),
    ),
    Effect.forever,
    Effect.forkScoped,
  );

/**
 * Subscribe before reading the inbox so arrivals during catch-up remain visible.
 * Notifications carry no message content. The `onReady` callback runs after
 * activation. The owning scope cancels the stream, which has no request timeout.
 */
export const inboxWakeups = (
  endpoint: URL,
  credential: Redacted.Redacted,
  onReady?: Effect.Effect<void>,
): Stream.Stream<boolean, McpReceiveError> =>
  Stream.unwrapScoped(
    Effect.gen(function* () {
      const queue =
        yield* Queue.unbounded<Take.Take<boolean, McpReceiveError>>();
      const active = yield* Deferred.make<undefined, McpReceiveError>();
      const state: Reception = {
        queue,
        active,
        transport: new StreamableHTTPClientTransport(endpoint, {
          requestInit: {
            headers: { Authorization: `Bearer ${Redacted.value(credential)}` },
            redirect: "error",
          },
        }),
        abort: new AbortController(),
        id: randomUUID(),
        phase: "opening",
        queued: false,
        lastSeen: Date.now(),
      };
      yield* Effect.addFinalizer(() => close(state));
      yield* open(state);
      yield* watchSilence(state);
      yield* onReady ?? Effect.void;
      return Stream.fromQueue(queue).pipe(
        Stream.flattenTake,
        Stream.tap(() =>
          Effect.sync(() => {
            state.queued = false;
          }),
        ),
      );
    }).pipe(Effect.withSpan("inboxWakeups")),
  );
