/** @file Scoped draft Events reception through the official HTTP transport. */

import {
  CLIENT_CAPABILITIES_META_KEY,
  CLIENT_INFO_META_KEY,
  PROTOCOL_VERSION_META_KEY,
  StreamableHTTPClientTransport,
  type StreamableHTTPClientTransportOptions,
  SUBSCRIPTION_ID_META_KEY,
} from "@modelcontextprotocol/client";
import {
  INBOX_PENDING_EVENT,
  packageVersion,
} from "@social-harness/social-harness";
import {
  Data,
  Deferred,
  Effect,
  Option,
  Queue,
  Schema,
  Stream,
  Take,
} from "effect";
import { randomUUID } from "node:crypto";

/** Reception failures contain no transport body, credentials or peer content. */
export class McpReceiveError extends Data.TaggedError("McpReceiveError")<{
  readonly reason: "transport-failed" | "decode-failed" | "already-listening";
}> {}

const transportFailed = new McpReceiveError({ reason: "transport-failed" });
const decodeFailed = new McpReceiveError({ reason: "decode-failed" });

const protocolVersion = "2026-07-28";
const NOTIFICATION_PREFIX = "notifications/events/";

/** Every stream notification names the subscription it belongs to. */
const notification = Schema.Struct({
  _meta: Schema.Struct({
    [SUBSCRIPTION_ID_META_KEY]: Schema.Union(Schema.String, Schema.Number),
  }),
});

/**
 * One open stream. It is `opening` until the daemon confirms it, then
 * `active`, and `done` once it fails or is closed, after which every message
 * is ignored.
 */
interface Reception {
  /** Holds at most one pending wake-up, or the failure that ended the stream. */
  readonly queue: Queue.Queue<Take.Take<undefined, McpReceiveError>>;
  readonly active: Deferred.Deferred<undefined, McpReceiveError>;
  readonly transport: StreamableHTTPClientTransport;
  readonly abort: AbortController;
  readonly id: string;
  phase: "opening" | "active" | "done";
  lastSeen: number;
}

const fail = (state: Reception, error: McpReceiveError) =>
  Effect.gen(function* () {
    if (state.phase === "done") {
      return;
    }
    state.phase = "done";
    yield* Deferred.fail(state.active, error);
    yield* Queue.offer(state.queue, Take.fail(error));
    state.abort.abort();
  });

/** Acts on one stream notification: the confirmation, a wake-up, a heartbeat or the end. */
const receive = (state: Reception, method: string, params: unknown) =>
  Effect.gen(function* () {
    const decoded = yield* Schema.decodeUnknown(notification)(params).pipe(
      Effect.option,
    );
    if (
      Option.isNone(decoded) ||
      decoded.value._meta[SUBSCRIPTION_ID_META_KEY] !== state.id
    ) {
      return yield* fail(state, decodeFailed);
    }
    state.lastSeen = Date.now();
    const kind = method.slice(NOTIFICATION_PREFIX.length);
    if (kind === "active") {
      state.phase = "active";
      yield* Deferred.succeed(state.active, undefined);
      return yield* Queue.offer(state.queue, Take.of(undefined));
    }
    if (state.phase === "opening") {
      return yield* fail(state, decodeFailed);
    }
    if (kind === "event") {
      return yield* Queue.offer(state.queue, Take.of(undefined));
    }
    if (kind === "terminated") {
      return yield* fail(state, transportFailed);
    }
    return kind === "heartbeat" || kind === "error"
      ? undefined
      : yield* fail(state, decodeFailed);
  });

const onMessage =
  (state: Reception): NonNullable<StreamableHTTPClientTransport["onmessage"]> =>
  (message) => {
    if (state.phase === "done") {
      return;
    }
    if ("error" in message) {
      Effect.runFork(
        fail(state, message.id === state.id ? transportFailed : decodeFailed),
      );
    } else if (!("method" in message)) {
      Effect.runFork(fail(state, transportFailed));
    } else if (message.method.startsWith(NOTIFICATION_PREFIX)) {
      Effect.runFork(receive(state, message.method, message.params));
    }
  };

const close = (state: Reception) =>
  Effect.sync(() => {
    state.phase = "done";
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
      Effect.runFork(fail(state, transportFailed));
    };
    state.transport.setProtocolVersion(protocolVersion);
    yield* Effect.tryPromise({
      try: () => state.transport.start(),
      catch: () => transportFailed,
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
              Effect.runFork(fail(state, transportFailed));
            },
          },
        ),
      catch: () => transportFailed,
    });
    yield* Deferred.await(state.active);
  }).pipe(
    Effect.timeoutFail({
      duration: "60 seconds",
      onTimeout: () => transportFailed,
    }),
  );
/** A daemon that sends nothing for a minute, not even a heartbeat, is gone. */
const watchSilence = (state: Reception) =>
  Effect.sleep("30 seconds").pipe(
    Effect.zipRight(
      Effect.suspend(() =>
        Date.now() - state.lastSeen > 60_000
          ? fail(state, transportFailed)
          : Effect.void,
      ),
    ),
    Effect.forever,
    Effect.forkScoped,
  );

/**
 * Emits once when the stream opens and again whenever the daemon says the
 * inbox has items, and fails when the stream ends. Wake-ups carry no message
 * content, and several that arrive before one is taken count as one. The
 * owning scope closes the stream, which has no request timeout.
 */
export const inboxWakeups = (
  endpoint: URL,
  options: StreamableHTTPClientTransportOptions,
): Stream.Stream<undefined, McpReceiveError> =>
  Stream.unwrapScoped(
    Effect.gen(function* () {
      const state: Reception = {
        queue: yield* Queue.sliding<Take.Take<undefined, McpReceiveError>>(1),
        active: yield* Deferred.make<undefined, McpReceiveError>(),
        transport: new StreamableHTTPClientTransport(endpoint, options),
        abort: new AbortController(),
        id: randomUUID(),
        phase: "opening",
        lastSeen: Date.now(),
      };
      yield* Effect.addFinalizer(() => close(state));
      yield* open(state);
      yield* watchSilence(state);
      return Stream.fromQueue(state.queue).pipe(Stream.flattenTake);
    }).pipe(Effect.withSpan("inboxWakeups")),
  );
