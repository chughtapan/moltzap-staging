/** @file Scoped MCP implementation of the public semantic HarnessEndpoint. */

import {
  Client,
  fromJsonSchema,
  type JsonSchemaType,
  ProtocolError,
  SdkHttpError,
  StreamableHTTPClientTransport,
} from "@modelcontextprotocol/client";
import {
  Data,
  Deferred,
  Effect,
  Redacted,
  Ref,
  type Scope,
  Stream,
} from "effect";
import type { SendResult } from "../host-protocol/send-result.js";
import { CollectiveError } from "../communication/collectives/contracts.js";
import {
  DeliveryAcknowledgeError,
  ListenError,
  SendError,
  sendFailureReasons,
} from "../communication/conversations/errors.js";
import { FirewallSendError } from "../host-protocol/advice.js";
import {
  HARNESS_ACKNOWLEDGE_DELIVERY_TOOL,
  HARNESS_READ_INBOX_TOOL,
  HARNESS_SEND_TOOL,
  INBOX_ITEM_EVENT,
  INBOX_PENDING_EVENT,
} from "../host-protocol/names.js";
import {
  decodeHarnessReadInboxResult,
  decodeHarnessSendErrorData,
  decodeHarnessSendResult,
  type DeliveryToken,
  type HarnessSendErrorData,
} from "../host-protocol/operations.js";
import { packageVersion } from "../release.js";
import {
  ConnectError,
  type HarnessEndpoint,
  type HarnessEndpointOptions,
  type InboundDelivery,
} from "./capability.js";
import { inboxWakeups } from "./events.js";
import { acknowledgeReason, isReason, listenFailure } from "./failures.js";
import { pollWakeups } from "./poll.js";

/* eslint-disable agent-code-guard/promise-type, @typescript-eslint/no-invalid-void-type -- The official MCP client lifecycle is Promise-native and is converted to Effect at this private edge. */

const MODERN_PROTOCOL_VERSION = "2026-07-28";
const CLIENT_IMPLEMENTATION = {
  name: "sharness-endpoint",
  version: packageVersion,
} as const;
class CloseError extends Data.TaggedError("CloseError") {}

/**
 * Connect to one registered daemon for the caller's scope. The daemon's
 * URL must use HTTPS or HTTP on literal `127.0.0.1` or `[::1]`, without userinfo
 * or a fragment. The URL is copied before execution, and authenticated requests
 * refuse redirects. Acquisition verifies support for the selected receive mode.
 * @param endpoint Explicit MCP URL for the configured endpoint daemon.
 * @param options Bearer credential and receive mode; polling is the default.
 * @returns An endpoint whose resources remain live for the caller's scope.
 */
export function acquireHarnessEndpoint(
  endpoint: URL,
  options: HarnessEndpointOptions,
): Effect.Effect<HarnessEndpoint, ConnectError, Scope.Scope> {
  const address = new URL(endpoint.href);
  if (!safeCredentialEndpoint(address)) {
    return Effect.fail(new ConnectError({ reason: "transport-failed" }));
  }
  return acquireEndpoint(address, options).pipe(
    Effect.withSpan("acquireHarnessEndpoint"),
  );
}

/**
 * Daemon credentials travel over TLS or literal loopback.
 * @param endpoint Address checked before a client sends its bearer.
 * @returns Whether the address can carry a credential without userinfo or fragments.
 */
export function safeCredentialEndpoint(endpoint: URL): boolean {
  const secure = endpoint.protocol === "https:";
  const loopback =
    endpoint.protocol === "http:" &&
    (endpoint.hostname === "127.0.0.1" || endpoint.hostname === "[::1]");
  const plainAddress =
    !endpoint.username && !endpoint.password && !endpoint.hash;
  return (secure || loopback) && plainAddress;
}

function closeClient(client: Client): Effect.Effect<void> {
  return closeQuietly(() => client.close());
}

function closeQuietly(close: () => Promise<void>): Effect.Effect<void> {
  return Effect.tryPromise({ try: close, catch: () => new CloseError() }).pipe(
    Effect.ignore,
  );
}

/**
 * Rebuild a typed refusal from validated JSON-RPC error data. An unrecognized
 * response, timeout or dropped connection can follow the daemon queueing the
 * message, so it is `outcome-unknown` unless the request never reached the
 * daemon.
 * @param cause Upstream rejection at the MCP tool boundary.
 * @returns A closed send failure without transport details.
 */
function sendFailure(
  cause: unknown,
): SendError | CollectiveError | FirewallSendError {
  if (!ProtocolError.isInstance(cause)) {
    return new SendError({
      reason: neverReachedDaemon(cause)
        ? "network-unavailable"
        : "outcome-unknown",
    });
  }
  return decodeHarnessSendErrorData(cause.data).pipe(
    Effect.map((decoded) => {
      if ("refusal" in decoded) {
        return new FirewallSendError(decoded);
      }
      if ("failure" in decoded) {
        return collectiveRefusal(decoded);
      }
      return new SendError({
        reason: isReason(decoded.reason, sendFailureReasons)
          ? decoded.reason
          : "outcome-unknown",
        ...(decoded.detail === undefined ? {} : { detail: decoded.detail }),
        ...("issues" in decoded ? { issues: decoded.issues } : {}),
      });
    }),
    Effect.orElseSucceed(() => new SendError({ reason: "outcome-unknown" })),
    Effect.runSync,
  );
}

/**
 * A refused collective send rebuilt from its error data.
 * @param decoded The `collective-failed` error data.
 * @returns The error, with the openOperationIds or the questions open elsewhere it names.
 */
function collectiveRefusal(
  decoded: Extract<
    HarnessSendErrorData,
    { readonly reason: "collective-failed" }
  >,
): CollectiveError {
  return new CollectiveError({
    ...(decoded.operationId === undefined
      ? {}
      : { operationId: decoded.operationId }),
    failure: decoded.failure,
  });
}

/**
 * HTTP statuses that the daemon's HTTP layer, or the MCP server SDK before it
 * dispatches a request, answers with. Any other status may follow a send that
 * ran: the SDK answers 499 for a call it closed mid-dispatch.
 */
const PRE_DISPATCH_STATUSES: ReadonlySet<number> = new Set([
  400, 401, 403, 404, 405,
]);

/**
 * Whether a rejected request certainly ran no daemon tool: the daemon turned
 * it away before dispatch, or the connection was refused.
 * @param cause A rejection that is not a JSON-RPC error.
 * @returns Whether the daemon cannot have acted on the request.
 */
function neverReachedDaemon(cause: unknown): boolean {
  if (SdkHttpError.isInstance(cause)) {
    return PRE_DISPATCH_STATUSES.has(cause.status);
  }
  return (
    cause instanceof Error &&
    cause.cause instanceof Error &&
    "code" in cause.cause &&
    cause.cause.code === "ECONNREFUSED"
  );
}

function callSend(
  client: Client,
  input: Parameters<HarnessEndpoint["send"]>[0],
): Effect.Effect<SendResult, SendError | CollectiveError | FirewallSendError> {
  return Effect.tryPromise({
    try: (signal) =>
      client.callTool(
        {
          name: HARNESS_SEND_TOOL,
          arguments: { input },
        },
        { signal },
      ),
    catch: sendFailure,
  }).pipe(
    Effect.flatMap((result) =>
      result.isError === true
        ? Effect.fail(new SendError({ reason: "outcome-unknown" }))
        : decodeHarnessSendResult(result.structuredContent).pipe(
            Effect.catchTag("ParseError", () =>
              Effect.fail(new SendError({ reason: "outcome-unknown" })),
            ),
          ),
    ),
  );
}

const eventsListOutput = fromJsonSchema<{
  events: ReadonlyArray<{ name: string; delivery: readonly string[] }>;
}>({
  type: "object",
  required: ["events"],
  properties: {
    events: {
      type: "array",
      items: {
        type: "object",
        required: ["name", "delivery"],
        properties: {
          name: { type: "string" },
          delivery: { type: "array", items: { type: "string" } },
        },
      },
    },
  },
} satisfies JsonSchemaType);

function supportsInboxEvents(
  client: Client,
  authenticated: boolean,
): Effect.Effect<boolean, ConnectError> {
  return Effect.tryPromise({
    try: (signal) =>
      client.request({ method: "events/list", params: {} }, eventsListOutput, {
        signal,
      }),
    catch: (cause) =>
      new ConnectError({
        reason:
          ProtocolError.isInstance(cause) && cause.code === -32601
            ? "incompatible-daemon"
            : "transport-failed",
      }),
  }).pipe(
    Effect.map((catalog) =>
      catalog.events.some(
        (event) =>
          event.name ===
            (authenticated ? INBOX_ITEM_EVENT : INBOX_PENDING_EVENT) &&
          event.delivery.includes(authenticated ? "poll" : "push"),
      ),
    ),
  );
}

function acquireConnection(
  client: Client,
  endpoint: URL,
  options: HarnessEndpointOptions,
): Effect.Effect<Client, ConnectError, Scope.Scope> {
  return Effect.uninterruptibleMask((restore) =>
    Effect.gen(function* () {
      const connected = yield* restore(connect(client, endpoint, options));
      yield* Effect.addFinalizer(() => closeClient(client));
      return connected;
    }),
  );
}

function connect(
  client: Client,
  endpoint: URL,
  options: HarnessEndpointOptions,
): Effect.Effect<Client, ConnectError> {
  return Effect.tryPromise({
    try: (signal) =>
      client.connect(
        new StreamableHTTPClientTransport(endpoint, {
          requestInit: {
            headers: {
              Authorization: `Bearer ${Redacted.value(options.credential)}`,
            },
            redirect: "error",
          },
        }),
        { signal },
      ),
    catch: () => new ConnectError({ reason: "transport-failed" }),
  }).pipe(
    Effect.flatMap(() =>
      supportsInboxEvents(client, options.receiveMode !== "push").pipe(
        Effect.flatMap((supported) =>
          supported
            ? Effect.succeed(client)
            : Effect.fail(new ConnectError({ reason: "incompatible-daemon" })),
        ),
      ),
    ),
    Effect.onError(() => closeClient(client)),
  );
}

/** The local lifetime guards receipts even when remote cleanup fails. */
interface ConsumerBinding {
  readonly metadata: Readonly<Record<string, unknown>>;
  readonly revoked: Deferred.Deferred<never, DeliveryAcknowledgeError>;
}

const callAcknowledgeDelivery = (
  client: Client,
  deliveryToken: DeliveryToken,
  consumer: ConsumerBinding,
): Effect.Effect<void, DeliveryAcknowledgeError> => {
  const acknowledge = Effect.tryPromise({
    try: (signal) =>
      client.callTool(
        {
          name: HARNESS_ACKNOWLEDGE_DELIVERY_TOOL,
          arguments: { deliveryToken },
          _meta: consumer.metadata,
        },
        { signal },
      ),
    catch: (cause) =>
      new DeliveryAcknowledgeError({ reason: acknowledgeReason(cause) }),
  }).pipe(
    Effect.flatMap((result) =>
      result.isError === true
        ? Effect.fail(
            new DeliveryAcknowledgeError({ reason: "transport-failed" }),
          )
        : Effect.void,
    ),
  );
  return Deferred.isDone(consumer.revoked).pipe(
    Effect.flatMap((closed) =>
      closed ? Deferred.await(consumer.revoked) : acknowledge,
    ),
    Effect.raceFirst(Deferred.await(consumer.revoked)),
  );
};

const decodeInbox = (result: Awaited<ReturnType<Client["callTool"]>>) =>
  result.isError === true
    ? Effect.fail(new ListenError({ reason: "transport-failed" }))
    : decodeHarnessReadInboxResult(result.structuredContent).pipe(
        Effect.catchTag("ParseError", () =>
          Effect.fail(new ListenError({ reason: "decode-failed" })),
        ),
      );

const unseenDeliveries = (
  client: Client,
  seen: Set<DeliveryToken>,
  items: Effect.Effect.Success<ReturnType<typeof decodeInbox>>["items"],
  consumer: ConsumerBinding,
): InboundDelivery[] => {
  const deliveries: InboundDelivery[] = [];
  for (const entry of items) {
    if (seen.has(entry.deliveryToken)) {
      continue;
    }
    seen.add(entry.deliveryToken);
    deliveries.push({
      eventId: entry.deliveryToken,
      item: entry.item,
      acknowledge: callAcknowledgeDelivery(
        client,
        entry.deliveryToken,
        consumer,
      ),
      ...(entry.guidance === undefined ? {} : { guidance: entry.guidance }),
    });
  }
  return deliveries;
};

/**
 * A complete bounded snapshot prunes seen tokens; partial reads retain them.
 * Recovery delivers canonical content and local guidance, independently of the
 * wakeup envelope or cursor, and only the receipt can retire unread work.
 *
 * @param client The connected MCP transport.
 * @param seen Tokens already yielded during this subscription.
 * @param consumer Read authority and the local receipt lifetime.
 * @returns Unseen deliveries from the pending snapshot.
 */
function pendingInbox(
  client: Client,
  seen: Set<DeliveryToken>,
  consumer: ConsumerBinding,
): Stream.Stream<InboundDelivery, ListenError> {
  const retained = new Set<DeliveryToken>();
  const page = (cursor?: string): Stream.Stream<InboundDelivery, ListenError> =>
    Stream.unwrap(
      Effect.tryPromise({
        try: (signal) =>
          client.callTool(
            {
              name: HARNESS_READ_INBOX_TOOL,
              arguments: cursor === undefined ? {} : { cursor },
              _meta: consumer.metadata,
            },
            { signal },
          ),
        catch: listenFailure,
      }).pipe(
        Effect.flatMap(decodeInbox),
        Effect.map((result) => {
          for (const entry of result.items) {
            retained.add(entry.deliveryToken);
          }
          const current = Stream.fromIterable(
            unseenDeliveries(client, seen, result.items, consumer),
          );
          return result.nextCursor === undefined
            ? current
            : Stream.concat(
                current,
                Stream.suspend(() => page(result.nextCursor)),
              );
        }),
      ),
    );
  return Stream.concat(
    page(),
    Stream.execute(
      Effect.sync(() => {
        for (const token of seen) {
          if (!retained.has(token)) {
            seen.delete(token);
          }
        }
      }),
    ),
  );
}

function acquireListenerSlot(
  active: Ref.Ref<boolean>,
): Effect.Effect<void, ListenError, Scope.Scope> {
  return Effect.uninterruptible(
    Effect.gen(function* () {
      const acquired = yield* Ref.modify(active, (isActive) =>
        isActive
          ? ([false, true] satisfies [boolean, boolean])
          : ([true, true] satisfies [boolean, boolean]),
      ).pipe(
        Effect.filterOrFail(
          (slotAcquired) => slotAcquired,
          () => new ListenError({ reason: "already-listening" }),
        ),
      );
      yield* Effect.addFinalizer(() => Ref.set(active, false));
      return acquired;
    }).pipe(Effect.asVoid),
  );
}

function messages(
  client: Client,
  listenerActive: Ref.Ref<boolean>,
  endpoint: URL,
  options: HarnessEndpointOptions,
): Stream.Stream<InboundDelivery, ListenError> {
  return Stream.unwrapScoped(
    Effect.gen(function* () {
      yield* acquireListenerSlot(listenerActive);
      const revoked = yield* Deferred.make<never, DeliveryAcknowledgeError>();
      const fence = Deferred.fail(
        revoked,
        new DeliveryAcknowledgeError({ reason: "consumer-revoked" }),
      ).pipe(Effect.asVoid);
      yield* Effect.addFinalizer(() => fence);
      const seen = new Set<DeliveryToken>();
      const wakeups =
        options.receiveMode === "push"
          ? inboxWakeups(endpoint, options.credential, options.onReady).pipe(
              Stream.as({}),
            )
          : pollWakeups(client, fence, options.onReady ?? Effect.void);
      return wakeups.pipe(
        Stream.flatMap((metadata) =>
          pendingInbox(client, seen, { metadata, revoked }),
        ),
      );
    }),
  );
}

function makeMcpClient(): Client {
  return new Client(CLIENT_IMPLEMENTATION, {
    capabilities: {},
    versionNegotiation: { mode: { pin: MODERN_PROTOCOL_VERSION } },
  });
}

function acquireEndpoint(
  endpoint: URL,
  options: HarnessEndpointOptions,
): Effect.Effect<HarnessEndpoint, ConnectError, Scope.Scope> {
  return Effect.gen(function* () {
    const client = makeMcpClient();
    yield* acquireConnection(client, endpoint, options);
    const listenerActive = yield* Ref.make(false);
    return {
      send: (input) => callSend(client, input),
      messages: messages(client, listenerActive, endpoint, options),
    } satisfies HarnessEndpoint;
  });
}

/* eslint-enable agent-code-guard/promise-type, @typescript-eslint/no-invalid-void-type -- Restore strict defaults after the Promise-native MCP edge. */

/** The capability this module acquires, and its connect and delivery types. */
export {
  ConnectError,
  type HarnessEndpoint,
  type HarnessEndpointOptions,
  type InboundDelivery,
} from "./capability.js";
