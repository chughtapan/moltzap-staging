/** @file Scoped MCP connections and tool calls shared by native host plugins. */
import {
  Client,
  ProtocolError,
  SdkHttpError,
  SdkError,
  SdkErrorCode,
  StreamableHTTPClientTransport,
  type Implementation,
} from "@modelcontextprotocol/client";
import {
  RequestError,
  FirewallRefusal,
  SendError,
  decodeHarnessSendErrorData,
  toolDefinitions,
  toolFailureSchemas,
  InboxError,
  DeliveryAcknowledgeError,
  HistoryError,
  RegistrationError,
  FirewallPolicyError,
  type ToolName,
  type ToolRequest,
  type ToolResult,
} from "@moltzap/social-harness";
import {
  Data,
  Duration,
  Effect,
  Either,
  Fiber,
  Ref,
  Redacted,
  Schema,
  type ParseResult,
  Scope,
  Stream,
} from "effect";
import { inboxWakeups, McpReceiveError } from "./inbox.js";

/** Connection failures omit the private endpoint and credential. */
export class McpConnectError extends Data.TaggedError("McpConnectError")<{
  readonly reason: "transport-failed" | "incompatible-daemon";
}> {}

/** Non-send tool failures never include unvalidated server or transport details. */
export class McpToolError extends Data.TaggedError("McpToolError")<{
  readonly tool: ToolName;
  readonly reason: "tool-failed" | "transport-failed" | "decode-failed";
}> {}

/** A send retains its closed domain refusal and uncertain-acceptance classification. */
export type ToolFailure<Name extends ToolName> = Name extends "send_message"
  ? SendError | RequestError | FirewallRefusal
  :
      | McpToolError
      | (Name extends "read_inbox" | "read_event"
          ? InboxError
          : Name extends "acknowledge_delivery"
            ? DeliveryAcknowledgeError
            : Name extends "search_conversations" | "read_conversation"
              ? HistoryError
              : Name extends "register"
                ? RegistrationError
                : Name extends
                      | "policy_read"
                      | "policy_update"
                      | "firewall_decisions"
                  ? FirewallPolicyError
                  : never);

type DomainFailure =
  | InboxError
  | DeliveryAcknowledgeError
  | HistoryError
  | RegistrationError
  | FirewallPolicyError;

/** One server and credential are captured for the connection's whole scope. */
export interface McpConnectionSettings {
  readonly endpoint: URL;
  readonly credential: Redacted.Redacted;
}

/** Reception uses the connection's captured server, without another credential setting. */
export interface InboxOptions {
  readonly receiveMode?: "push" | "poll";
  readonly onReady?: Effect.Effect<void>;
}

/** Inbox values contain a stable lookup token, content and saved local advice. */
export type InboxEntry = ToolResult<"read_inbox">["items"][number];

/** The stock MCP client remains visible to plugin code. */
export interface McpConnection {
  readonly client: Client;
  readonly consumeInbox: <E, R>(
    options: InboxOptions,
    accept: (entry: InboxEntry) => Effect.Effect<void, E, R>,
  ) => Effect.Effect<void, E | McpReceiveError, R>;
}

/** TLS and literal loopback can carry the private bearer without redirects. */
export function safeCredentialEndpoint(endpoint: URL): boolean {
  return (
    !endpoint.username &&
    !endpoint.password &&
    !endpoint.hash &&
    (endpoint.protocol === "https:" ||
      (endpoint.protocol === "http:" &&
        (endpoint.hostname === "127.0.0.1" || endpoint.hostname === "[::1]")))
  );
}

/** The caller's scope owns both the client and every active reception. */
export function acquireMcpConnection(
  settings: McpConnectionSettings,
  implementation: Implementation,
) {
  const endpoint = new URL(settings.endpoint.href);
  return acquireConnection(
    { endpoint, credential: settings.credential },
    { ...implementation },
  );
}

const acquireConnection = Effect.fn("Mcp.acquireConnection")(function* (
  settings: McpConnectionSettings,
  implementation: Implementation,
) {
  const endpoint = settings.endpoint;
  const credential = settings.credential;
  if (!safeCredentialEndpoint(endpoint)) {
    return yield* new McpConnectError({ reason: "transport-failed" });
  }
  const client = new Client(implementation, {
    capabilities: {},
    versionNegotiation: { mode: { pin: "2026-07-28" } },
  });
  yield* Effect.acquireRelease(
    Effect.tryPromise({
      try: (signal) =>
        client.connect(
          new StreamableHTTPClientTransport(endpoint, {
            requestInit: {
              headers: {
                Authorization: `Bearer ${Redacted.value(credential)}`,
              },
              redirect: "error",
            },
          }),
          { signal },
        ),
      catch: (cause) =>
        new McpConnectError({
          reason:
            cause instanceof SdkError &&
            cause.code === SdkErrorCode.EraNegotiationFailed
              ? "incompatible-daemon"
              : "transport-failed",
        }),
    }).pipe(
      Effect.onError(() =>
        Effect.tryPromise(() => client.close()).pipe(Effect.ignore),
      ),
    ),
    () => Effect.tryPromise(() => client.close()).pipe(Effect.ignore),
  );
  const receiving = yield* Ref.make(false);
  const scope = yield* Scope.Scope;
  return {
    client,
    consumeInbox: <E, R>(
      options: InboxOptions,
      accept: (entry: InboxEntry) => Effect.Effect<void, E, R>,
    ) =>
      Effect.acquireUseRelease(
        Effect.forkIn(
          Effect.interruptible(
            Effect.scoped(
              Effect.gen(function* () {
                yield* Effect.acquireRelease(
                  Ref.getAndSet(receiving, true).pipe(
                    Effect.filterOrFail(
                      (active) => !active,
                      () =>
                        new McpReceiveError({ reason: "already-listening" }),
                    ),
                  ),
                  () => Ref.set(receiving, false),
                );
                const ready = options.onReady ?? Effect.void;
                const wakeups: Stream.Stream<void, McpReceiveError> =
                  options.receiveMode === "push"
                    ? inboxWakeups(endpoint, credential, ready).pipe(
                        Stream.as(undefined),
                      )
                    : Stream.concat(
                        Stream.fromEffect(ready),
                        Stream.tick(Duration.millis(250)),
                      );
                yield* wakeups.pipe(
                  Stream.runForEach(() => consumePending(client, accept)),
                );
              }),
            ),
          ),
          scope,
        ),
        Fiber.join,
        Fiber.interrupt,
      ),
  } satisfies McpConnection;
});

/** Calls the named MCP tool once. An uncertain send never triggers a retry. */
export function callTool<Name extends ToolName>(
  client: Client,
  request: ToolRequest & { readonly name: Name },
): Effect.Effect<ToolResult<Name>, ToolFailure<Name>>;
export function callTool(
  client: Client,
  request: ToolRequest,
): Effect.Effect<unknown, unknown> {
  return Effect.tryPromise({
    try: (signal) => client.callTool(request, { signal }),
    catch: (error) =>
      request.name === "send_message"
        ? sendFailure(error)
        : managementFailure(request.name, error),
  }).pipe(
    Effect.flatMap((response) => {
      if (response.isError === true) {
        return Effect.fail(
          request.name === "send_message"
            ? new SendError({ reason: "outcome-unknown" })
            : new McpToolError({ tool: request.name, reason: "tool-failed" }),
        );
      }
      return decodeToolResult(request.name, response.structuredContent).pipe(
        Effect.mapError(() =>
          request.name === "send_message"
            ? new SendError({ reason: "outcome-unknown" })
            : new McpToolError({ tool: request.name, reason: "decode-failed" }),
        ),
      );
    }),
  );
}

function decodeToolResult(
  name: ToolName,
  value: unknown,
): Effect.Effect<ToolResult<ToolName>, ParseResult.ParseError> {
  switch (name) {
    case "event_subscription_status":
      return Schema.decodeUnknown(
        toolDefinitions.event_subscription_status.output,
      )(value);
    case "revoke_event_subscription":
      return Schema.decodeUnknown(
        toolDefinitions.revoke_event_subscription.output,
      )(value);
    case "resume_event_subscription":
      return Schema.decodeUnknown(
        toolDefinitions.resume_event_subscription.output,
      )(value);
    case "send_message":
      return Schema.decodeUnknown(toolDefinitions.send_message.output)(value);
    case "read_inbox":
      return Schema.decodeUnknown(toolDefinitions.read_inbox.output)(value);
    case "read_event":
      return Schema.decodeUnknown(toolDefinitions.read_event.output)(value);
    case "acknowledge_delivery":
      return Schema.decodeUnknown(toolDefinitions.acknowledge_delivery.output)(
        value,
      );
    case "register":
      return Schema.decodeUnknown(toolDefinitions.register.output)(value);
    case "status":
      return Schema.decodeUnknown(toolDefinitions.status.output)(value);
    case "search_conversations":
      return Schema.decodeUnknown(toolDefinitions.search_conversations.output)(
        value,
      );
    case "read_conversation":
      return Schema.decodeUnknown(toolDefinitions.read_conversation.output)(
        value,
      );
    case "policy_read":
      return Schema.decodeUnknown(toolDefinitions.policy_read.output)(value);
    case "policy_update":
      return Schema.decodeUnknown(toolDefinitions.policy_update.output)(value);
    case "firewall_decisions":
      return Schema.decodeUnknown(toolDefinitions.firewall_decisions.output)(
        value,
      );
    default:
      return name satisfies never;
  }
}

function sendFailure(
  cause: unknown,
): SendError | RequestError | FirewallRefusal {
  if (!ProtocolError.isInstance(cause)) {
    const refused =
      (SdkHttpError.isInstance(cause) &&
        [400, 401, 403, 404, 405].includes(cause.status)) ||
      (cause instanceof Error &&
        cause.cause instanceof Error &&
        "code" in cause.cause &&
        cause.cause.code === "ECONNREFUSED");
    return new SendError({
      reason: refused ? "network-unavailable" : "outcome-unknown",
    });
  }
  const decoded = Effect.runSync(
    decodeHarnessSendErrorData(cause.data).pipe(Effect.either),
  );
  return Either.match(decoded, {
    onLeft: () => new SendError({ reason: "outcome-unknown" }),
    onRight: (data) => {
      if ("refusal" in data) return new FirewallRefusal(data);
      if ("failure" in data)
        return new RequestError({
          failure: data.failure,
          ...(data.operationId === undefined
            ? {}
            : { operationId: data.operationId }),
        });
      return new SendError(data);
    },
  });
}

function managementFailure(
  name: ToolName,
  cause: unknown,
): DomainFailure | McpToolError {
  if (!ProtocolError.isInstance(cause))
    return new McpToolError({ tool: name, reason: "transport-failed" });
  const data = cause.data;
  function decode<A, I, E>(
    schema: Schema.Schema<A, I>,
    create: (data: A) => E,
  ): E | McpToolError {
    return Either.match(Schema.decodeUnknownEither(schema)(data), {
      onLeft: () => new McpToolError({ tool: name, reason: "tool-failed" }),
      onRight: create,
    });
  }
  switch (name) {
    case "read_inbox":
      return decode(
        toolFailureSchemas.read_inbox,
        (data) => new InboxError(data),
      );
    case "read_event":
      return decode(
        toolFailureSchemas.read_event,
        (data) => new InboxError(data),
      );
    case "acknowledge_delivery":
      return decode(
        toolFailureSchemas.acknowledge_delivery,
        (data) => new DeliveryAcknowledgeError(data),
      );
    case "search_conversations":
      return decode(
        toolFailureSchemas.search_conversations,
        (data) => new HistoryError(data),
      );
    case "read_conversation":
      return decode(
        toolFailureSchemas.read_conversation,
        (data) => new HistoryError(data),
      );
    case "register":
      return decode(
        toolFailureSchemas.register,
        (data) => new RegistrationError(data),
      );
    case "policy_read":
      return decode(
        toolFailureSchemas.policy_read,
        (data) => new FirewallPolicyError(data),
      );
    case "policy_update":
      return decode(
        toolFailureSchemas.policy_update,
        (data) => new FirewallPolicyError(data),
      );
    case "firewall_decisions":
      return decode(
        toolFailureSchemas.firewall_decisions,
        (data) => new FirewallPolicyError(data),
      );
    default:
      return new McpToolError({ tool: name, reason: "tool-failed" });
  }
}

function consumePending<E, R>(
  client: Client,
  accept: (entry: InboxEntry) => Effect.Effect<void, E, R>,
): Effect.Effect<void, E | McpReceiveError, R> {
  return Effect.gen(function* () {
    let cursor: string | undefined;
    do {
      const page = yield* callTool(client, {
        name: "read_inbox",
        arguments: cursor === undefined ? {} : { cursor },
      }).pipe(
        Effect.mapError(
          () => new McpReceiveError({ reason: "transport-failed" }),
        ),
      );
      for (const entry of page.items) {
        yield* accept(entry);
        yield* callTool(client, {
          name: "acknowledge_delivery",
          arguments: { deliveryToken: entry.deliveryToken },
        }).pipe(
          Effect.mapError(
            () => new McpReceiveError({ reason: "transport-failed" }),
          ),
        );
      }
      cursor = page.nextCursor;
    } while (cursor !== undefined);
  });
}

/** Native reception owns transport errors independently of domain send failures. */
export { McpReceiveError };
