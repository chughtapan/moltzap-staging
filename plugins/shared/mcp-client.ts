/** @file Scoped MCP connections and tool calls shared by native host plugins. */
import {
  Client,
  type Implementation,
  ProtocolError,
  SdkError,
  SdkErrorCode,
  SdkHttpError,
  StreamableHTTPClientTransport,
  type StreamableHTTPClientTransportOptions,
} from "@modelcontextprotocol/client";
import {
  decodeHarnessSendErrorData,
  FirewallPolicyError,
  FirewallRefusal,
  HistoryError,
  InboxError,
  RegistrationError,
  RequestError,
  SendError,
  toolDefinitions,
  toolFailureSchemas,
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
  type ParseResult,
  Redacted,
  Ref,
  Schema,
  Scope,
  Stream,
} from "effect";
import { inboxWakeups, McpReceiveError } from "./inbox-wakeups.js";

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
  | HistoryError
  | RegistrationError
  | FirewallPolicyError;

/** Native channels connect with the daemon credential supplied by their host. */
export interface McpConnectionSettings {
  readonly endpoint: URL;
  readonly credential: Redacted.Redacted;
  /** The host explicitly trusts plaintext HTTP on this connection. */
  readonly allowHttp?: boolean;
}

/**
 * Polling is the default. The `onReady` callback runs before the first poll or
 * after the push stream becomes active, before reading pending items.
 */
export interface InboxOptions {
  readonly receiveMode?: "push" | "poll";
  readonly onReady?: Effect.Effect<void>;
}

/** Inbox values contain a stable lookup token, content and saved local advice. */
export type InboxEntry = ToolResult<"read_inbox">["items"][number];

/**
 * Allows one inbox consumer at a time. Reading marks items delivered before the
 * callback runs. Callback failure stops reception; saved history retains the item.
 */
export interface McpConnection {
  readonly client: Client;
  readonly consumeInbox: <E, R>(
    options: InboxOptions,
    accept: (entry: InboxEntry) => Effect.Effect<void, E, R>,
  ) => Effect.Effect<void, E | McpReceiveError, R>;
}

/** TLS, literal loopback or host-approved HTTP can carry the private bearer. */
export function safeCredentialEndpoint(
  endpoint: URL,
  allowHttp = false,
): boolean {
  const privateAuthority =
    !endpoint.username && !endpoint.password && !endpoint.hash;
  const local =
    endpoint.hostname === "127.0.0.1" || endpoint.hostname === "[::1]";
  const secure =
    endpoint.protocol === "https:" ||
    (endpoint.protocol === "http:" && (local || allowHttp));
  return privateAuthority && secure;
}

/**
 * Returns after MCP negotiation succeeds. Closing the caller's scope interrupts
 * active reception and closes the client.
 */
export function acquireMcpConnection(
  settings: McpConnectionSettings,
  implementation: Implementation,
): Effect.Effect<McpConnection, McpConnectError, Scope.Scope> {
  const endpoint = new URL(settings.endpoint.href);
  return acquireConnection({ ...settings, endpoint }, { ...implementation });
}

export function callTool<Name extends ToolName>(
  client: Pick<Client, "callTool">,
  request: ToolRequest & { readonly name: Name },
): Effect.Effect<ToolResult<Name>, ToolFailure<Name>>;
/** Calls the named MCP tool once. An uncertain send never triggers a retry. */
export function callTool(
  client: Pick<Client, "callTool">,
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

function acquireConnection(
  settings: McpConnectionSettings,
  implementation: Implementation,
): Effect.Effect<McpConnection, McpConnectError, Scope.Scope> {
  return Effect.gen(function* () {
    const endpoint = settings.endpoint;
    const transportOptions: StreamableHTTPClientTransportOptions = {
      requestInit: {
        headers: {
          Authorization: `Bearer ${Redacted.value(settings.credential)}`,
        },
        redirect: "error",
      },
    };
    if (!safeCredentialEndpoint(endpoint, settings.allowHttp)) {
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
            new StreamableHTTPClientTransport(endpoint, transportOptions),
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
      ) => {
        const ready = options.onReady ?? Effect.void;
        const wakeups =
          options.receiveMode === "push"
            ? inboxWakeups(endpoint, transportOptions, ready).pipe(
                Stream.as(undefined),
              )
            : pollingWakeups(options);
        return runReception({ client, receiving, scope }, wakeups, accept);
      },
    } satisfies McpConnection;
  }).pipe(Effect.withSpan("Mcp.acquireConnection"));
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
    const httpRefused =
      SdkHttpError.isInstance(cause) &&
      [400, 401, 403, 404, 405].includes(cause.status);
    const refused = httpRefused || connectionRefused(cause);
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
      if ("refusal" in data) {
        return new FirewallRefusal(data);
      }
      if ("failure" in data) {
        return new RequestError({
          failure: data.failure,
          ...(data.operationId === undefined
            ? {}
            : { operationId: data.operationId }),
        });
      }
      return new SendError(data);
    },
  });
}

function managementFailure(
  name: ToolName,
  cause: unknown,
): DomainFailure | McpToolError {
  if (!ProtocolError.isInstance(cause)) {
    return new McpToolError({ tool: name, reason: "transport-failed" });
  }
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
    case "event_subscription_status":
    case "revoke_event_subscription":
    case "resume_event_subscription":
    case "send_message":
    case "status":
      return new McpToolError({ tool: name, reason: "tool-failed" });
    default:
      return name satisfies never;
  }
}

function consumePending<E, R>(
  client: Pick<Client, "callTool">,
  accept: (entry: InboxEntry) => Effect.Effect<void, E, R>,
): Effect.Effect<void, E | McpReceiveError, R> {
  return Effect.gen(function* () {
    let cursor: string | undefined;
    do {
      const page = yield* callTool(client, {
        name: "read_inbox",
        arguments: cursor === undefined ? {} : { cursor },
      }).pipe(
        Effect.catchTags({
          InboxError: () =>
            Effect.fail(new McpReceiveError({ reason: "transport-failed" })),
          McpToolError: () =>
            Effect.fail(new McpReceiveError({ reason: "transport-failed" })),
        }),
      );
      for (const entry of page.items) {
        yield* accept(entry);
      }
      cursor = page.nextCursor;
    } while (cursor !== undefined);
  });
}

/** Native reception owns transport errors independently of domain send failures. */
export { McpReceiveError };

function connectionRefused(cause: unknown): boolean {
  if (!(cause instanceof Error) || !(cause.cause instanceof Error)) {
    return false;
  }
  return "code" in cause.cause && cause.cause.code === "ECONNREFUSED";
}

function runReception<E, R>(
  owner: {
    readonly client: Pick<Client, "callTool">;
    readonly receiving: Ref.Ref<boolean>;
    readonly scope: Scope.Scope;
  },
  wakeups: Stream.Stream<undefined, McpReceiveError>,
  accept: (entry: InboxEntry) => Effect.Effect<void, E, R>,
) {
  return Effect.acquireUseRelease(
    Effect.forkIn(
      Effect.interruptible(
        receiveInbox(owner.client, owner.receiving, wakeups, accept),
      ),
      owner.scope,
    ),
    Fiber.join,
    Fiber.interrupt,
  );
}

function receiveInbox<E, R>(
  client: Pick<Client, "callTool">,
  receiving: Ref.Ref<boolean>,
  wakeups: Stream.Stream<undefined, McpReceiveError>,
  accept: (entry: InboxEntry) => Effect.Effect<void, E, R>,
) {
  return Effect.scoped(
    Effect.gen(function* () {
      yield* Effect.acquireRelease(
        Ref.getAndSet(receiving, true).pipe(
          Effect.filterOrFail(
            (active) => !active,
            () => new McpReceiveError({ reason: "already-listening" }),
          ),
        ),
        () => Ref.set(receiving, false),
      );
      yield* wakeups.pipe(
        Stream.runForEach(() => consumePending(client, accept)),
      );
    }),
  );
}

function pollingWakeups(options: Pick<InboxOptions, "onReady">) {
  return Stream.concat(
    Stream.fromEffect(options.onReady ?? Effect.void),
    Stream.tick(Duration.millis(250)),
  ).pipe(Stream.as(undefined));
}
