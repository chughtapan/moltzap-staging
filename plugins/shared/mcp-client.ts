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
  type ToolCallFailure,
  toolDefinitions,
  type ToolName,
  type ToolRequest,
  type ToolResult,
} from "@social-harness/social-harness";
import {
  Data,
  Effect,
  Fiber,
  Option,
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

type FailureContract<Name extends ToolName> =
  (typeof toolDefinitions)[Name]["failure"];

/**
 * How calling a tool can fail: the failure its definition reads from the
 * daemon's answer, and {@link McpToolError} for a tool whose definition does
 * not read every failure.
 */
export type ToolFailure<Name extends ToolName> =
  | Option.Option.Value<ReturnType<FailureContract<Name>["read"]>>
  | (FailureContract<Name>["total"] extends true ? never : McpToolError);

/** Native channels connect with the daemon credential supplied by their host. */
export interface McpConnectionSettings {
  readonly endpoint: URL;
  readonly credential: Redacted.Redacted;
  /** The host explicitly trusts plaintext HTTP on this connection. */
  readonly allowHttp?: boolean;
}

/** Inbox values contain a stable lookup token, content and saved local advice. */
export type InboxEntry = ToolResult<"receive_messages">["items"][number];

/**
 * Allows one inbox consumer at a time. Reading marks items delivered before the
 * callback runs. Callback failure stops reception; saved history retains the item.
 */
export interface McpConnection {
  readonly client: Client;
  readonly consumeInbox: <E, R>(
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

/** What a caller is told when a tool's definition does not read a failure. */
const reasons = {
  refused: "tool-failed",
  unanswered: "transport-failed",
  unreadable: "decode-failed",
} as const;

export function callTool<Name extends ToolName>(
  client: Pick<Client, "callTool">,
  request: ToolRequest & { readonly name: Name },
): Effect.Effect<ToolResult<Name>, ToolFailure<Name>>;
/**
 * Calls the named MCP tool once and never retries. The tool's definition
 * decodes its result and reads its failures.
 */
export function callTool(
  client: Pick<Client, "callTool">,
  request: ToolRequest,
): Effect.Effect<unknown, unknown> {
  const definition: ToolContract = toolDefinitions[request.name];
  const fail = (failure: ToolCallFailure) =>
    Option.getOrElse(
      definition.failure.read(failure),
      () =>
        new McpToolError({ tool: request.name, reason: reasons[failure.kind] }),
    );
  return Effect.tryPromise({
    try: (signal) => client.callTool(request, { signal }),
    catch: (cause) => fail(observed(cause)),
  }).pipe(
    Effect.flatMap((response) =>
      response.isError === true
        ? Effect.fail(fail({ kind: "refused", data: undefined }))
        : Schema.decodeUnknown(definition.output)(
            response.structuredContent,
          ).pipe(
            Effect.map((result): unknown => result),
            Effect.mapError(() => fail({ kind: "unreadable" })),
          ),
    ),
  );
}

/** The part of any tool's definition a call needs: its result and how its failures read. */
interface ToolContract {
  readonly output: Schema.Schema.AnyNoContext;
  readonly failure: {
    readonly read: (failure: ToolCallFailure) => Option.Option<unknown>;
  };
}

/** What a rejected call says about whether the daemon received the request. */
function observed(cause: unknown): ToolCallFailure {
  if (ProtocolError.isInstance(cause)) {
    return { kind: "refused", data: cause.data };
  }
  const refusedBeforeTheTool =
    SdkHttpError.isInstance(cause) &&
    [400, 401, 403, 404, 405].includes(cause.status);
  return {
    kind: "unanswered",
    sent: !(refusedBeforeTheTool || connectionRefused(cause)),
  };
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
        accept: (entry: InboxEntry) => Effect.Effect<void, E, R>,
      ) =>
        runReception(
          { client, receiving, scope },
          inboxWakeups(endpoint, transportOptions).pipe(Stream.as(undefined)),
          accept,
        ),
    } satisfies McpConnection;
  }).pipe(Effect.withSpan("Mcp.acquireConnection"));
}

function consumePending<E, R>(
  client: Pick<Client, "callTool">,
  accept: (entry: InboxEntry) => Effect.Effect<void, E, R>,
): Effect.Effect<void, E | McpReceiveError, R> {
  return Effect.gen(function* () {
    let cursor: string | undefined;
    do {
      const page = yield* callTool(client, {
        name: "receive_messages",
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
