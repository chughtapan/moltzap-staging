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
  Config,
  Data,
  Effect,
  Fiber,
  Option,
  Redacted,
  Ref,
  Schema,
  Scope,
} from "effect";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- Native host plugins read one credential without requiring a platform service from the host.
import { readFile } from "node:fs/promises";
import type { InboxEntry } from "./message-format.js";
import {
  MAXIMUM_WAIT_SECONDS,
  type ToolCallFailure,
  toolDefinitions,
  type ToolName,
  type ToolRequest,
  type ToolResult,
} from "./contracts.js";

/** Connection failures omit the private endpoint and credential. */
export class McpConnectError extends Data.TaggedError("McpConnectError")<{
  readonly reason: "transport-failed" | "incompatible-daemon";
}> {}

/** Reception ended: the daemon could not be read, or the connection already has a reader. */
export class McpReceiveError extends Data.TaggedError("McpReceiveError")<{
  readonly reason: "transport-failed" | "already-listening";
}> {}

/** The host's environment does not say how to reach the daemon. `message` names the variable to set. */
export class McpSettingsError extends Data.TaggedError("McpSettingsError")<{
  readonly message: string;
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

/**
 * Returns after MCP negotiation succeeds. Closing the caller's scope interrupts
 * active reception and closes the client.
 */
export function acquireMcpConnection(
  settings: McpConnectionSettings,
  implementation: Implementation,
): Effect.Effect<McpConnection, McpConnectError, Scope.Scope> {
  const { endpoint } = settings;
  const transportOptions: StreamableHTTPClientTransportOptions = {
    requestInit: {
      headers: {
        Authorization: `Bearer ${Redacted.value(settings.credential)}`,
      },
      redirect: "error",
    },
  };
  return Effect.gen(function* () {
    if (!safeCredentialEndpoint(endpoint, settings.allowHttp)) {
      return yield* new McpConnectError({ reason: "transport-failed" });
    }
    const client = new Client(
      { ...implementation },
      {
        capabilities: {},
        versionNegotiation: { mode: { pin: "2026-07-28" } },
      },
    );
    const close = Effect.tryPromise(() => client.close()).pipe(Effect.ignore);
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
      }).pipe(Effect.onError(() => close)),
      () => close,
    );
    const receiving = yield* Ref.make(false);
    const scope = yield* Scope.Scope;
    return {
      client,
      consumeInbox: (accept) =>
        Effect.acquireUseRelease(
          receiveInbox({ client, receiving }, accept).pipe(
            Effect.interruptible,
            Effect.forkIn(scope),
          ),
          Fiber.join,
          Fiber.interrupt,
        ),
    } satisfies McpConnection;
  }).pipe(Effect.withSpan("Mcp.acquireConnection"));
}

/** Longer than the longest wait a tool accepts, so a waiting read is not cut short. */
const CALL_TIMEOUT_MILLISECONDS = (MAXIMUM_WAIT_SECONDS + 30) * 1000;

const settingsError = (message: string) => () =>
  Effect.fail(new McpSettingsError({ message }));

/**
 * Connects the way the host process is configured: `SHARNESS_MCP_URL` names
 * the daemon, `SHARNESS_MCP_CREDENTIAL_FILE` the file holding its bearer, and
 * `SHARNESS_MCP_ALLOW_HTTP=true` permits plaintext HTTP to it.
 */
export const connectFromEnvironment = Effect.fn("Mcp.connectFromEnvironment")(
  function* (implementation: Implementation) {
    const endpoint = yield* Config.url("SHARNESS_MCP_URL").pipe(
      Effect.catchTag(
        "ConfigError",
        settingsError("SHARNESS_MCP_URL must be the daemon's MCP URL"),
      ),
    );
    const allowHttp = yield* Config.boolean("SHARNESS_MCP_ALLOW_HTTP").pipe(
      Config.withDefault(false),
      Effect.catchTag(
        "ConfigError",
        settingsError("SHARNESS_MCP_ALLOW_HTTP must be true or false"),
      ),
    );
    const unreadable = settingsError(
      "SHARNESS_MCP_CREDENTIAL_FILE must name a readable file",
    );
    const credentialFile = yield* Config.redacted(
      "SHARNESS_MCP_CREDENTIAL_FILE",
    ).pipe(Effect.catchTag("ConfigError", unreadable));
    const credential = yield* Effect.tryPromise(() =>
      readFile(Redacted.value(credentialFile), "utf8"),
    ).pipe(Effect.catchTag("UnknownException", unreadable));
    return yield* acquireMcpConnection(
      { endpoint, credential: Redacted.make(credential), allowHttp },
      implementation,
    );
  },
);

/** What a caller is told when a tool's definition does not read a failure. */
const reasons = {
  refused: "tool-failed",
  unanswered: "transport-failed",
  unreadable: "decode-failed",
} as const;

/**
 * Calls the named MCP tool once and never retries. The tool's definition
 * decodes its result and reads its failures.
 */
export function callTool<Name extends ToolName>(
  client: Pick<Client, "callTool">,
  request: ToolRequest & { readonly name: Name },
): Effect.Effect<ToolResult<Name>, ToolFailure<Name>> {
  return callToolAsWritten(client, request.name, request.arguments);
}

export function callToolAsWritten<Name extends ToolName>(
  client: Pick<Client, "callTool">,
  name: Name,
  written: Readonly<Record<string, unknown>>,
): Effect.Effect<ToolResult<Name>, ToolFailure<Name>>;
/**
 * Calls a tool as {@link callTool} does, with arguments as an agent wrote
 * them. The daemon validates them, and its refusal names each field the tool
 * does not take.
 */
export function callToolAsWritten(
  client: Pick<Client, "callTool">,
  name: ToolName,
  written: Readonly<Record<string, unknown>>,
): Effect.Effect<unknown, unknown> {
  const definition: ToolContract = toolDefinitions[name];
  const fail = (failure: ToolCallFailure) =>
    Option.getOrElse(
      definition.failure.read(failure),
      () => new McpToolError({ tool: name, reason: reasons[failure.kind] }),
    );
  return Effect.tryPromise({
    try: (signal) =>
      client.callTool(
        { name, arguments: written },
        { signal, timeout: CALL_TIMEOUT_MILLISECONDS },
      ),
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

/**
 * Hands over everything pending. The first read waits for a message when the
 * inbox is empty, so calling this in a loop neither spins nor misses one.
 */
function readPending<E, R>(
  client: Pick<Client, "callTool">,
  accept: (entry: InboxEntry) => Effect.Effect<void, E, R>,
): Effect.Effect<void, E | McpReceiveError, R> {
  return Effect.gen(function* () {
    let cursor: string | undefined;
    do {
      const page = yield* callTool(client, {
        name: "receive_messages",
        arguments:
          cursor === undefined ? { wait: MAXIMUM_WAIT_SECONDS } : { cursor },
      }).pipe(
        Effect.catchTags({
          InboxError: () =>
            Effect.fail(new McpReceiveError({ reason: "transport-failed" })),
          McpToolError: () =>
            Effect.fail(new McpReceiveError({ reason: "transport-failed" })),
        }),
      );
      for (const entry of page.entries) {
        yield* accept(entry);
      }
      cursor = page.nextCursor;
    } while (cursor !== undefined);
  });
}

function connectionRefused(cause: unknown): boolean {
  if (!(cause instanceof Error) || !(cause.cause instanceof Error)) {
    return false;
  }
  return "code" in cause.cause && cause.cause.code === "ECONNREFUSED";
}

/** Reads the inbox until interrupted or a read fails. A connection has one reader at a time. */
function receiveInbox<E, R>(
  owner: {
    readonly client: Pick<Client, "callTool">;
    readonly receiving: Ref.Ref<boolean>;
  },
  accept: (entry: InboxEntry) => Effect.Effect<void, E, R>,
) {
  return Effect.scoped(
    Effect.gen(function* () {
      yield* Effect.acquireRelease(
        Ref.getAndSet(owner.receiving, true).pipe(
          Effect.filterOrFail(
            (active) => !active,
            () => new McpReceiveError({ reason: "already-listening" }),
          ),
        ),
        () => Ref.set(owner.receiving, false),
      );
      return yield* Effect.forever(readPending(owner.client, accept));
    }),
  );
}

/** TLS, literal loopback or host-approved HTTP can carry the private bearer. */
function safeCredentialEndpoint(endpoint: URL, allowHttp = false): boolean {
  const privateAuthority =
    !endpoint.username && !endpoint.password && !endpoint.hash;
  const local =
    endpoint.hostname === "127.0.0.1" || endpoint.hostname === "[::1]";
  const secure =
    endpoint.protocol === "https:" ||
    (endpoint.protocol === "http:" && (local || allowHttp));
  return privateAuthority && secure;
}
