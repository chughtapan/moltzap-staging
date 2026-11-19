/** @file What a host needs beyond the official MCP client: a connection made from its environment, the inbox reader, and a send whose outcome is always stated. */
import {
  type CallToolResult,
  Client,
  type Implementation,
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
  Redacted,
  Ref,
  Schema,
  Scope,
} from "effect";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- Native host plugins read one credential without requiring a platform service from the host.
import { readFile } from "node:fs/promises";
import { plainUrl } from "../encoding.js";
import {
  type InboxEntry,
  MAXIMUM_WAIT_SECONDS,
  SendError,
  toolDefinitions,
} from "./tools.js";

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

/** Native channels connect with the daemon credential supplied by their host. */
export interface McpConnectionSettings {
  readonly endpoint: URL;
  readonly credential: Redacted.Redacted;
  /** The host explicitly trusts plaintext HTTP on this connection. */
  readonly allowHttp?: boolean;
}

/**
 * Allows one inbox consumer at a time. A page is committed when the reader
 * comes back for the next one, after the callback has accepted every entry in
 * it. Callback failure stops reception; the entries of that page are read
 * again by the next consumer.
 */
export interface McpConnection {
  /** The MCP SDK's connected `Client`, which a host calls tools on. */
  readonly mcp: Client;
  readonly consumeInbox: <E, R>(
    accept: (entry: InboxEntry) => Effect.Effect<void, E, R>,
  ) => Effect.Effect<void, E | McpReceiveError, R>;
}

/**
 * Returns after MCP negotiation succeeds. Closing the caller's scope interrupts
 * active reception and closes the connection.
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
    const mcp = new Client(
      { ...implementation },
      {
        capabilities: {},
        versionNegotiation: { mode: { pin: "2026-07-28" } },
      },
    );
    const close = Effect.tryPromise(() => mcp.close()).pipe(Effect.ignore);
    yield* Effect.acquireRelease(
      Effect.tryPromise({
        try: (signal) =>
          mcp.connect(
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
      mcp,
      consumeInbox: (accept) =>
        Effect.acquireUseRelease(
          receiveInbox(mcp, receiving, accept).pipe(
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

/**
 * Calls `send_message` once and never retries, and returns the tool's result
 * as the MCP client gives it. A call that ends without a readable result is
 * answered as a failed send whose reason is `network-unavailable` when the
 * request cannot have reached the daemon, and otherwise `outcome-unknown`: the
 * message may have been sent, so a host must not send it again.
 */
export function sendMessage(
  mcp: Pick<Client, "callTool">,
  sendArguments: Readonly<Record<string, unknown>>,
): Effect.Effect<CallToolResult> {
  return Effect.tryPromise((signal) =>
    mcp.callTool(
      { name: "send_message", arguments: sendArguments },
      { signal, timeout: CALL_TIMEOUT_MILLISECONDS },
    ),
  ).pipe(
    Effect.map((result) =>
      result.isError === true ||
      Schema.is(Schema.encodedSchema(toolDefinitions.send_message.output))(
        result.structuredContent,
      )
        ? result
        : unanswered("outcome-unknown"),
    ),
    Effect.catchAll(({ error }) =>
      Effect.succeed(
        unanswered(
          requestNotSent(error) ? "network-unavailable" : "outcome-unknown",
        ),
      ),
    ),
  );
}

/** The failed result of a send the daemon gave no readable answer to. */
function unanswered(
  reason: "network-unavailable" | "outcome-unknown",
): CallToolResult {
  return {
    isError: true,
    content: [{ type: "text", text: new SendError({ reason }).message }],
    structuredContent: { reason },
  };
}

/** Whether a rejected call shows the daemon never received the request. */
function requestNotSent(cause: unknown): boolean {
  return (
    (SdkHttpError.isInstance(cause) &&
      [400, 401, 403, 404, 405].includes(cause.status)) ||
    connectionRefused(cause)
  );
}

/**
 * One page of the pending inbox, waiting for a message when it is empty.
 * `cursor` is the `nextCursor` of the page before it, which the daemon then
 * does not return again.
 */
function receivePage(mcp: Pick<Client, "callTool">, cursor?: string) {
  const unreadable = () =>
    Effect.fail(new McpReceiveError({ reason: "transport-failed" }));
  return Effect.tryPromise((signal) =>
    mcp.callTool(
      {
        name: "receive_messages",
        arguments: {
          wait: MAXIMUM_WAIT_SECONDS,
          ...(cursor === undefined ? {} : { cursor }),
        },
      },
      { signal, timeout: CALL_TIMEOUT_MILLISECONDS },
    ),
  ).pipe(
    Effect.filterOrFail(
      (result) => result.isError !== true,
      () => new McpReceiveError({ reason: "transport-failed" }),
    ),
    Effect.flatMap((result) =>
      Schema.decodeUnknown(toolDefinitions.receive_messages.output)(
        result.structuredContent,
      ),
    ),
    Effect.catchTags({ UnknownException: unreadable, ParseError: unreadable }),
  );
}

function connectionRefused(cause: unknown): boolean {
  if (!(cause instanceof Error) || !(cause.cause instanceof Error)) {
    return false;
  }
  return "code" in cause.cause && cause.cause.code === "ECONNREFUSED";
}

/** Reads the inbox until interrupted or a read fails. A connection has one reader at a time. */
function receiveInbox<E, R>(
  mcp: Pick<Client, "callTool">,
  receiving: Ref.Ref<boolean>,
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
      let cursor: string | undefined;
      while (true) {
        const page = yield* receivePage(mcp, cursor);
        for (const entry of page.entries) {
          yield* accept(entry);
        }
        cursor = page.nextCursor ?? cursor;
      }
    }),
  );
}

/** TLS, literal loopback or host-approved HTTP can carry the private bearer. */
function safeCredentialEndpoint(endpoint: URL, allowHttp = false): boolean {
  const local =
    endpoint.hostname === "127.0.0.1" || endpoint.hostname === "[::1]";
  return (
    plainUrl(
      endpoint,
      local || allowHttp ? ["https:", "http:"] : ["https:"],
    ) !== undefined
  );
}
