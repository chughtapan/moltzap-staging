/**
 * @file Calls one sharnessd tool at SHARNESS_MCP_URL from the command line.
 *
 * `call TOOL [JSON]` sends the JSON object as the tool's arguments, {} when
 * omitted, and prints the structured result. A failure exits 1 and prints the
 * reason to stderr.
 */
import { FileSystem } from "@effect/platform";
import { NodeContext, NodeRuntime } from "@effect/platform-node";
import {
  Client,
  ProtocolError,
  StreamableHTTPClientTransport,
} from "@modelcontextprotocol/client";
import {
  Config,
  Console,
  Data,
  Effect,
  Option,
  Redacted,
  Schema,
  type Scope,
} from "effect";

const MCP_PROTOCOL_VERSION = "2026-07-28";
/** JSON-RPC's invalid-params code: the daemon refused the arguments itself. */
const INVALID_PARAMS = -32602;
const USAGE = "usage: node mcp.ts call TOOL [JSON]";

/** A command that did not complete. `message` is what the reader sees. */
class CommandError extends Data.TaggedError("CommandError")<{
  readonly message: string;
}> {}

/** The JSON-RPC error data the daemon attaches to a failed tool call. */
const failureData = Schema.Struct({
  reason: Schema.optional(Schema.String),
  detail: Schema.optional(Schema.String),
});

const toolArguments = Schema.parseJson(
  Schema.Record({ key: Schema.String, value: Schema.Unknown }),
);

const program = Effect.gen(function* () {
  const url = yield* Config.url("SHARNESS_MCP_URL").pipe(
    Effect.mapError(() => commandError(USAGE)),
  );
  // eslint-disable-next-line agent-code-guard/prefer-effect-platform -- two positional commands; the helper installs no @effect/cli
  const [command, first, second = "{}"] = process.argv.slice(2);
  if (command === "call" && first !== undefined) {
    return yield* call(url, first, second);
  }
  return yield* Effect.fail(commandError(USAGE));
}).pipe(Effect.scoped);

NodeRuntime.runMain(
  program.pipe(
    Effect.tapError((error) => Console.error(error.message)),
    Effect.provide(NodeContext.layer),
  ),
  { disableErrorReporting: true },
);

function call(
  url: URL,
  tool: string,
  json: string,
): Effect.Effect<void, CommandError, Scope.Scope | FileSystem.FileSystem> {
  return Effect.gen(function* () {
    const parsed = yield* Schema.decode(toolArguments)(json).pipe(
      Effect.mapError(() =>
        commandError(
          `the arguments must be one JSON object, such as '{"input":{"to":"agent:bob","text":"Hi"}}'`,
        ),
      ),
    );
    const client = yield* connect(url);
    const result = yield* callTool(client, tool, parsed);
    yield* Console.log(JSON.stringify(result, null, 2));
  });
}

/** Secret file paths and contents stay redacted outside the authenticated transport. */
const readMcpCredential = Effect.gen(function* () {
  const credentialFile = yield* Config.redacted(
    "SHARNESS_MCP_CREDENTIAL_FILE",
  ).pipe(
    Effect.mapError(() =>
      commandError("SHARNESS_MCP_CREDENTIAL_FILE must name a readable file"),
    ),
  );
  const fs = yield* FileSystem.FileSystem;
  const credential = yield* fs
    .readFileString(Redacted.value(credentialFile))
    .pipe(
      Effect.mapError(() =>
        commandError("SHARNESS_MCP_CREDENTIAL_FILE must name a readable file"),
      ),
    );
  return Redacted.make(credential);
});

function connect(
  url: URL,
): Effect.Effect<Client, CommandError, Scope.Scope | FileSystem.FileSystem> {
  return Effect.gen(function* () {
    if (!safeCredentialEndpoint(url)) {
      return yield* commandError(
        "MCP URL must use HTTPS or literal HTTP loopback without embedded credentials or fragments",
      );
    }
    const credential = yield* readMcpCredential;
    const client = new Client(
      { name: "sharness-mcp", version: "1.0.0" },
      {
        capabilities: {},
        versionNegotiation: { mode: { pin: MCP_PROTOCOL_VERSION } },
      },
    );
    return yield* Effect.acquireRelease(
      Effect.tryPromise({
        try: (signal) =>
          client.connect(
            new StreamableHTTPClientTransport(url, {
              requestInit: {
                headers: {
                  Authorization: `Bearer ${Redacted.value(credential)}`,
                },
                redirect: "error",
              },
            }),
            { signal },
          ),
        catch: () =>
          commandError(
            `could not connect to ${url.href}; check that sharnessd is running and that SHARNESS_MCP_URL names its port`,
          ),
      }).pipe(Effect.as(client)),
      () => Effect.tryPromise(() => client.close()).pipe(Effect.ignore),
    );
  });
}

/** The bearer can travel over TLS or literal loopback without URL credentials. */
function safeCredentialEndpoint(endpoint: URL): boolean {
  const privateAuthority =
    !endpoint.username && !endpoint.password && !endpoint.hash;
  const local =
    endpoint.hostname === "127.0.0.1" || endpoint.hostname === "[::1]";
  return (
    privateAuthority &&
    (endpoint.protocol === "https:" || (endpoint.protocol === "http:" && local))
  );
}

function callTool(
  client: Client,
  tool: string,
  args: Readonly<Record<string, unknown>>,
): Effect.Effect<unknown, CommandError> {
  return Effect.tryPromise({
    try: (signal) =>
      client.callTool({ name: tool, arguments: { ...args } }, { signal }),
    catch: (cause) => toolFailure(tool, cause),
  }).pipe(
    Effect.flatMap((result) =>
      result.isError === true
        ? Effect.fail(
            commandError(
              `${tool} failed: ${result.content
                .flatMap((part) => (part.type === "text" ? [part.text] : []))
                .join("\n")}`,
            ),
          )
        : Effect.succeed(result.structuredContent),
    ),
  );
}

/**
 * Reasons whose JSON-RPC message from the daemon already names the cause: a
 * send the agent's own firewall refused (its check and
 * next action), a refused collective (each unreachable member or failing
 * field), and a refused policy edit (the failing rule and the problem).
 */
const MESSAGE_REASONS: ReadonlySet<string> = new Set([
  "firewall-refused",
  "collective-failed",
  "invalid-rule",
  "fixed-rule",
]);

/**
 * Preserves daemon diagnostics for invalid arguments and MESSAGE_REASONS.
 * Otherwise reports the reason and detail.
 */
function toolFailure(tool: string, cause: unknown): CommandError {
  if (!ProtocolError.isInstance(cause)) {
    return commandError(`${tool} failed: ${String(cause)}`);
  }
  const { reason, detail } = Option.getOrElse(
    Schema.decodeUnknownOption(failureData)(cause.data),
    (): typeof failureData.Type => ({}),
  );
  const said =
    cause.code === INVALID_PARAMS ||
    reason === undefined ||
    MESSAGE_REASONS.has(reason)
      ? cause.message
      : [reason, ...(detail === undefined ? [] : [detail])].join("; ");
  return commandError(`${tool} failed: ${said}`);
}

function commandError(message: string): CommandError {
  return new CommandError({ message });
}
