/**
 * @file Calls the MCP tools of one `sharnessd` from the command line.
 * `SHARNESS_MCP_URL` names the daemon's MCP endpoint.
 *
 * `node mcp.ts register NAME` registers the daemon's agent under NAME. It
 * reads `status` first, so a rerun after a registration whose answer was
 * lost reports the active agent instead of registering again. Until the
 * daemon gives a final answer, the request waits in `registration.json` in
 * the current directory, and a rerun sends that same request, which the
 * daemon treats as the same registration.
 *
 * `node mcp.ts call TOOL [JSON]` calls one tool with a JSON object of
 * arguments, `{}` when omitted, and prints the tool's structured result.
 *
 * A refusal or failure exits 1 and prints the reason and the `Docs:` link of
 * the troubleshooting entry that names it on standard error.
 */
import { FileSystem } from "@effect/platform";
import { NodeContext, NodeRuntime } from "@effect/platform-node";
import {
  Client,
  ProtocolError,
  StreamableHTTPClientTransport,
} from "@modelcontextprotocol/client";
import { safeCredentialEndpoint } from "@moltzap/social-harness";
import {
  Config,
  Console,
  Data,
  Effect,
  Match,
  Option,
  Redacted,
  Schema,
  type Scope,
} from "effect";
import { randomBytes } from "node:crypto";

const MCP_PROTOCOL_VERSION = "2026-07-28";
/** JSON-RPC's invalid-params code: the daemon refused the arguments itself. */
const INVALID_PARAMS = -32602;
const REGISTRATION_FILE = "registration.json";
const DOCS = "https://docs.moltzap.xyz/guides/run-sharnessd#";
const USAGE =
  "usage: node mcp.ts register NAME, or node mcp.ts call TOOL [JSON]";

/**
 * A command that did not complete. `message` is what the reader sees, and
 * `entry` is the troubleshooting anchor that explains it.
 */
class CommandError extends Data.TaggedError("CommandError")<{
  readonly message: string;
  readonly entry: string;
}> {}

/**
 * A tool call that failed. `refused` marks a final daemon refusal, and
 * `entry` is the troubleshooting anchor that explains it.
 */
class ToolFailure extends Data.TaggedError("ToolFailure")<{
  readonly message: string;
  readonly refused: boolean;
  readonly entry: string;
}> {}

const statusResult = Schema.Union(
  Schema.Struct({ kind: Schema.Literal("unregistered") }),
  Schema.Struct({
    kind: Schema.Literal("active"),
    agent: Schema.Struct({ agentName: Schema.String }),
  }),
);

const registerResult = Schema.Struct({
  kind: Schema.Literal(
    "registered",
    "name_taken",
    "key_already_registered",
    "idempotency_conflict",
  ),
});

/** The `register` request, saved so that a rerun repeats it exactly. */
const savedRequest = Schema.Struct({
  operationId: Schema.String,
  principalId: Schema.String,
  agentName: Schema.String,
});

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
    Effect.mapError(() => commandError(USAGE, "helper-usage")),
  );
  // eslint-disable-next-line agent-code-guard/prefer-effect-platform -- two positional commands; the helper installs no @effect/cli
  const [command, first, second = "{}"] = process.argv.slice(2);
  if (command === "register" && first !== undefined) {
    return yield* register(url, first);
  }
  if (command === "call" && first !== undefined) {
    return yield* call(url, first, second);
  }
  return yield* Effect.fail(commandError(USAGE, "helper-usage"));
}).pipe(Effect.scoped);

NodeRuntime.runMain(
  program.pipe(
    Effect.tapError((error) =>
      Console.error(`${error.message}\nDocs: ${DOCS}${error.entry}`),
    ),
    Effect.provide(NodeContext.layer),
  ),
  { disableErrorReporting: true },
);

/**
 * Registers the daemon at `url` under `name`, or confirms it is already
 * active under that name.
 */
function register(
  url: URL,
  name: string,
): Effect.Effect<void, CommandError, FileSystem.FileSystem | Scope.Scope> {
  return Effect.gen(function* () {
    const client = yield* connect(url);
    const status = yield* callTool(client, "status", {}).pipe(
      Effect.mapError((failure) =>
        commandError(failure.message, failure.entry),
      ),
      Effect.flatMap((result) => decode(statusResult, result, "status")),
    );
    if (status.kind === "unregistered") {
      return yield* submit(client, name);
    }
    const active = status.agent.agentName;
    if (active !== name) {
      return yield* Effect.fail(
        commandError(
          `this daemon is already registered as agent:${active}`,
          "already-registered",
        ),
      );
    }
    return yield* Console.log(`already active: agent:${name}`);
  });
}

/**
 * Sends the saved or a new `register` request for `name`. A final answer
 * deletes the saved request; a failed call keeps it for the rerun.
 */
function submit(
  client: Client,
  name: string,
): Effect.Effect<void, CommandError, FileSystem.FileSystem> {
  return Effect.gen(function* () {
    const request = yield* savedOrNewRequest(name);
    const answer = yield* callTool(client, "register", request).pipe(
      Effect.catchTag("ToolFailure", (failure) =>
        failure.refused
          ? forgetRequest().pipe(
              Effect.zipRight(
                Effect.fail(commandError(failure.message, "register-failed")),
              ),
            )
          : Effect.fail(
              commandError(
                `${failure.message}; rerun the same command, which repeats the request saved in ${REGISTRATION_FILE}`,
                "register-failed",
              ),
            ),
      ),
    );
    const { kind } = yield* decode(registerResult, answer, "register");
    yield* forgetRequest();
    return yield* outcome(kind, name);
  });
}

function outcome(
  kind: typeof registerResult.Type.kind,
  name: string,
): Effect.Effect<void, CommandError> {
  return Match.value(kind).pipe(
    Match.when("registered", () => Console.log(`registered agent:${name}`)),
    Match.when("name_taken", () =>
      Effect.fail(
        commandError(
          `register agent:${name}: name_taken; another agent holds this name, so choose another`,
          "name-taken",
        ),
      ),
    ),
    Match.when("key_already_registered", () =>
      Effect.fail(
        commandError(
          `register agent:${name}: key_already_registered; this daemon's key belongs to another registered agent`,
          "key-already-registered",
        ),
      ),
    ),
    Match.when("idempotency_conflict", () =>
      Effect.fail(
        commandError(
          `register agent:${name}: idempotency_conflict; the Registry already saw this operation id with another request`,
          "idempotency-conflict",
        ),
      ),
    ),
    Match.exhaustive,
  );
}

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
          "helper-usage",
        ),
      ),
    );
    const client = yield* connect(url);
    const result = yield* callTool(client, tool, parsed).pipe(
      Effect.mapError((failure) =>
        commandError(failure.message, failure.entry),
      ),
    );
    yield* Console.log(JSON.stringify(result, null, 2));
  });
}

/**
 * The request saved in `registration.json` for `name`, or a new one created
 * there before it is sent. Creation fails rather than replace a request
 * another run saved at the same moment.
 */
function savedOrNewRequest(
  name: string,
): Effect.Effect<
  typeof savedRequest.Type,
  CommandError,
  FileSystem.FileSystem
> {
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    if (yield* fs.exists(REGISTRATION_FILE)) {
      const saved = yield* fs
        .readFileString(REGISTRATION_FILE)
        .pipe(Effect.flatMap(Schema.decode(Schema.parseJson(savedRequest))));
      if (saved.agentName !== name) {
        return yield* Effect.fail(
          commandError(
            `${REGISTRATION_FILE} registers agent:${saved.agentName}; rerun with that name, or delete ${REGISTRATION_FILE} to choose another`,
            "registration-json",
          ),
        );
      }
      return saved;
    }
    const request = {
      operationId: `opn_${randomBytes(16).toString("base64url")}`,
      principalId: `prn_${randomBytes(16).toString("base64url")}`,
      agentName: name,
    };
    yield* fs.writeFileString(
      REGISTRATION_FILE,
      `${JSON.stringify(request, null, 2)}\n`,
      { flag: "wx", mode: 0o600 },
    );
    return request;
  }).pipe(
    Effect.catchTags({
      BadArgument: (error) => Effect.fail(fileError(error.message)),
      SystemError: (error) => Effect.fail(fileError(error.message)),
      ParseError: (error) => Effect.fail(fileError(error.message)),
    }),
  );
}

function forgetRequest(): Effect.Effect<
  void,
  CommandError,
  FileSystem.FileSystem
> {
  return Effect.flatMap(FileSystem.FileSystem, (fs) =>
    fs.remove(REGISTRATION_FILE, { force: true }),
  ).pipe(
    Effect.catchTags({
      BadArgument: (error) => Effect.fail(fileError(error.message)),
      SystemError: (error) => Effect.fail(fileError(error.message)),
    }),
  );
}

/** Secret file paths and contents stay redacted outside the authenticated transport. */
const readMcpCredential = Effect.gen(function* () {
  const credentialFile = yield* Config.redacted(
    "SHARNESS_MCP_CREDENTIAL_FILE",
  ).pipe(
    Effect.mapError(() =>
      commandError(
        "SHARNESS_MCP_CREDENTIAL_FILE must name a readable file",
        "mcp-credential-file",
      ),
    ),
  );
  const fs = yield* FileSystem.FileSystem;
  const credential = yield* fs
    .readFileString(Redacted.value(credentialFile))
    .pipe(
      Effect.mapError(() =>
        commandError(
          "SHARNESS_MCP_CREDENTIAL_FILE must name a readable file",
          "mcp-credential-file",
        ),
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
        "could-not-connect",
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
            "could-not-connect",
          ),
      }).pipe(Effect.as(client)),
      () => Effect.tryPromise(() => client.close()).pipe(Effect.ignore),
    );
  });
}

function callTool(
  client: Client,
  tool: string,
  args: Readonly<Record<string, unknown>>,
): Effect.Effect<unknown, ToolFailure> {
  return Effect.tryPromise({
    try: (signal) =>
      client.callTool({ name: tool, arguments: { ...args } }, { signal }),
    catch: (cause) => toolFailure(tool, cause),
  }).pipe(
    Effect.flatMap((result) =>
      result.isError === true
        ? Effect.fail(
            new ToolFailure({
              message: `${tool} failed: ${result.content
                .flatMap((part) => (part.type === "text" ? [part.text] : []))
                .join("\n")}`,
              refused: false,
              entry: "tool-failed",
            }),
          )
        : Effect.succeed(result.structuredContent),
    ),
  );
}

/**
 * Reasons whose JSON-RPC message from the daemon already names the cause: an
 * admission refusal, a send the agent's own firewall refused (its check and
 * next action), a refused collective (each unreachable member or failing
 * field), and a refused policy edit (the failing rule and the problem).
 */
const MESSAGE_REASONS: ReadonlySet<string> = new Set([
  "admission-refused",
  "firewall-refused",
  "collective-failed",
  "invalid-rule",
  "fixed-rule",
]);

/**
 * The failure for a call the daemon or the transport rejected: the daemon's
 * message for invalid arguments, whose message names each invalid field, and
 * for {@link MESSAGE_REASONS}; otherwise its reason, then any detail. A
 * firewall refusal has its own troubleshooting entry.
 */
function toolFailure(tool: string, cause: unknown): ToolFailure {
  if (!ProtocolError.isInstance(cause)) {
    return new ToolFailure({
      message: `${tool} failed: ${String(cause)}`,
      refused: false,
      entry: "tool-failed",
    });
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
  return new ToolFailure({
    message: `${tool} failed: ${said}`,
    refused: cause.code === INVALID_PARAMS || reason === "admission-refused",
    entry: reason === "firewall-refused" ? "firewall-refused" : "tool-failed",
  });
}

function decode<A, I>(
  schema: Schema.Schema<A, I>,
  value: unknown,
  tool: string,
): Effect.Effect<A, CommandError> {
  return Schema.decodeUnknown(schema)(value).pipe(
    Effect.mapError(() =>
      commandError(
        `${tool} returned a result this script does not read: ${JSON.stringify(value)}`,
        "result-not-read",
      ),
    ),
  );
}

function fileError(detail: string): CommandError {
  return commandError(
    `could not use ${REGISTRATION_FILE}: ${detail}`,
    "registration-json",
  );
}

function commandError(message: string, entry: string): CommandError {
  return new CommandError({ message, entry });
}
