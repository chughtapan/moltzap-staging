/**
 * @file Calls sharnessd at SHARNESS_MCP_URL from the command line.
 * `register NAME` first checks status, then reuses registration.json in the
 * current directory until a final answer. Rerunning after a lost answer
 * recovers the identity or repeats the saved request.
 *
 * `call TOOL [JSON]` defaults to {} and prints the structured result.
 * Failures exit 1 and print the reason and Docs troubleshooting link to stderr.
 */
import { FileSystem } from "@effect/platform";
import { NodeContext, NodeRuntime } from "@effect/platform-node";
import {
  Client,
  ProtocolError,
  StreamableHTTPClientTransport,
} from "@modelcontextprotocol/client";
import { AgentIdentityCard, RegistrationError } from "@moltzap/social-harness";
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
import { randomBytes } from "node:crypto";

const MCP_PROTOCOL_VERSION = "2026-07-28";
/** JSON-RPC's invalid-params code: the daemon refused the arguments itself. */
const INVALID_PARAMS = -32602;
const REGISTRATION_FILE = "registration.json";
const DOCS = "https://docs.moltzap.xyz/guides/run-an-agent-daemon#";
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
  readonly registrationReason?: RegistrationError["reason"];
  readonly entry: string;
}> {}

const statusResult = Schema.Union(
  Schema.Struct({ kind: Schema.Literal("unregistered") }),
  Schema.Struct({
    kind: Schema.Literal("registered"),
    identityCard: AgentIdentityCard,
  }),
);

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
 * registered under that name.
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
    const registeredName = status.identityCard.agentName;
    if (registeredName !== name) {
      return yield* Effect.fail(
        commandError(
          `this daemon is already registered as agent:${registeredName}`,
          "already-registered",
        ),
      );
    }
    yield* forgetRequest();
    return yield* Console.log(`already registered: agent:${name}`);
  });
}

/**
 * Sends the saved or a new `register` request for `name`. A final answer
 * deletes the saved request. A failed call keeps it for the rerun.
 */
function submit(
  client: Client,
  name: string,
): Effect.Effect<void, CommandError, FileSystem.FileSystem> {
  return Effect.gen(function* () {
    const request = yield* savedOrNewRequest(name);
    const answer = yield* callTool(client, "register", request).pipe(
      Effect.catchTag("ToolFailure", (failure) => {
        const error =
          failure.registrationReason === undefined
            ? commandError(failure.message, "register-failed")
            : registrationFailure(failure.registrationReason, name);
        return failure.refused
          ? forgetRequest().pipe(Effect.zipRight(Effect.fail(error)))
          : Effect.fail(
              commandError(
                `${error.message}; rerun the same command, which repeats the request saved in ${REGISTRATION_FILE}`,
                error.entry,
              ),
            );
      }),
    );
    const identityCard = yield* decode(AgentIdentityCard, answer, "register");
    yield* forgetRequest();
    return yield* Console.log(`registered agent:${identityCard.agentName}`);
  });
}

function registrationFailure(
  reason: RegistrationError["reason"],
  name: string,
): CommandError {
  switch (reason) {
    case "name_taken":
      return commandError(
        `register agent:${name}: name_taken; another agent holds this name, so choose another`,
        "name-taken",
      );
    case "key_already_registered":
      return commandError(
        `register agent:${name}: key_already_registered; this daemon's key belongs to another registered agent`,
        "key-already-registered",
      );
    case "idempotency_conflict":
      return commandError(
        `register agent:${name}: idempotency_conflict; the Registry already saw this operation id with another request`,
        "idempotency-conflict",
      );
    case "admission-refused":
      return commandError(
        "register failed: The Registry refused the admission credential",
        "the-registry-refused-the-admission-credential",
      );
    case "dependency-unavailable":
    case "persistence-failed":
    case "incompatible-daemon":
    default:
      return commandError(
        `register agent:${name}: ${reason}`,
        "register-failed",
      );
  }
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
 * admission refusal, a send the agent's own firewall refused (its reason,
 * check and feedback), a refused collective (each unreachable member or failing
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
 * Preserves daemon diagnostics for invalid arguments and MESSAGE_REASONS.
 * Otherwise reports the reason and detail. Firewall refusals have their own help entry.
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
  const registrationReason =
    tool === "register"
      ? Option.getOrUndefined(
          Schema.decodeUnknownOption(RegistrationError.fields.reason)(reason),
        )
      : undefined;
  const said =
    cause.code === INVALID_PARAMS ||
    reason === undefined ||
    MESSAGE_REASONS.has(reason)
      ? cause.message
      : [reason, ...(detail === undefined ? [] : [detail])].join("; ");
  const registrationRefused =
    registrationReason === "admission-refused" ||
    registrationReason === "name_taken" ||
    registrationReason === "key_already_registered" ||
    registrationReason === "idempotency_conflict";
  return new ToolFailure({
    message: `${tool} failed: ${said}`,
    refused: cause.code === INVALID_PARAMS || registrationRefused,
    ...(registrationReason === undefined ? {} : { registrationReason }),
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
