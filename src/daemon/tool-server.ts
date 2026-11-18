/**
 * @file The daemon's MCP handler and the tools it serves. Each request is
 * answered from one agent: a call's arguments are decoded, its operation run,
 * its result encoded and its failure worded by the tool's definition.
 */

import { HttpApp } from "@effect/platform";
import {
  createMcpHandler,
  McpServer,
  ProtocolError,
} from "@modelcontextprotocol/server";
import {
  Cause,
  Data,
  Effect,
  Exit,
  JSONSchema,
  Option,
  ParseResult,
  Runtime,
  Schema,
} from "effect";
import { issuesText, type ValidationIssue } from "../communication/index.js";

/** What a request is answered from: an agent's tools, in listing order, and its Events calls. */
export interface AgentCalls {
  readonly tools: ReadonlyArray<ReturnType<typeof bind>>;
  readonly events: Readonly<
    Record<
      "list" | "subscribe" | "unsubscribe",
      (
        params: unknown,
      ) => Effect.Effect<Readonly<Record<string, unknown>>, JsonRpcError>
    >
  >;
}

/** The JSON-RPC error a tool call or an Events call is answered with. */
export class JsonRpcError extends Data.TaggedError("JsonRpcError")<{
  readonly code: number;
  readonly message: string;
  readonly data?: unknown;
}> {
  constructor(code: number, message: string, data?: unknown) {
    super({ code, message, data });
  }
}

/**
 * One admitted HTTP request: the agent that answers it, the runtime of the
 * fiber that carries it and the signal that it was aborted. It travels through
 * the MCP SDK as the request's authentication info, which the SDK reads
 * nothing from and hands to the server it builds for the request.
 */
class AdmittedRequest extends Data.Class<{
  readonly agent: AgentCalls;
  readonly runtime: Runtime.Runtime<never>;
  readonly signal: AbortSignal;
}> {}

/** The Events capability, which the SDK's capability type does not name. */
const eventsCapability = { events: {} };

const anyParams = Schema.standardSchemaV1(Schema.Unknown);

/** A tool's arguments and its result are objects, which is the JSON Schema MCP lists a tool with. */
const objectSchema = Schema.decodeUnknownSync(
  Schema.Struct(
    { type: Schema.Literal("object") },
    Schema.Record({ key: Schema.String, value: Schema.Unknown }),
  ),
);

/**
 * The daemon's one MCP handler. The returned function gives the HTTP app that
 * answers the current request from `agent`; the SDK parses the request,
 * negotiates the protocol revision, dispatches the method and frames the
 * response, and no MCP server outlives a request. The app is interruptible so
 * that a stopped agent or an aborted request ends a call in flight.
 */
export function makeMcpHandler(implementation: {
  readonly name: string;
  readonly version: string;
}) {
  const handler = createMcpHandler(({ authInfo }) =>
    requestServer(implementation, authInfo?.extra?.admitted),
  );
  return (agent: AgentCalls) =>
    Effect.runtime().pipe(
      Effect.flatMap((runtime) =>
        HttpApp.fromWebHandler((request) =>
          handler.fetch(request, {
            authInfo: {
              token: "",
              clientId: "",
              scopes: [],
              extra: {
                admitted: new AdmittedRequest({
                  agent,
                  runtime,
                  signal: request.signal,
                }),
              },
            },
          }),
        ),
      ),
      Effect.interruptible,
    );
}

/** What a tool definition says about a tool before any operation is attached. */
interface Definition<A, I, B, J, E> {
  readonly description: string;
  readonly input: Schema.Schema<A, I>;
  readonly output: Schema.Schema<B, J>;
  readonly annotations: {
    readonly readOnlyHint: boolean;
    readonly openWorldHint: boolean;
    readonly destructiveHint?: boolean;
    readonly idempotentHint?: boolean;
  };
  readonly failure: {
    readonly toData: (error: E) => unknown;
    /** The failure to answer when the tool ends in a way it does not declare. */
    readonly unexpected?: E;
  };
}

/**
 * Turns a tool definition and the operation behind it into a tool to serve.
 * `visible` is checked on every listing and call; a tool without it is
 * always offered.
 */
export function bind<A, I, B, J, E extends { readonly message: string }>(
  name: string,
  definition: Definition<A, I, B, J, E>,
  execute: (input: A) => Effect.Effect<B, E>,
  visible?: Effect.Effect<boolean>,
) {
  return {
    tool: {
      name,
      description: definition.description,
      inputSchema: objectSchema(jsonSchema(definition.input)),
      outputSchema: objectSchema(openObjects(jsonSchema(definition.output))),
      annotations: {
        destructiveHint: true,
        idempotentHint: false,
        ...definition.annotations,
      },
    },
    ...(visible === undefined ? {} : { visible }),
    handle: handler(definition, execute),
  };
}

/** The MCP server for one request, answering from the agent `admitted` names. */
function requestServer(
  implementation: { readonly name: string; readonly version: string },
  admitted: unknown,
) {
  const mcp = new McpServer(implementation, {
    capabilities: { tools: { listChanged: true }, ...eventsCapability },
  });
  if (!(admitted instanceof AdmittedRequest)) {
    return mcp;
  }
  const { agent } = admitted;
  const visible = (tool: AgentCalls["tools"][number]) =>
    tool.visible ?? Effect.succeed(true);
  mcp.server.setRequestHandler("tools/list", () =>
    run(
      admitted,
      Effect.filter(agent.tools, visible).pipe(
        Effect.map((tools) => ({ tools: tools.map(({ tool }) => tool) })),
      ),
    ),
  );
  mcp.server.setRequestHandler("tools/call", ({ params }) =>
    run(
      admitted,
      Effect.gen(function* () {
        const called = agent.tools.find(
          ({ tool }) => tool.name === params.name,
        );
        if (called === undefined || !(yield* visible(called))) {
          return yield* Effect.fail(new JsonRpcError(-32602, "Tool not found"));
        }
        return yield* called.handle(params.arguments ?? {});
      }),
    ),
  );
  for (const call of ["list", "subscribe", "unsubscribe"] as const) {
    mcp.server.setRequestHandler(
      `events/${call}`,
      { params: anyParams },
      (params) => run(admitted, agent.events[call](params)),
    );
  }
  return mcp;
}

/**
 * Runs the Effect that answers one MCP request in the runtime of the HTTP
 * request's fiber, so its spans keep their parent, and interrupts it when the
 * request is aborted. A failure rejects with the SDK's error for it, which the
 * SDK answers as the JSON-RPC error.
 */
function run<A>(
  { runtime, signal }: AdmittedRequest,
  effect: Effect.Effect<A, JsonRpcError>,
) {
  return new Promise<A>((resolve, reject) => {
    const interrupt = Runtime.runCallback(runtime)(effect, {
      onExit: Exit.match({
        onSuccess: resolve,
        onFailure: (cause) => {
          const { code, message, data } = Option.getOrElse(
            Cause.failureOption(cause),
            () => new JsonRpcError(-32603, "Internal error"),
          );
          reject(new ProtocolError(code, message, data));
        },
      }),
    });
    signal.addEventListener("abort", () => {
      interrupt();
    });
  });
}

/**
 * Answers a call of a tool: arguments are decoded with the definition's
 * input and the result is encoded with its output. A call that fails is
 * answered with a result marked as an error, whose text is the failure's
 * message and whose structured content is what the definition writes for it.
 */
function handler<A, I, B, J, E extends { readonly message: string }>(
  definition: Definition<A, I, B, J, E>,
  execute: (input: A) => Effect.Effect<B, E>,
) {
  const refusal = (error: E) =>
    failed(error.message, definition.failure.toData(error));
  const unexpected =
    definition.failure.unexpected === undefined
      ? failed(
          "The daemon could not complete this call. Try again; if it keeps failing, check the daemon's log.",
        )
      : refusal(definition.failure.unexpected);
  return (input: unknown) =>
    Effect.gen(function* () {
      const decoded = yield* Schema.decodeUnknown(definition.input, {
        onExcessProperty: "error",
        errors: "all",
      })(input).pipe(Effect.mapError(invalidArguments));
      const output = yield* execute(decoded).pipe(Effect.mapError(refusal));
      const encoded = yield* Schema.encode(definition.output)(output, {
        onExcessProperty: "error",
      }).pipe(Effect.mapError(() => unexpected));
      return {
        content: [{ type: "text" as const, text: JSON.stringify(encoded) }],
        structuredContent: encoded,
      };
    }).pipe(
      Effect.catchAllCause((cause) =>
        Effect.succeed(
          Option.getOrElse(Cause.failureOption(cause), () => unexpected),
        ),
      ),
    );
}

const MAXIMUM_VALIDATION_ISSUES = 20;

/**
 * A refusal of arguments that names at most 20 wrong fields, each by its path
 * and what the tool expects there. Submitted values are left out, and so is
 * the absent alternative of an optional field unless it is all there is to say.
 */
function invalidArguments(error: ParseResult.ParseError) {
  const found = ParseResult.ArrayFormatter.formatErrorSync(error).map(
    (issue): ValidationIssue => ({
      path: issue.path.map((key) =>
        typeof key === "number" ? key : String(key),
      ),
      message: expectation(issue),
    }),
  );
  const present = found.filter(
    ({ message }) => message !== "Expected undefined",
  );
  const issues = (present.length === 0 ? found : present).slice(
    0,
    MAXIMUM_VALIDATION_ISSUES,
  );
  return failed(`Invalid tool arguments: ${issuesText(issues)}`, {
    reason: "invalid-arguments",
    issues,
  });
}

/** What the tool expects where an issue was found, without the value that was there. */
function expectation(issue: ParseResult.ArrayFormatterIssue): string {
  if (issue._tag === "Missing") {
    return "Required field is missing";
  }
  if (
    issue._tag === "Unexpected" ||
    issue.message.startsWith("Expected never")
  ) {
    return "Unexpected field";
  }
  return issue.message.replace(/, actual [\s\S]*$/u, "");
}

/** The result of a call that failed: `message` for the model to read, and the failure's fields. */
function failed(message: string, structuredContent?: unknown) {
  return {
    isError: true,
    content: [{ type: "text" as const, text: message }],
    structuredContent,
  };
}

function jsonSchema<A, I>(schema: Schema.Schema<A, I>) {
  const definitions: Record<string, JSONSchema.JsonSchema7> = {};
  const root = JSONSchema.fromAST(schema.ast, {
    definitions,
    target: "jsonSchema2020-12",
    topLevelReferenceStrategy: "skip",
  });
  return {
    ...root,
    ...(Object.keys(definitions).length === 0 ? {} : { $defs: definitions }),
  };
}

/** The schema without `additionalProperties: false`, so a host accepts fields a result gains. */
function openObjects(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(openObjects);
  }
  if (value === null || typeof value !== "object") {
    return value;
  }
  return Object.fromEntries(
    Object.entries(value)
      .filter(
        ([key, nested]) => key !== "additionalProperties" || nested !== false,
      )
      .map(([key, nested]) => [key, openObjects(nested)]),
  );
}
