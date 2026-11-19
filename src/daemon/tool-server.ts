/**
 * @file The daemon's MCP handler and the tools it serves. Each request is
 * answered from one agent: a call's arguments are decoded, its operation run,
 * its result encoded and its failure worded by the tool's definition.
 */

import type { SqlClient } from "@effect/sql";
import { HttpApp } from "@effect/platform";
import {
  createMcpHandler,
  fromJsonSchema,
  McpServer,
  ProtocolError,
} from "@modelcontextprotocol/server";
import { EventsExtension } from "@modelcontextprotocol/server/ext/events";
import { nodeWebhookSender } from "@modelcontextprotocol/server/ext/events/node";
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
import type { CompiledContract } from "../firewall/index.js";
import { Cursor, PAGE_SIZE } from "../communication/index.js";
import { issuesText } from "../encoding.js";
import { Firewall } from "../firewall/service.js";
import { Identity } from "../identity.js";
import {
  type AgentAddress,
  clientToolDefinitions,
  INBOX_EVENT,
  InboxEntry,
  toolDefinitions,
} from "../mcp/tools.js";
import { SocialHarness } from "./agent.js";
import { makeInbox } from "./inbox.js";

/**
 * What a request is answered from: the agent's key, its tools in listing
 * order, and the daemon's Events extension, which serves that agent's inbox.
 */
export interface AgentCalls {
  readonly agentKey: string;
  readonly tools: ReadonlyArray<ReturnType<typeof bind>>;
  readonly events: EventsExtension;
}

/** The JSON-RPC error a tool call is answered with. */
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
 * that an aborted request ends a call in flight.
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

/** How often the SDK reads the inbox of an agent whose host holds a push stream or a webhook subscription. */
const INBOX_CHECK_MILLISECONDS = 1000;

/** The longest a webhook subscription lives without a refresh. */
const WEBHOOK_TTL_MILLISECONDS = 30 * 60_000;

/**
 * The daemon's Events extension, which every request's server is built with.
 * Its one event type is the inbox of the agent a request is admitted as: an
 * occurrence is an inbox entry, and a subscription's cursor is the read
 * number of the last entry it was sent. The SDK delivers by poll, push and
 * webhook, and keeps every subscription in memory.
 */
export const makeInboxEvents = Effect.gen(function* () {
  const run = Runtime.runPromise(yield* Effect.runtime<SqlClient.SqlClient>());
  const events = new EventsExtension({
    getPrincipal: ({ http }) => {
      const admitted = http?.authInfo?.extra?.admitted;
      return admitted instanceof AdmittedRequest
        ? admitted.agent.agentKey
        : null;
    },
    checkIntervalMs: INBOX_CHECK_MILLISECONDS,
    webhookTtlMs: WEBHOOK_TTL_MILLISECONDS,
    webhookSender: nodeWebhookSender(),
  });
  events.registerEvent(INBOX_EVENT, {
    description:
      "An inbox entry: its conversation address, its message and any guidance.",
    payloadSchema: fromJsonSchema<typeof InboxEntry.Encoded>(
      objectSchema(jsonSchema(InboxEntry)),
    ),
    check: (...[{ principal, maxEvents, signal }, , cursor]) =>
      run(
        Effect.gen(function* () {
          const inbox = yield* makeInbox(yield* Effect.fromNullable(principal));
          const page = yield* inbox.listInbox(
            Math.min(maxEvents ?? PAGE_SIZE, PAGE_SIZE),
            cursor === null ? undefined : yield* Schema.decode(Cursor)(cursor),
          );
          return {
            events: (yield* Schema.encode(Schema.Array(InboxEntry))(
              page.entries,
            )).map((data) => ({
              eventId: `${data.message.author.agentId}/${data.message.msgId}`,
              data,
            })),
            cursor: page.nextCursor,
            hasMore: page.hasMore,
          };
        }),
        { signal },
      ),
  });
  return events;
}).pipe(Effect.withSpan("makeInboxEvents"));

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

/** `status` names this agent. */
const statusTool = Effect.map(Identity, (identity) =>
  bind("status", toolDefinitions.status, () =>
    Effect.succeed({
      agentId: identity.me.agentId,
      agentName: identity.me.agentName,
    }),
  ),
);

/**
 * The tools of the server agent, which addresses any client. One that holds
 * norms lists them with `read_norms`.
 */
export function serverAgentTools(holdsNorms: boolean) {
  return Effect.fnUntraced(function* () {
    const harness = yield* SocialHarness;
    const firewall = yield* Firewall;
    return [
      yield* statusTool,
      bind("send_message", toolDefinitions.send_message, harness.send),
      bind(
        "receive_messages",
        toolDefinitions.receive_messages,
        harness.receive,
      ),
      bind(
        "list_conversations",
        toolDefinitions.list_conversations,
        harness.listConversations,
      ),
      bind(
        "read_conversation",
        toolDefinitions.read_conversation,
        harness.readHistory,
      ),
      bind(
        "read_firewall_decisions",
        toolDefinitions.read_firewall_decisions,
        firewall.readDecisions,
      ),
      ...(holdsNorms
        ? [normsTool(toolDefinitions.read_norms, firewall.readContracts())]
        : []),
    ];
  });
}

/**
 * The tools of a client. It sends to and reads its
 * conversation with the server agent, and names neither. With
 * `serverAgentContracts`, the norms the server agent holds, it has `read_norms`.
 */
export function clientTools(
  serverAgentAddress: AgentAddress,
  serverAgentContracts?: Effect.Effect<readonly CompiledContract[]>,
) {
  return Effect.fnUntraced(function* () {
    const harness = yield* SocialHarness;
    return [
      yield* statusTool,
      bind("send_message", clientToolDefinitions.send_message, (message) =>
        harness.send({ ...message, to: serverAgentAddress }),
      ),
      bind(
        "receive_messages",
        toolDefinitions.receive_messages,
        harness.receive,
      ),
      bind(
        "read_conversation",
        clientToolDefinitions.read_conversation,
        (page) => harness.readHistory({ ...page, address: serverAgentAddress }),
      ),
      ...(serverAgentContracts === undefined
        ? []
        : [normsTool(clientToolDefinitions.read_norms, serverAgentContracts)]),
    ];
  });
}

/** `read_norms` lists the contracts `held` reads: this agent's, or for a client the server agent's. */
function normsTool(
  definition: (
    | typeof toolDefinitions
    | typeof clientToolDefinitions
  )["read_norms"],
  held: Effect.Effect<readonly CompiledContract[]>,
) {
  const read = held.pipe(
    Effect.map((contracts) => ({
      norms: contracts.map((norm) => ({
        name: norm.name,
        description: norm.description,
        document: norm.source,
      })),
    })),
  );
  return bind("read_norms", definition, () => read);
}

/** Turns a tool definition and the operation behind it into a tool to serve. */
function bind<A, I, B, J, E extends { readonly message: string }>(
  name: string,
  definition: Definition<A, I, B, J, E>,
  execute: (input: A) => Effect.Effect<B, E>,
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
    handle: handler(definition, execute),
  };
}

/** The MCP server for one request, answering from the agent `admitted` names. */
function requestServer(
  implementation: { readonly name: string; readonly version: string },
  admitted: unknown,
) {
  if (!(admitted instanceof AdmittedRequest)) {
    return new McpServer(implementation);
  }
  const { agent } = admitted;
  const mcp = new McpServer(implementation, {
    capabilities: { tools: {} },
    extensions: [agent.events],
  });
  mcp.server.setRequestHandler("tools/list", () =>
    run(
      admitted,
      Effect.succeed({ tools: agent.tools.map(({ tool }) => tool) }),
    ),
  );
  mcp.server.setRequestHandler("tools/call", ({ params }) =>
    run(
      admitted,
      Effect.gen(function* () {
        const called = agent.tools.find(
          ({ tool }) => tool.name === params.name,
        );
        if (called === undefined) {
          return yield* Effect.fail(new JsonRpcError(-32602, "Tool not found"));
        }
        return yield* called.handle(params.arguments ?? {});
      }),
    ),
  );
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
 * A call that ends any other way is logged and answered as unexpected.
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
        Option.match(Cause.failureOption(cause), {
          onSome: Effect.succeed,
          onNone: () =>
            Effect.logError("A tool call ended unexpectedly", cause).pipe(
              Effect.as(unexpected),
            ),
        }),
      ),
    );
}

const MAXIMUM_VALIDATION_ISSUES = 20;

/** A field path and schema expectation, without the rejected value. */
interface ValidationIssue {
  readonly path: ReadonlyArray<string | number>;
  readonly message: string;
}

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
