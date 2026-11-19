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
import { issuesText } from "../encoding.js";
import { Firewall } from "../firewall/service.js";
import { Identity } from "../identity.js";
import {
  clientToolDefinitions,
  directToolDefinitions,
  type MessageAddressInput,
  type SubscriptionStatus,
  toolDefinitions,
} from "../mcp/tools.js";
import { compiledPolicyEdit, HeldNorms } from "../norms/index.js";
import { SocialHarness } from "./agent.js";

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
 * The tools of a peer and of the server agent: each addresses any agent and
 * owns its message policy. Without `groups` an address names one agent.
 */
export function peerTools(policyConfigured: boolean, groups: boolean) {
  return Effect.fnUntraced(function* (
    status: Effect.Effect<SubscriptionStatus>,
  ) {
    const harness = yield* SocialHarness;
    const firewall = yield* Firewall;
    return [
      yield* statusTool(status),
      groups
        ? bind("send_message", toolDefinitions.send_message, harness.send)
        : bind(
            "send_message",
            directToolDefinitions.send_message,
            harness.send,
          ),
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
      groups
        ? bind(
            "read_conversation",
            toolDefinitions.read_conversation,
            harness.readHistory,
          )
        : bind(
            "read_conversation",
            directToolDefinitions.read_conversation,
            harness.readHistory,
          ),
      ...(policyConfigured
        ? [
            bind("read_policy", toolDefinitions.read_policy, () =>
              firewall.readPolicy(),
            ),
            bind("update_policy", toolDefinitions.update_policy, (update) =>
              compiledPolicyEdit(update).pipe(
                Effect.flatMap(firewall.updatePolicy),
              ),
            ),
          ]
        : []),
      bind(
        "read_firewall_decisions",
        toolDefinitions.read_firewall_decisions,
        firewall.readDecisions,
      ),
      ...(yield* normsTool(toolDefinitions.read_norms)),
    ];
  });
}

/**
 * The tools of a client in client/server mode. It sends to and reads its
 * conversation with the server agent, and names neither.
 */
export function clientTools(serverAgentAddress: MessageAddressInput) {
  return Effect.fnUntraced(function* (
    status: Effect.Effect<SubscriptionStatus>,
  ) {
    const harness = yield* SocialHarness;
    return [
      yield* statusTool(status),
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
      ...(yield* normsTool(clientToolDefinitions.read_norms)),
    ];
  });
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

/** `status` names this agent and reports its subscription as `status` reads it. */
function statusTool(status: Effect.Effect<SubscriptionStatus>) {
  return Effect.map(Identity, (identity) =>
    bind("status", toolDefinitions.status, () =>
      Effect.map(status, (subscription) => ({
        agentId: identity.me.agentId,
        agentName: identity.me.agentName,
        subscription,
      })),
    ),
  );
}

/**
 * `read_norms` lists the norms held: this agent's, or for a client the server
 * agent's. An agent that holds none has no such tool.
 */
function normsTool(
  definition: (
    | typeof toolDefinitions
    | typeof clientToolDefinitions
  )["read_norms"],
) {
  return Effect.gen(function* () {
    const heldNorms = yield* Effect.serviceOption(HeldNorms);
    if (Option.isNone(heldNorms)) {
      return [];
    }
    const read = heldNorms.value.read().pipe(
      Effect.map((held) => ({
        norms: held.map((norm) => ({
          name: norm.name,
          description: norm.description,
          document: norm.source,
        })),
      })),
    );
    return [bind("read_norms", definition, () => read)];
  });
}

/** The MCP server for one request, answering from the agent `admitted` names. */
function requestServer(
  implementation: { readonly name: string; readonly version: string },
  admitted: unknown,
) {
  const mcp = new McpServer(implementation, {
    capabilities: { tools: {}, ...eventsCapability },
  });
  if (!(admitted instanceof AdmittedRequest)) {
    return mcp;
  }
  const { agent } = admitted;
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
