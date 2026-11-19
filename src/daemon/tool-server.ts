/**
 * @file The daemon's MCP handler and the tools it serves. Each request gets an
 * MCP server built for the agent that answers it, with the tools of that
 * agent's role registered on it.
 */

import type { SqlClient } from "@effect/sql";
import { HttpApp } from "@effect/platform";
import {
  createMcpHandler,
  McpServer,
  type StandardSchemaWithJSON,
} from "@modelcontextprotocol/server";
import { EventsExtension } from "@modelcontextprotocol/server/ext/events";
import { nodeWebhookSender } from "@modelcontextprotocol/server/ext/events/node";
import {
  type Context,
  Data,
  Effect,
  JSONSchema,
  Runtime,
  Schema,
} from "effect";
import type { Firewall } from "../firewall/service.js";
import type { SocialHarness } from "./agent.js";
import {
  Cursor,
  Message,
  MsgId,
  Page,
  PAGE_SIZE,
  PaginatedRequest,
  wellFormedString,
} from "../communication/index.js";
import { compareAscii, exactStruct, isStrictlyAscending } from "../encoding.js";
import {
  type CompiledContract,
  firewallDecisionsRequestSchema,
  firewallDecisionsSchema,
  LocalGuidance,
} from "../firewall/index.js";
import { type AgentCard, AgentId, AgentName } from "../identity.js";
import { makeInbox } from "./inbox.js";
import {
  AgentAddress,
  INBOX_EVENT,
  InboxEntry,
  SendError,
  SendRequest,
  SendResult,
} from "./values.js";

/**
 * What a request is answered from: one running agent, its role and the
 * daemon's Events extension, which serves that agent's inbox.
 */
export interface AgentCalls {
  readonly agentKey: string;
  readonly card: AgentCard;
  readonly harness: Context.Tag.Service<typeof SocialHarness>;
  readonly firewall: Context.Tag.Service<typeof Firewall>;
  readonly events: EventsExtension;
  /**
   * The server agent's address when this agent is a client, which talks only
   * to the server agent and so names no recipient and no conversation.
   */
  readonly serverAgent?: AgentAddress;
  /**
   * Reads the contracts `read_norms` lists: this agent's, or for a client the
   * server agent's. An agent without it has no `read_norms`.
   */
  readonly norms?: Effect.Effect<readonly CompiledContract[]>;
}

/**
 * One admitted HTTP request: the agent that answers it and the runtime of the
 * fiber that carries it. It travels through the MCP SDK as the request's
 * authentication info, which the SDK reads nothing from and hands to the
 * server it builds for the request.
 */
class AdmittedRequest extends Data.Class<{
  readonly agent: AgentCalls;
  readonly runtime: Runtime.Runtime<never>;
}> {}

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
              extra: { admitted: new AdmittedRequest({ agent, runtime }) },
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
    payloadSchema: standard(InboxEntry),
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

/** One request's MCP server and the runtime its tool calls run in. */
interface Served {
  readonly mcp: McpServer;
  readonly runtime: Runtime.Runtime<never>;
}

const noArguments = Schema.Record({
  key: Schema.String,
  value: Schema.Never,
}).annotations({
  jsonSchema: { type: "object", properties: {}, additionalProperties: false },
});

/** A page of one conversation's records: each a message the agent sent, or the stored inbox entry of one it received. */
const historyRecordsPage = Schema.Struct({
  records: Schema.Array(
    Schema.Union(
      Schema.Struct({
        kind: Schema.Literal("sent"),
        message: Schema.typeSchema(Message),
      }),
      Schema.Struct({
        kind: Schema.Literal("received"),
        address: AgentAddress,
        message: Schema.typeSchema(Message),
        guidance: Schema.optionalWith(LocalGuidance, { exact: true }),
      }),
    ),
  ).pipe(Schema.maxItems(PAGE_SIZE)),
  ...Page.fields,
});

/** A send that ends unexpectedly may have been sent. */
const sendOutcomeUnknown = new SendError({ reason: "outcome-unknown" });

/** How a message states one act of a norm, as every reader of the norms needs it. */
const ACT_SYNTAX =
  "A message is one act and nothing else: (act :ontology <norm> :field value ...). Quote text as a JSON string, write a multi-select as a list such as (option1 option3), and send an act without fields as (act :ontology <norm>). The opening act omits replyToMsgId in send_message; every later act supplies the id of the message it answers.";

/** The MCP server for one request, with the tools of the agent `admitted` names. */
function requestServer(
  implementation: { readonly name: string; readonly version: string },
  admitted: unknown,
) {
  if (!(admitted instanceof AdmittedRequest)) {
    return new McpServer(implementation);
  }
  const { agent, runtime } = admitted;
  const { card, harness, norms, serverAgent } = agent;
  const mcp = new McpServer(implementation, { extensions: [agent.events] });
  const served = { mcp, runtime };
  register(
    served,
    "status",
    {
      description:
        "Read this agent's identifier and name. Other agents address this agent as agent:<agentName>.",
      input: noArguments,
      output: Schema.Struct({ agentId: AgentId, agentName: AgentName }),
      readOnly: true,
    },
    () => Effect.succeed({ agentId: card.agentId, agentName: card.agentName }),
  );
  register(
    served,
    "receive_messages",
    {
      description:
        "Receive inbox entries in order, each with its conversation address, its message and any guidance. Keep the nextCursor of every result and pass it as cursor on the next call to receive the entries after it. Without cursor the call starts from now: it returns no earlier entries.",
      input: exactStruct({ ...PaginatedRequest.fields }),
      output: Schema.Struct({
        entries: Schema.Array(InboxEntry).pipe(Schema.maxItems(PAGE_SIZE)),
        nextCursor: Cursor,
      }),
      readOnly: true,
    },
    harness.receive,
  );
  if (serverAgent === undefined) {
    registerServerAgentTools(served, agent);
  } else {
    registerClientTools(served, harness, serverAgent);
  }
  if (norms !== undefined) {
    register(
      served,
      "read_norms",
      {
        description: `${
          serverAgent === undefined
            ? "List the installed norms and their complete documents: the roles, when each act may be sent and the fields it takes."
            : "List the norms the server agent holds, each with its complete document: the roles, when each act may be sent and the fields it takes. The server agent checks your messages against them."
        } ${ACT_SYNTAX}`,
        input: noArguments,
        output: exactStruct({
          norms: Schema.Array(
            exactStruct({
              name: Schema.String,
              description: Schema.String,
              document: Schema.String,
            }),
          ),
        }),
        readOnly: true,
      },
      () =>
        Effect.map(norms, (contracts) => ({
          norms: contracts.map((norm) => ({
            name: norm.name,
            description: norm.description,
            document: norm.source,
          })),
        })),
    );
  }
  return mcp;
}

/** The tools only the server agent has: it addresses any client and reads its firewall's decisions. */
function registerServerAgentTools(
  served: Served,
  { firewall, harness }: AgentCalls,
) {
  register(
    served,
    "send_message",
    {
      description:
        "Send a message. Returns id, the message id, and address, the canonical conversation address. A refusal names the argument or rule to correct. An outcome-unknown failure must not be retried as a new send. Use receive_messages to read what others send.",
      input: SendRequest,
      output: SendResult,
      unexpected: sendOutcomeUnknown,
    },
    harness.send,
  );
  register(
    served,
    "list_conversations",
    {
      description:
        "List the address of every conversation with a message. Use read_conversation to read one address.",
      input: noArguments,
      output: Schema.Struct({
        addresses: Schema.Array(AgentAddress),
      }).pipe(
        Schema.filter((page) =>
          isStrictlyAscending(page.addresses, compareAscii),
        ),
      ),
      readOnly: true,
    },
    harness.listConversations,
  );
  register(
    served,
    "read_conversation",
    {
      description:
        "Read one conversation's records: the messages you sent and the inbox entries of the messages you received. A page holds the oldest records after cursor; pass nextCursor as cursor, with the same address, until it is absent.",
      input: exactStruct({
        address: AgentAddress,
        ...PaginatedRequest.fields,
      }),
      output: historyRecordsPage,
      readOnly: true,
    },
    harness.readHistory,
  );
  register(
    served,
    "read_firewall_decisions",
    {
      description:
        "Read completed firewall decision summaries since inclusive Unix milliseconds. Each gives the decision applied (allowed, refused or withheld), the reason and the deciding rules' registered advice, without message content or private model evidence.",
      input: firewallDecisionsRequestSchema,
      output: firewallDecisionsSchema,
      readOnly: true,
    },
    firewall.readDecisions,
  );
}

/** The tools of a client, which name no recipient and no conversation: both are `serverAgent`. */
function registerClientTools(
  served: Served,
  harness: AgentCalls["harness"],
  serverAgent: AgentAddress,
) {
  register(
    served,
    "send_message",
    {
      description:
        "Send a message to the server agent. Returns the message id. An outcome-unknown failure must not be retried as a new send.",
      input: exactStruct({
        text: Schema.propertySignature(wellFormedString).annotations({
          description: "The message text.",
        }),
        replyToMsgId: Schema.optional(MsgId),
      }),
      output: SendResult,
      unexpected: sendOutcomeUnknown,
    },
    (message) => harness.send({ ...message, to: serverAgent }),
  );
  register(
    served,
    "read_conversation",
    {
      description:
        "Read the messages you sent and received in your conversation with the server agent. Follow nextCursor until absent.",
      input: exactStruct({ ...PaginatedRequest.fields }),
      output: historyRecordsPage,
      readOnly: true,
    },
    (page) => harness.readHistory({ ...page, address: serverAgent }),
  );
}

/**
 * Registers a tool whose calls `execute` answers. A call runs in the runtime
 * of the HTTP request's fiber, so its spans keep their parent, and is
 * interrupted when the request is aborted. Its value is the result's
 * structured content. A typed failure is a result marked as an error. A call
 * that ends any other way is logged and answered as `unexpected`, or with a
 * fixed text that says nothing of what went wrong.
 */
function register<A, I, B extends object, J extends object, E extends Failure>(
  { mcp, runtime }: Served,
  name: string,
  tool: {
    readonly description: string;
    readonly input: Schema.Schema<A, I>;
    readonly output: Schema.Schema<B, J>;
    readonly readOnly?: true;
    readonly unexpected?: E;
  },
  execute: (input: A) => Effect.Effect<B, E>,
) {
  const outputSchema: StandardSchemaWithJSON<object, object> = standard(
    tool.output,
  );
  mcp.registerTool(
    name,
    {
      description: tool.description,
      inputSchema: standard(tool.input),
      outputSchema,
      annotations: tool.readOnly && {
        readOnlyHint: true,
        openWorldHint: false,
      },
    },
    (input, ctx) =>
      Runtime.runPromise(runtime)(
        execute(input).pipe(
          Effect.flatMap((value) =>
            Effect.orDie(Schema.encode(tool.output)(value)),
          ),
          Effect.map((structuredContent) => ({ structuredContent })),
          Effect.catchAll((failure) =>
            Effect.succeed(failed(failure.message, failure)),
          ),
          Effect.catchAllDefect((defect) =>
            Effect.logError("A tool call ended unexpectedly", defect).pipe(
              Effect.as(
                tool.unexpected === undefined
                  ? failed(
                      "The daemon could not complete this call. Try again; if it keeps failing, check the daemon's log.",
                    )
                  : failed(tool.unexpected.message, tool.unexpected),
              ),
            ),
          ),
        ),
        { signal: ctx.mcpReq.signal },
      ),
  );
}

/**
 * `schema` as the Standard Schema the MCP SDK validates a value with and
 * lists as JSON Schema. Effect's own Standard Schema carries no JSON Schema,
 * which the SDK requires. A value with a field the schema does not name is
 * invalid.
 */
function standard<A, I>(
  schema: Schema.Schema<A, I>,
): StandardSchemaWithJSON<I, A> {
  const definitions: Record<string, JSONSchema.JsonSchema7> = {};
  const root = JSONSchema.fromAST(schema.ast, {
    definitions,
    target: "jsonSchema2020-12",
    topLevelReferenceStrategy: "skip",
  });
  const json = () => ({
    ...root,
    ...(Object.keys(definitions).length === 0 ? {} : { $defs: definitions }),
  });
  return {
    "~standard": {
      ...Schema.standardSchemaV1(schema, { onExcessProperty: "error" })[
        "~standard"
      ],
      jsonSchema: { input: json, output: json },
    },
  };
}

/** A typed failure of a tool: what a model reads of it and the fields a host reads. */
interface Failure {
  readonly message: string;
  readonly reason: string;
  readonly detail?: unknown;
  readonly checkId?: string;
  readonly refusal?: string;
  readonly rule?: string;
  readonly feedback?: readonly string[];
}

/** The result of a call that failed: `message` as its text, and the failure's fields. */
function failed(text: string, failure?: Failure) {
  return {
    isError: true as const,
    content: [{ type: "text" as const, text }],
    structuredContent: failure && {
      reason: failure.reason,
      checkId: failure.checkId,
      refusal: failure.refusal,
      rule: failure.rule,
      feedback: failure.feedback,
      detail: failure.detail,
    },
  };
}
