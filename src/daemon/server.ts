/**
 * @file The daemon's HTTP endpoint. It admits each request as the agent of the
 * owner its OAuth token names and answers it from an MCP server built for
 * that agent, with the tools of the agent's role and the inbox event.
 */

import {
  HttpApp,
  HttpBody,
  HttpServerRequest,
  HttpServerResponse,
} from "@effect/platform";
import { NodeHttpServer } from "@effect/platform-node";
import { discoverAuthorizationServerMetadata } from "@modelcontextprotocol/client";
import {
  type AuthInfo,
  createMcpHandler,
  getOAuthProtectedResourceMetadataUrl,
  hostHeaderValidationResponse,
  McpServer,
  OAuthError,
  OAuthErrorCode,
  oauthMetadataResponse,
  originValidationResponse,
  requireBearerAuth,
  type StandardSchemaWithJSON,
} from "@modelcontextprotocol/server";
import { EventsExtension } from "@modelcontextprotocol/server/ext/events";
import { nodeWebhookSender } from "@modelcontextprotocol/server/ext/events/node";
import {
  Data,
  Effect,
  JSONSchema,
  Match,
  Order,
  Runtime,
  Schema,
  type Scope,
  Stream,
} from "effect";
import { createRemoteJWKSet, jwtVerify } from "jose";
import { AsyncLocalStorage } from "node:async_hooks";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- NodeHttpServer accepts the native server constructor.
import { createServer } from "node:http";
import type { InstalledNorms } from "../norms/index.js";
import type { DaemonSettings } from "./settings.js";
import packageJson from "../../package.json" with { type: "json" };
import {
  agentNamed,
  agentOf,
  type Conversations,
  history,
  type InboxEntry as InboxEntryOf,
  PAGE_SIZE,
  peersOf,
  readDecisions,
  readInbox,
  receive,
  send,
} from "../conversations.js";
import { checkRecordSchema, LocalGuidance } from "../firewall.js";
import {
  AGENT_NAME_RULE,
  type AgentCard,
  AgentId,
  AgentName,
  Message,
  MsgId,
  wellFormedString,
} from "../messages.js";

// safer-arch-ignore no-fat-orchestrator: The daemon's orchestrator: it registers every tool.

/** The version of this package, as the daemon reports it to MCP clients and in telemetry. */
export const packageVersion: string = packageJson.version;

/** The scheme of an address naming one agent. */
const AGENT_ADDRESS_PREFIX = "agent:";

const isAgentName = Schema.is(AgentName);

/** The address of one agent. */
const AgentAddress = Schema.String.pipe(
  Schema.filter(
    (value) =>
      value.startsWith(AGENT_ADDRESS_PREFIX) &&
      isAgentName(value.slice(AGENT_ADDRESS_PREFIX.length)),
    { description: `agent:<name>; a name is ${AGENT_NAME_RULE}` },
  ),
  Schema.brand("AgentAddress"),
  Schema.annotations({ identifier: "AgentAddress" }),
);
type AgentAddress = typeof AgentAddress.Type;

/** How the agents of the daemon address the agent named `agentName`. */
const addressOf = (agentName: AgentName): AgentAddress =>
  AgentAddress.make(`${AGENT_ADDRESS_PREFIX}${agentName}`);

/** One message as the host reads it: its conversation's address, the message and any guidance. */
const InboxEntry = Schema.Struct({
  address: AgentAddress,
  message: Message,
  guidance: Schema.optionalWith(LocalGuidance, { exact: true }),
});

/**
 * One record of a conversation's history: a message the agent sent, or the
 * inbox entry of a message it received.
 */
const HistoryRecord = Schema.Union(
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
);

/**
 * An address a send or a history read cannot use: it names no agent of this
 * daemon (`unknown-agent`) or the calling agent itself (`invalid-address`).
 */
class AddressError extends Data.TaggedError("AddressError")<{
  readonly reason: "unknown-agent" | "invalid-address";
  readonly address: string;
}> {
  override get message(): string {
    return this.reason === "unknown-agent"
      ? `This daemon has no agent named ${this.address}. Check the name with its owner.`
      : `The address ${this.address} names you, not another agent.`;
  }
}

/**
 * A send that ended unexpectedly: it may already have happened, so a new send
 * can duplicate it.
 */
class SendOutcomeUnknown extends Data.TaggedError("SendError")<{
  readonly reason: "outcome-unknown";
}> {
  override get message(): string {
    return "send failed: the send did not complete; the message may or may not have been sent";
  }
}

/** A place in an ordered read, as decimal text. A read returns what follows it. */
const Cursor = Schema.String.pipe(Schema.pattern(/^\d{1,15}$/u)).annotations({
  description: "the nextCursor of an earlier result",
});

/** Omit cursor to begin and pass the returned nextCursor unchanged. */
const PaginatedRequest = Schema.Struct({
  cursor: Schema.optional(Cursor),
});

/** Absence of nextCursor marks the end of the listing. */
const Page = Schema.Struct({
  nextCursor: Schema.optional(Cursor),
});

const noArguments = Schema.Record({
  key: Schema.String,
  value: Schema.Never,
}).annotations({
  jsonSchema: { type: "object", properties: {}, additionalProperties: false },
});

/** What `send` takes: the recipient's address, the text and the message it answers. */
const SendRequest = Schema.Struct({
  to: Schema.propertySignature(AgentAddress).annotations({
    description: "The recipient: agent:<name>.",
  }),
  text: Schema.propertySignature(wellFormedString).annotations({
    description: "The message text.",
  }),
  replyToMsgId: Schema.optional(MsgId).annotations({
    description:
      "The ID of the message this replies to. Omit when starting a new norm interaction.",
  }),
}).annotations({ identifier: "SendRequest" });
/** @useDeclaredType */
type SendRequest = typeof SendRequest.Type;

/**
 * What a send returns: the message id and the conversation's canonical
 * address. Accepted work continues independently of the caller's lifetime.
 */
const SendResult = Schema.Struct({
  id: MsgId,
  address: AgentAddress,
}).annotations({ identifier: "SendResult" });

/** A page of one conversation's records. Absence of nextCursor marks its end. */
const historyRecordsPage = Schema.Struct({
  records: Schema.Array(HistoryRecord).pipe(Schema.maxItems(PAGE_SIZE)),
  ...Page.fields,
});

/**
 * The longest a receive_messages call may wait. It stays under the time after
 * which hosts and proxies give up on a tool call.
 */
const MAXIMUM_WAIT_SECONDS = 50;

const sendOutcomeUnknown = new SendOutcomeUnknown({
  reason: "outcome-unknown",
});

/** How a message states one act of a norm, as every reader of the norms needs it. */
const ACT_SYNTAX =
  "A message is one act and nothing else: (act :ontology <norm> :field value ...). Quote text as a JSON string, write a multi-select as a list such as (option1 option3), and send an act without fields as (act :ontology <norm>). The opening act omits replyToMsgId in send_message; every later act supplies the id of the message it answers.";

/**
 * The runtime of the fiber that carries the HTTP request the MCP SDK is
 * answering. The SDK builds the request's server and calls its tools from
 * promises, which this storage follows.
 */
const requestRuntime = new AsyncLocalStorage<Runtime.Runtime<never>>();

/** How often the SDK reads the inbox of an agent whose host holds a push stream or a webhook subscription. */
const INBOX_CHECK_MILLISECONDS = 1000;

/** The longest a webhook subscription lives without a refresh. */
const WEBHOOK_TTL_MILLISECONDS = 30 * 60_000;

/** The name of the event whose occurrences are an agent's inbox entries, in read order. */
const INBOX_EVENT = "org.social-harness/inbox.pending";

/** The issuer's metadata could not be read, names another issuer or names no key set. */
class AuthenticationRefused extends Data.TaggedError("AuthenticationRefused") {}

/** Returns after listening. Scope closure disconnects streaming requests before the listener closes. */
export const serveMcp = Effect.fn("Mcp.serve")(function* (
  port: number,
  app: HttpApp.Default<never, Scope.Scope>,
  host: string,
) {
  const listener = yield* Effect.sync(createServer);
  const http = yield* NodeHttpServer.make(() => listener, { host, port });
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      listener.closeAllConnections();
    }),
  );
  yield* http.serve(app);
});

/**
 * The daemon's Events extension, which every request's server is built with.
 * Its one event type is the inbox of the agent a request is admitted as: an
 * occurrence is an inbox entry, and a subscription's cursor is the read
 * number of the last entry it was sent. The SDK delivers by poll, push and
 * webhook, and keeps every subscription in memory.
 */
const makeInboxEvents = Effect.fn("makeInboxEvents")(function* (
  conversations: Conversations,
) {
  const run = Runtime.runPromise(yield* Effect.runtime());
  const events = new EventsExtension({
    getPrincipal: ({ http }) =>
      inRequest(
        Effect.map(
          admittedAgent(conversations, http?.authInfo),
          ({ agentId }) => agentId,
        ),
      ),
    checkIntervalMs: INBOX_CHECK_MILLISECONDS,
    webhookTtlMs: WEBHOOK_TTL_MILLISECONDS,
    webhookSender: nodeWebhookSender(),
  });
  events.registerEvent(INBOX_EVENT, {
    description:
      "An inbox entry: its conversation address, its message and any guidance.",
    payloadSchema: standard(InboxEntry),
    check: (...[{ principal, maxEvents, signal }, , cursor]) =>
      run(inboxOccurrences(conversations, principal, { cursor, maxEvents }), {
        signal,
      }),
  });
  return events;
});

/**
 * The app the listener serves. The MCP SDK validates the Host and Origin
 * headers against the resource's hostname, serves the OAuth discovery
 * documents, and checks the bearer token's scope and expiry; a refusal is the
 * SDK's response. Any path but `/mcp` answers 404. A token is a JWT the issuer
 * signed for the resource with a subject, verified against the key set the
 * issuer's metadata names. The SDK's one MCP handler answers an admitted
 * request: it parses the request, negotiates the protocol revision, builds an
 * MCP server for the agent of the token's subject, dispatches the method and
 * frames the response, on a stream that ends when the token expires. No MCP
 * server outlives a request, and an aborted request ends a call in flight.
 */
export const makeMcpHttpApp = Effect.fn("makeMcpHttpApp")(function* (
  { issuer, resource, scope }: DaemonSettings["authentication"],
  conversations: Conversations,
  norms?: InstalledNorms,
) {
  const events = yield* makeInboxEvents(conversations);
  const mcp = createMcpHandler(({ authInfo }) =>
    inRequest(
      Effect.map(admittedAgent(conversations, authInfo), (agent) =>
        requestServer(agent, {
          conversations,
          events,
          ...(norms === undefined ? {} : { norms }),
        }),
      ),
    ),
  );
  const metadata = yield* Effect.tryPromise({
    try: () => discoverAuthorizationServerMetadata(issuer),
    catch: () => new AuthenticationRefused(),
  });
  if (typeof metadata?.jwks_uri !== "string") {
    return yield* new AuthenticationRefused();
  }
  const keys = createRemoteJWKSet(new URL(metadata.jwks_uri));
  const hostnames = [resource.hostname];
  const discovery = {
    oauthMetadata: metadata,
    resourceServerUrl: resource,
    scopesSupported: [scope],
  };
  const bearer = requireBearerAuth({
    requiredScopes: [scope],
    resourceMetadataUrl: getOAuthProtectedResourceMetadataUrl(resource),
    verifier: {
      // eslint-disable-next-line agent-code-guard/async-keyword -- The MCP SDK's authorization API is promise-based.
      verifyAccessToken: async (token) => {
        const verified = await jwtVerify(token, keys, {
          issuer,
          audience: resource.href,
          algorithms: ["RS256"],
        }).catch(() => undefined);
        if (!verified?.payload.sub) {
          throw new OAuthError(OAuthErrorCode.InvalidToken, "Invalid token");
        }
        const { sub, scope: granted, exp } = verified.payload;
        return {
          token,
          clientId: "",
          scopes: typeof granted === "string" ? granted.split(" ") : [],
          expiresAt: exp,
          extra: { issuer, subject: sub },
        };
      },
    },
  });
  return Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const web = new Request(new URL(request.url, resource), {
      method: request.method,
      headers: request.headers,
    });
    const answered =
      hostHeaderValidationResponse(web, hostnames) ??
      originValidationResponse(web, hostnames) ??
      oauthMetadataResponse(web, discovery);
    if (answered !== undefined) {
      return HttpServerResponse.fromWeb(answered);
    }
    if (new URL(web.url).pathname !== "/mcp") {
      return HttpServerResponse.empty({ status: 404 });
    }
    // eslint-disable-next-line agent-code-guard/effect-promise -- The SDK's gate resolves to its refusal and does not reject.
    const authInfo = yield* Effect.promise(() => bearer(web));
    if (authInfo instanceof Response) {
      return HttpServerResponse.fromWeb(authInfo);
    }
    const runtime = yield* Effect.runtime();
    const response = yield* HttpApp.fromWebHandler((mcpRequest) =>
      requestRuntime.run(runtime, () => mcp.fetch(mcpRequest, { authInfo })),
    ).pipe(Effect.interruptible, Effect.orDie);
    return yield* limitResponseStream(
      response,
      Stream.interruptAfter(
        Math.max(0, (authInfo.expiresAt ?? 0) * 1000 - Date.now()),
      ),
    );
  });
});

/** Ends a streamed response body when `limit` says so. Other bodies are returned as they are. */
function limitResponseStream(
  response: HttpServerResponse.HttpServerResponse,
  limit: <A, E>(body: Stream.Stream<A, E>) => Stream.Stream<A, E>,
) {
  return Match.value(response.body).pipe(
    Match.tag("Stream", (body) =>
      HttpServerResponse.setBody(
        response,
        HttpBody.stream(limit(body.stream), body.contentType),
      ),
    ),
    Match.orElse(() => response),
  );
}

/** The agent of the owner an admitted request's token names. */
function admittedAgent(conversations: Conversations, authInfo?: AuthInfo) {
  return agentOf(conversations, {
    issuer: String(authInfo?.extra?.issuer),
    subject: String(authInfo?.extra?.subject),
  }).pipe(Effect.orDie);
}

/** The MCP server for one request, with the inbox event and the tools of `agent`'s role. */
function requestServer(
  agent: AgentCard,
  served: {
    readonly conversations: Conversations;
    readonly events: EventsExtension;
    readonly norms?: InstalledNorms;
  },
) {
  const { conversations, norms } = served;
  const mcp = new McpServer(
    { name: "sharnessd", version: packageVersion },
    { extensions: [served.events] },
  );
  const isServerAgent = agent.agentId === conversations.serverAgent.agentId;
  register(
    mcp,
    "status",
    {
      description:
        "Read this agent's identifier and name. Other agents address this agent as agent:<agentName>.",
      input: noArguments,
      output: Schema.Struct({ agentId: AgentId, agentName: AgentName }),
      readOnly: true,
    },
    () =>
      Effect.succeed({ agentId: agent.agentId, agentName: agent.agentName }),
  );
  register(
    mcp,
    "receive_messages",
    {
      description:
        "Receive inbox entries in order, each with its conversation address, its message and any guidance. Keep the nextCursor of every result and pass it as cursor on the next call to receive the entries after it. Without cursor the call starts from now: it returns no earlier entries. Pass wait to wait up to that many seconds when no entry follows the cursor; the call returns as soon as one becomes readable, or with no entries when the wait ends.",
      input: Schema.Struct({
        ...PaginatedRequest.fields,
        wait: Schema.optional(
          Schema.Number.pipe(
            Schema.positive(),
            Schema.lessThanOrEqualTo(MAXIMUM_WAIT_SECONDS),
          ).annotations({
            description: `a number of seconds, at most ${String(MAXIMUM_WAIT_SECONDS)}, to wait for an entry when none follows the cursor. Call receive_messages again with nextCursor to keep waiting. Without wait the call returns at once.`,
          }),
        ),
      }),
      output: Schema.Struct({
        entries: Schema.Array(InboxEntry).pipe(Schema.maxItems(PAGE_SIZE)),
        nextCursor: Cursor,
      }),
      readOnly: true,
    },
    (request) =>
      Effect.map(receive(conversations, agent, request), (page) => ({
        entries: page.entries.map(inboxEntry),
        nextCursor: page.nextCursor,
      })),
  );
  if (isServerAgent) {
    registerServerAgentTools(mcp, conversations, agent);
  } else {
    registerClientTools(mcp, conversations, agent);
  }
  if (norms !== undefined) {
    register(
      mcp,
      "read_norms",
      {
        description: `${
          isServerAgent
            ? "List the installed norms and their complete documents: the roles, when each act may be sent and the fields it takes."
            : "List the norms the server agent holds, each with its complete document: the roles, when each act may be sent and the fields it takes. The server agent checks your messages against them."
        } ${ACT_SYNTAX}`,
        input: noArguments,
        output: Schema.Struct({
          norms: Schema.Array(
            Schema.Struct({
              name: Schema.String,
              description: Schema.String,
              document: Schema.String,
            }),
          ),
        }),
        readOnly: true,
      },
      () =>
        Effect.succeed({
          norms: norms.map((norm) => ({
            name: norm.name,
            description: norm.description,
            document: norm.source,
          })),
        }),
    );
  }
  return mcp;
}

/**
 * Runs `work` for the MCP SDK in the runtime of the HTTP request's fiber, so
 * its spans keep their parent, and interrupts it when `signal` aborts.
 */
function inRequest<A>(work: Effect.Effect<A>, signal?: AbortSignal) {
  return Effect.runPromise(
    Effect.fromNullable(requestRuntime.getStore()).pipe(
      Effect.flatMap((runtime) => Effect.provide(work, runtime)),
      Effect.orDie,
    ),
    { signal },
  );
}

/** The server agent's tools: it addresses any client and reads its firewall's decisions. */
function registerServerAgentTools(
  mcp: McpServer,
  conversations: Conversations,
  agent: AgentCard,
) {
  registerConversationTools(
    mcp,
    { conversations, agent },
    {
      description:
        "Send a message. Returns id, the message id, and address, the canonical conversation address. A refusal names the argument or rule to correct. An outcome-unknown failure must not be retried as a new send. Use receive_messages to read what others send.",
      input: SendRequest,
      peer: ({ to }) => addressedAgent(conversations, agent, to),
    },
    {
      description:
        "Read one conversation's records: the messages you sent and the inbox entries of the messages you received. A page holds the oldest records after cursor; pass nextCursor as cursor, with the same address, until it is absent.",
      input: Schema.Struct({
        address: AgentAddress,
        ...PaginatedRequest.fields,
      }),
      peer: ({ address }) => addressedAgent(conversations, agent, address),
    },
  );
  register(
    mcp,
    "list_conversations",
    {
      description:
        "List the address of every conversation with a message. Use read_conversation to read one address.",
      input: noArguments,
      output: Schema.Struct({ addresses: Schema.Array(AgentAddress) }),
      readOnly: true,
    },
    () =>
      Effect.map(peersOf(conversations, agent), (peers) => ({
        addresses: peers.map(addressOf).sort(Order.string),
      })),
  );
  register(
    mcp,
    "read_firewall_decisions",
    {
      description:
        "Read completed firewall decision summaries since inclusive Unix milliseconds. Each gives the decision applied (allowed, refused or withheld), the reason and the deciding rules' registered advice, without message content or private model evidence.",
      input: Schema.Struct({ since: Schema.optional(Schema.NonNegativeInt) }),
      output: Schema.Struct({ decisions: Schema.Array(checkRecordSchema) }),
      readOnly: true,
    },
    ({ since }) =>
      Effect.map(readDecisions(conversations, since ?? 0), (decisions) => ({
        decisions,
      })),
  );
}

/** The tools of a client, which name no recipient and no conversation: both are the server agent. */
function registerClientTools(
  mcp: McpServer,
  conversations: Conversations,
  agent: AgentCard,
) {
  const peer = () => Effect.succeed(conversations.serverAgent);
  registerConversationTools(
    mcp,
    { conversations, agent },
    {
      description:
        "Send a message to the server agent. Returns the message id. An outcome-unknown failure must not be retried as a new send.",
      input: Schema.Struct({
        text: Schema.propertySignature(wellFormedString).annotations({
          description: "The message text.",
        }),
        replyToMsgId: Schema.optional(MsgId),
      }),
      peer,
    },
    {
      description:
        "Read the messages you sent and received in your conversation with the server agent. Follow nextCursor until absent.",
      input: Schema.Struct({ ...PaginatedRequest.fields }),
      peer,
    },
  );
}

/** One of the two conversation tools as a role has it: what it says, what it takes and the peer its input names. */
interface PeerTool<Input, Encoded> {
  readonly description: string;
  readonly input: Schema.Schema<Input, Encoded>;
  readonly peer: (input: Input) => Effect.Effect<AgentCard, AddressError>;
}

/**
 * Registers `send_message` and `read_conversation` for `agent`. The server
 * agent names its peer by address; a client's peer is the server agent.
 */
function registerConversationTools<
  Sent extends Omit<SendRequest, "to">,
  SentEncoded,
  Read extends { readonly cursor?: string },
  ReadEncoded,
>(
  mcp: McpServer,
  context: {
    readonly conversations: Conversations;
    readonly agent: AgentCard;
  },
  sending: PeerTool<Sent, SentEncoded>,
  reading: PeerTool<Read, ReadEncoded>,
) {
  const { conversations, agent } = context;
  register(
    mcp,
    "send_message",
    {
      description: sending.description,
      input: sending.input,
      output: SendResult,
      unexpected: sendOutcomeUnknown,
    },
    (input) =>
      Effect.gen(function* () {
        const recipient = yield* sending.peer(input);
        const id = yield* send(conversations, {
          text: input.text,
          replyToMsgId: input.replyToMsgId,
          author: agent,
          recipient,
        });
        return { id, address: addressOf(recipient.agentName) };
      }),
  );
  register(
    mcp,
    "read_conversation",
    {
      description: reading.description,
      input: reading.input,
      output: historyRecordsPage,
      readOnly: true,
    },
    (input) =>
      Effect.gen(function* () {
        const other = yield* reading.peer(input);
        const page = yield* history(conversations, agent, other, input.cursor);
        return {
          ...page,
          records: page.records.map((record) =>
            record.kind === "sent"
              ? record
              : { kind: record.kind, ...inboxEntry(record) },
          ),
        };
      }),
  );
}

/**
 * Registers a tool whose calls `execute` answers. A call runs in the runtime
 * of the HTTP request's fiber and is interrupted when the request is aborted.
 * Its value is the result's structured content. A typed failure is a result
 * marked as an error. A call that ends any other way is logged and answered
 * as `unexpected`, or with a fixed text that says nothing of what went wrong.
 */
function register<A, I, B extends object, J extends object, E extends Failure>(
  mcp: McpServer,
  name: string,
  tool: {
    readonly description: string;
    readonly input: Schema.Schema<A, I>;
    readonly output: Schema.Schema<B, J>;
    readonly readOnly?: true;
    readonly unexpected?: Failure;
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
      inRequest(
        execute(input).pipe(
          Effect.flatMap((value) =>
            Effect.orDie(Schema.encode(tool.output)(value)),
          ),
          Effect.map((structuredContent) => ({ structuredContent })),
          Effect.catchAll((failure) => Effect.succeed(failed(failure))),
          Effect.catchAllDefect((defect) =>
            Effect.logError("A tool call ended unexpectedly", defect).pipe(
              Effect.as(
                tool.unexpected === undefined
                  ? {
                      isError: true as const,
                      content: [
                        {
                          type: "text" as const,
                          text: "The daemon could not complete this call. Try again; if it keeps failing, check the daemon's log.",
                        },
                      ],
                    }
                  : failed(tool.unexpected),
              ),
            ),
          ),
        ),
        ctx.mcpReq.signal,
      ),
  );
}

/** A typed failure of a tool: what a model reads of it, and its own fields, which a host reads. */
interface Failure {
  readonly message: string;
  readonly reason: string;
}

/** The result of a call that failed with `failure`: its message as the text, and its fields without its tag. */
function failed(failure: Failure) {
  return {
    isError: true as const,
    content: [{ type: "text" as const, text: failure.message }],
    structuredContent: Object.fromEntries(
      Object.entries(failure).filter(([field]) => field !== "_tag"),
    ),
  };
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

/**
 * One check of the inbox event: at most `maxEvents` of the entries after
 * `cursor` in the inbox of the agent `agentId` names, as occurrences.
 */
function inboxOccurrences(
  conversations: Conversations,
  agentId: string | null,
  after: { readonly cursor: string | null; readonly maxEvents?: number },
) {
  return Effect.gen(function* () {
    const page = yield* readInbox(
      conversations,
      yield* Effect.fromNullable(agentId),
      {
        limit: Math.min(after.maxEvents ?? PAGE_SIZE, PAGE_SIZE),
        ...(after.cursor === null
          ? {}
          : { cursor: yield* Schema.decode(Cursor)(after.cursor) }),
      },
    );
    return {
      events: (yield* Schema.encode(Schema.Array(InboxEntry))(
        page.entries.map(inboxEntry),
      )).map((data) => ({
        eventId: `${data.message.author.agentId}/${data.message.msgId}`,
        data,
      })),
      cursor: page.nextCursor,
      hasMore: page.hasMore,
    };
  });
}

/** An inbox entry as a host reads it: under the address of its message's author. */
function inboxEntry(entry: InboxEntryOf): typeof InboxEntry.Type {
  return {
    address: addressOf(entry.message.author.agentName),
    message: entry.message,
    ...(entry.guidance === undefined ? {} : { guidance: entry.guidance }),
  };
}

/** The other agent `address` names for `self`. */
function addressedAgent(
  conversations: Conversations,
  self: AgentCard,
  address: AgentAddress,
) {
  return Effect.gen(function* () {
    const agent = yield* agentNamed(
      conversations,
      address.slice(AGENT_ADDRESS_PREFIX.length),
    );
    if (agent === undefined) {
      return yield* new AddressError({ reason: "unknown-agent", address });
    }
    if (agent.agentId === self.agentId) {
      return yield* new AddressError({ reason: "invalid-address", address });
    }
    return agent;
  });
}
