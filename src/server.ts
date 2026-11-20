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
import { SqlClient } from "@effect/sql";
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
  Runtime,
  Schema,
  type Scope,
  Stream,
} from "effect";
import { createRemoteJWKSet, jwtVerify } from "jose";
import { AsyncLocalStorage } from "node:async_hooks";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- NodeHttpServer accepts the native server constructor.
import { createServer } from "node:http";
import type { DaemonSettings } from "./settings.js";
import {
  AgentAddress,
  conversationsOf,
  type Daemon,
  getOrCreateOwnedAgent,
  historyRecords,
  type HostedAgent,
  InboxEntry,
  readInboxEntries,
  receive,
  send,
  SendError,
  SendRequest,
  SendResult,
} from "./agent.js";
import {
  Cursor,
  Message,
  MsgId,
  Page,
  PAGE_SIZE,
  PaginatedRequest,
  wellFormedString,
} from "./communication/index.js";
import { compareAscii, exactStruct, isStrictlyAscending } from "./encoding.js";
import {
  firewallDecisionsRequestSchema,
  firewallDecisionsSchema,
  LocalGuidance,
} from "./firewall/index.js";
import { AgentId, AgentName } from "./identity.js";
import { packageVersion } from "./version.js";

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

/** The issuer's metadata could not be read, names another issuer or names no key set. */
class AuthenticationRefused extends Data.TaggedError("AuthenticationRefused") {}

/**
 * The runtime of the fiber that carries the HTTP request the MCP SDK is
 * answering. The SDK builds the request's server and calls its tools from
 * promises, which this storage follows.
 */
const requestRuntime = new AsyncLocalStorage<
  Runtime.Runtime<SqlClient.SqlClient>
>();

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
  daemon: Daemon,
) {
  const sql = yield* SqlClient.SqlClient;
  const events = yield* makeInboxEvents();
  const mcp = createMcpHandler(({ authInfo }) =>
    inRequest(
      Effect.map(admittedAgent(authInfo), (agent) =>
        requestServer(daemon, agent, events),
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
    const runtime = yield* Effect.runtime<SqlClient.SqlClient>();
    const response = yield* HttpApp.fromWebHandler((mcpRequest) =>
      requestRuntime.run(runtime, () => mcp.fetch(mcpRequest, { authInfo })),
    ).pipe(Effect.interruptible, Effect.orDie);
    const untilExpiry = Stream.interruptAfter(
      Math.max(0, (authInfo.expiresAt ?? 0) * 1000 - Date.now()),
    );
    return yield* Match.value(response.body).pipe(
      Match.tag("Stream", (body) =>
        HttpServerResponse.setBody(
          response,
          HttpBody.stream(untilExpiry(body.stream), body.contentType),
        ),
      ),
      Match.orElse(() => response),
    );
  }).pipe(Effect.provideService(SqlClient.SqlClient, sql));
});

/** The agent of the owner an admitted request's token names. */
function admittedAgent(authInfo?: AuthInfo) {
  return getOrCreateOwnedAgent({
    issuer: String(authInfo?.extra?.issuer),
    subject: String(authInfo?.extra?.subject),
  }).pipe(Effect.orDie);
}

/**
 * Runs `work` for the MCP SDK in the runtime of the HTTP request's fiber, so
 * its spans keep their parent, and interrupts it when `signal` aborts.
 */
function inRequest<A>(
  work: Effect.Effect<A, never, SqlClient.SqlClient>,
  signal?: AbortSignal,
) {
  return Effect.runPromise(
    Effect.fromNullable(requestRuntime.getStore()).pipe(
      Effect.flatMap((runtime) => Effect.provide(work, runtime)),
      Effect.orDie,
    ),
    { signal },
  );
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

/** The MCP server for one request, with the inbox event and the tools of `agent`'s role. */
function requestServer(
  daemon: Daemon,
  agent: HostedAgent,
  events: EventsExtension,
) {
  const mcp = new McpServer(
    { name: "sharnessd", version: packageVersion },
    { extensions: [events] },
  );
  const { card } = agent;
  const isServerAgent = agent.agentKey === daemon.serverAgent.agentKey;
  /**
   * The longest a receive_messages call may wait. It stays under the time
   * after which hosts and proxies give up on a tool call.
   */
  const maximumWaitSeconds = 50;
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
    () => Effect.succeed({ agentId: card.agentId, agentName: card.agentName }),
  );
  register(
    mcp,
    "receive_messages",
    {
      description:
        "Receive inbox entries in order, each with its conversation address, its message and any guidance. Keep the nextCursor of every result and pass it as cursor on the next call to receive the entries after it. Without cursor the call starts from now: it returns no earlier entries. Pass wait to wait up to that many seconds when no entry follows the cursor; the call returns as soon as one becomes readable, or with no entries when the wait ends.",
      input: exactStruct({
        ...PaginatedRequest.fields,
        wait: Schema.optional(
          Schema.Number.pipe(
            Schema.positive(),
            Schema.lessThanOrEqualTo(maximumWaitSeconds),
          ).annotations({
            description: `a number of seconds, at most ${String(maximumWaitSeconds)}, to wait for an entry when none follows the cursor. Call receive_messages again with nextCursor to keep waiting. Without wait the call returns at once.`,
          }),
        ),
      }),
      output: Schema.Struct({
        entries: Schema.Array(InboxEntry).pipe(Schema.maxItems(PAGE_SIZE)),
        nextCursor: Cursor,
      }),
      readOnly: true,
    },
    (request) => receive(daemon, agent, request),
  );
  if (isServerAgent) {
    registerServerAgentTools(mcp, daemon, agent);
  } else {
    registerClientTools(mcp, daemon, agent);
  }
  if (daemon.hasNorms) {
    register(
      mcp,
      "read_norms",
      {
        description: `${
          isServerAgent
            ? "List the installed norms and their complete documents: the roles, when each act may be sent and the fields it takes."
            : "List the norms the server agent holds, each with its complete document: the roles, when each act may be sent and the fields it takes. The server agent checks your messages against them."
        } A message is one act and nothing else: (act :ontology <norm> :field value ...). Quote text as a JSON string, write a multi-select as a list such as (option1 option3), and send an act without fields as (act :ontology <norm>). The opening act omits replyToMsgId in send_message; every later act supplies the id of the message it answers.`,
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
        Effect.map(daemon.firewall.readContracts(), (contracts) => ({
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
  mcp: McpServer,
  daemon: Daemon,
  agent: HostedAgent,
) {
  register(
    mcp,
    "send_message",
    {
      description:
        "Send a message. Returns id, the message id, and address, the canonical conversation address. A refusal names the argument or rule to correct. An outcome-unknown failure must not be retried as a new send. Use receive_messages to read what others send.",
      input: SendRequest,
      output: SendResult,
      unexpected: sendOutcomeUnknown,
    },
    (request) => send(daemon, agent, request),
  );
  register(
    mcp,
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
    () => conversationsOf(agent),
  );
  register(
    mcp,
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
    (request) => historyRecords(agent, request),
  );
  register(
    mcp,
    "read_firewall_decisions",
    {
      description:
        "Read completed firewall decision summaries since inclusive Unix milliseconds. Each gives the decision applied (allowed, refused or withheld), the reason and the deciding rules' registered advice, without message content or private model evidence.",
      input: firewallDecisionsRequestSchema,
      output: firewallDecisionsSchema,
      readOnly: true,
    },
    daemon.firewall.readDecisions,
  );
}

/** The tools of a client, which name no recipient and no conversation: both are the server agent. */
function registerClientTools(
  mcp: McpServer,
  daemon: Daemon,
  agent: HostedAgent,
) {
  const serverAgent = AgentAddress.make(
    `agent:${daemon.serverAgent.card.agentName}`,
  );
  register(
    mcp,
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
    (message) => send(daemon, agent, { ...message, to: serverAgent }),
  );
  register(
    mcp,
    "read_conversation",
    {
      description:
        "Read the messages you sent and received in your conversation with the server agent. Follow nextCursor until absent.",
      input: exactStruct({ ...PaginatedRequest.fields }),
      output: historyRecordsPage,
      readOnly: true,
    },
    (page) => historyRecords(agent, { ...page, address: serverAgent }),
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
    readonly unexpected?: E;
  },
  execute: (input: A) => Effect.Effect<B, E, SqlClient.SqlClient>,
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
        ctx.mcpReq.signal,
      ),
  );
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

/** The name of the event whose occurrences are an agent's inbox entries, in read order. */
const INBOX_EVENT = "org.social-harness/inbox.pending";

/**
 * The daemon's Events extension, which every request's server is built with.
 * Its one event type is the inbox of the agent a request is admitted as: an
 * occurrence is an inbox entry, and a subscription's cursor is the read
 * number of the last entry it was sent. The SDK delivers by poll, push and
 * webhook, and keeps every subscription in memory.
 */
function makeInboxEvents() {
  return Effect.gen(function* () {
    const run = Runtime.runPromise(
      yield* Effect.runtime<SqlClient.SqlClient>(),
    );
    const events = new EventsExtension({
      getPrincipal: ({ http }) =>
        inRequest(
          Effect.map(admittedAgent(http?.authInfo), ({ agentKey }) => agentKey),
        ),
      /** How often the SDK reads the inbox of an agent whose host holds a push stream or a webhook subscription. */
      checkIntervalMs: 1000,
      /** The longest a webhook subscription lives without a refresh. */
      webhookTtlMs: 30 * 60_000,
      webhookSender: nodeWebhookSender(),
    });
    events.registerEvent(INBOX_EVENT, {
      description:
        "An inbox entry: its conversation address, its message and any guidance.",
      payloadSchema: standard(InboxEntry),
      check: (...[{ principal, maxEvents, signal }, , cursor]) =>
        run(inboxOccurrences(principal, cursor, maxEvents), { signal }),
    });
    return events;
  }).pipe(Effect.withSpan("makeInboxEvents"));
}

/**
 * One check of the inbox event: at most `maxEvents` of the entries after
 * `cursor` in the inbox of the agent stored under `agentKey`, as occurrences.
 */
function inboxOccurrences(
  agentKey: string | null,
  cursor: string | null,
  maxEvents?: number,
) {
  return Effect.gen(function* () {
    const page = yield* readInboxEntries(yield* Effect.fromNullable(agentKey), {
      limit: Math.min(maxEvents ?? PAGE_SIZE, PAGE_SIZE),
      ...(cursor === null
        ? {}
        : { cursor: yield* Schema.decode(Cursor)(cursor) }),
    });
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
  });
}
