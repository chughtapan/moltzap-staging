/** @file MCP binds the harness services and owns its HTTP listener. */

import {
  type HttpApp,
  HttpBody,
  HttpServerRequest,
  HttpServerResponse,
} from "@effect/platform";
import { NodeHttpServer } from "@effect/platform-node";
import {
  Cause,
  type Context,
  Deferred,
  Duration,
  Effect,
  JSONSchema,
  Match,
  Option,
  Schema,
  type Scope,
  Stream,
} from "effect";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- NodeHttpServer accepts the native server constructor.
import { createServer } from "node:http";
import type { MessageAddressInput } from "../../communication/messages.js";
import type { RequestError } from "../../communication/service.js";
import type { OwnerAccount } from "../ownership.js";
import { issuesText, SendError } from "../../communication/errors.js";
import { Firewall, type FirewallRefusal } from "../../firewall/service.js";
import { AgentIdentityCard } from "../../identity/agent-cards.js";
import { Identity } from "../../identity/service.js";
import { HeldNorms } from "../../norms/configuration.js";
import {
  CallToolResult,
  McpServerClient,
  ProtocolError,
  Tool,
  ToolAnnotations,
} from "../../vendor/effect-mcp/McpSchema.js";
import { makeHttpApp, McpServer } from "../../vendor/effect-mcp/McpServer.js";
import { InboxError } from "../inbox.js";
import {
  type SendRequest,
  TextSendArguments,
  validationIssues,
} from "../message-format.js";
import { SocialHarness } from "../social-harness.js";
import {
  authenticationChallenge,
  makeRequestAdmission,
  type McpAuthentication,
  protectedResourcePath,
  protectedResourceResponse,
} from "./authentication.js";
import {
  clientToolDefinitions,
  eventListInput,
  eventStreamInput,
  eventSubscribeInput,
  eventUnsubscribeInput,
  type HarnessWebhookEvents,
  INBOX_PENDING_EVENT,
  sendErrorData,
  toolDefinitions,
} from "./contracts.js";
import { countPendingInbox, inboxPendingEvent } from "./webhook.js";

interface AgentServerSettings {
  readonly webhook: HarnessWebhookEvents;
  /** Leaves questions and answers out of send_message, which then sends text only. */
  readonly hideCollectives: boolean;
  /**
   * Set when this agent is a client in client/server mode: the address of the
   * server agent, the only agent it exchanges messages with.
   */
  readonly serverAgent: Option.Option<MessageAddressInput>;
  readonly stopped: Deferred.Deferred<undefined>;
  readonly implementation: { readonly name: string; readonly version: string };
}

/** A failed send as the wire carries it: its message, and data a caller can read back. */
const sendFailure = (error: SendError | RequestError | FirewallRefusal) =>
  new ProtocolError(-32603, error.message, sendErrorData(error));

/** A send that ended without a declared result may or may not have been accepted. */
const sendOutcomeUnknown = sendFailure(
  new SendError({ reason: "outcome-unknown" }),
);

/** The answer for a tool that ended in a way it does not declare. */
const unexpectedFailure = new ProtocolError(
  -32603,
  "The daemon could not complete this call. Try again; if it keeps failing, check the daemon's log.",
);

const reasonError = (error: {
  readonly reason: string;
  readonly message: string;
}) => new ProtocolError(-32603, error.message, { reason: error.reason });
const loopback = (host: string) =>
  ["127.0.0.1", "[::1]", "localhost"].includes(host);

const decodeEventArguments = <A, I>(
  schema: Schema.Schema<A, I>,
  input: unknown,
) =>
  Schema.decodeUnknown(schema)(input).pipe(
    Effect.catchTag("ParseError", () =>
      Effect.fail(new ProtocolError(-32602, "Invalid event arguments")),
    ),
  );
const requireEvent = (
  name: string,
  expected: string,
  cursor?: string | null,
) => {
  if (name !== expected) {
    return Effect.fail(
      new ProtocolError(-32011, "Event not found", { kind: "event" }),
    );
  }
  return cursor == null
    ? Effect.void
    : Effect.fail(
        new ProtocolError(
          -32014,
          "Event replay is unavailable; read the inbox",
          { feature: "cursor" },
        ),
      );
};

/** Registers the retained agent's Events handlers without acquiring delivery workers. */
const installEvents = Effect.fn("installEvents")(function* (
  webhook: HarnessWebhookEvents,
) {
  const harness = yield* SocialHarness;
  const identity = yield* Identity;
  const server = yield* McpServer;
  const registered = identity
    .me()
    .pipe(
      Effect.flatMap((status) =>
        status.kind === "registered"
          ? Effect.void
          : Effect.fail(
              new ProtocolError(-32012, "Endpoint is not registered"),
            ),
      ),
    );
  server.events.current = {
    list: (raw) =>
      decodeEventArguments(eventListInput, raw).pipe(
        Effect.flatMap((input) =>
          input.cursor === undefined
            ? Effect.succeed({
                events: [
                  {
                    name: INBOX_PENDING_EVENT,
                    description: "A wakeup to read the pending inbox.",
                    delivery: ["push", "webhook"],
                    inputSchema: {
                      type: "object",
                      properties: {},
                      additionalProperties: false,
                    },
                    payloadSchema: {
                      type: "object",
                      properties: {
                        pendingCount: { type: "integer", minimum: 0 },
                      },
                      required: ["pendingCount"],
                    },
                  },
                ],
              })
            : Effect.fail(
                new ProtocolError(-32602, "Invalid event catalog cursor"),
              ),
        ),
      ),
    stream: (raw) => streamPending(harness, registered, raw),
    subscribe: (raw) =>
      Effect.gen(function* () {
        const input = yield* decodeEventArguments(eventSubscribeInput, raw);
        yield* registered;
        yield* requireEvent(input.name, INBOX_PENDING_EVENT, input.cursor);
        if (input.delivery.mode !== "webhook") {
          return yield* Effect.fail(
            new ProtocolError(-32602, "Unsupported delivery mode"),
          );
        }
        return yield* webhook.subscribe(input, registered);
      }),
    unsubscribe: (raw) =>
      Effect.gen(function* () {
        const input = yield* decodeEventArguments(eventUnsubscribeInput, raw);
        yield* requireEvent(input.name, INBOX_PENDING_EVENT);
        yield* webhook.unsubscribe(input);
        return {};
      }),
  };
}, Effect.withSpan("installEvents"));

function streamPending(
  harness: Context.Tag.Service<typeof SocialHarness>,
  registered: Effect.Effect<void, ProtocolError>,
  raw: unknown,
) {
  return Effect.scoped(
    Effect.gen(function* () {
      const input = yield* decodeEventArguments(eventStreamInput, raw);
      yield* registered;
      yield* requireEvent(input.name, INBOX_PENDING_EVENT, input.cursor);
      const context = yield* McpServerClient;
      const notify = context.notify;
      if (notify === undefined) {
        return yield* Effect.fail(
          new ProtocolError(-32603, "Event stream unavailable"),
        );
      }
      yield* notify("notifications/events/active", {
        cursor: null,
        truncated: false,
      });
      yield* Effect.sleep("20 seconds").pipe(
        Effect.zipRight(
          notify("notifications/events/heartbeat", { cursor: null }),
        ),
        Effect.forever,
        Effect.forkScoped,
      );
      yield* Stream.concat(Stream.make(undefined), harness.inboxChanged).pipe(
        Stream.runForEach(() =>
          countPendingInbox(harness).pipe(
            Effect.catchTag("InboxError", () =>
              Effect.fail(new ProtocolError(-32603, "Inbox unavailable")),
            ),
            Effect.flatMap((count) =>
              notify(
                "notifications/events/event",
                inboxPendingEvent(count, Date.now()),
              ),
            ),
          ),
        ),
      );
      return {};
    }),
  );
}

/** One retained agent context owns its tools, inbox and notification subscribers. */
export const makeAgentMcpApp = Effect.fn("makeAgentMcpApp")(function* (
  settings: AgentServerSettings,
) {
  const harness = yield* SocialHarness;
  const identity = yield* Identity;
  const firewall = yield* Firewall;
  const server = yield* McpServer.make;
  const registered = identity
    .me()
    .pipe(Effect.map((state) => state.kind === "registered"));
  yield* installEvents(settings.webhook).pipe(
    Effect.provideService(McpServer, server),
  );
  const policyConfigured = firewall
    .readPolicy()
    .pipe(Effect.match({ onFailure: () => false, onSuccess: () => true }));
  const norms = yield* Effect.serviceOption(HeldNorms);

  yield* Option.match(settings.serverAgent, {
    onNone: () =>
      installMessageTools(server, harness, registered, settings).pipe(
        Effect.zipRight(
          installPolicyTools(server, firewall, registered, policyConfigured),
        ),
      ),
    onSome: (serverAgent) =>
      installClientTools(server, harness, registered, serverAgent),
  });
  yield* installIdentityTools(server, identity, settings.webhook);
  if (Option.isSome(norms)) {
    yield* installNormTools(
      server,
      norms.value,
      registered,
      Option.isSome(settings.serverAgent)
        ? clientToolDefinitions.read_norms
        : toolDefinitions.read_norms,
    );
  }

  const app = yield* makeHttpApp(settings.implementation).pipe(
    Effect.provideService(McpServer, server),
  );
  const unavailable = Deferred.await(settings.stopped).pipe(
    Effect.interruptible,
    Effect.as(HttpServerResponse.text("Agent unavailable", { status: 503 })),
  );
  return Deferred.isDone(settings.stopped).pipe(
    Effect.flatMap((stopped) =>
      stopped ? unavailable : app.pipe(Effect.raceFirst(unavailable)),
    ),
    Effect.flatMap((response) =>
      responseWhileRunning(response, settings.stopped),
    ),
  );
});

/** Streams retain the agent-stop signal after their HTTP response is created. */
function responseWhileRunning(
  response: HttpServerResponse.HttpServerResponse,
  stopped: Deferred.Deferred<undefined>,
) {
  return Match.value(response.body).pipe(
    Match.tag("Stream", (body) =>
      HttpServerResponse.setBody(
        response,
        HttpBody.stream(
          body.stream.pipe(Stream.interruptWhen(Deferred.await(stopped))),
          body.contentType,
        ),
      ),
    ),
    Match.orElse(() => response),
  );
}

/** Authentication selects the account before any agent context or tool is acquired. */
export const makeMcpHttpApp = Effect.fn("makeMcpHttpApp")(function* (
  authentication: McpAuthentication,
  selectApp: (
    owner: OwnerAccount,
  ) => Effect.Effect<HttpApp.Default<never, Scope.Scope>, unknown, Scope.Scope>,
) {
  const admit = yield* makeRequestAdmission(authentication);
  return guardMcp(selectApp, authentication, admit);
});

function safeLocalUrl(value: string): boolean {
  const url = Schema.decodeUnknownOption(Schema.URL)(value);
  if (Option.isNone(url)) {
    return false;
  }
  const uncredentialed = !url.value.username && !url.value.password;
  const webProtocol = ["http:", "https:"].includes(url.value.protocol);
  return (
    uncredentialed &&
    !url.value.hash &&
    webProtocol &&
    loopback(url.value.hostname)
  );
}

function installMessageTools(
  server: Context.Tag.Service<typeof McpServer>,
  harness: Context.Tag.Service<typeof SocialHarness>,
  registered: Effect.Effect<boolean>,
  settings: Pick<AgentServerSettings, "hideCollectives">,
) {
  return Effect.gen(function* () {
    const send = {
      name: "send_message",
      execute: (request: SendRequest) => harness.send(request),
      failure: sendFailure,
      unexpected: sendOutcomeUnknown,
      visible: registered,
    };
    yield* settings.hideCollectives
      ? bind(server, {
          ...send,
          definition: {
            ...toolDefinitions.send_message,
            description:
              "Send a message. Returns the accepted send id and canonical conversation address. A delivery-pending or outcome-unknown failure must not be retried as a new send.",
            input: TextSendArguments,
          },
        })
      : bind(server, { ...send, definition: toolDefinitions.send_message });
    yield* installReceiveTool(server, harness, registered);
    yield* bind(server, {
      name: "list_conversations",
      definition: toolDefinitions.list_conversations,
      execute: harness.listConversations,
      failure: reasonError,
      visible: registered,
    });
    yield* bind(server, {
      name: "read_conversation",
      definition: toolDefinitions.read_conversation,
      execute: harness.readHistory,
      failure: reasonError,
      visible: registered,
    });
  });
}
/** Binds the tool that hands over pending inbox items and marks them delivered. */
function installReceiveTool(
  server: Context.Tag.Service<typeof McpServer>,
  harness: Context.Tag.Service<typeof SocialHarness>,
  registered: Effect.Effect<boolean>,
) {
  return bind(server, {
    name: "receive_messages",
    definition: toolDefinitions.receive_messages,
    execute: Effect.fn(function* ({ wait, ...request }) {
      const page = yield* harness.listInbox({
        ...request,
        ...(wait === undefined ? {} : { wait: Duration.seconds(wait) }),
      });
      for (const item of page.items) {
        yield* harness
          .acknowledgeDelivery(item.deliveryToken)
          .pipe(
            Effect.catchTag("DeliveryAcknowledgeError", () =>
              Effect.fail(new InboxError({ reason: "persistence-failed" })),
            ),
          );
      }
      return page;
    }),
    failure: reasonError,
    visible: registered,
  });
}

/**
 * The tools of a client in client/server mode. It sends to and reads its
 * conversation with the server agent, and names neither.
 */
function installClientTools(
  server: Context.Tag.Service<typeof McpServer>,
  harness: Context.Tag.Service<typeof SocialHarness>,
  registered: Effect.Effect<boolean>,
  serverAgent: MessageAddressInput,
) {
  return Effect.gen(function* () {
    yield* bind(server, {
      name: "send_message",
      definition: clientToolDefinitions.send_message,
      execute: ({ text }) => harness.send({ to: serverAgent, text }),
      failure: sendFailure,
      unexpected: sendOutcomeUnknown,
      visible: registered,
    });
    yield* installReceiveTool(server, harness, registered);
    yield* bind(server, {
      name: "read_conversation",
      definition: clientToolDefinitions.read_conversation,
      execute: (page) => harness.readHistory({ ...page, address: serverAgent }),
      failure: reasonError,
      visible: registered,
    });
  });
}
function installIdentityTools(
  server: Context.Tag.Service<typeof McpServer>,
  identity: Context.Tag.Service<typeof Identity>,
  webhook: HarnessWebhookEvents,
) {
  return Effect.gen(function* () {
    yield* bind(server, {
      name: "status",
      definition: toolDefinitions.status,
      execute: () =>
        Effect.gen(function* () {
          const state = yield* identity.me();
          if (state.kind === "unregistered") {
            return state;
          }
          const identityCard = yield* Schema.encode(AgentIdentityCard)(
            state.identityCard,
          ).pipe(Effect.orDie);
          return {
            kind: "registered" as const,
            identityCard,
            webhook: yield* webhook.status,
          };
        }),
      failure: reasonError,
      visible: Effect.succeed(true),
    });
  });
}

/** `read_norms` lists the norms this agent holds in its active policy. */
function installNormTools(
  server: Context.Tag.Service<typeof McpServer>,
  norms: Context.Tag.Service<typeof HeldNorms>,
  registered: Effect.Effect<boolean>,
  definition:
    | typeof toolDefinitions.read_norms
    | typeof clientToolDefinitions.read_norms,
) {
  return bind(server, {
    name: "read_norms",
    definition,
    execute: () =>
      norms.read().pipe(
        Effect.map((held) => ({
          norms: held.map((norm) => ({
            name: norm.name,
            description: norm.description,
            ...(norm.protocol === undefined ? {} : { protocol: norm.protocol }),
            document: norm.source,
          })),
        })),
      ),
    failure: reasonError,
    visible: registered,
  });
}

function installPolicyTools(
  server: Context.Tag.Service<typeof McpServer>,
  firewall: Context.Tag.Service<typeof Firewall>,
  registered: Effect.Effect<boolean>,
  policyConfigured: Effect.Effect<boolean>,
) {
  return Effect.gen(function* () {
    yield* bind(server, {
      name: "read_policy",
      definition: toolDefinitions.read_policy,
      execute: () => firewall.readPolicy(),
      failure: reasonError,
      visible: Effect.zipWith(
        registered,
        policyConfigured,
        (active, configured) => active && configured,
      ),
    });
    yield* bind(server, {
      name: "update_policy",
      definition: toolDefinitions.update_policy,
      execute: firewall.updatePolicy,
      failure: (error) =>
        new ProtocolError(-32603, error.message, {
          reason: error.reason,
          ...error.data,
        }),
      visible: Effect.zipWith(
        registered,
        policyConfigured,
        (active, configured) => active && configured,
      ),
    });
    yield* bind(server, {
      name: "read_firewall_decisions",
      definition: toolDefinitions.read_firewall_decisions,
      execute: firewall.readDecisions,
      failure: reasonError,
      visible: registered,
    });
  });
}
/** A refusal of arguments that names each wrong field in its message and its data. */
function invalidArguments(issues: ReturnType<typeof validationIssues>) {
  return new ProtocolError(
    -32602,
    `Invalid tool arguments: ${issuesText(issues)}`,
    { issues },
  );
}

function bind<A, I, B, J, E>(
  server: Context.Tag.Service<typeof McpServer>,
  options: {
    readonly name: string;
    readonly definition: {
      readonly description: string;
      readonly input: Schema.Schema<A, I>;
      readonly output: Schema.Schema<B, J>;
      readonly annotations: {
        readonly readOnlyHint: boolean;
        readonly openWorldHint: boolean;
        readonly destructiveHint?: boolean;
        readonly idempotentHint?: boolean;
      };
    };
    readonly execute: (input: A) => Effect.Effect<B, E>;
    readonly failure: (error: E) => ProtocolError;
    /** What to answer when the tool ends in a way it does not declare. */
    readonly unexpected?: ProtocolError;
    readonly visible: Effect.Effect<boolean>;
  },
) {
  const { name, definition, execute, failure, visible } = options;
  const unexpected = options.unexpected ?? unexpectedFailure;
  return server.addTool({
    tool: new Tool({
      name,
      description: definition.description,
      inputSchema: jsonSchema(definition.input),
      outputSchema: openObjects(jsonSchema(definition.output)),
      annotations: new ToolAnnotations({
        destructiveHint: true,
        idempotentHint: false,
        ...definition.annotations,
      }),
    }),
    visible,
    handle: (input) =>
      Effect.gen(function* () {
        const decoded = yield* Schema.decodeUnknown(definition.input, {
          onExcessProperty: "error",
          errors: "all",
        })(input).pipe(
          Effect.catchTag("ParseError", (error) =>
            Effect.fail(invalidArguments(validationIssues(error))),
          ),
        );
        const output = yield* execute(decoded).pipe(Effect.mapError(failure));
        const encoded = yield* Schema.encode(definition.output)(output, {
          onExcessProperty: "error",
        }).pipe(Effect.catchTag("ParseError", () => Effect.fail(unexpected)));
        return new CallToolResult({
          content: [{ type: "text", text: JSON.stringify(encoded) }],
          structuredContent: encoded,
        });
      }).pipe(
        Effect.catchAllCause((cause) => {
          const expected = Cause.failureOption(cause);
          if (Option.isSome(expected)) {
            return Effect.fail(expected.value);
          }
          return Effect.fail(unexpected);
        }),
      ),
  });
}

function guardMcp(
  selectApp: (
    owner: OwnerAccount,
  ) => Effect.Effect<HttpApp.Default<never, Scope.Scope>, unknown, Scope.Scope>,
  authentication: McpAuthentication,
  admit: Effect.Effect.Success<ReturnType<typeof makeRequestAdmission>>,
) {
  return Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const host = request.headers.host;
    const origin = request.headers.origin;
    const oauth = authentication.kind === "oauth" ? authentication : undefined;
    const allowedHost =
      host !== undefined &&
      (oauth === undefined
        ? safeLocalUrl(`http://${host}`)
        : host.toLowerCase() === oauth.resource.host.toLowerCase());
    const allowedOrigin =
      origin === undefined ||
      (oauth === undefined
        ? safeLocalUrl(origin)
        : oauth.allowedOrigins.includes(origin));
    if (!allowedHost || !allowedOrigin) {
      return HttpServerResponse.text("Forbidden", { status: 403 });
    }
    const path = new URL(request.url, "http://localhost").pathname;
    if (
      oauth !== undefined &&
      path === protectedResourcePath(oauth) &&
      request.method === "GET"
    ) {
      return HttpServerResponse.fromWeb(protectedResourceResponse(oauth));
    }
    if (path !== "/mcp") {
      return HttpServerResponse.empty({ status: 404 });
    }
    if (request.method !== "POST") {
      return HttpServerResponse.empty({
        status: 405,
        headers: { allow: "POST" },
      });
    }
    const authorityRequest = new Request(
      oauth?.resource ?? "http://127.0.0.1/mcp",
      { headers: request.headers },
    );
    return yield* admit(authorityRequest).pipe(
      Effect.matchEffect({
        onFailure: () =>
          Effect.succeed(
            HttpServerResponse.fromWeb(authenticationChallenge(authentication)),
          ),
        onSuccess: ({ owner, expiresAt }) =>
          selectApp(owner).pipe(
            Effect.flatMap((app) => app),
            Effect.flatMap((response) => expireResponse(response, expiresAt)),
            Effect.catchAll(() =>
              Effect.succeed(
                HttpServerResponse.text("Agent unavailable", { status: 503 }),
              ),
            ),
          ),
      }),
    );
  });
}

/** A request cannot outlive the authority that admitted its streaming body. */
function expireResponse(
  response: HttpServerResponse.HttpServerResponse,
  expiresAt?: number,
) {
  if (expiresAt === undefined) {
    return response;
  }
  return Match.value(response.body).pipe(
    Match.tag("Stream", (body) =>
      HttpServerResponse.setBody(
        response,
        HttpBody.stream(
          body.stream.pipe(
            Stream.interruptAfter(Math.max(0, expiresAt - Date.now())),
          ),
          body.contentType,
        ),
      ),
    ),
    Match.orElse(() => response),
  );
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

/** Equivalent single-item tuple notation also supports native clients using draft-07 validators. */
function openObjects(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(openObjects);
  }
  if (value === null || typeof value !== "object") {
    return value;
  }
  const entries = Object.entries(value);
  const prefixItems: unknown = entries.find(
    ([key]) => key === "prefixItems",
  )?.[1];
  const singleTuple =
    Array.isArray(prefixItems) &&
    prefixItems.length === 1 &&
    entries.some(([key, nested]) => key === "items" && nested === false);
  return Object.fromEntries([
    ...entries.flatMap(([key, nested]) => {
      if (key === "additionalProperties" && nested === false) {
        return [];
      }
      if (singleTuple && (key === "items" || key === "maxItems")) {
        return [];
      }
      if (singleTuple && key === "prefixItems") {
        return [["items", openObjects(prefixItems[0])]];
      }
      return [[key, openObjects(nested)]];
    }),
    ...(singleTuple ? [["maxItems", 1]] : []),
  ]);
}

/** Local bearer mode uses the same default address for binding and advertisement. */
const LOOPBACK_HOST = "127.0.0.1";

/** Returns after listening. Scope closure disconnects streaming requests before the listener closes. */
export const serveMcp = Effect.fn("Mcp.serve")(function* (
  port: number,
  app: HttpApp.Default<never, Scope.Scope>,
  host: string = LOOPBACK_HOST,
) {
  const listener = yield* Effect.sync(createServer);
  const server = yield* NodeHttpServer.make(() => listener, {
    host,
    port,
  });
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      listener.closeAllConnections();
    }),
  );
  yield* server.serve(app);
});
