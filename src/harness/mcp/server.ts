/** @file One agent's MCP app: its tools and its inbox events. */

import { HttpServerResponse } from "@effect/platform";
import {
  Cause,
  type Context,
  Deferred,
  Duration,
  Effect,
  JSONSchema,
  Option,
  Schema,
  Stream,
} from "effect";
import type { MessageAddressInput } from "../../communication/messages.js";
import type { RequestError } from "../../communication/service.js";
import { issuesText, SendError } from "../../communication/errors.js";
import { Firewall, type FirewallRefusal } from "../../firewall/index.js";
import { Identity } from "../../identity/service.js";
import { HeldNorms } from "../../norms/index.js";
import {
  CallToolResult,
  ProtocolError,
  Tool,
  ToolAnnotations,
} from "../../vendor/effect-mcp/McpSchema.js";
import { makeHttpApp, McpServer } from "../../vendor/effect-mcp/McpServer.js";
import { InboxError } from "../inbox.js";
import { validationIssues } from "../message-format.js";
import { SocialHarness } from "../social-harness.js";
import {
  clientToolDefinitions,
  eventListInput,
  eventSubscribeInput,
  eventUnsubscribeInput,
  type HarnessWebhookEvents,
  INBOX_PENDING_EVENT,
  inboxPendingCatalogEntry,
  sendErrorData,
  textToolDefinitions,
  toolDefinitions,
} from "./contracts.js";
import { limitResponseStream } from "./http.js";

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

type Harness = Context.Tag.Service<typeof SocialHarness>;

/**
 * Serves one agent's tools and inbox events until the agent stops, after
 * which every request, and every stream still open, gets 503.
 */
export const makeAgentMcpApp = Effect.fn("makeAgentMcpApp")(function* (
  settings: AgentServerSettings,
) {
  const harness = yield* SocialHarness;
  const firewall = yield* Firewall;
  const server = yield* McpServer.make;
  server.events.current = inboxEvents(settings.webhook);
  yield* Effect.forEach(
    [
      yield* statusTool(settings.webhook),
      ...Option.match(settings.serverAgent, {
        onNone: () => peerTools(harness, firewall, settings),
        onSome: (serverAgent) => clientTools(harness, serverAgent),
      }),
      ...Option.toArray(yield* Effect.serviceOption(HeldNorms)).map((norms) =>
        normsTool(norms, Option.isSome(settings.serverAgent)),
      ),
    ],
    server.addTool,
    { concurrency: 1, discard: true },
  );
  const app = yield* makeHttpApp(settings.implementation).pipe(
    Effect.provideService(McpServer, server),
  );
  const stopped = Deferred.await(settings.stopped);
  const unavailable = stopped.pipe(
    Effect.interruptible,
    Effect.as(HttpServerResponse.text("Agent unavailable", { status: 503 })),
  );
  return Deferred.isDone(settings.stopped).pipe(
    Effect.flatMap((done) =>
      done ? unavailable : app.pipe(Effect.raceFirst(unavailable)),
    ),
    Effect.flatMap((response) =>
      limitResponseStream(response, Stream.interruptWhen(stopped)),
    ),
  );
});

/** The tools of a peer: it addresses any agent and owns its message policy. */
function peerTools(
  harness: Harness,
  firewall: Context.Tag.Service<typeof Firewall>,
  settings: Pick<AgentServerSettings, "hideCollectives">,
) {
  const policyConfigured = firewall
    .readPolicy()
    .pipe(Effect.match({ onFailure: () => false, onSuccess: () => true }));
  return [
    settings.hideCollectives
      ? sendTool(textToolDefinitions.send_message, harness.send)
      : sendTool(toolDefinitions.send_message, harness.send),
    receiveTool(harness),
    bind({
      name: "list_conversations",
      definition: toolDefinitions.list_conversations,
      execute: harness.listConversations,
    }),
    bind({
      name: "read_conversation",
      definition: toolDefinitions.read_conversation,
      execute: harness.readHistory,
    }),
    bind({
      name: "read_policy",
      definition: toolDefinitions.read_policy,
      execute: () => firewall.readPolicy(),
      visible: policyConfigured,
    }),
    bindWith({
      name: "update_policy",
      definition: toolDefinitions.update_policy,
      execute: firewall.updatePolicy,
      failure: (error) =>
        new ProtocolError(-32603, error.message, {
          reason: error.reason,
          ...error.data,
        }),
      visible: policyConfigured,
    }),
    bind({
      name: "read_firewall_decisions",
      definition: toolDefinitions.read_firewall_decisions,
      execute: firewall.readDecisions,
    }),
  ];
}

/**
 * The tools of a client in client/server mode. It sends to and reads its
 * conversation with the server agent, and names neither.
 */
function clientTools(harness: Harness, serverAgent: MessageAddressInput) {
  return [
    sendTool(clientToolDefinitions.send_message, ({ text }) =>
      harness.send({ to: serverAgent, text }),
    ),
    receiveTool(harness),
    bind({
      name: "read_conversation",
      definition: clientToolDefinitions.read_conversation,
      execute: (page) => harness.readHistory({ ...page, address: serverAgent }),
    }),
  ];
}

/** A send that ended without a declared result may or may not have been accepted. */
function sendTool<A, I, B, J>(
  definition: Definition<A, I, B, J>,
  execute: (
    input: A,
  ) => Effect.Effect<B, SendError | RequestError | FirewallRefusal>,
) {
  const failure = (error: SendError | RequestError | FirewallRefusal) =>
    new ProtocolError(-32603, error.message, sendErrorData(error));
  return bindWith({
    name: "send_message",
    definition,
    execute,
    failure,
    unexpected: failure(new SendError({ reason: "outcome-unknown" })),
  });
}

/** Hands over pending inbox items and marks them delivered. */
function receiveTool(harness: Harness) {
  return bind({
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
  });
}

function statusTool(webhook: HarnessWebhookEvents) {
  return Effect.map(Identity, (identity) =>
    bind({
      name: "status",
      definition: toolDefinitions.status,
      execute: () =>
        Effect.map(webhook.status, (status) => ({
          agentId: identity.me.agentId,
          agentName: identity.me.agentName,
          webhook: status,
        })),
    }),
  );
}

/** `read_norms` lists the norms held: this agent's, or for a client the server agent's. */
function normsTool(
  norms: Context.Tag.Service<typeof HeldNorms>,
  forClient: boolean,
) {
  return bind({
    name: "read_norms",
    definition: forClient
      ? clientToolDefinitions.read_norms
      : toolDefinitions.read_norms,
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
  });
}

/** What a tool definition says about a tool before any operation is attached. */
interface Definition<A, I, B, J> {
  readonly description: string;
  readonly input: Schema.Schema<A, I>;
  readonly output: Schema.Schema<B, J>;
  readonly annotations: {
    readonly readOnlyHint: boolean;
    readonly openWorldHint: boolean;
    readonly destructiveHint?: boolean;
    readonly idempotentHint?: boolean;
  };
}

interface Binding<A, I, B, J, E> {
  readonly name: string;
  readonly definition: Definition<A, I, B, J>;
  readonly execute: (input: A) => Effect.Effect<B, E>;
  /** What to answer when the tool ends in a way it does not declare. */
  readonly unexpected?: ProtocolError;
  /** Checked on every listing and call. A tool without it is always offered. */
  readonly visible?: Effect.Effect<boolean>;
}

/** Binds a tool whose failures carry a reason and a message, which is how they reach the caller. */
function bind<A, I, B, J>(
  binding: Binding<A, I, B, J, { readonly reason: string; message: string }>,
) {
  return bindWith({
    ...binding,
    failure: (error) =>
      new ProtocolError(-32603, error.message, { reason: error.reason }),
  });
}

/**
 * Turns a tool definition and the operation behind it into a registered
 * tool: arguments are decoded with the definition's input, the result is
 * encoded with its output, and a failure is worded by `failure`.
 */
function bindWith<A, I, B, J, E>(
  options: Binding<A, I, B, J, E> & {
    readonly failure: (error: E) => ProtocolError;
  },
) {
  const { name, definition, execute, failure, visible } = options;
  const unexpected =
    options.unexpected ??
    new ProtocolError(
      -32603,
      "The daemon could not complete this call. Try again; if it keeps failing, check the daemon's log.",
    );
  return {
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
    ...(visible === undefined ? {} : { visible }),
    handle: (input: unknown) =>
      Effect.gen(function* () {
        const decoded = yield* Schema.decodeUnknown(definition.input, {
          onExcessProperty: "error",
          errors: "all",
        })(input).pipe(Effect.mapError(invalidArguments));
        const output = yield* execute(decoded).pipe(Effect.mapError(failure));
        const encoded = yield* Schema.encode(definition.output)(output, {
          onExcessProperty: "error",
        }).pipe(Effect.mapError(() => unexpected));
        return new CallToolResult({
          content: [{ type: "text", text: JSON.stringify(encoded) }],
          structuredContent: encoded,
        });
      }).pipe(
        Effect.catchAllCause((cause) =>
          Effect.fail(
            Option.getOrElse(Cause.failureOption(cause), () => unexpected),
          ),
        ),
      ),
  };
}

/** A refusal of arguments that names each wrong field in its message and its data. */
function invalidArguments(error: Parameters<typeof validationIssues>[0]) {
  const issues = validationIssues(error);
  return new ProtocolError(
    -32602,
    `Invalid tool arguments: ${issuesText(issues)}`,
    { issues },
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

/**
 * The one event a host can follow: a wake-up saying how many inbox items
 * wait. A host subscribes a webhook for it, and reads the items themselves
 * with `receive_messages`.
 */
function inboxEvents(webhook: HarnessWebhookEvents) {
  const decode = <A, I>(schema: Schema.Schema<A, I>, input: unknown) =>
    Schema.decodeUnknown(schema)(input).pipe(
      Effect.catchTag("ParseError", () =>
        Effect.fail(new ProtocolError(-32602, "Invalid event arguments")),
      ),
    );
  return {
    list: (raw: unknown) =>
      decode(eventListInput, raw).pipe(
        Effect.filterOrFail(
          (input) => input.cursor === undefined,
          () => new ProtocolError(-32602, "Invalid event catalog cursor"),
        ),
        Effect.as({ events: [inboxPendingCatalogEntry] }),
      ),
    subscribe: (raw: unknown) =>
      decode(eventSubscribeInput, raw).pipe(
        Effect.flatMap(onlyInboxPending),
        Effect.filterOrFail(
          (input) => input.delivery.mode === "webhook",
          () => new ProtocolError(-32602, "Unsupported delivery mode"),
        ),
        Effect.flatMap(webhook.subscribe),
      ),
    unsubscribe: (raw: unknown) =>
      decode(eventUnsubscribeInput, raw).pipe(
        Effect.flatMap(onlyInboxPending),
        Effect.flatMap(webhook.unsubscribe),
        Effect.as({}),
      ),
  };
}

/** Refuses any event but the inbox wake-up, and any request to replay it. */
function onlyInboxPending<
  A extends { readonly name: string; readonly cursor?: string | null },
>(input: A) {
  if (input.name !== INBOX_PENDING_EVENT) {
    return Effect.fail(
      new ProtocolError(-32011, "Event not found", { kind: "event" }),
    );
  }
  return input.cursor == null
    ? Effect.succeed(input)
    : Effect.fail(
        new ProtocolError(
          -32014,
          "Event replay is unavailable; read the inbox",
          {
            feature: "cursor",
          },
        ),
      );
}
