/** @file Exposes an agent's MCP tools over authenticated loopback HTTP. */

import {
  type HttpApp,
  HttpServerRequest,
  HttpServerResponse,
} from "@effect/platform";
import { NodeHttpServer } from "@effect/platform-node";
import {
  Cause,
  type Context,
  Effect,
  JSONSchema,
  Option,
  Redacted,
  Schema,
  type Scope,
} from "effect";
import { createHash, timingSafeEqual } from "node:crypto";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- NodeHttpServer accepts the native server constructor.
import { createServer } from "node:http";
import { Firewall } from "../../firewall/service.js";
import { AgentIdentityCard } from "../../identity/agent-cards.js";
import { Identity } from "../../identity/service.js";
import {
  CallToolResult,
  ProtocolError,
  Tool,
  ToolAnnotations,
} from "../../vendor/effect-mcp/McpSchema.js";
import { makeHttpApp, McpServer } from "../../vendor/effect-mcp/McpServer.js";
import { validationIssues } from "../message-format.js";
import { SocialHarness } from "../social-harness.js";
import {
  type HarnessWebhookEvents,
  sendErrorData,
  toolDefinitions,
} from "./contracts.js";
import { installEvents, webhookHttpClientLayer } from "./events.js";

interface ServerSettings {
  readonly credential: Redacted.Redacted;
  readonly implementation: { readonly name: string; readonly version: string };
}

const reasonError = (error: { readonly reason: string }) =>
  new ProtocolError(
    -32603,
    error.reason === "admission-refused"
      ? "The Registry refused the admission credential. Supply this network’s admission credential and register again."
      : "Operation failed",
    { reason: error.reason },
  );
const digest = (value: string) => createHash("sha256").update(value).digest();
const loopback = (host: string) =>
  ["127.0.0.1", "[::1]", "localhost"].includes(host);

/** Serves the agent's MCP catalog using the supplied bearer credential. */
export const makeMcpHttpApp = Effect.fn("makeMcpHttpApp")(function* (
  settings: ServerSettings,
) {
  const harness = yield* SocialHarness;
  const identity = yield* Identity;
  const firewall = yield* Firewall;
  const server = yield* McpServer.make;
  const registered = identity
    .me()
    .pipe(Effect.map((state) => state.kind === "registered"));
  const callback = yield* installEvents().pipe(
    Effect.provideService(McpServer, server),
    Effect.provide(webhookHttpClientLayer),
  );
  const policyConfigured = firewall
    .readPolicy()
    .pipe(Effect.match({ onFailure: () => false, onSuccess: () => true }));

  yield* installMessageTools(server, harness, registered);
  yield* installIdentityTools(server, harness, identity);
  yield* installPolicyTools(server, firewall, registered, policyConfigured);
  yield* installWebhookTools(server, callback, registered);

  const app = yield* makeHttpApp(settings.implementation).pipe(
    Effect.provideService(McpServer, server),
  );
  return guardMcp(app, settings.credential);
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
) {
  return Effect.gen(function* () {
    yield* bind(server, {
      name: "send_message",
      definition: toolDefinitions.send_message,
      execute: ({ input }) => harness.send(input),
      failure: (error) =>
        new ProtocolError(-32603, error.message, sendErrorData(error)),
      visible: registered,
    });
    yield* bind(server, {
      name: "read_inbox",
      definition: toolDefinitions.read_inbox,
      execute: harness.listInbox,
      failure: reasonError,
      visible: registered,
    });
    yield* bind(server, {
      name: "read_event",
      definition: toolDefinitions.read_event,
      execute: ({ eventId }) => harness.getInboxEvent(eventId),
      failure: reasonError,
      visible: registered,
    });
    yield* bind(server, {
      name: "acknowledge_delivery",
      definition: toolDefinitions.acknowledge_delivery,
      execute: ({ deliveryToken }) =>
        harness.acknowledgeDelivery(deliveryToken).pipe(Effect.as({})),
      failure: reasonError,
      visible: registered,
    });
    yield* bind(server, {
      name: "search_conversations",
      definition: toolDefinitions.search_conversations,
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
function installIdentityTools(
  server: Context.Tag.Service<typeof McpServer>,
  harness: Context.Tag.Service<typeof SocialHarness>,
  identity: Context.Tag.Service<typeof Identity>,
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
          return { kind: "registered" as const, identityCard };
        }),
      failure: reasonError,
      visible: Effect.succeed(true),
    });
    yield* bind(server, {
      name: "register",
      definition: toolDefinitions.register,
      execute: (request) =>
        harness.register(request).pipe(
          Effect.tap(() => server.toolsChanged),
          Effect.flatMap((identityCard) =>
            Schema.encode(AgentIdentityCard)(identityCard).pipe(Effect.orDie),
          ),
        ),
      failure: reasonError,
      visible: Effect.succeed(true),
    });
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
      name: "policy_read",
      definition: toolDefinitions.policy_read,
      execute: () => firewall.readPolicy(),
      failure: reasonError,
      visible: Effect.zipWith(
        registered,
        policyConfigured,
        (active, configured) => active && configured,
      ),
    });
    yield* bind(server, {
      name: "policy_update",
      definition: toolDefinitions.policy_update,
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
      name: "firewall_decisions",
      definition: toolDefinitions.firewall_decisions,
      execute: firewall.readDecisions,
      failure: reasonError,
      visible: registered,
    });
  });
}
function installWebhookTools(
  server: Context.Tag.Service<typeof McpServer>,
  callback: HarnessWebhookEvents,
  registered: Effect.Effect<boolean>,
) {
  return Effect.gen(function* () {
    yield* bind(server, {
      name: "event_subscription_status",
      definition: toolDefinitions.event_subscription_status,
      execute: () => callback.status,
      failure: (error) => error,
      visible: registered,
    });
    yield* bind(server, {
      name: "revoke_event_subscription",
      definition: toolDefinitions.revoke_event_subscription,
      execute: () => callback.revoke.pipe(Effect.as({})),
      failure: (error) => error,
      visible: registered,
    });
    yield* bind(server, {
      name: "resume_event_subscription",
      definition: toolDefinitions.resume_event_subscription,
      execute: () => callback.resume.pipe(Effect.as({})),
      failure: (error) => error,
      visible: registered,
    });
  });
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
    readonly visible: Effect.Effect<boolean>;
  },
) {
  const { name, definition, execute, failure, visible } = options;
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
            Effect.fail(
              new ProtocolError(-32602, "Invalid tool arguments", {
                issues: validationIssues(error),
              }),
            ),
          ),
        );
        const output = yield* execute(decoded).pipe(Effect.mapError(failure));
        const encoded = yield* Schema.encode(definition.output)(output, {
          onExcessProperty: "error",
        }).pipe(
          Effect.catchTag("ParseError", () =>
            Effect.fail(
              new ProtocolError(-32603, "Invalid operation result", {
                reason:
                  name === "send_message"
                    ? "outcome-unknown"
                    : "incompatible-daemon",
              }),
            ),
          ),
        );
        return new CallToolResult({
          content: [{ type: "text", text: JSON.stringify(encoded) }],
          structuredContent: encoded,
        });
      }).pipe(
        // eslint-disable-next-line agent-code-guard/no-effect-error-coalescing -- The wire boundary sanitizes defects while preserving every declared ProtocolError.
        Effect.catchAllCause((cause) => {
          const expected = Cause.failureOption(cause);
          if (Option.isSome(expected)) {
            return Effect.fail(expected.value);
          }
          return Effect.fail(
            new ProtocolError(
              -32603,
              "Operation failed",
              name === "send_message"
                ? { reason: "outcome-unknown" }
                : undefined,
            ),
          );
        }),
      ),
  });
}

function guardMcp(
  app: HttpApp.Default<never, Scope.Scope>,
  credential: Redacted.Redacted,
) {
  return Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const host = request.headers.host;
    const origin = request.headers.origin;
    if (
      host === undefined ||
      !safeLocalUrl(`http://${host}`) ||
      (origin !== undefined && !safeLocalUrl(origin))
    ) {
      return HttpServerResponse.text("Forbidden", { status: 403 });
    }
    if (new URL(request.url, "http://localhost").pathname !== "/mcp") {
      return HttpServerResponse.empty({ status: 404 });
    }
    if (request.method !== "POST") {
      return HttpServerResponse.empty({
        status: 405,
        headers: { allow: "POST" },
      });
    }
    const authorization = request.headers.authorization;
    if (
      !authorization?.startsWith("Bearer ") ||
      !timingSafeEqual(
        digest(authorization.slice(7)),
        digest(Redacted.value(credential)),
      )
    ) {
      return HttpServerResponse.text("Unauthorized", {
        status: 401,
        headers: { "www-authenticate": "Bearer" },
      });
    }
    return yield* app;
  });
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

function openObjects(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(openObjects);
  }
  if (value === null || typeof value !== "object") {
    return value;
  }
  return Object.fromEntries(
    Object.entries(value).flatMap(([key, nested]) =>
      key === "additionalProperties" && nested === false
        ? []
        : [[key, openObjects(nested)]],
    ),
  );
}

/** The listener and advertised MCP URL use the same literal loopback address. */
export const LOOPBACK_HOST = "127.0.0.1";

/** Returns after listening. Scope closure disconnects streaming requests before the listener closes. */
export const serveMcp = Effect.fn("Mcp.serve")(function* (
  port: number,
  app: HttpApp.Default<never, Scope.Scope>,
) {
  const listener = yield* Effect.sync(createServer);
  const server = yield* NodeHttpServer.make(() => listener, {
    host: LOOPBACK_HOST,
    port,
  });
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      listener.closeAllConnections();
    }),
  );
  yield* server.serve(app);
});
