/** @file MCP tools bind SocialHarness, Identity and Firewall directly. */
import {
  type HttpApp,
  HttpServerRequest,
  HttpServerResponse,
} from "@effect/platform";
import {
  Cause,
  Effect,
  JSONSchema,
  Option,
  Redacted,
  Schema,
  type Scope,
} from "effect";
import { createHash, timingSafeEqual } from "node:crypto";
import { Firewall } from "../../firewall/service.js";
import { sendErrorData } from "../../host-protocol/operations.js";
import { validationIssues } from "../../host-protocol/validation-errors.js";
import { AgentIdentityCard } from "../../identity/agent.js";
import { Identity } from "../../identity/service.js";
import {
  CallToolResult,
  ProtocolError,
  Tool,
  ToolAnnotations,
} from "../../vendor/effect-mcp/McpSchema.js";
import { makeHttpApp, McpServer } from "../../vendor/effect-mcp/McpServer.js";
import { SocialHarness } from "../service.js";
import { toolDefinitions } from "./contracts.js";
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

/** Captures service dependencies once; each call executes in the protocol engine's request scope. */
export const makeMcpHttpApp = Effect.fn("Mcp.makeHttpApp")(function* (
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

  function bind<A, I, B, J, E>(
    name: string,
    definition: {
      readonly description: string;
      readonly input: Schema.Schema<A, I>;
      readonly output: Schema.Schema<B, J>;
      readonly annotations: {
        readonly readOnlyHint: boolean;
        readonly openWorldHint: boolean;
        readonly destructiveHint?: boolean;
        readonly idempotentHint?: boolean;
      };
    },
    execute: (input: A) => Effect.Effect<B, E>,
    failure: (error: E) => ProtocolError,
    visible: Effect.Effect<boolean> = registered,
  ) {
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
            Effect.mapError(
              (error) =>
                new ProtocolError(-32602, "Invalid tool arguments", {
                  issues: validationIssues(error),
                }),
            ),
          );
          const output = yield* execute(decoded).pipe(Effect.mapError(failure));
          const encoded = yield* Schema.encode(definition.output)(output, {
            onExcessProperty: "error",
          }).pipe(
            Effect.mapError(
              () =>
                new ProtocolError(-32603, "Invalid operation result", {
                  reason:
                    name === "send_message"
                      ? "outcome-unknown"
                      : "incompatible-daemon",
                }),
            ),
          );
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

  yield* bind(
    "send_message",
    toolDefinitions.send_message,
    ({ input }) => harness.send(input),
    (error) => new ProtocolError(-32603, error.message, sendErrorData(error)),
  );
  yield* bind(
    "read_inbox",
    toolDefinitions.read_inbox,
    harness.listInbox,
    reasonError,
  );
  yield* bind(
    "read_event",
    toolDefinitions.read_event,
    ({ eventId }) => harness.getInboxEvent(eventId),
    reasonError,
  );
  yield* bind(
    "acknowledge_delivery",
    toolDefinitions.acknowledge_delivery,
    ({ deliveryToken }) =>
      harness.acknowledgeDelivery(deliveryToken).pipe(Effect.as({})),
    reasonError,
  );
  yield* bind(
    "search_conversations",
    toolDefinitions.search_conversations,
    harness.listConversations,
    reasonError,
  );
  yield* bind(
    "read_conversation",
    toolDefinitions.read_conversation,
    harness.readHistory,
    reasonError,
  );
  yield* bind(
    "status",
    toolDefinitions.status,
    () =>
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
    reasonError,
    Effect.succeed(true),
  );
  yield* bind(
    "register",
    toolDefinitions.register,
    (request) =>
      harness.register(request).pipe(
        Effect.tap(() =>
          server.notifications["notifications/tools/list_changed"]({}),
        ),
        Effect.flatMap((identityCard) =>
          Schema.encode(AgentIdentityCard)(identityCard).pipe(Effect.orDie),
        ),
      ),
    reasonError,
    Effect.succeed(true),
  );
  yield* bind(
    "policy_read",
    toolDefinitions.policy_read,
    () => firewall.readPolicy(),
    reasonError,
    Effect.zipWith(
      registered,
      policyConfigured,
      (active, configured) => active && configured,
    ),
  );
  yield* bind(
    "policy_update",
    toolDefinitions.policy_update,
    firewall.updatePolicy,
    (error) =>
      new ProtocolError(-32603, error.message, {
        reason: error.reason,
        ...error.data,
      }),
    Effect.zipWith(
      registered,
      policyConfigured,
      (active, configured) => active && configured,
    ),
  );
  yield* bind(
    "firewall_decisions",
    toolDefinitions.firewall_decisions,
    firewall.readDecisions,
    reasonError,
  );
  yield* bind(
    "event_subscription_status",
    toolDefinitions.event_subscription_status,
    () => callback.status,
    (error) => error,
  );
  yield* bind(
    "revoke_event_subscription",
    toolDefinitions.revoke_event_subscription,
    () => callback.revoke.pipe(Effect.as({})),
    (error) => error,
  );
  yield* bind(
    "resume_event_subscription",
    toolDefinitions.resume_event_subscription,
    () => callback.resume.pipe(Effect.as({})),
    (error) => error,
  );

  const app = yield* makeHttpApp(settings.implementation).pipe(
    Effect.provideService(McpServer, server),
  );
  const guarded: HttpApp.Default<never, Scope.Scope> = Effect.gen(function* () {
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
        digest(Redacted.value(settings.credential)),
      )
    ) {
      return HttpServerResponse.text("Unauthorized", {
        status: 401,
        headers: { "www-authenticate": "Bearer" },
      });
    }
    return yield* app;
  });
  return guarded;
});

function safeLocalUrl(value: string): boolean {
  const url = Schema.decodeUnknownOption(Schema.URL)(value);
  return (
    Option.isSome(url) &&
    !url.value.username &&
    !url.value.password &&
    !url.value.hash &&
    ["http:", "https:"].includes(url.value.protocol) &&
    loopback(url.value.hostname)
  );
}

/** The canonical contract is pure and also serves optional plugin helpers. */
export {
  toolDefinitions,
  type ToolName,
  type ToolRequest,
  type ToolResult,
} from "./contracts.js";
/** The platform owns HTTP requests, sockets and shutdown. */
export { LOOPBACK_HOST, serveMcp } from "./http.js";
