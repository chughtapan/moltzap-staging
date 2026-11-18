/** @file MCP tests substitute service behavior while exercising the real protocol and HTTP boundary. */
import { HttpApp, HttpClient } from "@effect/platform";
import { NodeHttpServer } from "@effect/platform-node";
import { SqlClient } from "@effect/sql";
import {
  Client,
  StreamableHTTPClientTransport,
} from "@modelcontextprotocol/client";
import {
  Context,
  Deferred,
  Effect,
  Layer,
  Option,
  Redacted,
  Ref,
  type Scope,
  Stream,
} from "effect";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- NodeHttpServer accepts the native server constructor.
import { createServer } from "node:http";
import type { HeldNorms } from "../../norms/index.js";
import { testAgentCard } from "../../__tests__/agents.js";
import { openTestDatabase } from "../../database/testing.js";
import { FirewallPolicyError } from "../../firewall/index.js";
import { Firewall } from "../../firewall/service.js";
import { type AgentName, Identity } from "../../identity.js";
import { SocialHarness } from "../agent.js";
import { makeMcpHttpApp, type McpAuthentication } from "../listener.js";
import { roleOf } from "../startup.js";
import { makeWebhookEvents, webhookHttpClientLayer } from "../subscriptions.js";

/** The credential has no authority beyond this process. */
export const mcpTestCredential = Redacted.make(
  "mcp-test-credential-0000000000000001",
);
/** Local requests use the same bearer as the server fixture. */
const mcpTestHeaders = {
  authorization: `Bearer ${Redacted.value(mcpTestCredential)}`,
  host: "127.0.0.1",
};

/** The daemon's HTTP app over a database, serving one agent whose `harness` operations a test replaces one by one. */
export const mcpFixture = Effect.fn("mcpFixture")(function* (
  options: {
    readonly hideCollectives?: boolean;
    /** Makes the agent a client of the server agent with this name. */
    readonly serverAgentName?: AgentName;
    readonly serverAgentCredential?: Redacted.Redacted;
    readonly authentication?: McpAuthentication;
    readonly harness?: Partial<Context.Tag.Service<typeof SocialHarness>>;
  } = {},
) {
  const database = yield* openTestDatabase();
  const card = testAgentCard("alice");
  const identity = Identity.of({
    me: card,
    resolve: () => Effect.dieMessage("Unexpected identity resolution"),
  });
  const harness = SocialHarness.of({
    send: () => Effect.dieMessage("Unexpected send"),
    listInbox: () => Effect.succeed({ entries: [] }),
    receive: () => Effect.succeed({ entries: [] }),
    listConversations: () => Effect.succeed({ addresses: [] }),
    readHistory: () => Effect.succeed({ records: [] }),
    inboxChanged: Stream.never,
    ...options.harness,
  });
  const unused = () => Effect.dieMessage("Unexpected firewall assessment");
  const firewall = Firewall.of({
    checkIncoming: unused,
    checkOutgoing: unused,
    readPolicy: () =>
      Effect.fail(new FirewallPolicyError({ reason: "not-configured" })),
    readNorms: () =>
      Effect.fail(new FirewallPolicyError({ reason: "not-configured" })),
    updatePolicy: unused,
    readDecisions: unused,
  });
  const callbackClient = yield* Layer.build(webhookHttpClientLayer);
  const subscription = yield* makeWebhookEvents(card.agentName).pipe(
    Effect.provideService(
      HttpClient.HttpClient,
      Context.get(callbackClient, HttpClient.HttpClient),
    ),
    Effect.provideService(SocialHarness, harness),
    Effect.provideService(SqlClient.SqlClient, database.sql),
  );
  const stopped = yield* Deferred.make<undefined>();
  const role = roleOf(
    {
      settings: {
        serverAgentName: options.serverAgentName,
        hideGroups: false,
        hideCollectives: options.hideCollectives ?? false,
      },
      serverAgentHeldNorms: yield* Ref.make(
        Option.none<Context.Tag.Service<typeof HeldNorms>>(),
      ),
    },
    card.agentName,
    card.agentName,
  );
  const agent = {
    tools: yield* role
      .tools(subscription)
      .pipe(
        Effect.provideService(SocialHarness, harness),
        Effect.provideService(Identity, identity),
        Effect.provideService(Firewall, firewall),
      ),
    events: subscription,
    stopped,
  };
  const owners = yield* Ref.make<
    ReadonlyArray<{ readonly issuer: string; readonly subject: string }>
  >([]);
  const app = yield* makeMcpHttpApp(
    options.authentication ?? { kind: "local", credential: mcpTestCredential },
    (owner) =>
      Ref.update(owners, (values) => [...values, owner]).pipe(Effect.as(agent)),
    Option.fromNullable(options.serverAgentCredential),
  );
  return {
    app,
    stopped,
    owners,
    fetch: HttpApp.toWebHandler(app),
    card,
  };
});

/** The listener and every in-flight request belong to the calling scope. */
export const loopbackMcpEndpoint = Effect.fn(function* (
  app: HttpApp.Default<never, Scope.Scope>,
) {
  const server = yield* NodeHttpServer.make(createServer, {
    host: "127.0.0.1",
    port: 0,
  });
  yield* server.serve(app);
  if (server.address._tag !== "TcpAddress") {
    return yield* Effect.dieMessage("Expected TCP listener");
  }
  return new URL(`http://127.0.0.1:${server.address.port}/mcp`);
});

/** A stock client connects through HTTP encoding and closes with the test scope. */
export const connectInProcessClient = Effect.fn("connectInProcessClient")(
  function* (fetch: typeof globalThis.fetch) {
    const client = new Client(
      { name: "mcp-contract-test", version: "1" },
      {
        capabilities: {},
        versionNegotiation: { mode: { pin: "2026-07-28" } },
      },
    );
    // eslint-disable-next-line agent-code-guard/acquire-release-requires-scope -- The caller supplies Scope and owns the client connection.
    yield* Effect.acquireRelease(
      Effect.tryPromise(() =>
        client.connect(
          new StreamableHTTPClientTransport(new URL("http://127.0.0.1/mcp"), {
            fetch,
            requestInit: { headers: mcpTestHeaders },
          }),
        ),
      ),
      () => Effect.tryPromise(() => client.close()).pipe(Effect.ignore),
    );
    return client;
  },
);

/** A request can vary IDs, metadata and cancellation without changing the application contract. */
export function mcpRequest(
  method: string,
  params: Readonly<Record<string, unknown>> = {},
  options: {
    readonly id?: string | number;
    readonly headers?: Readonly<Record<string, string>>;
    readonly signal?: AbortSignal;
    readonly version?: string;
  } = {},
) {
  const version = options.version ?? "2026-07-28";
  return new Request("http://127.0.0.1/mcp", {
    method: "POST",
    headers: {
      ...mcpTestHeaders,
      "content-type": "application/json",
      accept: "application/json, text/event-stream",
      "mcp-protocol-version": version,
      "mcp-method": method,
      ...(typeof params.name === "string" ? { "mcp-name": params.name } : {}),
      ...options.headers,
    },
    body: JSON.stringify({
      jsonrpc: "2.0",
      id: options.id ?? 1,
      method,
      params: {
        ...params,
        _meta: {
          "io.modelcontextprotocol/protocolVersion": version,
          "io.modelcontextprotocol/clientCapabilities": {},
          "io.modelcontextprotocol/clientInfo": {
            name: "mcp-contract-test",
            version: "1",
          },
        },
      },
    }),
    signal: options.signal,
  });
}
