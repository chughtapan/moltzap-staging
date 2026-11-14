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
import type { MessageAddressInput } from "../../communication/index.js";
import { testAgentCard } from "../../__tests__/agents.js";
import { openTestDatabase } from "../../communication/__tests__/store-fixtures.js";
import { FirewallPolicyError } from "../../firewall/index.js";
import { Firewall } from "../../firewall/service.js";
import { Identity } from "../../identity.js";
import { makeMcpHttpApp, type McpAuthentication } from "../http.js";
import { makeAgentMcpApp } from "../server.js";
import { SocialHarness } from "../social-harness.js";
import { makeWebhookEvents, webhookHttpClientLayer } from "../webhook.js";

/** The credential has no authority beyond this process. */
export const mcpTestCredential = Redacted.make(
  "mcp-test-credential-0000000000000001",
);
/** Local requests use the same bearer as the server fixture. */
const mcpTestHeaders = {
  authorization: `Bearer ${Redacted.value(mcpTestCredential)}`,
  host: "127.0.0.1",
};

/** An MCP app over a database, whose `harness` operations a test replaces one by one. */
export const mcpFixture = Effect.fn("mcpFixture")(function* (
  options: {
    readonly hideCollectives?: boolean;
    readonly serverAgent?: MessageAddressInput;
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
    acknowledgeDelivery: () => Effect.void,
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
  const webhook = yield* makeWebhookEvents(database.directory).pipe(
    Effect.provideService(
      HttpClient.HttpClient,
      Context.get(callbackClient, HttpClient.HttpClient),
    ),
    Effect.provideService(SocialHarness, harness),
    Effect.provideService(SqlClient.SqlClient, database.sql),
  );
  const stopped = yield* Deferred.make<undefined>();
  const agentApp = yield* makeAgentMcpApp({
    webhook,
    stopped,
    hideCollectives: options.hideCollectives ?? false,
    serverAgent: Option.fromNullable(options.serverAgent),
    implementation: { name: "mcp-contract-test", version: "1" },
  }).pipe(
    Effect.provideService(SocialHarness, harness),
    Effect.provideService(Identity, identity),
    Effect.provideService(Firewall, firewall),
  );
  const owners = yield* Ref.make<
    ReadonlyArray<{ readonly issuer: string; readonly subject: string }>
  >([]);
  const app = yield* makeMcpHttpApp(
    options.authentication ?? { kind: "local", credential: mcpTestCredential },
    (owner) =>
      Ref.update(owners, (values) => [...values, owner]).pipe(
        Effect.as(agentApp),
      ),
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
