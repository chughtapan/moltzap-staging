/** @file MCP tests substitute service behavior while exercising the real protocol and HTTP boundary. */
import { HttpApp } from "@effect/platform";
import { SqlClient } from "@effect/sql";
import {
  Client,
  StreamableHTTPClientTransport,
} from "@modelcontextprotocol/client";
import { Effect, Redacted, Ref } from "effect";
import type { AgentName } from "../../identity.js";
import type { SocialHarness } from "../agent.js";
import { testAgentCard } from "../../__tests__/agents.js";
import { Firewall } from "../../firewall/service.js";
import { openTestDatabase } from "../../test.js";
import { makeMcpHttpApp } from "../listener.js";
import { issuerFixture, requiredScope } from "../testing.js";
import { makeInboxEvents } from "../tool-server.js";
import { AgentAddress } from "../values.js";

/** The Host of the fixture's resource, unless a test names another resource. */
const mcpTestHeaders = { host: "127.0.0.1" };

/**
 * The daemon's HTTP app over a database, serving one agent whose `harness`
 * operations a test replaces one by one. The agent is the server agent unless
 * the options name another. Its issuer signs the token of the one
 * owner, which `fetch` presents for a request that carries no authorization.
 */
export const mcpFixture = Effect.fn("mcpFixture")(function* (
  options: {
    /** Makes the agent a client of the server agent with this name. */
    readonly serverAgentName?: AgentName;
    /** The daemon's resource, whose host requests name. */
    readonly resource?: URL;
    readonly harness?: Partial<SocialHarness>;
  } = {},
) {
  const database = yield* openTestDatabase();
  const card = testAgentCard("alice");
  const harness: SocialHarness = {
    send: () => Effect.dieMessage("Unexpected send"),
    receive: () => Effect.succeed({ entries: [], nextCursor: "0" }),
    listConversations: () => Effect.succeed({ addresses: [] }),
    readHistory: () => Effect.succeed({ records: [] }),
    ...options.harness,
  };
  const unused = () => Effect.dieMessage("Unexpected firewall assessment");
  const firewall = Firewall.of({
    checkIncoming: unused,
    checkOutgoing: unused,
    readContracts: () => Effect.succeed([]),
    readDecisions: unused,
  });
  const agent = {
    agentKey: card.agentName,
    card,
    harness,
    firewall,
    serverAgent:
      options.serverAgentName === undefined
        ? undefined
        : AgentAddress.make(`agent:${options.serverAgentName}`),
    norms: undefined,
    events: yield* makeInboxEvents.pipe(
      Effect.provideService(SqlClient.SqlClient, database.sql),
    ),
  };
  const owners = yield* Ref.make<
    ReadonlyArray<{ readonly issuer: string; readonly subject: string }>
  >([]);
  const resource =
    options.resource ?? new URL(`https://${mcpTestHeaders.host}/mcp`);
  const issuer = yield* issuerFixture().pipe(Effect.orDie);
  const owner = { issuer: issuer.issuer, subject: "owner" };
  const token = yield* issuer
    .token(owner.subject, resource.href)
    .pipe(Effect.orDie);
  const app = yield* makeMcpHttpApp(
    {
      resource,
      issuer: issuer.issuer,
      scope: requiredScope,
    },
    (admitted) =>
      Ref.update(owners, (values) => [...values, admitted]).pipe(
        Effect.as(agent),
      ),
  );
  const handle = HttpApp.toWebHandler(app);
  return {
    app,
    owners,
    issuer,
    owner,
    credential: Redacted.make(token),
    fetch: (request: Request) => {
      if (!request.headers.has("authorization")) {
        request.headers.set("authorization", `Bearer ${token}`);
      }
      return handle(request);
    },
    card,
  };
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
