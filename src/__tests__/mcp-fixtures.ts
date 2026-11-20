/** @file MCP tests exercise the real protocol and HTTP boundary over the agents of a test database. */
import { HttpApp } from "@effect/platform";
import {
  Client,
  StreamableHTTPClientTransport,
} from "@modelcontextprotocol/client";
import { Effect } from "effect";
import { getOrCreateOwnedAgent } from "../daemon/agent.js";
import { makeMcpHttpApp } from "../daemon/server.js";
import { type Firewall, makeFirewall } from "../firewall/firewall.js";
import { AgentName } from "../identity.js";
import { testDaemon } from "./service-fixtures.js";
import { issuerFixture, openTestDatabase, requiredScope } from "./test.js";

/** The Host of the fixture's resource, unless a test names another resource. */
const mcpTestHeaders = { host: "127.0.0.1" };

/**
 * The daemon's HTTP app over a database that holds two agents: Alice, whose
 * owner's token `fetch` presents for a request that carries no authorization,
 * and Bob. Alice is the server agent and Bob her client, unless the options
 * make Alice the client. The server agent's firewall holds no policy, so it
 * allows every message.
 */
export const mcpFixture = Effect.fn("mcpFixture")(function* (
  options: {
    /** Makes Bob the server agent and Alice his client. */
    readonly client?: true;
    /** The daemon's resource, whose host requests name. */
    readonly resource?: URL;
    /** Operations that replace those of the server agent's firewall. */
    readonly firewall?: Partial<Firewall>;
  } = {},
) {
  const database = yield* openTestDatabase();
  const resource =
    options.resource ?? new URL(`https://${mcpTestHeaders.host}/mcp`);
  const issuer = yield* issuerFixture().pipe(Effect.orDie);
  const agent = (subject: string, name: string) =>
    getOrCreateOwnedAgent(
      { issuer: issuer.issuer, subject },
      AgentName.make(name),
    ).pipe(database.run, Effect.orDie);
  const alice = yield* agent("owner", "alice");
  const bob = yield* agent("peer", "bob");
  const serverAgent = options.client ? bob : alice;
  const firewall = makeFirewall({
    me: serverAgent,
    historyLimit: 256,
  });
  const app = yield* makeMcpHttpApp(
    { resource, issuer: issuer.issuer, scope: requiredScope },
    yield* testDaemon(serverAgent, { ...firewall, ...options.firewall }),
  ).pipe(database.run);
  const token = yield* issuer.token("owner", resource.href).pipe(Effect.orDie);
  const handle = HttpApp.toWebHandler(app);
  return {
    issuer,
    sql: database.sql,
    fetch: (request: Request) => {
      if (!request.headers.has("authorization")) {
        request.headers.set("authorization", `Bearer ${token}`);
      }
      return handle(request);
    },
    card: alice,
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
