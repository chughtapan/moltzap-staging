/**
 * @file The daemon in a test's own process: its real HTTP app over a test
 * database and a test OAuth issuer, with the server agent's real firewall.
 * A test signs agents in and calls their tools as a host does. `callTool`
 * and the helpers over it also drive a daemon process.
 */

import { HttpApp } from "@effect/platform";
import {
  Client,
  StreamableHTTPClientTransport,
} from "@modelcontextprotocol/client";
import { Effect, Schema } from "effect";
import type { TestPolicy } from "./firewall-fixtures.js";
import {
  AgentAddress,
  getOrCreateOwnedAgent,
  HistoryRecord,
  InboxEntry,
  openDaemon,
} from "../daemon/agent.js";
import { makeMcpHttpApp } from "../daemon/server.js";
import { checkRecordSchema, type Firewall, makeFirewall } from "../firewall.js";
import { AgentId, AgentName, MsgId } from "../messages.js";
import { type InstalledNorms, structureCheck } from "../norms/index.js";
import { issuerFixture, openTestDatabase, requiredScope } from "./test.js";

/** The resource every in-process daemon is. */
const resource = new URL("https://mcp.example.test/mcp");

const protocolVersion = "2026-07-28";

/** What a provider reads of the message it judges and the history before it. */
export const decisionState = Schema.decodeUnknownSync(
  Schema.parseJson(
    Schema.Struct({
      direction: Schema.Literal("incoming", "outgoing"),
      candidate: Schema.Struct({ text: Schema.String }),
      history: Schema.Array(Schema.Struct({ text: Schema.String })),
      earlierHistoryOmitted: Schema.Boolean,
    }),
  ),
);

/** A signed-in owner: its agent, how the server agent addresses it, and an MCP connection as it. */
export interface SignedIn {
  readonly agent: { readonly agentId: AgentId; readonly agentName: AgentName };
  readonly address: AgentAddress;
  readonly mcp: Client;
}

/**
 * A daemon whose server agent is named `alice`, behind a firewall with the
 * given policy, norms, notice and history limit, or one that allows every
 * message. `firewall` replaces operations of that firewall. `signInAs` gives
 * an MCP connection as the agent of the owner a name stands for, created
 * under that name. `request` sends one raw MCP request with the bearer token
 * it is given.
 */
export const inProcessDaemon = Effect.fn("inProcessDaemon")(function* (
  options: {
    readonly policy?: TestPolicy;
    readonly norms?: InstalledNorms;
    readonly notice?: string;
    readonly historyLimit?: number;
    readonly firewall?: Partial<Firewall>;
  } = {},
) {
  const database = yield* openTestDatabase();
  const issuer = yield* issuerFixture().pipe(Effect.orDie);
  const join = (name: string) =>
    getOrCreateOwnedAgent(
      { issuer: issuer.issuer, subject: name },
      AgentName.make(name),
    ).pipe(database.run, Effect.orDie);
  const serverAgent = yield* join("alice");
  const firewall = makeFirewall({
    ...(options.policy ?? {}),
    me: serverAgent,
    historyLimit: options.historyLimit ?? 256,
    ...(options.notice === undefined ? {} : { notice: options.notice }),
    ...(options.norms === undefined
      ? {}
      : { structureCheck: structureCheck(options.norms) }),
  });
  const app = yield* makeMcpHttpApp(
    { resource, issuer: issuer.issuer, scope: requiredScope },
    yield* openDaemon({
      serverAgent,
      firewall: { ...firewall, ...options.firewall },
    }),
  ).pipe(database.run, Effect.orDie);
  const handle = HttpApp.toWebHandler(app);
  const token = (
    subject: string,
    claims: Parameters<typeof issuer.token>[2] = {},
  ) => issuer.token(subject, resource.href, claims).pipe(Effect.orDie);
  return {
    sql: database.sql,
    issuer,
    resource,
    token,
    request: (
      bearer: string,
      method: string,
      params: Readonly<Record<string, unknown>> = {},
    ) =>
      Effect.tryPromise(() => handle(mcpRequest(bearer, method, params))).pipe(
        Effect.orDie,
      ),
    signInAs: (name: string) =>
      Effect.gen(function* () {
        const agent = yield* join(name);
        const mcp = yield* connect(handle, yield* token(name));
        const signedIn: SignedIn = {
          agent,
          address: AgentAddress.make(`agent:${name}`),
          mcp,
        };
        return signedIn;
      }),
  };
});

const fields = Schema.decodeUnknownSync(
  Schema.Record({ key: Schema.String, value: Schema.Unknown }),
);

/** What tests read of the result of each tool they call. */
const results = {
  status: Schema.Struct({ agentId: AgentId, agentName: AgentName }),
  send_message: Schema.Struct({ id: MsgId, address: AgentAddress }),
  receive_messages: Schema.Struct({
    entries: Schema.Array(InboxEntry),
    nextCursor: Schema.String,
  }),
  list_conversations: Schema.Struct({
    addresses: Schema.Array(AgentAddress),
  }),
  read_conversation: Schema.Struct({
    records: Schema.Array(HistoryRecord),
    nextCursor: Schema.optional(Schema.String),
  }),
  read_firewall_decisions: Schema.Struct({
    decisions: Schema.Array(checkRecordSchema),
  }),
};
type ToolName = keyof typeof results;

export function callTool<Name extends ToolName>(
  agent: Pick<SignedIn, "mcp">,
  name: Name,
  input?: object,
): Effect.Effect<
  (typeof results)[Name]["Type"],
  Readonly<Record<string, unknown>>
>;
/**
 * Calls a tool as `agent` and decodes its result. A call the daemon answers
 * as failed fails with the result's structured fields and, as `message`, the
 * text a model reads. Interrupting the call aborts its request.
 */
export function callTool(
  agent: Pick<SignedIn, "mcp">,
  name: ToolName,
  input: object = {},
): Effect.Effect<unknown, Readonly<Record<string, unknown>>> {
  const output: Schema.Schema.AnyNoContext = results[name];
  return Effect.tryPromise((signal) =>
    agent.mcp.callTool({ name, arguments: { ...input } }, { signal }),
  ).pipe(
    Effect.orDie,
    Effect.flatMap((result) =>
      result.isError === true
        ? Effect.fail({
            ...fields(result.structuredContent ?? {}),
            message: result.content
              .flatMap((part) => (part.type === "text" ? [part.text] : []))
              .join("\n"),
          })
        : Schema.decodeUnknown(output)(result.structuredContent).pipe(
            Effect.map((decoded): unknown => decoded),
            Effect.orDie,
          ),
    ),
  );
}

/** Sends a message as `sender`. The server agent names its recipient in `to`; a client names none. */
export const send = (
  sender: Pick<SignedIn, "mcp">,
  input: {
    readonly to?: AgentAddress;
    readonly text: string;
    readonly replyToMsgId?: MsgId;
  },
) => callTool(sender, "send_message", input);

/** The messages of the first page of the agent's inbox, from its first entry. */
export const inboxOf = (agent: Pick<SignedIn, "mcp">) =>
  Effect.map(callTool(agent, "receive_messages", { cursor: "0" }), (page) =>
    page.entries.map((entry) => entry.message),
  );

/** One raw MCP request to the daemon, as the holder of `bearer`. */
function mcpRequest(
  bearer: string,
  method: string,
  params: Readonly<Record<string, unknown>>,
) {
  return new Request(resource, {
    method: "POST",
    headers: {
      host: resource.host,
      authorization: `Bearer ${bearer}`,
      "content-type": "application/json",
      accept: "application/json, text/event-stream",
      "mcp-protocol-version": protocolVersion,
      "mcp-method": method,
    },
    body: JSON.stringify({
      jsonrpc: "2.0",
      id: 1,
      method,
      params: {
        ...params,
        _meta: {
          "io.modelcontextprotocol/protocolVersion": protocolVersion,
          "io.modelcontextprotocol/clientCapabilities": {},
          "io.modelcontextprotocol/clientInfo": {
            name: "daemon-test",
            version: "1",
          },
        },
      },
    }),
  });
}

/** An SDK client connected to the daemon `handle` answers for, as the holder of `bearer`. The scope closes it. */
function connect(
  handle: ReturnType<typeof HttpApp.toWebHandler>,
  bearer: string,
) {
  return Effect.gen(function* () {
    const mcp = new Client(
      { name: "daemon-test", version: "1" },
      { versionNegotiation: { mode: { pin: protocolVersion } } },
    );
    const transport = new StreamableHTTPClientTransport(resource, {
      fetch: (input, init) => handle(new Request(input, init)),
      requestInit: {
        headers: { host: resource.host, authorization: `Bearer ${bearer}` },
      },
    });
    yield* Effect.tryPromise(() => mcp.connect(transport)).pipe(Effect.orDie);
    yield* Effect.addFinalizer(() =>
      Effect.tryPromise(() => mcp.close()).pipe(Effect.ignore),
    );
    return mcp;
  });
}
