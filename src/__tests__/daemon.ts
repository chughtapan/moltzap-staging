/**
 * @file The daemon in a test's own process: `server.ts` opened on a test
 * database with the server agent's real firewall. A test signs agents in and
 * calls their tools as a host does, through the daemon's MCP handler.
 * `callTool` and the helpers over it also drive a daemon process.
 */

import {
  Client,
  StreamableHTTPClientTransport,
} from "@modelcontextprotocol/client";
import { Effect, Schema } from "effect";
import type { TestPolicy } from "./firewall-fixtures.js";
import { checkRecordSchema, type Firewall, makeFirewall } from "../firewall.js";
import { AgentId, AgentName, Message, MsgId } from "../messages.js";
import { type InstalledNorms, structureCheck } from "../norms/index.js";
import { makeMcpHandler, openDaemon, type Owner } from "../server.js";
import { openTestDatabase } from "./test.js";

/** The issuer of every owner a test signs in by name. */
const issuer = "urn:social-harness:test";

/** The URL the SDK client addresses; no listener answers it. */
const endpoint = new URL("http://127.0.0.1/mcp");

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
  readonly address: string;
  readonly mcp: Client;
}

/**
 * A daemon whose server agent is named `alice` and owned by the owner
 * `alice`, behind a firewall with the given policy, norms, notice and history
 * limit, or one that allows every message. `firewall` replaces operations of
 * that firewall. `signInAs` gives an MCP connection as the agent of an owner:
 * the owner a name stands for, or the owner given.
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
  const { norms } = options;
  const daemon = yield* openDaemon(database.url, {
    owner: { issuer, subject: "alice" },
    name: AgentName.make("alice"),
    firewall: (me) => ({
      ...makeFirewall({
        ...(options.policy ?? {}),
        me,
        historyLimit: options.historyLimit ?? 256,
        ...(options.notice === undefined ? {} : { notice: options.notice }),
        ...(norms === undefined
          ? {}
          : { structureCheck: structureCheck(norms) }),
      }),
      ...options.firewall,
    }),
    ...(norms === undefined
      ? {}
      : {
          norms: norms.map((norm) => ({
            name: norm.name,
            description: norm.description,
            document: norm.source,
          })),
        }),
  }).pipe(Effect.orDie);
  const answerAs = yield* makeMcpHandler(daemon);
  return {
    sql: database.sql,
    daemon,
    signInAs: (owner: string | Owner) =>
      Effect.gen(function* () {
        const answer = yield* answerAs({
          token: "",
          clientId: "",
          scopes: [],
          extra:
            typeof owner === "string"
              ? { issuer, subject: owner }
              : { ...owner },
        });
        const mcp = yield* connect(answer);
        const agent = yield* callTool({ mcp }, "status").pipe(Effect.orDie);
        const signedIn: SignedIn = {
          agent,
          address: `agent:${agent.agentName}`,
          mcp,
        };
        return signedIn;
      }),
  };
});

const fields = Schema.decodeUnknownSync(
  Schema.Record({ key: Schema.String, value: Schema.Unknown }),
);

/** An inbox entry as a host reads it. */
const inboxEntry = Schema.Struct({
  address: Schema.String,
  message: Message,
  guidance: Schema.optional(Schema.Unknown),
});

/** Reads an inbox entry from what the inbox event delivers. */
export const decodeInboxEntry = Schema.decodeUnknown(inboxEntry);

/** What tests read of the result of each tool they call. */
const results = {
  status: Schema.Struct({ agentId: AgentId, agentName: AgentName }),
  send_message: Schema.Struct({ id: MsgId, address: Schema.String }),
  receive_messages: Schema.Struct({
    entries: Schema.Array(inboxEntry),
    nextCursor: Schema.String,
  }),
  list_conversations: Schema.Struct({
    addresses: Schema.Array(Schema.String),
  }),
  read_conversation: Schema.Struct({
    records: Schema.Array(
      Schema.Struct({
        kind: Schema.Literal("sent", "received"),
        message: Message,
      }),
    ),
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
    readonly to?: string;
    readonly text: string;
    readonly replyToMsgId?: MsgId;
  },
) => callTool(sender, "send_message", input);

/** The messages of the first page of the agent's inbox, from its first entry. */
export const inboxOf = (agent: Pick<SignedIn, "mcp">) =>
  Effect.map(callTool(agent, "receive_messages", { cursor: "0" }), (page) =>
    page.entries.map((entry) => entry.message),
  );

/** An SDK client whose requests `answer` answers. The scope closes it. */
function connect(answer: (request: Request) => ReturnType<typeof fetch>) {
  return Effect.gen(function* () {
    const mcp = new Client(
      { name: "daemon-test", version: "1" },
      { versionNegotiation: { mode: { pin: "2026-07-28" } } },
    );
    const transport = new StreamableHTTPClientTransport(endpoint, {
      fetch: (input, init) => answer(new Request(input, init)),
    });
    yield* Effect.tryPromise(() => mcp.connect(transport)).pipe(Effect.orDie);
    yield* Effect.addFinalizer(() =>
      Effect.tryPromise(() => mcp.close()).pipe(Effect.ignore),
    );
    return mcp;
  });
}
