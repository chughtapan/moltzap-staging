/** @file Owners who sign in to one daemon each get their own agent, inbox and history, in both modes and on both databases. */
import { HttpClient } from "@effect/platform";
import { NodeHttpClient } from "@effect/platform-node";
import { SqlClient } from "@effect/sql";
import {
  MAXIMUM_WAIT_SECONDS,
  toolDefinitions,
} from "@social-harness/social-harness";
import {
  databaseAccess,
  databaseConnection,
} from "@social-harness/social-harness/test";
import {
  ConfigProvider,
  Context,
  Effect,
  Layer,
  Redacted,
  Schema,
} from "effect";
import { describe, expect, it } from "vitest";
import { callTool } from "../../../plugins/shared/mcp-client.js";
import { send } from "../agent-fixture.js";
import { processTrace } from "../scenario-fixture.js";
import {
  type Agent,
  type Daemon,
  sharedDaemon,
  stopProcess,
} from "./daemon-fixture.js";
import { acquirePostgres } from "./postgres-fixture.js";

/** In client/server mode Alice is the server agent, which signs in with the daemon's own credential. */
const SERVER_AGENT_NAME = "alice-server";

const UNAUTHORIZED = 401;
const AGENT_UNAVAILABLE = 503;

/** What `status` says about an agent. */
const identity = ({ agentId, agentName }: Agent) => ({ agentId, agentName });

/**
 * Calls a tool the way a client in client/server mode does. Its send_message
 * and read_conversation name no recipient and no conversation.
 */
function callAsClient<A, I>(
  agent: Agent,
  name: string,
  input: Readonly<Record<string, unknown>>,
  output: Schema.Schema<A, I>,
) {
  return Effect.tryPromise(() =>
    agent.connection.client.callTool({ name, arguments: { ...input } }),
  ).pipe(
    Effect.filterOrFail((result) => result.isError !== true),
    Effect.flatMap((result) =>
      Schema.decodeUnknown(output)(result.structuredContent),
    ),
  );
}

/** Reads one of an agent's conversations. */
type ReadConversation = (
  agent: Agent,
) => Effect.Effect<
  typeof toolDefinitions.read_conversation.output.Type,
  unknown
>;

/** The agent's conversation with `peer`, named by the peer's address. */
const conversationWith =
  (peer: Agent): ReadConversation =>
  (agent) =>
    callTool(agent.connection.client, {
      name: "read_conversation",
      arguments: { address: peer.address },
    });

/** A client's conversation with the server agent, the only one it has and so not named. */
const clientConversation: ReadConversation = (agent) =>
  callAsClient(
    agent,
    "read_conversation",
    {},
    toolDefinitions.read_conversation.output,
  );

/** The identifiers of the messages a conversation holds, in order, once it holds `count`. */
function awaitMessages(agent: Agent, read: ReadConversation, count: number) {
  const ids = read(agent).pipe(
    Effect.map((page) =>
      page.records.flatMap((record) =>
        record.kind === "result" ? [] : [record.message.msgId],
      ),
    ),
  );
  return ids.pipe(
    Effect.delay("20 millis"),
    Effect.repeat({ until: (seen) => seen.length >= count }),
    Effect.timeout("20 seconds"),
  );
}

/** The agent's pending items, waiting for one to arrive. A read removes what it returns. */
const awaitInbox = (agent: Agent) =>
  callTool(agent.connection.client, {
    name: "receive_messages",
    arguments: { wait: MAXIMUM_WAIT_SECONDS },
  }).pipe(Effect.map((page) => page.items));

function expectEmptyInbox(agent: Agent) {
  return Effect.gen(function* () {
    const page = yield* callTool(agent.connection.client, {
      name: "receive_messages",
      arguments: {},
    });
    expect(page.items).toHaveLength(0);
  });
}

/** The daemon's database, opened as the daemon opens it. */
function openDatabase(daemon: Daemon) {
  return Effect.gen(function* () {
    const access = yield* databaseAccess.pipe(
      Effect.withConfigProvider(
        ConfigProvider.fromMap(new Map(Object.entries(daemon.environment))),
      ),
    );
    return Context.get(
      yield* Layer.build(databaseConnection(access.locator)),
      SqlClient.SqlClient,
    );
  });
}

/** The status the daemon answers a request that presents `bearer`, or no bearer. */
function requestStatus(daemon: Daemon, bearer?: Redacted.Redacted) {
  return HttpClient.HttpClient.pipe(
    Effect.flatMap((client) =>
      client.post(daemon.endpoint.href, {
        headers:
          bearer === undefined
            ? {}
            : { authorization: `Bearer ${Redacted.value(bearer)}` },
      }),
    ),
    Effect.map((response) => response.status),
    Effect.provide(NodeHttpClient.layer),
  );
}

/**
 * The server agent and a client send concurrently and both save one order,
 * and a second client cannot address the first.
 */
function expectClientServerExchange(
  daemon: Daemon,
  server: Agent,
  client: Agent,
  earlier: readonly string[],
) {
  return Effect.gen(function* () {
    const concurrent = yield* Effect.forEach(
      [0, 1, 2, 3, 4, 5],
      (index): Effect.Effect<string, unknown> =>
        index % 2 === 0
          ? send(server, {
              to: client.address,
              text: `concurrent ${index}`,
            }).pipe(Effect.map(({ id }): string => id))
          : callAsClient(
              client,
              "send_message",
              { text: `concurrent ${index}` },
              toolDefinitions.send_message.output,
            ).pipe(Effect.map(({ id }): string => id)),
      { concurrency: 6 },
    );
    const count = earlier.length + concurrent.length;
    const serverOrder = yield* awaitMessages(
      server,
      conversationWith(client),
      count,
    );
    expect(yield* awaitMessages(client, clientConversation, count)).toEqual(
      serverOrder,
    );
    expect(new Set(serverOrder)).toEqual(new Set([...earlier, ...concurrent]));
    const other = yield* daemon.agent("carol");
    expect(
      yield* send(other, {
        to: client.address,
        text: "from one client to another",
      }).pipe(Effect.isFailure),
    ).toBe(true);
  });
}

/**
 * The request bound allows a remote PostgreSQL tunnel's round trips; it is not
 * a delivery latency assertion.
 */
const scenario = (
  clientServer: boolean,
  database?: Effect.Effect.Success<typeof acquirePostgres>,
) =>
  Effect.gen(function* () {
    const daemon = yield* sharedDaemon({
      ...(database === undefined ? {} : { database }),
      ...(clientServer
        ? {
            settings: {
              mode: {
                kind: "client-server",
                serverAgentName: SERVER_AGENT_NAME,
              },
            },
          }
        : {}),
    });
    const sql = yield* openDatabase(daemon);
    const owners = sql`SELECT * FROM owner_bindings`;
    expect(yield* owners).toHaveLength(0);
    const original = yield* daemon.start();
    const atStart = (yield* owners).length;
    expect(atStart).toBe(clientServer ? 1 : 0);
    expect(yield* requestStatus(daemon)).toBe(UNAUTHORIZED);
    expect(yield* owners).toHaveLength(atStart);

    const aliceBearer = clientServer
      ? daemon.serverAgentBearer
      : yield* daemon.bearer("alice");
    const bobBearer = yield* daemon.bearer("bob");
    const sent = yield* Effect.scoped(
      Effect.gen(function* () {
        const alice = yield* daemon.signIn(aliceBearer);
        const bob = yield* daemon.signIn(bobBearer);
        const bobReads = clientServer
          ? clientConversation
          : conversationWith(alice);
        expect(alice.agentId).not.toBe(bob.agentId);
        if (clientServer) {
          expect(alice.agentName).toBe(SERVER_AGENT_NAME);
        }
        expect(identity(yield* daemon.signIn(aliceBearer))).toEqual(
          identity(alice),
        );
        expect(yield* owners).toHaveLength(2);

        const first = yield* send(alice, {
          to: bob.address,
          text: "read before the restart",
        });
        expect(yield* awaitInbox(bob)).toMatchObject([
          { item: { message: { msgId: first.id } } },
        ]);
        yield* expectEmptyInbox(bob);
        yield* expectEmptyInbox(alice);
        expect(yield* awaitMessages(alice, conversationWith(bob), 1)).toEqual([
          first.id,
        ]);
        expect(yield* awaitMessages(bob, bobReads, 1)).toEqual([first.id]);
        const unread = yield* send(alice, {
          to: bob.address,
          text: "unread across the restart",
        });
        return { alice: identity(alice), bob: identity(bob), first, unread };
      }),
    );
    yield* stopProcess(original);
    const restarted = yield* daemon.start();

    const bob = yield* daemon.signIn(bobBearer);
    const alice = yield* daemon.signIn(aliceBearer);
    expect(identity(bob)).toEqual(sent.bob);
    expect(identity(alice)).toEqual(sent.alice);
    expect(yield* awaitInbox(bob)).toMatchObject([
      { item: { message: { msgId: sent.unread.id } } },
    ]);
    yield* expectEmptyInbox(bob);
    const earlier = [sent.first.id, sent.unread.id];
    expect(
      yield* awaitMessages(
        bob,
        clientServer ? clientConversation : conversationWith(alice),
        earlier.length,
      ),
    ).toEqual(earlier);
    expect(yield* owners).toHaveLength(2);
    if (clientServer) {
      yield* expectClientServerExchange(daemon, alice, bob, earlier);
    }

    yield* sql`DELETE FROM agent_profiles WHERE agent_name = ${bob.agentName}`;
    yield* stopProcess(restarted);
    const serving = yield* daemon.start();
    expect(identity(yield* daemon.signIn(aliceBearer))).toEqual(sent.alice);
    expect(yield* requestStatus(daemon, bobBearer)).toBe(AGENT_UNAVAILABLE);
    expect(serving.child.exitCode).toBeNull();
  });

describe.each([
  { mode: "P2P", clientServer: false },
  { mode: "client/server", clientServer: true },
])("in $mode mode", ({ clientServer }) => {
  it(
    "creates no agent before sign-in, keeps each owner's agent, inbox and history across a restart, and serves one owner when another's record is lost",
    processTrace(scenario(clientServer)),
    180_000,
  );

  it(
    "does so on PostgreSQL with a restricted login",
    processTrace(
      acquirePostgres.pipe(
        Effect.flatMap((database) => scenario(clientServer, database)),
      ),
    ),
    240_000,
  );
});
