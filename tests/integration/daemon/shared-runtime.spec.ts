/** @file Owners who sign in to one daemon each get their own agent, inbox and history. */
import { HttpClient } from "@effect/platform";
import { NodeHttpClient } from "@effect/platform-node";
import { SqlClient } from "@effect/sql";
import {
  MAXIMUM_WAIT_SECONDS,
  type toolDefinitions,
} from "@social-harness/social-harness";
import {
  databaseConnection,
  databaseUrl,
} from "@social-harness/social-harness/test";
import { ConfigProvider, Context, Effect, Layer, Redacted } from "effect";
import { expect, it } from "vitest";
import { send } from "../agent-fixture.js";
import { processTrace } from "../scenario-fixture.js";
import { callTool } from "../tool-call.js";
import {
  type Daemon,
  SERVER_AGENT_NAME,
  SERVER_AGENT_OWNER,
  sharedDaemon,
  type SignedIn,
  stopProcess,
} from "./daemon-fixture.js";

const UNAUTHORIZED = 401;
const AGENT_UNAVAILABLE = 503;

/** Reads one of an agent's conversations. */
type ReadConversation = (
  agent: SignedIn,
) => Effect.Effect<
  typeof toolDefinitions.read_conversation.output.Type,
  unknown
>;

/** The server agent's conversation with `client`. */
const conversationWith =
  (client: SignedIn): ReadConversation =>
  (server) =>
    callTool(server.connection.mcp, {
      name: "read_conversation",
      arguments: { address: client.address },
    });

/** A client's conversation with the server agent, the only one it has and so not named. */
const clientConversation: ReadConversation = (client) =>
  callTool(client.connection.mcp, {
    name: "read_conversation",
    arguments: {},
  });

/**
 * The identifiers of the messages a conversation holds, in order, once it
 * holds `count`. A received message is saved after its firewall check, so the
 * read repeats; its bound allows a remote PostgreSQL's round trips.
 */
function awaitMessages(agent: SignedIn, read: ReadConversation, count: number) {
  const ids = read(agent).pipe(
    Effect.map((page) => page.records.map((record) => record.message.msgId)),
  );
  return ids.pipe(
    Effect.delay("20 millis"),
    Effect.repeat({ until: (seen) => seen.length >= count }),
    Effect.timeout("20 seconds"),
  );
}

/** The agent's pending inbox entries, waiting for one to arrive. A read marks what it returns delivered. */
const awaitInbox = (agent: SignedIn) =>
  callTool(agent.connection.mcp, {
    name: "receive_messages",
    arguments: { wait: MAXIMUM_WAIT_SECONDS },
  }).pipe(Effect.map((page) => page.entries));

function expectEmptyInbox(agent: SignedIn) {
  return Effect.gen(function* () {
    const page = yield* callTool(agent.connection.mcp, {
      name: "receive_messages",
      arguments: {},
    });
    expect(page.entries).toHaveLength(0);
  });
}

/** The daemon's database, opened as the daemon opens it. */
function openDatabase(daemon: Daemon) {
  return Effect.gen(function* () {
    const url = yield* databaseUrl.pipe(
      Effect.withConfigProvider(
        ConfigProvider.fromMap(new Map(Object.entries(daemon.environment))),
      ),
    );
    return Context.get(
      yield* Layer.build(databaseConnection(url)),
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
  server: SignedIn,
  client: SignedIn,
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
          : send(client, { text: `concurrent ${index}` }).pipe(
              Effect.map(({ id }): string => id),
            ),
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
    const other = yield* daemon.signInAs("carol");
    expect(
      yield* send(other, {
        to: client.address,
        text: "from one client to another",
      }).pipe(Effect.isFailure),
    ).toBe(true);
  });
}

/**
 * One daemon, its server agent and a client, from before the first sign-in to
 * a restart after the client's agent record is lost. The server agent exists
 * from startup.
 */
const scenario = Effect.gen(function* () {
  const daemon = yield* sharedDaemon();
  const sql = yield* openDatabase(daemon);
  const owners = sql`SELECT * FROM owner_bindings`;
  expect(yield* owners).toHaveLength(0);
  const original = yield* daemon.start();
  expect(yield* owners).toHaveLength(1);
  expect(yield* requestStatus(daemon)).toBe(UNAUTHORIZED);
  expect(yield* owners).toHaveLength(1);

  const serverBearer = yield* daemon.bearer(SERVER_AGENT_OWNER);
  const clientBearer = yield* daemon.bearer("bob");
  const sent = yield* Effect.scoped(
    Effect.gen(function* () {
      const server = yield* daemon.signIn(serverBearer);
      const client = yield* daemon.signIn(clientBearer);
      expect(server.agent.agentId).not.toBe(client.agent.agentId);
      expect(server.agent.agentName).toBe(SERVER_AGENT_NAME);
      expect((yield* daemon.signIn(serverBearer)).agent).toEqual(server.agent);
      expect(yield* owners).toHaveLength(2);

      const first = yield* send(server, {
        to: client.address,
        text: "read before the restart",
      });
      expect(yield* awaitInbox(client)).toMatchObject([
        { item: { message: { msgId: first.id } } },
      ]);
      yield* expectEmptyInbox(client);
      yield* expectEmptyInbox(server);
      expect(yield* awaitMessages(server, conversationWith(client), 1)).toEqual(
        [first.id],
      );
      expect(yield* awaitMessages(client, clientConversation, 1)).toEqual([
        first.id,
      ]);
      const unread = yield* send(server, {
        to: client.address,
        text: "unread across the restart",
      });
      return { server: server.agent, client: client.agent, first, unread };
    }),
  );
  yield* stopProcess(original);
  const restarted = yield* daemon.start();

  const client = yield* daemon.signIn(clientBearer);
  const server = yield* daemon.signIn(serverBearer);
  expect(client.agent).toEqual(sent.client);
  expect(server.agent).toEqual(sent.server);
  expect(yield* awaitInbox(client)).toMatchObject([
    { item: { message: { msgId: sent.unread.id } } },
  ]);
  yield* expectEmptyInbox(client);
  const earlier = [sent.first.id, sent.unread.id];
  expect(
    yield* awaitMessages(client, clientConversation, earlier.length),
  ).toEqual(earlier);
  expect(yield* owners).toHaveLength(2);
  yield* expectClientServerExchange(daemon, server, client, earlier);

  yield* sql`DELETE FROM agent_cards WHERE agent_name = ${client.agent.agentName}`;
  yield* stopProcess(restarted);
  const serving = yield* daemon.start();
  expect((yield* daemon.signIn(serverBearer)).agent).toEqual(sent.server);
  expect(yield* requestStatus(daemon, clientBearer)).toBe(AGENT_UNAVAILABLE);
  expect(serving.child.exitCode).toBeNull();
});

it(
  "creates only the server agent before sign-in, keeps each owner's agent, inbox and history across a restart, and serves the server agent when a client's record is lost",
  processTrace(scenario),
  180_000,
);
