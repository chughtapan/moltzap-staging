/** @file Real OAuth accounts share one daemon while identity, inbox and recovery remain isolated. */
import type { Client } from "@modelcontextprotocol/client";
import { FileSystem, HttpBody, HttpClient } from "@effect/platform";
import { NodeHttpClient } from "@effect/platform-node";
import { SqlClient } from "@effect/sql";
import {
  AgentAddress,
  AgentIdentityCard,
  toolDefinitions,
} from "@social-harness/social-harness";
import {
  databaseAccess,
  databaseConnection,
  makeInbox,
  withAgentTransaction,
} from "@social-harness/social-harness/test";
import {
  ConfigProvider,
  Context,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  Schema,
} from "effect";
import { expect, it } from "vitest";
import { callTool as call } from "../../../plugins/shared/mcp-client.js";
import { processTrace } from "../scenario-fixture.js";
import {
  acquireDaemonProcess,
  acquireProcessInfrastructure,
  type DaemonProcessFixture,
  makeDaemonProcessFixture,
  type ProcessInfrastructure,
  reservePort,
  type RunningProcess,
  stopProcess,
} from "./daemon-fixture.js";
import { connect, issuerFixture, requiredScope } from "./oauth-fixture.js";
import { acquirePostgres } from "./postgres-fixture.js";

/** An account that owns one agent of the daemon. */
interface Owner {
  readonly issuer: string;
  readonly subject: string;
}

/** In client/server mode Alice is the server agent, which the daemon creates and its own credential admits. */
const SERVER_AGENT_NAME = "alice-server";
const SERVER_AGENT_OWNER: Owner = {
  issuer: "urn:social-harness:server-agent",
  subject: "owner",
};

/* eslint-disable agent-code-guard/no-hardcoded-assertion-literals -- HTTP statuses and closed tool outcomes are external conformance contracts. */
/** The existing tools return each caller's retained view without consuming delivery. */
function expectConversation(
  client: Client,
  conversation: Conversation,
  kind: "sent" | "received",
  messageIds: readonly string[],
) {
  return Effect.gen(function* () {
    if (Option.isSome(conversation.address)) {
      const catalog = yield* call(client, {
        name: "list_conversations",
        arguments: {},
      });
      expect(catalog.addresses).toContain(conversation.address.value);
    }
    const page = yield* conversation.read(client);
    expect(page.records.map((record) => record.kind)).toEqual(
      messageIds.map(() => kind),
    );
    expect(
      page.records.flatMap((record) =>
        record.kind !== "result" && record.kind === kind
          ? [record.message.msgId]
          : [],
      ),
    ).toEqual(messageIds);
    expect(page.nextCursor).toBeUndefined();
  });
}

/** The public challenge must not allocate durable owner state. */
function expectUnauthenticated(
  endpoint: URL,
  sql: SqlClient.SqlClient,
  ownersAtStart: number,
) {
  return Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient;
    const refused = yield* client.post(endpoint.href);
    expect(refused.status).toBe(401);
    expect(yield* sql`SELECT * FROM owner_bindings`).toHaveLength(
      ownersAtStart,
    );
  }).pipe(Effect.provide(NodeHttpClient.layer));
}

/** One unusable signing record cannot close the listener or another owner's retained context. */
function expectFailedAgentIsolation(input: {
  readonly sql: SqlClient.SqlClient;
  readonly fixture: DaemonProcessFixture;
  readonly running: RunningProcess;
  readonly aliceOwner: Owner;
  readonly aliceToken: string;
  readonly bobToken: string;
  readonly expectedStatus: unknown;
}) {
  const { sql, fixture, running, aliceToken, bobToken, expectedStatus } = input;
  return Effect.gen(function* () {
    const [alice] = yield* sql<
      Record<"agent_key", string>
    >`SELECT agent_key FROM owner_bindings WHERE issuer = ${input.aliceOwner.issuer} AND subject = ${input.aliceOwner.subject}`;
    if (alice === undefined) {
      return yield* Effect.dieMessage("Expected Alice's owner binding");
    }
    yield* withAgentTransaction(
      alice.agent_key,
      sql`UPDATE agent_profiles SET private_key = 'unreadable test ciphertext' WHERE agent_key = ${alice.agent_key}`,
    ).pipe(Effect.provideService(SqlClient.SqlClient, sql));
    yield* stopProcess(running);
    const restarted = yield* acquireDaemonProcess(fixture);
    const bob = yield* connect(fixture.endpoint, bobToken);
    expect(yield* call(bob, { name: "status", arguments: {} })).toEqual(
      expectedStatus,
    );
    const client = yield* HttpClient.HttpClient;
    const response = yield* client.post(fixture.endpoint.href, {
      headers: { authorization: `Bearer ${aliceToken}` },
    });
    expect(response.status).toBe(503);
    expect(restarted.child.exitCode).toBeNull();
  }).pipe(Effect.provide(NodeHttpClient.layer));
}

function removeInitialCopies(fixture: DaemonProcessFixture) {
  return Effect.gen(function* () {
    const filesystem = yield* FileSystem.FileSystem;
    yield* filesystem.remove(fixture.admissionCredentialFile);
  });
}

/** Reads retained registration inputs through the same tenant transaction as the daemon. */
function ownerProfile(
  sql: SqlClient.SqlClient,
  issuer: string,
  subject: string,
) {
  return Effect.gen(function* () {
    const [binding] = yield* sql<{
      readonly agentKey: string;
    }>`SELECT agent_key AS "agentKey" FROM owner_bindings WHERE issuer=${issuer} AND subject=${subject}`;
    if (binding === undefined) {
      return undefined;
    }
    const [profile] = yield* withAgentTransaction(
      binding.agentKey,
      sql<{
        readonly operation: string;
        readonly privateKey: string;
      }>`SELECT registration_operation_id AS operation,private_key AS "privateKey" FROM agent_profiles WHERE agent_key=${binding.agentKey}`,
    );
    return profile === undefined
      ? undefined
      : { agentKey: binding.agentKey, ...profile };
  }).pipe(Effect.provideService(SqlClient.SqlClient, sql));
}

/** A disconnected first caller leaves one durable identity and does not stall another owner. */
function cancelFirstAcquisition(input: {
  readonly infrastructure: ProcessInfrastructure;
  readonly endpoint: URL;
  readonly sql: SqlClient.SqlClient;
  readonly aliceOwner: Owner;
  readonly aliceToken: string;
  readonly bob: Client;
  readonly bobStatus: unknown;
}) {
  return Effect.gen(function* () {
    const resume = yield* Effect.acquireRelease(
      input.infrastructure.holdRegistry,
      (release) => Effect.sync(release),
    );
    const body = yield* HttpBody.json({
      jsonrpc: "2.0",
      id: "first-connection",
      method: "tools/list",
      params: {},
    });
    const client = yield* HttpClient.HttpClient;
    const first = yield* client
      .post(input.endpoint.href, {
        headers: {
          authorization: `Bearer ${input.aliceToken}`,
          accept: "application/json, text/event-stream",
          "mcp-protocol-version": "2025-11-25",
        },
        body,
      })
      .pipe(Effect.either, Effect.forkScoped);
    const before = yield* Effect.iterate(
      yield* ownerProfile(
        input.sql,
        input.aliceOwner.issuer,
        input.aliceOwner.subject,
      ),
      {
        while: (profile) => profile === undefined,
        body: () =>
          Effect.sleep("10 millis").pipe(
            Effect.andThen(
              ownerProfile(
                input.sql,
                input.aliceOwner.issuer,
                input.aliceOwner.subject,
              ),
            ),
          ),
      },
    ).pipe(Effect.timeout("10 seconds"));
    expect(
      Exit.isInterrupted(
        yield* Fiber.interrupt(first).pipe(Effect.timeout("2 seconds")),
      ),
    ).toBe(true);
    expect(yield* call(input.bob, { name: "status", arguments: {} })).toEqual(
      input.bobStatus,
    );
    yield* Effect.sync(resume);
    const alice = yield* connect(input.endpoint, input.aliceToken);
    const status = yield* call(alice, { name: "status", arguments: {} });
    expect(
      yield* ownerProfile(
        input.sql,
        input.aliceOwner.issuer,
        input.aliceOwner.subject,
      ),
    ).toEqual(before);
    return { alice, status };
  }).pipe(Effect.provide(NodeHttpClient.layer));
}

/** A read removes the items it returns from the pending inbox. */
function expectEmptyInbox(client: Client) {
  return Effect.gen(function* () {
    expect(
      (yield* call(client, { name: "receive_messages", arguments: {} })).items,
    ).toHaveLength(0);
  });
}

function awaitInbox(client: Client) {
  return Effect.gen(function* () {
    return yield* Effect.iterate(
      yield* call(client, { name: "receive_messages", arguments: {} }),
      {
        while: (page) => page.items.length === 0,
        body: () =>
          Effect.sleep("50 millis").pipe(
            Effect.andThen(
              call(client, { name: "receive_messages", arguments: {} }),
            ),
          ),
      },
    );
  }).pipe(Effect.timeout("120 seconds"));
}

/** The production inbox store observes unread work without consuming it through MCP. */
function awaitUnreadDelivery(
  sql: SqlClient.SqlClient,
  issuer: string,
  subject: string,
  messageId: string,
) {
  return Effect.gen(function* () {
    const [owner] = yield* sql<{
      // eslint-disable-next-line @typescript-eslint/naming-convention -- SQL owns the stored column name.
      readonly agent_key: string;
    }>`SELECT agent_key FROM owner_bindings WHERE issuer=${issuer} AND subject=${subject}`;
    if (owner === undefined) {
      return yield* Effect.dieMessage("Expected the delivery owner's binding");
    }
    const inbox = yield* makeInbox(owner.agent_key).pipe(
      Effect.provideService(SqlClient.SqlClient, sql),
    );
    const page = yield* Effect.iterate(yield* inbox.listInbox(), {
      while: (result) => result.items.length === 0,
      body: () =>
        Effect.sleep("50 millis").pipe(Effect.andThen(inbox.listInbox())),
    });
    expect(page.items).toHaveLength(1);
    const entry = page.items[0];
    if (entry === undefined) {
      return yield* Effect.dieMessage(
        "Expected unread delivery before restart",
      );
    }
    expect(entry.item).toMatchObject({ message: { msgId: messageId } });
    return entry;
  }).pipe(Effect.timeout("120 seconds"));
}

/** Opens the same configured database as the separate serving process. */
function openRuntimeDatabase(fixture: DaemonProcessFixture) {
  return Effect.gen(function* () {
    const access = yield* databaseAccess.pipe(
      Effect.withConfigProvider(
        ConfigProvider.fromMap(new Map(Object.entries(fixture.environment))),
      ),
    );
    return Context.get(
      yield* Layer.build(databaseConnection(access.locator)),
      SqlClient.SqlClient,
    );
  });
}

/** Host history publishes incoming messages only after their asynchronous firewall checks finish. */
function awaitConversationMessages(
  client: Client,
  conversation: Conversation,
  count: number,
) {
  const read = conversation
    .read(client)
    .pipe(
      Effect.map((page) =>
        page.records.flatMap((record) =>
          record.kind === "result" ? [] : [record.message.msgId],
        ),
      ),
    );
  const initial: readonly string[] = [];
  return Effect.iterate(initial, {
    while: (ids) => ids.length < count,
    body: () => read.pipe(Effect.tap(() => Effect.sleep("20 millis"))),
  }).pipe(Effect.timeout("20 seconds"));
}

/** How an agent reads one of its conversations, and the address it is listed under. */
interface Conversation {
  readonly address: Option.Option<AgentAddress>;
  readonly read: (
    client: Client,
  ) => Effect.Effect<
    typeof toolDefinitions.read_conversation.output.Type,
    unknown
  >;
}

/** A conversation an agent names by its address. */
function conversationWith(address: AgentAddress): Conversation {
  return {
    address: Option.some(address),
    read: (client) =>
      call(client, { name: "read_conversation", arguments: { address } }),
  };
}

/** A client's conversation with the server agent, the only one it has and so not named. */
const clientConversation: Conversation = {
  address: Option.none(),
  read: (client) =>
    callAsClient(
      client,
      "read_conversation",
      {},
      toolDefinitions.read_conversation.output,
    ),
};

/**
 * Calls a tool the way a client in client/server mode does. Its send_message
 * and read_conversation name no recipient and no conversation.
 */
function callAsClient<A, I>(
  client: Client,
  name: string,
  input: Readonly<Record<string, unknown>>,
  output: Schema.Schema<A, I>,
) {
  return Effect.tryPromise(() =>
    client.callTool({ name, arguments: { ...input } }),
  ).pipe(
    Effect.filterOrFail((result) => result.isError !== true),
    Effect.flatMap((result) =>
      Schema.decodeUnknown(output)(result.structuredContent),
    ),
  );
}

/** Enrollment remains independent of transport, including bootstrap server selection. */
const openSharedDaemon = (
  central: boolean,
  database?: Effect.Effect.Success<typeof acquirePostgres>,
) =>
  Effect.gen(function* () {
    const infrastructure = yield* acquireProcessInfrastructure;
    if (central) {
      yield* Effect.acquireRelease(infrastructure.holdRouter, (release) =>
        Effect.sync(release),
      );
    }
    const issuer = yield* issuerFixture;
    const port = yield* reservePort;
    const resource = new URL(`https://127.0.0.1:${port}/mcp`);
    const fixture = yield* makeDaemonProcessFixture(
      infrastructure,
      "shared-runtime",
      undefined,
      {
        database,
        settings: {
          port,
          ...(central
            ? {
                transport: {
                  kind: "client-server",
                  serverAgentName: SERVER_AGENT_NAME,
                },
              }
            : {}),
          authentication: {
            kind: "oauth",
            resource: resource.href,
            issuer: issuer.issuer,
            scope: requiredScope,
          },
          agentName: undefined,
        },
      },
    );
    const sql = yield* openRuntimeDatabase(fixture);
    expect(yield* sql`SELECT * FROM owner_bindings`).toHaveLength(0);
    yield* removeInitialCopies(fixture);
    const daemon = yield* acquireDaemonProcess(fixture);
    yield* expectUnauthenticated(fixture.endpoint, sql, central ? 1 : 0);
    const aliceOwner = central
      ? SERVER_AGENT_OWNER
      : { issuer: issuer.issuer, subject: "alice" };
    const aliceToken = central
      ? fixture.serverAgentCredential
      : yield* issuer.token("alice", resource.href);
    const bobToken = yield* issuer.token("bob", resource.href);
    const bob = yield* connect(fixture.endpoint, bobToken);
    const b = yield* call(bob, { name: "status", arguments: {} });
    const { alice, status: a } = yield* cancelFirstAcquisition({
      infrastructure,
      endpoint: fixture.endpoint,
      sql,
      aliceOwner,
      aliceToken,
      bob,
      bobStatus: b,
    });
    if (a.kind !== "registered" || b.kind !== "registered") {
      return yield* Effect.dieMessage("Expected registered OAuth agents");
    }
    const [aliceCard, bobCard] = yield* Effect.all([
      Schema.decodeUnknown(AgentIdentityCard)(a.identityCard),
      Schema.decodeUnknown(AgentIdentityCard)(b.identityCard),
    ]);
    const bobAddress = AgentAddress.make(`agent:${bobCard.agentName}`);
    const aliceAddress = AgentAddress.make(`agent:${aliceCard.agentName}`);
    return {
      infrastructure,
      issuer,
      resource,
      fixture,
      sql,
      daemon,
      aliceOwner,
      aliceToken,
      bobToken,
      alice,
      bob,
      a,
      b,
      aliceCard,
      bobCard,
      bobAddress,
      aliceAddress,
    };
  });

function expectCentralExchange(
  shared: Effect.Effect.Success<ReturnType<typeof openSharedDaemon>>,
  recovered: Client,
  initialIds: readonly string[],
) {
  const { fixture, aliceToken, issuer, resource, bobAddress } = shared;
  return Effect.gen(function* () {
    const sender = yield* connect(fixture.endpoint, aliceToken);
    const concurrent = yield* Effect.all(
      [0, 1, 2, 3, 4, 5].map((index) =>
        index % 2 === 0
          ? call(sender, {
              name: "send_message",
              arguments: { to: bobAddress, text: `concurrent ${index}` },
            })
          : callAsClient(
              recovered,
              "send_message",
              { text: `concurrent ${index}` },
              toolDefinitions.send_message.output,
            ),
      ),
      { concurrency: 6 },
    );
    const expectedCount = concurrent.length + initialIds.length;
    const serverIds = yield* awaitConversationMessages(
      sender,
      conversationWith(bobAddress),
      expectedCount,
    );
    const clientIds = yield* awaitConversationMessages(
      recovered,
      clientConversation,
      expectedCount,
    );
    expect(serverIds).toEqual(clientIds);
    expect(new Set(serverIds)).toEqual(
      new Set([...initialIds, ...concurrent.map((message) => message.id)]),
    );
    const thirdToken = yield* issuer.token("carol", resource.href);
    const third = yield* connect(fixture.endpoint, thirdToken);
    yield* call(third, { name: "status", arguments: {} });
    expect(
      yield* call(third, {
        name: "send_message",
        arguments: { to: bobAddress, text: "client-to-client refused" },
      }).pipe(Effect.isFailure),
    ).toBe(true);
  });
}

/** The observation bound allows a remote PostgreSQL tunnel's round trips; it is not a delivery latency assertion. */
const scenario = (
  database?: Effect.Effect.Success<typeof acquirePostgres>,
  central = false,
) =>
  Effect.gen(function* () {
    const shared = yield* database === undefined
      ? openSharedDaemon(central)
      : openSharedDaemon(central, database);
    const {
      fixture,
      sql,
      daemon,
      issuer,
      aliceOwner,
      aliceToken,
      bobToken,
      alice,
      bob,
      a,
      b,
      aliceCard,
      bobCard,
      bobAddress,
      aliceAddress,
    } = shared;
    expect(aliceCard.agentId).not.toBe(bobCard.agentId);
    const bobReads = central
      ? clientConversation
      : conversationWith(aliceAddress);
    const again = yield* connect(fixture.endpoint, aliceToken);
    expect(yield* call(again, { name: "status", arguments: {} })).toEqual(a);
    const sent = yield* call(alice, {
      name: "send_message",
      arguments: {
        to: bobAddress,
        text: "shared runtime delivery",
      },
    });
    const pending = yield* awaitInbox(bob);
    const entry = pending.items[0];
    if (entry === undefined) {
      return yield* Effect.dieMessage("Expected Bob's delivery");
    }
    expect(
      (yield* call(alice, { name: "receive_messages", arguments: {} })).items,
    ).toHaveLength(0);
    yield* expectEmptyInbox(bob);
    yield* expectConversation(alice, conversationWith(bobAddress), "sent", [
      sent.id,
    ]);
    yield* expectConversation(bob, bobReads, "received", [sent.id]);
    const restartMessage = yield* call(alice, {
      name: "send_message",
      arguments: {
        to: bobAddress,
        text: "unread delivery across restart",
      },
    });
    const unread = yield* awaitUnreadDelivery(
      sql,
      issuer.issuer,
      "bob",
      restartMessage.id,
    );
    yield* stopProcess(daemon);
    const restoredDaemon = yield* acquireDaemonProcess(fixture);
    const recovered = yield* connect(fixture.endpoint, bobToken);
    expect(yield* call(recovered, { name: "status", arguments: {} })).toEqual(
      b,
    );
    yield* expectConversation(recovered, bobReads, "received", [
      sent.id,
      restartMessage.id,
    ]);
    const restored = yield* awaitInbox(recovered);
    expect(restored.items).toEqual([unread]);
    yield* expectEmptyInbox(recovered);
    expect(yield* sql`SELECT * FROM owner_bindings`).toHaveLength(2);
    if (central) {
      yield* expectCentralExchange(shared, recovered, [
        sent.id,
        restartMessage.id,
      ]);
    }
    yield* expectFailedAgentIsolation({
      sql,
      fixture,
      running: restoredDaemon,
      aliceOwner,
      aliceToken,
      bobToken,
      expectedStatus: b,
    });
  });

it(
  "authenticates before provisioning and recovers two isolated agents through one endpoint",
  processTrace(Effect.scoped(scenario())),
  180_000,
);

it(
  "recovers signed peer delivery through one PostgreSQL-backed MCP daemon with a restricted login",
  processTrace(Effect.scoped(acquirePostgres.pipe(Effect.flatMap(scenario)))),
  240_000,
);

it(
  "delivers and recovers through one central MCP daemon while Router is unavailable",
  processTrace(Effect.scoped(scenario(undefined, true))),
  180_000,
);

it(
  "delivers and recovers centrally with PostgreSQL row isolation and no Router",
  processTrace(
    Effect.scoped(
      acquirePostgres.pipe(
        Effect.flatMap((database) => scenario(database, true)),
      ),
    ),
  ),
  240_000,
);

/* eslint-enable agent-code-guard/no-hardcoded-assertion-literals -- Restore ordinary assertion rules after the conformance trace. */
