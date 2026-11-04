/** @file Real OAuth accounts share one daemon while identity, inbox and recovery remain isolated. */
import {
  FileSystem,
  HttpClient,
  HttpServerRequest,
  HttpServerResponse,
} from "@effect/platform";
import { NodeHttpClient, NodeHttpServer } from "@effect/platform-node";
import { SqlClient } from "@effect/sql";
import {
  Client,
  StreamableHTTPClientTransport,
} from "@modelcontextprotocol/client";
import {
  AgentIdentityCard,
  type InboxEntry,
  MessageAddressInput,
} from "@moltzap/social-harness";
import {
  databaseAccess,
  databaseConnection,
  makeInbox,
  withAgentTransaction,
} from "@moltzap/social-harness/test";
import { ConfigProvider, Context, Effect, Layer, Schema } from "effect";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- NodeHttpServer owns the issuer listener's lifetime.
import { createServer } from "node:http";
import { dirname, join } from "node:path";
import { expect, it } from "vitest";
import { callTool as call } from "../../../plugins/shared/mcp-client.js";
import { processTrace } from "../scenario-fixture.js";
import {
  acquireDaemonProcess,
  acquireProcessInfrastructure,
  type DaemonProcessFixture,
  makeDaemonProcessFixture,
  reservePort,
  type RunningProcess,
  stopProcess,
} from "./daemon-fixture.js";
import { acquirePostgres } from "./postgres-fixture.js";

/* eslint-disable agent-code-guard/no-hardcoded-assertion-literals -- HTTP statuses and closed tool outcomes are external conformance contracts. */
const requiredScope = "use:mcp";

const issuerFixture = Effect.gen(function* () {
  const keys = yield* Effect.tryPromise(() => generateKeyPair("RS256"));
  const publicKey = yield* Effect.tryPromise(() => exportJWK(keys.publicKey));
  const server = yield* NodeHttpServer.make(createServer, {
    host: "127.0.0.1",
    port: 0,
  });
  if (server.address._tag !== "TcpAddress") {
    return yield* Effect.dieMessage("Expected issuer TCP listener");
  }
  const issuer = `http://127.0.0.1:${server.address.port}/`;
  yield* server.serve(
    Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest;
      return yield* HttpServerResponse.json(
        request.url === "/.well-known/openid-configuration"
          ? { issuer, jwks_uri: `${issuer}jwks` }
          : { keys: [{ ...publicKey, kid: "shared-test", alg: "RS256" }] },
      ).pipe(Effect.orDie);
    }),
  );
  const token = (subject: string, audience: string) =>
    Effect.tryPromise(() =>
      new SignJWT({ scope: requiredScope })
        .setProtectedHeader({ alg: "RS256", kid: "shared-test" })
        .setIssuer(issuer)
        .setAudience(audience)
        .setSubject(subject)
        .setExpirationTime("5m")
        .sign(keys.privateKey),
    );
  return { issuer, token };
});

function connect(endpoint: URL, token: string) {
  return Effect.acquireRelease(
    Effect.gen(function* () {
      const client = new Client(
        { name: "shared-daemon-evaluation", version: "1" },
        {
          capabilities: {},
          versionNegotiation: { mode: { pin: "2026-07-28" } },
        },
      );
      yield* Effect.tryPromise(() =>
        client.connect(
          new StreamableHTTPClientTransport(endpoint, {
            requestInit: {
              headers: {
                authorization: `Bearer ${token}`,
              },
            },
          }),
        ),
      );
      return client;
    }),
    (client) => Effect.tryPromise(() => client.close()).pipe(Effect.ignore),
  );
}

/** The public challenge must not allocate durable owner state. */
function expectUnauthenticated(endpoint: URL, sql: SqlClient.SqlClient) {
  return Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient;
    const refused = yield* client.post(endpoint.href);
    expect(refused.status).toBe(401);
    expect(yield* sql`SELECT * FROM owner_bindings`).toHaveLength(0);
  }).pipe(Effect.provide(NodeHttpClient.layer));
}

/** One unusable signing record cannot close the listener or another owner's retained context. */
function expectFailedAgentIsolation(input: {
  readonly sql: SqlClient.SqlClient;
  readonly fixture: DaemonProcessFixture;
  readonly running: RunningProcess;
  readonly aliceToken: string;
  readonly bobToken: string;
  readonly expectedStatus: unknown;
}) {
  const { sql, fixture, running, aliceToken, bobToken, expectedStatus } = input;
  return Effect.gen(function* () {
    const [alice] = yield* sql<
      Record<"agent_key", string>
    >`SELECT agent_key FROM owner_bindings WHERE subject = 'alice'`;
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

function removeSetupFiles(fixture: DaemonProcessFixture) {
  return Effect.gen(function* () {
    const filesystem = yield* FileSystem.FileSystem;
    yield* filesystem.remove(
      join(dirname(fixture.agentPrivateKeyFile), "setup.json"),
    );
    yield* filesystem.remove(fixture.agentPrivateKeyFile);
    yield* filesystem.remove(fixture.admissionCredentialFile);
  });
}

/** Consuming the inbox preserves its saved event for the authenticated owner. */
function expectDeliveredEvent(client: Client, entry: InboxEntry) {
  return Effect.gen(function* () {
    expect(
      (yield* call(client, { name: "read_inbox", arguments: {} })).items,
    ).toHaveLength(0);
    const { deliveryToken, ...event } = entry;
    expect(
      yield* call(client, {
        name: "read_event",
        arguments: { eventId: deliveryToken },
      }),
    ).toEqual(event);
  });
}

function awaitInbox(client: Client) {
  return Effect.gen(function* () {
    return yield* Effect.iterate(
      yield* call(client, { name: "read_inbox", arguments: {} }),
      {
        while: (page) => page.items.length === 0,
        body: () =>
          Effect.sleep("50 millis").pipe(
            Effect.andThen(call(client, { name: "read_inbox", arguments: {} })),
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

/** The observation bound allows a remote PostgreSQL tunnel's round trips; it is not a delivery latency assertion. */
const scenario = (database?: Effect.Effect.Success<typeof acquirePostgres>) =>
  Effect.gen(function* () {
    const infrastructure = yield* acquireProcessInfrastructure;
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
          authentication: {
            kind: "oauth",
            resource: resource.href,
            issuer: issuer.issuer,
            scope: requiredScope,
          },
          agent: undefined,
        },
      },
    );
    const access = yield* databaseAccess.pipe(
      Effect.withConfigProvider(
        ConfigProvider.fromMap(new Map(Object.entries(fixture.environment))),
      ),
    );
    const sql = Context.get(
      yield* Layer.build(databaseConnection(access.locator)),
      SqlClient.SqlClient,
    );
    expect(yield* sql`SELECT * FROM owner_bindings`).toHaveLength(0);
    yield* removeSetupFiles(fixture);
    const daemon = yield* acquireDaemonProcess(fixture);
    yield* expectUnauthenticated(fixture.endpoint, sql);
    const aliceToken = yield* issuer.token("alice", resource.href);
    const bobToken = yield* issuer.token("bob", resource.href);
    const alice = yield* connect(fixture.endpoint, aliceToken);
    const bob = yield* connect(fixture.endpoint, bobToken);
    const a = yield* call(alice, { name: "status", arguments: {} });
    const b = yield* call(bob, { name: "status", arguments: {} });
    if (a.kind !== "registered" || b.kind !== "registered") {
      return yield* Effect.dieMessage(
        "OAuth accounts must register automatically",
      );
    }
    const aliceCard = yield* Schema.decodeUnknown(AgentIdentityCard)(
      a.identityCard,
    );
    const bobCard = yield* Schema.decodeUnknown(AgentIdentityCard)(
      b.identityCard,
    );
    const bobAddress = Schema.decodeUnknownSync(MessageAddressInput)(
      `agent:${bobCard.agentName}`,
    );
    expect(aliceCard.agentId).not.toBe(bobCard.agentId);
    const again = yield* connect(fixture.endpoint, aliceToken);
    expect(yield* call(again, { name: "status", arguments: {} })).toEqual(a);
    yield* call(alice, {
      name: "send_message",
      arguments: {
        input: {
          to: bobAddress,
          text: "shared runtime delivery",
        },
      },
    });
    const pending = yield* awaitInbox(bob);
    const entry = pending.items[0];
    if (entry === undefined) {
      return yield* Effect.dieMessage("Expected Bob's delivery");
    }
    expect(
      (yield* call(alice, { name: "read_inbox", arguments: {} })).items,
    ).toHaveLength(0);
    expect(
      yield* call(alice, {
        name: "read_event",
        arguments: { eventId: entry.deliveryToken },
      }).pipe(Effect.isFailure),
    ).toBe(true);
    yield* expectDeliveredEvent(bob, entry);
    const restartMessage = yield* call(alice, {
      name: "send_message",
      arguments: {
        input: {
          to: bobAddress,
          text: "unread delivery across restart",
        },
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
    const restored = yield* awaitInbox(recovered);
    expect(restored.items).toEqual([unread]);
    yield* expectDeliveredEvent(recovered, unread);
    yield* expectDeliveredEvent(recovered, entry);
    expect(yield* sql`SELECT * FROM owner_bindings`).toHaveLength(2);
    yield* expectFailedAgentIsolation({
      sql,
      fixture,
      running: restoredDaemon,
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

/* eslint-enable agent-code-guard/no-hardcoded-assertion-literals -- Restore ordinary assertion rules after the conformance trace. */
