/** @file Qualifies shared-agent persistence on SQLite and restricted-role PostgreSQL. */
import { NodeHttpClient } from "@effect/platform-node";
import { SqlClient } from "@effect/sql";
import { scopedLive as it } from "@effect/vitest";
import {
  AgentCard,
  AgentSigningAuthority,
  InboxEvent,
  Message,
  Request,
  RequestedSchema,
} from "@moltzap/social-harness";
import {
  Firewall,
  type FirewallOptions,
  Identity,
  Registry,
} from "@moltzap/social-harness/service";
import {
  acceptMessage,
  databaseConnection,
  DecisionProviderError,
  encodeRuntimeValue,
  failMessage,
  getOrCreateOwnedAgent,
  initializeCommunication,
  loadAgentRegistration,
  makeInbox,
  migrateDaemon,
  putCollectiveOperation,
  readAcceptedMessage,
  readCollectiveOperation,
  readCollectiveOperationIds,
  readUnpreparedMessages,
  retireCollectiveOperation,
  transaction,
  withAgentTransaction,
} from "@moltzap/social-harness/test";
import { Duration, Effect, Exit, Layer, Redacted, Schema } from "effect";
import { generateKeyPairSync, randomBytes, randomUUID } from "node:crypto";
import { expect } from "vitest";

import { acquirePostgres } from "./postgres-fixture.js";

const testAgentCard = (name: string) =>
  Schema.decodeUnknownSync(AgentCard)({
    agentId: `agt_${randomBytes(16).toString("base64url")}`,
    principalId: `prn_${randomBytes(16).toString("base64url")}`,
    agentName: name,
  });

const unlockKey = Redacted.make(new Uint8Array(32).fill(3));

const qualifyTenants = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const owner = { issuer: "https://issuer.example", subject: "first" };
  const agentKeys = yield* Effect.all(
    Array.from({ length: 8 }, () => getOrCreateOwnedAgent(owner, unlockKey)),
    { concurrency: 8 },
  );
  const first = yield* getOrCreateOwnedAgent(owner, unlockKey);
  for (const agentKey of agentKeys) {
    expect(agentKey).toBe(first);
  }
  const second = yield* getOrCreateOwnedAgent(
    { ...owner, subject: "second" },
    unlockKey,
  );
  const role = yield* sql<{
    readonly rolsuper: boolean;
    readonly rolbypassrls: boolean;
  }>`
    SELECT rolsuper, rolbypassrls FROM pg_roles WHERE rolname = current_user`;
  expect(role[0]?.rolsuper).toBeFalsy();
  expect(role[0]?.rolbypassrls).toBeFalsy();
  const keys = [first, second];
  yield* Effect.all(
    Array.from({ length: 12 })
      .flatMap(() => keys)
      .map((key) => {
        if (key === undefined) {
          return Effect.dieMessage("Expected a qualification agent");
        }
        return withAgentTransaction(
          key,
          sql<
            Record<"agent_key", string>
          >`SELECT agent_key FROM agent_profiles`,
        ).pipe(
          Effect.map((rows) => {
            expect(rows.map((row) => row.agent_key)).toEqual([key]);
            return rows;
          }),
        );
      }),
    { concurrency: 8 },
  );
  expect(yield* sql`SELECT * FROM agent_profiles`).toHaveLength(0);
  const before = yield* loadAgentRegistration(first, unlockKey);
  const interrupted = yield* withAgentTransaction(
    first,
    sql`UPDATE agent_profiles SET agent_name='rolled-back' WHERE agent_key=${first}`.pipe(
      Effect.zipRight(Effect.fail("rollback")),
    ),
  ).pipe(Effect.exit);
  expect(Exit.isFailure(interrupted)).toBeTruthy();
  expect(yield* sql`SELECT * FROM agent_profiles`).toHaveLength(0);
  expect(yield* getOrCreateOwnedAgent(owner, unlockKey)).toEqual(first);
  expect((yield* loadAgentRegistration(first, unlockKey)).registration).toEqual(
    before.registration,
  );
  return keys;
});

const qualifyInbox = (keys: readonly string[]) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const message = yield* Message.create({
      author: testAgentCard("alice"),
      recipients: [testAgentCard("bob"), testAgentCard("carol")],
      content: [{ type: "text", text: "Postgres inbox qualification" }],
    });
    const address = yield* Schema.decodeUnknown(InboxEvent.fields.address)(
      "agent:alice",
    );
    for (const key of keys) {
      const inbox = yield* makeInbox(key);
      yield* inbox.queueIncoming(address, { kind: "message", message });
      const pending = (yield* inbox.pending(10))[0];
      if (pending === undefined) {
        return yield* Effect.dieMessage(
          "Expected independent pending work for each agent",
        );
      }
      yield* inbox.publish(pending, {
        kind: "allowed",
        item: { kind: "multicast", message },
        save: Effect.void,
        visibility: [],
      });
      expect(
        (yield* inbox.listInbox()).items.map((item) => item.deliveryToken),
      ).toEqual([pending.token]);
      const reopened = yield* makeInbox(key);
      expect(yield* reopened.getInboxEvent(pending.token)).toEqual(
        yield* inbox.getInboxEvent(pending.token),
      );
    }
    expect(yield* sql`SELECT * FROM harness_items`).toHaveLength(0);
    return keys;
  });

const qualifyProtocol = (keys: readonly string[]) =>
  Effect.gen(function* () {
    const author = testAgentCard("protocol-author");
    const recipient = testAgentCard("protocol-recipient");
    const message = yield* Message.create({
      author,
      recipients: [recipient],
      content: [{ type: "text", text: "Shared-store acceptance" }],
    });
    const canonicalMessage = yield* encodeRuntimeValue(
      yield* Schema.encode(Message)(message),
    );
    const reference = { authorAgentId: author.agentId, msgId: message.msgId };
    const request = yield* Request.create({
      author,
      recipients: [recipient],
      op: "gather",
      question: "Acknowledge receipt",
      deadline: 60,
      requestedSchema: yield* Schema.decodeUnknown(RequestedSchema)({
        type: "object",
        properties: { received: { type: "boolean" } },
        required: ["received"],
      }),
    });
    for (const key of keys) {
      yield* initializeCommunication(key, author);
      yield* acceptMessage(key, {
        authorAgentId: author.agentId,
        msgId: message.msgId,
        conversationId: "qualification",
        canonicalMessage,
      });
      expect((yield* readAcceptedMessage(key, reference))?.message).toEqual(
        message,
      );
      expect(
        (yield* readUnpreparedMessages(key)).map((item) => item.message),
      ).toEqual([message]);
      yield* Effect.all(
        Array.from({ length: 8 }, () =>
          putCollectiveOperation(key, { origin: "local", request }),
        ),
        { concurrency: 8 },
      );
      expect(
        (yield* readCollectiveOperation(key, request.operationId))?.deadlineAt,
      ).toBe(request.deadlineAt);
    }
    const first = keys[0];
    const second = keys[1];
    if (first === undefined || second === undefined) {
      return yield* Effect.dieMessage("Expected two isolated protocol owners");
    }
    const failure = "membership-invalid";
    yield* failMessage(first, reference, failure);
    const before = yield* readCollectiveOperation(first, request.operationId);
    const phases = yield* Effect.all(
      Array.from({ length: 2 }, () =>
        transaction(
          first,
          Effect.gen(function* () {
            const state = yield* readCollectiveOperation(
              first,
              request.operationId,
            );
            yield* Effect.sleep("10 millis");
            yield* retireCollectiveOperation(first, request.operationId);
            return state?.phase;
          }),
        ),
      ),
      { concurrency: 2 },
    );
    expect(phases.filter((phase) => phase === before?.phase)).toHaveLength(1);
    expect(new Set(phases).size).toBe(2);
    expect((yield* readAcceptedMessage(first, reference))?.failure).toBe(
      failure,
    );
    expect(
      (yield* readAcceptedMessage(second, reference))?.failure,
    ).toBeUndefined();
    expect(yield* readUnpreparedMessages(first)).toHaveLength(0);
    expect(yield* readUnpreparedMessages(second)).toHaveLength(1);
    expect(
      yield* readCollectiveOperationIds(first, "local", "completed"),
    ).toEqual([request.operationId]);
    expect(
      yield* readCollectiveOperationIds(second, "local", "collecting"),
    ).toEqual([request.operationId]);
    const sql = yield* SqlClient.SqlClient;
    yield* sql.onDialectOrElse({
      pg: () =>
        Effect.gen(function* () {
          expect(yield* sql`SELECT * FROM message_intents`).toHaveLength(0);
          expect(yield* sql`SELECT * FROM collective_operations`).toHaveLength(
            0,
          );
        }),
      orElse: () => Effect.void,
    });
    return keys;
  });

const qualifyFirewall = (keys: readonly string[]) =>
  Effect.gen(function* () {
    const authority = yield* AgentSigningAuthority.fromPkcs8(
      Redacted.make(
        generateKeyPairSync("ed25519").privateKey.export({
          format: "pem",
          type: "pkcs8",
        }),
      ),
    );
    const signer = AgentSigningAuthority.publicKey(authority);
    const registry = Registry.layer({
      origin: new URL("https://registry.example"),
      registrySignerPublicKey: signer,
      requestTimeout: Duration.seconds(5),
    }).pipe(Layer.provide(NodeHttpClient.layer));
    const options: FirewallOptions = {
      mode: "enforcement",
      facts: [],
      settings: {
        timeoutMilliseconds: 5000,
        maximumInputBytes: 100000,
        maximumResultBytes: 100000,
      },
      text: {
        initialRules: [],
        provider: {
          decide: () =>
            Effect.fail(new DecisionProviderError({ reason: "transport" })),
        },
        settings: {
          minimumProbability: 0.5,
          maximumRequestBytes: 60000,
          timeoutMilliseconds: 5000,
        },
      },
    };
    const rules = keys.map(
      (key) =>
        ({
          id: key,
          applies: "both",
          text: `Remember policy ${key}.`,
        }) satisfies {
          readonly id: string;
          readonly applies: "both";
          readonly text: string;
        },
    );
    for (const [index, key] of keys.entries()) {
      const rule = rules[index];
      if (rule === undefined) {
        return yield* Effect.dieMessage("Expected an agent policy");
      }
      const identity = Identity.layer({
        agentKey: key,
        signingAuthority: authority,
        registrySignerPublicKey: signer,
        admissionCredential: Effect.succeed(Redacted.make("qualification")),
      }).pipe(Layer.provide(registry));
      const configured = Firewall.layer(key, options).pipe(
        Layer.provide(identity),
      );
      const saved = yield* Effect.gen(function* () {
        const firewall = yield* Firewall;
        expect((yield* firewall.readPolicy()).rules).toEqual([]);
        yield* firewall.updatePolicy({ add: [rule] });
        return yield* firewall.readDecisions({});
      }).pipe(Effect.provide(configured));
      expect(saved.activations).toHaveLength(1);
      expect(saved.activations[0]?.time).toBeGreaterThan(0);
      yield* Effect.gen(function* () {
        const firewall = yield* Firewall;
        expect((yield* firewall.readPolicy()).rules).toEqual([rule]);
        expect(yield* firewall.readDecisions({})).toEqual(saved);
      }).pipe(
        Effect.provide(
          Firewall.layer(key, options).pipe(Layer.provide(identity)),
        ),
      );
    }
    const sql = yield* SqlClient.SqlClient;
    expect(yield* sql`SELECT * FROM firewall_policy`).toHaveLength(0);
    expect(yield* sql`SELECT * FROM firewall_activations`).toHaveLength(0);
    return keys;
  });

it(
  "isolates concurrent owners and clears transaction context on a pooled non-owner login",
  () =>
    Effect.gen(function* () {
      const database = yield* acquirePostgres;
      const keys = yield* qualifyTenants.pipe(
        Effect.flatMap(qualifyInbox),
        Effect.flatMap(qualifyProtocol),
        Effect.flatMap(qualifyFirewall),
        Effect.provide(databaseConnection(database.runtimeLocator)),
      );
      const retained = yield* database.admin<
        Record<"agent_key", string>
      >`SELECT agent_key FROM agent_profiles`;
      expect(new Set(retained.map((row) => row.agent_key))).toEqual(
        new Set(keys),
      );
    }).pipe(Effect.scoped),
  { timeout: 120_000 },
);

it("isolates accepted messages and requests between SQLite agents", () =>
  Effect.gen(function* () {
    yield* migrateDaemon;
    yield* qualifyProtocol([randomUUID(), randomUUID()]);
  }).pipe(
    Effect.provide(databaseConnection(Redacted.make("sqlite::memory:"))),
  ));
