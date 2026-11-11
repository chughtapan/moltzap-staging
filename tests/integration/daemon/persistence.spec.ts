/** @file Qualifies shared-agent persistence on restricted-role PostgreSQL. */
import { SqlClient } from "@effect/sql";
import { scopedLive as it } from "@effect/vitest";
import { AgentCard, InboxEvent, Message } from "@social-harness/social-harness";
import {
  Firewall,
  type FirewallOptions,
  Identity,
} from "@social-harness/social-harness/service";
import {
  databaseConnection,
  DecisionProviderError,
  getOrCreateOwnedAgent,
  makeInbox,
  withAgentTransaction,
} from "@social-harness/social-harness/test";
import { Context, Effect, Exit, Layer, Schema } from "effect";
import { randomBytes } from "node:crypto";
import { expect } from "vitest";

import { acquirePostgres } from "./postgres-fixture.js";

const testAgentCard = (name: string) =>
  Schema.decodeUnknownSync(AgentCard)({
    agentId: `agt_${randomBytes(16).toString("base64url")}`,
    principalId: `prn_${randomBytes(16).toString("base64url")}`,
    agentName: name,
  });

/** The card the daemon gives the agent stored under `agentKey`. */
const cardOf = (agentKey: string) =>
  Layer.build(Identity.layer(agentKey)).pipe(
    Effect.map((identity) => Context.get(identity, Identity).me),
    Effect.scoped,
  );

const qualifyTenants = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const owner = { issuer: "https://issuer.example", subject: "first" };
  const agentKeys = yield* Effect.all(
    Array.from({ length: 8 }, () => getOrCreateOwnedAgent(owner)),
    { concurrency: 8 },
  );
  const first = yield* getOrCreateOwnedAgent(owner);
  for (const agentKey of agentKeys) {
    expect(agentKey).toBe(first);
  }
  const second = yield* getOrCreateOwnedAgent({ ...owner, subject: "second" });
  const role = yield* sql<{
    readonly rolsuper: boolean;
    readonly rolbypassrls: boolean;
  }>`
    SELECT rolsuper, rolbypassrls FROM pg_roles WHERE rolname = current_user`;
  expect(role[0]?.rolsuper).toBeFalsy();
  expect(role[0]?.rolbypassrls).toBeFalsy();
  const before = yield* cardOf(first);
  const interrupted = yield* withAgentTransaction(
    first,
    sql`UPDATE agent_profiles SET agent_name='rolled-back' WHERE agent_key=${first}`.pipe(
      Effect.zipRight(Effect.fail("rollback")),
    ),
  ).pipe(Effect.exit);
  expect(Exit.isFailure(interrupted)).toBeTruthy();
  expect(yield* getOrCreateOwnedAgent(owner)).toEqual(first);
  expect(yield* cardOf(first)).toEqual(before);
  return [first, second];
});

/** A transaction of the agent stored under `key` reads that agent's inbox rows and no other's. */
const readsOnlyOwnItems = (key: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* withAgentTransaction(
      key,
      sql<Record<"agent_key", string>>`SELECT agent_key FROM harness_items`,
    );
    expect(rows.map((row) => row.agent_key)).toEqual([key]);
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
      expect((yield* reopened.listInbox()).items).toEqual(
        (yield* inbox.listInbox()).items,
      );
    }
    yield* Effect.all(
      Array.from({ length: 12 })
        .flatMap(() => keys)
        .map(readsOnlyOwnItems),
      { concurrency: 8 },
    );
    expect(yield* sql`SELECT * FROM harness_items`).toHaveLength(0);
    return keys;
  });

const qualifyFirewall = (keys: readonly string[]) =>
  Effect.gen(function* () {
    const options: FirewallOptions = {
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
      const identity = Identity.layer(key);
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
