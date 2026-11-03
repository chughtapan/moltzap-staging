/** @file Qualifies ownership transactions and RLS with a real non-owner PostgreSQL login. */
import { SqlClient } from "@effect/sql";
import { scopedLive as it } from "@effect/vitest";
import { Config, Context, Effect, Exit, Layer, Redacted, Schema } from "effect";
import { randomBytes } from "node:crypto";
import { expect } from "vitest";
import { testAgentCard } from "../__tests__/agents.js";
import { Message } from "../communication/service.js";
import { makeInbox, MessageAddress } from "../harness/inbox.js";
import { getOrCreateOwnedAgent } from "../harness/ownership.js";
import { migrateDaemon } from "./migrations.js";
import { databaseConnection } from "./sql.js";
import { withAgentTransaction } from "./transactions.js";

const unlockKey = Redacted.make(new Uint8Array(32).fill(3));
const runtimePassword = randomBytes(24).toString("hex");

const qualifyTenants = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const owner = { issuer: "https://issuer.example", subject: "first" };
  const profiles = yield* Effect.all(
    Array.from({ length: 8 }, () => getOrCreateOwnedAgent(owner, unlockKey)),
    { concurrency: 8 },
  );
  const first = yield* getOrCreateOwnedAgent(owner, unlockKey);
  for (const profile of profiles) {
    expect(profile).toEqual(first);
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
  const keys = [first.agent_key, second.agent_key];
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
  const interrupted = yield* withAgentTransaction(
    first.agent_key,
    sql`UPDATE agent_profiles SET agent_name='rolled-back' WHERE agent_key=${first.agent_key}`.pipe(
      Effect.zipRight(Effect.fail("rollback")),
    ),
  ).pipe(Effect.exit);
  expect(Exit.isFailure(interrupted)).toBeTruthy();
  expect(yield* sql`SELECT * FROM agent_profiles`).toHaveLength(0);
  expect(yield* getOrCreateOwnedAgent(owner, unlockKey)).toEqual(first);
  return keys;
});

const qualifyInbox = (keys: readonly string[]) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const message = yield* Message.create({
      author: testAgentCard("agent:alice"),
      recipients: [testAgentCard("agent:bob"), testAgentCard("agent:carol")],
      content: [{ type: "text", text: "Postgres inbox qualification" }],
    });
    const address = yield* Schema.decodeUnknown(MessageAddress)("agent:alice");
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

it("isolates concurrent owners and clears transaction context on a pooled non-owner login", () =>
  Effect.gen(function* () {
    const locator = yield* Config.redacted("SHARNESS_PG_TEST_URL");
    const adminLayer = databaseConnection(locator);
    const admin = Context.get(
      yield* Layer.build(adminLayer),
      SqlClient.SqlClient,
    );
    const suffix = randomBytes(8).toString("hex");
    const schema = `qualification_${suffix}`;
    const role = `runtime_${suffix}`;
    yield* admin`CREATE SCHEMA ${admin(schema)}`;
    yield* admin`CREATE ROLE ${admin(role)} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS PASSWORD ${admin.literal("'" + runtimePassword + "'")}`;
    yield* Effect.addFinalizer(() =>
      admin`DROP SCHEMA ${admin(schema)} CASCADE`.pipe(
        Effect.zipRight(admin`DROP ROLE ${admin(role)}`),
        Effect.orDie,
      ),
    );
    const schemaUrl = new URL(Redacted.value(locator));
    schemaUrl.searchParams.set("options", `-c search_path=${schema}`);
    yield* migrateDaemon.pipe(
      Effect.provide(databaseConnection(Redacted.make(schemaUrl.href))),
    );
    yield* admin`GRANT USAGE ON SCHEMA ${admin(schema)} TO ${admin(role)}`;
    yield* admin`GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA ${admin(schema)} TO ${admin(role)}`;
    yield* admin`GRANT USAGE ON ALL SEQUENCES IN SCHEMA ${admin(schema)} TO ${admin(role)}`;
    schemaUrl.username = role;
    schemaUrl.password = runtimePassword;
    const keys = yield* qualifyTenants.pipe(
      Effect.flatMap(qualifyInbox),
      Effect.provide(databaseConnection(Redacted.make(schemaUrl.href))),
    );
    const retained = yield* admin<
      Record<"agent_key", string>
    >`SELECT agent_key FROM ${admin(schema)}.agent_profiles`;
    expect(new Set(retained.map((row) => row.agent_key))).toEqual(
      new Set(keys),
    );
  }));
