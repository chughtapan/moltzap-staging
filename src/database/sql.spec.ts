/** @file An agent's transaction reaches only that agent's rows, and a nested one hands the outer agent its rows back. */
import type { SqlError } from "@effect/sql/SqlError";
import { SqlClient } from "@effect/sql";
import { scopedLive as it } from "@effect/vitest";
import { Effect, FastCheck as fc, Fiber, Layer, Predicate } from "effect";
import { expect } from "vitest";
import { databaseConnection, withAgentRows } from "./sql.js";
import { openTestDatabase, restrictedLogin } from "./testing.js";

/**
 * One row for each table whose rows belong to an agent, without its agent
 * key. A table follows the table its rows refer to.
 */
const rows: Readonly<
  Record<string, Readonly<Record<string, string | number>>>
> = {
  inbox_entries: {
    dedupe_key: "dedupe",
    delivery_token: "token",
    address: "agent:alice",
    kind: "message",
    status: "unchecked",
  },
  inbox_visibility: { author: "author", message_id: "message", visible: 1 },
  subscriptions: { subscription: "{}" },
  firewall_checks: {
    check_id: "check",
    direction: "outgoing",
    message: "{}",
    input: "{}",
    result: "{}",
    decided_at: 0,
    decision: "allowed",
  },
  firewall_policy: { policy: "{}" },
  firewall_activations: {
    activated_at: 0,
    revision: "revision",
    added_rule_ids: "[]",
    removed_rule_ids: "[]",
  },
  message_log: {
    conversation_id: "conversation",
    ordinal: 0,
    author_agent_id: "author",
    msg_id: "message",
    message: "{}",
  },
  collectives: {
    request_id: "request",
    requester_agent_id: "requester",
    role: "requester",
    request: "{}",
    deadline_at: 0,
  },
  collective_copies: { copy_id: "copy", request_id: "request" },
  collective_responses: {
    request_id: "request",
    recipient: "recipient",
    message: "{}",
  },
};
const tables = Object.keys(rows);

/** The SQLSTATE of a statement a row policy refuses, which no key or reference conflict gives. */
const REFUSED_BY_ROW_POLICY = "42501";

/** The SQLSTATE PostgreSQL gave for a failed statement. */
function sqlState(error: SqlError) {
  return Predicate.hasProperty(error.cause, "code")
    ? error.cause.code
    : undefined;
}

/** A new database, reached as a login that is no superuser and cannot bypass row policies. */
const restricted = Layer.unwrapScoped(
  openTestDatabase().pipe(
    Effect.flatMap(restrictedLogin),
    Effect.map(databaseConnection),
  ),
);

it.prop(
  "lets an agent's transaction read, add and remove only the rows stored under its own agent key, however many agents' transactions share the pool",
  [fc.uniqueArray(fc.string({ minLength: 1 }), { minLength: 2, maxLength: 4 })],
  ([agentKeys]) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const agentKeysIn = (table: string) =>
        Effect.map(
          sql<{
            agentKey: string;
          }>`SELECT agent_key AS "agentKey" FROM ${sql(table)}`,
          (stored) => stored.map((row) => row.agentKey),
        );
      const add = (table: string, agentKey: string) =>
        sql`INSERT INTO ${sql(table)} ${sql.insert({ agent_key: agentKey, ...rows[table] })}`;
      const [first] = agentKeys;
      const turns = Array.from({ length: 6 }).flatMap(() => agentKeys);
      const another = `${agentKeys.join("")}.`;

      yield* Effect.forEach(
        agentKeys,
        (agentKey) =>
          withAgentRows(
            agentKey,
            Effect.forEach(tables, (table) => add(table, agentKey), {
              concurrency: 1,
            }),
          ),
        { concurrency: agentKeys.length },
      );
      const reached = yield* Effect.forEach(
        turns,
        (agentKey) =>
          withAgentRows(
            agentKey,
            Effect.forEach(
              tables,
              (table) =>
                Effect.all({
                  read: agentKeysIn(table),
                  addingForAnother: sql
                    .withTransaction(add(table, another))
                    .pipe(Effect.flip, Effect.map(sqlState)),
                }),
              { concurrency: 1 },
            ),
          ),
        { concurrency: 8 },
      );
      yield* withAgentRows(
        first ?? "",
        Effect.forEach(
          tables.toReversed(),
          (table) => sql`DELETE FROM ${sql(table)}`,
          { concurrency: 1 },
        ),
      );
      const remaining = yield* Effect.forEach(
        agentKeys,
        (agentKey) =>
          withAgentRows(
            agentKey,
            Effect.forEach(tables, agentKeysIn, { concurrency: 1 }),
          ),
        { concurrency: 1 },
      );
      const outsideAnyAgent = yield* Effect.forEach(tables, agentKeysIn, {
        concurrency: 1,
      });

      expect(reached).toEqual(
        turns.map((agentKey) =>
          tables.map(() => ({
            read: [agentKey],
            addingForAnother: REFUSED_BY_ROW_POLICY,
          })),
        ),
      );
      expect(remaining).toEqual(
        agentKeys.map((agentKey) =>
          tables.map(() => (agentKey === first ? [] : [agentKey])),
        ),
      );
      expect(outsideAnyAgent).toEqual(tables.map(() => []));
    }).pipe(Effect.provide(restricted)),
  { fastCheck: { numRuns: 10 } },
);

it.each([
  { ending: "succeeds", nested: Effect.void },
  {
    ending: "fails",
    nested: Effect.flatMap(SqlClient.SqlClient, (sql) => sql`SELECT 1/0`),
  },
  { ending: "is interrupted", nested: Effect.interrupt },
])(
  "restores the outer agent key after a nested agent's transaction $ending",
  ({ nested }) =>
    Effect.gen(function* () {
      const { run, sql } = yield* openTestDatabase();
      const agentKey = Effect.map(
        sql<{
          value: string;
        }>`SELECT current_setting('social_harness.agent_key', true) AS value`,
        (settings) => settings[0]?.value,
      );

      const restored = yield* run(
        withAgentRows(
          "outer",
          withAgentRows("inner", nested).pipe(
            Effect.fork,
            Effect.flatMap(Fiber.await),
            Effect.zipRight(agentKey),
          ),
        ),
      );

      expect(restored).toBe("outer");
    }),
);
