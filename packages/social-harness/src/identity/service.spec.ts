/** @file An agent's own card and name resolution among the agents of one daemon. */

import { SqlClient } from "@effect/sql";
import { scopedLive as it } from "@effect/vitest";
import { Context, Effect, Layer } from "effect";
import { expect } from "vitest";
import { createTestAgent } from "../__tests__/agents.js";
import { openTestDatabase } from "../communication/__tests__/store-fixtures.js";
import { AgentName } from "./agent-cards.js";
import { Identity } from "./service.js";

const names = { alice: "alice", bob: "bob" };

const fixture = Effect.gen(function* () {
  const database = yield* openTestDatabase();
  return {
    alice: yield* database.run(createTestAgent(names.alice)),
    bob: yield* database.run(createTestAgent(names.bob)),
    open: (agentKey: string) =>
      Layer.build(Identity.layer(agentKey)).pipe(
        Effect.provideService(SqlClient.SqlClient, database.sql),
        Effect.map((context) => Context.get(context, Identity)),
      ),
  };
});

it("gives each agent its own card under the name it was created with", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const alice = yield* f.open(f.alice.agentKey);
    const bob = yield* f.open(f.bob.agentKey);
    expect(alice.me.agentName).toBe(names.alice);
    expect(bob.me.agentName).toBe(names.bob);
    expect(alice.me.agentId).not.toBe(bob.me.agentId);
    expect((yield* f.open(f.alice.agentKey)).me).toEqual(alice.me);
  }));

it("resolves names in input order and fails with the first unknown name", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const alice = yield* f.open(f.alice.agentKey);
    const missing = AgentName.make("missing-agent");
    expect(
      yield* alice.resolve([f.bob.card.agentName, f.alice.card.agentName]),
    ).toEqual([f.bob.card, f.alice.card]);
    expect(
      yield* alice
        .resolve([
          f.bob.card.agentName,
          missing,
          AgentName.make("also-missing"),
        ])
        .pipe(Effect.flip),
    ).toMatchObject({ reason: "unknown-agent", agentName: missing });
  }));

it("does not open for a key the daemon has no agent under", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    expect(yield* f.open("no-such-key").pipe(Effect.flip)).toMatchObject({
      reason: "unknown-agent",
    });
  }));
