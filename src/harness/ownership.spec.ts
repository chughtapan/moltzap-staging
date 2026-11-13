/** @file Each owner is bound to one agent, which keeps its identifiers across connections and interrupted setup. */
import { SqlClient } from "@effect/sql";
import { scopedLive as it } from "@effect/vitest";
import { Context, Effect, Exit, Layer } from "effect";
import { describe, expect } from "vitest";
import { openTestDatabase } from "../communication/__tests__/store-fixtures.js";
import { AgentName, Identity } from "../identity.js";
import { getOrCreateOwnedAgent } from "./ownership.js";

const owner = { issuer: "https://issuer.example", subject: "owner" };

/** A prepared database no other test shares. */
const database = Layer.scoped(
  SqlClient.SqlClient,
  openTestDatabase().pipe(Effect.map(({ sql }) => sql)),
);

/** The card the daemon gives the agent stored under `agentKey`. */
const cardOf = (agentKey: string) =>
  Layer.build(Identity.layer(agentKey)).pipe(
    Effect.map((identity) => Context.get(identity, Identity).me),
  );

describe("the agent an owner's account is bound to", () => {
  it("gives concurrent and repeated connections of one owner the same agent", () =>
    Effect.gen(function* () {
      const agentKeys = yield* Effect.all(
        Array.from({ length: 8 }, () => getOrCreateOwnedAgent(owner)),
        { concurrency: 8 },
      );
      const reopened = yield* getOrCreateOwnedAgent(owner);
      expect(new Set(agentKeys)).toEqual(new Set([reopened]));
      const card = yield* cardOf(reopened);
      expect(yield* cardOf(yield* getOrCreateOwnedAgent(owner))).toEqual(card);
      const sql = yield* SqlClient.SqlClient;
      expect(yield* sql`SELECT * FROM owner_bindings`).toHaveLength(1);
    }).pipe(Effect.provide(database)));

  it("distinguishes both issuer and subject without merging accounts", () =>
    Effect.gen(function* () {
      const agentKeys = [
        yield* getOrCreateOwnedAgent(owner),
        yield* getOrCreateOwnedAgent({
          ...owner,
          issuer: "https://other.example",
        }),
        yield* getOrCreateOwnedAgent({ ...owner, subject: "another" }),
      ];
      expect(new Set(agentKeys).size).toBe(agentKeys.length);
      const cards = yield* Effect.forEach(agentKeys, cardOf, {
        concurrency: 1,
      });
      expect(new Set(cards.map((card) => card.principalId)).size).toBe(
        cards.length,
      );
      expect(new Set(cards.map((card) => card.agentId)).size).toBe(
        cards.length,
      );
    }).pipe(Effect.provide(database)));
});

describe("an agent's saved profile", () => {
  it("rolls back the owner binding when saving its profile fails", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TRIGGER reject_profile BEFORE INSERT ON agent_profiles BEGIN
      SELECT RAISE(ABORT, 'profile unavailable'); END`;
      expect(
        Exit.isFailure(yield* getOrCreateOwnedAgent(owner).pipe(Effect.exit)),
      ).toBeTruthy();
      expect(yield* sql`SELECT * FROM owner_bindings`).toHaveLength(0);
      yield* sql`DROP TRIGGER reject_profile`;
      const recovered = yield* getOrCreateOwnedAgent(owner);
      expect((yield* cardOf(recovered)).agentName).toMatch(/^agent-/u);
    }).pipe(Effect.provide(database)));

  it("refuses to rename a saved agent", () =>
    Effect.gen(function* () {
      yield* getOrCreateOwnedAgent(owner);
      const sql = yield* SqlClient.SqlClient;
      const before = yield* sql`SELECT * FROM agent_profiles`;
      const changed = yield* getOrCreateOwnedAgent(
        owner,
        AgentName.make("another"),
      ).pipe(Effect.exit);
      expect(Exit.isFailure(changed)).toBe(true);
      expect(yield* sql`SELECT * FROM agent_profiles`).toEqual(before);
    }).pipe(Effect.provide(database)));
});
