/** @file Each owner is bound to one agent, which keeps its identifiers across connections and interrupted setup, has its own card and resolves the names of the daemon's other agents. */
import { SqlClient } from "@effect/sql";
import { scopedLive as it } from "@effect/vitest";
import { Context, Effect, Exit, FastCheck as fc, Layer } from "effect";
import { expect } from "vitest";
import { createTestAgent } from "../__tests__/agents.js";
import { AgentName, Identity } from "../identity.js";
import { openTestDatabase } from "../test.js";
import { getOrCreateOwnedAgentKey, identityLayer } from "./owners.js";

const owner = { issuer: "https://issuer.example", subject: "owner" };

/** A prepared database no other test shares. Each property run builds one, so the property bounds its runs. */
const database = Layer.scoped(
  SqlClient.SqlClient,
  openTestDatabase().pipe(Effect.map(({ sql }) => sql)),
);

/** The card the daemon gives the agent stored under `agentKey`. */
const cardOf = (agentKey: string) =>
  Layer.build(identityLayer(agentKey)).pipe(
    Effect.map((identity) => Context.get(identity, Identity).me),
  );

const accounts = fc.array(
  fc.record({
    issuer: fc.constantFrom("https://issuer.example", "https://other.example"),
    subject: fc.constantFrom("owner", "another", "third"),
  }),
  { minLength: 1, maxLength: 8 },
);

it.prop(
  "binds each issuer and subject pair to one agent of its own, however many connections arrive at once or again",
  [accounts],
  ([owners]) =>
    Effect.gen(function* () {
      const account = ({ issuer, subject }: (typeof owners)[number]) =>
        `${issuer} ${subject}`;
      const distinct = new Set(owners.map(account)).size;

      const agentKeys = yield* Effect.all(
        owners.map((owner) => getOrCreateOwnedAgentKey(owner)),
        { concurrency: owners.length },
      );
      const again = yield* Effect.forEach(
        owners,
        (owner) => getOrCreateOwnedAgentKey(owner),
        { concurrency: 1 },
      );
      const cards = yield* Effect.forEach(agentKeys, cardOf, {
        concurrency: 1,
      });
      const sql = yield* SqlClient.SqlClient;

      expect(again).toEqual(agentKeys);
      expect(
        new Set(
          owners.map(
            (owner, index) => `${account(owner)} ${agentKeys[index] ?? ""}`,
          ),
        ).size,
      ).toBe(distinct);
      for (const identifiers of [
        agentKeys,
        cards.map((card) => card.agentId),
        cards.map((card) => card.principalId),
      ]) {
        expect(new Set(identifiers).size).toBe(distinct);
      }
      expect(yield* sql`SELECT * FROM owner_bindings`).toHaveLength(distinct);
    }).pipe(Effect.provide(database)),
  { fastCheck: { numRuns: 25 } },
);

it("rolls back the owner binding when saving its card fails", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`ALTER TABLE agent_cards ADD CONSTRAINT reject_card CHECK (false) NOT VALID`;
    expect(
      Exit.isFailure(yield* getOrCreateOwnedAgentKey(owner).pipe(Effect.exit)),
    ).toBeTruthy();
    expect(yield* sql`SELECT * FROM owner_bindings`).toHaveLength(0);
    yield* sql`ALTER TABLE agent_cards DROP CONSTRAINT reject_card`;
    const recovered = yield* getOrCreateOwnedAgentKey(owner);
    expect((yield* cardOf(recovered)).agentName).toMatch(/^agent-/u);
  }).pipe(Effect.provide(database)));

it("refuses to rename a saved agent", () =>
  Effect.gen(function* () {
    yield* getOrCreateOwnedAgentKey(owner);
    const sql = yield* SqlClient.SqlClient;
    const before = yield* sql`SELECT * FROM agent_cards`;
    const changed = yield* getOrCreateOwnedAgentKey(
      owner,
      AgentName.make("another"),
    ).pipe(Effect.exit);
    expect(Exit.isFailure(changed)).toBe(true);
    expect(yield* sql`SELECT * FROM agent_cards`).toEqual(before);
  }).pipe(Effect.provide(database)));

const names = { alice: "alice", bob: "bob" };

const fixture = Effect.gen(function* () {
  const database = yield* openTestDatabase();
  return {
    alice: yield* database.run(createTestAgent(names.alice)),
    bob: yield* database.run(createTestAgent(names.bob)),
    open: (agentKey: string) =>
      Layer.build(identityLayer(agentKey)).pipe(
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
