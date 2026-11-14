/** @file Each owner is bound to one agent, which keeps its identifiers across connections and interrupted setup. */
import { SqlClient } from "@effect/sql";
import { scopedLive as it } from "@effect/vitest";
import { Context, Effect, Exit, FastCheck as fc, Layer } from "effect";
import { expect } from "vitest";
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
        owners.map((owner) => getOrCreateOwnedAgent(owner)),
        { concurrency: owners.length },
      );
      const again = yield* Effect.forEach(
        owners,
        (owner) => getOrCreateOwnedAgent(owner),
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
);

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
