/** @file Each owner is bound to one agent, which keeps its identifiers across connections and interrupted setup. */
import { NodeContext } from "@effect/platform-node";
import { SqlClient } from "@effect/sql";
import { scopedLive as it } from "@effect/vitest";
import { AgentName } from "@social-harness/social-harness";
import { Identity } from "@social-harness/social-harness/service";
import {
  databaseConnection,
  getOrCreateOwnedAgent,
  migrateDaemon,
} from "@social-harness/social-harness/test";
import { Context, Effect, Exit, Layer, Redacted } from "effect";
import { expect } from "vitest";
import { acquirePostgres } from "./postgres-fixture.js";

const owner = { issuer: "https://issuer.example", subject: "owner" };
const database = databaseConnection(Redacted.make("sqlite::memory:"));

/** The card the daemon gives the agent stored under `agentKey`. */
const cardOf = (agentKey: string) =>
  Layer.build(Identity.layer(agentKey)).pipe(
    Effect.map((identity) => Context.get(identity, Identity).me),
  );

it("gives concurrent and repeated connections of one owner the same agent", () =>
  Effect.gen(function* () {
    yield* migrateDaemon;
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
    yield* migrateDaemon;
    const agentKeys = [
      yield* getOrCreateOwnedAgent(owner),
      yield* getOrCreateOwnedAgent({
        ...owner,
        issuer: "https://other.example",
      }),
      yield* getOrCreateOwnedAgent({ ...owner, subject: "another" }),
    ];
    expect(new Set(agentKeys).size).toBe(agentKeys.length);
    const cards = yield* Effect.forEach(agentKeys, cardOf, { concurrency: 1 });
    expect(new Set(cards.map((card) => card.principalId)).size).toBe(
      cards.length,
    );
    expect(new Set(cards.map((card) => card.agentId)).size).toBe(cards.length);
  }).pipe(Effect.provide(database)));

it("rolls back the owner binding when saving its profile fails", () =>
  Effect.gen(function* () {
    yield* migrateDaemon;
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
    yield* migrateDaemon;
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

it(
  "gives concurrent PostgreSQL connections of one owner the same agent with a restricted login",
  () =>
    Effect.gen(function* () {
      const postgres = yield* acquirePostgres;
      const runtime = Context.get(
        yield* Layer.build(databaseConnection(postgres.runtimeLocator)),
        SqlClient.SqlClient,
      );
      const provision = getOrCreateOwnedAgent(owner).pipe(
        Effect.provideService(SqlClient.SqlClient, runtime),
      );
      const agentKeys = yield* Effect.all(
        Array.from({ length: 8 }, () => provision),
        { concurrency: 8 },
      );
      const reopened = yield* provision;
      expect(new Set(agentKeys)).toEqual(new Set([reopened]));
      const card = (agentKey: string) =>
        cardOf(agentKey).pipe(
          Effect.provideService(SqlClient.SqlClient, runtime),
        );
      expect(yield* card(yield* provision)).toEqual(yield* card(reopened));
      expect(yield* postgres.admin`SELECT * FROM owner_bindings`).toHaveLength(
        1,
      );
    }).pipe(Effect.provide(NodeContext.layer)),
  60_000,
);

it(
  "rolls back a PostgreSQL owner binding when its profile cannot be stored",
  () =>
    Effect.gen(function* () {
      const postgres = yield* acquirePostgres;
      const sql = postgres.admin;
      yield* sql`CREATE FUNCTION reject_profile() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'profile unavailable'; END $$`;
      yield* sql`CREATE TRIGGER reject_profile BEFORE INSERT ON agent_profiles FOR EACH ROW EXECUTE FUNCTION reject_profile()`;
      const runtime = Context.get(
        yield* Layer.build(databaseConnection(postgres.runtimeLocator)),
        SqlClient.SqlClient,
      );
      const provision = getOrCreateOwnedAgent(owner).pipe(
        Effect.provideService(SqlClient.SqlClient, runtime),
      );
      expect(Exit.isFailure(yield* provision.pipe(Effect.exit))).toBeTruthy();
      expect(yield* sql`SELECT * FROM owner_bindings`).toHaveLength(0);
      yield* sql`DROP TRIGGER reject_profile ON agent_profiles`;
      const recovered = yield* provision;
      expect(
        (yield* cardOf(recovered).pipe(
          Effect.provideService(SqlClient.SqlClient, runtime),
        )).agentName,
      ).toMatch(/^agent-/u);
    }).pipe(Effect.provide(NodeContext.layer)),
  60_000,
);
