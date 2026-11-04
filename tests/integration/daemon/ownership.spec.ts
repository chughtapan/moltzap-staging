/** @file Owner bindings preserve registration identity across reconnects and interrupted setup. */
import { NodeContext } from "@effect/platform-node";
import { SqlClient } from "@effect/sql";
import { scopedLive as it } from "@effect/vitest";
import { AgentSigningAuthority } from "@moltzap/social-harness";
import {
  databaseConnection,
  getOrCreateOwnedAgent,
  loadAgentRegistration,
  migrateDaemon,
} from "@moltzap/social-harness/test";
import { Context, Effect, Exit, Layer, Redacted } from "effect";
import { expect } from "vitest";
import { acquirePostgres } from "./postgres-fixture.js";

const unlockKey = Redacted.make(new Uint8Array(32).fill(7));
const owner = { issuer: "https://issuer.example", subject: "owner" };
const database = databaseConnection(Redacted.make("sqlite::memory:"));

// @agent-code-guard/regression-only: Real SQL covers racing setup, rollback and encrypted identity recovery.
it("reuses identity material for concurrent and repeated connections", () =>
  Effect.gen(function* () {
    yield* migrateDaemon;
    const agentKeys = yield* Effect.all(
      Array.from({ length: 8 }, () => getOrCreateOwnedAgent(owner, unlockKey)),
      { concurrency: 8 },
    );
    const reopened = yield* getOrCreateOwnedAgent(owner, unlockKey);
    expect(agentKeys).toHaveLength(8);
    for (const agentKey of agentKeys) {
      expect(agentKey).toBe(reopened);
    }
    const hydrated = yield* loadAgentRegistration(reopened, unlockKey);
    const repeated = yield* loadAgentRegistration(
      yield* getOrCreateOwnedAgent(owner, unlockKey),
      unlockKey,
    );
    expect(repeated.registration).toEqual(hydrated.registration);
    expect(AgentSigningAuthority.publicKey(repeated.signingAuthority)).toEqual(
      AgentSigningAuthority.publicKey(hydrated.signingAuthority),
    );
    const sql = yield* SqlClient.SqlClient;
    const bindings = yield* sql`SELECT * FROM owner_bindings`;
    expect(bindings).toHaveLength(1);
  }).pipe(Effect.provide(database)));

it("distinguishes both issuer and subject without merging accounts", () =>
  Effect.gen(function* () {
    yield* migrateDaemon;
    const first = yield* getOrCreateOwnedAgent(owner, unlockKey);
    const otherIssuer = yield* getOrCreateOwnedAgent(
      { ...owner, issuer: "https://other.example" },
      unlockKey,
    );
    const otherSubject = yield* getOrCreateOwnedAgent(
      { ...owner, subject: "another" },
      unlockKey,
    );
    expect(new Set([first, otherIssuer, otherSubject]).size).toBe(
      [first, otherIssuer, otherSubject].length,
    );
    const registrations = yield* Effect.forEach(
      [first, otherIssuer, otherSubject],
      (agentKey) => loadAgentRegistration(agentKey, unlockKey),
      { concurrency: 1 },
    );
    expect(
      new Set(registrations.map((value) => value.registration.principalId))
        .size,
    ).toBe(registrations.length);
  }).pipe(Effect.provide(database)));

it("rolls back the owner binding when saving its profile fails", () =>
  Effect.gen(function* () {
    yield* migrateDaemon;
    const sql = yield* SqlClient.SqlClient;
    yield* sql`CREATE TRIGGER reject_profile BEFORE INSERT ON agent_profiles BEGIN
      SELECT RAISE(ABORT, 'profile unavailable'); END`;
    expect(
      Exit.isFailure(
        yield* getOrCreateOwnedAgent(owner, unlockKey).pipe(Effect.exit),
      ),
    ).toBeTruthy();
    expect(yield* sql`SELECT * FROM owner_bindings`).toHaveLength(0);
    yield* sql`DROP TRIGGER reject_profile`;
    const recovered = yield* getOrCreateOwnedAgent(owner, unlockKey);
    expect(
      (yield* loadAgentRegistration(recovered, unlockKey)).registration
        .agentName,
    ).toMatch(/^agent-/u);
  }).pipe(Effect.provide(database)));

it("rejects a credential substituted from another agent or unlocked with another key", () =>
  Effect.gen(function* () {
    yield* migrateDaemon;
    const first = yield* getOrCreateOwnedAgent(owner, unlockKey);
    const second = yield* getOrCreateOwnedAgent(
      { ...owner, subject: "another" },
      unlockKey,
    );
    const sql = yield* SqlClient.SqlClient;
    yield* sql`UPDATE agent_profiles SET private_key = (SELECT private_key FROM agent_profiles WHERE agent_key = ${first}) WHERE agent_key = ${second}`;
    const substituted = yield* loadAgentRegistration(second, unlockKey).pipe(
      Effect.exit,
    );
    const wrongKey = yield* loadAgentRegistration(
      first,
      Redacted.make(new Uint8Array(32).fill(9)),
    ).pipe(Effect.exit);
    expect(Exit.isFailure(substituted)).toBeTruthy();
    expect(Exit.isFailure(wrongKey)).toBeTruthy();
  }).pipe(Effect.provide(database)));

it("rejects attempts to replace the saved identity", () =>
  Effect.gen(function* () {
    yield* migrateDaemon;
    const agentKey = yield* getOrCreateOwnedAgent(owner, unlockKey);
    const hydrated = yield* loadAgentRegistration(agentKey, unlockKey);
    const sql = yield* SqlClient.SqlClient;
    const before = yield* sql`SELECT * FROM agent_profiles`;
    const changed = yield* getOrCreateOwnedAgent(owner, unlockKey, {
      ...hydrated.registration,
      privateKey: Redacted.make("replacement"),
    }).pipe(Effect.exit);
    expect(Exit.isFailure(changed)).toBe(true);
    expect(yield* sql`SELECT * FROM agent_profiles`).toEqual(before);
  }).pipe(Effect.provide(database)));

it(
  "reuses owner identity across concurrent PostgreSQL connections with a restricted login",
  () =>
    Effect.gen(function* () {
      const postgres = yield* acquirePostgres;
      const runtime = Context.get(
        yield* Layer.build(databaseConnection(postgres.runtimeLocator)),
        SqlClient.SqlClient,
      );
      const provision = getOrCreateOwnedAgent(owner, unlockKey).pipe(
        Effect.provideService(SqlClient.SqlClient, runtime),
      );
      const agentKeys = yield* Effect.all(
        Array.from({ length: 8 }, () => provision),
        { concurrency: 8 },
      );
      const reopened = yield* provision;
      expect(new Set(agentKeys)).toEqual(new Set([reopened]));
      const load = loadAgentRegistration(reopened, unlockKey).pipe(
        Effect.provideService(SqlClient.SqlClient, runtime),
      );
      const hydrated = yield* load;
      const repeated = yield* load;
      expect(repeated.registration).toEqual(hydrated.registration);
      expect(
        AgentSigningAuthority.publicKey(repeated.signingAuthority),
      ).toEqual(AgentSigningAuthority.publicKey(hydrated.signingAuthority));
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
      const provision = getOrCreateOwnedAgent(owner, unlockKey).pipe(
        Effect.provideService(SqlClient.SqlClient, runtime),
      );
      expect(Exit.isFailure(yield* provision.pipe(Effect.exit))).toBeTruthy();
      expect(yield* sql`SELECT * FROM owner_bindings`).toHaveLength(0);
      yield* sql`DROP TRIGGER reject_profile ON agent_profiles`;
      const recovered = yield* provision;
      expect(
        (yield* loadAgentRegistration(recovered, unlockKey).pipe(
          Effect.provideService(SqlClient.SqlClient, runtime),
        )).registration.agentName,
      ).toMatch(/^agent-/u);
    }).pipe(Effect.provide(NodeContext.layer)),
  60_000,
);
