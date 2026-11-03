/** @file Owner bindings preserve registration identity across reconnects and interrupted setup. */
import { SqlClient } from "@effect/sql";
import { scopedLive as it } from "@effect/vitest";
import { Effect, Exit, Redacted } from "effect";
import { expect } from "vitest";
import { migrateDaemon } from "../database/migrations.js";
import { openCredential } from "../database/secrets.js";
import { databaseConnection } from "../database/sql.js";
import { AgentSigningAuthority } from "../identity/keys.js";
import { getOrCreateOwnedAgent } from "./ownership.js";

const unlockKey = Redacted.make(new Uint8Array(32).fill(7));
const owner = { issuer: "https://issuer.example", subject: "owner" };
const database = databaseConnection(Redacted.make("sqlite::memory:"));

// @agent-code-guard/regression-only: Real SQLite covers racing setup, rollback and encrypted identity recovery.
it("reuses identity material for concurrent and repeated connections", () =>
  Effect.gen(function* () {
    yield* migrateDaemon;
    const profiles = yield* Effect.all(
      Array.from({ length: 8 }, () => getOrCreateOwnedAgent(owner, unlockKey)),
      { concurrency: 8 },
    );
    expect(profiles).toHaveLength(8);
    for (const profile of profiles) {
      expect(profile).toEqual(profiles[0]);
    }
    const reopened = yield* getOrCreateOwnedAgent(owner, unlockKey);
    expect(reopened).toEqual(profiles[0]);
    const privateKey = yield* openCredential(
      reopened.private_key,
      reopened.agent_key,
      unlockKey,
    );
    yield* AgentSigningAuthority.fromPkcs8(privateKey);
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
    expect(
      new Set([first.agent_key, otherIssuer.agent_key, otherSubject.agent_key])
        .size,
    ).toBe([first, otherIssuer, otherSubject].length);
    expect(
      new Set([
        first.principal_id,
        otherIssuer.principal_id,
        otherSubject.principal_id,
      ]).size,
    ).toBe([first, otherIssuer, otherSubject].length);
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
    expect(recovered.agent_name).toMatch(/^agent-/u);
  }).pipe(Effect.provide(database)));

it("rejects a credential substituted from another agent or unlocked with another key", () =>
  Effect.gen(function* () {
    yield* migrateDaemon;
    const first = yield* getOrCreateOwnedAgent(owner, unlockKey);
    const second = yield* getOrCreateOwnedAgent(
      { ...owner, subject: "another" },
      unlockKey,
    );
    const substituted = yield* openCredential(
      first.private_key,
      second.agent_key,
      unlockKey,
    ).pipe(Effect.exit);
    const wrongKey = yield* openCredential(
      first.private_key,
      first.agent_key,
      Redacted.make(new Uint8Array(32).fill(9)),
    ).pipe(Effect.exit);
    expect(Exit.isFailure(substituted)).toBeTruthy();
    expect(Exit.isFailure(wrongKey)).toBeTruthy();
  }).pipe(Effect.provide(database)));
