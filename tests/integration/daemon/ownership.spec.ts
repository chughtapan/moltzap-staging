/** @file On PostgreSQL under a restricted login, each owner is bound to one agent across concurrent connections and interrupted setup. */
import { NodeContext } from "@effect/platform-node";
import { SqlClient } from "@effect/sql";
import { scopedLive as it } from "@effect/vitest";
import { Identity } from "@social-harness/social-harness/service";
import {
  databaseConnection,
  getOrCreateOwnedAgentKey,
} from "@social-harness/social-harness/test";
import { Context, Effect, Exit, Layer } from "effect";
import { expect } from "vitest";
import { acquirePostgres } from "./postgres-fixture.js";

const owner = { issuer: "https://issuer.example", subject: "owner" };

/** The card the daemon gives the agent stored under `agentKey`. */
const cardOf = (agentKey: string) =>
  Layer.build(Identity.layer(agentKey)).pipe(
    Effect.map((identity) => Context.get(identity, Identity).me),
  );

it(
  "gives concurrent PostgreSQL connections of one owner the same agent with a restricted login",
  () =>
    Effect.gen(function* () {
      const postgres = yield* acquirePostgres;
      const runtime = Context.get(
        yield* Layer.build(databaseConnection(postgres.runtimeLocator)),
        SqlClient.SqlClient,
      );
      const provision = getOrCreateOwnedAgentKey(owner).pipe(
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
      const provision = getOrCreateOwnedAgentKey(owner).pipe(
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
