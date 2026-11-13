/** @file Binds each authenticated owner to one agent and records that agent's identifier, name and principal. */
import { SqlClient } from "@effect/sql";
import { Data, Effect, Schema } from "effect";
import { randomBytes, randomUUID } from "node:crypto";
import { executeQuery, makeQueries } from "../database/index.js";
import { AgentId, AgentName, PrincipalId } from "../identity.js";

/** Authentication supplies both fields; email and client-provided agent selectors are excluded. */
export interface OwnerAccount {
  readonly issuer: string;
  readonly subject: string;
}

/** Local bearer access names the personal owner of a self-hosted database. */
export const localOwner = {
  issuer: "urn:social-harness:self-hosted",
  subject: "owner",
};

/** The owner of the server agent, which signs in with the daemon's own credential. */
export const serverAgentOwner = {
  issuer: "urn:social-harness:server-agent",
  subject: "owner",
};

const profileRow = Schema.Struct({
  agent_key: Schema.UUID,
  agent_id: AgentId,
  agent_name: AgentName,
  principal_id: PrincipalId,
});

const bindingRow = Schema.Struct({
  agent_key: Schema.UUID,
  issuer: Schema.String,
  subject: Schema.String,
});

type OwnershipTables = Record<"owner_bindings", typeof bindingRow.Type> &
  Record<"agent_profiles", typeof profileRow.Type>;

/**
 * The owner's agent has another name than the one asked for, the name belongs
 * to another owner's agent, or the agent's record is missing. Carries no
 * account claims.
 * Exported so dependent declarations can name the inferred Effect failure.
 * @public
 */
export class OwnershipError extends Data.TaggedError("OwnershipError") {}

const minted = (prefix: string) =>
  `${prefix}${randomBytes(16).toString("base64url")}`;

/**
 * The key of the owner's agent, creating the agent on first use with a new
 * identifier and principal. Concurrent first calls for one owner return the
 * same key. With `agentName`, the agent is created under that name and an
 * existing agent must already have it.
 */
export const getOrCreateOwnedAgent = Effect.fn("getOrCreateOwnedAgent")(
  function* (owner: OwnerAccount, agentName?: AgentName) {
    const sql = yield* SqlClient.SqlClient;
    const queries = yield* makeQueries<OwnershipTables>();
    const lookup = () =>
      executeQuery(
        queries
          .selectFrom("owner_bindings")
          .selectAll()
          .where("issuer", "=", owner.issuer)
          .where("subject", "=", owner.subject),
      ).pipe(Effect.flatMap(Schema.decodeUnknown(Schema.Array(bindingRow))));
    const ownedKey = (agentKey: string) =>
      executeQuery(
        queries
          .selectFrom("agent_profiles")
          .select("agent_name")
          .where("agent_key", "=", agentKey),
      ).pipe(
        Effect.filterOrFail(
          ([profile]) =>
            profile !== undefined &&
            (agentName === undefined || profile.agent_name === agentName),
          () => new OwnershipError(),
        ),
        Effect.as(agentKey),
      );
    const existing = (yield* lookup())[0];
    if (existing !== undefined) {
      return yield* ownedKey(existing.agent_key);
    }
    const candidateKey = randomUUID();
    const profile = yield* Schema.decodeUnknown(profileRow)({
      agent_key: candidateKey,
      agent_id: minted("agt_"),
      agent_name: agentName ?? `agent-${randomBytes(12).toString("hex")}`,
      principal_id: minted("prn_"),
    });
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* executeQuery(
          queries
            .insertInto("owner_bindings")
            .values({
              agent_key: candidateKey,
              issuer: owner.issuer,
              subject: owner.subject,
            })
            .onConflict((conflict) =>
              conflict.columns(["issuer", "subject"]).doNothing(),
            ),
        );
        const binding = (yield* lookup())[0];
        if (binding === undefined) {
          return yield* new OwnershipError();
        }
        if (binding.agent_key === candidateKey) {
          yield* executeQuery(
            queries
              .insertInto("agent_profiles")
              .values(profile)
              .onConflict((conflict) =>
                conflict.column("agent_name").doNothing(),
              ),
          );
        }
        return yield* ownedKey(binding.agent_key);
      }),
    );
  },
  Effect.withSpan("getOrCreateOwnedAgent"),
);

/** The key of every agent the daemon has created. */
export const listOwnedAgentKeys = Effect.gen(function* () {
  const queries = yield* makeQueries<OwnershipTables>();
  const rows = yield* executeQuery(
    queries.selectFrom("owner_bindings").select("agent_key"),
  );
  return yield* Effect.forEach(
    rows,
    (row) => Schema.decodeUnknown(Schema.UUID)(row.agent_key),
    { concurrency: 1 },
  );
}).pipe(Effect.withSpan("listOwnedAgentKeys"));
