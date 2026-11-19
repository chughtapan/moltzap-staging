/** @file Gives each owner one agent, saved as one row with its card, and names the daemon's agents from their cards. */
import { SqlClient, type Statement } from "@effect/sql";
import { Data, Effect, Layer, Schema } from "effect";
import { randomBytes, randomUUID } from "node:crypto";
import {
  AgentCard,
  type AgentName,
  Identity,
  IdentityError,
} from "../identity.js";

/** Authentication supplies both fields; email and host-provided agent selectors are excluded. */
export interface Owner {
  readonly issuer: string;
  readonly subject: string;
}

/**
 * The owner's agent has another name than the one asked for, or the name
 * belongs to another owner's agent. Carries no account claims.
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
export const getOrCreateOwnedAgentKey = Effect.fn("getOrCreateOwnedAgentKey")(
  function* (owner: Owner, agentName?: AgentName) {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO agent_cards ${sql.insert({
      agent_key: randomUUID(),
      issuer: owner.issuer,
      subject: owner.subject,
      agent_id: minted("agt_"),
      agent_name: agentName ?? `agent-${randomBytes(12).toString("hex")}`,
      principal_id: minted("prn_"),
    })} ON CONFLICT DO NOTHING`;
    const [saved] = yield* sql<{
      agentKey: string;
      agentName: string;
    }>`SELECT agent_key AS "agentKey", agent_name AS "agentName" FROM agent_cards
      WHERE issuer = ${owner.issuer} AND subject = ${owner.subject}`;
    if (
      saved === undefined ||
      (agentName !== undefined && saved.agentName !== agentName)
    ) {
      return yield* new OwnershipError();
    }
    return saved.agentKey;
  },
);

/** The key of every agent the daemon has created. */
export const listOwnedAgentKeys = SqlClient.SqlClient.pipe(
  Effect.flatMap(
    (sql) =>
      sql<{
        agentKey: string;
      }>`SELECT agent_key AS "agentKey" FROM agent_cards`,
  ),
  Effect.map((rows) => rows.map((row) => row.agentKey)),
  Effect.withSpan("listOwnedAgentKeys"),
);

/** The identity of the agent stored under `agentKey`, resolving names in the same database. */
export function identityLayer(
  agentKey: string,
): Layer.Layer<Identity, IdentityError, SqlClient.SqlClient> {
  return Layer.effect(
    Identity,
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      /** The card of the one agent `condition` selects. */
      const card = (condition: Statement.Fragment, agentName?: AgentName) =>
        sql`SELECT agent_id AS "agentId", agent_name AS "agentName", principal_id AS "principalId"
          FROM agent_cards WHERE ${condition}`.pipe(
          Effect.flatMap(Schema.decodeUnknown(Schema.Array(AgentCard))),
          Effect.mapError(
            () => new IdentityError({ reason: "persistence-failed" }),
          ),
          Effect.flatMap(([found]) =>
            found === undefined
              ? Effect.fail(
                  new IdentityError({ reason: "unknown-agent", agentName }),
                )
              : Effect.succeed(found),
          ),
        );
      return Identity.of({
        me: yield* card(sql`agent_key = ${agentKey}`),
        resolve: (names) =>
          Effect.forEach(
            names,
            (agentName) => card(sql`agent_name = ${agentName}`, agentName),
            { concurrency: 1 },
          ).pipe(Effect.withSpan("Identity.resolve")),
      });
    }).pipe(Effect.withSpan("Identity.layer")),
  );
}
