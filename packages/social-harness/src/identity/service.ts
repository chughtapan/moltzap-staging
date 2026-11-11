/** @file Reads this agent's card and the cards of the other agents on its daemon. */

import { SqlClient } from "@effect/sql";
import { Context, Data, Effect, Layer, Schema } from "effect";
import { AgentCard, type AgentName } from "./agent-cards.js";

/**
 * The daemon has no such agent, or its record of agents could not be read.
 * `agentName` is the name that was looked up.
 */
export class IdentityError extends Data.TaggedError("IdentityError")<{
  readonly agentName?: AgentName;
  readonly reason: "persistence-failed" | "unknown-agent";
}> {}

interface IdentityService {
  /** This agent. */
  readonly me: AgentCard;
  /** The agents with these names, in input order. Fails on the first name no agent has. */
  readonly resolve: (
    names: readonly AgentName[],
  ) => Effect.Effect<readonly AgentCard[], IdentityError>;
}

/** Names this agent and finds the other agents of the same daemon by name. */
export class Identity extends Context.Tag("SocialHarness/Identity")<
  Identity,
  IdentityService
>() {
  /** The agent stored under `agentKey`, resolving names in the same database. */
  static readonly layer = identityLayer;
}

function identityLayer(
  agentKey: string,
): Layer.Layer<Identity, IdentityError, SqlClient.SqlClient> {
  return Layer.effect(
    Identity,
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const [me] = yield* cards(sql, "agent_key", agentKey);
      if (me === undefined) {
        return yield* new IdentityError({ reason: "unknown-agent" });
      }
      return Identity.of({
        me,
        resolve: (names) =>
          Effect.forEach(names, (agentName) => named(sql, agentName), {
            concurrency: 1,
          }).pipe(Effect.withSpan("Identity.resolve")),
      });
    }).pipe(Effect.withSpan("Identity.layer")),
  );
}

function named(sql: SqlClient.SqlClient, agentName: AgentName) {
  return cards(sql, "agent_name", agentName).pipe(
    Effect.flatMap(([card]) =>
      card === undefined
        ? Effect.fail(new IdentityError({ reason: "unknown-agent", agentName }))
        : Effect.succeed(card),
    ),
  );
}

/** The cards of the agents whose `column` holds `value`: one, or none. */
function cards(
  sql: SqlClient.SqlClient,
  column: "agent_key" | "agent_name",
  value: string,
) {
  return sql`SELECT agent_id AS "agentId",agent_name AS "agentName",principal_id AS "principalId" FROM agent_profiles WHERE ${sql(column)}=${value}`.pipe(
    Effect.flatMap(Schema.decodeUnknown(Schema.Array(AgentCard))),
    Effect.catchTags({
      SqlError: () => new IdentityError({ reason: "persistence-failed" }),
      ParseError: () => new IdentityError({ reason: "persistence-failed" }),
    }),
  );
}
