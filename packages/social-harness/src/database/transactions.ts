/** @file Compiles typed queries for execution on the existing Effect SQL connection. */
import { SqlClient } from "@effect/sql";
import { Effect } from "effect";

/** Tenant context lasts only for this transaction and cannot remain on a pooled connection. */
export const withAgentTransaction = <A, E, R>(
  agentKey: string,
  work: Effect.Effect<A, E, R>,
) =>
  Effect.flatMap(SqlClient.SqlClient, (sql) =>
    sql.withTransaction(
      Effect.gen(function* () {
        yield* sql.onDialectOrElse({
          pg: () =>
            sql`SELECT set_config('social_harness.agent_key', ${agentKey}, true)`.pipe(
              Effect.asVoid,
            ),
          orElse: () => Effect.void,
        });
        return yield* work;
      }),
    ),
  ).pipe(Effect.withSpan("withAgentTransaction"));
