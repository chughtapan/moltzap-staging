/** @file Scopes tenant authority to a SQL transaction or nested savepoint. */
import { SqlClient } from "@effect/sql";
import { Effect } from "effect";

/** A nested tenant operation restores its caller after its savepoint has closed. */
export const withAgentTransaction = <A, E, R>(
  agentKey: string,
  work: Effect.Effect<A, E, R>,
) =>
  Effect.flatMap(SqlClient.SqlClient, (sql) =>
    sql.withTransaction(
      sql.onDialectOrElse({
        pg: () => postgresAgentTransaction(sql, agentKey, work),
        orElse: () => work,
      }),
    ),
  ).pipe(Effect.withSpan("withAgentTransaction"));

/** The inner savepoint rolls back a failed query before restoring the outer transaction's tenant. */
function postgresAgentTransaction<A, E, R>(
  sql: SqlClient.SqlClient,
  agentKey: string,
  work: Effect.Effect<A, E, R>,
) {
  return Effect.gen(function* () {
    const [row] = yield* sql<{
      previous: string | null;
    }>`SELECT current_setting('social_harness.agent_key', true) AS previous`;
    // eslint-disable-next-line agent-code-guard/acquire-release-requires-scope -- acquireUseRelease brackets this transaction-local setting itself; no resource escapes the call.
    return yield* Effect.acquireUseRelease(
      sql`SELECT set_config('social_harness.agent_key', ${agentKey}, true)`,
      () => sql.withTransaction(work),
      () =>
        sql`SELECT set_config('social_harness.agent_key', ${row?.previous ?? ""}, true)`.pipe(
          Effect.orDie,
        ),
    );
  });
}
