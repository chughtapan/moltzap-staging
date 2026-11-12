/** @file Opens SQL transactions, runs work once one commits, and scopes tenant authority to one. */
import { SqlClient } from "@effect/sql";
import { Context, Effect, Option } from "effect";

/** The work to run once the transaction a fiber is in commits. */
class Committed extends Context.Tag("SocialHarness/Committed")<
  Committed,
  Array<Effect.Effect<void>>
>() {}

/**
 * Runs `work` in a transaction, or in a savepoint of the one already open.
 * Work handed to `afterCommit` inside it runs once the outermost transaction
 * has committed, and never when that transaction rolls back.
 */
export const transaction = Effect.fn("transaction")(function* <A, E, R>(
  work: Effect.Effect<A, E, R>,
) {
  const sql = yield* SqlClient.SqlClient;
  if (Option.isSome(yield* Effect.serviceOption(Committed))) {
    return yield* sql.withTransaction(work);
  }
  const committed: Array<Effect.Effect<void>> = [];
  return yield* Effect.uninterruptibleMask((restore) =>
    sql
      .withTransaction(restore(work))
      .pipe(
        Effect.provideService(Committed, committed),
        Effect.zipLeft(
          Effect.suspend(() => Effect.all(committed, { discard: true })),
        ),
      ),
  );
});

/** Runs `work` once the open transaction commits, or at once when none is open. */
export const afterCommit = (work: Effect.Effect<void>) =>
  Effect.serviceOption(Committed).pipe(
    Effect.flatMap(
      Option.match({
        onNone: () => work,
        onSome: (committed) => Effect.sync(() => committed.push(work)),
      }),
    ),
    Effect.asVoid,
  );

/** A nested tenant operation restores its caller after its savepoint has closed. */
export const withAgentTransaction = Effect.fn("withAgentTransaction")(
  <A, E, R>(agentKey: string, work: Effect.Effect<A, E, R>) =>
    Effect.flatMap(SqlClient.SqlClient, (sql) =>
      sql.withTransaction(
        sql.onDialectOrElse({
          pg: () => postgresAgentTransaction(sql, agentKey, work),
          orElse: () => work,
        }),
      ),
    ),
);

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
