/** @file Records which mode a database serves. */
import { SqlClient } from "@effect/sql/SqlClient";
import { Effect } from "effect";
import { CommunicationError } from "./errors.js";

/** Marks the database as serving `mode`. Fails with `membership-invalid` when it already serves another. */
export function claimMode(mode: string) {
  return Effect.flatMap(SqlClient, (sql) =>
    sql.withTransaction(
      Effect.gen(function* () {
        yield* sql`INSERT INTO communication_mode(singleton,kind) VALUES(1,${mode}) ON CONFLICT(singleton) DO NOTHING`;
        const [row] = yield* sql<{
          kind: string;
        }>`SELECT kind FROM communication_mode WHERE singleton=1`;
        if (row?.kind !== mode) {
          return yield* new CommunicationError({
            reason: "membership-invalid",
          });
        }
      }),
    ),
  ).pipe(
    Effect.catchTag(
      "SqlError",
      () => new CommunicationError({ reason: "persistence-failed" }),
    ),
    Effect.withSpan("claimMode"),
  );
}
