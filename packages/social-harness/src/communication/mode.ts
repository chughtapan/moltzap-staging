/** @file Pins transport ownership before agents acquire their communication services. */
import { SqlClient } from "@effect/sql/SqlClient";
import { Effect } from "effect";
import { CommunicationError } from "./errors.js";
const unavailable = () =>
  new CommunicationError({ reason: "persistence-failed" });
const invalid = () => new CommunicationError({ reason: "membership-invalid" });
/** A database belongs to one transport profile, including its selected server identity. */
export function bindCommunicationMode(
  kind: "p2p" | "client-server",
  serverName = "",
) {
  return Effect.flatMap(SqlClient, (sql) =>
    sql.withTransaction(
      Effect.gen(function* () {
        yield* sql`INSERT INTO communication_mode(singleton,kind,server_name) VALUES(1,${kind},${serverName}) ON CONFLICT(singleton) DO NOTHING`;
        yield* sql.onDialectOrElse({
          pg: () =>
            sql`SELECT singleton FROM communication_mode WHERE singleton=1 FOR UPDATE`.pipe(
              Effect.asVoid,
            ),
          orElse: () => Effect.void,
        });
        if (kind === "client-server") {
          yield* sql`UPDATE communication_mode SET server_name=${serverName} WHERE singleton=1 AND kind=${kind} AND NOT EXISTS(SELECT 1 FROM message_sequences)`;
        }
        const [row] = yield* sql<{
          kind: string;
          serverName: string;
        }>`SELECT kind,server_name AS "serverName" FROM communication_mode WHERE singleton=1`;
        if (row?.kind !== kind || row.serverName !== serverName) {
          return yield* invalid();
        }
      }),
    ),
  ).pipe(
    Effect.mapError((error) =>
      error instanceof CommunicationError ? error : unavailable(),
    ),
    Effect.withSpan("bindCommunicationMode"),
  );
}
