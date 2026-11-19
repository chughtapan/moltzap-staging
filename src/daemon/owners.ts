/** @file Binds each owner to one agent, records that agent's card and names the daemon's agents from their cards. */
import { SqlClient } from "@effect/sql";
import { Data, Effect, Layer, Schema } from "effect";
import { randomBytes, randomUUID } from "node:crypto";
import { executeQuery, makeQueries } from "../database/index.js";
import {
  AgentCard,
  AgentId,
  AgentName,
  Identity,
  IdentityError,
  PrincipalId,
} from "../identity.js";

/** Authentication supplies both fields; email and host-provided agent selectors are excluded. */
export interface Owner {
  readonly issuer: string;
  readonly subject: string;
}

const cardRow = Schema.Struct({
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
  Record<"agent_cards", typeof cardRow.Type>;

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
export const getOrCreateOwnedAgentKey = Effect.fn("getOrCreateOwnedAgentKey")(
  function* (owner: Owner, agentName?: AgentName) {
    const sql = yield* SqlClient.SqlClient;
    const queries = yield* makeQueries<OwnershipTables>();
    const ownerBinding = () =>
      executeQuery(
        queries
          .selectFrom("owner_bindings")
          .selectAll()
          .where("issuer", "=", owner.issuer)
          .where("subject", "=", owner.subject),
      ).pipe(Effect.flatMap(Schema.decodeUnknown(Schema.Array(bindingRow))));
    const keyIfCardMatches = (agentKey: string) =>
      executeQuery(
        queries
          .selectFrom("agent_cards")
          .select("agent_name")
          .where("agent_key", "=", agentKey),
      ).pipe(
        Effect.filterOrFail(
          ([card]) =>
            card !== undefined &&
            (agentName === undefined || card.agent_name === agentName),
          () => new OwnershipError(),
        ),
        Effect.as(agentKey),
      );
    const existing = (yield* ownerBinding())[0];
    if (existing !== undefined) {
      return yield* keyIfCardMatches(existing.agent_key);
    }
    const candidateKey = randomUUID();
    const card = yield* Schema.decodeUnknown(cardRow)({
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
        const binding = (yield* ownerBinding())[0];
        if (binding === undefined) {
          return yield* new OwnershipError();
        }
        if (binding.agent_key === candidateKey) {
          yield* executeQuery(
            queries
              .insertInto("agent_cards")
              .values(card)
              .onConflict((conflict) =>
                conflict.column("agent_name").doNothing(),
              ),
          );
        }
        return yield* keyIfCardMatches(binding.agent_key);
      }),
    );
  },
);

/** The identity of the agent stored under `agentKey`, resolving names in the same database. */
export function identityLayer(
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
  return makeQueries<OwnershipTables>().pipe(
    Effect.flatMap((queries) =>
      executeQuery(
        queries
          .selectFrom("agent_cards")
          .select([
            "agent_id as agentId",
            "agent_name as agentName",
            "principal_id as principalId",
          ])
          .where(column, "=", value),
      ),
    ),
    Effect.flatMap(Schema.decodeUnknown(Schema.Array(AgentCard))),
    Effect.provideService(SqlClient.SqlClient, sql),
    Effect.catchTags({
      SqlError: () => new IdentityError({ reason: "persistence-failed" }),
      ParseError: () => new IdentityError({ reason: "persistence-failed" }),
    }),
  );
}
