/** @file Agent identifiers, names and cards, and the service that names the agents of a daemon. */

import { SqlClient } from "@effect/sql";
import { Context, Data, Effect, Layer, Schema } from "effect";
import { canonicalIdentifier } from "../encoding/index.js";

const IDENTIFIER_BYTE_LENGTH = 16;

/* eslint-disable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare --
 * Named Effect Schemas share their domain names with the nominal types they decode.
 */

/** Identifies one agent. The daemon mints it when it creates the agent. */
export const AgentId = canonicalIdentifier(
  "AgentId",
  "agt_",
  IDENTIFIER_BYTE_LENGTH,
);
/** Validated nominal value decoded by {@link AgentId}. */
export type AgentId = typeof AgentId.Type;

/** Opaque identity of the principal represented by an agent. */
export const PrincipalId = canonicalIdentifier(
  "PrincipalId",
  "prn_",
  IDENTIFIER_BYTE_LENGTH,
);
/** Validated nominal value decoded by {@link PrincipalId}. */
export type PrincipalId = typeof PrincipalId.Type;

/** The rule every agent name meets, as validation errors state it. */
export const AGENT_NAME_RULE =
  "3 to 32 lowercase letters, digits or single hyphens, starting and ending with a letter or digit";

/** The name other agents address an agent by, unique on its daemon. */
export const AgentName = Schema.String.pipe(
  Schema.minLength(3),
  Schema.maxLength(32),
  Schema.pattern(/^[a-z0-9]+(-[a-z0-9]+)*$/),
  Schema.brand("AgentName"),
  Schema.annotations({
    identifier: "AgentName",
    description: `an agent name: ${AGENT_NAME_RULE}`,
  }),
);
/** Validated nominal value decoded by {@link AgentName}. */
export type AgentName = typeof AgentName.Type;

/* eslint-enable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare --
 * Restore the general naming rules after the public Schema/type pairs.
 */

/**
 * Identifies an agent and the principal it represents. Messages, results and
 * policy decisions name their agents with this value.
 */
// eslint-disable-next-line @typescript-eslint/naming-convention -- Effect Schemas share the public domain name they decode.
export const AgentCard = Schema.Struct({
  agentId: AgentId,
  agentName: AgentName,
  principalId: PrincipalId,
}).annotations({ identifier: "AgentCard" });
/**
 * An agent's identifier, name and principal, as its daemon recorded them.
 * @useDeclaredType
 */
// eslint-disable-next-line @typescript-eslint/no-redeclare -- The same-named Schema and type form one boundary model.
export type AgentCard = typeof AgentCard.Type;

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
