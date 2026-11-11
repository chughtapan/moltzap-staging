/** @file Saves local registration and resolves verified agent identities. */

import type { SqlClient } from "@effect/sql";
import { Context, Data, Effect, Layer, Schema } from "effect";

import { executeQuery, makeQueries } from "../database/sql.js";
import { withAgentTransaction } from "../database/transactions.js";
import {
  decodeCanonical,
  encodeCanonical,
  sameBytes,
} from "../encoding/index.js";
import {
  type AgentCard,
  agentCardOf,
  AgentIdentityCard,
  type AgentName,
  type PrincipalId,
  type VerifiedAgentIdentityCard,
} from "./agent-cards.js";
import { type AgentPublicKey, AgentSigningAuthority } from "./keys.js";
import {
  type RegistrationId,
  Registry,
  type RegistryError,
} from "./registry.js";

/** Registration storage or peer resolution failed. `agentName` identifies a failed lookup. */
export class IdentityError extends Data.TaggedError("IdentityError")<{
  readonly agentName?: AgentName;
  readonly reason:
    | "persistence-failed"
    | "incompatible-daemon"
    | "dependency-unavailable"
    | "unknown-agent";
}> {}

const persistenceFailure = () =>
  new IdentityError({ reason: "persistence-failed" });
const representationFailure = () =>
  new IdentityError({ reason: "incompatible-daemon" });

const identityRow = Schema.Struct({
  agent_id: Schema.NonEmptyString,
  canonical_agent_card: Schema.Uint8ArrayFromSelf.pipe(
    Schema.filter((bytes) => bytes.length > 0),
  ),
});

interface IdentityBinding {
  readonly agentId: string;
  readonly canonicalAgentCard: Uint8Array;
}

type IdentityTables = Record<
  "identity_binding",
  typeof identityRow.Type & Record<"agent_key", string>
>;

const readIdentityBinding = (agentKey: string) =>
  withAgentTransaction(
    agentKey,
    Effect.gen(function* () {
      const queries = yield* makeQueries<IdentityTables>();
      const row = (yield* executeQuery(
        queries
          .selectFrom("identity_binding")
          .select(["agent_id", "canonical_agent_card"])
          .where("agent_key", "=", agentKey),
      ).pipe(Effect.mapError(persistenceFailure)))[0];
      if (row === undefined) {
        return undefined;
      }
      const decoded = yield* Schema.decodeUnknown(identityRow)(row).pipe(
        Effect.mapError(representationFailure),
      );
      return {
        agentId: decoded.agent_id,
        canonicalAgentCard: Uint8Array.from(decoded.canonical_agent_card),
      };
    }),
  ).pipe(
    Effect.catchTag("SqlError", () => persistenceFailure()),
    Effect.withSpan("readIdentityBinding"),
  );

/** An atomic retry accepts identical bytes and refuses replacement of the saved identity. */
function bindIdentity(agentKey: string, binding: IdentityBinding) {
  return withAgentTransaction(
    agentKey,
    Effect.gen(function* () {
      const existing = yield* readIdentityBinding(agentKey);
      if (existing !== undefined) {
        if (
          existing.agentId !== binding.agentId ||
          !sameBytes(existing.canonicalAgentCard, binding.canonicalAgentCard)
        ) {
          return yield* representationFailure();
        }
        return;
      }
      const queries = yield* makeQueries<IdentityTables>();
      yield* executeQuery(
        queries.insertInto("identity_binding").values({
          agent_key: agentKey,
          agent_id: binding.agentId,
          canonical_agent_card: Uint8Array.from(binding.canonicalAgentCard),
        }),
      ).pipe(Effect.mapError(persistenceFailure));
    }),
  ).pipe(
    Effect.catchTag("SqlError", () => persistenceFailure()),
    Effect.withSpan("bindIdentity"),
  );
}

function readSavedIdentity(input: {
  readonly agentKey: string;
  readonly registrySignerPublicKey: AgentPublicKey;
  readonly agentPublicKey: AgentPublicKey;
}): Effect.Effect<
  VerifiedAgentIdentityCard | undefined,
  IdentityError,
  SqlClient.SqlClient
> {
  return Effect.gen(function* () {
    const row = yield* readIdentityBinding(input.agentKey);
    if (row === undefined) {
      return undefined;
    }
    const encoded = yield* decodeCanonical(
      AgentIdentityCard,
      row.canonicalAgentCard,
    ).pipe(Effect.mapError(representationFailure));
    const card = yield* AgentIdentityCard.verify({
      identityCard: encoded,
      registrySignerPublicKey: input.registrySignerPublicKey,
    }).pipe(Effect.mapError(representationFailure));
    if (
      row.agentId !== card.agentId ||
      card.publicKey.x !== input.agentPublicKey.x
    ) {
      return yield* representationFailure();
    }
    return card;
  }).pipe(Effect.withSpan("readSavedIdentity"));
}

function saveIdentity(
  agentKey: string,
  card: VerifiedAgentIdentityCard,
): Effect.Effect<void, IdentityError, SqlClient.SqlClient> {
  return encodeCanonical(AgentIdentityCard, card).pipe(
    Effect.mapError(representationFailure),
    Effect.flatMap((canonicalAgentCard) =>
      bindIdentity(agentKey, { agentId: card.agentId, canonicalAgentCard }),
    ),
    Effect.asVoid,
  );
}

/** Callers can correct registration conflicts or replace a refused admission credential. */
class RegistrationError extends Schema.TaggedError<RegistrationError>()(
  "RegistrationError",
  {
    reason: Schema.Literal(
      "admission-refused",
      "name_taken",
      "key_already_registered",
      "idempotency_conflict",
      "dependency-unavailable",
    ),
  },
) {}

/** Reuse the same operation, principal, name and signing key when retrying registration. */
export interface RegistrationRequest {
  readonly operationId: RegistrationId;
  readonly principalId: PrincipalId;
  readonly agentName: AgentName;
}

interface IdentityService {
  /** This agent's registered identity. */
  readonly me: VerifiedAgentIdentityCard;
  /** Resolves names in input order and identifies the first name that fails. */
  readonly resolve: (
    names: readonly AgentName[],
  ) => Effect.Effect<readonly AgentCard[], IdentityError>;
}

/** Holds this agent's registered identity and resolves other agents to verified identities. */
export class Identity extends Context.Tag("SocialHarness/Identity")<
  Identity,
  IdentityService
>() {
  /**
   * Opens the saved identity, checked against the Registry signer and agent
   * key, or registers the agent and saves its card when none is saved.
   */
  static readonly layer = identityLayer;
}

interface IdentitySettings {
  readonly agentKey: string;
  readonly registrySignerPublicKey: AgentPublicKey;
  readonly signingAuthority: AgentSigningAuthority;
  readonly registration: RegistrationRequest;
}

function identityLayer(
  settings: IdentitySettings,
): Layer.Layer<
  Identity,
  IdentityError | RegistrationError,
  Registry | SqlClient.SqlClient
> {
  return Layer.effect(
    Identity,
    Effect.gen(function* () {
      const registry = yield* Registry;
      const saved = yield* readSavedIdentity({
        agentKey: settings.agentKey,
        registrySignerPublicKey: settings.registrySignerPublicKey,
        agentPublicKey: AgentSigningAuthority.publicKey(
          settings.signingAuthority,
        ),
      });
      return Identity.of({
        me: saved ?? (yield* register(settings, registry)),
        resolve: (names) =>
          Effect.forEach(names, (name) => resolveAgent(registry, name), {
            concurrency: 1,
          }),
      });
    }),
  );
}

function resolveAgent(
  registry: Context.Tag.Service<typeof Registry>,
  agentName: AgentName,
) {
  return registry.lookup({ agentName }).pipe(
    Effect.mapError(() => resolveFailure(agentName)),
    Effect.flatMap((result) =>
      result.kind === "found"
        ? Effect.succeed(agentCardOf(result.identityCard))
        : Effect.fail(
            new IdentityError({ reason: "unknown-agent", agentName }),
          ),
    ),
  );
}

function registrationFailure(error: RegistryError): RegistrationError {
  return new RegistrationError({
    reason:
      error.reason === "admission-refused"
        ? "admission-refused"
        : "dependency-unavailable",
  });
}

function resolveFailure(agentName: AgentName): IdentityError {
  return new IdentityError({ reason: "dependency-unavailable", agentName });
}

/** Registers with the same operation, principal, name and key on every attempt, so a retry is safe. */
function register(
  settings: IdentitySettings,
  registry: Context.Tag.Service<typeof Registry>,
) {
  return Effect.gen(function* () {
    const result = yield* registry
      .register({
        request: settings.registration,
        signingAuthority: settings.signingAuthority,
      })
      .pipe(Effect.mapError(registrationFailure));
    if (result.kind !== "registered") {
      return yield* new RegistrationError({ reason: result.kind });
    }
    yield* saveIdentity(settings.agentKey, result.identityCard);
    return result.identityCard;
  });
}
