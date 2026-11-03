/** @file Saves local registration and resolves verified agent identities. */

import { SqlClient } from "@effect/sql";
import {
  Context,
  Data,
  Effect,
  Layer,
  Option,
  type Redacted,
  Ref,
  Schema,
} from "effect";
import {
  decodeCanonical,
  encodeCanonical,
  sameBytes,
} from "../encoding/canonical-json.js";
import {
  type AgentCard,
  agentCardOf,
  AgentIdentityCard,
  type AgentName,
  type AgentPublicKey,
  AgentSigningAuthority,
  type PrincipalId,
  type VerifiedAgentIdentityCard,
} from "./agent-cards.js";
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

const readIdentityBinding = () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const row = (yield* sql
      .unsafe<Readonly<Record<string, unknown>>>(
        "SELECT agent_id, canonical_agent_card FROM identity_binding WHERE singleton = 1",
      )
      .pipe(Effect.mapError(persistenceFailure)))[0];
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
  }).pipe(Effect.withSpan("readIdentityBinding"));

/** An atomic retry accepts identical bytes and refuses replacement of the saved identity. */
function bindIdentity(binding: IdentityBinding) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    return yield* sql
      .withTransaction(
        Effect.gen(function* () {
          const existing = yield* readIdentityBinding();
          if (existing !== undefined) {
            if (
              existing.agentId !== binding.agentId ||
              !sameBytes(
                existing.canonicalAgentCard,
                binding.canonicalAgentCard,
              )
            ) {
              return yield* representationFailure();
            }
            return;
          }
          yield* sql
            .unsafe(
              "INSERT INTO identity_binding (singleton, agent_id, canonical_agent_card) VALUES (1, ?, ?)",
              [binding.agentId, Uint8Array.from(binding.canonicalAgentCard)],
            )
            .pipe(Effect.mapError(persistenceFailure));
        }),
      )
      .pipe(Effect.catchTag("SqlError", () => persistenceFailure()));
  }).pipe(Effect.withSpan("bindIdentity"));
}

function readSavedIdentity(input: {
  readonly registrySignerPublicKey: AgentPublicKey;
  readonly agentPublicKey: AgentPublicKey;
}): Effect.Effect<
  VerifiedAgentIdentityCard | undefined,
  IdentityError,
  SqlClient.SqlClient
> {
  return Effect.gen(function* () {
    const row = yield* readIdentityBinding();
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
  card: VerifiedAgentIdentityCard,
): Effect.Effect<void, IdentityError, SqlClient.SqlClient> {
  return encodeCanonical(AgentIdentityCard, card).pipe(
    Effect.mapError(representationFailure),
    Effect.flatMap((canonicalAgentCard) =>
      bindIdentity({ agentId: card.agentId, canonicalAgentCard }),
    ),
    Effect.asVoid,
  );
}

/** Callers can correct registration conflicts or replace a refused admission credential. */
export class RegistrationError extends Schema.TaggedError<RegistrationError>()(
  "RegistrationError",
  {
    reason: Schema.Literal(
      "admission-refused",
      "name_taken",
      "key_already_registered",
      "idempotency_conflict",
      "dependency-unavailable",
      "persistence-failed",
      "incompatible-daemon",
    ),
  },
) {}

/** Reuse the same operation, principal, name and signing key when retrying registration. */
export interface RegistrationRequest {
  readonly operationId: RegistrationId;
  readonly principalId: PrincipalId;
  readonly agentName: AgentName;
}

/** A saved identity is available before communication recovery finishes. */
export type IdentityState =
  | Readonly<{ kind: "unregistered" }>
  | Readonly<{ kind: "registered"; identityCard: VerifiedAgentIdentityCard }>;

interface IdentityService {
  /** Reports saved registration independently of communication startup. */
  readonly me: () => Effect.Effect<IdentityState>;
  /** Saves the verified card before returning. Retry an interrupted call with the same request. */
  readonly register: (
    request: RegistrationRequest,
  ) => Effect.Effect<VerifiedAgentIdentityCard, RegistrationError>;
  /** Resolves names in input order and identifies the first name that fails. */
  readonly resolve: (
    names: readonly AgentName[],
  ) => Effect.Effect<readonly AgentCard[], IdentityError>;
}

/** Reports saved registration and resolves other agents to verified identities. */
export class Identity extends Context.Tag("SocialHarness/Identity")<
  Identity,
  IdentityService
>() {
  /** Verifies saved registration against the configured Registry signer and agent key. */
  static readonly layer = identityLayer;
}

interface IdentitySettings {
  readonly registrySignerPublicKey: AgentPublicKey;
  readonly signingAuthority: AgentSigningAuthority;
  readonly admissionCredential: Effect.Effect<Redacted.Redacted, unknown>;
}

function identityLayer(
  settings: IdentitySettings,
): Layer.Layer<Identity, IdentityError, Registry | SqlClient.SqlClient> {
  return Layer.effect(
    Identity,
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql
        .unsafe(`CREATE TABLE IF NOT EXISTS identity_binding (
      singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
      agent_id TEXT NOT NULL,
      canonical_agent_card BLOB NOT NULL
    )`)
        .pipe(Effect.mapError(persistenceFailure));
      const current = yield* Ref.make(
        Option.fromNullable(
          yield* readSavedIdentity({
            registrySignerPublicKey: settings.registrySignerPublicKey,
            agentPublicKey: AgentSigningAuthority.publicKey(
              settings.signingAuthority,
            ),
          }),
        ),
      );
      return yield* makeIdentity(settings, current);
    }),
  );
}

function makeIdentity(
  settings: IdentitySettings,
  current: Ref.Ref<Option.Option<VerifiedAgentIdentityCard>>,
) {
  return Effect.gen(function* () {
    const registry = yield* Registry;
    const sql = yield* SqlClient.SqlClient;
    const gate = yield* Effect.makeSemaphore(1);
    return Identity.of({
      me: () =>
        Ref.get(current).pipe(
          Effect.map(
            (card): IdentityState =>
              Option.isSome(card)
                ? { kind: "registered", identityCard: card.value }
                : { kind: "unregistered" },
          ),
        ),
      register: (request) =>
        gate.withPermits(1)(
          registerLocal(request, settings, registry, current).pipe(
            Effect.provideService(SqlClient.SqlClient, sql),
          ),
        ),
      resolve: (names) =>
        Effect.forEach(names, (name) => resolveAgent(registry, name), {
          concurrency: 1,
        }),
    });
  });
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

const admissionFailure = () =>
  new RegistrationError({ reason: "incompatible-daemon" });

function registerLocal(
  request: RegistrationRequest,
  settings: IdentitySettings,
  registry: Context.Tag.Service<typeof Registry>,
  current: Ref.Ref<Option.Option<VerifiedAgentIdentityCard>>,
) {
  return Effect.gen(function* () {
    const admissionCredential = yield* settings.admissionCredential.pipe(
      Effect.mapError(admissionFailure),
    );
    const result = yield* registry
      .register({
        request: { ...request },
        admissionCredential,
        signingAuthority: settings.signingAuthority,
      })
      .pipe(Effect.mapError(registrationFailure));
    if (result.kind !== "registered") {
      return yield* new RegistrationError({ reason: result.kind });
    }
    const card = result.identityCard;
    yield* saveIdentity(card).pipe(
      Effect.catchTag("IdentityError", (error) =>
        Effect.fail(
          new RegistrationError({
            reason:
              error.reason === "persistence-failed"
                ? "persistence-failed"
                : "incompatible-daemon",
          }),
        ),
      ),
      Effect.tap(() =>
        Ref.update(current, (saved) =>
          Option.isSome(saved) ? saved : Option.some(card),
        ),
      ),
      Effect.uninterruptible,
    );
    return card;
  });
}
