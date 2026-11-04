/** @file Binds authenticated owners to stable registration inputs before network registration. */
import { SqlClient } from "@effect/sql";
import { Data, Effect, Redacted, Schema } from "effect";
import { exportPKCS8, generateKeyPair } from "jose";
import { randomBytes, randomUUID } from "node:crypto";
import { openCredential, sealCredential } from "../database/secrets.js";
import { executeQuery, makeQueries } from "../database/sql.js";
import { withAgentTransaction } from "../database/transactions.js";
import {
  AgentName,
  AgentSigningAuthority,
  PrincipalId,
} from "../identity/agent-cards.js";
import { RegistrationId } from "../identity/registry.js";

/** Authentication supplies both fields; email and client-provided agent selectors are excluded. */
export interface OwnerAccount {
  readonly issuer: string;
  readonly subject: string;
}

/** Local bearer access names the personal owner of a self-hosted database. */
export const localOwner = {
  issuer: "urn:social-harness:self-hosted",
  subject: "owner",
};

interface RegistrationSeed {
  readonly agentName: AgentName;
  readonly principalId: PrincipalId;
  readonly operationId: RegistrationId;
  readonly privateKey: Redacted.Redacted;
}

const profileRow = Schema.Struct({
  agent_key: Schema.UUID,
  agent_name: AgentName,
  principal_id: PrincipalId,
  registration_operation_id: RegistrationId,
  private_key: Schema.String,
});

const bindingRow = Schema.Struct({
  agent_key: Schema.UUID,
  issuer: Schema.String,
  subject: Schema.String,
});

type OwnershipTables = Record<"owner_bindings", typeof bindingRow.Type> &
  Record<"agent_profiles", typeof profileRow.Type>;

/**
 * Persistence failures retain existing records and do not disclose keys or account claims.
 * Exported so dependent declarations can name the inferred Effect failure.
 * @public
 */
export class OwnershipError extends Data.TaggedError("OwnershipError") {}

const freshKey = Effect.gen(function* () {
  const keys = yield* Effect.tryPromise(() =>
    generateKeyPair("EdDSA", { extractable: true }),
  );
  return Redacted.make(
    yield* Effect.tryPromise(() => exportPKCS8(keys.privateKey)),
  );
});

function readProfile(agentKey: string) {
  return Effect.gen(function* () {
    const queries = yield* makeQueries<OwnershipTables>();
    const rows = yield* executeQuery(
      queries
        .selectFrom("agent_profiles")
        .selectAll()
        .where("agent_key", "=", agentKey),
    );
    return yield* Schema.decodeUnknown(profileRow)(rows[0]);
  });
}

/** Reconfiguration may repeat the original seed, but cannot silently replace a saved identity. */
function verifySeed(
  profile: typeof profileRow.Type,
  unlockKey: Redacted.Redacted<Uint8Array>,
  seed?: RegistrationSeed,
) {
  if (seed === undefined) {
    return Effect.void;
  }
  return openCredential(profile.private_key, profile.agent_key, unlockKey).pipe(
    Effect.flatMap((key) => {
      const sameRegistration =
        profile.agent_name === seed.agentName &&
        profile.principal_id === seed.principalId &&
        profile.registration_operation_id === seed.operationId;
      return sameRegistration &&
        Redacted.value(key) === Redacted.value(seed.privateKey)
        ? Effect.void
        : Effect.fail(new OwnershipError());
    }),
  );
}

/** Concurrent first connections return one agent key with durable registration inputs. */
export const getOrCreateOwnedAgent = Effect.fn("getOrCreateOwnedAgent")(
  function* (
    owner: OwnerAccount,
    unlockKey: Redacted.Redacted<Uint8Array>,
    seed?: RegistrationSeed,
  ) {
    const sql = yield* SqlClient.SqlClient;
    const queries = yield* makeQueries<OwnershipTables>();
    const lookup = () =>
      executeQuery(
        queries
          .selectFrom("owner_bindings")
          .selectAll()
          .where("issuer", "=", owner.issuer)
          .where("subject", "=", owner.subject),
      ).pipe(Effect.flatMap(Schema.decodeUnknown(Schema.Array(bindingRow))));
    const ownedKey = (agentKey: string) =>
      withAgentTransaction(
        agentKey,
        readProfile(agentKey).pipe(
          Effect.tap((profile) => verifySeed(profile, unlockKey, seed)),
          Effect.as(agentKey),
        ),
      );
    const existing = (yield* lookup())[0];
    if (existing !== undefined) {
      return yield* ownedKey(existing.agent_key);
    }
    const candidateKey = randomUUID();
    const name = seed?.agentName ?? `agent-${randomBytes(12).toString("hex")}`;
    const principal =
      seed?.principalId ?? `prn_${randomBytes(16).toString("base64url")}`;
    const operation =
      seed?.operationId ?? `opn_${randomBytes(16).toString("base64url")}`;
    const privateKey = seed === undefined ? yield* freshKey : seed.privateKey;
    yield* AgentSigningAuthority.fromPkcs8(privateKey);
    const sealed = yield* sealCredential(privateKey, candidateKey, unlockKey);
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
        const binding = (yield* lookup())[0];
        if (binding === undefined) {
          return yield* new OwnershipError();
        }
        if (binding.agent_key === candidateKey) {
          yield* withAgentTransaction(
            candidateKey,
            executeQuery(
              queries.insertInto("agent_profiles").values({
                agent_key: candidateKey,
                agent_name: yield* Schema.decodeUnknown(AgentName)(name),
                principal_id:
                  yield* Schema.decodeUnknown(PrincipalId)(principal),
                registration_operation_id:
                  yield* Schema.decodeUnknown(RegistrationId)(operation),
                private_key: sealed,
              }),
            ),
          );
        }
        return yield* ownedKey(binding.agent_key);
      }),
    );
  },
  Effect.withSpan("getOrCreateOwnedAgent"),
);

/** The private catalog restores known agents without exposing their owners to host tools. */
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

/** Hydration uses the saved registration operation and signing key on every restart. */
export const loadAgentRegistration = Effect.fn("loadAgentRegistration")(
  function* (agentKey: string, unlockKey: Redacted.Redacted<Uint8Array>) {
    const profile = yield* withAgentTransaction(
      agentKey,
      readProfile(agentKey),
    );
    const key = yield* openCredential(profile.private_key, agentKey, unlockKey);
    return {
      signingAuthority: yield* AgentSigningAuthority.fromPkcs8(key),
      registration: {
        agentName: profile.agent_name,
        principalId: profile.principal_id,
        operationId: profile.registration_operation_id,
      },
    };
  },
);
