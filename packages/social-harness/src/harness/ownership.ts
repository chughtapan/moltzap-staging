/** @file Binds authenticated owners to stable registration inputs before network registration. */
import { SqlClient } from "@effect/sql";
import { Data, Effect, Redacted, Schema } from "effect";
import { exportPKCS8, generateKeyPair } from "jose";
import { randomBytes, randomUUID } from "node:crypto";
import { sealCredential } from "../database/secrets.js";
import { executeQuery, makeQueries } from "../database/sql.js";
import { withAgentTransaction } from "../database/transactions.js";
import { AgentName, PrincipalId } from "../identity/agent-cards.js";
import { RegistrationId } from "../identity/registry.js";

/** Authentication supplies both fields; email and client-provided agent selectors are excluded. */
export interface OwnerAccount {
  readonly issuer: string;
  readonly subject: string;
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

/** Persistence failures retain existing records and do not disclose keys or account claims. */
export class OwnershipError extends Data.TaggedError("OwnershipError") {}

/** Concurrent first connections converge on one profile with one durable registration identity. */
export const getOrCreateOwnedAgent = Effect.fn("getOrCreateOwnedAgent")(
  function* (owner: OwnerAccount, unlockKey: Redacted.Redacted<Uint8Array>) {
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
    const readProfile = (agentKey: string) =>
      withAgentTransaction(
        agentKey,
        executeQuery(
          queries
            .selectFrom("agent_profiles")
            .selectAll()
            .where("agent_key", "=", agentKey),
        ).pipe(
          Effect.flatMap((rows) => Schema.decodeUnknown(profileRow)(rows[0])),
        ),
      );
    const existing = (yield* lookup())[0];
    if (existing !== undefined) {
      return yield* readProfile(existing.agent_key);
    }
    const candidateKey = randomUUID();
    const name = `agent-${randomBytes(12).toString("hex")}`;
    const principal = `prn_${randomBytes(16).toString("base64url")}`;
    const operation = `opn_${randomBytes(16).toString("base64url")}`;
    const keys = yield* Effect.tryPromise(() =>
      generateKeyPair("EdDSA", { extractable: true }),
    );
    const privateKey = Redacted.make(
      yield* Effect.tryPromise(() => exportPKCS8(keys.privateKey)),
    );
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
        return yield* readProfile(binding.agent_key);
      }),
    );
  },
  Effect.withSpan("getOrCreateOwnedAgent"),
);
