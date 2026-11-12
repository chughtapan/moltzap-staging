/** @file Seals persisted credentials to their owning record using authenticated encryption. */
import { SqlClient } from "@effect/sql";
import { Data, Effect, Redacted, Schema } from "effect";
import { flattenedDecrypt, FlattenedEncrypt } from "jose";

const sealedCredential = Schema.parseJson(
  Schema.Struct({
    protected: Schema.String,
    iv: Schema.String,
    ciphertext: Schema.String,
    tag: Schema.String,
    aad: Schema.String,
  }),
);

/**
 * Invalid unlock material or changed ciphertext never exposes plaintext or provider errors.
 * Exported so dependent declarations can name the inferred Effect failure.
 * @public
 */
export class StoredCredentialError extends Data.TaggedError(
  "StoredCredentialError",
) {}

/** Encryption binds ciphertext to an immutable record key, preventing cross-record substitution. */
export const sealCredential = (
  secret: Redacted.Redacted,
  recordKey: string,
  unlockKey: Redacted.Redacted<Uint8Array>,
) =>
  Effect.tryPromise({
    try: () =>
      new FlattenedEncrypt(new TextEncoder().encode(Redacted.value(secret)))
        .setProtectedHeader({ alg: "dir", enc: "A256GCM" })
        .setAdditionalAuthenticatedData(new TextEncoder().encode(recordKey))
        .encrypt(Redacted.value(unlockKey)),
    catch: () => new StoredCredentialError(),
  }).pipe(Effect.map((value) => JSON.stringify(value)));

/** Decryption accepts only the stored cipher suite and the requested record binding. */
export const openCredential = Effect.fn("openCredential")(function* (
  sealed: string,
  recordKey: string,
  unlockKey: Redacted.Redacted<Uint8Array>,
) {
  const encoded = yield* Schema.decodeUnknown(sealedCredential)(sealed).pipe(
    Effect.catchTag("ParseError", () =>
      Effect.fail(new StoredCredentialError()),
    ),
  );
  const result = yield* Effect.tryPromise({
    try: () =>
      flattenedDecrypt(encoded, Redacted.value(unlockKey), {
        keyManagementAlgorithms: ["dir"],
        contentEncryptionAlgorithms: ["A256GCM"],
      }),
    catch: () => new StoredCredentialError(),
  });
  if (
    result.additionalAuthenticatedData === undefined ||
    !Buffer.from(result.additionalAuthenticatedData).equals(
      Buffer.from(recordKey),
    )
  ) {
    return yield* new StoredCredentialError();
  }
  return yield* Effect.try({
    try: () =>
      Redacted.make(
        new TextDecoder("utf-8", { fatal: true }).decode(result.plaintext),
      ),
    catch: () => new StoredCredentialError(),
  });
}, Effect.withSpan("openCredential"));

const verifierRecord = "database/encryption";
const verifierPlaintext = "org.social-harness/database-key/v1";

/** Database verification failures carry no ciphertext, plaintext or SQL details. */
class DatabaseKeyError extends Data.TaggedError("DatabaseKeyError")<{
  readonly reason: "missing" | "key" | "populated";
}> {}

const readVerifier = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{
    readonly verifier: string;
  }>`SELECT verifier FROM database_encryption WHERE singleton=1`;
  return rows[0];
});

/** The verifier proves the supplied key belongs to this database without changing any record. */
export const verifyDatabaseKey = Effect.fn("verifyDatabaseKey")(function* (
  unlockKey: Redacted.Redacted<Uint8Array>,
) {
  const row = yield* readVerifier;
  if (row === undefined) {
    return yield* new DatabaseKeyError({ reason: "missing" });
  }
  const plaintext = yield* openCredential(
    row.verifier,
    verifierRecord,
    unlockKey,
  ).pipe(
    Effect.catchTag(
      "StoredCredentialError",
      () => new DatabaseKeyError({ reason: "key" }),
    ),
  );
  if (Redacted.value(plaintext) !== verifierPlaintext) {
    return yield* new DatabaseKeyError({ reason: "key" });
  }
});

/** Deployment initializes only empty databases; an existing verifier is always checked unchanged. */
export const initializeDatabaseKey = Effect.fn("initializeDatabaseKey")(
  function* (unlockKey: Redacted.Redacted<Uint8Array>) {
    if ((yield* readVerifier) !== undefined) {
      return yield* verifyDatabaseKey(unlockKey);
    }
    const sql = yield* SqlClient.SqlClient;
    const [owners] = yield* sql<{
      readonly count: number;
    }>`SELECT COUNT(*) AS count FROM owner_bindings`;
    const [profiles] = yield* sql<{
      readonly count: number;
    }>`SELECT COUNT(*) AS count FROM agent_profiles`;
    if (Number(owners?.count) !== 0 || Number(profiles?.count) !== 0) {
      return yield* new DatabaseKeyError({ reason: "populated" });
    }
    const sealed = yield* sealCredential(
      Redacted.make(verifierPlaintext),
      verifierRecord,
      unlockKey,
    );
    yield* sql`INSERT INTO database_encryption (singleton,verifier) VALUES (1,${sealed})`;
  },
);
