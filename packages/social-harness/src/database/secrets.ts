/** @file Seals persisted credentials to their owning record using authenticated encryption. */
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
