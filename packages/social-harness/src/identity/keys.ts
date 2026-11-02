/** @file Ed25519 keys, private signing authority and shared General JWS representation. */

import { ed25519 } from "@noble/curves/ed25519.js";
import {
  Data,
  Effect,
  Either,
  Encoding,
  Option,
  Redacted,
  Schema,
} from "effect";
import {
  calculateJwkThumbprintUri,
  type CryptoKey,
  exportJWK,
  importJWK,
  importPKCS8,
  type JWK,
} from "jose";
import { exactStruct } from "../encoding/canonical-json.js";
import { decodeCanonicalBase64Url } from "../encoding/identifier.js";
import { type BoundedCache, makeBoundedCache } from "./bounded-cache.js";

const PUBLIC_KEY_BYTE_LENGTH = 32;
const SIGNATURE_BYTE_LENGTH = 64;

const FIELD_MODULUS = Uint8Array.from([
  0xed, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
  0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
  0xff, 0xff, 0xff, 0xff, 0xff, 0x7f,
]);

const SCALAR_ORDER = Uint8Array.from([
  0xed, 0xd3, 0xf5, 0x5c, 0x1a, 0x63, 0x12, 0x58, 0xd6, 0x9c, 0xf7, 0xa2, 0xde,
  0xf9, 0xde, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00, 0x00, 0x10,
]);

const bytesFromHex = (value: string): Uint8Array =>
  new Uint8Array(Buffer.from(value, "hex"));

const SMALL_ORDER_POINTS = Object.freeze([
  bytesFromHex("00".repeat(32)),
  bytesFromHex(`01${"00".repeat(31)}`),
  bytesFromHex(
    "26e8958fc2b227b045c3f489f2ef98f0d5dfac05d3c63339b13802886d53fc05",
  ),
  bytesFromHex(
    "c7176a703d4dd84fba3c0b760d10670f2a2053fa2c39ccc64ec7fd7792ac037a",
  ),
  bytesFromHex(`ec${"ff".repeat(30)}7f`),
  bytesFromHex(`ed${"ff".repeat(30)}7f`),
  bytesFromHex(`ee${"ff".repeat(30)}7f`),
]);

const isLessThanLittleEndian = (
  value: Uint8Array,
  upperBound: Uint8Array,
): boolean => {
  if (value.byteLength !== upperBound.byteLength) {
    return false;
  }
  for (let index = value.byteLength - 1; index >= 0; index -= 1) {
    const valueByte = value[index];
    const boundByte = upperBound[index];
    if (valueByte === undefined || boundByte === undefined) {
      return false;
    }
    if (valueByte !== boundByte) {
      return valueByte < boundByte;
    }
  }
  return false;
};

const hasCanonicalPointEncoding = (value: Uint8Array): boolean => {
  if (value.byteLength !== PUBLIC_KEY_BYTE_LENGTH) {
    return false;
  }
  const coordinate = new Uint8Array(value);
  const finalByte = coordinate[PUBLIC_KEY_BYTE_LENGTH - 1];
  if (finalByte === undefined) {
    return false;
  }
  coordinate[PUBLIC_KEY_BYTE_LENGTH - 1] = finalByte & 0x7f;
  return isLessThanLittleEndian(coordinate, FIELD_MODULUS);
};

const encodesSmallOrderPoint = (value: Uint8Array): boolean =>
  SMALL_ORDER_POINTS.some((point) => {
    for (let index = 0; index < PUBLIC_KEY_BYTE_LENGTH; index += 1) {
      const valueByte = value[index];
      const pointByte = point[index];
      if (valueByte === undefined || pointByte === undefined) {
        return false;
      }
      const mask = index === PUBLIC_KEY_BYTE_LENGTH - 1 ? 0x7f : 0xff;
      if ((valueByte & mask) !== pointByte) {
        return false;
      }
    }
    return true;
  });

const hasAcceptedPublicKeyEncoding = (value: Uint8Array): boolean =>
  hasCanonicalPointEncoding(value) && !encodesSmallOrderPoint(value);

const decodeAcceptedPublicKey = (value: string): Uint8Array | undefined => {
  const bytes = decodeCanonicalBase64Url(value);
  return bytes !== undefined && hasAcceptedPublicKeyEncoding(bytes)
    ? bytes
    : undefined;
};

/**
 * Rejects alternate Ed25519 point and scalar encodings before cryptographic
 * verification. The public-key schema separately rejects small-order keys.
 */
export const hasCanonicalEd25519SignatureEncoding = (
  signature: Uint8Array,
): boolean => {
  if (signature.byteLength !== SIGNATURE_BYTE_LENGTH) {
    return false;
  }
  const encodedPoint = signature.subarray(0, PUBLIC_KEY_BYTE_LENGTH);
  const scalar = signature.subarray(PUBLIC_KEY_BYTE_LENGTH);
  return (
    hasCanonicalPointEncoding(encodedPoint) &&
    isLessThanLittleEndian(scalar, SCALAR_ORDER)
  );
};

type PublicKeyValue = Readonly<{
  crv: "Ed25519";
  kty: "OKP";
  x: string;
}>;

const missingMember = Symbol("missingMember");

const hasExactPublicKeyNames = (value: object): boolean => {
  const keys = Reflect.ownKeys(value);
  const names = new Set(keys);
  return (
    keys.length === 3 && names.has("crv") && names.has("kty") && names.has("x")
  );
};

const readPublicKeyMember = (
  value: object,
  name: "crv" | "kty" | "x",
): unknown => {
  const descriptor = Object.getOwnPropertyDescriptor(value, name);
  if (
    descriptor === undefined ||
    descriptor.enumerable !== true ||
    !("value" in descriptor)
  ) {
    return missingMember;
  }
  return descriptor.value;
};

const hasPublicKeyMembers = (value: object): value is PublicKeyValue => {
  if (!hasExactPublicKeyNames(value)) {
    return false;
  }
  const crv = readPublicKeyMember(value, "crv");
  const kty = readPublicKeyMember(value, "kty");
  const x = readPublicKeyMember(value, "x");
  return crv === "Ed25519" && kty === "OKP" && typeof x === "string";
};

const isFrozenPublicKey = (value: unknown): value is PublicKeyValue => {
  try {
    return (
      typeof value === "object" &&
      value !== null &&
      Object.isFrozen(value) &&
      hasPublicKeyMembers(value)
    );
    // eslint-disable-next-line agent-code-guard/bare-catch -- A Schema predicate converts hostile reflection into ordinary validation failure.
  } catch {
    return false;
  }
};

const publicKeyValueSchema = Schema.declare(isFrozenPublicKey, {
  identifier: "Ed25519PublicKeyValue",
});

const publicKeyRepresentation = Schema.Struct({
  crv: Schema.Literal("Ed25519"),
  kty: Schema.Literal("OKP"),
  x: Schema.String.pipe(
    Schema.filter((value) => decodeAcceptedPublicKey(value) !== undefined, {
      identifier: "Ed25519PublicKeyCoordinate",
      description: "Canonical non-small-order Ed25519 public-key coordinate",
    }),
  ),
}).annotations({
  identifier: "Ed25519PublicKeyRepresentation",
  parseOptions: {
    exact: true,
    onExcessProperty: "error",
  },
});

/** Exact immutable Ed25519 public JWK. */
// eslint-disable-next-line @typescript-eslint/naming-convention -- Effect Schemas use the public model's type name so callers import one vocabulary term.
export const AgentPublicKey = Schema.transform(
  publicKeyRepresentation,
  publicKeyValueSchema,
  {
    strict: true,
    decode: (value) =>
      Object.freeze({
        crv: value.crv,
        kty: value.kty,
        x: value.x,
      }),
    encode: (value) => ({
      crv: value.crv,
      kty: value.kty,
      x: value.x,
    }),
  },
).pipe(
  Schema.brand("AgentPublicKey"),
  Schema.annotations({
    identifier: "AgentPublicKey",
    description: "Exact immutable Ed25519 public JWK",
    parseOptions: {
      exact: true,
      onExcessProperty: "error",
    },
  }),
);

/** Validated immutable Ed25519 public JWK. */
// eslint-disable-next-line @typescript-eslint/no-redeclare -- Effect's same-named Schema and inferred type form one public boundary model.
export type AgentPublicKey = typeof AgentPublicKey.Type;

/**
 * Reports failure to read a key, to derive its standard thumbprint URI or
 * X25519 key, or to import it.
 */
export class Ed25519PublicKeyOperationError extends Data.TaggedError(
  "Ed25519PublicKeyOperationError",
) {}

const publicKeyOperationFailure = (): Ed25519PublicKeyOperationError =>
  new Ed25519PublicKeyOperationError();

/**
 * Most keys that each per-key cache below keeps: eight times
 * `envelope.ts → MAXIMUM_RECIPIENTS`, so the keys of eight full
 * conversations stay cached. Each entry is one short string or one imported
 * public `CryptoKey`.
 */
const PUBLIC_KEY_CACHE_ENTRIES = 1_024;

/**
 * Immutable keys share imported material by their canonical coordinate even
 * when separate card decodes produce separate objects. Failed derivations and
 * verification outcomes stay out of these bounded caches.
 */
const cachedPerPublicKey = <A>(
  entries: BoundedCache<string, A>,
  publicKey: AgentPublicKey,
  derive: (
    jwk: PublicKeyValue,
  ) => Effect.Effect<A, Ed25519PublicKeyOperationError>,
): Effect.Effect<A, Ed25519PublicKeyOperationError> =>
  Effect.suspend(() => {
    const coordinate = publicKey.x;
    return Option.match(entries.get(coordinate), {
      onNone: () =>
        Effect.tap(derive(publicKey), (derived) => {
          entries.set(coordinate, derived);
        }),
      onSome: (cached) => Effect.succeed(cached),
    });
  });

const thumbprintUris = makeBoundedCache<string, string>(
  PUBLIC_KEY_CACHE_ENTRIES,
);

/**
 * Derives the RFC JWK thumbprint URI for a validated Ed25519 public key.
 *
 * @param publicKey Validated public key.
 * @returns The standard SHA-256 JWK thumbprint URI.
 * @failure Ed25519PublicKeyOperationError when JOSE cannot derive the URI.
 */
export const ed25519PublicKeyThumbprintUri = (
  publicKey: AgentPublicKey,
): Effect.Effect<string, Ed25519PublicKeyOperationError> =>
  cachedPerPublicKey(thumbprintUris, publicKey, (jwk) =>
    Effect.tryPromise({
      try: () => calculateJwkThumbprintUri(jwk, "sha256"),
      catch: publicKeyOperationFailure,
    }),
  );

const importPublicJwk = (
  jwk: PublicKeyValue | X25519PublicJwk,
  algorithm: string,
): Effect.Effect<CryptoKey, Ed25519PublicKeyOperationError> =>
  Effect.tryPromise({
    try: () => importJWK(jwk, algorithm),
    catch: publicKeyOperationFailure,
  });

const verificationKeys = makeBoundedCache<string, CryptoKey>(
  PUBLIC_KEY_CACHE_ENTRIES,
);

/** Imports a validated Ed25519 public key to verify signatures under it. */
export const ed25519VerificationKey = (
  publicKey: AgentPublicKey,
): Effect.Effect<CryptoKey, Ed25519PublicKeyOperationError> =>
  cachedPerPublicKey(verificationKeys, publicKey, (jwk) =>
    importPublicJwk(jwk, "Ed25519"),
  );

/** Selects key management for sealed bodies and import of the opening key. */
export const SEALED_BODY_KEY_MANAGEMENT_ALGORITHM = "ECDH-ES+A256KW";

/** X25519 public JWK of one agent: the key a sealed body is encrypted to. */
interface X25519PublicJwk {
  readonly crv: "X25519";
  readonly kty: "OKP";
  readonly x: string;
}

/**
 * Maps an Ed25519 public key to X25519 with RFC 7748. Fails for canonical
 * encodings that are not curve points. `fromPkcs8` derives its private half.
 */
export const x25519PublicJwk = (
  publicKey: PublicKeyValue,
): Either.Either<X25519PublicJwk, Ed25519PublicKeyOperationError> =>
  Either.try({
    try: (): X25519PublicJwk => ({
      crv: "X25519",
      kty: "OKP",
      x: Encoding.encodeBase64Url(
        ed25519.utils.toMontgomery(
          Encoding.decodeBase64Url(publicKey.x).pipe(Either.getOrThrow),
        ),
      ),
    }),
    catch: publicKeyOperationFailure,
  });

const sealingKeys = makeBoundedCache<string, CryptoKey>(
  PUBLIC_KEY_CACHE_ENTRIES,
);

/** Imports the `x25519PublicJwk` image of a validated key to seal bodies to. */
export const x25519SealingKey = (
  publicKey: AgentPublicKey,
): Effect.Effect<CryptoKey, Ed25519PublicKeyOperationError> =>
  cachedPerPublicKey(sealingKeys, publicKey, (jwk) =>
    Effect.flatMap(x25519PublicJwk(jwk), (agreementKey) =>
      importPublicJwk(agreementKey, SEALED_BODY_KEY_MANAGEMENT_ALGORITHM),
    ),
  );

declare const agentSigningAuthorityBrand: unique symbol;

/**
 * Opaque authority over one imported Ed25519 private key and the X25519
 * opening key derived from it.
 */
export interface AgentSigningAuthority {
  readonly [agentSigningAuthorityBrand]: "AgentSigningAuthority";
}

/**
 * The supplied private-key material cannot act as an Ed25519 signer or yield
 * its X25519 opening key. The public contract represents every such cause with
 * this one empty error.
 */
export class InvalidAgentPrivateKeyError extends Data.TaggedError(
  "InvalidAgentPrivateKeyError",
) {}

type AuthorityState = Readonly<{
  privateKey: CryptoKey;
  openingKey: CryptoKey;
  publicKey: AgentPublicKey;
}>;

const authorityState = new WeakMap<AgentSigningAuthority, AuthorityState>();

const getAuthorityState = (
  authority: AgentSigningAuthority,
): AuthorityState => {
  const state = authorityState.get(authority);
  // eslint-disable-next-line agent-code-guard/require-assertion-rationale -- The construction invariant inserts state before this opaque value escapes.
  return state!;
};

const invalidPrivateKey = (): InvalidAgentPrivateKeyError =>
  new InvalidAgentPrivateKeyError();

/** Length of the SHA-512 expansion whose first half is the X25519 secret. */
const EXPANDED_SECRET_BYTES = 64;

/**
 * Erases standalone seed/expansion buffers, including the RFC 8032 nonce
 * prefix. Erases only the view in larger shared buffers to preserve unrelated
 * data, such as other values in a Node Buffer pool.
 */
const erasableBytes = (bytes: Uint8Array): Uint8Array =>
  bytes.byteOffset === 0 && bytes.buffer.byteLength <= EXPANDED_SECRET_BYTES
    ? new Uint8Array(bytes.buffer)
    : bytes;

/**
 * Erases key bytes when the effect settles. JOSE requires base64url strings
 * that cannot be erased and remain until garbage collection.
 */
const zeroAfter = <A, E>(
  bytes: Uint8Array,
  effect: Effect.Effect<A, E>,
): Effect.Effect<A, E> =>
  effect.pipe(
    Effect.ensuring(
      Effect.sync(() => {
        erasableBytes(bytes).fill(0);
      }),
    ),
  );

/**
 * Derives the X25519 private key from the clamped first half of SHA-512 over
 * the Ed25519 seed. Import rejects a key pair whose halves disagree.
 */
const importFromSeed = (
  seed: Uint8Array,
  publicKey: AgentPublicKey,
): Effect.Effect<CryptoKey, InvalidAgentPrivateKeyError> =>
  Effect.gen(function* () {
    const agreementPublicKey = yield* Either.mapLeft(
      x25519PublicJwk(publicKey),
      invalidPrivateKey,
    );
    const secret = yield* Effect.try({
      try: () => ed25519.utils.toMontgomerySecret(seed),
      catch: invalidPrivateKey,
    });
    return yield* zeroAfter(
      secret,
      Effect.tryPromise({
        try: () =>
          importJWK(
            { ...agreementPublicKey, d: Encoding.encodeBase64Url(secret) },
            SEALED_BODY_KEY_MANAGEMENT_ALGORITHM,
            { extractable: false },
          ),
        catch: invalidPrivateKey,
      }),
    );
  });

/**
 * Decodes the seed from the private JWK exported from the PKCS#8 key, derives
 * the opening key against the Ed25519 public key of the same seed, and erases
 * the decoded seed.
 */
const importOpeningKey = (
  exportedKey: JWK,
  publicKey: AgentPublicKey,
): Effect.Effect<CryptoKey, InvalidAgentPrivateKeyError> =>
  Effect.gen(function* () {
    if (exportedKey.d === undefined) {
      return yield* new InvalidAgentPrivateKeyError();
    }
    const seed = yield* Either.mapLeft(
      Encoding.decodeBase64Url(exportedKey.d),
      invalidPrivateKey,
    );
    return yield* zeroAfter(seed, importFromSeed(seed, publicKey));
  });

/**
 * Uses an extractable import to derive the public JWK and opening seed, and
 * retains a separate non-extractable signing key.
 */
const fromPkcs8 = (
  pkcs8: Redacted.Redacted,
): Effect.Effect<AgentSigningAuthority, InvalidAgentPrivateKeyError> =>
  Effect.gen(function* () {
    const extractableKey = yield* Effect.tryPromise({
      try: () =>
        importPKCS8(Redacted.value(pkcs8), "Ed25519", {
          extractable: true,
        }),
      catch: invalidPrivateKey,
    });
    const exportedKey = yield* Effect.tryPromise({
      try: () => exportJWK(extractableKey),
      catch: invalidPrivateKey,
    });
    const publicKey = yield* Schema.decodeUnknown(AgentPublicKey)(
      {
        crv: exportedKey.crv,
        kty: exportedKey.kty,
        x: exportedKey.x,
      },
      {
        exact: true,
        onExcessProperty: "error",
      },
    ).pipe(Effect.mapError(invalidPrivateKey));
    const openingKey = yield* importOpeningKey(exportedKey, publicKey);

    const privateKey = yield* Effect.tryPromise({
      try: () =>
        importPKCS8(Redacted.value(pkcs8), "Ed25519", {
          extractable: false,
        }),
      catch: invalidPrivateKey,
    });
    // eslint-disable-next-line agent-code-guard/require-assertion-rationale -- This assertion is safe because this module alone constructs values after inserting their WeakMap state.
    const authority = Object.freeze({}) as AgentSigningAuthority;
    authorityState.set(authority, { privateKey, openingKey, publicKey });
    return authority;
  });

const publicKey = (authority: AgentSigningAuthority): AgentPublicKey =>
  getAuthorityState(authority).publicKey;

/**
 * Returns the non-extractable key to identity-owned signed-artifact modules.
 *
 * @param authority Authority whose private key is needed.
 * @returns The authority's non-extractable private key.
 */
export const agentSigningPrivateKey = (
  authority: AgentSigningAuthority,
): CryptoKey => getAuthorityState(authority).privateKey;

/**
 * Returns the non-extractable X25519 key to the identity-owned sealed-body
 * module.
 *
 * @param authority Authority whose opening key is needed.
 * @returns The authority's non-extractable X25519 opening key.
 */
export const agentOpeningPrivateKey = (
  authority: AgentSigningAuthority,
): CryptoKey => getAuthorityState(authority).openingKey;

/**
 * Imports redacted, unencrypted Ed25519 PKCS#8 text and retains non-extractable
 * signing and X25519 opening keys. Invalid material fails with
 * `InvalidAgentPrivateKeyError`. `publicKey` returns the immutable key used in
 * registration and signing. Private keys stay inaccessible.
 */
// eslint-disable-next-line @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- The approved Effect-style API uses one name for its opaque type and capability value.
export const AgentSigningAuthority = Object.freeze({
  fromPkcs8,
  publicKey,
});

const canonicalBase64Url = Schema.String.pipe(
  Schema.filter((value) => decodeCanonicalBase64Url(value) !== undefined),
);

const jwsSignatureRepresentation = exactStruct({
  protected: canonicalBase64Url,
  signature: Schema.String.pipe(
    Schema.filter((value) => {
      const bytes = decodeCanonicalBase64Url(value);
      return bytes !== undefined && hasCanonicalEd25519SignatureEncoding(bytes);
    }),
  ),
});

/** Exact General JWS with one canonical Ed25519 signature and no extra members. */
export const generalJwsRepresentation = exactStruct({
  payload: canonicalBase64Url,
  signatures: Schema.Tuple(jwsSignatureRepresentation),
});

type SingleSignatureJwsRepresentation = typeof generalJwsRepresentation.Type;

/** Retains immutable copies of every admitted JWS member. */
export const snapshotRepresentation = (
  representation: SingleSignatureJwsRepresentation,
): SingleSignatureJwsRepresentation =>
  Object.freeze({
    payload: representation.payload,
    signatures: Object.freeze([
      Object.freeze({
        protected: representation.signatures[0].protected,
        signature: representation.signatures[0].signature,
      }),
    ] as const),
  });
