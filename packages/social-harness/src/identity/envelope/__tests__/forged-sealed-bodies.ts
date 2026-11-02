/**
 * @file Sealed bodies built by hand, as a dishonest sender could build them.
 * Each helper follows RFC 7516 and RFC 7518 directly with WebCrypto, so a test
 * controls the exact protected-header bytes and every content key.
 */

import { Effect, Encoding } from "effect";
import { exportJWK, generateKeyPair } from "jose";
import type { VerifiedAgentIdentityCard } from "../../agent.js";
import { encodeCanonicalJson } from "../../../encoding/canonical-json.js";
import { x25519PublicJwk } from "../../keys.js";

const KEY_MANAGEMENT_ALGORITHM = "ECDH-ES+A256KW";
const BLOCK_BYTES = 16;
const CONTENT_KEY_BYTES = 32;
const IV_BYTES = 12;

/** The GCM field reduction polynomial in the bit-reflected GHASH order. */
const GHASH_REDUCTION = 0xe1n << 120n;

const utf8Encoder = new TextEncoder();
const subtle = crypto.subtle;

/** Bytes in an ArrayBuffer, the form every WebCrypto input requires. */
type Bytes = Uint8Array<ArrayBuffer>;

const failure = (step: string) => () => new Error(`${step} failed`);

const concatenate = (...parts: readonly Uint8Array[]): Bytes => {
  const bytes = new Uint8Array(
    parts.reduce((total, part) => total + part.byteLength, 0),
  );
  let offset = 0;
  for (const part of parts) {
    bytes.set(part, offset);
    offset += part.byteLength;
  }
  return bytes;
};

const uint32 = (value: number): Bytes => {
  const bytes = new Uint8Array(4);
  new DataView(bytes.buffer).setUint32(0, value);
  return bytes;
};

/**
 * Computes the salted plaintext and its base64url SHA-256 commitment, as
 * `SealedBody.seal` does.
 *
 * @param salt Bytes placed before the plaintext; seal uses 32 random bytes.
 * @param plaintext Bytes the commitment covers after the salt.
 * @returns The salted plaintext and its commitment.
 */
export const commitTo = (salt: Uint8Array, plaintext: Uint8Array) =>
  Effect.tryPromise({
    try: () => subtle.digest("SHA-256", concatenate(salt, plaintext)),
    catch: failure("commitment digest"),
  }).pipe(
    Effect.map((digest) => ({
      saltedPlaintext: concatenate(salt, plaintext),
      commitment: Encoding.encodeBase64Url(new Uint8Array(digest)),
    })),
    Effect.withSpan("commitTo"),
  );

const importKey = (
  bytes: Bytes,
  algorithm: "AES-CBC" | "AES-GCM" | "AES-KW",
  usage: "encrypt" | "wrapKey",
) =>
  Effect.tryPromise({
    try: () => subtle.importKey("raw", bytes, algorithm, true, [usage]),
    catch: failure(`${algorithm} key import`),
  });

/**
 * Applies the Concat KDF of RFC 7518, section 4.6.2, for ECDH-ES+A256KW with
 * empty party information.
 *
 * @param sharedSecret X25519 agreement output.
 * @returns The 32-byte key-encryption key.
 */
const concatKdf = (sharedSecret: ArrayBuffer) => {
  const algorithm = utf8Encoder.encode(KEY_MANAGEMENT_ALGORITHM);
  return Effect.tryPromise({
    try: () =>
      subtle.digest(
        "SHA-256",
        concatenate(
          uint32(1),
          new Uint8Array(sharedSecret),
          uint32(algorithm.byteLength),
          algorithm,
          uint32(0),
          uint32(0),
          uint32(256),
        ),
      ),
    catch: failure("Concat KDF"),
  });
};

/**
 * Derives the ECDH-ES+A256KW key-encryption key for one recipient: X25519
 * agreement with a fresh ephemeral key, then the Concat KDF.
 *
 * @param recipient Recipient AgentIdentityCard.
 * @returns The key-encryption key and the ephemeral public JWK.
 */
const deriveKeyEncryptionKey = (recipient: VerifiedAgentIdentityCard) =>
  Effect.gen(function* () {
    const recipientJwk = yield* x25519PublicJwk(recipient.publicKey);
    const recipientKey = yield* Effect.tryPromise({
      try: () =>
        subtle.importKey(
          "jwk",
          { ...recipientJwk },
          { name: "X25519" },
          true,
          [],
        ),
      catch: failure("recipient key import"),
    });
    const ephemeral = yield* Effect.tryPromise({
      try: () =>
        generateKeyPair(KEY_MANAGEMENT_ALGORITHM, {
          crv: "X25519",
          extractable: true,
        }),
      catch: failure("ephemeral key generation"),
    });
    const sharedSecret = yield* Effect.tryPromise({
      try: () =>
        subtle.deriveBits(
          { name: "X25519", public: recipientKey },
          ephemeral.privateKey,
          256,
        ),
      catch: failure("key agreement"),
    });
    const keyEncryptionKey = yield* concatKdf(sharedSecret);
    const ephemeralJwk = yield* Effect.tryPromise({
      try: () => exportJWK(ephemeral.publicKey),
      catch: failure("ephemeral key export"),
    });
    return {
      keyEncryptionKey: new Uint8Array(keyEncryptionKey),
      epk: { crv: ephemeralJwk.crv, kty: ephemeralJwk.kty, x: ephemeralJwk.x },
    };
  });

/**
 * Wraps `contentKey` to one recipient as ECDH-ES+A256KW does: AES key wrap
 * under the derived key-encryption key.
 *
 * @param recipient Recipient AgentIdentityCard.
 * @param contentKey 32-byte content-encryption key.
 * @returns The recipient entry with its ephemeral public key.
 */
const wrapContentKey = (
  recipient: VerifiedAgentIdentityCard,
  contentKey: Bytes,
) =>
  Effect.gen(function* () {
    const { keyEncryptionKey, epk } = yield* deriveKeyEncryptionKey(recipient);
    const content = yield* importKey(contentKey, "AES-GCM", "encrypt");
    const wrapping = yield* importKey(keyEncryptionKey, "AES-KW", "wrapKey");
    const wrappedKey = yield* Effect.tryPromise({
      try: () => subtle.wrapKey("raw", content, wrapping, "AES-KW"),
      catch: failure("key wrap"),
    });
    return {
      encrypted_key: Encoding.encodeBase64Url(new Uint8Array(wrappedKey)),
      header: { epk },
    };
  });

interface ManualSealInput {
  readonly protectedHeaderText: string;
  readonly saltedPlaintext: Uint8Array;
  readonly recipients: readonly VerifiedAgentIdentityCard[];
}

/**
 * Seals one content key to every recipient under exactly the protected-header
 * text given, which may be any spelling a sender chooses.
 *
 * @param input Header text, salted plaintext, and two or more recipients in
 * canonical order.
 * @returns Canonical bytes of the General JWE.
 */
export const sealManually = (input: ManualSealInput) =>
  Effect.gen(function* () {
    const contentKey = crypto.getRandomValues(
      new Uint8Array(CONTENT_KEY_BYTES),
    );
    const iv = crypto.getRandomValues(new Uint8Array(IV_BYTES));
    const protectedHeader = Encoding.encodeBase64Url(input.protectedHeaderText);
    const key = yield* importKey(contentKey, "AES-GCM", "encrypt");
    const sealed = yield* Effect.tryPromise({
      try: () =>
        subtle.encrypt(
          {
            name: "AES-GCM",
            iv,
            additionalData: utf8Encoder.encode(protectedHeader),
          },
          key,
          Uint8Array.from(input.saltedPlaintext),
        ),
      catch: failure("content encryption"),
    });
    const recipients = yield* Effect.forEach(
      input.recipients,
      (recipient) => wrapContentKey(recipient, contentKey),
      { concurrency: 1 },
    );
    const ciphertextAndTag = new Uint8Array(sealed);
    return yield* encodeCanonicalJson({
      ciphertext: Encoding.encodeBase64Url(
        ciphertextAndTag.subarray(0, -BLOCK_BYTES),
      ),
      iv: Encoding.encodeBase64Url(iv),
      protected: protectedHeader,
      recipients,
      tag: Encoding.encodeBase64Url(ciphertextAndTag.subarray(-BLOCK_BYTES)),
    });
  }).pipe(Effect.withSpan("sealManually"));

const toBigInt = (bytes: Uint8Array): bigint =>
  bytes.reduce((value, byte) => (value << 8n) | BigInt(byte), 0n);

const toBlock = (value: bigint): Bytes =>
  Uint8Array.from(Array(BLOCK_BYTES).keys(), (index) =>
    Number((value >> BigInt(8 * (BLOCK_BYTES - 1 - index))) & 0xffn),
  );

/**
 * Multiplies in GF(2^128) as GHASH does (NIST SP 800-38D, algorithm 1).
 *
 * @param left First factor in GHASH bit order.
 * @param right Second factor in GHASH bit order.
 * @returns The product.
 */
const gfMultiply = (left: bigint, right: bigint): bigint => {
  let product = 0n;
  let shifted = right;
  for (let bit = 127n; bit >= 0n; bit -= 1n) {
    if ((left >> bit) & 1n) {
      product ^= shifted;
    }
    shifted = shifted & 1n ? (shifted >> 1n) ^ GHASH_REDUCTION : shifted >> 1n;
  }
  return product;
};

const gfPower = (base: bigint, exponent: bigint): bigint => {
  let result = 1n << 127n;
  let square = base;
  for (let remaining = exponent; remaining > 0n; remaining >>= 1n) {
    if (remaining & 1n) {
      result = gfMultiply(result, square);
    }
    square = gfMultiply(square, square);
  }
  return result;
};

const ghashBlocks = (bytes: Uint8Array): bigint[] =>
  Array.from(
    Array(Math.ceil(bytes.byteLength / BLOCK_BYTES)).keys(),
    (index) => {
      const block = new Uint8Array(BLOCK_BYTES);
      block.set(bytes.subarray(index * BLOCK_BYTES, (index + 1) * BLOCK_BYTES));
      return toBigInt(block);
    },
  );

/**
 * Encrypts one AES block: AES-CBC with a zero IV encrypts its first block
 * exactly as the raw cipher does.
 *
 * @param key 32-byte AES key.
 * @param block 16-byte block.
 * @returns The encrypted block as a field element.
 */
const encryptBlock = (key: Bytes, block: Bytes) =>
  Effect.gen(function* () {
    const cbcKey = yield* importKey(key, "AES-CBC", "encrypt");
    const encrypted = yield* Effect.tryPromise({
      try: () =>
        subtle.encrypt(
          { name: "AES-CBC", iv: new Uint8Array(BLOCK_BYTES) },
          cbcKey,
          block,
        ),
      catch: failure("block encryption"),
    });
    return toBigInt(new Uint8Array(encrypted).subarray(0, BLOCK_BYTES));
  });

interface CollisionInput {
  readonly firstKey: Bytes;
  readonly secondKey: Bytes;
  readonly iv: Bytes;
  readonly additionalData: Bytes;
  readonly ciphertext: Bytes;
}

/**
 * Rewrites the first ciphertext block so the A256GCM tag is the same under
 * both keys, the multi-key collision of Dodis et al. (2018). The tag is linear
 * in each ciphertext block, so one free block solves the equation.
 *
 * @param input Two content keys, the IV, the additional data, and a ciphertext
 * of at least one block whose first block is rewritten.
 * @returns The ciphertext and the tag that authenticates it under both keys.
 */
const collideTag = (input: CollisionInput) =>
  Effect.gen(function* () {
    const counterZero = concatenate(input.iv, uint32(1));
    const firstHash = yield* encryptBlock(
      input.firstKey,
      new Uint8Array(BLOCK_BYTES),
    );
    const secondHash = yield* encryptBlock(
      input.secondKey,
      new Uint8Array(BLOCK_BYTES),
    );
    const firstMask = yield* encryptBlock(input.firstKey, counterZero);
    const secondMask = yield* encryptBlock(input.secondKey, counterZero);
    const ciphertext = Uint8Array.from(input.ciphertext);
    const blocks = [
      ...ghashBlocks(input.additionalData),
      ...ghashBlocks(ciphertext),
      (BigInt(input.additionalData.byteLength * 8) << 64n) |
        BigInt(ciphertext.byteLength * 8),
    ];
    const freeIndex = Math.ceil(input.additionalData.byteLength / BLOCK_BYTES);
    const weight = (index: number) =>
      gfPower(firstHash, BigInt(blocks.length - index)) ^
      gfPower(secondHash, BigInt(blocks.length - index));
    const target = blocks.reduce(
      (sum, block, index) =>
        index === freeIndex ? sum : sum ^ gfMultiply(block, weight(index)),
      firstMask ^ secondMask,
    );
    const freeBlock = gfMultiply(
      target,
      gfPower(weight(freeIndex), (1n << 128n) - 2n),
    );
    ciphertext.set(toBlock(freeBlock));
    blocks[freeIndex] = freeBlock;
    const ghash = blocks.reduce(
      (accumulator, block) => gfMultiply(accumulator ^ block, firstHash),
      0n,
    );
    return { ciphertext, tag: toBlock(ghash ^ firstMask) };
  });

interface TwoKeyInput {
  readonly protectedHeaderText: string;
  readonly recipients: readonly [
    VerifiedAgentIdentityCard,
    VerifiedAgentIdentityCard,
  ];
  readonly ciphertextBytes: number;
}

/**
 * Builds one sealed body whose two entries wrap different content keys over a
 * ciphertext and tag that authenticate under both, so jose opens it to a
 * different plaintext for each recipient.
 *
 * @param input Header text, two recipients in canonical order, and the
 * ciphertext length (at least one block).
 * @returns Canonical bytes of the General JWE.
 */
export const sealTwoKeyCollision = (input: TwoKeyInput) =>
  Effect.gen(function* () {
    const firstKey = crypto.getRandomValues(new Uint8Array(CONTENT_KEY_BYTES));
    const secondKey = crypto.getRandomValues(new Uint8Array(CONTENT_KEY_BYTES));
    const iv = crypto.getRandomValues(new Uint8Array(IV_BYTES));
    const protectedHeader = Encoding.encodeBase64Url(input.protectedHeaderText);
    const { ciphertext, tag } = yield* collideTag({
      firstKey,
      secondKey,
      iv,
      additionalData: utf8Encoder.encode(protectedHeader),
      ciphertext: crypto.getRandomValues(new Uint8Array(input.ciphertextBytes)),
    });
    const recipients = [
      yield* wrapContentKey(input.recipients[0], firstKey),
      yield* wrapContentKey(input.recipients[1], secondKey),
    ];
    return yield* encodeCanonicalJson({
      ciphertext: Encoding.encodeBase64Url(ciphertext),
      iv: Encoding.encodeBase64Url(iv),
      protected: protectedHeader,
      recipients,
      tag: Encoding.encodeBase64Url(tag),
    });
  }).pipe(Effect.withSpan("sealTwoKeyCollision"));
