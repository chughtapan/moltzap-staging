/** @file Registered members and ordinary signed envelopes for encryption behavior tests. */

import { Effect, Either, Encoding, Redacted, Schema } from "effect";
import { exportJWK, GeneralEncrypt, generateKeyPair } from "jose";
import { generateKeyPairSync } from "node:crypto";
import { encodeCanonicalJson } from "../../encoding/canonical-json.js";
import {
  AgentId,
  AgentName,
  PrincipalId,
  type VerifiedAgentIdentityCard,
} from "../agent-cards.js";
import {
  type AgentPublicKey,
  AgentSigningAuthority,
  x25519PublicJwk,
} from "../keys.js";
import {
  EnvelopeId,
  SealedBody,
  SignedMessage,
  type VerifiedSignedMessage,
} from "../message-security.js";
import {
  AgentIdentityCardIssuedAt,
  makeVerifiedCard,
} from "./authentication-fixtures.js";

/**
 * Building fixtures dominates these tests: every group member costs an
 * Ed25519 key, two PKCS#8 imports, and an issued AgentIdentityCard, and the largest
 * groups have 129 members and seal about 256 KB to 128 recipients. Loaded CI
 * hosts need more than the default five seconds.
 */
export const KEY_AGREEMENT_HEAVY_TIMEOUT_MS = 60_000;

/** How many members `makeGroup` builds, and tests open as, at once. */
export const FIXTURE_CONCURRENCY = 8;

/** A short text plaintext that the tests seal unless they need other bytes. */
export const plaintext = new TextEncoder().encode("sealed outer body");

/** A registered agent: its verified AgentIdentityCard and its signing authority. */
export interface Member {
  readonly identityCard: VerifiedAgentIdentityCard;
  readonly authority: AgentSigningAuthority;
}

const identifier = (prefix: string, byte: number): string =>
  `${prefix}${Encoding.encodeBase64Url(new Uint8Array(16).fill(byte))}`;

const makeAuthority = () =>
  AgentSigningAuthority.fromPkcs8(
    Redacted.make(
      generateKeyPairSync("ed25519").privateKey.export({
        format: "pem",
        type: "pkcs8",
      }),
    ),
  );

/**
 * Issues an AgentIdentityCard whose identifiers all repeat `byte`, so two cards with
 * the same byte share an AgentId.
 *
 * @param registrySigningAuthority Registry that signs the card.
 * @param byte Byte repeated through the AgentId, PrincipalId, and name.
 * @param publicKey Ed25519 key the card carries.
 * @returns The verified card.
 */
export const issueCard = (
  registrySigningAuthority: AgentSigningAuthority,
  byte: number,
  publicKey: AgentPublicKey,
) =>
  makeVerifiedCard({
    agentId: Schema.decodeUnknownSync(AgentId)(identifier("agt_", byte)),
    principalId: Schema.decodeUnknownSync(PrincipalId)(
      identifier("prn_", byte),
    ),
    agentName: Schema.decodeUnknownSync(AgentName)(`member-${byte}`),
    publicKey,
    issuedAt: Schema.decodeUnknownSync(AgentIdentityCardIssuedAt)(
      "2026-10-05T12:00:00Z",
    ),
    registrySigningAuthority,
  });

/**
 * Generates a fresh authority and has the Registry issue its AgentIdentityCard, with
 * `byte` repeated through the card's identifiers.
 */
export const makeMember = (
  registrySigningAuthority: AgentSigningAuthority,
  byte: number,
) =>
  Effect.gen(function* () {
    const authority = yield* makeAuthority();
    const identityCard = yield* issueCard(
      registrySigningAuthority,
      byte,
      AgentSigningAuthority.publicKey(authority),
    );
    const member: Member = { identityCard, authority };
    return member;
  }).pipe(Effect.withSpan("makeMember"));

/**
 * Builds a sender, a peer, an outsider, and a recipient list that starts with
 * the sender, then the peer, then further members up to `recipientCount`.
 * The identifier bytes increase along the list, so it is in canonical order.
 *
 * @param recipientCount Number of recipients, sender included.
 * @returns The named members, the recipient list, and the Registry.
 */
export const makeGroup = (recipientCount: number) =>
  Effect.gen(function* () {
    const registrySigningAuthority = yield* makeAuthority();
    const sender = yield* makeMember(registrySigningAuthority, 1);
    const peer = yield* makeMember(registrySigningAuthority, 2);
    const outsider = yield* makeMember(registrySigningAuthority, 255);
    const others = yield* Effect.all(
      Array.from(Array(Math.max(recipientCount - 2, 0)).keys(), (index) =>
        makeMember(registrySigningAuthority, index + 3),
      ),
      { concurrency: FIXTURE_CONCURRENCY },
    );
    const recipients = [sender, peer, ...others].slice(0, recipientCount);
    return { sender, peer, outsider, recipients, registrySigningAuthority };
  }).pipe(Effect.withSpan("makeGroup"));

/** The members and Registry that `makeGroup` builds. */
export type Group = Effect.Effect.Success<ReturnType<typeof makeGroup>>;

/**
 * The EnvelopeId whose 16 bytes all repeat `byte`. `sealFrom` and `signBody`
 * both default to byte 1, so a test that names no EnvelopeId seals and signs
 * under the same one.
 */
export const envelopeIdOf = (byte: number) =>
  Schema.decodeUnknownSync(EnvelopeId)(identifier("env_", byte));

/**
 * Seals `bytes` from `sender` to the recipients' AgentCards.
 *
 * @param sender Sender whose AgentId the header binds.
 * @param recipients Recipients in the order the sender lists their cards.
 * @param bytes Plaintext to seal.
 * @param envelopeIdByte Byte repeated through the EnvelopeId the header binds.
 * @returns The sealed body.
 */
export const sealFrom = (
  sender: Member,
  recipients: readonly Member[],
  bytes: Uint8Array,
  envelopeIdByte = 1,
) =>
  SealedBody.seal({
    senderAgentId: sender.identityCard.agentId,
    recipientIdentityCards: recipients.map((member) => member.identityCard),
    envelopeId: envelopeIdOf(envelopeIdByte),
    plaintext: bytes,
  });

/**
 * Signs `body` from `signer` to the recipients.
 *
 * @param signer Member that signs.
 * @param recipients Recipients the SignedMessage names.
 * @param body Body bytes, sealed or not.
 * @param envelopeIdByte Byte repeated through the EnvelopeId.
 * @returns The verified SignedMessage.
 */
export const signBody = (
  signer: Member,
  recipients: readonly Member[],
  body: Uint8Array,
  envelopeIdByte = 1,
) =>
  SignedMessage.sign({
    identityCard: signer.identityCard,
    signingAuthority: signer.authority,
    recipientAgentIds: new Set(
      recipients.map((member) => member.identityCard.agentId),
    ),
    envelopeId: envelopeIdOf(envelopeIdByte),
    body,
  });

/**
 * Opens `signedMessage` as `member`, with its own card and authority.
 *
 * @param member Local agent.
 * @param signedMessage Verified SignedMessage carrying a sealed body.
 * @returns The plaintext.
 */
export const openAs = (member: Member, signedMessage: VerifiedSignedMessage) =>
  SealedBody.open({
    identityCard: member.identityCard,
    signingAuthority: member.authority,
    signedMessage,
  });

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

/**
 * Seals `plaintext` to the sender and peer with one ephemeral key named in
 * the protected header, spelled as seal spells a single-recipient header, then
 * drops the per-recipient copies jose adds.
 *
 * @param sender Sender and first recipient.
 * @param peer Second recipient.
 * @returns Canonical bytes of a JWE that jose opens but SealedBody refuses.
 */
export const sealWithSharedEphemeralKey = (sender: Member, peer: Member) =>
  Effect.gen(function* () {
    const ephemeral = yield* Effect.tryPromise({
      try: () =>
        generateKeyPair("ECDH-ES+A256KW", { crv: "X25519", extractable: true }),
      catch: () => new Error("ephemeral key generation failed"),
    });
    const ephemeralPublicKey = yield* Effect.tryPromise({
      try: () => exportJWK(ephemeral.publicKey),
      catch: () => new Error("ephemeral key export failed"),
    });
    const sharedEphemeralKey = { epk: ephemeral.privateKey };
    const { saltedPlaintext, commitment } = yield* commitTo(
      crypto.getRandomValues(new Uint8Array(32)),
      plaintext,
    );
    const representation = yield* Effect.tryPromise({
      try: () =>
        new GeneralEncrypt(saltedPlaintext)
          .setProtectedHeader({
            alg: "ECDH-ES+A256KW",
            enc: "A256GCM",
            "org.social-harness/commitment": commitment,
            "org.social-harness/envelope-id": envelopeIdOf(1),
            "org.social-harness/sender": sender.identityCard.agentId,
            epk: {
              x: ephemeralPublicKey.x,
              crv: ephemeralPublicKey.crv,
              kty: ephemeralPublicKey.kty,
            },
          })
          .addRecipient(
            Either.getOrThrow(x25519PublicJwk(sender.identityCard.publicKey)),
          )
          .setKeyManagementParameters(sharedEphemeralKey)
          .addRecipient(
            Either.getOrThrow(x25519PublicJwk(peer.identityCard.publicKey)),
          )
          .setKeyManagementParameters(sharedEphemeralKey)
          .encrypt(),
      catch: () => new Error("shared-key sealing failed"),
    });
    return yield* encodeCanonicalJson({
      ...representation,
      recipients: representation.recipients.map((recipient) => ({
        encrypted_key: recipient.encrypted_key,
      })),
    });
  }).pipe(Effect.withSpan("sealWithSharedEphemeralKey"));
