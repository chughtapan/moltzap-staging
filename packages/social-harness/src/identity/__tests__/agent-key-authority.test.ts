/** @file Private Ed25519 authority import, opacity, derivation, and failure tests. */

import { Effect, Either, Encoding, Redacted, Schema } from "effect";
import * as fc from "fast-check";
import { importJWK } from "jose";
import { createPrivateKey, createPublicKey, type KeyObject } from "node:crypto";
import { expect, it } from "vitest";
import {
  agentOpeningPrivateKey,
  AgentPublicKey,
  AgentSigningAuthority,
  agentSigningPrivateKey,
  InvalidAgentPrivateKeyError,
  x25519PublicJwk,
} from "../keys.js";

const RFC_PRIVATE_KEY = `-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIJ1hsZ3v/VpguoRK9JLsLMREScVpezJpGXA7rAMcrn9g
-----END PRIVATE KEY-----`;
const RFC_PUBLIC_X = "11qYAYKxCrfVS_7TyWQHOg7hcvPapiMlrwIaaPcHURo";
const X25519_PRIVATE_KEY = `-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VuBCIEIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
-----END PRIVATE KEY-----`;
const PUBLIC_ONLY_KEY = createPublicKey(
  createPrivateKey(RFC_PRIVATE_KEY),
).export({
  type: "spki",
  format: "pem",
});
const PRIVATE_SENTINEL = "private-sentinel-must-not-escape";
const PKCS8_ED25519_PREFIX = Buffer.from(
  "302e020100300506032b657004220420",
  "hex",
);

/** The RFC 7748 curve25519 base point, u = 9. */
const X25519_BASE_POINT = Buffer.from(`09${"00".repeat(31)}`, "hex");

const pemFromDer = (der: Uint8Array): string => {
  const base64 = Buffer.from(der).toString("base64");
  return `-----BEGIN PRIVATE KEY-----\n${base64}\n-----END PRIVATE KEY-----`;
};

const publicJwk = (privateKey: KeyObject) =>
  createPublicKey(privateKey).export({ format: "jwk" });

const compareGeneratedSeedWithNode = (seed: Uint8Array) => {
  const der = Buffer.concat([PKCS8_ED25519_PREFIX, Buffer.from(seed)]);
  const expected = publicJwk(
    createPrivateKey({
      key: der,
      format: "der",
      type: "pkcs8",
    }),
  );

  return Effect.gen(function* () {
    const authority = yield* AgentSigningAuthority.fromPkcs8(
      Redacted.make(pemFromDer(der)),
    );
    expect(AgentSigningAuthority.publicKey(authority).x).toBe(expected.x);
  });
};

const pemFromSeed = (seed: Uint8Array): string =>
  pemFromDer(Buffer.concat([PKCS8_ED25519_PREFIX, seed]));

/**
 * Reads the opening key's X25519 public key as its scalar multiple of the
 * base point, which is the only way to observe a non-extractable key.
 *
 * @param authority Authority whose opening key is read.
 * @returns The X25519 public key as lowercase hex.
 */
const openingPublicKeyHex = (authority: AgentSigningAuthority) =>
  Effect.gen(function* () {
    const basePoint = yield* Effect.tryPromise({
      try: () =>
        crypto.subtle.importKey(
          "raw",
          X25519_BASE_POINT,
          { name: "X25519" },
          true,
          [],
        ),
      catch: () => new Error("base-point import failed"),
    });
    const publicKey = yield* Effect.tryPromise({
      try: () =>
        crypto.subtle.deriveBits(
          { name: "X25519", public: basePoint },
          agentOpeningPrivateKey(authority),
          256,
        ),
      catch: () => new Error("opening-key derivation failed"),
    });
    return Buffer.from(publicKey).toString("hex");
  });

it("imports an Ed25519 PKCS#8 key as an opaque AgentSigningAuthority", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const authority = yield* AgentSigningAuthority.fromPkcs8(
        Redacted.make(RFC_PRIVATE_KEY),
      );
      const publicKey = AgentSigningAuthority.publicKey(authority);
      const privateKey = agentSigningPrivateKey(authority);
      const payload = new TextEncoder().encode("identity-authority");
      const signature = yield* Effect.tryPromise({
        try: () => crypto.subtle.sign("Ed25519", privateKey, payload),
        catch: () => new Error("signing failed"),
      });
      const verifyingKey = yield* Effect.tryPromise({
        try: () => importJWK(publicKey, "Ed25519"),
        catch: () => new Error("public-key import failed"),
      });
      const verifies = yield* Effect.tryPromise({
        try: () =>
          crypto.subtle.verify("Ed25519", verifyingKey, signature, payload),
        catch: () => new Error("signature verification failed"),
      });

      expect(publicKey).toEqual({
        crv: "Ed25519",
        kty: "OKP",
        x: RFC_PUBLIC_X,
      });
      expect(verifies).toBe(true);
      expect(privateKey).toMatchObject({
        algorithm: { name: "Ed25519" },
        extractable: false,
        type: "private",
        usages: ["sign"],
      });
      expect(Reflect.set(publicKey, "x", "A".repeat(43))).toBe(false);
      expect(AgentSigningAuthority.publicKey(authority).x).toBe(RFC_PUBLIC_X);
    }),
  ));

it("matches Node's public-key derivation for generated Ed25519 seeds", () =>
  fc.assert(
    fc.asyncProperty(fc.uint8Array({ minLength: 32, maxLength: 32 }), (seed) =>
      Effect.runPromise(compareGeneratedSeedWithNode(seed)),
    ),
    { numRuns: 32 },
  ));

it("collapses unusable private keys to InvalidAgentPrivateKeyError without leaking input", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const encryptedKey = createPrivateKey(RFC_PRIVATE_KEY).export({
        type: "pkcs8",
        format: "pem",
        cipher: "aes-256-cbc",
        passphrase: "test-passphrase",
      });
      const inputs = [
        PRIVATE_SENTINEL,
        X25519_PRIVATE_KEY,
        PUBLIC_ONLY_KEY,
        encryptedKey,
        `-----BEGIN PUBLIC KEY-----
MCowBQYDK2VwAyEA${PRIVATE_SENTINEL}
-----END PUBLIC KEY-----`,
      ];

      for (const input of inputs) {
        const error = yield* AgentSigningAuthority.fromPkcs8(
          Redacted.make(input),
        ).pipe(Effect.flip);

        expect(error).toStrictEqual(new InvalidAgentPrivateKeyError());
        expect(JSON.stringify(error)).not.toContain(PRIVATE_SENTINEL);
        expect(String(error)).not.toContain(PRIVATE_SENTINEL);
      }
    }),
  ));

/**
 * Reads the X25519 image that `x25519PublicJwk` gives an Ed25519 public key.
 *
 * @param publicKey Validated Ed25519 public key.
 * @returns The X25519 public key as lowercase hex.
 */
const cardImageHex = (publicKey: AgentPublicKey) =>
  Buffer.from(
    Encoding.decodeBase64Url(
      Either.getOrThrow(x25519PublicJwk(publicKey)).x,
    ).pipe(Either.getOrThrow),
  ).toString("hex");

it("maps the RFC 7748 edwards25519 base point to the curve25519 base point", () => {
  const edwardsBasePoint = Schema.decodeUnknownSync(AgentPublicKey)({
    crv: "Ed25519",
    kty: "OKP",
    x: Encoding.encodeBase64Url(Buffer.from(`58${"66".repeat(31)}`, "hex")),
  });

  expect(x25519PublicJwk(edwardsBasePoint)).toStrictEqual(
    Either.right({
      crv: "X25519",
      kty: "OKP",
      x: Encoding.encodeBase64Url(X25519_BASE_POINT),
    }),
  );
});

it.each([
  {
    vector: "RFC 8032 TEST 1",
    seed: "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60",
    x25519PublicKey:
      "d85e07ec22b0ad881537c2f44d662d1a143cf830c57aca4305d85c7a90f6b62e",
  },
  {
    vector: "libsodium ed25519_convert",
    seed: "421151a459faeade3d247115f94aedae42318124095afabe4d1451a559faedee",
    x25519PublicKey:
      "f1814f0e8ff1043d8a44d25babff3cedcae6c22c3edaa48f857ae70de2baae50",
  },
])(
  "derives the $vector X25519 opening key and its AgentIdentityCard image",
  ({ seed, x25519PublicKey }) =>
    Effect.runPromise(
      Effect.gen(function* () {
        const authority = yield* AgentSigningAuthority.fromPkcs8(
          Redacted.make(pemFromSeed(Buffer.from(seed, "hex"))),
        );
        expect(yield* openingPublicKeyHex(authority)).toBe(x25519PublicKey);
        expect(cardImageHex(AgentSigningAuthority.publicKey(authority))).toBe(
          x25519PublicKey,
        );
      }),
    ),
);

it("derives a non-extractable opening key that matches the AgentIdentityCard key's X25519 image", () =>
  fc.assert(
    fc.asyncProperty(fc.uint8Array({ minLength: 32, maxLength: 32 }), (seed) =>
      Effect.runPromise(
        Effect.gen(function* () {
          const authority = yield* AgentSigningAuthority.fromPkcs8(
            Redacted.make(pemFromSeed(seed)),
          );
          expect(yield* openingPublicKeyHex(authority)).toBe(
            cardImageHex(AgentSigningAuthority.publicKey(authority)),
          );
          expect(agentOpeningPrivateKey(authority)).toMatchObject({
            algorithm: { name: "X25519" },
            extractable: false,
            type: "private",
            usages: ["deriveBits"],
          });
        }),
      ),
    ),
    { numRuns: 32 },
  ));
