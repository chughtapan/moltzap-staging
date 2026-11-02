/** @file Ed25519 keys, signing authority, key agreement, and strict signature encodings. */

import { it } from "@effect/vitest";
import { ed25519 } from "@noble/curves/ed25519.js";
import { Effect, Either, Encoding, Redacted, Schema } from "effect";
import * as fc from "fast-check";
import { calculateJwkThumbprintUri, importJWK } from "jose";
import {
  createPrivateKey,
  createPublicKey,
  generateKeyPairSync,
  type KeyObject,
  sign as signWithPrivateKey,
} from "node:crypto";
import { describe, expect } from "vitest";
import {
  agentOpeningPrivateKey,
  AgentPublicKey,
  AgentSigningAuthority,
  agentSigningPrivateKey,
  ed25519PublicKeyThumbprintUri,
  hasCanonicalEd25519SignatureEncoding,
  InvalidAgentPrivateKeyError,
  x25519PublicJwk,
} from "./keys.js";

const RFC_PUBLIC_X = "11qYAYKxCrfVS_7TyWQHOg7hcvPapiMlrwIaaPcHURo";
const RFC_THUMBPRINT_URI =
  "urn:ietf:params:oauth:jwk-thumbprint:sha-256:kPrK_qmxVWaYVA9wwBF6Iuo3vVzz7TxHCTwXBygrS4k";

const validPublicKey = {
  crv: "Ed25519",
  kty: "OKP",
  x: RFC_PUBLIC_X,
};

const smallOrderPointHex = [
  "00".repeat(32),
  `01${"00".repeat(31)}`,
  "26e8958fc2b227b045c3f489f2ef98f0d5dfac05d3c63339b13802886d53fc05",
  "c7176a703d4dd84fba3c0b760d10670f2a2053fa2c39ccc64ec7fd7792ac037a",
  `ec${"ff".repeat(30)}7f`,
  `ed${"ff".repeat(30)}7f`,
  `ee${"ff".repeat(30)}7f`,
] as const;

const encodePoint = (hex: string, highBit: boolean): string => {
  const bytes = Uint8Array.from(Buffer.from(hex, "hex"));
  const finalByte = bytes[31];
  if (highBit && finalByte !== undefined) {
    bytes[31] = finalByte | 0x80;
  }
  return Buffer.from(bytes).toString("base64url");
};

const invalidPublicKeys = [
  {
    description: "missing crv",
    representation: { kty: "OKP", x: RFC_PUBLIC_X },
  },
  {
    description: "wrong crv",
    representation: { ...validPublicKey, crv: "ed25519" },
  },
  {
    description: "wrong kty",
    representation: { ...validPublicKey, kty: "EC" },
  },
  {
    description: "short x",
    representation: { ...validPublicKey, x: "A".repeat(42) },
  },
  {
    description: "long x",
    representation: { ...validPublicKey, x: "A".repeat(44) },
  },
  {
    description: "padded x",
    representation: { ...validPublicKey, x: `${RFC_PUBLIC_X}=` },
  },
  {
    description: "standard alphabet",
    representation: { ...validPublicKey, x: `${"A".repeat(42)}/` },
  },
  {
    description: "whitespace",
    representation: { ...validPublicKey, x: `${"A".repeat(42)} ` },
  },
  {
    description: "nonzero trailing bits",
    representation: { ...validPublicKey, x: `${"A".repeat(42)}B` },
  },
  {
    description: "private material",
    representation: { ...validPublicKey, d: "A".repeat(43) },
  },
  {
    description: "algorithm",
    representation: { ...validPublicKey, alg: "Ed25519" },
  },
  {
    description: "key identifier",
    representation: { ...validPublicKey, kid: "key" },
  },
  {
    description: "unknown member",
    representation: { ...validPublicKey, extension: true },
  },
] as const;

const decodeSucceeds = (value: unknown): boolean =>
  Either.match(Schema.decodeUnknownEither(AgentPublicKey)(value), {
    onLeft: () => false,
    onRight: () => true,
  });

/** A distinct on-curve, prime-order Ed25519 public key for each index. */
const indexedPublicKey = (index: number) =>
  Schema.decodeUnknownSync(AgentPublicKey)({
    crv: "Ed25519",
    kty: "OKP",
    x: Buffer.from(
      ed25519.Point.BASE.multiply(BigInt(index + 1)).toBytes(),
    ).toString("base64url"),
  });

describe("AgentPublicKey thumbprints", () => {
  it.effect("derives each key's own thumbprint after another key's", () =>
    Effect.gen(function* () {
      const first = indexedPublicKey(0);
      const second = indexedPublicKey(1);
      const firstThumbprint = yield* ed25519PublicKeyThumbprintUri(first);
      const secondThumbprint = yield* ed25519PublicKeyThumbprintUri(second);
      const expected = yield* Effect.tryPromise({
        try: () => calculateJwkThumbprintUri(second, "sha256"),
        catch: (cause) => new Error("thumbprint failed", { cause }),
      });

      expect(secondThumbprint).toBe(expected);
      expect(secondThumbprint).not.toBe(firstThumbprint);
    }),
  );

  it.effect("matches the RFC thumbprint URI", () =>
    Effect.gen(function* () {
      const publicKey =
        yield* Schema.decodeUnknown(AgentPublicKey)(validPublicKey);
      const thumbprint = yield* ed25519PublicKeyThumbprintUri(publicKey);
      expect(thumbprint).toBe(RFC_THUMBPRINT_URI);
      expect(Schema.encodeSync(AgentPublicKey)(publicKey)).toEqual(
        validPublicKey,
      );
    }),
  );
});

describe("AgentPublicKey representation", () => {
  it("round-trips generated Ed25519 public keys", () => {
    for (let index = 0; index < 16; index += 1) {
      const { publicKey } = generateKeyPairSync("ed25519");
      const exported = publicKey.export({ format: "jwk" });
      const representation = {
        crv: "Ed25519" as const,
        kty: "OKP" as const,
        x: exported.x,
      };
      const decoded = Schema.decodeUnknownSync(AgentPublicKey)(representation);
      expect(Schema.encodeSync(AgentPublicKey)(decoded)).toEqual(
        representation,
      );
    }
  });

  it("returns an immutable snapshot", () => {
    const source = { ...validPublicKey };
    const publicKey = Schema.decodeUnknownSync(AgentPublicKey)(source);
    source.x = "A".repeat(43);

    expect(Object.isFrozen(publicKey)).toBe(true);
    expect(Reflect.set(publicKey, "x", "A".repeat(43))).toBe(false);
    expect(publicKey.x).toBe(RFC_PUBLIC_X);
  });
});

describe("AgentPublicKey rejection", () => {
  it.each(invalidPublicKeys)("rejects $description", ({ representation }) => {
    expect(decodeSucceeds(representation)).toBe(false);
  });

  it("rejects every small-order encoding with either sign bit", () => {
    fc.assert(
      fc.property(
        fc.constantFrom(...smallOrderPointHex),
        fc.boolean(),
        (hex, highBit) => {
          const representation = {
            ...validPublicKey,
            x: encodePoint(hex, highBit),
          };
          expect(decodeSucceeds(representation)).toBe(false);
        },
      ),
    );
  });
});

describe("Ed25519 signature representation", () => {
  it("accepts signatures produced by an Ed25519 private key", () => {
    const { privateKey } = generateKeyPairSync("ed25519");
    const signature = signWithPrivateKey(
      null,
      Buffer.from("accepted signature"),
      privateKey,
    );

    expect(hasCanonicalEd25519SignatureEncoding(signature)).toBe(true);
  });

  it("rejects a scalar equal to the Ed25519 group order", () => {
    const { privateKey } = generateKeyPairSync("ed25519");
    const signature = Uint8Array.from(
      signWithPrivateKey(null, Buffer.from("scalar boundary"), privateKey),
    );
    signature.set(
      [
        0xed, 0xd3, 0xf5, 0x5c, 0x1a, 0x63, 0x12, 0x58, 0xd6, 0x9c, 0xf7, 0xa2,
        0xde, 0xf9, 0xde, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10,
      ],
      32,
    );

    expect(hasCanonicalEd25519SignatureEncoding(signature)).toBe(false);
  });

  it("rejects an R coordinate equal to the field modulus", () => {
    const { privateKey } = generateKeyPairSync("ed25519");
    const signature = Uint8Array.from(
      signWithPrivateKey(null, Buffer.from("point boundary"), privateKey),
    );
    signature.set([
      0xed, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
      0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
      0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f,
    ]);

    expect(hasCanonicalEd25519SignatureEncoding(signature)).toBe(false);
  });
});

const RFC_PRIVATE_KEY = `-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIJ1hsZ3v/VpguoRK9JLsLMREScVpezJpGXA7rAMcrn9g
-----END PRIVATE KEY-----`;

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
