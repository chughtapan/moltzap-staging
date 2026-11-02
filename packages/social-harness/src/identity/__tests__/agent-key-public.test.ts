/** @file Canonical Ed25519 public-key, thumbprint, and signature representation tests. */

import { it as effectIt } from "@effect/vitest";
import { ed25519 } from "@noble/curves/ed25519.js";
import { Effect, Either, Schema } from "effect";
import * as fc from "fast-check";
import { calculateJwkThumbprintUri, exportJWK } from "jose";
import { generateKeyPairSync, sign as signWithPrivateKey } from "node:crypto";
import { describe, expect } from "vitest";
import {
  AgentPublicKey,
  ed25519PublicKeyThumbprintUri,
  ed25519VerificationKey,
  hasCanonicalEd25519SignatureEncoding,
} from "../keys.js";

const it = effectIt;
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

const encodeSucceeds = (value: unknown): boolean =>
  Either.match(Schema.encodeUnknownEither(AgentPublicKey)(value), {
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

/**
 * A caller that bypasses the Schema can pass an object whose `x` changes
 * between reads. Here the first read names `stored` and the next names
 * `other`; the cache must still store `stored`'s own import under `stored`.
 */
it.effect("stores each import under the key it was imported from", () =>
  Effect.gen(function* () {
    const stored = indexedPublicKey(2);
    const other = indexedPublicKey(3);
    const reads = [stored.x, other.x];
    const shifting = Object.freeze({
      crv: "Ed25519" as const,
      kty: "OKP" as const,
      get x() {
        return reads.shift() ?? stored.x;
      },
    });
    // eslint-disable-next-line agent-code-guard/require-assertion-rationale -- The getter object models a caller that bypasses the AgentPublicKey Schema.
    yield* ed25519VerificationKey(shifting as AgentPublicKey);

    const imported = yield* ed25519VerificationKey(stored);
    const exported = yield* Effect.tryPromise({
      try: () => exportJWK(imported),
      catch: (cause) => new Error("export failed", { cause }),
    });
    expect(exported.x).toBe(stored.x);
  }),
);

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

  it("rejects hidden members on encoded domain values", () => {
    const privateMaterial = { ...validPublicKey };
    Object.defineProperty(privateMaterial, "d", {
      enumerable: false,
      value: "A".repeat(43),
    });
    const symbolMember = {
      ...validPublicKey,
      [Symbol("private")]: "A".repeat(43),
    };

    expect(encodeSucceeds(Object.freeze(privateMaterial))).toBe(false);
    expect(encodeSucceeds(Object.freeze(symbolMember))).toBe(false);
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
