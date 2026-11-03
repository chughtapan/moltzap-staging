/** @file Canonical agent identifiers, names, issuance times, and card evidence. */

import { Effect, Either, Encoding, Schema } from "effect";
import * as fc from "fast-check";
import { describe, expect, it } from "vitest";
import {
  AgentIdentityCardIssuedAt,
  makeIdentityFixture,
} from "./__tests__/authentication-fixtures.js";
import {
  AgentId,
  AgentIdentityCard,
  AgentIdentityCardDigest,
  AgentName,
  PrincipalId,
} from "./agent-cards.js";
import { AgentSigningAuthority } from "./keys.js";
import { EnvelopeId } from "./message-security.js";
import { RegistrationId } from "./registry.js";

const IDENTIFIER_BYTE_LENGTH = 16;
const DIGEST_BYTE_LENGTH = 32;

const decodeSucceeds = <A, I>(
  schema: Schema.Schema<A, I>,
  value: unknown,
): boolean =>
  Schema.decodeUnknownEither(schema)(value).pipe(
    Either.match({
      onLeft: () => false,
      onRight: () => true,
    }),
  );

const expectCanonicalRoundTrip = <A>(
  schema: Schema.Schema<A, string>,
  encoded: string,
): void => {
  const value = Schema.decodeUnknownSync(schema)(encoded);
  expect(Schema.encodeSync(schema)(value)).toBe(encoded);
};

const identifierRoundTrips: ReadonlyArray<
  readonly [prefix: string, assertRoundTrip: (encoded: string) => void]
> = [
  [
    "agt_",
    (encoded) => {
      expectCanonicalRoundTrip(AgentId, encoded);
    },
  ],
  [
    "prn_",
    (encoded) => {
      expectCanonicalRoundTrip(PrincipalId, encoded);
    },
  ],
  [
    "opn_",
    (encoded) => {
      expectCanonicalRoundTrip(RegistrationId, encoded);
    },
  ],
  [
    "env_",
    (encoded) => {
      expectCanonicalRoundTrip(EnvelopeId, encoded);
    },
  ],
] as const;

describe("canonical identifiers", () => {
  it("round-trips every canonical identifier spelling", () => {
    expect.hasAssertions();
    fc.assert(
      fc.property(
        fc.uint8Array({
          minLength: IDENTIFIER_BYTE_LENGTH,
          maxLength: IDENTIFIER_BYTE_LENGTH,
        }),
        (bytes) => {
          const payload = Buffer.from(bytes).toString("base64url");
          for (const [prefix, assertRoundTrip] of identifierRoundTrips) {
            const encoded = `${prefix}${payload}`;
            assertRoundTrip(encoded);
          }
        },
      ),
    );
  });

  it.each([
    { reason: "wrong prefix", value: `prn_${"A".repeat(22)}` },
    { reason: "short payload", value: `agt_${"A".repeat(21)}` },
    { reason: "long payload", value: `agt_${"A".repeat(23)}` },
    { reason: "padding", value: `agt_${"A".repeat(22)}=` },
    { reason: "standard alphabet plus", value: `agt_${"A".repeat(21)}+` },
    { reason: "standard alphabet slash", value: `agt_${"A".repeat(21)}/` },
    { reason: "whitespace", value: `agt_${"A".repeat(21)} ` },
    { reason: "nonzero trailing bits", value: `agt_${"A".repeat(21)}B` },
  ])("rejects a noncanonical AgentId with $reason", ({ value }) => {
    expect(decodeSucceeds(AgentId, value)).toBe(false);
  });
});

describe("AgentIdentityCardDigest", () => {
  it("round-trips every canonical digest spelling", () => {
    fc.assert(
      fc.property(
        fc.uint8Array({
          minLength: DIGEST_BYTE_LENGTH,
          maxLength: DIGEST_BYTE_LENGTH,
        }),
        (bytes) => {
          const encoded = `acd_${Buffer.from(bytes).toString("base64url")}`;
          expectCanonicalRoundTrip(AgentIdentityCardDigest, encoded);
        },
      ),
    );
  });

  it.each([
    { reason: "short payload", value: `acd_${"A".repeat(42)}` },
    { reason: "long payload", value: `acd_${"A".repeat(44)}` },
    { reason: "padding", value: `acd_${"A".repeat(43)}=` },
    { reason: "nonzero trailing bits", value: `acd_${"A".repeat(42)}B` },
  ])("rejects a noncanonical digest with $reason", ({ value }) => {
    expect(decodeSucceeds(AgentIdentityCardDigest, value)).toBe(false);
  });
});

describe("AgentName", () => {
  it.each(["abc", "a".repeat(32), "agent-7"])(
    "accepts the AgentName %s without normalization",
    (value) => {
      const decoded = Schema.decodeUnknownSync(AgentName)(value);
      expect(Schema.encodeSync(AgentName)(decoded)).toBe(value);
    },
  );

  it.each([
    "ab",
    "a".repeat(33),
    "Agent",
    "agent_name",
    "agént",
    "-agent",
    "agent-",
    "agent--name",
    "agent name",
  ])("rejects the invalid AgentName %s", (value) => {
    expect(decodeSucceeds(AgentName, value)).toBe(false);
  });
});

describe("AgentIdentityCardIssuedAt", () => {
  it.each(["2026-07-30T17:42:05Z", "2000-02-29T00:00:00Z"])(
    "round-trips the whole-second UTC time %s",
    (value) => {
      expectCanonicalRoundTrip(AgentIdentityCardIssuedAt, value);
    },
  );

  it.each([
    { reason: "fractional seconds", value: "2026-07-30T17:42:05.000Z" },
    { reason: "UTC offset", value: "2026-07-30T17:42:05+00:00" },
    { reason: "space separator", value: "2026-07-30 17:42:05Z" },
    { reason: "leap-second spelling", value: "2026-07-30T17:42:60Z" },
    { reason: "missing leading zero", value: "2026-7-30T17:42:05Z" },
    { reason: "lowercase UTC marker", value: "2026-07-30T17:42:05z" },
    { reason: "invalid calendar day", value: "2026-02-29T17:42:05Z" },
  ])("rejects $reason", ({ value }) => {
    expect(decodeSucceeds(AgentIdentityCardIssuedAt, value)).toBe(false);
  });
});

it("retains a card's signed claims independently of public-key mutation", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const fixture = yield* makeIdentityFixture;
      const before = yield* Schema.encode(AgentIdentityCard)(
        fixture.identityCard,
      );
      expect(
        Reflect.set(fixture.identityCard.publicKey, "x", "A".repeat(43)),
      ).toBe(false);
      const parsed = yield* Schema.decodeUnknown(AgentIdentityCard)(before);
      const verified = yield* AgentIdentityCard.verify({
        identityCard: parsed,
        registrySignerPublicKey: AgentSigningAuthority.publicKey(
          fixture.registryAuthority,
        ),
      });
      expect(yield* Schema.encode(AgentIdentityCard)(verified)).toStrictEqual(
        before,
      );
    }),
  ));

it("rejects an identity-point universal signature on an agent identity card", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const fixture = yield* makeIdentityFixture;
      const representation = yield* Schema.encode(AgentIdentityCard)(
        fixture.identityCard,
      ).pipe(
        Effect.flatMap(
          Schema.decodeUnknown(
            Schema.Struct({
              payload: Schema.String,
              signatures: Schema.Tuple(
                Schema.Struct({
                  protected: Schema.String,
                  signature: Schema.String,
                }),
              ),
            }),
          ),
        ),
      );
      const universalSignature = new Uint8Array(64);
      universalSignature[0] = 1;
      const parsed = yield* Schema.decodeUnknown(AgentIdentityCard)({
        ...representation,
        signatures: [
          {
            ...representation.signatures[0],
            signature: Encoding.encodeBase64Url(universalSignature),
          },
        ],
      });
      const result = yield* AgentIdentityCard.verify({
        identityCard: parsed,
        registrySignerPublicKey: AgentSigningAuthority.publicKey(
          fixture.registryAuthority,
        ),
      }).pipe(Effect.either);
      expect(
        Either.match(result, { onLeft: () => true, onRight: () => false }),
      ).toBe(true);
    }),
  ));
