/** @file Signed messages preserve attribution and sealed messages restrict access to their members. */

import { ed25519 } from "@noble/curves/ed25519.js";
import { Effect, Either, Encoding, Option, Redacted, Schema } from "effect";
import * as fc from "fast-check";
import { generalDecrypt, GeneralEncrypt } from "jose";
import { createHash, generateKeyPairSync } from "node:crypto";
import { expect, it } from "vitest";
import {
  decodeCanonicalJson,
  encodeCanonicalJson,
} from "../encoding/canonical-json.js";
import { WIRE_VERSION } from "../version.js";
import {
  AgentIdentityCardIssuedAt,
  makeVerifiedCard,
} from "./__tests__/authentication-fixtures.js";
import {
  commitTo,
  envelopeIdOf,
  FIXTURE_CONCURRENCY,
  type Group,
  issueCard,
  KEY_AGREEMENT_HEAVY_TIMEOUT_MS,
  makeGroup,
  makeMember,
  type Member,
  openAs,
  plaintext,
  sealFrom,
  sealManually,
  sealTwoKeyCollision,
  sealWithSharedEphemeralKey,
  signBody,
} from "./__tests__/envelope-fixtures.js";
import {
  AgentId,
  AgentName,
  digestAgentIdentityCard,
  PrincipalId,
  type VerifiedAgentIdentityCard,
} from "./agent-cards.js";
import {
  agentOpeningPrivateKey,
  AgentPublicKey,
  AgentSigningAuthority,
  agentSigningPrivateKey,
  ed25519PublicKeyThumbprintUri,
  x25519PublicJwk,
} from "./keys.js";
import {
  EnvelopeId,
  SealedBody,
  SealedBodyOpeningError,
  SealedBodySealingError,
  SignedMessage,
  SignedMessageSigningError,
  type SignedMessage as SignedMessageValue,
  type VerifiedSignedMessage,
} from "./message-security.js";

const MAXIMUM_BODY_BYTES = 262_144;
const MAXIMUM_RECIPIENTS = 128;

const MIXED_ORDER_PUBLIC_KEY_X = "lZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZk";

const rawSignature = Schema.Struct({
  protected: Schema.String,
  signature: Schema.String,
});
const rawRepresentation = Schema.Struct({
  payload: Schema.String,
  signatures: Schema.Tuple(rawSignature),
});
const rawPayload = Schema.Struct({
  kind: Schema.String,
  sharnessVersion: Schema.String,
  senderAgentId: Schema.String,
  agentCardDigest: Schema.String,
  recipientAgentIds: Schema.Array(Schema.String),
  envelopeId: Schema.String,
  body: Schema.String,
});
const signedProtectedHeader = Schema.Struct({
  alg: Schema.String,
  kid: Schema.String,
  typ: Schema.String,
});

type RawRepresentation = typeof rawRepresentation.Type;
type RawPayload = typeof rawPayload.Type;
type RawProtectedHeader = typeof signedProtectedHeader.Type;

interface IdentityFixture {
  readonly identityCard: VerifiedAgentIdentityCard;
  readonly agentSigningAuthority: AgentSigningAuthority;
  readonly registrySignerPublicKey: ReturnType<
    typeof AgentSigningAuthority.publicKey
  >;
}

const identifier = (prefix: string, byte: number): string =>
  `${prefix}${Encoding.encodeBase64Url(new Uint8Array(16).fill(byte))}`;

const makeSigningAuthority = () => {
  const { privateKey } = generateKeyPairSync("ed25519");
  const privateKeyPem = privateKey.export({
    format: "pem",
    type: "pkcs8",
  });
  return AgentSigningAuthority.fromPkcs8(Redacted.make(privateKeyPem));
};

const makeIdentityFixture = Effect.gen(function* () {
  const registrySigningAuthority = yield* makeSigningAuthority();
  const agentSigningAuthority = yield* makeSigningAuthority();
  const identityCard = yield* makeVerifiedCard({
    agentId: Schema.decodeUnknownSync(AgentId)(identifier("agt_", 250)),
    principalId: Schema.decodeUnknownSync(PrincipalId)(identifier("prn_", 249)),
    agentName: Schema.decodeUnknownSync(AgentName)("snapshot-sender"),
    publicKey: AgentSigningAuthority.publicKey(agentSigningAuthority),
    issuedAt: Schema.decodeUnknownSync(AgentIdentityCardIssuedAt)(
      "2026-07-30T12:00:00Z",
    ),
    registrySigningAuthority,
  });
  return {
    identityCard,
    agentSigningAuthority,
    registrySignerPublicKey: AgentSigningAuthority.publicKey(
      registrySigningAuthority,
    ),
  };
});

const makeAgentId = (byte: number) =>
  Schema.decodeUnknownSync(AgentId)(identifier("agt_", byte));

const makeEnvelopeId = (byte: number) =>
  Schema.decodeUnknownSync(EnvelopeId)(identifier("env_", byte));

const recipientSet = (count: number): ReadonlySet<AgentId> => {
  const recipients = new Set<AgentId>();
  for (let index = 0; index < count; index += 1) {
    recipients.add(makeAgentId(index));
  }
  return recipients;
};

const signMessage = (
  fixture: IdentityFixture,
  body: Uint8Array,
  recipients: ReadonlySet<AgentId>,
  envelopeIdByte: number,
) =>
  SignedMessage.sign({
    identityCard: fixture.identityCard,
    signingAuthority: fixture.agentSigningAuthority,
    recipientAgentIds: recipients,
    envelopeId: makeEnvelopeId(envelopeIdByte),
    body,
  });

const makeSnapshotFixture = Effect.gen(function* () {
  const identity = yield* makeIdentityFixture;
  const firstRecipient = Schema.decodeUnknownSync(AgentId)(
    identifier("agt_", 3),
  );
  const secondRecipient = Schema.decodeUnknownSync(AgentId)(
    identifier("agt_", 4),
  );
  const recipients = new Set([firstRecipient]);
  const body = Uint8Array.from([1, 2, 3, 4]);
  const signedMessage = yield* signMessage(identity, body, recipients, 5);
  return {
    identityCard: identity.identityCard,
    registrySignerPublicKey: identity.registrySignerPublicKey,
    body,
    firstRecipient,
    recipients,
    secondRecipient,
    signedMessage,
  };
});

const messageRepresentation = (signedMessage: SignedMessageValue) =>
  Schema.encode(SignedMessage)(signedMessage).pipe(
    Effect.flatMap(Schema.decodeUnknown(rawRepresentation)),
  );

const decodeBase64Url = (value: string) =>
  Either.match(Encoding.decodeBase64Url(value), {
    onLeft: (error) => Effect.fail(error),
    onRight: (bytes) => Effect.succeed(bytes),
  });

const wasRejected = <Right, Left>(
  result: Either.Either<Right, Left>,
): boolean =>
  Either.match(result, {
    onLeft: () => true,
    onRight: () => false,
  });

type PayloadMutation = (payload: RawPayload) => RawPayload;
type HeaderMutation = (header: RawProtectedHeader) => RawProtectedHeader;

const mutatePayload = (
  representation: RawRepresentation,
  mutate: PayloadMutation,
) =>
  Effect.gen(function* () {
    const bytes = yield* decodeBase64Url(representation.payload);
    const parsed = yield* decodeCanonicalJson(rawPayload, bytes);
    const encoded = yield* encodeCanonicalJson(mutate(parsed));
    return {
      ...representation,
      payload: Encoding.encodeBase64Url(encoded),
    };
  });

const mutateProtectedHeader = (
  representation: RawRepresentation,
  mutate: HeaderMutation,
) =>
  Effect.gen(function* () {
    const signature = representation.signatures[0];
    const bytes = yield* decodeBase64Url(signature.protected);
    const parsed = yield* decodeCanonicalJson(signedProtectedHeader, bytes);
    const encoded = yield* encodeCanonicalJson(mutate(parsed));
    return {
      ...representation,
      signatures: [
        {
          ...signature,
          protected: Encoding.encodeBase64Url(encoded),
        },
      ] as const,
    };
  });

const mutateSignature = (representation: RawRepresentation) =>
  Effect.gen(function* () {
    const signature = representation.signatures[0];
    const bytes = Uint8Array.from(yield* decodeBase64Url(signature.signature));
    const firstByte = bytes[0];
    if (firstByte === undefined) {
      return representation;
    }
    bytes[0] = firstByte ^ 1;
    return {
      ...representation,
      signatures: [
        {
          ...signature,
          signature: Encoding.encodeBase64Url(bytes),
        },
      ] as const,
    };
  });

const replaceSignature = (
  representation: RawRepresentation,
  signature: Uint8Array,
): RawRepresentation => ({
  ...representation,
  signatures: [
    {
      ...representation.signatures[0],
      signature: Encoding.encodeBase64Url(signature),
    },
  ],
});

/**
 * Signs a message's exact protected header and payload with another
 * authority's key, as that key's holder would to pass the message off as the
 * original sender's.
 */
const resignWith = (
  representation: RawRepresentation,
  authority: AgentSigningAuthority,
) =>
  Effect.tryPromise({
    try: () =>
      crypto.subtle.sign(
        "Ed25519",
        agentSigningPrivateKey(authority),
        new TextEncoder().encode(
          `${representation.signatures[0].protected}.${representation.payload}`,
        ),
      ),
    catch: (cause) => new Error("re-signing failed", { cause }),
  }).pipe(
    Effect.map((signature) =>
      replaceSignature(representation, new Uint8Array(signature)),
    ),
  );

const mutationIsRejected = (
  representation: RawRepresentation,
  identityCard: VerifiedAgentIdentityCard,
) =>
  Schema.decodeUnknown(SignedMessage)(representation, {
    exact: true,
    onExcessProperty: "error",
  }).pipe(
    Effect.flatMap((signedMessage) =>
      SignedMessage.verify({ signedMessage, identityCard }),
    ),
    Effect.either,
    Effect.map(Either.isLeft),
  );

const actualEncodedByteLength = (signedMessage: SignedMessageValue) =>
  messageRepresentation(signedMessage).pipe(
    Effect.flatMap(encodeCanonicalJson),
    Effect.map((bytes) => bytes.byteLength),
  );

const assertLengthInvariant = (
  fixture: IdentityFixture,
  body: Uint8Array,
  recipientCount: number,
  envelopeIdByte: number,
) =>
  Effect.gen(function* () {
    const signedMessage = yield* signMessage(
      fixture,
      body,
      recipientSet(recipientCount),
      envelopeIdByte,
    );
    const representation = yield* messageRepresentation(signedMessage);
    const parsed = yield* Schema.decodeUnknown(SignedMessage)(representation);
    const actualLength = yield* actualEncodedByteLength(parsed);
    expect(SignedMessage.encodedByteLength(parsed)).toBe(actualLength);
  });

interface NamedMutation {
  readonly name: string;
  readonly representation: RawRepresentation;
}

const namedMutation = <E, R>(
  name: string,
  mutation: Effect.Effect<RawRepresentation, E, R>,
) => mutation.pipe(Effect.map((representation) => ({ name, representation })));

const payloadMutations = (representation: RawRepresentation) =>
  [
    namedMutation(
      "sender",
      mutatePayload(representation, (value) => ({
        ...value,
        senderAgentId: makeAgentId(200),
      })),
    ),
    namedMutation(
      "recipients",
      mutatePayload(representation, (value) => ({
        ...value,
        recipientAgentIds: [makeAgentId(201)],
      })),
    ),
    namedMutation(
      "message ID",
      mutatePayload(representation, (value) => ({
        ...value,
        envelopeId: makeEnvelopeId(202),
      })),
    ),
    namedMutation(
      "body",
      mutatePayload(representation, (value) => ({
        ...value,
        body: Encoding.encodeBase64Url(Uint8Array.from([9, 8, 7])),
      })),
    ),
    namedMutation(
      "version",
      mutatePayload(representation, (value) => ({
        ...value,
        sharnessVersion: `${value.sharnessVersion}-changed`,
      })),
    ),
    namedMutation(
      "card digest",
      mutatePayload(representation, (value) => ({
        ...value,
        agentCardDigest: `acd_${Encoding.encodeBase64Url(new Uint8Array(32).fill(203))}`,
      })),
    ),
  ] as const;

const headerAndSignatureMutations = (representation: RawRepresentation) =>
  [
    namedMutation(
      "key ID",
      mutateProtectedHeader(representation, (value) => ({
        ...value,
        kid: `${value.kid}-changed`,
      })),
    ),
    namedMutation(
      "type",
      mutateProtectedHeader(representation, (value) => ({
        ...value,
        typ: `${value.typ}-changed`,
      })),
    ),
    namedMutation("signature", mutateSignature(representation)),
  ] as const;

const cryptographicMutations = (representation: RawRepresentation) =>
  Effect.all(
    [
      ...payloadMutations(representation),
      ...headerAndSignatureMutations(representation),
    ],
    { concurrency: 1 },
  );

const assertMutationRejected = (
  mutation: NamedMutation,
  identityCard: VerifiedAgentIdentityCard,
) =>
  Effect.gen(function* () {
    const rejected = yield* mutationIsRejected(
      mutation.representation,
      identityCard,
    );
    expect(rejected, mutation.name).toBe(true);
  });

const parsedLengthProperty = (fixture: IdentityFixture) =>
  fc.asyncProperty(
    fc.uint8Array({ maxLength: 4096 }),
    fc.integer({ min: 1, max: MAXIMUM_RECIPIENTS }),
    fc.integer({ min: 0, max: 247 }),
    (body, recipientCount, envelopeIdByte) =>
      Effect.runPromise(
        assertLengthInvariant(fixture, body, recipientCount, envelopeIdByte),
      ),
  );

const verifyParsedLengthProperty = Effect.gen(function* () {
  const fixture = yield* makeIdentityFixture;
  yield* Effect.tryPromise({
    try: () => fc.assert(parsedLengthProperty(fixture), { numRuns: 24 }),
    catch: (cause) =>
      new Error("parsed SignedMessage length property failed", { cause }),
  });
});

it("retains the signed body and recipients independently of caller mutations", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const fixture = yield* makeSnapshotFixture;
      const {
        identityCard,
        body,
        firstRecipient,
        recipients,
        secondRecipient,
        signedMessage,
      } = fixture;
      body[0] = 99;
      recipients.add(secondRecipient);
      const returnedBody = signedMessage.body;
      returnedBody[1] = 99;

      expect(signedMessage.recipientAgentIds).toStrictEqual([firstRecipient]);
      expect(signedMessage.body).toStrictEqual(Uint8Array.from([1, 2, 3, 4]));

      const representation = yield* Schema.encode(SignedMessage)(signedMessage);
      const parsed = yield* Schema.decodeUnknown(SignedMessage)(representation);
      const verified = yield* SignedMessage.verify({
        signedMessage: parsed,
        identityCard,
      });

      expect(verified.recipientAgentIds).toStrictEqual([firstRecipient]);
      expect(verified.body).toStrictEqual(Uint8Array.from([1, 2, 3, 4]));
    }),
  ));

it("keeps encoded messages unchanged when callers mutate public views", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const fixture = yield* makeSnapshotFixture;
      const messageBefore = yield* messageRepresentation(fixture.signedMessage);

      expect(
        Reflect.set(
          fixture.signedMessage.recipientAgentIds,
          "0",
          fixture.secondRecipient,
        ),
      ).toBe(false);
      const body = fixture.signedMessage.body;
      body.fill(0);

      const messageAfter = yield* messageRepresentation(fixture.signedMessage);
      expect(messageAfter).toStrictEqual(messageBefore);
    }),
  ));

it("rejects every cryptographically covered field mutation after the original verifies", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const fixture = yield* makeSnapshotFixture;
      yield* SignedMessage.verify({
        signedMessage: fixture.signedMessage,
        identityCard: fixture.identityCard,
      });
      const representation = yield* messageRepresentation(
        fixture.signedMessage,
      );
      const mutations: readonly NamedMutation[] =
        yield* cryptographicMutations(representation);

      yield* Effect.forEach(
        mutations,
        (mutation) => assertMutationRejected(mutation, fixture.identityCard),
        { concurrency: 1, discard: true },
      );
    }),
  ));

it("verifies each message under its own card's key, not another key already imported", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const first = yield* makeIdentityFixture;
      const second = yield* makeIdentityFixture;
      const firstMessage = yield* signMessage(
        first,
        Uint8Array.from([1]),
        recipientSet(1),
        1,
      );
      yield* SignedMessage.verify({
        signedMessage: firstMessage,
        identityCard: first.identityCard,
      });
      const secondMessage = yield* signMessage(
        second,
        Uint8Array.from([2]),
        recipientSet(1),
        2,
      );
      const forged = yield* resignWith(
        yield* messageRepresentation(secondMessage),
        first.agentSigningAuthority,
      );

      expect(yield* mutationIsRejected(forged, second.identityCard)).toBe(true);
      yield* SignedMessage.verify({
        signedMessage: secondMessage,
        identityCard: second.identityCard,
      });
    }),
  ));

it("rejects an identity-point universal signature on a signed message", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const fixture = yield* makeSnapshotFixture;
      const universalSignature = new Uint8Array(64);
      universalSignature[0] = 1;
      const signedMessage = yield* messageRepresentation(fixture.signedMessage);

      const parsedMessage = yield* Schema.decodeUnknown(SignedMessage)(
        replaceSignature(signedMessage, universalSignature),
      );
      const messageResult = yield* SignedMessage.verify({
        signedMessage: parsedMessage,
        identityCard: fixture.identityCard,
      }).pipe(Effect.either);

      expect(wasRejected(messageResult)).toBe(true);
    }),
  ));

/** The Registry legitimately attests a canonical mixed-order agent key. */
const makeMixedOrderCard = Effect.gen(function* () {
  return yield* makeVerifiedCard({
    agentId: makeAgentId(254),
    principalId: PrincipalId.make(identifier("prn_", 253)),
    agentName: AgentName.make("mixed-order-sender"),
    publicKey: yield* Schema.decodeUnknown(AgentPublicKey)({
      crv: "Ed25519",
      kty: "OKP",
      x: MIXED_ORDER_PUBLIC_KEY_X,
    }),
    issuedAt: yield* Schema.decodeUnknown(AgentIdentityCardIssuedAt)(
      "2026-07-30T12:00:00Z",
    ),
    registrySigningAuthority: yield* makeSigningAuthority(),
  });
});

/** For A=B+T of order-two T, odd k makes S=1+k verify only after multiplying by the cofactor. */
function cofactoredSignature(input: Uint8Array, publicKey: Uint8Array) {
  const order = (1n << 252n) + 27742317777372353535851937790883648493n;
  const r = ed25519.Point.BASE.toBytes();
  const hash = createHash("sha512")
    .update(r)
    .update(publicKey)
    .update(input)
    .digest();
  const k = BigInt(`0x${Buffer.from(hash).reverse().toString("hex")}`) % order;
  if (k % 2n === 0n) {
    return undefined;
  }
  const scalar = Buffer.from(
    ((1n + k) % order).toString(16).padStart(64, "0"),
    "hex",
  ).reverse();
  return Buffer.concat([r, scalar]);
}

/** The body selects an odd challenge for the current card, header and emitted version. */
function makeCofactoredMessage(identityCard: VerifiedAgentIdentityCard) {
  return Effect.gen(function* () {
    const header = Encoding.encodeBase64Url(
      yield* encodeCanonicalJson({
        alg: "Ed25519",
        kid: yield* ed25519PublicKeyThumbprintUri(identityCard.publicKey),
        typ: "application/vnd.sharness.signed-message+jws",
      }),
    );
    const cardDigest = yield* digestAgentIdentityCard(identityCard);
    const publicKey = yield* decodeBase64Url(identityCard.publicKey.x);
    for (let attempt = 0; attempt < 256; attempt += 1) {
      const payload = Encoding.encodeBase64Url(
        yield* encodeCanonicalJson({
          kind: "signedMessage",
          sharnessVersion: WIRE_VERSION,
          senderAgentId: identityCard.agentId,
          agentCardDigest: cardDigest,
          recipientAgentIds: [makeAgentId(0)],
          envelopeId: makeEnvelopeId(1),
          body: Encoding.encodeBase64Url(
            new TextEncoder().encode(`cofactored-${attempt}`),
          ),
        }),
      );
      const input = new TextEncoder().encode(`${header}.${payload}`);
      const signature = cofactoredSignature(input, publicKey);
      if (signature !== undefined) {
        const signedMessage = yield* Schema.decodeUnknown(SignedMessage)({
          payload,
          signatures: [
            {
              protected: header,
              signature: Encoding.encodeBase64Url(signature),
            },
          ],
        });
        return { signedMessage, input, signature, publicKey };
      }
    }
    return yield* Effect.dieMessage("no odd Ed25519 challenge found");
  });
}

it("rejects a signature accepted only by cofactored Ed25519 verification", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const identityCard = yield* makeMixedOrderCard;
      const candidate = yield* makeCofactoredMessage(identityCard);
      expect(
        ed25519.verify(
          candidate.signature,
          candidate.input,
          candidate.publicKey,
          { zip215: false },
        ),
      ).toBe(true);
      const result = yield* SignedMessage.verify({
        signedMessage: candidate.signedMessage,
        identityCard,
      }).pipe(Effect.either);
      expect(wasRejected(result)).toBe(true);
    }),
  ));

it("derives the exact maximum complete SignedMessage encoding", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const fixture = yield* makeIdentityFixture;
      const signedMessage = yield* signMessage(
        fixture,
        new Uint8Array(MAXIMUM_BODY_BYTES),
        recipientSet(MAXIMUM_RECIPIENTS),
        248,
      );
      const actualLength = yield* actualEncodedByteLength(signedMessage);

      expect(actualLength).toBe(SignedMessage.maximumEncodedByteLength);
      expect(
        yield* signMessage(
          fixture,
          new Uint8Array(MAXIMUM_BODY_BYTES + 1),
          recipientSet(MAXIMUM_RECIPIENTS),
          249,
        ).pipe(Effect.isFailure),
      ).toBe(true);
      expect(SignedMessage.encodedByteLength(signedMessage)).toBe(actualLength);
    }),
  ));

it("reports the actual retained encoding length for parsed messages", () => {
  expect.hasAssertions();
  return Effect.runPromise(verifyParsedLengthProperty);
});

const SENDER_HEADER = "org.social-harness/sender";
const ENVELOPE_ID_HEADER = "org.social-harness/envelope-id";
const COMMITMENT_HEADER = "org.social-harness/commitment";
const SALT_BYTES = 32;

const utf8Encoder = new TextEncoder();
const utf8Decoder = new TextDecoder();

const rawSealedBody = Schema.Struct({
  ciphertext: Schema.String,
  iv: Schema.String,
  protected: Schema.String,
  recipients: Schema.Array(
    Schema.Struct({
      encrypted_key: Schema.String,
      header: Schema.optional(
        Schema.Struct({
          epk: Schema.Struct({
            crv: Schema.String,
            kty: Schema.String,
            x: Schema.String,
          }),
        }),
      ),
    }),
  ),
  tag: Schema.String,
});
type RawSealedBody = typeof rawSealedBody.Type;

const sealedProtectedHeader = Schema.Record({
  key: Schema.String,
  value: Schema.Unknown,
});

const readSealedBody = (sealed: Uint8Array): RawSealedBody =>
  Schema.decodeUnknownSync(Schema.parseJson(rawSealedBody))(
    utf8Decoder.decode(sealed),
  );

const readProtectedHeader = (body: RawSealedBody) =>
  Schema.decodeUnknownSync(Schema.parseJson(sealedProtectedHeader))(
    utf8Decoder.decode(
      Encoding.decodeBase64Url(body.protected).pipe(Either.getOrThrow),
    ),
  );

const writeProtectedHeader = (header: unknown): string =>
  Encoding.encodeBase64Url(JSON.stringify(header));

/**
 * Replaces the first base64url character, which encodes the six high bits of
 * the first byte, so the result stays canonical and changes one byte.
 *
 * @param value Canonical base64url text.
 * @returns The same text with a different first byte.
 */
const flipFirstByte = (value: string): string =>
  `${value.startsWith("A") ? "B" : "A"}${value.slice(1)}`;

const BASE64URL_ALPHABET =
  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

/**
 * Sets the lowest bit of the final base64url character. When the text length
 * is not a multiple of four that bit is padding, which a canonical spelling
 * leaves clear, so the result is a noncanonical spelling of the same bytes.
 * The jose library decodes such a spelling and opens the body, so only the
 * representation check refuses it.
 *
 * @param value Canonical base64url text whose length is not a multiple of four.
 * @returns A noncanonical spelling of the same bytes.
 */
const respellFinalCharacter = (value: string): string =>
  `${value.slice(0, -1)}${BASE64URL_ALPHABET.charAt(BASE64URL_ALPHABET.indexOf(value.slice(-1)) + 1)}`;

const freshSalt = () => crypto.getRandomValues(new Uint8Array(SALT_BYTES));

/**
 * Spells a protected header exactly as `SealedBody.seal` does for two or more
 * recipients, from the base64url SHA-256 of the salted plaintext, the
 * EnvelopeId `signBody` uses by default, and the sender.
 */
const honestHeaderText = (commitment: string, sender: Member) =>
  JSON.stringify({
    alg: "ECDH-ES+A256KW",
    enc: "A256GCM",
    [COMMITMENT_HEADER]: commitment,
    [ENVELOPE_ID_HEADER]: envelopeIdOf(1),
    [SENDER_HEADER]: sender.identityCard.agentId,
  });

/**
 * Carries a signed message through its wire representation and verifies it
 * as a recipient does.
 *
 * @param signedMessage Message as the sender produced it.
 * @param senderCard Sender's verified AgentIdentityCard.
 * @returns The message as a recipient verifies it.
 */
const receive = (
  signedMessage: VerifiedSignedMessage,
  senderCard: VerifiedAgentIdentityCard,
) =>
  Schema.encode(SignedMessage)(signedMessage).pipe(
    Effect.flatMap(Schema.decodeUnknown(SignedMessage)),
    Effect.flatMap((decoded) =>
      SignedMessage.verify({
        signedMessage: decoded,
        identityCard: senderCard,
      }),
    ),
  );

/**
 * Opens a sealed body with jose alone, trying every entry, as a positive
 * control: a body jose opens is refused only by SealedBody's own checks.
 *
 * @param sealed Sealed body bytes.
 * @param member Recipient whose opening key is used.
 * @returns The salted plaintext jose decrypts.
 */
const joseOpen = (sealed: Uint8Array, member: Member) =>
  Effect.tryPromise({
    try: () => {
      const body = readSealedBody(sealed);
      return generalDecrypt(
        { ...body, recipients: [...body.recipients] },
        agentOpeningPrivateKey(member.authority),
      );
    },
    catch: () => new Error("jose refused the body"),
  }).pipe(Effect.map((decrypted) => decrypted.plaintext));

it.each([1, 3, 32])(
  "opens a body sealed to %i recipients for every recipient, the sender included",
  (recipientCount) =>
    Effect.runPromise(
      Effect.gen(function* () {
        const group = yield* makeGroup(recipientCount);
        const sealed = yield* sealFrom(
          group.sender,
          group.recipients,
          plaintext,
        );
        const signedMessage = yield* signBody(
          group.sender,
          group.recipients,
          sealed,
        );

        const opened = yield* Effect.forEach(
          group.recipients,
          (member) => openAs(member, signedMessage),
          { concurrency: FIXTURE_CONCURRENCY },
        );

        expect(group.recipients).toHaveLength(recipientCount);
        expect(group.recipients).toContain(group.sender);
        expect(opened).toEqual(group.recipients.map(() => plaintext));
        expect(
          Buffer.from(
            Encoding.decodeBase64Url(readSealedBody(sealed).ciphertext).pipe(
              Either.getOrThrow,
            ),
          ).includes(Buffer.from(plaintext)),
        ).toBe(false);
      }),
    ),
  KEY_AGREEMENT_HEAVY_TIMEOUT_MS,
);

/**
 * AgentId bytes whose spellings sort differently as text: byte 66 spells
 * `agt_QkJC…` and byte 67 spells `agt_Q0ND…`, so a text sort puts 67 first
 * while the canonical bytewise recipient order puts 66 first.
 */
const TEXT_ORDER_INVERTING_BYTES = [66, 67];

/**
 * Seals from `sender` with the recipient cards in the order given, signs to
 * the same recipients, and checks that every recipient opens the plaintext.
 *
 * @param sender Sender of the SignedMessage.
 * @param recipients Recipients in the order the sender lists their cards.
 * @returns Completion once every recipient opens.
 */
const expectEveryListedRecipientOpens = (
  sender: Member,
  recipients: readonly Member[],
) =>
  Effect.gen(function* () {
    const sealed = yield* sealFrom(sender, recipients, plaintext);
    const signedMessage = yield* signBody(sender, recipients, sealed);

    const opened = yield* Effect.forEach(
      recipients,
      (member) => openAs(member, signedMessage),
      { concurrency: FIXTURE_CONCURRENCY },
    );

    expect(opened).toEqual(recipients.map(() => plaintext));
  });

/**
 * Runs `expectEveryListedRecipientOpens` over generated orders of the
 * recipient cards, starting with the reverse of the canonical order.
 *
 * @param sender Sender of the SignedMessage.
 * @param recipients Recipients in canonical order.
 * @returns The fast-check run, rejecting with the shrunk counterexample.
 */
const everyRecipientCardOrderOpens = (
  sender: Member,
  recipients: readonly Member[],
) =>
  fc.assert(
    fc.asyncProperty(
      fc.shuffledSubarray([...recipients], {
        minLength: recipients.length,
        maxLength: recipients.length,
      }),
      (order) =>
        Effect.runPromise(expectEveryListedRecipientOpens(sender, order)),
    ),
    { numRuns: 8, examples: [[[...recipients].reverse()]] },
  );

/**
 * Canonical recipient order is bytewise. These identities reverse their order
 * when sorted as text, and the caller supplies them in generated orders.
 */
it(
  "opens for every recipient whatever order the sender lists the recipient cards in",
  () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const group = yield* makeGroup(2);
        const textOrderInverting = yield* Effect.forEach(
          TEXT_ORDER_INVERTING_BYTES,
          (byte) => makeMember(group.registrySigningAuthority, byte),
          { concurrency: FIXTURE_CONCURRENCY },
        );

        yield* Effect.tryPromise({
          try: () =>
            everyRecipientCardOrderOpens(group.sender, [
              ...group.recipients,
              ...textOrderInverting,
            ]),
          catch: (counterexample) => counterexample,
        });
      }),
    ),
  KEY_AGREEMENT_HEAVY_TIMEOUT_MS,
);

/**
 * A fresh salt prevents the commitment from confirming a guessed plaintext;
 * ordinary round trips also pass with a constant salt.
 */
it("commits two seals of the same plaintext under different salts", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const group = yield* makeGroup(2);

      const sealed = yield* Effect.forEach(
        [1, 2],
        () => sealFrom(group.sender, group.recipients, plaintext),
        { concurrency: 1 },
      );

      const [first, second] = sealed.map(
        (body) => readProtectedHeader(readSealedBody(body))[COMMITMENT_HEADER],
      );
      expect(first).not.toEqual(second);
    }),
  ));

it("refuses to open for an agent that is not a recipient", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const group = yield* makeGroup(3);
      const sealed = yield* sealFrom(group.sender, group.recipients, plaintext);
      const signedMessage = yield* signBody(
        group.sender,
        group.recipients,
        sealed,
      );

      const outcome = yield* openAs(group.outsider, signedMessage).pipe(
        Effect.either,
      );

      expect(outcome).toStrictEqual(Either.left(new SealedBodyOpeningError()));
    }),
  ));

it.each([
  {
    part: "ciphertext",
    tamper: (body: RawSealedBody): RawSealedBody => ({
      ...body,
      ciphertext: flipFirstByte(body.ciphertext),
    }),
  },
  {
    part: "initialization vector",
    tamper: (body: RawSealedBody): RawSealedBody => ({
      ...body,
      iv: flipFirstByte(body.iv),
    }),
  },
  {
    part: "authentication tag",
    tamper: (body: RawSealedBody): RawSealedBody => ({
      ...body,
      tag: flipFirstByte(body.tag),
    }),
  },
  {
    part: "encrypted_key of the opening recipient",
    tamper: (body: RawSealedBody): RawSealedBody => ({
      ...body,
      recipients: body.recipients.map((recipient, index) =>
        index === 1
          ? {
              ...recipient,
              encrypted_key: flipFirstByte(recipient.encrypted_key),
            }
          : recipient,
      ),
    }),
  },
  /**
   * The ciphertext bytes are unchanged, so content authentication alone cannot
   * refuse this noncanonical spelling.
   */
  {
    part: "ciphertext spelling",
    tamper: (body: RawSealedBody): RawSealedBody => ({
      ...body,
      ciphertext: respellFinalCharacter(body.ciphertext),
    }),
  },
  /**
   * The tag bytes are unchanged; decoding to the expected length is insufficient.
   */
  {
    part: "authentication tag spelling",
    tamper: (body: RawSealedBody): RawSealedBody => ({
      ...body,
      tag: respellFinalCharacter(body.tag),
    }),
  },
  /**
   * The wrapped key bytes are unchanged, so key unwrapping alone cannot refuse
   * this noncanonical spelling.
   */
  {
    part: "encrypted_key spelling of the opening recipient",
    tamper: (body: RawSealedBody): RawSealedBody => ({
      ...body,
      recipients: body.recipients.map((recipient, index) =>
        index === 1
          ? {
              ...recipient,
              encrypted_key: respellFinalCharacter(recipient.encrypted_key),
            }
          : recipient,
      ),
    }),
  },
  /**
   * The entry header is outside authenticated data. The jose opener accepts
   * this respelling, so the representation check must refuse it.
   */
  {
    part: "ephemeral-key spelling of the opening recipient",
    tamper: (body: RawSealedBody): RawSealedBody => ({
      ...body,
      recipients: body.recipients.map((recipient, index) =>
        index === 1 && recipient.header !== undefined
          ? {
              ...recipient,
              header: {
                epk: {
                  ...recipient.header.epk,
                  x: respellFinalCharacter(recipient.header.epk.x),
                },
              },
            }
          : recipient,
      ),
    }),
  },
  /**
   * The opener can decrypt its own entry. The representation check must also
   * require an ephemeral key in every other entry.
   */
  {
    part: "ephemeral-key placement of another recipient's entry",
    tamper: (body: RawSealedBody): RawSealedBody => ({
      ...body,
      recipients: body.recipients.map((recipient, index) =>
        index === 0 ? { encrypted_key: recipient.encrypted_key } : recipient,
      ),
    }),
  },
])("refuses a sealed body with a tampered $part", ({ tamper }) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const group = yield* makeGroup(3);
      const sealed = yield* sealFrom(group.sender, group.recipients, plaintext);
      const tampered = yield* encodeCanonicalJson(
        tamper(readSealedBody(sealed)),
      );
      const signedMessage = yield* signBody(
        group.sender,
        group.recipients,
        tampered,
      );

      const outcome = yield* openAs(group.peer, signedMessage).pipe(
        Effect.either,
      );

      expect(outcome).toStrictEqual(Either.left(new SealedBodyOpeningError()));
    }),
  ),
);

it("refuses a body whose protected-header sender was rewritten to the re-signing member", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const group = yield* makeGroup(3);
      const sealed = yield* sealFrom(group.sender, group.recipients, plaintext);
      const body = readSealedBody(sealed);
      const forged = yield* encodeCanonicalJson({
        ...body,
        protected: writeProtectedHeader({
          ...readProtectedHeader(body),
          [SENDER_HEADER]: group.peer.identityCard.agentId,
        }),
      });
      const signedMessage = yield* signBody(
        group.peer,
        group.recipients,
        forged,
      );

      const outcome = yield* openAs(group.sender, signedMessage).pipe(
        Effect.either,
      );

      expect(outcome).toStrictEqual(Either.left(new SealedBodyOpeningError()));
    }),
  ));

it("refuses an unchanged sealed body re-signed by another member", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const group = yield* makeGroup(3);
      const sealed = yield* sealFrom(group.sender, group.recipients, plaintext);
      const signedMessage = yield* signBody(
        group.peer,
        group.recipients,
        sealed,
      );

      const outcome = yield* openAs(group.sender, signedMessage).pipe(
        Effect.either,
      );

      expect(outcome).toStrictEqual(Either.left(new SealedBodyOpeningError()));
    }),
  ));

it("refuses the recipients whose entries the sender moved out of canonical order", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const group = yield* makeGroup(3);
      const sealed = yield* sealFrom(group.sender, group.recipients, plaintext);
      const body = readSealedBody(sealed);
      const firstTwoSwapped = yield* encodeCanonicalJson({
        ...body,
        recipients: [
          ...body.recipients.slice(1, 2),
          ...body.recipients.slice(0, 1),
          ...body.recipients.slice(2),
        ],
      });
      const signedMessage = yield* signBody(
        group.sender,
        group.recipients,
        firstTwoSwapped,
      );

      const outcomes = yield* Effect.forEach(
        group.recipients,
        (member) => openAs(member, signedMessage).pipe(Effect.either),
        { concurrency: 1 },
      );

      expect(outcomes).toStrictEqual([
        Either.left(new SealedBodyOpeningError()),
        Either.left(new SealedBodyOpeningError()),
        Either.right(plaintext),
      ]);
    }),
  ));

it("refuses a body whose entry count differs from the SignedMessage recipient count", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const group = yield* makeGroup(3);
      const sealed = yield* sealFrom(group.sender, group.recipients, plaintext);
      const signedMessage = yield* signBody(
        group.sender,
        group.recipients.slice(0, 2),
        sealed,
      );

      const outcome = yield* openAs(group.sender, signedMessage).pipe(
        Effect.either,
      );

      expect(outcome).toStrictEqual(Either.left(new SealedBodyOpeningError()));
    }),
  ));

it("refuses a body with fewer entries than SignedMessage recipients", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const group = yield* makeGroup(3);
      const sealed = yield* sealFrom(
        group.sender,
        group.recipients.slice(0, 2),
        plaintext,
      );
      const signedMessage = yield* signBody(
        group.sender,
        group.recipients,
        sealed,
      );

      const outcome = yield* openAs(group.sender, signedMessage).pipe(
        Effect.either,
      );

      expect(outcome).toStrictEqual(Either.left(new SealedBodyOpeningError()));
    }),
  ));

it("refuses to open with an AgentIdentityCard that does not belong to the authority", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const group = yield* makeGroup(3);
      const sealed = yield* sealFrom(group.sender, group.recipients, plaintext);
      const signedMessage = yield* signBody(
        group.sender,
        group.recipients,
        sealed,
      );

      const outcome = yield* SealedBody.open({
        identityCard: group.peer.identityCard,
        signingAuthority: group.sender.authority,
        signedMessage,
      }).pipe(Effect.either);

      expect(outcome).toStrictEqual(Either.left(new SealedBodyOpeningError()));
    }),
  ));

/**
 * The foreign card names an entry that decrypts under the supplied authority.
 * Opening must compare the card and authority keys independently of unwrapping.
 */
it("refuses a foreign AgentIdentityCard whose entry the sender wrapped to the opening authority", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const group = yield* makeGroup(2);
      const { saltedPlaintext, commitment } = yield* commitTo(
        freshSalt(),
        plaintext,
      );
      const sealed = yield* sealManually({
        protectedHeaderText: honestHeaderText(commitment, group.sender),
        saltedPlaintext,
        recipients: [group.sender.identityCard, group.sender.identityCard],
      });
      const signedMessage = yield* signBody(
        group.sender,
        group.recipients,
        sealed,
      );

      const outcome = yield* SealedBody.open({
        identityCard: group.peer.identityCard,
        signingAuthority: group.sender.authority,
        signedMessage,
      }).pipe(Effect.either);
      const joseOpened = yield* joseOpen(sealed, group.sender);

      expect(outcome).toStrictEqual(Either.left(new SealedBodyOpeningError()));
      expect(joseOpened).toEqual(saltedPlaintext);
    }),
  ));

it.each([
  {
    body: "a plaintext body",
    make: () => Effect.succeed(utf8Encoder.encode("plain outer body")),
  },
  {
    body: "an empty body",
    make: () => Effect.succeed(new Uint8Array()),
  },
  {
    body: "canonical JSON that is not a JWE",
    make: () => encodeCanonicalJson({ kind: "message", text: "hello" }),
  },
  {
    body: "a sealed body with an added aad member",
    make: (sealed: Uint8Array) =>
      encodeCanonicalJson({ ...readSealedBody(sealed), aad: "AAAA" }),
  },
  {
    body: "a sealed body in non-canonical JSON",
    make: (sealed: Uint8Array) =>
      Effect.succeed(
        utf8Encoder.encode(JSON.stringify(readSealedBody(sealed), null, 1)),
      ),
  },
])("refuses $body", ({ make }) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const group = yield* makeGroup(2);
      const sealed = yield* sealFrom(group.sender, group.recipients, plaintext);
      const signedMessage = yield* signBody(
        group.sender,
        group.recipients,
        yield* make(sealed),
      );

      const outcome = yield* openAs(group.peer, signedMessage).pipe(
        Effect.either,
      );

      expect(outcome).toStrictEqual(Either.left(new SealedBodyOpeningError()));
    }),
  ),
);

it("refuses a single-recipient body whose ephemeral key sits in the recipient header", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const group = yield* makeGroup(2);
      const sealed = yield* sealFrom(group.sender, group.recipients, plaintext);
      const body = readSealedBody(sealed);
      const firstRecipientOnly = yield* encodeCanonicalJson({
        ...body,
        recipients: body.recipients.slice(0, 1),
      });
      const signedMessage = yield* signBody(
        group.sender,
        [group.sender],
        firstRecipientOnly,
      );

      const outcome = yield* openAs(group.sender, signedMessage).pipe(
        Effect.either,
      );

      expect(outcome).toStrictEqual(Either.left(new SealedBodyOpeningError()));
    }),
  ));

it("refuses a multi-recipient body that shares one ephemeral key through the protected header", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const group = yield* makeGroup(2);
      const sharedEpk = yield* sealWithSharedEphemeralKey(
        group.sender,
        group.peer,
      );
      const signedMessage = yield* signBody(
        group.sender,
        group.recipients,
        sharedEpk,
      );

      const outcome = yield* openAs(group.peer, signedMessage).pipe(
        Effect.either,
      );
      const joseOpened = yield* joseOpen(sharedEpk, group.peer);

      expect(outcome).toStrictEqual(Either.left(new SealedBodyOpeningError()));
      expect(joseOpened.subarray(SALT_BYTES)).toEqual(plaintext);
    }),
  ));

/**
 * Seals `plaintext` to the sender and peer with jose as `SealedBody.seal`
 * does, with `addedMembers` spread over the protected header: a new name is
 * appended and an existing name keeps its position with the new value.
 *
 * @param sender Sender and first recipient.
 * @param peer Second recipient.
 * @param addedMembers Protected-header members to add or override.
 * @returns Canonical bytes of the General JWE.
 */
const sealWithProtectedMembers = (
  sender: Member,
  peer: Member,
  addedMembers: Readonly<Record<string, string>>,
) =>
  Effect.gen(function* () {
    const { saltedPlaintext, commitment } = yield* commitTo(
      freshSalt(),
      plaintext,
    );
    const representation = yield* Effect.tryPromise({
      try: () =>
        new GeneralEncrypt(saltedPlaintext)
          .setProtectedHeader({
            alg: "ECDH-ES+A256KW",
            enc: "A256GCM",
            [COMMITMENT_HEADER]: commitment,
            [ENVELOPE_ID_HEADER]: envelopeIdOf(1),
            [SENDER_HEADER]: sender.identityCard.agentId,
            ...addedMembers,
          })
          .addRecipient(
            Either.getOrThrow(x25519PublicJwk(sender.identityCard.publicKey)),
          )
          .addRecipient(
            Either.getOrThrow(x25519PublicJwk(peer.identityCard.publicKey)),
          )
          .encrypt(),
      catch: () => new Error("hand sealing failed"),
    });
    return yield* encodeCanonicalJson(representation);
  });

interface ProtectedMembersCase {
  readonly added: string;
  readonly members: Readonly<Record<string, string>>;
  readonly result: string;
  readonly expected: Either.Either<Uint8Array, SealedBodyOpeningError>;
}

/**
 * Hand sealing authenticates the altered header. The positive control opens
 * it with jose, isolating the closed header grammar from content authentication.
 */
it.each<ProtectedMembersCase>([
  {
    added: "no member",
    members: {},
    result: "the plaintext",
    expected: Either.right(plaintext),
  },
  {
    added: "a content-type member",
    members: { cty: "text/plain" },
    result: "SealedBodyOpeningError",
    expected: Either.left(new SealedBodyOpeningError()),
  },
  {
    added: "a weaker key-wrap algorithm",
    members: { alg: "ECDH-ES+A128KW" },
    result: "SealedBodyOpeningError",
    expected: Either.left(new SealedBodyOpeningError()),
  },
])(
  "returns $result for a hand-sealed body whose protected header adds $added",
  ({ members, expected }) =>
    Effect.runPromise(
      Effect.gen(function* () {
        const group = yield* makeGroup(2);
        const sealed = yield* sealWithProtectedMembers(
          group.sender,
          group.peer,
          members,
        );
        const signedMessage = yield* signBody(
          group.sender,
          group.recipients,
          sealed,
        );

        const outcome = yield* openAs(group.peer, signedMessage).pipe(
          Effect.either,
        );
        const joseOpened = yield* joseOpen(sealed, group.peer);

        expect(outcome).toStrictEqual(expected);
        expect(joseOpened.subarray(SALT_BYTES)).toEqual(plaintext);
      }),
    ),
);

interface HeaderSpellingCase {
  readonly spelling: string;
  readonly headerText: (commitment: string, group: Group) => string;
  readonly expected: Either.Either<Uint8Array, SealedBodyOpeningError>;
}

/**
 * A256GCM authenticates any header bytes the sender chooses. Hand-built
 * headers exercise exact spelling separately from successful decryption.
 */
it.each<HeaderSpellingCase>([
  {
    spelling: "seal's own spelling",
    headerText: (commitment, group) =>
      honestHeaderText(commitment, group.sender),
    expected: Either.right(plaintext),
  },
  {
    spelling: "reordered members",
    headerText: (commitment, group) =>
      JSON.stringify({
        enc: "A256GCM",
        alg: "ECDH-ES+A256KW",
        [COMMITMENT_HEADER]: commitment,
        [ENVELOPE_ID_HEADER]: envelopeIdOf(1),
        [SENDER_HEADER]: group.sender.identityCard.agentId,
      }),
    expected: Either.left(new SealedBodyOpeningError()),
  },
  {
    spelling: "whitespace between members",
    headerText: (commitment, group) =>
      JSON.stringify(
        JSON.parse(honestHeaderText(commitment, group.sender)),
        null,
        1,
      ),
    expected: Either.left(new SealedBodyOpeningError()),
  },
  {
    spelling: "a repeated sender member",
    headerText: (commitment, group) =>
      `${honestHeaderText(commitment, group.peer).slice(0, -1)},${JSON.stringify(SENDER_HEADER)}:${JSON.stringify(group.sender.identityCard.agentId)}}`,
    expected: Either.left(new SealedBodyOpeningError()),
  },
  {
    spelling: "an escaped member name",
    headerText: (commitment, group) =>
      honestHeaderText(commitment, group.sender).replace(
        '"alg"',
        '"\\u0061lg"',
      ),
    expected: Either.left(new SealedBodyOpeningError()),
  },
  /**
   * The UTF-8 decoder strips a leading byte-order mark, so the decoded text
   * equals seal's spelling and only the byte comparison refuses it.
   */
  {
    spelling: "a leading byte-order mark",
    headerText: (commitment, group) =>
      `\uFEFF${honestHeaderText(commitment, group.sender)}`,
    expected: Either.left(new SealedBodyOpeningError()),
  },
])(
  "returns the expected outcome for a protected header with $spelling",
  ({ headerText, expected }) =>
    Effect.runPromise(
      Effect.gen(function* () {
        const group = yield* makeGroup(2);
        const { saltedPlaintext, commitment } = yield* commitTo(
          freshSalt(),
          plaintext,
        );
        const sealed = yield* sealManually({
          protectedHeaderText: headerText(commitment, group),
          saltedPlaintext,
          recipients: group.recipients.map((member) => member.identityCard),
        });
        const signedMessage = yield* signBody(
          group.sender,
          group.recipients,
          sealed,
        );

        const outcome = yield* openAs(group.peer, signedMessage).pipe(
          Effect.either,
        );
        const joseOpened = yield* joseOpen(sealed, group.peer);

        expect(outcome).toStrictEqual(expected);
        expect(joseOpened).toEqual(saltedPlaintext);
      }),
    ),
);

interface CommitmentCase {
  readonly mismatch: string;
  readonly committedSalt: Uint8Array;
  readonly committedPlaintext: Uint8Array;
  readonly encryptedSalt: Uint8Array;
  readonly encryptedPlaintext: Uint8Array;
  readonly expected: Either.Either<Uint8Array, SealedBodyOpeningError>;
}

const firstSalt = new Uint8Array(SALT_BYTES).fill(1);
const secondSalt = new Uint8Array(SALT_BYTES).fill(2);

/**
 * Hand sealing permits a valid ciphertext with a mismatched commitment or a
 * truncated salt. Successful jose decryption must not bypass either check.
 */
it.each<CommitmentCase>([
  {
    mismatch: "no mismatch",
    committedSalt: firstSalt,
    committedPlaintext: plaintext,
    encryptedSalt: firstSalt,
    encryptedPlaintext: plaintext,
    expected: Either.right(plaintext),
  },
  {
    mismatch: "a commitment to another plaintext",
    committedSalt: firstSalt,
    committedPlaintext: utf8Encoder.encode("another outer body"),
    encryptedSalt: firstSalt,
    encryptedPlaintext: plaintext,
    expected: Either.left(new SealedBodyOpeningError()),
  },
  {
    mismatch: "a swapped salt",
    committedSalt: firstSalt,
    committedPlaintext: plaintext,
    encryptedSalt: secondSalt,
    encryptedPlaintext: plaintext,
    expected: Either.left(new SealedBodyOpeningError()),
  },
  {
    mismatch: "a salted plaintext shorter than the salt",
    committedSalt: firstSalt.subarray(0, 16),
    committedPlaintext: new Uint8Array(),
    encryptedSalt: firstSalt.subarray(0, 16),
    encryptedPlaintext: new Uint8Array(),
    expected: Either.left(new SealedBodyOpeningError()),
  },
])(
  "returns the expected outcome for a hand-sealed body with $mismatch",
  (input) =>
    Effect.runPromise(
      Effect.gen(function* () {
        const group = yield* makeGroup(2);
        const { commitment } = yield* commitTo(
          input.committedSalt,
          input.committedPlaintext,
        );
        const { saltedPlaintext } = yield* commitTo(
          input.encryptedSalt,
          input.encryptedPlaintext,
        );
        const sealed = yield* sealManually({
          protectedHeaderText: honestHeaderText(commitment, group.sender),
          saltedPlaintext,
          recipients: group.recipients.map((member) => member.identityCard),
        });
        const signedMessage = yield* signBody(
          group.sender,
          group.recipients,
          sealed,
        );

        const outcome = yield* openAs(group.peer, signedMessage).pipe(
          Effect.either,
        );
        const joseOpened = yield* joseOpen(sealed, group.peer);

        expect(outcome).toStrictEqual(input.expected);
        expect(joseOpened).toEqual(saltedPlaintext);
      }),
    ),
);

/**
 * A256GCM alone accepts this body under two keys and yields different
 * plaintexts. The commitment must prevent either recipient from accepting it.
 */
it("refuses a two-key body whose entries wrap different content keys over one colliding ciphertext", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const group = yield* makeGroup(2);
      const { commitment } = yield* commitTo(freshSalt(), plaintext);
      const sealed = yield* sealTwoKeyCollision({
        protectedHeaderText: honestHeaderText(commitment, group.sender),
        recipients: [group.sender.identityCard, group.peer.identityCard],
        ciphertextBytes: 64,
      });
      const signedMessage = yield* signBody(
        group.sender,
        group.recipients,
        sealed,
      );

      const joseOpened = yield* Effect.forEach(
        group.recipients,
        (member) => joseOpen(sealed, member),
        { concurrency: 1 },
      );
      const outcomes = yield* Effect.forEach(
        group.recipients,
        (member) => openAs(member, signedMessage).pipe(Effect.either),
        { concurrency: 1 },
      );

      expect(joseOpened[0]).not.toEqual(joseOpened[1]);
      expect(outcomes).toStrictEqual([
        Either.left(new SealedBodyOpeningError()),
        Either.left(new SealedBodyOpeningError()),
      ]);
    }),
  ));

interface OuterEnvelopeIdCase {
  readonly outer: string;
  readonly outerEnvelopeIdByte: number;
  readonly result: string;
  readonly expected: Either.Either<Uint8Array, SealedBodyOpeningError>;
}

/**
 * The sender can sign unchanged sealed bytes under another EnvelopeId. Opening
 * must bind the protected-header id to the verified outer message.
 */
it.each<OuterEnvelopeIdCase>([
  {
    outer: "the EnvelopeId they were sealed for",
    outerEnvelopeIdByte: 2,
    result: "the plaintext",
    expected: Either.right(plaintext),
  },
  {
    outer: "a different EnvelopeId",
    outerEnvelopeIdByte: 3,
    result: "SealedBodyOpeningError",
    expected: Either.left(new SealedBodyOpeningError()),
  },
])(
  "returns $result for sealed bytes their sender signs under $outer",
  ({ outerEnvelopeIdByte, expected }) =>
    Effect.runPromise(
      Effect.gen(function* () {
        const group = yield* makeGroup(3);
        const sealed = yield* sealFrom(
          group.sender,
          group.recipients,
          plaintext,
          2,
        );
        const signedMessage = yield* signBody(
          group.sender,
          group.recipients,
          sealed,
          outerEnvelopeIdByte,
        );
        const received = yield* receive(
          signedMessage,
          group.sender.identityCard,
        );

        const outcome = yield* openAs(group.peer, received).pipe(Effect.either);

        expect(received.envelopeId).toBe(envelopeIdOf(outerEnvelopeIdByte));
        expect(outcome).toStrictEqual(expected);
      }),
    ),
);

it(
  "refuses to seal to no recipients, a repeated card or AgentId, or more than 128 recipients",
  () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const group = yield* makeGroup(129);
        const sameAgentIdOtherKey = yield* makeMember(
          group.registrySigningAuthority,
          1,
        );

        const outcomes = yield* Effect.forEach(
          [
            [],
            [group.sender, group.sender],
            [group.sender, sameAgentIdOtherKey],
            group.recipients,
          ],
          (recipients) =>
            sealFrom(group.sender, recipients, plaintext).pipe(Effect.either),
          { concurrency: 1 },
        );

        expect(outcomes).toStrictEqual([
          Either.left(new SealedBodySealingError()),
          Either.left(new SealedBodySealingError()),
          Either.left(new SealedBodySealingError()),
          Either.left(new SealedBodySealingError()),
        ]);
      }),
    ),
  KEY_AGREEMENT_HEAVY_TIMEOUT_MS,
);

it("refuses to seal to an AgentIdentityCard key that is not an Ed25519 curve point", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const group = yield* makeGroup(1);
      const offCurveCard = yield* issueCard(
        group.registrySigningAuthority,
        9,
        Schema.decodeUnknownSync(AgentPublicKey)({
          crv: "Ed25519",
          kty: "OKP",
          x: "AgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        }),
      );

      const outcome = yield* SealedBody.seal({
        senderAgentId: group.sender.identityCard.agentId,
        recipientIdentityCards: [group.sender.identityCard, offCurveCard],
        envelopeId: envelopeIdOf(1),
        plaintext,
      }).pipe(Effect.either);

      expect(outcome).toStrictEqual(Either.left(new SealedBodySealingError()));
    }),
  ));

const SIGNED_MESSAGE_BODY_CAP = 262_144;

/**
 * Seals `bytes` from the sender to the group, then checks that the peer opens
 * exactly `bytes` and that `sealedByteLength` reports the sealed length.
 *
 * @param group Sender, peer, and recipients.
 * @param bytes Plaintext to seal.
 * @returns Completion once both checks pass.
 */
const expectExactRoundTrip = (group: Group, bytes: Uint8Array) =>
  Effect.gen(function* () {
    const sealed = yield* sealFrom(group.sender, group.recipients, bytes);
    const signedMessage = yield* signBody(
      group.sender,
      group.recipients,
      sealed,
    );

    expect(yield* openAs(group.peer, signedMessage)).toEqual(bytes);
    expect(
      SealedBody.sealedByteLength({
        plaintextByteLength: bytes.byteLength,
        recipientCount: group.recipients.length,
      }),
    ).toStrictEqual(Option.some(sealed.byteLength));
  });

/**
 * Runs `expectExactRoundTrip` over generated plaintexts of 0 to 2,048 bytes,
 * always including the empty plaintext, whose salted form is exactly the salt
 * and so sits on the boundary of the short-plaintext refusal.
 *
 * @param group Sender, peer, and recipients.
 * @returns The fast-check run, rejecting with the shrunk counterexample.
 */
const generatedPlaintextsRoundTrip = (group: Group) =>
  fc.assert(
    fc.asyncProperty(fc.uint8Array({ maxLength: 2048 }), (bytes) =>
      Effect.runPromise(expectExactRoundTrip(group, bytes)),
    ),
    { numRuns: 16, examples: [[new Uint8Array()]] },
  );

it(
  "opens every generated plaintext exactly and seals it to the reported length",
  () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const group = yield* makeGroup(3);
        yield* Effect.tryPromise({
          try: () => generatedPlaintextsRoundTrip(group),
          catch: (counterexample) => counterexample,
        });
      }),
    ),
  KEY_AGREEMENT_HEAVY_TIMEOUT_MS,
);

it.each([1, 2, 32, 128])(
  "seals and opens the reported largest plaintext for %i recipients within the body cap, and one more byte exceeds it",
  (recipientCount) =>
    Effect.runPromise(
      Effect.gen(function* () {
        const group = yield* makeGroup(recipientCount);
        const largestPlaintextBytes = Option.getOrThrow(
          SealedBody.maximumPlaintextByteLength(recipientCount),
        );

        const largest = yield* sealFrom(
          group.sender,
          group.recipients,
          new Uint8Array(largestPlaintextBytes),
        );
        const oneMore = yield* sealFrom(
          group.sender,
          group.recipients,
          new Uint8Array(largestPlaintextBytes + 1),
        );
        const signedMessage = yield* signBody(
          group.sender,
          group.recipients,
          largest,
        );

        expect((yield* openAs(group.sender, signedMessage)).byteLength).toBe(
          largestPlaintextBytes,
        );
        expect(largest.byteLength).toBeLessThanOrEqual(SIGNED_MESSAGE_BODY_CAP);
        expect(oneMore.byteLength).toBeGreaterThan(SIGNED_MESSAGE_BODY_CAP);
        expect(
          yield* signBody(group.sender, group.recipients, oneMore).pipe(
            Effect.either,
          ),
        ).toStrictEqual(Either.left(new SignedMessageSigningError()));
        expect(
          SealedBody.sealedByteLength({
            plaintextByteLength: largestPlaintextBytes,
            recipientCount,
          }),
        ).toStrictEqual(Option.some(largest.byteLength));
      }),
    ),
  KEY_AGREEMENT_HEAVY_TIMEOUT_MS,
);

it.each([
  { plaintextByteLength: 0, recipientCount: 0 },
  { plaintextByteLength: 0, recipientCount: 129 },
  { plaintextByteLength: 0, recipientCount: 1.5 },
  { plaintextByteLength: -1, recipientCount: 1 },
  { plaintextByteLength: 0.5, recipientCount: 1 },
  { plaintextByteLength: Number.MAX_SAFE_INTEGER, recipientCount: 1 },
])(
  "reports no sealed length for $plaintextByteLength plaintext bytes to $recipientCount recipients",
  (input) => {
    expect(SealedBody.sealedByteLength(input)).toStrictEqual(Option.none());
  },
);

it.each([0, 129, 2.5])(
  "reports no largest plaintext for %d recipients",
  (recipientCount) => {
    expect(SealedBody.maximumPlaintextByteLength(recipientCount)).toStrictEqual(
      Option.none(),
    );
  },
);
