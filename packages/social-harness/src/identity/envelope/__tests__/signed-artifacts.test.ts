/** @file AgentIdentityCard and SignedMessage immutability, representation, bounds, and signature tests. */

import { ed25519 } from "@noble/curves/ed25519.js";
import { Effect, Either, Encoding, Redacted, Schema } from "effect";
import * as fc from "fast-check";
import { createHash, generateKeyPairSync } from "node:crypto";
import { expect, it } from "vitest";
import {
  AgentIdentityCardIssuedAt,
  makeVerifiedCard,
} from "../../__tests__/agent-card-fixtures.js";
import {
  decodeCanonicalJson,
  encodeCanonicalJson,
} from "../../../encoding/canonical-json.js";
import {
  AgentId,
  AgentIdentityCard,
  AgentIdentityCardVerificationError,
  AgentName,
  digestAgentIdentityCard,
  PrincipalId,
  type VerifiedAgentIdentityCard,
} from "../../agent.js";
import {
  AgentPublicKey,
  AgentSigningAuthority,
  agentSigningPrivateKey,
  ed25519PublicKeyThumbprintUri,
} from "../../keys.js";
import { WIRE_VERSION } from "../../wire-version.js";
import {
  EnvelopeId,
  SignedMessage,
  SignedMessageSigningError,
  type SignedMessage as SignedMessageValue,
} from "../signed-message.js";

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
const rawProtectedHeader = Schema.Struct({
  alg: Schema.String,
  kid: Schema.String,
  typ: Schema.String,
});

type RawRepresentation = typeof rawRepresentation.Type;
type RawPayload = typeof rawPayload.Type;
type RawProtectedHeader = typeof rawProtectedHeader.Type;

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

const agentCardRepresentation = (identityCard: VerifiedAgentIdentityCard) =>
  Schema.encode(AgentIdentityCard)(identityCard).pipe(
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
    const parsed = yield* decodeCanonicalJson(rawProtectedHeader, bytes);
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

it("keeps encoded artifacts unchanged when callers mutate public views", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const fixture = yield* makeSnapshotFixture;
      const cardBefore = yield* Schema.encode(AgentIdentityCard)(
        fixture.identityCard,
      );
      const messageBefore = yield* messageRepresentation(fixture.signedMessage);

      expect(
        Reflect.set(fixture.identityCard.publicKey, "x", "A".repeat(43)),
      ).toBe(false);
      expect(
        Reflect.set(
          fixture.signedMessage.recipientAgentIds,
          "0",
          fixture.secondRecipient,
        ),
      ).toBe(false);
      const body = fixture.signedMessage.body;
      body.fill(0);

      const cardAfter = yield* Schema.encode(AgentIdentityCard)(
        fixture.identityCard,
      );
      const messageAfter = yield* messageRepresentation(fixture.signedMessage);
      expect(cardAfter).toStrictEqual(cardBefore);
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

/**
 * A Proxy over a valid key passes the AgentPublicKey Schema, which inspects
 * property descriptors only, yet throws when its `x` is read.
 */
it("fails AgentIdentityCard verification with its typed error when the Registry key cannot be read", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const fixture = yield* makeIdentityFixture;
      const registrySignerPublicKey = Schema.validateSync(AgentPublicKey)(
        new Proxy(fixture.registrySignerPublicKey, {
          get: () => {
            throw new Error("unreadable key");
          },
        }),
      );

      const outcome = yield* AgentIdentityCard.verify({
        identityCard: fixture.identityCard,
        registrySignerPublicKey,
      }).pipe(Effect.either);

      expect(outcome).toStrictEqual(
        Either.left(new AgentIdentityCardVerificationError()),
      );
    }),
  ));

it("fails signing with its typed error when the sender's AgentIdentityCard cannot be read", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const fixture = yield* makeIdentityFixture;
      const identityCard = new Proxy(fixture.identityCard, {
        get: () => {
          throw new Error("unreadable card");
        },
      });

      const outcome = yield* SignedMessage.sign({
        identityCard,
        signingAuthority: fixture.agentSigningAuthority,
        recipientAgentIds: recipientSet(1),
        envelopeId: makeEnvelopeId(1),
        body: Uint8Array.from([1]),
      }).pipe(Effect.either);

      expect(outcome).toStrictEqual(
        Either.left(new SignedMessageSigningError()),
      );
    }),
  ));

it("rejects the identity-point universal signature on every signed artifact", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const fixture = yield* makeSnapshotFixture;
      const universalSignature = new Uint8Array(64);
      universalSignature[0] = 1;
      const [identityCard, signedMessage] = yield* Effect.all(
        [
          agentCardRepresentation(fixture.identityCard),
          messageRepresentation(fixture.signedMessage),
        ] as const,
        { concurrency: 2 },
      );

      const parsedCard = yield* Schema.decodeUnknown(AgentIdentityCard)(
        replaceSignature(identityCard, universalSignature),
      );
      const parsedMessage = yield* Schema.decodeUnknown(SignedMessage)(
        replaceSignature(signedMessage, universalSignature),
      );
      const cardResult = yield* AgentIdentityCard.verify({
        identityCard: parsedCard,
        registrySignerPublicKey: fixture.registrySignerPublicKey,
      }).pipe(Effect.either);
      const messageResult = yield* SignedMessage.verify({
        signedMessage: parsedMessage,
        identityCard: fixture.identityCard,
      }).pipe(Effect.either);

      expect(wasRejected(cardResult)).toBe(true);
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
