/**
 * @file SealedBody round trips, sender and EnvelopeId binding, commitment,
 * header exactness, and refusals.
 */

import { Effect, Either, Encoding, Schema } from "effect";
import * as fc from "fast-check";
import {
  exportJWK,
  generalDecrypt,
  GeneralEncrypt,
  generateKeyPair,
} from "jose";
import { expect, it } from "vitest";
import type { VerifiedAgentIdentityCard } from "../../agent.js";
import { encodeCanonicalJson } from "../../../encoding/canonical-json.js";
import {
  agentOpeningPrivateKey,
  AgentPublicKey,
  x25519PublicJwk,
} from "../../keys.js";
import {
  SealedBody,
  SealedBodyOpeningError,
  SealedBodySealingError,
} from "../sealed-body.js";
import {
  SignedMessage,
  type VerifiedSignedMessage,
} from "../signed-message.js";
import {
  commitTo,
  sealManually,
  sealTwoKeyCollision,
} from "./forged-sealed-bodies.js";
import {
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
  signBody,
} from "./sealed-body-fixtures.js";

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

const rawProtectedHeader = Schema.Record({
  key: Schema.String,
  value: Schema.Unknown,
});

const readSealedBody = (sealed: Uint8Array): RawSealedBody =>
  Schema.decodeUnknownSync(Schema.parseJson(rawSealedBody))(
    utf8Decoder.decode(sealed),
  );

const readProtectedHeader = (body: RawSealedBody) =>
  Schema.decodeUnknownSync(Schema.parseJson(rawProtectedHeader))(
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
 * Seals `plaintext` to the sender and peer with one ephemeral key named in
 * the protected header, spelled as seal spells a single-recipient header, then
 * drops the per-recipient copies jose adds.
 *
 * @param sender Sender and first recipient.
 * @param peer Second recipient.
 * @returns Canonical bytes of a JWE that jose opens but SealedBody refuses.
 */
const sealWithSharedEphemeralKey = (sender: Member, peer: Member) =>
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
