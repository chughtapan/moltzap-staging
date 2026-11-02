/**
 * @file Sealed SignedMessage bodies: multi-recipient JWE bound to the sender
 * AgentId and outer EnvelopeId and committed to one plaintext.
 */

import { Data, Effect, Either, Encoding, Option, Schema } from "effect";
import { generalDecrypt, GeneralEncrypt } from "jose";
import {
  decodeCanonicalJson,
  encodeCanonicalJson,
  exactStruct,
} from "../../encoding/canonical-json.js";
import {
  compareAgentIds,
  decodeCanonicalBase64Url,
  hasCanonicalBase64UrlLength,
} from "../../encoding/identifier.js";
import { AgentId, type VerifiedAgentIdentityCard } from "../agent.js";
import {
  agentOpeningPrivateKey,
  AgentSigningAuthority,
  sameEd25519PublicKey,
  SEALED_BODY_KEY_MANAGEMENT_ALGORITHM,
  x25519SealingKey,
} from "../keys.js";
import {
  EnvelopeId,
  isParsedSignedMessage,
  MAXIMUM_BODY_BYTES,
  MAXIMUM_RECIPIENTS,
  type VerifiedSignedMessage,
} from "./signed-message.js";

const CONTENT_ENCRYPTION_ALGORITHM = "A256GCM";
const COMMITMENT_HEADER = "org.social-harness/commitment";
const ENVELOPE_ID_HEADER = "org.social-harness/envelope-id";
const SENDER_HEADER = "org.social-harness/sender";
const COMMITMENT_BYTES = 32;
const IV_BYTES = 12;
const TAG_BYTES = 16;
const WRAPPED_KEY_BYTES = 40;
const X25519_PUBLIC_KEY_BYTES = 32;

/**
 * A random salt precedes the plaintext inside the ciphertext, so the
 * commitment in the protected header reveals nothing about the plaintext.
 */
const SALT_BYTES = 32;

const utf8Decoder = new TextDecoder("utf-8", { fatal: true });

const canonicalBase64Url = Schema.String.pipe(
  Schema.filter((value) => decodeCanonicalBase64Url(value) !== undefined),
);

const encodedBytes = (byteLength: number) =>
  Schema.String.pipe(
    Schema.filter((value) => hasCanonicalBase64UrlLength(value, byteLength)),
  );

const encodedCommitment = encodedBytes(COMMITMENT_BYTES);

const ephemeralPublicKey = exactStruct({
  crv: Schema.Literal("X25519"),
  kty: Schema.Literal("OKP"),
  x: encodedBytes(X25519_PUBLIC_KEY_BYTES),
});

const sealedBodyRepresentation = exactStruct({
  ciphertext: canonicalBase64Url,
  iv: encodedBytes(IV_BYTES),
  protected: canonicalBase64Url,
  recipients: Schema.Array(
    exactStruct({
      encrypted_key: encodedBytes(WRAPPED_KEY_BYTES),
      header: Schema.optional(exactStruct({ epk: ephemeralPublicKey })),
    }),
  ).pipe(Schema.minItems(1), Schema.maxItems(MAXIMUM_RECIPIENTS)),
  tag: encodedBytes(TAG_BYTES),
});

type SealedBodyRepresentation = typeof sealedBodyRepresentation.Type;

const protectedHeader = exactStruct({
  alg: Schema.Literal(SEALED_BODY_KEY_MANAGEMENT_ALGORITHM),
  enc: Schema.Literal(CONTENT_ENCRYPTION_ALGORITHM),
  epk: Schema.optional(ephemeralPublicKey),
  [COMMITMENT_HEADER]: encodedCommitment,
  [ENVELOPE_ID_HEADER]: EnvelopeId,
  [SENDER_HEADER]: AgentId,
});

type ProtectedHeader = typeof protectedHeader.Type;

const protectedHeaderJson = Schema.parseJson(protectedHeader);

/**
 * Builds the protected-header members in their one accepted order. The sealer
 * hands jose this object, and the opener rebuilds it to require that exact
 * spelling.
 *
 * @param commitment Base64url SHA-256 of the salted plaintext.
 * @param envelopeId Outer SignedMessage EnvelopeId bound by the header.
 * @param senderAgentId Sender bound by the header.
 * @returns The members in serialization order.
 */
const protectedHeaderMembers = (
  commitment: string,
  envelopeId: EnvelopeId,
  senderAgentId: AgentId,
) => ({
  alg: SEALED_BODY_KEY_MANAGEMENT_ALGORITHM,
  enc: CONTENT_ENCRYPTION_ALGORITHM,
  [COMMITMENT_HEADER]: commitment,
  [ENVELOPE_ID_HEADER]: envelopeId,
  [SENDER_HEADER]: senderAgentId,
});

/** Fixed-width members use the same protected-header builder as actual sealing. */
const sealedRepresentationSizes = (() => {
  const encoded = (length: number) =>
    Encoding.encodeBase64Url(new Uint8Array(length));
  const epk = {
    kty: "OKP",
    crv: "X25519",
    x: encoded(X25519_PUBLIC_KEY_BYTES),
  };
  const header = protectedHeaderMembers(
    encoded(COMMITMENT_BYTES),
    Schema.decodeUnknownSync(EnvelopeId)(`env_${encoded(16)}`),
    Schema.decodeUnknownSync(AgentId)(`agt_${encoded(16)}`),
  );
  const bytes = (value: object) =>
    new TextEncoder().encode(JSON.stringify(value)).byteLength;
  const entry = { encrypted_key: encoded(WRAPPED_KEY_BYTES), header: { epk } };
  const representation = (
    protectedMembers: object,
    recipients: readonly object[],
  ) => ({
    ciphertext: "",
    iv: encoded(IV_BYTES),
    protected: Encoding.encodeBase64Url(
      new TextEncoder().encode(JSON.stringify(protectedMembers)),
    ),
    recipients,
    tag: encoded(TAG_BYTES),
  });
  return {
    single: bytes(
      representation({ ...header, epk }, [
        { encrypted_key: encoded(WRAPPED_KEY_BYTES) },
      ]),
    ),
    multiple: bytes(representation(header, [])) - 1,
    recipient: bytes(entry) + 1,
  };
})();
const SINGLE_RECIPIENT_FIXED_BYTES = sealedRepresentationSizes.single;
const MULTIPLE_RECIPIENT_FIXED_BYTES = sealedRepresentationSizes.multiple;
const RECIPIENT_ENTRY_BYTES = sealedRepresentationSizes.recipient;

/**
 * Commits to the salted plaintext. A256GCM does not bind the content key to
 * the ciphertext, so a sender could wrap a different key for each recipient
 * over one ciphertext and tag that authenticate under both. The commitment in
 * the authenticated header lets at most one plaintext open.
 *
 * @param saltedPlaintext Salt followed by the plaintext.
 * @param failure Error for a digest failure.
 * @returns The base64url SHA-256 digest.
 */
const commitmentOf = <E>(saltedPlaintext: Uint8Array, failure: () => E) =>
  Effect.tryPromise({
    try: () => crypto.subtle.digest("SHA-256", new Uint8Array(saltedPlaintext)),
    catch: failure,
  }).pipe(
    Effect.map((digest) => Encoding.encodeBase64Url(new Uint8Array(digest))),
  );

/** A body cannot be sealed from the supplied sender to the supplied recipients. */
export class SealedBodySealingError extends Data.TaggedError(
  "SealedBodySealingError",
) {}

/** A verified SignedMessage body does not open as a sealed body for this agent. */
export class SealedBodyOpeningError extends Data.TaggedError(
  "SealedBodyOpeningError",
) {}

const sealingFailure = (): SealedBodySealingError =>
  new SealedBodySealingError();

const openingFailure = (): SealedBodyOpeningError =>
  new SealedBodyOpeningError();

interface SealInput {
  readonly senderAgentId: AgentId;
  readonly recipientIdentityCards: readonly VerifiedAgentIdentityCard[];
  readonly envelopeId: EnvelopeId;
  readonly plaintext: Uint8Array;
}

const isRecipientCount = (recipientCount: number): boolean =>
  Number.isInteger(recipientCount) &&
  recipientCount >= 1 &&
  recipientCount <= MAXIMUM_RECIPIENTS;

type CardMembers = Pick<VerifiedAgentIdentityCard, "agentId" | "publicKey">;

/**
 * Copies the members sealing and opening read. Callers run it under a try, so
 * a card that throws on read fails with the caller's typed error.
 */
const cardMembers = (card: VerifiedAgentIdentityCard): CardMembers => ({
  agentId: card.agentId,
  publicKey: card.publicKey,
});

const snapshotRecipients = (
  recipientIdentityCards: readonly VerifiedAgentIdentityCard[],
): Effect.Effect<readonly CardMembers[], SealedBodySealingError> =>
  Effect.gen(function* () {
    const recipients = yield* Effect.try({
      try: () =>
        Array.from(recipientIdentityCards, (card) => cardMembers(card)),
      catch: sealingFailure,
    });
    const distinctAgentIds = new Set(
      recipients.map((recipient) => recipient.agentId),
    );
    if (
      !isRecipientCount(recipients.length) ||
      distinctAgentIds.size !== recipients.length
    ) {
      return yield* new SealedBodySealingError();
    }
    return recipients.sort((left, right) =>
      compareAgentIds(left.agentId, right.agentId),
    );
  });

/**
 * Encrypts one plaintext to every recipient's AgentIdentityCard key, binds the sender
 * AgentId and the outer EnvelopeId in the protected header, and commits the
 * header to the salted plaintext.
 *
 * Each recipient entry carries its own ephemeral key, which is the layout
 * jose's supported multi-recipient ECDH-ES+A256KW API produces.
 *
 * Recipient entries follow the canonical SignedMessage recipient order, so the
 * caller signs the returned bytes as a SignedMessage body from the same sender
 * under the same EnvelopeId to exactly these recipients. A sender that must
 * read its own body includes its card among the recipients.
 *
 * @param input Sender AgentId, 1 to 128 distinct verified recipient cards,
 * the EnvelopeId the caller passes to `SignedMessage.sign`, and the plaintext
 * to seal.
 * @returns The canonical JSON bytes of the General JWE.
 */
/**
 * Reads the caller's SealInput once under a try, copying the plaintext, so an
 * input whose members throw fails as a sealing error and later steps read only
 * this copy.
 */
const snapshotSealInput = (
  input: SealInput,
): Effect.Effect<SealInput, SealedBodySealingError> =>
  Effect.try({
    try: () => {
      const { recipientIdentityCards, senderAgentId, envelopeId, plaintext } =
        input;
      return {
        recipientIdentityCards,
        senderAgentId,
        envelopeId,
        plaintext: new Uint8Array(plaintext),
      };
    },
    catch: sealingFailure,
  });

const seal = (
  input: SealInput,
): Effect.Effect<Uint8Array, SealedBodySealingError> =>
  Effect.gen(function* () {
    const sealing = yield* snapshotSealInput(input);
    const recipients = yield* snapshotRecipients(
      sealing.recipientIdentityCards,
    );
    const saltedPlaintext = yield* Effect.try({
      try: () => {
        const bytes = new Uint8Array(SALT_BYTES + sealing.plaintext.byteLength);
        bytes.set(crypto.getRandomValues(new Uint8Array(SALT_BYTES)));
        bytes.set(sealing.plaintext, SALT_BYTES);
        return bytes;
      },
      catch: sealingFailure,
    });
    const commitment = yield* commitmentOf(saltedPlaintext, sealingFailure);
    const recipientKeys = yield* Effect.forEach(
      recipients,
      (recipient) => x25519SealingKey(recipient.publicKey),
      { concurrency: 1 },
    ).pipe(Effect.mapError(sealingFailure));
    const encryption = new GeneralEncrypt(saltedPlaintext).setProtectedHeader(
      protectedHeaderMembers(
        commitment,
        sealing.envelopeId,
        sealing.senderAgentId,
      ),
    );
    for (const recipientKey of recipientKeys) {
      encryption.addRecipient(recipientKey);
    }
    const representation = yield* Effect.tryPromise({
      try: () => encryption.encrypt(),
      catch: sealingFailure,
    });
    return yield* encodeCanonicalJson(representation).pipe(
      Effect.mapError(sealingFailure),
    );
  });

/**
 * Rebuilds the one spelling jose produces for a header: `JSON.stringify` of
 * the members in `protectedHeaderMembers` order, followed for a single
 * recipient by the `epk` that jose appends with members `x`, `crv`, `kty`.
 *
 * @param header Decoded protected header.
 * @returns The exact header text a sealer produces.
 */
const expectedHeaderText = (header: ProtectedHeader): string => {
  const members = protectedHeaderMembers(
    header[COMMITMENT_HEADER],
    header[ENVELOPE_ID_HEADER],
    header[SENDER_HEADER],
  );
  return JSON.stringify(
    header.epk === undefined
      ? members
      : {
          ...members,
          epk: { x: header.epk.x, crv: header.epk.crv, kty: header.epk.kty },
        },
  );
};

/**
 * Decodes the protected header and requires the exact bytes a sealer writes.
 *
 * The header is not JCS for a single recipient, because jose appends `epk`
 * after the other members. A256GCM authenticates whatever bytes the sender
 * chose, so the byte comparison is what stops a sender from respelling the
 * header with reordered, repeated, escaped, or spaced members. It compares
 * the encoded bytes rather than the decoded text, because the UTF-8 decoder
 * strips a leading byte-order mark.
 *
 * @param encodedHeader The canonical base64url `protected` member of the
 * General JWE.
 * @returns The decoded header.
 */
const decodeProtectedHeader = (
  encodedHeader: string,
): Effect.Effect<ProtectedHeader, SealedBodyOpeningError> =>
  Effect.gen(function* () {
    const headerText = yield* Effect.try({
      try: () =>
        utf8Decoder.decode(
          Encoding.decodeBase64Url(encodedHeader).pipe(Either.getOrThrow),
        ),
      catch: openingFailure,
    });
    const header = yield* Schema.decodeUnknown(protectedHeaderJson)(
      headerText,
      { exact: true, onExcessProperty: "error" },
    ).pipe(Effect.mapError(openingFailure));
    if (
      Encoding.encodeBase64Url(expectedHeaderText(header)) !== encodedHeader
    ) {
      return yield* new SealedBodyOpeningError();
    }
    return header;
  });

type RecipientEntry = SealedBodyRepresentation["recipients"][number];

/**
 * Selects the one entry sealed to `agentId` after requiring the one
 * ephemeral-key placement the sealer produces: the protected header carries it
 * for a single recipient, and each recipient header carries its own otherwise.
 * Entries follow the canonical SignedMessage recipient order, so the agent's
 * position in that list is its entry and no other entry is tried.
 */
const localRecipientEntry = (
  representation: SealedBodyRepresentation,
  header: ProtectedHeader,
  recipientAgentIds: readonly AgentId[],
  agentId: AgentId,
): Effect.Effect<RecipientEntry, SealedBodyOpeningError> => {
  const { recipients } = representation;
  const single = recipients.length === 1;
  const entry = recipients[recipientAgentIds.indexOf(agentId)];
  const validPlacement =
    (header.epk !== undefined) === single &&
    recipients.every(
      (recipient) => (recipient.header === undefined) === single,
    );
  return recipients.length === recipientAgentIds.length &&
    entry !== undefined &&
    validPlacement
    ? Effect.succeed(entry)
    : Effect.fail(new SealedBodyOpeningError());
};

interface OpenInput {
  readonly identityCard: VerifiedAgentIdentityCard;
  readonly signingAuthority: AgentSigningAuthority;
  readonly signedMessage: VerifiedSignedMessage;
}

/**
 * Copies the local card's members under a try and requires its key to be the
 * authority's, so a card that throws on read or belongs to another agent fails
 * with `SealedBodyOpeningError`.
 */
const authorityCard = (
  input: OpenInput,
): Effect.Effect<CardMembers, SealedBodyOpeningError> =>
  Effect.gen(function* () {
    const card = yield* Effect.try({
      try: () => cardMembers(input.identityCard),
      catch: openingFailure,
    });
    const sameKey = yield* sameEd25519PublicKey(
      () => AgentSigningAuthority.publicKey(input.signingAuthority),
      () => card.publicKey,
    ).pipe(Effect.mapError(openingFailure));
    if (!sameKey) {
      return yield* new SealedBodyOpeningError();
    }
    return card;
  });

/**
 * Reads the caller's OpenInput once under a try, so an input whose members
 * throw fails as an opening error and later steps read only this copy.
 */
const snapshotOpenInput = (
  input: OpenInput,
): Effect.Effect<OpenInput, SealedBodyOpeningError> =>
  Effect.try({
    try: () => {
      const { identityCard, signingAuthority, signedMessage } = input;
      return { identityCard, signingAuthority, signedMessage };
    },
    catch: openingFailure,
  });

/**
 * Decrypts a verified SignedMessage body sealed to the local agent.
 *
 * It first refuses a SignedMessage this package did not parse, so a view whose
 * member reads throw fails as an opening error, and an AgentIdentityCard whose key is
 * not the authority's Ed25519 key.
 * A successful unwrap cannot prove that pairing: a sender may wrap any entry
 * to any key, and an Ed25519 key and its negation share one X25519 key.
 *
 * It then refuses a body that is not an exact sealed body, a protected-header
 * sender or EnvelopeId that differs from the verified SignedMessage sender or
 * EnvelopeId, an entry count that differs from the SignedMessage recipient
 * count, an agent the SignedMessage does not name, any authentication failure,
 * and a decryption that does not match the header commitment. Every recipient
 * that opens a given body therefore obtains the same plaintext, but a sender
 * can still make a body open for some recipients and not others. The sealed
 * bytes open only inside a SignedMessage from the sender and EnvelopeId they
 * were sealed for.
 *
 * @param input The local agent's AgentIdentityCard and authority, and the verified
 * SignedMessage.
 * @returns The sealed plaintext.
 */
const open = (
  input: OpenInput,
): Effect.Effect<Uint8Array, SealedBodyOpeningError> =>
  Effect.gen(function* () {
    const opening = yield* snapshotOpenInput(input);
    if (!isParsedSignedMessage(opening.signedMessage)) {
      return yield* new SealedBodyOpeningError();
    }
    const card = yield* authorityCard(opening);
    const representation = yield* decodeCanonicalJson(
      sealedBodyRepresentation,
      opening.signedMessage.body,
    ).pipe(Effect.mapError(openingFailure));
    const header = yield* decodeProtectedHeader(representation.protected);
    const entry = yield* localRecipientEntry(
      representation,
      header,
      opening.signedMessage.recipientAgentIds,
      card.agentId,
    );
    if (
      header[SENDER_HEADER] !== opening.signedMessage.senderAgentId ||
      header[ENVELOPE_ID_HEADER] !== opening.signedMessage.envelopeId
    ) {
      return yield* new SealedBodyOpeningError();
    }
    const decrypted = yield* Effect.tryPromise({
      try: () =>
        generalDecrypt(
          { ...representation, recipients: [entry] },
          agentOpeningPrivateKey(opening.signingAuthority),
          {
            keyManagementAlgorithms: [SEALED_BODY_KEY_MANAGEMENT_ALGORITHM],
            contentEncryptionAlgorithms: [CONTENT_ENCRYPTION_ALGORITHM],
          },
        ),
      catch: openingFailure,
    });
    const commitment = yield* commitmentOf(decrypted.plaintext, openingFailure);
    if (
      decrypted.plaintext.byteLength < SALT_BYTES ||
      commitment !== header[COMMITMENT_HEADER]
    ) {
      return yield* new SealedBodyOpeningError();
    }
    return decrypted.plaintext.slice(SALT_BYTES);
  });

const fixedByteLength = (recipientCount: number): number =>
  recipientCount === 1
    ? SINGLE_RECIPIENT_FIXED_BYTES
    : MULTIPLE_RECIPIENT_FIXED_BYTES + RECIPIENT_ENTRY_BYTES * recipientCount;

/**
 * Computes `ceil(4n / 3)`, the unpadded base64url length of `n` bytes, without
 * fractional arithmetic.
 *
 * @param byteLength Non-negative safe integer.
 * @returns The encoded length.
 */
const base64UrlLength = (byteLength: number): number => {
  const remainder = byteLength % 3;
  return byteLength + (byteLength - remainder) / 3 + (remainder === 0 ? 0 : 1);
};

interface SealedByteLengthInput {
  readonly plaintextByteLength: number;
  readonly recipientCount: number;
}

/**
 * Returns the exact length of the body `seal` produces, so a consumer checks a
 * SignedMessage body bound without reproducing the representation formula.
 *
 * @param input Plaintext length and recipient count.
 * @returns The sealed length, or none when the recipient count is not an
 * integer from 1 to 128, the plaintext length is not a non-negative safe
 * integer, or the sealed length would exceed `Number.MAX_SAFE_INTEGER`.
 */
const sealedByteLength = (
  input: SealedByteLengthInput,
): Option.Option<number> => {
  if (
    !isRecipientCount(input.recipientCount) ||
    !Number.isSafeInteger(input.plaintextByteLength) ||
    input.plaintextByteLength < 0
  ) {
    return Option.none();
  }
  const byteLength =
    base64UrlLength(input.plaintextByteLength + SALT_BYTES) +
    fixedByteLength(input.recipientCount);
  return Number.isSafeInteger(byteLength)
    ? Option.some(byteLength)
    : Option.none();
};

/**
 * Returns the largest plaintext whose sealed body fits one SignedMessage body.
 *
 * @param recipientCount Number of recipients.
 * @returns The largest plaintext length, or none when the recipient count is
 * not an integer from 1 to 128.
 */
const maximumPlaintextByteLength = (
  recipientCount: number,
): Option.Option<number> => {
  if (!isRecipientCount(recipientCount)) {
    return Option.none();
  }
  const ciphertextBudget = MAXIMUM_BODY_BYTES - fixedByteLength(recipientCount);
  return Option.some(Math.floor((3 * ciphertextBudget) / 4) - SALT_BYTES);
};

/**
 * Seals a SignedMessage body to its recipients, opens a verified sealed body,
 * and reports sealed sizes. `SignedMessage.sign` never seals; the caller
 * chooses which bodies to seal and signs the sealed bytes.
 */
// eslint-disable-next-line @typescript-eslint/naming-convention -- The deep-module capability uses its domain name, as SignedMessage and AgentSigningAuthority do.
export const SealedBody = Object.freeze({
  seal,
  open,
  sealedByteLength,
  maximumPlaintextByteLength,
});
