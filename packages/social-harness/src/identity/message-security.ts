/** @file Signs, encrypts, and verifies messages bound to their sender and recipients. */

import {
  Brand,
  Data,
  Effect,
  Either,
  Encoding,
  Option,
  ParseResult,
  Schema,
  type SchemaAST,
} from "effect";
import {
  generalDecrypt,
  GeneralEncrypt,
  GeneralSign,
  generalVerify,
} from "jose";
import {
  decodeCanonicalJson,
  encodeCanonicalJson,
  exactStruct,
} from "../encoding/canonical-json.js";
import {
  canonicalIdentifier,
  compareAgentIds,
  decodeCanonicalBase64Url,
  hasCanonicalBase64UrlLength,
} from "../encoding/identifier.js";
import { WIRE_VERSION, WireVersions } from "../version.js";
import {
  AgentId,
  AgentIdentityCardDigest,
  digestAgentIdentityCard,
  type VerifiedAgentIdentityCard,
} from "./agent-cards.js";
import {
  agentOpeningPrivateKey,
  type AgentPublicKey,
  AgentSigningAuthority,
  agentSigningPrivateKey,
  ed25519PublicKeyThumbprintUri,
  ed25519VerificationKey,
  generalJwsRepresentation,
  SEALED_BODY_KEY_MANAGEMENT_ALGORITHM,
  snapshotRepresentation,
  x25519SealingKey,
} from "./keys.js";

/** Identifies one signed envelope across transport retries. */
// eslint-disable-next-line @typescript-eslint/naming-convention -- Effect Schemas share the public domain name they decode.
export const EnvelopeId = canonicalIdentifier("EnvelopeId", "env_", 16);
/**
 * An envelope identifier validated by its public schema.
 * @useDeclaredType
 */
// eslint-disable-next-line @typescript-eslint/no-redeclare -- The same-named Schema and type form one boundary model.
export type EnvelopeId = typeof EnvelopeId.Type;

const SIGNED_MESSAGE_TYPE = "application/vnd.sharness.signed-message+jws";
/** Largest opaque body, in bytes, that one SignedMessage carries. */
const MAXIMUM_BODY_BYTES = 262_144;
/** Largest recipient set that one SignedMessage names. */
const MAXIMUM_RECIPIENTS = 128;
const ED25519_SIGNATURE_BYTES = 64;

/** Complete General JWS bound, encoded from a maximal payload and fixed-width signature. */
const MAXIMUM_ENCODED_BYTES = Effect.runSync(
  Effect.gen(function* () {
    const encoded = (length: number) =>
      Encoding.encodeBase64Url(new Uint8Array(length));
    const payloadBytes = yield* encodeCanonicalJson({
      kind: "signedMessage",
      sharnessVersion: WIRE_VERSION,
      senderAgentId: `agt_${encoded(16)}`,
      agentCardDigest: `acd_${encoded(32)}`,
      recipientAgentIds: Array.from(
        { length: MAXIMUM_RECIPIENTS },
        () => `agt_${encoded(16)}`,
      ),
      envelopeId: `env_${encoded(16)}`,
      body: encoded(MAXIMUM_BODY_BYTES),
    });
    const headerBytes = yield* encodeCanonicalJson({
      alg: "Ed25519",
      kid: `urn:ietf:params:oauth:jwk-thumbprint:sha-256:${encoded(32)}`,
      typ: SIGNED_MESSAGE_TYPE,
    });
    return (yield* encodeCanonicalJson({
      payload: Encoding.encodeBase64Url(payloadBytes),
      signatures: [
        {
          protected: Encoding.encodeBase64Url(headerBytes),
          signature: encoded(ED25519_SIGNATURE_BYTES),
        },
      ],
    })).byteLength;
  }),
);

type SignedMessageRepresentation = typeof generalJwsRepresentation.Type;

const signedMessageHeader = exactStruct({
  alg: Schema.Literal("Ed25519"),
  kid: Schema.String,
  typ: Schema.Literal(SIGNED_MESSAGE_TYPE),
});

const recipientsAreCanonical = (
  recipientAgentIds: readonly AgentId[],
): boolean => {
  for (let index = 1; index < recipientAgentIds.length; index += 1) {
    const previous = recipientAgentIds[index - 1];
    const current = recipientAgentIds[index];
    if (previous === undefined || current === undefined) {
      return false;
    }
    if (compareAgentIds(previous, current) >= 0) {
      return false;
    }
  }
  return true;
};

const recipientAgentIds = Schema.Array(AgentId).pipe(
  Schema.minItems(1),
  Schema.maxItems(MAXIMUM_RECIPIENTS),
  Schema.filter(recipientsAreCanonical),
);

const opaqueBody = Schema.String.pipe(
  Schema.filter((value) => {
    const bytes = decodeCanonicalBase64Url(value);
    return bytes !== undefined && bytes.byteLength <= MAXIMUM_BODY_BYTES;
  }),
);

const payload = exactStruct({
  kind: Schema.Literal("signedMessage"),
  sharnessVersion: WireVersions.history,
  senderAgentId: AgentId,
  agentCardDigest: AgentIdentityCardDigest,
  recipientAgentIds,
  envelopeId: EnvelopeId,
  body: opaqueBody,
});

/** Immutable attributed-message fields exposed to Router consumers. */
export interface SignedMessage {
  readonly senderAgentId: AgentId;
  readonly agentCardDigest: AgentIdentityCardDigest;
  readonly recipientAgentIds: readonly AgentId[];
  readonly envelopeId: EnvelopeId;
  readonly body: Uint8Array;
}

interface SignedMessageState {
  readonly wireVersion: string;
  readonly representation: SignedMessageRepresentation;
  readonly signedMessageHeader: typeof signedMessageHeader.Type;
  readonly encodedByteLength: number;
}

const messageState = new WeakMap<object, SignedMessageState>();

/**
 * Whether this package parsed or signed `value`. The WeakMap lookup runs no
 * Proxy trap, so callers test it before reading any member of a message.
 */
const isParsedSignedMessage = (value: unknown): boolean =>
  typeof value === "object" && value !== null && messageState.has(value);

const hasSignedMessageState = (value: unknown): value is SignedMessage =>
  typeof value === "object" &&
  value !== null &&
  Object.isFrozen(value) &&
  messageState.has(value);

const signedMessageView = Schema.declare(hasSignedMessageState, {
  identifier: "SignedMessageView",
});

const parseIssue = (
  ast: SchemaAST.Transformation,
  actual: unknown,
): ParseResult.ParseIssue =>
  new ParseResult.Type(ast, actual, "Expected an exact SignedMessage");

const decodeBase64Url = (
  value: string,
  ast: SchemaAST.Transformation,
): Effect.Effect<Uint8Array, ParseResult.ParseIssue> => {
  const bytes = decodeCanonicalBase64Url(value);
  return bytes === undefined
    ? Effect.fail(parseIssue(ast, value))
    : Effect.succeed(bytes);
};

const makeView = (
  decodedPayload: typeof payload.Type,
  body: Uint8Array,
): SignedMessage => {
  const recipients = Object.freeze([...decodedPayload.recipientAgentIds]);
  const bodySnapshot = new Uint8Array(body);
  const view: SignedMessage = {
    senderAgentId: decodedPayload.senderAgentId,
    agentCardDigest: decodedPayload.agentCardDigest,
    recipientAgentIds: recipients,
    envelopeId: decodedPayload.envelopeId,
    get body() {
      return new Uint8Array(bodySnapshot);
    },
  };
  return Object.freeze(view);
};

const decodeRepresentation = (
  representation: SignedMessageRepresentation,
  ast: SchemaAST.Transformation,
): Effect.Effect<SignedMessage, ParseResult.ParseIssue> =>
  Effect.gen(function* () {
    const payloadBytes = yield* decodeBase64Url(representation.payload, ast);
    const headerBytes = yield* decodeBase64Url(
      representation.signatures[0].protected,
      ast,
    );
    const decodedPayload = yield* decodeCanonicalJson(
      payload,
      payloadBytes,
    ).pipe(Effect.mapError(() => parseIssue(ast, representation)));
    const decodedHeader = yield* decodeCanonicalJson(
      signedMessageHeader,
      headerBytes,
    ).pipe(Effect.mapError(() => parseIssue(ast, representation)));
    const body = yield* decodeBase64Url(decodedPayload.body, ast);
    const retained = snapshotRepresentation(representation);
    const encoded = yield* encodeCanonicalJson(retained).pipe(
      Effect.mapError(() => parseIssue(ast, representation)),
    );
    if (encoded.byteLength > MAXIMUM_ENCODED_BYTES) {
      return yield* Effect.fail(parseIssue(ast, representation));
    }
    const view = makeView(decodedPayload, body);
    messageState.set(view, {
      wireVersion: decodedPayload.sharnessVersion,
      representation: retained,
      signedMessageHeader: Object.freeze({ ...decodedHeader }),
      encodedByteLength: encoded.byteLength,
    });
    return view;
  });

const encodeView = (
  message: SignedMessage,
  ast: SchemaAST.Transformation,
): Effect.Effect<SignedMessageRepresentation, ParseResult.ParseIssue> => {
  const state = messageState.get(message);
  return state === undefined
    ? Effect.fail(parseIssue(ast, message))
    : Effect.succeed(state.representation);
};

const signedMessageSchema = Schema.transformOrFail(
  generalJwsRepresentation,
  signedMessageView,
  {
    strict: true,
    decode: (representation, ...[, ast]) =>
      decodeRepresentation(representation, ast),
    encode: (message, ...[, ast]) => encodeView(message, ast),
  },
).annotations({
  identifier: "SignedMessage",
  parseOptions: {
    exact: true,
    onExcessProperty: "error",
  },
});

/** SignedMessage whose sender attribution and signature are verified. */
export type VerifiedSignedMessage = SignedMessage &
  Brand.Brand<"VerifiedSignedMessage">;

const brandVerifiedSignedMessage = Brand.nominal<VerifiedSignedMessage>();

/** A message cannot be signed under the supplied immutable identity. */
export class SignedMessageSigningError extends Data.TaggedError(
  "SignedMessageSigningError",
) {}

/** A SignedMessage does not bind to the supplied verified AgentIdentityCard. */
export class SignedMessageVerificationError extends Data.TaggedError(
  "SignedMessageVerificationError",
) {}

/** A decoded outer envelope whose version is admitted by the live window. */
export type LiveSignedMessage = SignedMessage &
  Brand.Brand<"LiveSignedMessage">;

const live = signedMessageSchema.pipe(
  Schema.filterEffect((message) =>
    WireVersions.live(messageState.get(message)?.wireVersion ?? ""),
  ),
  Schema.brand("LiveSignedMessage"),
);

const signingFailure = (): SignedMessageSigningError =>
  new SignedMessageSigningError();

const verificationFailure = (): SignedMessageVerificationError =>
  new SignedMessageVerificationError();

/** @inline */
interface SignInput {
  readonly identityCard: VerifiedAgentIdentityCard;
  readonly signingAuthority: AgentSigningAuthority;
  readonly recipientAgentIds: ReadonlySet<AgentId>;
  readonly envelopeId: EnvelopeId;
  readonly body: Uint8Array;
}

const snapshotMessageRecipients = (recipientAgentIds: ReadonlySet<AgentId>) =>
  Effect.gen(function* () {
    const recipients = yield* Effect.try({
      try: () => Array.from(recipientAgentIds),
      catch: signingFailure,
    });
    recipients.sort(compareAgentIds);
    if (
      recipients.length === 0 ||
      recipients.length > MAXIMUM_RECIPIENTS ||
      !recipientsAreCanonical(recipients)
    ) {
      return yield* new SignedMessageSigningError();
    }
    return recipients;
  });

const snapshotBody = (candidate: Uint8Array) =>
  Effect.gen(function* () {
    const body = yield* Effect.try({
      try: () => new Uint8Array(candidate),
      catch: signingFailure,
    });
    if (body.byteLength > MAXIMUM_BODY_BYTES) {
      return yield* new SignedMessageSigningError();
    }
    return body;
  });

/** Captured caller input keeps later signing steps independent of mutable getters. */
interface SigningRecord extends SignInput {
  readonly senderAgentId: AgentId;
  readonly publicKey: AgentPublicKey;
}

/**
 * A throwing input getter becomes a signing failure. Later signing steps
 * use the captured values so repeated getters cannot change the request.
 */
const snapshotSignInput = (
  input: SignInput,
): Effect.Effect<SigningRecord, SignedMessageSigningError> =>
  Effect.try({
    try: () => {
      const {
        identityCard,
        signingAuthority,
        recipientAgentIds,
        envelopeId,
        body,
      } = input;
      return {
        identityCard,
        signingAuthority,
        recipientAgentIds,
        envelopeId,
        body,
        senderAgentId: identityCard.agentId,
        publicKey: identityCard.publicKey,
      };
    },
    catch: signingFailure,
  });

const prepareSigningMaterial = (input: SigningRecord) =>
  Effect.gen(function* () {
    if (
      AgentSigningAuthority.publicKey(input.signingAuthority).x !==
      input.publicKey.x
    ) {
      return yield* new SignedMessageSigningError();
    }
    const recipients = yield* snapshotMessageRecipients(
      input.recipientAgentIds,
    );
    const body = yield* snapshotBody(input.body);
    const agentCardDigest = yield* digestAgentIdentityCard(
      input.identityCard,
    ).pipe(Effect.mapError(signingFailure));
    const kid = yield* ed25519PublicKeyThumbprintUri(input.publicKey).pipe(
      Effect.mapError(signingFailure),
    );
    const payloadBytes = yield* encodeCanonicalJson({
      kind: "signedMessage",
      sharnessVersion: yield* WireVersions.emitted,
      senderAgentId: input.senderAgentId,
      agentCardDigest,
      recipientAgentIds: recipients,
      envelopeId: input.envelopeId,
      body: Encoding.encodeBase64Url(body),
    }).pipe(Effect.mapError(signingFailure));
    return { kid, payloadBytes };
  });

const sign = (
  input: SignInput,
): Effect.Effect<
  LiveSignedMessage & VerifiedSignedMessage,
  SignedMessageSigningError
> =>
  Effect.gen(function* () {
    const signing = yield* snapshotSignInput(input);
    const { kid, payloadBytes } = yield* prepareSigningMaterial(signing);
    const representation = yield* Effect.tryPromise({
      try: () =>
        new GeneralSign(payloadBytes)
          .addSignature(agentSigningPrivateKey(signing.signingAuthority))
          .setProtectedHeader({
            alg: "Ed25519",
            kid,
            typ: SIGNED_MESSAGE_TYPE,
          })
          .sign(),
      catch: signingFailure,
    });
    const admitted = yield* Schema.decodeUnknown(live)(representation).pipe(
      Effect.mapError(signingFailure),
    );
    return Brand.nominal<LiveSignedMessage & VerifiedSignedMessage>()(admitted);
  });

/** @inline */
interface VerifyInput {
  readonly signedMessage: SignedMessage;
  readonly identityCard: VerifiedAgentIdentityCard;
}

/** Checks the message's one Ed25519 signature against `publicKey`. */
const verifySignature = (
  state: SignedMessageState,
  publicKey: AgentPublicKey,
): Effect.Effect<void, SignedMessageVerificationError> =>
  Effect.gen(function* () {
    const key = yield* ed25519VerificationKey(publicKey).pipe(
      Effect.mapError(verificationFailure),
    );
    yield* Effect.tryPromise({
      try: () => {
        const signature = state.representation.signatures[0];
        return generalVerify(
          {
            payload: state.representation.payload,
            signatures: [
              {
                protected: signature.protected,
                signature: signature.signature,
              },
            ],
          },
          key,
          { algorithms: ["Ed25519"] },
        );
      },
      catch: verificationFailure,
    });
  });

/**
 * A throwing input getter becomes a verification failure. Later checks use
 * the captured values so repeated getters cannot change the claimed sender.
 */
const snapshotVerifyInput = (input: VerifyInput) =>
  Effect.try({
    try: () => {
      const { signedMessage, identityCard } = input;
      return {
        signedMessage,
        identityCard,
        senderAgentId: identityCard.agentId,
        publicKey: identityCard.publicKey,
      };
    },
    catch: verificationFailure,
  });

function verify(
  input: VerifyInput & { readonly signedMessage: LiveSignedMessage },
): Effect.Effect<
  LiveSignedMessage & VerifiedSignedMessage,
  SignedMessageVerificationError
>;
function verify(
  input: VerifyInput,
): Effect.Effect<VerifiedSignedMessage, SignedMessageVerificationError>;
function verify(
  input: VerifyInput,
): Effect.Effect<VerifiedSignedMessage, SignedMessageVerificationError> {
  return Effect.gen(function* () {
    const { signedMessage, identityCard, senderAgentId, publicKey } =
      yield* snapshotVerifyInput(input);
    const state = messageState.get(signedMessage);
    if (state === undefined) {
      return yield* new SignedMessageVerificationError();
    }
    const expectedDigest = yield* digestAgentIdentityCard(identityCard).pipe(
      Effect.mapError(verificationFailure),
    );
    const expectedKid = yield* ed25519PublicKeyThumbprintUri(publicKey).pipe(
      Effect.mapError(verificationFailure),
    );
    const messageMatches =
      signedMessage.senderAgentId === senderAgentId &&
      signedMessage.agentCardDigest === expectedDigest;
    if (!messageMatches || state.signedMessageHeader.kid !== expectedKid) {
      return yield* new SignedMessageVerificationError();
    }
    yield* verifySignature(state, publicKey);
    return brandVerifiedSignedMessage(signedMessage);
  });
}

const encodedByteLength = (signedMessage: SignedMessage): number =>
  messageState.get(signedMessage)?.encodedByteLength ?? 0;

/* eslint-disable @typescript-eslint/no-redeclare, @typescript-eslint/naming-convention, agent-code-guard/require-assertion-rationale --
 * The accepted same-name Schema/type API retains the exact private General JWS AST while erasing only its encoded TypeScript type.
 */
/**
 * Retains the complete General JWS, snapshots recipients and copies body bytes
 * on every read. Parsing accepts history versions without verifying
 * attribution. `live` additionally requires a live version.
 *
 * `sign` requires the authority key to match the verified sender card. It
 * checks bounds, sorts recipients by AgentId bytes and returns a verified live
 * message. Seal bodies before signing when encryption is required.
 *
 * `verify` checks sender, card digest, protected key identifier, type and
 * signature against the verified sender card. It preserves the live brand.
 *
 * `encodedByteLength` reports the UTF-8 size of the retained JWS in canonical
 * JSON. `maximumEncodedByteLength` bounds an enclosing request. `wireVersion`
 * reads the retained signed version.
 */
export const SignedMessage = Object.assign(signedMessageSchema, {
  live: live as Schema.Schema<LiveSignedMessage, unknown>,
  sign,
  verify,
  encodedByteLength,
  wireVersion: (message: SignedMessage): string =>
    messageState.get(message)?.wireVersion ?? "",
  maximumEncodedByteLength: MAXIMUM_ENCODED_BYTES,
}) as Schema.Schema<SignedMessage, unknown> &
  Readonly<{
    readonly live: Schema.Schema<LiveSignedMessage, unknown>;
    readonly sign: typeof sign;
    readonly verify: typeof verify;
    readonly encodedByteLength: typeof encodedByteLength;
    readonly wireVersion: (message: SignedMessage) => string;
    readonly maximumEncodedByteLength: number;
  }>;
/* eslint-enable @typescript-eslint/no-redeclare, @typescript-eslint/naming-convention, agent-code-guard/require-assertion-rationale --
 * The accepted same-name Schema/type API retains the exact private General JWS AST while erasing only its encoded TypeScript type.
 */

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

const sealedBodyHeader = exactStruct({
  alg: Schema.Literal(SEALED_BODY_KEY_MANAGEMENT_ALGORITHM),
  enc: Schema.Literal(CONTENT_ENCRYPTION_ALGORITHM),
  epk: Schema.optional(ephemeralPublicKey),
  [COMMITMENT_HEADER]: encodedCommitment,
  [ENVELOPE_ID_HEADER]: EnvelopeId,
  [SENDER_HEADER]: AgentId,
});

type ProtectedHeader = typeof sealedBodyHeader.Type;

const protectedHeaderJson = Schema.parseJson(sealedBodyHeader);

/** Shares protected-header serialization order between sealing and opening. */
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
 * A256GCM does not commit to one content key. The authenticated SHA-256
 * commitment to salted plaintext prevents different recipient keys from
 * opening one ciphertext as different plaintexts.
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

const snapshotRecipientCards = (
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

/** Snapshots input under a try so throwing getters fail as sealing errors. */
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

/**
 * Encrypts to 1–128 distinct verified recipient cards. Sign the returned
 * canonical General JWE bytes with the same sender, EnvelopeId and recipients.
 * Include the sender as a recipient if it needs to read its own body.
 */
const seal = (
  input: SealInput,
): Effect.Effect<Uint8Array, SealedBodySealingError> =>
  Effect.gen(function* () {
    const sealing = yield* snapshotSealInput(input);
    const recipients = yield* snapshotRecipientCards(
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

/** Matches jose serialization, including the `epk` appended for one recipient. */
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
 * Requires the exact header bytes sealing produces. GCM authenticates
 * noncanonical spellings too. Compare encoded bytes because UTF-8 decoding
 * strips a leading byte-order mark.
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
 * Selects the local agent's entry in canonical recipient order. A single
 * recipient carries `epk` in the protected header and multiple recipients
 * carry one per entry.
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
    if (
      AgentSigningAuthority.publicKey(input.signingAuthority).x !==
      card.publicKey.x
    ) {
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
 * Requires a message parsed and verified by this package. Opens it with the
 * local card and authority. Checks exact sealed-body encoding, recipient count, sender, EnvelopeId,
 * authentication and plaintext commitment. All recipients that open the body
 * obtain the same plaintext. A sender can prevent some recipients from opening.
 *
 * The local Ed25519 key must match the authority even if decryption succeeds:
 * a key and its negation share an X25519 key.
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
 * Computes the unpadded base64url size `ceil(4n / 3)` for a non-negative safe
 * integer without fractional arithmetic.
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
 * Returns the exact sealed-body size. Returns none for a recipient count
 * other than an integer from 1–128, a non-integer or unsafe plaintext size,
 * a negative plaintext size, or an unsafe result.
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
 * Returns the largest plaintext size that fits a SignedMessage body, or none
 * unless the recipient count is an integer from 1–128.
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
 * Encrypts bodies before `SignedMessage.sign` and opens verified messages.
 * Use the size helpers to check bounds before sealing.
 */
// eslint-disable-next-line @typescript-eslint/naming-convention -- The deep-module capability uses its domain name, as SignedMessage and AgentSigningAuthority do.
export const SealedBody = Object.freeze({
  seal,
  open,
  sealedByteLength,
  maximumPlaintextByteLength,
});
