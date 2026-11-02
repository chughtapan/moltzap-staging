/** @file Canonical end-to-end signed message representation, signing, and verification. */

import {
  Brand,
  Data,
  Effect,
  Encoding,
  ParseResult,
  Schema,
  type SchemaAST,
} from "effect";
import { GeneralSign, generalVerify } from "jose";
import {
  decodeCanonicalJson,
  encodeCanonicalJson,
  exactStruct,
} from "../../encoding/canonical-json.js";
import {
  canonicalIdentifier,
  compareAgentIds,
  decodeCanonicalBase64Url,
} from "../../encoding/identifier.js";
import {
  AgentId,
  AgentIdentityCardDigest,
  digestAgentIdentityCard,
  type VerifiedAgentIdentityCard,
} from "../agent.js";
import {
  type AgentPublicKey,
  AgentSigningAuthority,
  agentSigningPrivateKey,
  ed25519PublicKeyThumbprintUri,
  ed25519VerificationKey,
  generalJwsRepresentation,
  sameEd25519PublicKey,
  snapshotRepresentation,
} from "../keys.js";
import { WIRE_VERSION, WireVersions } from "../wire-version.js";

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
export const MAXIMUM_BODY_BYTES = 262_144;
/** Largest recipient set that one SignedMessage names. */
export const MAXIMUM_RECIPIENTS = 128;
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

const protectedHeader = exactStruct({
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
  readonly protectedHeader: typeof protectedHeader.Type;
  readonly encodedByteLength: number;
}

const messageState = new WeakMap<object, SignedMessageState>();

/**
 * Whether this package parsed or signed `value`. The WeakMap lookup runs no
 * Proxy trap, so callers test it before reading any member of a message.
 */
export const isParsedSignedMessage = (value: unknown): boolean =>
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
      protectedHeader,
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
      protectedHeader: Object.freeze({ ...decodedHeader }),
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

const snapshotRecipients = (recipientAgentIds: ReadonlySet<AgentId>) =>
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
    const sameKey = yield* sameEd25519PublicKey(
      () => AgentSigningAuthority.publicKey(input.signingAuthority),
      () => input.publicKey,
    ).pipe(Effect.mapError(signingFailure));
    if (!sameKey) {
      return yield* new SignedMessageSigningError();
    }
    const recipients = yield* snapshotRecipients(input.recipientAgentIds);
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
    const headerMatches =
      state.protectedHeader.kid === expectedKid &&
      state.protectedHeader.alg === "Ed25519" &&
      state.protectedHeader.typ === SIGNED_MESSAGE_TYPE;
    if (!messageMatches || !headerMatches) {
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
 * Parses and signs attributed messages while retaining their complete General JWS.
 *
 * Decode and re-encode messages through this Schema. Recipients are an
 * immutable snapshot, and each read of `body` returns a defensive copy.
 * Changes to caller-owned values cannot change the retained signed bytes.
 * Parsing admits retained-history versions but does not verify attribution.
 * `live` additionally requires a version in the live acceptance window.
 *
 * `sign` requires a verified card carrying the authority's public key.
 * Signing copies the body and recipient set, enforces their bounds, and
 * orders recipients by decoded AgentId bytes. It derives the sender, card
 * digest, and emitted version, then returns a verified live message.
 * It signs the supplied body bytes, so callers seal them before signing
 * when the message requires encryption. Failure is `SignedMessageSigningError`.
 *
 * `verify` checks the sender, complete card digest, protected key identifier,
 * type, and signature against the supplied verified sender card.
 * Success returns `VerifiedSignedMessage` and preserves any live-version brand.
 * Failure is `SignedMessageVerificationError`. Only `sign` and `verify`
 * establish the verified state.
 *
 * `encodedByteLength` is total for parsed messages and returns the exact UTF-8
 * canonical-JSON length of the retained General JWS. Use
 * `maximumEncodedByteLength` to bound an enclosing request. `wireVersion`
 * reads the message's retained signed version.
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
