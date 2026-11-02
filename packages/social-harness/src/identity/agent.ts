/** @file Agent identities, public cards and Registry-signed identity documents. */

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
import { createHash } from "node:crypto";
import {
  decodeCanonicalJson,
  encodeCanonicalJson,
} from "../encoding/canonical-json.js";
import { exactStruct } from "../encoding/exact.js";
import { canonicalIdentifier } from "../encoding/identifier.js";
import {
  AgentPublicKey,
  AgentSigningAuthority,
  agentSigningPrivateKey,
  ed25519PublicKeyThumbprintUri,
  ed25519VerificationKey,
  generalJwsRepresentation,
  snapshotRepresentation,
} from "./keys.js";
import { AGENT_CARD_TYPE, WireVersions } from "./wire-version.js";

/** Signing capabilities expose the public key without exposing private key material. */
export {
  AgentPublicKey,
  AgentSigningAuthority,
  InvalidAgentPrivateKeyError,
} from "./keys.js";

const IDENTIFIER_BYTE_LENGTH = 16;

/* eslint-disable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare --
 * Named Effect Schemas share their domain names with the nominal types they decode.
 */

/** Canonical network identity minted by the Registry. */
export const AgentId = canonicalIdentifier(
  "AgentId",
  "agt_",
  IDENTIFIER_BYTE_LENGTH,
);
/** Validated nominal value decoded by {@link AgentId}. */
export type AgentId = typeof AgentId.Type;

/** Opaque identity of the principal represented by an agent. */
export const PrincipalId = canonicalIdentifier(
  "PrincipalId",
  "prn_",
  IDENTIFIER_BYTE_LENGTH,
);
/** Validated nominal value decoded by {@link PrincipalId}. */
export type PrincipalId = typeof PrincipalId.Type;

/** The rule every agent name meets, as validation errors state it. */
export const AGENT_NAME_RULE =
  "3 to 32 lowercase letters, digits or single hyphens, starting and ending with a letter or digit";

/** Immutable Registry-wide human-facing agent handle. */
export const AgentName = Schema.String.pipe(
  Schema.minLength(3),
  Schema.maxLength(32),
  Schema.pattern(/^[a-z0-9]+(-[a-z0-9]+)*$/),
  Schema.brand("AgentName"),
  Schema.annotations({
    identifier: "AgentName",
    description: `an agent name: ${AGENT_NAME_RULE}`,
  }),
);
/** Validated nominal value decoded by {@link AgentName}. */
export type AgentName = typeof AgentName.Type;

/* eslint-enable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare --
 * Restore the general naming rules after the public Schema/type pairs.
 */

const DIGEST_BYTE_LENGTH = 32;
const WHOLE_SECOND_UTC =
  /^\d{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\d|3[01])T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\dZ$/;

const isWholeSecondUtc = (value: string): boolean => {
  if (typeof value !== "string") {
    return false;
  }
  const epochMilliseconds = Date.parse(value);
  if (!Number.isFinite(epochMilliseconds)) {
    return false;
  }
  const wholeSecondValue = `${value.slice(0, -1)}.000Z`;
  return new Date(epochMilliseconds).toISOString() === wholeSecondValue;
};

/** Digest binding a message to one complete immutable AgentIdentityCard. */
// eslint-disable-next-line @typescript-eslint/naming-convention -- Effect Schemas share the public domain name they decode.
export const AgentIdentityCardDigest = canonicalIdentifier(
  "AgentIdentityCardDigest",
  "acd_",
  DIGEST_BYTE_LENGTH,
);
/** Validated nominal value decoded by AgentIdentityCardDigest. */
// eslint-disable-next-line @typescript-eslint/no-redeclare -- The same-named Schema and type form one boundary model.
export type AgentIdentityCardDigest = typeof AgentIdentityCardDigest.Type;

/** Shared whole-second UTC validation for parsed and issued AgentCards. */
export const agentIdentityCardIssuedAtSchema = Schema.String.pipe(
  Schema.pattern(WHOLE_SECOND_UTC),
  Schema.filter(isWholeSecondUtc, {
    identifier: "AgentIdentityCardIssuedAt",
    description: "AgentIdentityCard issuance time in whole-second UTC",
  }),
  Schema.brand("AgentIdentityCardIssuedAt"),
  Schema.annotations({
    identifier: "AgentIdentityCardIssuedAt",
    description: "AgentIdentityCard issuance time in whole-second UTC",
  }),
);
type AgentIdentityCardRepresentation = typeof generalJwsRepresentation.Type;

const protectedHeader = exactStruct({
  alg: Schema.Literal("Ed25519"),
  kid: Schema.String,
  typ: Schema.Literal(AGENT_CARD_TYPE),
});

const payload = exactStruct({
  kind: Schema.Literal("agentCard"),
  sharnessVersion: WireVersions.history,
  agentId: AgentId,
  principalId: PrincipalId,
  agentName: AgentName,
  publicKey: AgentPublicKey,
  issuedAt: agentIdentityCardIssuedAtSchema,
});

/** Immutable public identity fields carried by an AgentIdentityCard. */
export interface AgentIdentityCard extends AgentCard {
  readonly publicKey: AgentPublicKey;
  readonly issuedAt: string;
}

interface AgentIdentityCardState {
  readonly wireVersion: string;
  readonly representation: AgentIdentityCardRepresentation;
  readonly protectedHeader: typeof protectedHeader.Type;
  /** SHA-256 over the retained canonical JWS, fixed for the card's lifetime. */
  readonly digest: AgentIdentityCardDigest;
}

const cardState = new WeakMap<object, AgentIdentityCardState>();
const verifiedCards = new WeakSet();

const hasAgentIdentityCardState = (
  value: unknown,
): value is AgentIdentityCard =>
  typeof value === "object" &&
  value !== null &&
  Object.isFrozen(value) &&
  cardState.has(value);

const agentIdentityCardView = Schema.declare(hasAgentIdentityCardState, {
  identifier: "AgentCardView",
});

const parseIssue = (
  ast: SchemaAST.Transformation,
  actual: unknown,
): ParseResult.ParseIssue =>
  new ParseResult.Type(ast, actual, "Expected an exact AgentIdentityCard");

const decodeBase64Url = (
  value: string,
  ast: SchemaAST.Transformation,
): Effect.Effect<Uint8Array, ParseResult.ParseIssue> =>
  Encoding.decodeBase64Url(value).pipe(
    Effect.mapError(() => parseIssue(ast, value)),
  );

/** The refined SHA-256 digest of a card's retained canonical bytes. */
const digestOf = (
  bytes: Uint8Array,
  issue: () => ParseResult.ParseIssue,
): Effect.Effect<AgentIdentityCardDigest, ParseResult.ParseIssue> =>
  Effect.try({
    try: () => createHash("sha256").update(bytes).digest(),
    catch: issue,
  }).pipe(
    Effect.flatMap((hash) =>
      Schema.decodeUnknown(AgentIdentityCardDigest)(
        `acd_${Encoding.encodeBase64Url(hash)}`,
      ).pipe(Effect.mapError(issue)),
    ),
  );

const decodeRepresentation = (
  representation: AgentIdentityCardRepresentation,
  ast: SchemaAST.Transformation,
): Effect.Effect<AgentIdentityCard, ParseResult.ParseIssue> =>
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
    const publicKey = yield* Schema.decodeUnknown(AgentPublicKey)(
      Object.freeze({
        crv: decodedPayload.publicKey.crv,
        kty: decodedPayload.publicKey.kty,
        x: decodedPayload.publicKey.x,
      }),
    ).pipe(Effect.mapError(() => parseIssue(ast, representation)));
    const card = Object.freeze({
      agentId: decodedPayload.agentId,
      principalId: decodedPayload.principalId,
      agentName: decodedPayload.agentName,
      publicKey,
      issuedAt: decodedPayload.issuedAt,
    });
    const retained = snapshotRepresentation(representation);
    const retainedBytes = yield* encodeCanonicalJson(retained).pipe(
      Effect.mapError(() => parseIssue(ast, representation)),
    );
    const digest = yield* digestOf(retainedBytes, () =>
      parseIssue(ast, representation),
    );
    cardState.set(card, {
      wireVersion: decodedPayload.sharnessVersion,
      representation: retained,
      protectedHeader: Object.freeze({ ...decodedHeader }),
      digest,
    });
    return card;
  });

const encodeView = (
  card: AgentIdentityCard,
  ast: SchemaAST.Transformation,
): Effect.Effect<AgentIdentityCardRepresentation, ParseResult.ParseIssue> => {
  const state = cardState.get(card);
  return state === undefined
    ? Effect.fail(parseIssue(ast, card))
    : Effect.succeed(state.representation);
};

const agentIdentityCardSchema = Schema.transformOrFail(
  generalJwsRepresentation,
  agentIdentityCardView,
  {
    strict: true,
    decode: (representation, ...[, ast]) =>
      decodeRepresentation(representation, ast),
    encode: (card, ...[, ast]) => encodeView(card, ast),
  },
).annotations({
  identifier: "AgentIdentityCard",
  parseOptions: {
    exact: true,
    onExcessProperty: "error",
  },
});

/** AgentIdentityCard verified against a deployment-pinned Registry signer. */
export type VerifiedAgentIdentityCard = AgentIdentityCard &
  Brand.Brand<"VerifiedAgentIdentityCard">;

const brandVerifiedAgentIdentityCard =
  Brand.nominal<VerifiedAgentIdentityCard>();

/** Recognizes identity documents whose retained Registry signature this module verified. */
export const verifiedAgentIdentityCardSchema = Schema.declare(
  (value: unknown): value is VerifiedAgentIdentityCard =>
    typeof value === "object" &&
    value !== null &&
    cardState.has(value) &&
    verifiedCards.has(value),
  { identifier: "VerifiedAgentIdentityCard" },
);

/**
 * Identifies an agent and the principal it represents without exposing the
 * signed card representation. Host messages, results and policy decisions
 * retain this view from the verified AgentIdentityCard.
 */
// eslint-disable-next-line @typescript-eslint/naming-convention -- Effect Schemas share the public domain name they decode.
export const AgentCard = Schema.Struct({
  agentId: AgentId,
  agentName: AgentName,
  principalId: PrincipalId,
}).annotations({ identifier: "AgentCard" });
/**
 * Identity captured from a verified AgentIdentityCard. The principal is the declaration
 * recorded by Registry.
 * @useDeclaredType
 */
// eslint-disable-next-line @typescript-eslint/no-redeclare -- The same-named Schema and type form one boundary model.
export type AgentCard = typeof AgentCard.Type;

/**
 * Project an immutable Registry identity for host results.
 * @param identityCard A card verified against the pinned Registry signer.
 * @returns The card's public fields.
 */
export function agentCardOf(
  identityCard: VerifiedAgentIdentityCard,
): AgentCard {
  return Object.freeze({
    agentId: identityCard.agentId,
    agentName: identityCard.agentName,
    principalId: identityCard.principalId,
  });
}

/** A parsed AgentIdentityCard does not verify against the pinned Registry signer. */
export class AgentIdentityCardVerificationError extends Data.TaggedError(
  "AgentIdentityCardVerificationError",
) {}

const verificationFailure = (): AgentIdentityCardVerificationError =>
  new AgentIdentityCardVerificationError();

const verifyAgentIdentityCard = (input: {
  readonly identityCard: AgentIdentityCard;
  readonly registrySignerPublicKey: AgentPublicKey;
}): Effect.Effect<
  VerifiedAgentIdentityCard,
  AgentIdentityCardVerificationError
> =>
  Effect.gen(function* () {
    const state = cardState.get(input.identityCard);
    if (state === undefined) {
      return yield* new AgentIdentityCardVerificationError();
    }
    const expectedKid = yield* ed25519PublicKeyThumbprintUri(
      input.registrySignerPublicKey,
    ).pipe(Effect.mapError(verificationFailure));
    if (
      state.protectedHeader.alg !== "Ed25519" ||
      state.protectedHeader.typ !== AGENT_CARD_TYPE ||
      state.protectedHeader.kid !== expectedKid
    ) {
      return yield* new AgentIdentityCardVerificationError();
    }
    const key = yield* ed25519VerificationKey(
      input.registrySignerPublicKey,
    ).pipe(Effect.mapError(verificationFailure));
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
          {
            algorithms: ["Ed25519"],
          },
        );
      },
      catch: verificationFailure,
    });
    verifiedCards.add(input.identityCard);
    return brandVerifiedAgentIdentityCard(input.identityCard);
  });

/**
 * Identity-owned digest over the complete retained AgentIdentityCard JWS.
 *
 * @param identityCard Parsed AgentIdentityCard to digest.
 * @returns The refined SHA-256 AgentIdentityCard digest.
 */
export const digestAgentIdentityCard = (
  identityCard: AgentIdentityCard,
): Effect.Effect<AgentIdentityCardDigest, AgentIdentityCardVerificationError> =>
  Effect.suspend(() => {
    const state = cardState.get(identityCard);
    return state === undefined
      ? Effect.fail(new AgentIdentityCardVerificationError())
      : Effect.succeed(state.digest);
  }).pipe(Effect.withSpan("digestAgentIdentityCard"));

/* eslint-disable @typescript-eslint/no-redeclare, @typescript-eslint/naming-convention --
 * Schema and type declarations share their domain name.
 */
/** The retained signed version lets members derive the same conversation foundation. */
const wireVersion = (card: AgentIdentityCard): string =>
  cardState.get(card)?.wireVersion ?? "";

/**
 * Parses immutable cards and verifies their Registry attestation.
 *
 * Use this Schema to decode and re-encode a card. Decoding retains its exact
 * General JWS and exposes a frozen public-key snapshot. Parsing alone does
 * not establish that the Registry signed the card.
 *
 * `verify` checks the signature and protected key identifier against the
 * deployment-pinned `registrySignerPublicKey`. Only successful verification
 * returns `VerifiedAgentIdentityCard`. A rejected card fails with
 * `AgentIdentityCardVerificationError`.
 *
 * `wireVersion` reads the retained signed version, which members use to
 * derive the same conversation foundation.
 */
export const AgentIdentityCard = Object.assign(agentIdentityCardSchema, {
  verify: verifyAgentIdentityCard,
  wireVersion,
});
/* eslint-enable @typescript-eslint/no-redeclare, @typescript-eslint/naming-convention --
 * Schema and type declarations share their domain name.
 */

/** Complete immutable claims accepted by the card-signing capability. */
export interface AgentIdentityCardClaims {
  readonly kind: "agentCard";
  readonly sharnessVersion: string;
  readonly agentId: AgentId;
  readonly principalId: PrincipalId;
  readonly agentName: AgentName;
  readonly publicKey: AgentPublicKey;
  readonly issuedAt: typeof agentIdentityCardIssuedAtSchema.Type;
}

interface CardSigning {
  readonly issuedAt: typeof agentIdentityCardIssuedAtSchema;
  readonly publicKeyId: typeof ed25519PublicKeyThumbprintUri;
  readonly sign: (
    payload: AgentIdentityCardClaims,
    authority: AgentSigningAuthority,
  ) => Effect.Effect<AgentIdentityCard, AgentIdentityCardVerificationError>;
}

/** Card signing retains private key access and wire-profile details inside Identity. */
export const agentIdentityCardSigning: CardSigning = Object.freeze({
  issuedAt: agentIdentityCardIssuedAtSchema,
  publicKeyId: ed25519PublicKeyThumbprintUri,
  sign: signCardPayload,
});

function signCardPayload(
  payload: AgentIdentityCardClaims,
  authority: AgentSigningAuthority,
): Effect.Effect<AgentIdentityCard, AgentIdentityCardVerificationError> {
  return Effect.gen(function* () {
    const failure = () => new AgentIdentityCardVerificationError();
    const bytes = yield* encodeCanonicalJson(payload).pipe(
      Effect.mapError(failure),
    );
    const kid = yield* ed25519PublicKeyThumbprintUri(
      AgentSigningAuthority.publicKey(authority),
    ).pipe(Effect.mapError(failure));
    const representation = yield* Effect.tryPromise({
      try: () =>
        new GeneralSign(bytes)
          .addSignature(agentSigningPrivateKey(authority))
          .setProtectedHeader({
            alg: "Ed25519",
            kid,
            typ: AGENT_CARD_TYPE,
          })
          .sign(),
      catch: failure,
    });
    return yield* Schema.decodeUnknown(AgentIdentityCard)(representation).pipe(
      Effect.mapError(failure),
    );
  }).pipe(Effect.withSpan("agentIdentityCardSigning"));
}
