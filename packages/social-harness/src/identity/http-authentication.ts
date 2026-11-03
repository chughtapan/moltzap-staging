/** @file Signs HTTP requests and authenticates registered callers with bounded replay and identity state. */

import type { CryptoKey } from "jose";
import { HttpClientRequest, type HttpServerRequest } from "@effect/platform";
import {
  Brand,
  Cause,
  Clock,
  Context,
  Data,
  Deferred,
  Effect,
  Layer,
  Option,
  Ref,
  Schema,
} from "effect";
import { httpbis, type VerifyingKey } from "http-message-signatures";
import { createHash, randomBytes, webcrypto } from "node:crypto";
import {
  type BareItem,
  type InnerList,
  type Item,
  parseDictionary,
  serializeDictionary,
} from "structured-headers";
import type { RegistryLookupResult } from "./registry.js";
import {
  decodeCanonicalJson,
  encodeCanonicalJson,
  exactStruct,
  parseCanonicalJson,
} from "../encoding/canonical-json.js";
import { hasCanonicalBase64UrlLength } from "../encoding/identifier.js";
import {
  LIVE_WIRE_VERSIONS,
  WIRE_VERSION_PATTERN,
  WireVersions,
} from "../version.js";
import {
  AgentId,
  type VerifiedAgentIdentityCard,
  verifiedAgentIdentityCardSchema,
} from "./agent-cards.js";
import { type BoundedCache, makeBoundedCache } from "./bounded-cache.js";
import {
  type AgentPublicKey,
  AgentSigningAuthority,
  agentSigningPrivateKey,
  ed25519PublicKeyThumbprintUri,
  ed25519VerificationKey,
  hasCanonicalEd25519SignatureEncoding,
} from "./keys.js";

// safer-arch-ignore no-module-cycle: Registry is imported only for its result type; authentication has no runtime dependency on its client.

/** The request representation is not valid for the selected operation. */
export class MalformedRequestError extends Schema.TaggedError<MalformedRequestError>()(
  "MalformedRequestError",
  {},
) {}

/** The request does not prove the required identity or admission authority. */
export class AuthenticationFailedError extends Schema.TaggedError<AuthenticationFailedError>()(
  "AuthenticationFailedError",
  {},
) {}

/** No exact HTTP route owns the request target. */
export class RouteNotFoundError extends Schema.TaggedError<RouteNotFoundError>()(
  "RouteNotFoundError",
  {},
) {}

/** The selected route does not accept the request method. */
export class MethodNotAllowedError extends Schema.TaggedError<MethodNotAllowedError>()(
  "MethodNotAllowedError",
  {},
) {}

/** The request carries a different wire compatibility value. */
export class VersionMismatchError extends Schema.TaggedError<VersionMismatchError>()(
  "VersionMismatchError",
  {
    supported: Schema.optionalWith(Schema.Array(Schema.String), {
      default: () => LIVE_WIRE_VERSIONS,
    }),
  },
) {
  /** A mismatch response advertises at least one calendar wire version. */
  static readonly supportedSchema = Schema.NonEmptyArray(
    Schema.String.pipe(Schema.pattern(WIRE_VERSION_PATTERN)),
  );
}

/** The received body exceeds the selected route's derived representation cap. */
export class PayloadTooLargeError extends Schema.TaggedError<PayloadTooLargeError>()(
  "PayloadTooLargeError",
  {},
) {}

/** The selected route cannot consume the request content framing. */
export class UnsupportedMediaTypeError extends Schema.TaggedError<UnsupportedMediaTypeError>()(
  "UnsupportedMediaTypeError",
  {},
) {}

/** A finite immediate resource permit is unavailable. */
export class OverloadedError extends Schema.TaggedError<OverloadedError>()(
  "OverloadedError",
  {},
) {}

/** A required service or durable operation cannot currently complete. */
export class UnavailableError extends Schema.TaggedError<UnavailableError>()(
  "UnavailableError",
  {},
) {}

/** An unexpected implementation failure prevented a closed result. */
export class InternalServerError extends Schema.TaggedError<InternalServerError>()(
  "InternalServerError",
  {},
) {}

/** Service adapters construct and decode the same closed HTTP envelope failures as their clients. */
export const httpErrors = Object.freeze({
  AuthenticationFailedError,
  InternalServerError,
  MalformedRequestError,
  MethodNotAllowedError,
  OverloadedError,
  PayloadTooLargeError,
  RouteNotFoundError,
  UnavailableError,
  UnsupportedMediaTypeError,
  VersionMismatchError,
});

/** An agent-owned HTTP request could not be signed. */
export class AgentSigningError extends Data.TaggedError("AgentSigningError") {}

const SIGNATURE_LABEL = "sharness";
const SIGNATURE_INTERVAL_SECONDS = 300;
const FUTURE_SKEW_SECONDS = 5;
const SIGNATURE_BYTES = 64;

const NORMAL_FIELDS = Object.freeze([
  "@method",
  "@authority",
  "@path",
  "@query",
  "content-digest",
  "content-type",
  "sharness-version",
]);

const REGISTRATION_FIELDS = Object.freeze([...NORMAL_FIELDS, "authorization"]);

const PARAMETERS = Object.freeze([
  "created",
  "expires",
  "keyid",
  "nonce",
  "alg",
  "tag",
]);

/** Closed request-signature profile selected by the owning HTTP route. */
type HttpSignatureProfile = "normal" | "registration";

const PROFILES = {
  normal: { fields: NORMAL_FIELDS, tag: "sharness-request-v1" },
  registration: {
    fields: REGISTRATION_FIELDS,
    tag: "sharness-registration-v1",
  },
};

/**
 * Exact RFC 8941 SHA-256 content-digest field value.
 *
 * @param bodyBytes Exact request body octets.
 * @returns The single supported digest dictionary member.
 */
export const contentDigest = (bodyBytes: Uint8Array): string =>
  `sha-256=:${createHash("sha256").update(bodyBytes).digest("base64")}:`;

const toMessageHeaders = (
  headers: Readonly<Record<string, string>>,
): Record<string, string> => Object.fromEntries(Object.entries(headers));

/* eslint-disable agent-code-guard/promise-type, agent-code-guard/then-chain -- http-message-signatures requires this Promise callback */
const signBytes = (
  authority: AgentSigningAuthority,
  bytes: Buffer,
): Promise<Buffer> => {
  const copiedBytes = new Uint8Array(bytes);
  return webcrypto.subtle
    .sign("Ed25519", agentSigningPrivateKey(authority), copiedBytes)
    .then((signature) => Buffer.from(signature));
};
/* eslint-enable agent-code-guard/promise-type, agent-code-guard/then-chain -- restore Effect-first Promise rules */

interface SigningParameters {
  readonly created: Date;
  readonly expires: Date;
  readonly keyid: string;
  readonly nonce: string;
}

const makeSigningParameters = (
  signingAuthority: AgentSigningAuthority,
): Effect.Effect<SigningParameters, AgentSigningError> =>
  Effect.gen(function* () {
    const createdMilliseconds = yield* Clock.currentTimeMillis;
    const created = new Date(Math.floor(createdMilliseconds / 1_000) * 1_000);
    const expires = new Date(
      created.getTime() + SIGNATURE_INTERVAL_SECONDS * 1_000,
    );
    const nonce = yield* Effect.try({
      try: () => randomBytes(16).toString("base64url"),
      catch: () => new AgentSigningError(),
    });
    const keyid = yield* ed25519PublicKeyThumbprintUri(
      AgentSigningAuthority.publicKey(signingAuthority),
    ).pipe(
      Effect.catchTag("Ed25519PublicKeyOperationError", () =>
        Effect.fail(new AgentSigningError()),
      ),
    );
    return { created, expires, keyid, nonce };
  });

const prepareRequest = (input: {
  readonly httpRequest: HttpClientRequest.HttpClientRequest;
  readonly bodyBytes: Uint8Array;
  readonly wireVersion: string;
}): HttpClientRequest.HttpClientRequest =>
  input.httpRequest.pipe(
    HttpClientRequest.bodyUint8Array(input.bodyBytes, "application/json"),
    HttpClientRequest.setHeader(
      "content-digest",
      contentDigest(input.bodyBytes),
    ),
    HttpClientRequest.setHeader("sharness-version", input.wireVersion),
  );

const signPreparedRequest = (input: {
  readonly prepared: HttpClientRequest.HttpClientRequest;
  readonly profile: HttpSignatureProfile;
  readonly signingAuthority: AgentSigningAuthority;
  readonly parameters: SigningParameters;
}) =>
  Effect.tryPromise({
    try: () =>
      httpbis.signMessage(
        {
          name: SIGNATURE_LABEL,
          fields: [...PROFILES[input.profile].fields],
          params: [...PARAMETERS],
          paramValues: {
            ...input.parameters,
            alg: "ed25519",
            tag: PROFILES[input.profile].tag,
          },
          key: {
            id: input.parameters.keyid,
            alg: "ed25519",
            sign: (bytes) => signBytes(input.signingAuthority, bytes),
          },
        },
        {
          method: input.prepared.method,
          url: input.prepared.url,
          headers: toMessageHeaders(input.prepared.headers),
        },
      ),
    catch: () => new AgentSigningError(),
  });

/**
 * Installs and signs the canonical body bytes on a request whose method and
 * URL are already selected. The route selects the signature profile.
 */
export const signHttpRequest = (input: {
  readonly httpRequest: HttpClientRequest.HttpClientRequest;
  readonly bodyBytes: Uint8Array;
  readonly signingAuthority: AgentSigningAuthority;
  readonly profile: HttpSignatureProfile;
}): Effect.Effect<HttpClientRequest.HttpClientRequest, AgentSigningError> =>
  Effect.gen(function* () {
    const parameters = yield* makeSigningParameters(input.signingAuthority);
    const prepared = prepareRequest({
      ...input,
      wireVersion: yield* WireVersions.emitted,
    });
    const signed = yield* signPreparedRequest({
      prepared,
      parameters,
      profile: input.profile,
      signingAuthority: input.signingAuthority,
    });
    return prepared.pipe(
      HttpClientRequest.setHeader(
        "signature-input",
        String(signed.headers["Signature-Input"]),
      ),
      HttpClientRequest.setHeader(
        "signature",
        String(signed.headers.Signature),
      ),
    );
  }).pipe(Effect.withSpan("signHttpRequest"));

type ParsedSignature = Readonly<{
  fields: readonly string[];
  parameters: ReadonlyMap<string, BareItem>;
  signature: Uint8Array;
}>;

/** Replay material retained after exact signature verification. */
export type VerifiedHttpSignature = Readonly<{
  readonly nonce: string;
  readonly expires: number;
  readonly verifiedAt: number;
}>;

const isItem = (value: Item | InnerList): value is Item =>
  !Array.isArray(value[0]);

const readCoveredFields = (
  value?: Item | InnerList,
): Option.Option<
  Readonly<{
    fields: readonly string[];
    parameters: ReadonlyMap<string, BareItem>;
  }>
> => {
  if (value === undefined || isItem(value)) {
    return Option.none();
  }
  const [fieldItems, parameters] = value;
  const fields: string[] = [];
  for (const field of fieldItems) {
    if (typeof field[0] !== "string" || field[1].size !== 0) {
      return Option.none();
    }
    fields.push(field[0]);
  }
  return Option.some({ fields, parameters });
};

const readSignatureBytes = (
  value?: Item | InnerList,
): Option.Option<Uint8Array> => {
  if (
    value === undefined ||
    !isItem(value) ||
    !(value[0] instanceof ArrayBuffer) ||
    value[1].size !== 0
  ) {
    return Option.none();
  }
  const signature = new Uint8Array(value[0]);
  return signature.byteLength === SIGNATURE_BYTES &&
    hasCanonicalEd25519SignatureEncoding(signature)
    ? Option.some(signature)
    : Option.none();
};

const isExactSignatureDictionary = (
  header: string,
  dictionary: ReturnType<typeof parseDictionary>,
): boolean =>
  serializeDictionary(dictionary) === header &&
  dictionary.size === 1 &&
  dictionary.has(SIGNATURE_LABEL);

const parseExactSignature = (
  signatureInputHeader: string,
  signatureHeader: string,
): Option.Option<ParsedSignature> => {
  try {
    const inputDictionary = parseDictionary(signatureInputHeader);
    const signatureDictionary = parseDictionary(signatureHeader);
    if (!isExactSignatureDictionary(signatureInputHeader, inputDictionary)) {
      return Option.none();
    }
    if (!isExactSignatureDictionary(signatureHeader, signatureDictionary)) {
      return Option.none();
    }
    const covered = readCoveredFields(inputDictionary.get(SIGNATURE_LABEL));
    const signature = readSignatureBytes(
      signatureDictionary.get(SIGNATURE_LABEL),
    );
    if (Option.isNone(covered) || Option.isNone(signature)) {
      return Option.none();
    }
    return Option.some({
      fields: covered.value.fields,
      parameters: covered.value.parameters,
      signature: signature.value,
    });
    // eslint-disable-next-line agent-code-guard/bare-catch -- Parser rejection is expected untrusted input.
  } catch {
    return Option.none();
  }
};

const hasExactSequence = (
  actual: readonly string[],
  expected: readonly string[],
): boolean =>
  actual.length === expected.length &&
  actual.every((value, index) => value === expected[index]);

const readInteger = (
  parameters: ReadonlyMap<string, BareItem>,
  name: string,
): Option.Option<number> => {
  const value = parameters.get(name);
  return typeof value === "number" && Number.isSafeInteger(value)
    ? Option.some(value)
    : Option.none();
};

interface SignatureParameterValues {
  readonly created: number;
  readonly expires: number;
  readonly keyid: string;
  readonly nonce: string;
  readonly tag: string;
}

const readParameterValues = (
  parameters: ReadonlyMap<string, BareItem>,
): Option.Option<SignatureParameterValues> => {
  const created = readInteger(parameters, "created");
  const expires = readInteger(parameters, "expires");
  const keyid = parameters.get("keyid");
  const nonce = parameters.get("nonce");
  const algorithm = parameters.get("alg");
  const tag = parameters.get("tag");
  const missingTimes = Option.isNone(created) || Option.isNone(expires);
  const invalidIdentity =
    typeof keyid !== "string" || typeof nonce !== "string";
  const invalidProfile = algorithm !== "ed25519" || typeof tag !== "string";
  if (missingTimes || invalidIdentity || invalidProfile) {
    return Option.none();
  }
  return Option.some({
    created: created.value,
    expires: expires.value,
    keyid,
    nonce,
    tag,
  });
};

const hasExpectedIdentityParameters = (
  values: SignatureParameterValues,
  input: {
    readonly profile: HttpSignatureProfile;
    readonly expectedKeyId: string;
  },
): boolean => {
  const tagMatches = values.tag === PROFILES[input.profile].tag;
  const keyMatches =
    values.keyid === input.expectedKeyId &&
    hasCanonicalBase64UrlLength(values.nonce, 16);
  return tagMatches && keyMatches;
};

const hasAcceptableTimes = (
  values: SignatureParameterValues,
  nowSeconds: number,
): boolean =>
  values.created <= values.expires &&
  values.expires - values.created <= SIGNATURE_INTERVAL_SECONDS &&
  values.created <= nowSeconds + FUTURE_SKEW_SECONDS &&
  values.expires >= nowSeconds;

const exactParameters = (
  parsed: ParsedSignature,
  profile: HttpSignatureProfile,
  expectedKeyId: string,
  nowSeconds: number,
): Option.Option<VerifiedHttpSignature> => {
  if (!hasExactSequence([...parsed.parameters.keys()], PARAMETERS)) {
    return Option.none();
  }
  const values = readParameterValues(parsed.parameters);
  if (
    Option.isNone(values) ||
    !hasExpectedIdentityParameters(values.value, {
      profile,
      expectedKeyId,
    }) ||
    !hasAcceptableTimes(values.value, nowSeconds)
  ) {
    return Option.none();
  }
  return Option.some({
    nonce: values.value.nonce,
    expires: values.value.expires,
    verifiedAt: nowSeconds,
  });
};

const serverRequestUrl = (
  request: HttpServerRequest.HttpServerRequest,
): Option.Option<URL> => {
  const authority = request.headers.host ?? "";
  if (!authority) {
    return Option.none();
  }
  try {
    return Option.some(new URL(request.url, `http://${authority}`));
    // eslint-disable-next-line agent-code-guard/bare-catch -- Invalid authority is expected untrusted input.
  } catch {
    return Option.none();
  }
};

interface SignatureEnvelope {
  readonly signatureInput: string;
  readonly signature: string;
  readonly url: URL;
}

const readSignatureEnvelope = (input: {
  readonly httpRequest: HttpServerRequest.HttpServerRequest;
  readonly bodyBytes: Uint8Array;
}): Option.Option<SignatureEnvelope> => {
  const signatureInput = input.httpRequest.headers["signature-input"];
  const signature = input.httpRequest.headers.signature;
  const url = serverRequestUrl(input.httpRequest);
  if (
    typeof signatureInput !== "string" ||
    typeof signature !== "string" ||
    Option.isNone(url) ||
    input.httpRequest.headers["content-digest"] !==
      contentDigest(input.bodyBytes)
  ) {
    return Option.none();
  }
  return Option.some({
    signatureInput,
    signature,
    url: url.value,
  });
};

const makeVerifyingKey = (
  keyid: string,
  publicKey: CryptoKey,
): VerifyingKey => ({
  id: keyid,
  algs: ["ed25519"],
  verify: (bytes, candidate) => {
    const copiedSignature = new Uint8Array(candidate);
    const copiedBytes = new Uint8Array(bytes);
    return webcrypto.subtle.verify(
      "Ed25519",
      publicKey,
      copiedSignature,
      copiedBytes,
    );
  },
});

interface AcceptedSignatureParameters {
  readonly keyid: string;
  readonly signature: VerifiedHttpSignature;
}

const acceptSignatureParameters = (input: {
  readonly parsed: ParsedSignature;
  readonly profile: HttpSignatureProfile;
  readonly publicKey: AgentPublicKey;
}): Effect.Effect<AcceptedSignatureParameters, AuthenticationFailedError> =>
  Effect.gen(function* () {
    const keyid = yield* ed25519PublicKeyThumbprintUri(input.publicKey).pipe(
      Effect.catchTag("Ed25519PublicKeyOperationError", () =>
        Effect.fail(new AuthenticationFailedError()),
      ),
    );
    const nowSeconds = Math.floor((yield* Clock.currentTimeMillis) / 1_000);
    const signature = exactParameters(
      input.parsed,
      input.profile,
      keyid,
      nowSeconds,
    );
    if (Option.isNone(signature)) {
      return yield* new AuthenticationFailedError();
    }
    return { keyid, signature: signature.value };
  });

/** Effect Clock enforces the signature interval before the library checks its proof. */
const verifyWithLibrary = (input: {
  readonly httpRequest: HttpServerRequest.HttpServerRequest;
  readonly profile: HttpSignatureProfile;
  readonly url: URL;
  readonly keyid: string;
  readonly publicKey: CryptoKey;
}): Effect.Effect<boolean | null, AuthenticationFailedError> => {
  const key = makeVerifyingKey(input.keyid, input.publicKey);
  return Effect.tryPromise({
    try: () =>
      httpbis.verifyMessage(
        {
          all: true,
          requiredFields: [...PROFILES[input.profile].fields],
          requiredParams: [...PARAMETERS],
          tolerance: Number.MAX_SAFE_INTEGER,
          keyLookup: () => Promise.resolve(key),
        },
        {
          method: input.httpRequest.method,
          url: input.url,
          headers: toMessageHeaders(input.httpRequest.headers),
        },
      ),
    catch: () => new AuthenticationFailedError(),
  });
};

/**
 * Verifies the selected route profile against the exact body bytes. Returns
 * the nonce and verification second for replay admission.
 */
export const verifyHttpRequestSignature = (input: {
  readonly httpRequest: HttpServerRequest.HttpServerRequest;
  readonly bodyBytes: Uint8Array;
  readonly publicKey: AgentPublicKey;
  readonly profile: HttpSignatureProfile;
}): Effect.Effect<VerifiedHttpSignature, AuthenticationFailedError> =>
  Effect.gen(function* () {
    const envelope = readSignatureEnvelope(input);
    if (Option.isNone(envelope)) {
      return yield* new AuthenticationFailedError();
    }
    const parsed = parseExactSignature(
      envelope.value.signatureInput,
      envelope.value.signature,
    );
    if (
      Option.isNone(parsed) ||
      !hasExactSequence(parsed.value.fields, PROFILES[input.profile].fields)
    ) {
      return yield* new AuthenticationFailedError();
    }
    const accepted = yield* acceptSignatureParameters({
      parsed: parsed.value,
      profile: input.profile,
      publicKey: input.publicKey,
    });
    const publicKey = yield* ed25519VerificationKey(input.publicKey).pipe(
      Effect.catchTag("Ed25519PublicKeyOperationError", () =>
        Effect.fail(new AuthenticationFailedError()),
      ),
    );
    const verified = yield* verifyWithLibrary({
      httpRequest: input.httpRequest,
      profile: input.profile,
      url: envelope.value.url,
      keyid: accepted.keyid,
      publicKey,
    });
    if (verified !== true) {
      return yield* new AuthenticationFailedError();
    }
    return accepted.signature;
  }).pipe(Effect.withSpan("verifyHttpRequestSignature"));

/**
 * Verifies bootstrap HTTP proof between the Registry's key admission and durable
 * nonce admission. The nonce is claimed before checking live wire compatibility,
 * so an incompatible signed request still consumes its nonce.
 */
export function verifyBootstrapRequest<
  KeyError,
  KeyContext,
  NonceError,
  NonceContext,
>(input: {
  readonly httpRequest: HttpServerRequest.HttpServerRequest;
  readonly bodyBytes: Uint8Array;
  readonly admitPublicKey: (
    parsedBody: unknown,
  ) => Effect.Effect<AgentPublicKey, KeyError, KeyContext>;
  readonly claimNonce: (
    signature: VerifiedHttpSignature,
  ) => Effect.Effect<void, NonceError, NonceContext>;
}): Effect.Effect<
  {
    readonly parsedBody: unknown;
    readonly signature: VerifiedHttpSignature;
  },
  | KeyError
  | NonceError
  | MalformedRequestError
  | AuthenticationFailedError
  | VersionMismatchError,
  KeyContext | NonceContext
> {
  return Effect.gen(function* () {
    const parsedBody = yield* parseCanonicalJson(
      new Uint8Array(input.bodyBytes),
    ).pipe(
      Effect.catchTag("CanonicalJsonError", () =>
        Effect.fail(new MalformedRequestError()),
      ),
    );
    const publicKey = yield* input.admitPublicKey(parsedBody);
    const signature = yield* verifyHttpRequestSignature({
      httpRequest: input.httpRequest,
      bodyBytes: input.bodyBytes,
      publicKey,
      profile: "registration",
    });
    yield* input.claimNonce(signature);
    const version = input.httpRequest.headers["sharness-version"];
    if (version === undefined || !(yield* WireVersions.live(version))) {
      return yield* new VersionMismatchError();
    }
    return { parsedBody, signature };
  }).pipe(Effect.withSpan("verifyBootstrapRequest"));
}

/** Signs one canonical registered-agent envelope for its HTTP destination. */
export function signAgentRequest(input: {
  readonly httpRequest: HttpClientRequest.HttpClientRequest;
  readonly callerAgentId: AgentId;
  readonly encodedRequest: unknown;
  readonly signingAuthority: AgentSigningAuthority;
}): Effect.Effect<HttpClientRequest.HttpClientRequest, AgentSigningError> {
  return encodeCanonicalJson({
    callerAgentId: input.callerAgentId,
    request: input.encodedRequest,
  }).pipe(
    Effect.catchTag("CanonicalJsonError", () =>
      Effect.fail(new AgentSigningError()),
    ),
    Effect.flatMap((bodyBytes) =>
      signHttpRequest({
        httpRequest: input.httpRequest,
        bodyBytes,
        signingAuthority: input.signingAuthority,
        profile: "normal",
      }),
    ),
  );
}

/** Request body and caller identity established by Authentication. */
export type VerifiedAgentRequest = Readonly<{
  readonly callerAgentId: AgentId;
  readonly identityCard: VerifiedAgentIdentityCard;
  readonly request: unknown;
}> &
  Brand.Brand<"VerifiedAgentRequest">;

type RequestAuthenticationFailure =
  | MalformedRequestError
  | AuthenticationFailedError
  | VersionMismatchError
  | OverloadedError
  | UnavailableError;

/** Authentication fails before any operation receives a trusted caller. */
export class AuthenticationError extends Data.TaggedError(
  "AuthenticationError",
)<{
  readonly reason:
    | "malformed"
    | "authentication-failed"
    | "version-mismatch"
    | "overloaded"
    | "unavailable";
}> {}

interface AuthenticationService {
  readonly verifyRequest: (input: {
    readonly request: HttpServerRequest.HttpServerRequest;
    readonly body: Uint8Array;
  }) => Effect.Effect<VerifiedAgentRequest, AuthenticationError>;
}

/** Verifies signed request attribution and admits each nonce once. */
export class Authentication extends Context.Tag("SocialHarness/Authentication")<
  Authentication,
  AuthenticationService
>() {
  /** Returns verified caller claims before operation-specific decoding. */
  static readonly verifyRequest = Effect.serviceFunctionEffect(
    Authentication,
    (service) => service.verifyRequest,
  );
  /** Captures a card lookup and finite replay/lookup capacities without acquiring a Registry client. */
  static readonly layer = (
    input: AuthenticationSettings,
  ): Layer.Layer<Authentication> =>
    Layer.effect(Authentication, makeService(input));
}

interface AuthenticationSettings {
  readonly liveNonceCapacity: number;
  readonly identityCardCacheCapacity: number;
  readonly registryLookupConcurrencyLimit: number;
  readonly lookup: RegistryLookup;
}

function authenticationFailure(
  error: RequestAuthenticationFailure,
): AuthenticationError {
  switch (error._tag) {
    case "MalformedRequestError":
      return new AuthenticationError({ reason: "malformed" });
    case "AuthenticationFailedError":
      return new AuthenticationError({ reason: "authentication-failed" });
    case "VersionMismatchError":
      return new AuthenticationError({ reason: "version-mismatch" });
    case "OverloadedError":
      return new AuthenticationError({ reason: "overloaded" });
    case "UnavailableError":
      return new AuthenticationError({ reason: "unavailable" });
    default:
      return error satisfies never;
  }
}

const brandVerifiedAgentRequest = Brand.nominal<VerifiedAgentRequest>();

const registeredAgentBody = exactStruct({
  callerAgentId: AgentId,
  request: Schema.Unknown,
});

function makeService(
  input: AuthenticationSettings,
): Effect.Effect<AuthenticationService> {
  return Effect.gen(function* () {
    const nonces = yield* Ref.make<ReadonlyMap<string, number>>(new Map());
    const resolveCard = yield* makeCardResolver(
      {
        cacheCapacity: input.identityCardCacheCapacity,
        lookupConcurrencyLimit: input.registryLookupConcurrencyLimit,
      },
      input.lookup,
    );
    return Authentication.of({
      verifyRequest: (request) =>
        verifyRequest(request, {
          liveNonceCapacity: input.liveNonceCapacity,
          nonces,
          resolveCard,
        }).pipe(Effect.mapError(authenticationFailure)),
    });
  });
}

function verifyRequest(
  input: {
    readonly body: Uint8Array;
    readonly request: HttpServerRequest.HttpServerRequest;
  },
  dependencies: {
    readonly liveNonceCapacity: number;
    readonly nonces: Ref.Ref<ReadonlyMap<string, number>>;
    readonly resolveCard: CardResolver;
  },
): Effect.Effect<VerifiedAgentRequest, RequestAuthenticationFailure> {
  return Effect.gen(function* () {
    const body = yield* decodeCanonicalJson(
      registeredAgentBody,
      new Uint8Array(input.body),
    ).pipe(
      Effect.catchTag("CanonicalJsonError", () =>
        Effect.fail(new MalformedRequestError()),
      ),
    );
    const identityCard = yield* dependencies.resolveCard(body.callerAgentId);
    const signature = yield* verifyHttpRequestSignature({
      httpRequest: input.request,
      bodyBytes: input.body,
      publicKey: identityCard.publicKey,
      profile: "normal",
    });
    const claim = yield* claimNonce(dependencies.nonces, {
      capacity: dependencies.liveNonceCapacity,
      nonce: signature.nonce,
      expires: signature.expires,
      now: signature.verifiedAt,
    });
    if (claim === "replayed") {
      return yield* new AuthenticationFailedError();
    }
    if (claim === "full") {
      return yield* new OverloadedError();
    }
    const version = input.request.headers["sharness-version"];
    if (version === undefined || !(yield* WireVersions.live(version))) {
      return yield* new VersionMismatchError();
    }
    return makeVerifiedAgentRequest({
      callerAgentId: body.callerAgentId,
      identityCard: identityCard,
      request: body.request,
    });
  });
}

type CardResolutionError =
  | AuthenticationFailedError
  | OverloadedError
  | UnavailableError;

/** In-flight card lookups, keyed by agent, each awaited by later callers. */
type PendingResolutions = ReadonlyMap<
  AgentId,
  Deferred.Deferred<VerifiedAgentIdentityCard, CardResolutionError>
>;

type RegistryLookup = (request: {
  readonly agentId: AgentId;
}) => Effect.Effect<RegistryLookupResult, unknown>;

type CacheAccess =
  | Readonly<{
      readonly kind: "cached";
      readonly card: VerifiedAgentIdentityCard;
    }>
  | Readonly<{
      readonly kind: "join";
      readonly deferred: Deferred.Deferred<
        VerifiedAgentIdentityCard,
        CardResolutionError
      >;
    }>
  | Readonly<{
      readonly kind: "lead";
      readonly deferred: Deferred.Deferred<
        VerifiedAgentIdentityCard,
        CardResolutionError
      >;
    }>;

type RestoreInterruptibility = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
) => Effect.Effect<A, E, R>;

interface CardResolutionSettings {
  readonly cacheCapacity: number;
  readonly lookupConcurrencyLimit: number;
}

interface CardResolverDependencies {
  readonly pendingResolutions: Ref.Ref<PendingResolutions>;
  readonly cards: BoundedCache<AgentId, VerifiedAgentIdentityCard>;
  readonly lookup: RegistryLookup;
  readonly lookupPermits: Effect.Semaphore;
}

function makeCardResolver(
  settings: CardResolutionSettings,
  lookup: RegistryLookup,
): Effect.Effect<
  (
    agentId: AgentId,
  ) => Effect.Effect<VerifiedAgentIdentityCard, CardResolutionError>
> {
  return Effect.gen(function* () {
    const pendingResolutions = yield* Ref.make<PendingResolutions>(new Map());
    const lookupPermits = yield* Effect.makeSemaphore(
      settings.lookupConcurrencyLimit,
    );
    const dependencies: CardResolverDependencies = {
      pendingResolutions,
      cards: makeBoundedCache<AgentId, VerifiedAgentIdentityCard>(
        settings.cacheCapacity,
      ),
      lookup,
      lookupPermits,
    };
    return (
      agentId: AgentId,
    ): Effect.Effect<VerifiedAgentIdentityCard, CardResolutionError> =>
      Effect.uninterruptibleMask((restoreInterruptibility) =>
        resolveCard(dependencies, agentId, restoreInterruptibility),
      );
  });
}

type CardResolver = Effect.Effect.Success<ReturnType<typeof makeCardResolver>>;

function resolveCard(
  dependencies: CardResolverDependencies,
  agentId: AgentId,
  restoreInterruptibility: RestoreInterruptibility,
): Effect.Effect<VerifiedAgentIdentityCard, CardResolutionError> {
  return Effect.gen(function* () {
    const candidate = yield* Deferred.make<
      VerifiedAgentIdentityCard,
      CardResolutionError
    >();
    const access = yield* beginCardResolution(
      dependencies.pendingResolutions,
      dependencies.cards,
      agentId,
      candidate,
    );
    return yield* resolveCacheAccess(
      dependencies,
      agentId,
      access,
      restoreInterruptibility,
    );
  });
}

function resolveCacheAccess(
  dependencies: CardResolverDependencies,
  agentId: AgentId,
  access: CacheAccess,
  restoreInterruptibility: RestoreInterruptibility,
): Effect.Effect<VerifiedAgentIdentityCard, CardResolutionError> {
  // eslint-disable-next-line default-case -- the switch is exhaustive
  switch (access.kind) {
    case "cached":
      return Effect.succeed(access.card);
    case "join":
      return restoreInterruptibility(Deferred.await(access.deferred));
    case "lead":
      return Effect.forkDaemon(
        runLeadingResolution(dependencies, agentId, access.deferred).pipe(
          Effect.interruptible,
        ),
      ).pipe(
        Effect.zipRight(
          restoreInterruptibility(Deferred.await(access.deferred)),
        ),
      );
  }
}

function runLeadingResolution(
  dependencies: CardResolverDependencies,
  agentId: AgentId,
  deferred: Deferred.Deferred<VerifiedAgentIdentityCard, CardResolutionError>,
): Effect.Effect<VerifiedAgentIdentityCard, CardResolutionError> {
  return lookupCard(dependencies, agentId).pipe(
    Effect.tap((card) =>
      retainResolvedCard(dependencies.cards, {
        agentId,
        card,
      }),
    ),
    Effect.onExit((exit) =>
      removePendingResolution(dependencies.pendingResolutions, agentId).pipe(
        Effect.zipRight(Deferred.done(deferred, exit)),
      ),
    ),
  );
}

/**
 * Lookup failure or defect settles pending resolution as unavailable.
 * Interruption remains interruption.
 */
function lookupCard(
  dependencies: CardResolverDependencies,
  agentId: AgentId,
): Effect.Effect<VerifiedAgentIdentityCard, CardResolutionError> {
  return dependencies.lookupPermits
    .withPermitsIfAvailable(1)(
      Effect.suspend(() => dependencies.lookup({ agentId })).pipe(
        Effect.catchAllCause((cause) =>
          Cause.isInterrupted(cause)
            ? Effect.interrupt
            : Effect.fail(new UnavailableError()),
        ),
      ),
    )
    .pipe(
      Effect.flatMap((attempted) => resolveLookupAttempt(attempted, agentId)),
    );
}

const isVerifiedIdentityCard = Schema.is(verifiedAgentIdentityCardSchema);

/**
 * Requires a card this package verified for the requested agent. An explicit
 * missing identity refuses authentication. Other provider failures leave it
 * unavailable. Provider getters may throw.
 */
function resolveLookupAttempt(
  attempted: Option.Option<RegistryLookupResult>,
  agentId: AgentId,
): Effect.Effect<VerifiedAgentIdentityCard, CardResolutionError> {
  if (Option.isNone(attempted)) {
    return Effect.fail(new OverloadedError());
  }
  const result = attempted.value;
  return Effect.try({
    try: (): Effect.Effect<
      VerifiedAgentIdentityCard,
      AuthenticationFailedError | UnavailableError
    > => {
      const kind = result.kind;
      if (kind === "not_found") {
        return Effect.fail(new AuthenticationFailedError());
      }
      const identityCard = result.identityCard;
      return kind === "found" &&
        isVerifiedIdentityCard(identityCard) &&
        identityCard.agentId === agentId
        ? Effect.succeed(identityCard)
        : Effect.fail(new UnavailableError());
    },
    catch: () => new UnavailableError(),
  }).pipe(Effect.flatten);
}

/** Synchronous cache access and pending-lookup selection cannot interleave. */
function beginCardResolution(
  pendingResolutions: Ref.Ref<PendingResolutions>,
  cards: BoundedCache<AgentId, VerifiedAgentIdentityCard>,
  agentId: AgentId,
  candidate: Deferred.Deferred<VerifiedAgentIdentityCard, CardResolutionError>,
): Effect.Effect<CacheAccess> {
  return Ref.modify(
    pendingResolutions,
    (current): readonly [CacheAccess, PendingResolutions] => {
      const card = cards.get(agentId);
      if (Option.isSome(card)) {
        return [{ kind: "cached", card: card.value }, current];
      }
      const pending = current.get(agentId);
      if (pending !== undefined) {
        return [{ kind: "join", deferred: pending }, current];
      }
      return [
        { kind: "lead", deferred: candidate },
        new Map(current).set(agentId, candidate),
      ];
    },
  );
}

function removePendingResolution(
  pendingResolutions: Ref.Ref<PendingResolutions>,
  agentId: AgentId,
): Effect.Effect<void> {
  return Ref.update(pendingResolutions, (current) => {
    const pending = new Map(current);
    pending.delete(agentId);
    return pending;
  });
}

function retainResolvedCard(
  cards: BoundedCache<AgentId, VerifiedAgentIdentityCard>,
  input: {
    readonly agentId: AgentId;
    readonly card: VerifiedAgentIdentityCard;
  },
): Effect.Effect<void> {
  return Effect.sync(() => {
    cards.set(input.agentId, input.card);
  });
}

type NonceClaim = "claimed" | "replayed" | "full";

interface NonceClaimInput {
  readonly capacity: number;
  readonly nonce: string;
  readonly expires: number;
  readonly now: number;
}

function claimNonce(
  nonces: Ref.Ref<ReadonlyMap<string, number>>,
  input: NonceClaimInput,
): Effect.Effect<NonceClaim> {
  return Ref.modify(nonces, (current) => updateNonces(current, input));
}

function updateNonces(
  current: ReadonlyMap<string, number>,
  input: NonceClaimInput,
): readonly [NonceClaim, ReadonlyMap<string, number>] {
  const live = new Map<string, number>();
  for (const [retainedNonce, retainedExpiry] of current) {
    if (retainedExpiry >= input.now) {
      live.set(retainedNonce, retainedExpiry);
    }
  }
  if (live.has(input.nonce)) {
    return ["replayed", live];
  }
  if (live.size >= input.capacity) {
    return ["full", live];
  }
  live.set(input.nonce, input.expires);
  return ["claimed", live];
}

function makeVerifiedAgentRequest(input: {
  readonly callerAgentId: AgentId;
  readonly identityCard: VerifiedAgentIdentityCard;
  readonly request: unknown;
}): VerifiedAgentRequest {
  return brandVerifiedAgentRequest(Object.freeze({ ...input }));
}
