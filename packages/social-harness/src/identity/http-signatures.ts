/** @file Creates and verifies HTTP signature proofs and their closed wire failures. */

import type { CryptoKey } from "jose";
import { HttpClientRequest, type HttpServerRequest } from "@effect/platform";
import { Clock, Data, Effect, Option, Schema } from "effect";
import { httpbis, type VerifyingKey } from "http-message-signatures";
import { createHash, randomBytes, webcrypto } from "node:crypto";
import {
  type BareItem,
  type InnerList,
  type Item,
  parseDictionary,
  serializeDictionary,
} from "structured-headers";
import type { AgentId } from "./agent.js";
import {
  encodeCanonicalJson,
  parseCanonicalJson,
} from "../encoding/canonical-json.js";
import { hasCanonicalBase64UrlLength } from "../encoding/identifier.js";
import {
  type AgentPublicKey,
  AgentSigningAuthority,
  agentSigningPrivateKey,
  ed25519PublicKeyThumbprintUri,
  ed25519VerificationKey,
  hasCanonicalEd25519SignatureEncoding,
} from "./keys.js";
import {
  LIVE_WIRE_VERSIONS,
  WIRE_VERSION_PATTERN,
  WireVersions,
} from "./wire-version.js";

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
 * Installs one exact Social Harness HTTP signature profile on a prepared request.
 *
 * @param input Request and fixed profile inputs.
 * @param input.httpRequest Request whose method and URL are already selected.
 * @param input.bodyBytes Canonical body octets to install and sign.
 * @param input.signingAuthority Opaque authority for the caller key.
 * @param input.profile Route-owned signature profile.
 * @returns The request with its body and exact signed fields installed.
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
 * Verifies the exact profile and returns its replay nonce.
 *
 * @param input Request and fixed verification inputs.
 * @param input.httpRequest Bounded server request at the selected route.
 * @param input.bodyBytes Copied body octets covered by the digest.
 * @param input.publicKey Public key selected by the boundary owner.
 * @param input.profile Route-owned signature profile.
 * @returns Replay material bound to the exact accepted verification second.
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
