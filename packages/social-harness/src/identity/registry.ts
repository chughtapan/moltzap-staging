/** @file Registry requests, wire representations and verified HTTP calls. */
import {
  HttpClient,
  HttpClientRequest,
  type HttpClientResponse,
} from "@effect/platform";
import {
  Context,
  Data,
  Duration,
  Effect,
  Layer,
  Redacted,
  Schema,
} from "effect";
import {
  decodeCanonicalJson,
  encodeCanonicalJson,
  exactParseOptions,
  exactStruct,
} from "../encoding/canonical-json.js";
import { canonicalIdentifier } from "../encoding/identifier.js";
import { WireVersions } from "../version.js";
import {
  AgentId,
  AgentIdentityCard,
  AgentName,
  PrincipalId,
  type VerifiedAgentIdentityCard,
  verifiedAgentIdentityCardSchema,
} from "./agent-cards.js";
import {
  signHttpRequest,
  VersionMismatchError,
} from "./http-authentication.js";
import { AgentPublicKey, AgentSigningAuthority } from "./keys.js";

/** Idempotency identity for a registration operation. */
// eslint-disable-next-line @typescript-eslint/naming-convention -- Effect Schemas share the public domain name they decode.
export const RegistrationId = canonicalIdentifier("RegistrationId", "opn_", 16);
/** Validated nominal value decoded by RegistrationId. */
// eslint-disable-next-line @typescript-eslint/no-redeclare -- The same-named Schema and type form one boundary model.
export type RegistrationId = typeof RegistrationId.Type;

/** Closed Registry bootstrap registration request. */
// eslint-disable-next-line @typescript-eslint/naming-convention -- Effect Schemas share the public domain name they decode.
export const RegistryRegisterRequest = exactStruct({
  operationId: RegistrationId,
  principalId: PrincipalId,
  agentName: AgentName,
  publicKey: AgentPublicKey,
}).annotations({ identifier: "RegistryRegisterRequest" });
/** Validated Registry bootstrap registration request. */
// eslint-disable-next-line @typescript-eslint/no-redeclare -- The same-named Schema and type form one boundary model.
export type RegistryRegisterRequest = typeof RegistryRegisterRequest.Type;

/** Closed Registry lookup selector. */
// eslint-disable-next-line @typescript-eslint/naming-convention -- Effect Schemas share the public domain name they decode.
const RegistryLookupRequest = Schema.Union(
  exactStruct({
    agentId: Schema.propertySignature(AgentId).annotations({
      description:
        "Look up this Registry-minted agent identity (agt_ plus canonical base64url for 16 bytes); use alone.",
    }),
  }),
  exactStruct({
    agentName: Schema.propertySignature(AgentName).annotations({
      description:
        "Look up this immutable Registry-wide agent handle; use alone.",
    }),
  }),
).annotations({
  identifier: "RegistryLookupRequest",
  parseOptions: exactParseOptions,
});
/** Validated Registry lookup selector. */
// eslint-disable-next-line @typescript-eslint/no-redeclare -- The same-named Schema and type form one boundary model.
type RegistryLookupRequest = typeof RegistryLookupRequest.Type;

/**
 * Returns name, key and idempotency conflicts as results. Authentication and
 * transport failures remain errors.
 */
export type RegistryRegisterResult =
  | Readonly<{ kind: "registered"; identityCard: VerifiedAgentIdentityCard }>
  | Readonly<{ kind: "name_taken" }>
  | Readonly<{ kind: "key_already_registered" }>
  | Readonly<{ kind: "idempotency_conflict" }>;

/** A found card is Registry-verified. A missing identity is a normal result. */
export type RegistryLookupResult =
  | Readonly<{ kind: "found"; identityCard: VerifiedAgentIdentityCard }>
  | Readonly<{ kind: "not_found" }>;

/** The admission credential authorizes registration, and the signing authority proves possession of the agent key. */
type RegistryRegisterCall = Readonly<{
  request: Omit<RegistryRegisterRequest, "publicKey">;
  admissionCredential: Redacted.Redacted;
  signingAuthority: AgentSigningAuthority;
}>;

/** Validated registration outcomes shared through registryContract. */
const registerResultSchema = Schema.Union(
  exactStruct({
    kind: Schema.Literal("registered"),
    agentCard: verifiedAgentIdentityCardSchema,
  }),
  exactStruct({ kind: Schema.Literal("name_taken") }),
  exactStruct({ kind: Schema.Literal("key_already_registered") }),
  exactStruct({ kind: Schema.Literal("idempotency_conflict") }),
);

/** Validated lookup outcomes shared through registryContract. */
const lookupResultSchema = Schema.Union(
  exactStruct({
    kind: Schema.Literal("found"),
    agentCard: verifiedAgentIdentityCardSchema,
  }),
  exactStruct({ kind: Schema.Literal("not_found") }),
);

/** Exact registration response representation used by both HTTP adapters. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- Private HTTP and storage adapters share this Schema; the package root does not export it.
export const registerResponseSchema = Schema.Union(
  exactStruct({
    kind: Schema.Literal("registered"),
    agentCard: AgentIdentityCard,
  }),
  exactStruct({ kind: Schema.Literal("name_taken") }),
  exactStruct({ kind: Schema.Literal("key_already_registered") }),
  exactStruct({ kind: Schema.Literal("idempotency_conflict") }),
);

/** Exact lookup response representation used by both HTTP adapters. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- Private HTTP adapters share this Schema; the package root does not export it.
export const lookupResponseSchema = Schema.Union(
  exactStruct({
    kind: Schema.Literal("found"),
    agentCard: AgentIdentityCard,
  }),
  exactStruct({ kind: Schema.Literal("not_found") }),
);

/** Exact bootstrap registration HTTP body. */
const registrationBody = exactStruct({
  request: RegistryRegisterRequest,
});

/** Exact bootstrap registration request path. */
const REGISTER_PATH = "/v1/identities:register";
/** Exact public lookup request path. */
const LOOKUP_PATH = "/v1/identities:lookup";
/** Exact Registry readiness path. */
const HEALTH_PATH = "/healthz";

/** Effect Router patterns escape the literal colon in Registry paths. */
const REGISTER_ROUTE = "/v1/identities::register";
/** Effect Router pattern for the literal lookup path. */
const LOOKUP_ROUTE = "/v1/identities::lookup";
/** Exact paths used to classify method mismatches at the fallback route. */
const KNOWN_PATHS = new Set([REGISTER_PATH, LOOKUP_PATH, HEALTH_PATH]);

const maximumIdentifier = "agt_AAAAAAAAAAAAAAAAAAAAAA";
const maximumOperationId = "opn_AAAAAAAAAAAAAAAAAAAAAA";
const maximumPrincipalId = "prn_AAAAAAAAAAAAAAAAAAAAAA";
const maximumName = "a".repeat(32);
const maximumCoordinate = "A".repeat(43);
const byteLength = (value: unknown): number =>
  new TextEncoder().encode(JSON.stringify(value)).byteLength;

/** Derived exact maximum bootstrap registration body size. */
const REGISTER_BODY_CAP = byteLength({
  request: {
    operationId: maximumOperationId,
    principalId: maximumPrincipalId,
    agentName: maximumName,
    publicKey: {
      crv: "Ed25519",
      kty: "OKP",
      x: maximumCoordinate,
    },
  },
});

/** Derived exact maximum public lookup body size. */
const LOOKUP_BODY_CAP = Math.max(
  byteLength({ agentId: maximumIdentifier }),
  byteLength({ agentName: maximumName }),
);

/** Registration body, durable operation identity, and response share exact codecs. */
const registryRegisterCodec = {
  request: { encode: canonicalCodec(RegistryRegisterRequest).encode },
  body: { encode: canonicalCodec(registrationBody).encode },
  response: canonicalCodec(registerResponseSchema),
};

/** Public lookup bytes use the same closed representations on both HTTP sides. */
const registryLookupCodec = {
  request: canonicalCodec(RegistryLookupRequest),
  response: canonicalCodec(lookupResponseSchema),
};

/** Stored cards retain their complete canonical signed representation. */
const registryIdentityCardCodec = canonicalCodec(AgentIdentityCard);

/** Registry HTTP adapters and persistence share these exact routes, bounds, request and result schemas, and canonical codecs. */
export const registryContract = Object.freeze({
  HEALTH_PATH,
  KNOWN_PATHS,
  LOOKUP_BODY_CAP,
  LOOKUP_PATH,
  LOOKUP_ROUTE,
  lookupResultSchema,
  RegistrationId,
  REGISTER_BODY_CAP,
  REGISTER_PATH,
  REGISTER_ROUTE,
  registerResultSchema,
  registrationBody,
  registryIdentityCardCodec,
  registryLookupCodec,
  RegistryLookupRequest,
  registryRegisterCodec,
  RegistryRegisterRequest,
});

/** Registry access failed before a verified result could be returned. */
export class RegistryError extends Data.TaggedError("RegistryError")<{
  readonly reason:
    | "connection"
    | "timeout"
    | "invalid-response"
    | "signing"
    | "http-envelope"
    | "admission-refused";
}> {}
/** Complete registration and identity lookup calls to a Registry service. */
interface RegistryService {
  readonly register: (
    call: RegistryRegisterCall,
  ) => Effect.Effect<RegistryRegisterResult, RegistryError>;
  readonly lookup: (
    request: RegistryLookupRequest,
  ) => Effect.Effect<RegistryLookupResult, RegistryError>;
}

/** Registers an agent key and resolves identities through a pinned Registry. */
export class Registry extends Context.Tag("SocialHarness/Registry")<
  Registry,
  RegistryService
>() {
  /** Submits a registration attempt with a fresh HTTP proof and its retained operation id. */
  static readonly register = Effect.serviceFunctionEffect(
    Registry,
    (service) => service.register,
  );
  /** Resolves an immutable identity card. An absent identity returns `not_found`. */
  static readonly lookup = Effect.serviceFunctionEffect(
    Registry,
    (service) => service.lookup,
  );
  /** Pins transport, deadline and Registry signing authority for this client. */
  static readonly layer = (input: {
    readonly origin: URL;
    readonly registrySignerPublicKey: AgentPublicKey;
    readonly requestTimeout: Duration.Duration;
  }): Layer.Layer<Registry, never, HttpClient.HttpClient> =>
    Layer.effect(Registry, makeRegistryService(input));
}

function makeRegistryService(input: {
  readonly origin: URL;
  readonly registrySignerPublicKey: AgentPublicKey;
  readonly requestTimeout: Duration.Duration;
}): Effect.Effect<
  Context.Tag.Service<typeof Registry>,
  never,
  HttpClient.HttpClient
> {
  const origin = new URL(input.origin.href);
  const requestTimeout = Duration.millis(
    Duration.toMillis(input.requestTimeout),
  );
  return Effect.map(
    HttpClient.HttpClient,
    (client): Context.Tag.Service<typeof Registry> => {
      const register = makeRegister({
        client,
        origin,
        registrySignerPublicKey: input.registrySignerPublicKey,
        requestTimeout,
      });
      const lookup = makeLookup({
        client,
        origin,
        registrySignerPublicKey: input.registrySignerPublicKey,
        requestTimeout,
      });
      return Registry.of({
        register: (call) => register(call),
        lookup: (request) => lookup(request),
      });
    },
  );
}

const errorEnvelope = new Map<number, string>([
  [400, '{"error":"malformed"}'],
  [401, '{"error":"authentication_failed"}'],
  [404, '{"error":"not_found"}'],
  [405, '{"error":"method_not_allowed"}'],
  [413, '{"error":"payload_too_large"}'],
  [415, '{"error":"unsupported_media_type"}'],
  [429, '{"error":"overloaded"}'],
  [503, '{"error":"unavailable"}'],
  [500, '{"error":"internal"}'],
]);

const invalidResponse = (): RegistryError =>
  new RegistryError({ reason: "invalid-response" });

const textDecoder = new TextDecoder("utf-8", {
  fatal: true,
  ignoreBOM: true,
});

const exactText = (bytes: Uint8Array): string | undefined => {
  try {
    return textDecoder.decode(bytes);
    // eslint-disable-next-line agent-code-guard/bare-catch -- Invalid UTF-8 is an ordinary closed-response validation failure.
  } catch {
    return undefined;
  }
};

type ClientOperation = "register" | "lookup";
type RegistryClientInput = Readonly<{
  client: HttpClient.HttpClient;
  origin: URL;
  registrySignerPublicKey: AgentPublicKey;
  requestTimeout: Duration.Duration;
}>;

const connectionFailure = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, RegistryError, R> =>
  effect.pipe(
    // eslint-disable-next-line agent-code-guard/no-effect-error-coalescing -- The client exposes one transport failure without leaking platform implementation details.
    Effect.mapError(() => new RegistryError({ reason: "connection" })),
  );

const readResponseBytes = (
  response: HttpClientResponse.HttpClientResponse,
): Effect.Effect<Uint8Array, RegistryError> =>
  connectionFailure(response.arrayBuffer).pipe(
    Effect.map((buffer) => new Uint8Array(buffer)),
  );

const decodeEnvelope = (
  operation: ClientOperation,
  response: HttpClientResponse.HttpClientResponse,
  bytes: Uint8Array,
): Effect.Effect<never, RegistryError> => {
  if (response.headers["content-type"] !== "application/json") {
    return Effect.fail(invalidResponse());
  }
  if (response.status === 412) {
    return decodeCanonicalJson(
      Schema.Struct({
        error: Schema.Literal("version_mismatch"),
        supported: VersionMismatchError.supportedSchema,
      }),
      bytes,
    ).pipe(
      Effect.mapError(invalidResponse),
      Effect.flatMap(() =>
        Effect.fail(new RegistryError({ reason: "http-envelope" })),
      ),
    );
  }
  const expected = errorEnvelope.get(response.status);
  const actual = exactText(bytes);
  if (expected === undefined || actual !== expected) {
    return Effect.fail(invalidResponse());
  }
  if (operation === "lookup" && response.status === 401) {
    return Effect.fail(invalidResponse());
  }
  return Effect.fail(
    new RegistryError({
      reason: response.status === 401 ? "admission-refused" : "http-envelope",
    }),
  );
};

const execute = (input: {
  readonly client: HttpClient.HttpClient;
  readonly request: HttpClientRequest.HttpClientRequest;
  readonly operation: ClientOperation;
}): Effect.Effect<
  readonly [HttpClientResponse.HttpClientResponse, Uint8Array],
  RegistryError
> =>
  connectionFailure(input.client.execute(input.request)).pipe(
    Effect.flatMap((response) =>
      readResponseBytes(response).pipe(
        Effect.flatMap((bytes) =>
          response.status === 200
            ? Effect.succeed<
                readonly [HttpClientResponse.HttpClientResponse, Uint8Array]
              >([response, bytes])
            : decodeEnvelope(input.operation, response, bytes),
        ),
      ),
    ),
  );

const requireJsonSuccess = (
  response: HttpClientResponse.HttpClientResponse,
): Effect.Effect<void, RegistryError> =>
  response.headers["content-type"] === "application/json"
    ? Effect.void
    : Effect.fail(invalidResponse());

const verifyCard = (
  agentCard: AgentIdentityCard,
  registrySignerPublicKey: AgentPublicKey,
): Effect.Effect<VerifiedAgentIdentityCard, RegistryError> =>
  AgentIdentityCard.verify({
    identityCard: agentCard,
    registrySignerPublicKey,
  }).pipe(Effect.mapError(invalidResponse));

const withDeadline = <A, E>(
  effect: Effect.Effect<A, E>,
  timeout: Duration.Duration,
): Effect.Effect<A, E | RegistryError> =>
  effect.pipe(
    Effect.timeoutFail({
      duration: timeout,
      onTimeout: () => new RegistryError({ reason: "timeout" }),
    }),
  );

const makeSignedRegistrationRequest = (
  input: RegistryClientInput,
  call: RegistryRegisterCall,
  registerRequest: typeof RegistryRegisterRequest.Type,
) =>
  Effect.gen(function* () {
    const bodyBytes = yield* registryRegisterCodec.body
      .encode({
        request: registerRequest,
      })
      .pipe(Effect.mapError(invalidResponse));
    const httpRequest = HttpClientRequest.post(
      new URL(REGISTER_PATH, input.origin),
    ).pipe(
      HttpClientRequest.setHeader(
        "authorization",
        `Sharness-Admission ${Redacted.value(call.admissionCredential)}`,
      ),
    );
    return yield* signHttpRequest({
      httpRequest,
      bodyBytes,
      signingAuthority: call.signingAuthority,
      profile: "registration",
    });
  });

/**
 * A successful registration returns the requested principal and name under
 * the key derived from the signing authority.
 */
const registrationMatchesCall = (
  agentCard: VerifiedAgentIdentityCard,
  call: RegistryRegisterCall,
  registerRequest: typeof RegistryRegisterRequest.Type,
): boolean =>
  agentCard.principalId === registerRequest.principalId &&
  agentCard.agentName === registerRequest.agentName &&
  agentCard.publicKey.x ===
    AgentSigningAuthority.publicKey(call.signingAuthority).x;

const decodeRegistrationResult = (
  input: RegistryClientInput,
  call: RegistryRegisterCall,
  registerRequest: typeof RegistryRegisterRequest.Type,
  responseBytes: Uint8Array,
): Effect.Effect<RegistryRegisterResult, RegistryError> =>
  Effect.gen(function* () {
    const result = yield* registryRegisterCodec.response
      .decode(responseBytes)
      .pipe(Effect.mapError(invalidResponse));
    if (result.kind !== "registered") {
      return Object.freeze({ kind: result.kind });
    }
    const agentCard = yield* verifyCard(
      result.agentCard,
      input.registrySignerPublicKey,
    );
    if (!registrationMatchesCall(agentCard, call, registerRequest)) {
      return yield* invalidResponse();
    }
    return Object.freeze({ kind: "registered", identityCard: agentCard });
  });

function makeRegister(
  input: RegistryClientInput,
): (
  call: RegistryRegisterCall,
) => Effect.Effect<RegistryRegisterResult, RegistryError> {
  return (candidate) =>
    Effect.gen(function* () {
      const call = candidate;
      const registerRequest = {
        ...call.request,
        publicKey: AgentSigningAuthority.publicKey(call.signingAuthority),
      };
      const request = yield* makeSignedRegistrationRequest(
        input,
        call,
        registerRequest,
      );
      const [response, responseBytes] = yield* execute({
        client: input.client,
        request,
        operation: "register",
      });
      yield* requireJsonSuccess(response);
      return yield* decodeRegistrationResult(
        input,
        call,
        registerRequest,
        responseBytes,
      );
    }).pipe(
      (effect) => withDeadline(effect, input.requestTimeout),
      Effect.catchTag(
        "AgentSigningError",
        () => new RegistryError({ reason: "signing" }),
      ),
    );
}

function makeLookup(
  input: RegistryClientInput,
): (
  call: typeof RegistryLookupRequest.Type,
) => Effect.Effect<RegistryLookupResult, RegistryError> {
  return (candidate) =>
    Effect.gen(function* () {
      const call = candidate;
      const bodyBytes = yield* registryLookupCodec.request
        .encode(call)
        .pipe(Effect.mapError(invalidResponse));
      const request = HttpClientRequest.post(
        new URL(LOOKUP_PATH, input.origin),
      ).pipe(
        HttpClientRequest.bodyUint8Array(bodyBytes, "application/json"),
        HttpClientRequest.setHeader(
          "sharness-version",
          yield* WireVersions.emitted,
        ),
      );
      const [response, responseBytes] = yield* execute({
        operation: "lookup",
        client: input.client,
        request,
      });
      yield* requireJsonSuccess(response);
      const result = yield* registryLookupCodec.response
        .decode(responseBytes)
        .pipe(Effect.mapError(invalidResponse));
      if (result.kind === "not_found") {
        return Object.freeze({ kind: "not_found" });
      }
      const agentCard = yield* verifyCard(
        result.agentCard,
        input.registrySignerPublicKey,
      );
      if (
        ("agentId" in call && agentCard.agentId !== call.agentId) ||
        ("agentName" in call && agentCard.agentName !== call.agentName)
      ) {
        return yield* invalidResponse();
      }
      return Object.freeze({ kind: "found", identityCard: agentCard });
    }).pipe((effect) => withDeadline(effect, input.requestTimeout));
}

function canonicalCodec<A, I>(schema: Schema.Schema<A, I>) {
  return {
    encode: (value: A) =>
      Schema.encode(schema)(value).pipe(Effect.flatMap(encodeCanonicalJson)),
    decode: (bytes: Uint8Array) => decodeCanonicalJson(schema, bytes),
  };
}
