/** @file Authenticated Router HTTP calls share one wire grammar and closed failures. */

import {
  HttpClient,
  HttpClientRequest,
  type HttpClientResponse,
} from "@effect/platform";
import canonicalize from "canonicalize";
import {
  Context,
  Data,
  Duration,
  Effect,
  Either,
  Encoding,
  Layer,
  Option,
  Schema,
} from "effect";
import type { AgentSigningAuthority } from "../../identity/keys.js";
import { decodeCanonical } from "../../encoding/canonical-json.js";
import {
  canonicalIdentifier,
  hasCanonicalBase64UrlLength,
} from "../../encoding/identifier.js";
import { AgentId } from "../../identity/agent-cards.js";
import {
  AgentSigningError,
  AuthenticationFailedError,
  InternalServerError,
  MalformedRequestError,
  MethodNotAllowedError,
  OverloadedError,
  PayloadTooLargeError,
  RouteNotFoundError,
  signAgentRequest,
  UnavailableError,
  UnsupportedMediaTypeError,
  VersionMismatchError,
} from "../../identity/http-authentication.js";
import { SignedMessage } from "../../identity/message-security.js";
import { LIVE_WIRE_VERSIONS } from "../../version.js";

/** Exact Router send target. */
const routerSendPath = "/v1/messages:send";

/** Exact Router poll target. */
const routerPollPath = "/v1/messages:poll";

/** Exact Router health target. */
const routerHealthPath = "/healthz";

/** Effect HTTP Router pattern for the exact send target. */
const routerSendRoutePattern = "/v1/messages::send";

/** Effect HTTP Router pattern for the exact poll target. */
const routerPollRoutePattern = "/v1/messages::poll";

/** Exact media type for Router-owned JSON representations. */
const routerJsonContentType = "application/json";

const INSTANCE_BYTE_LENGTH = 16;

const DIGEST_BYTE_LENGTH = 32;

/** Exact protected header carried by every PollCursor. */
const pollCursorProtectedHeader = Object.freeze({
  alg: "dir" as const,
  enc: "A256GCM" as const,
  typ: "application/vnd.sharness.poll-cursor+jwe",
});

/** The fixed string fields are declared in canonical key order. */
const pollCursorProtectedHeaderJson = JSON.stringify(pollCursorProtectedHeader);

/** Canonical encoded protected header carried by every PollCursor. */
const encodedPollCursorProtectedHeader = Encoding.encodeBase64Url(
  new TextEncoder().encode(pollCursorProtectedHeaderJson),
);

/** Prefix separating PollCursor values from unrefined compact JWE text. */
const pollCursorPrefix = "plc_";

/* eslint-disable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare --
 * Named Effect Schemas share their domain names with the types they decode.
 */

/** Identifies one volatile Router process instance. */
export const RouterInstanceId = canonicalIdentifier(
  "RouterInstanceId",
  "rti_",
  INSTANCE_BYTE_LENGTH,
);

/**
 * Validated Router process identity.
 * @useDeclaredType
 */
export type RouterInstanceId = typeof RouterInstanceId.Type;

/** Equality receipt for one complete retained SignedMessage. */
export const SignedMessageDigest = canonicalIdentifier(
  "SignedMessageDigest",
  "smd_",
  DIGEST_BYTE_LENGTH,
);

/**
 * Validated SignedMessage equality receipt.
 * @useDeclaredType
 */
export type SignedMessageDigest = typeof SignedMessageDigest.Type;

/** Opaque, authenticated continuation for one caller and Router instance. */
export const PollCursor = Schema.String.pipe(
  Schema.filter(hasCanonicalCursorShape, {
    identifier: "PollCursor",
    description: "Canonical opaque Router poll continuation",
  }),
  Schema.brand("PollCursor"),
  Schema.annotations({
    identifier: "PollCursor",
    description: "Canonical opaque Router poll continuation",
  }),
);

/**
 * Validated opaque Router poll continuation.
 * @useDeclaredType
 */
export type PollCursor = typeof PollCursor.Type;

const exact = {
  exact: true,
  onExcessProperty: "error" as const,
};

const closedStruct = <Fields extends Schema.Struct.Fields>(fields: Fields) =>
  Schema.Struct(fields).annotations({ parseOptions: exact });

/** Exact private send shape that preserves instance-fence precedence. */
const RawRouterSendRequest = closedStruct({
  expectedRouterInstanceId: RouterInstanceId,
  mode: Schema.Literal("initial", "retry"),
  signedMessage: Schema.Unknown,
}).annotations({ identifier: "RawRouterSendRequest" });

/** Private send shape before the identity-owned artifact is decoded. */
type RawRouterSendRequest = typeof RawRouterSendRequest.Type;

const sendRequest = closedStruct({
  expectedRouterInstanceId: RouterInstanceId,
  mode: Schema.Literal("initial", "retry"),
  signedMessage: SignedMessage.live,
}).annotations({ identifier: "RouterSendRequest" });

/** Exact Schema for one send request. */
export const RouterSendRequest: Schema.Schema<
  RouterSendRequest,
  typeof sendRequest.Encoded
> = sendRequest;

/**
 * The observed Router instance fences a send across restarts. A retry retains
 * the original SignedMessage bytes and never appends another feed entry.
 * @useDeclaredType
 */
export type RouterSendRequest = typeof sendRequest.Type;

const accepted = closedStruct({
  kind: Schema.Literal("accepted"),
  routerInstanceId: RouterInstanceId,
  signedMessageDigest: SignedMessageDigest,
});

const routerRestarted = closedStruct({
  kind: Schema.Literal("router_restarted"),
  routerInstanceId: RouterInstanceId,
});

const messageInvalid = closedStruct({
  kind: Schema.Literal("message_invalid"),
});

const idempotencyConflict = closedStruct({
  kind: Schema.Literal("idempotency_conflict"),
});

const retryIdentityUnknown = closedStruct({
  kind: Schema.Literal("retry_identity_unknown"),
});

const sendResult = Schema.Union(
  accepted,
  routerRestarted,
  messageInvalid,
  idempotencyConflict,
  retryIdentityUnknown,
).annotations({ identifier: "RouterSendResult" });

/** Exact Schema for every closed send outcome. */
export const RouterSendResult: Schema.Schema<
  RouterSendResult,
  typeof sendResult.Encoded
> = sendResult;

/**
 * `accepted` permits checking equality with the retained message, but proves
 * no delivery or durability. `router_restarted` fences sends to an old instance.
 * A retained identity conflicts with `initial`. A `retry` conflicts only when
 * its bytes differ and reports `retry_identity_unknown` after eviction.
 * @useDeclaredType
 */
export type RouterSendResult = typeof sendResult.Type;

const pollRequest = closedStruct({
  pollCursor: Schema.optionalWith(PollCursor, { exact: true }),
}).annotations({ identifier: "RouterPollRequest" });

/** Exact Schema for one endpoint-wide poll request. */
export const RouterPollRequest: Schema.Schema<
  RouterPollRequest,
  typeof pollRequest.Encoded
> = pollRequest;

/**
 * Omitting the cursor starts at the current tail without replaying retained messages.
 * @useDeclaredType
 */
export type RouterPollRequest = typeof pollRequest.Type;

const batch = closedStruct({
  kind: Schema.Literal("batch"),
  routerInstanceId: RouterInstanceId,
  signedMessages: Schema.Array(SignedMessage.live),
  pollCursor: PollCursor,
});

const feedGap = closedStruct({
  kind: Schema.Literal("feed_gap"),
  routerInstanceId: RouterInstanceId,
});

const cursorInvalid = closedStruct({
  kind: Schema.Literal("cursor_invalid"),
});

const pollResult = Schema.Union(batch, feedGap, cursorInvalid).annotations({
  identifier: "RouterPollResult",
});

/** Exact Schema for every closed poll outcome. */
export const RouterPollResult: Schema.Schema<
  RouterPollResult,
  typeof pollResult.Encoded
> = pollResult;

/**
 * A batch's messages remain unverified until the endpoint checks them all.
 * Advance the cursor after verifying and durably accepting every message.
 * `feed_gap` returns no partial batch and `cursor_invalid` reveals no Router
 * instance.
 * @useDeclaredType
 */
export type RouterPollResult = typeof pollResult.Type;

/* eslint-enable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- restore the shared project rules */

/** The Router connection could not be established or used. */
export class RouterConnectionError extends Data.TaggedError(
  "RouterConnectionError",
) {}

/** The configured complete Router call deadline expired. */
export class RouterRequestTimeoutError extends Data.TaggedError(
  "RouterRequestTimeoutError",
) {}

/** A Router response did not match the selected operation contract. */
export class RouterInvalidResponseError extends Data.TaggedError(
  "RouterInvalidResponseError",
) {}

/**
 * Closed failures represented by Router HTTP error envelopes.
 * @inline
 */
type RouterHttpEnvelopeError =
  | MalformedRequestError
  | AuthenticationFailedError
  | RouteNotFoundError
  | MethodNotAllowedError
  | VersionMismatchError
  | PayloadTooLargeError
  | UnsupportedMediaTypeError
  | OverloadedError
  | UnavailableError
  | InternalServerError;

/**
 * Request failures are distinct from the closed send and poll result values.
 * @inline
 */
export type RouterClientError =
  | RouterHttpEnvelopeError
  | RouterConnectionError
  | RouterRequestTimeoutError
  | RouterInvalidResponseError
  | AgentSigningError;

interface RouterHttpErrorRepresentation {
  readonly status: number;
  readonly body: string;
  readonly make: () => RouterHttpEnvelopeError;
}

const routerHttpErrors = Object.freeze({
  MalformedRequestError: {
    status: 400,
    body: '{"error":"malformed"}',
    make: () => new MalformedRequestError(),
  },
  AuthenticationFailedError: {
    status: 401,
    body: '{"error":"authentication_failed"}',
    make: () => new AuthenticationFailedError(),
  },
  RouteNotFoundError: {
    status: 404,
    body: '{"error":"not_found"}',
    make: () => new RouteNotFoundError(),
  },
  MethodNotAllowedError: {
    status: 405,
    body: '{"error":"method_not_allowed"}',
    make: () => new MethodNotAllowedError(),
  },
  VersionMismatchError: {
    status: 412,
    body: JSON.stringify({
      error: "version_mismatch",
      supported: LIVE_WIRE_VERSIONS,
    }),
    make: () => new VersionMismatchError(),
  },
  PayloadTooLargeError: {
    status: 413,
    body: '{"error":"payload_too_large"}',
    make: () => new PayloadTooLargeError(),
  },
  UnsupportedMediaTypeError: {
    status: 415,
    body: '{"error":"unsupported_media_type"}',
    make: () => new UnsupportedMediaTypeError(),
  },
  OverloadedError: {
    status: 429,
    body: '{"error":"overloaded"}',
    make: () => new OverloadedError(),
  },
  UnavailableError: {
    status: 503,
    body: '{"error":"unavailable"}',
    make: () => new UnavailableError(),
  },
  InternalServerError: {
    status: 500,
    body: '{"error":"internal"}',
    make: () => new InternalServerError(),
  },
} satisfies Readonly<
  Record<RouterHttpEnvelopeError["_tag"], RouterHttpErrorRepresentation>
>);

const isRouterHttpErrorTag = (
  tag: string,
): tag is keyof typeof routerHttpErrors => Object.hasOwn(routerHttpErrors, tag);

/**
 * Finds the exact status and body for one recognized Router failure tag.
 *
 * @param tag Candidate typed failure tag.
 * @returns Its exact envelope or `undefined` when the tag is not recognized.
 */
const routerHttpErrorEnvelope = (
  tag: string,
): Readonly<{ status: number; body: string }> | undefined =>
  isRouterHttpErrorTag(tag) ? routerHttpErrors[tag] : undefined;

/** Whether text is the JCS encoding of encoded. */
const isCanonicalRouterJson = (text: string, encoded: unknown): boolean => {
  const canonical = canonicalize(encoded);
  return canonical !== undefined && text === canonical;
};

/**
 * Decodes one exact status and body pair into its typed Router failure.
 *
 * @param status Candidate HTTP status.
 * @param body Candidate exact body text.
 * @returns The matching typed failure, when the pair is declared.
 */
const routerHttpErrorFromResponse = (
  status: number,
  body: string,
): Option.Option<RouterHttpEnvelopeError> => {
  if (status === 412) {
    return Schema.decodeUnknownOption(
      Schema.parseJson(
        closedStruct({
          error: Schema.Literal("version_mismatch"),
          supported: VersionMismatchError.supportedSchema,
        }),
      ),
    )(body).pipe(
      Option.filter((value) => isCanonicalRouterJson(body, value)),
      Option.map(({ supported }) => new VersionMismatchError({ supported })),
    );
  }
  const representation = Object.values(routerHttpErrors).find(
    (candidate) => candidate.status === status && candidate.body === body,
  );
  return representation === undefined
    ? Option.none()
    : Option.some(representation.make());
};

/** A Router enclosing representation cannot be measured safely. */
class RouterRepresentationLengthError extends Data.TaggedError(
  "RouterRepresentationLengthError",
) {}

/** Exact maxima for the Router-owned enclosing representations. */
interface RouterRepresentationLimits {
  readonly sendRequestBodyBytes: number;
  readonly pollRequestBodyBytes: number;
  readonly oneMessageBatchBytes: number;
}

const utf8Encoder = new TextEncoder();

const signedMessagePlaceholder = "__signed_message__";

const signedMessagePlaceholderByteLength = utf8Encoder.encode(
  JSON.stringify(signedMessagePlaceholder),
).byteLength;

const maximumAgentId = "agt_AAAAAAAAAAAAAAAAAAAAAA";

const maximumRouterInstanceId = "rti_AAAAAAAAAAAAAAAAAAAAAA";

/**
 * Measures the complete prefixed Compact JWE for the bound caller, Router
 * instance and order width.
 */
const pollCursorEncodedLength = (input: {
  readonly agentId: AgentId;
  readonly routerInstanceId: RouterInstanceId;
  readonly lastScannedOrder: bigint;
}): number => {
  const plaintext = canonicalize({
    agentId: input.agentId,
    routerInstanceId: input.routerInstanceId,
    lastScannedOrder: input.lastScannedOrder.toString(10),
  });
  if (plaintext === undefined) {
    return Number.POSITIVE_INFINITY;
  }
  const plaintextLength = utf8Encoder.encode(plaintext).byteLength;
  return (
    pollCursorPrefix.length +
    encodedPollCursorProtectedHeader.length +
    1 +
    1 +
    16 +
    1 +
    base64UrlLength(plaintextLength) +
    1 +
    22
  );
};

/** Maximum ASCII characters in a valid PollCursor. */
const maximumPollCursorLength = pollCursorEncodedLength({
  agentId: Schema.decodeUnknownSync(AgentId)(maximumAgentId),
  routerInstanceId: Schema.decodeUnknownSync(RouterInstanceId)(
    maximumRouterInstanceId,
  ),
  lastScannedOrder: (1n << 128n) - 1n,
});

function base64UrlLength(byteLength: number): number {
  return Math.ceil((byteLength * 8) / 6);
}

const canonicalByteLength = (
  value: unknown,
): Effect.Effect<number, RouterRepresentationLengthError> => {
  const encoded = canonicalize(value);
  if (encoded === undefined) {
    return Effect.fail(new RouterRepresentationLengthError());
  }
  return Effect.succeed(utf8Encoder.encode(encoded).byteLength);
};

const supportedLength = (value: number): boolean =>
  Number.isSafeInteger(value) && value >= 0;

const checkedAdd = (
  left: number,
  right: number,
): Effect.Effect<number, RouterRepresentationLengthError> => {
  const result = left + right;
  if (
    !supportedLength(left) ||
    !supportedLength(right) ||
    !supportedLength(result)
  ) {
    return Effect.fail(new RouterRepresentationLengthError());
  }
  return Effect.succeed(result);
};

const maximumSendBody = {
  callerAgentId: maximumAgentId,
  request: {
    expectedRouterInstanceId: maximumRouterInstanceId,
    mode: "initial",
    signedMessage: signedMessagePlaceholder,
  },
};

const maximumPollBody = {
  callerAgentId: maximumAgentId,
  request: {
    pollCursor: "A".repeat(maximumPollCursorLength),
  },
};

const maximumOneMessageBatch = {
  kind: "batch",
  pollCursor: "A".repeat(maximumPollCursorLength),
  routerInstanceId: maximumRouterInstanceId,
  signedMessages: [signedMessagePlaceholder],
};

/**
 * Calculates exact enclosing limits while treating SignedMessage as an
 * identity-owned byte sequence.
 *
 * @param maximumSignedMessageByteLength Identity-owned complete message limit.
 * @returns Exact maximum Router request and one-message result byte lengths.
 */
const calculateRouterRepresentationLimits = (
  maximumSignedMessageByteLength: number,
): Effect.Effect<RouterRepresentationLimits, RouterRepresentationLengthError> =>
  Effect.gen(function* () {
    const sendTemplateBytes = yield* canonicalByteLength(maximumSendBody);
    const pollRequestBodyBytes = yield* canonicalByteLength(maximumPollBody);
    const batchTemplateBytes = yield* canonicalByteLength(
      maximumOneMessageBatch,
    );
    const sendFixedBytes =
      sendTemplateBytes - signedMessagePlaceholderByteLength;
    const batchFixedBytes =
      batchTemplateBytes - signedMessagePlaceholderByteLength;
    return Object.freeze({
      sendRequestBodyBytes: yield* checkedAdd(
        sendFixedBytes,
        maximumSignedMessageByteLength,
      ),
      pollRequestBodyBytes,
      oneMessageBatchBytes: yield* checkedAdd(
        batchFixedBytes,
        maximumSignedMessageByteLength,
      ),
    });
  }).pipe(Effect.withSpan("calculateRouterRepresentationLimits"));

/** Exact limits derived from the current closed Router representations. */
const routerRepresentationLimits = Effect.runSync(
  calculateRouterRepresentationLimits(SignedMessage.maximumEncodedByteLength),
);

interface CursorSegments {
  readonly encodedHeader: string;
  readonly encryptedKey: string;
  readonly iv: string;
  readonly ciphertext: string;
  readonly tag: string;
}

const cursorSegmentsPattern = new RegExp(
  `^(?=[\\s\\S]{0,${maximumPollCursorLength}}$)${pollCursorPrefix}([^.]*?)\\.([^.]*?)\\.([^.]*?)\\.([^.]*?)\\.([^.]*?)$`,
);

function hasCanonicalCursorShape(value: string): boolean {
  const segments = extractCursorSegments(value);
  if (segments === undefined || segments.encryptedKey !== "") {
    return false;
  }
  if (
    segments.encodedHeader !== encodedPollCursorProtectedHeader ||
    !hasCanonicalBase64UrlLength(segments.iv, 12)
  ) {
    return false;
  }
  return (
    hasCanonicalBase64UrlLength(segments.tag, 16) &&
    hasCanonicalCiphertext(segments.ciphertext)
  );
}

function extractCursorSegments(value: string): CursorSegments | undefined {
  const match = cursorSegmentsPattern.exec(value);
  let result: CursorSegments | undefined;
  if (match !== null) {
    result = {
      encodedHeader: segmentAt(match, 1),
      encryptedKey: segmentAt(match, 2),
      iv: segmentAt(match, 3),
      ciphertext: segmentAt(match, 4),
      tag: segmentAt(match, 5),
    };
  }
  return result;
}

function segmentAt(segments: readonly string[], index: number): string {
  return segments[index] ?? "";
}

function hasCanonicalCiphertext(ciphertext: string): boolean {
  return Either.match(Encoding.decodeBase64Url(ciphertext), {
    onLeft: () => false,
    onRight: (decoded) =>
      decoded.byteLength > 0 &&
      Encoding.encodeBase64Url(decoded) === ciphertext,
  });
}

/** Router processes and clients share these exact routes, representation limits, request and result schemas, cursor encodings, and HTTP error projection. */
export const routerContract = Object.freeze({
  calculateRouterRepresentationLimits,
  encodedPollCursorProtectedHeader,
  maximumPollCursorLength,
  PollCursor,
  pollCursorEncodedLength,
  pollCursorPrefix,
  pollCursorProtectedHeader,
  RawRouterSendRequest,
  routerHealthPath,
  routerHttpErrorEnvelope,
  RouterInstanceId,
  routerInstanceIdByteLength: INSTANCE_BYTE_LENGTH,
  routerJsonContentType,
  routerPollPath,
  RouterPollRequest,
  RouterPollResult,
  routerPollRoutePattern,
  routerRepresentationLimits,
  routerSendPath,
  RouterSendRequest,
  RouterSendResult,
  routerSendRoutePattern,
  SignedMessageDigest,
});

/** @inline */
interface RouterClientService {
  readonly send: (input: {
    readonly request: RouterSendRequest;
    readonly callerAgentId: AgentId;
    readonly signingAuthority: AgentSigningAuthority;
  }) => Effect.Effect<RouterSendResult, RouterClientError>;
  readonly poll: (input: {
    readonly request: RouterPollRequest;
    readonly callerAgentId: AgentId;
    readonly signingAuthority: AgentSigningAuthority;
  }) => Effect.Effect<RouterPollResult, RouterClientError>;
}

interface RouterClientSettings {
  readonly origin: URL;
  readonly sendTimeout: Duration.Duration;
  readonly pollTimeout: Duration.Duration;
}

/**
 * Authenticates each call with the agent's signing authority. Router retains
 * accepted messages until eviction or restart. Endpoints verify delivery and
 * establish durable history. See the
 * [Router contract](https://github.com/social-harness/moltzap/blob/main/docs/reference/spec/communication/protocol.md#router).
 */
export class Router extends Context.Tag("@moltzap/social-harness/Router")<
  Router,
  RouterClientService
>() {
  /**
   * `initial` appends under the observed Router instance. A `retry` checks exact
   * retained bytes without appending. The caller must be the sender. Acceptance
   * proves neither delivery nor durable storage. After uncertain transport
   * outcomes, retry the same message instead of signing another.
   */
  static readonly send: (input: {
    readonly request: RouterSendRequest;
    readonly callerAgentId: AgentId;
    readonly signingAuthority: AgentSigningAuthority;
  }) => Effect.Effect<RouterSendResult, RouterClientError, Router> =
    Effect.serviceFunctionEffect(Router, (service) => service.send);

  /**
   * Omit the cursor for an immediate empty tail anchor. Continue to poll
   * addressed, parsed but unverified messages in Router order. Verify and durably
   * accept the whole batch before advancing its cursor.
   *
   * `feed_gap` requires history recovery. After `cursor_invalid`, obtain a new
   * omitted-cursor anchor because the error supplies no replacement instance.
   */
  static readonly poll: (input: {
    readonly request: RouterPollRequest;
    readonly callerAgentId: AgentId;
    readonly signingAuthority: AgentSigningAuthority;
  }) => Effect.Effect<RouterPollResult, RouterClientError, Router> =
    Effect.serviceFunctionEffect(Router, (service) => service.poll);

  /**
   * Snapshots the origin and durations when constructed. Each timeout covers
   * the complete authenticated call, including response decoding. The caller
   * supplies the HTTP transport and owns retry and recovery policy.
   */
  static readonly layer = (input: {
    readonly origin: URL;
    readonly sendTimeout: Duration.Duration;
    readonly pollTimeout: Duration.Duration;
  }): Layer.Layer<Router, never, HttpClient.HttpClient> =>
    Layer.effect(Router, makeRouterClient(input));
}

function makeRouterClient(input: {
  readonly origin: URL;
  readonly sendTimeout: Duration.Duration;
  readonly pollTimeout: Duration.Duration;
}): Effect.Effect<RouterClientService, never, HttpClient.HttpClient> {
  const settings = Object.freeze({
    origin: new URL(input.origin.href),
    sendTimeout: snapshotDuration(input.sendTimeout),
    pollTimeout: snapshotDuration(input.pollTimeout),
  });
  return HttpClient.HttpClient.pipe(
    Effect.map((client) => makeClientService(client, settings)),
    Effect.withSpan("makeRouterClient"),
  );
}

const invalidResponse = () => new RouterInvalidResponseError();

const utf8Decoder = new TextDecoder("utf-8", {
  fatal: true,
  ignoreBOM: true,
});

const decodeUtf8 = (
  bytes: Uint8Array,
): Effect.Effect<string, RouterInvalidResponseError> =>
  Effect.try({
    try: () => utf8Decoder.decode(bytes),
    catch: invalidResponse,
  });

/**
 * Gateway statuses an intermediary answers with while the Router behind it is
 * down or restarting.
 */
const gatewayStatuses: ReadonlySet<number> = new Set([502, 503, 504]);

/**
 * Whether a response is a gateway's page rather than a Router answer: a
 * gateway status without the Router's JSON content type. The Router itself
 * could not be reached, so this is a connection failure, not an invalid
 * Router response.
 */
const isGatewayPage = (
  response: HttpClientResponse.HttpClientResponse,
): boolean =>
  gatewayStatuses.has(response.status) &&
  response.headers["content-type"] !== routerJsonContentType;

const decodeResponse = <A, I>(
  schema: Schema.Schema<A, I>,
  response: HttpClientResponse.HttpClientResponse,
): Effect.Effect<
  A,
  RouterHttpEnvelopeError | RouterConnectionError | RouterInvalidResponseError
> =>
  Effect.gen(function* () {
    if (isGatewayPage(response)) {
      return yield* Effect.fail(new RouterConnectionError());
    }
    const bytes = yield* response.arrayBuffer.pipe(
      Effect.catchTag("ResponseError", () =>
        Effect.fail(new RouterConnectionError()),
      ),
      Effect.map((buffer) => new Uint8Array(buffer)),
    );
    if (response.headers["content-type"] !== routerJsonContentType) {
      return yield* Effect.fail(invalidResponse());
    }
    if (response.status === 200) {
      return yield* decodeCanonical(schema, bytes).pipe(
        Effect.mapError(invalidResponse),
      );
    }
    const text = yield* decodeUtf8(bytes);
    const recognized = routerHttpErrorFromResponse(response.status, text);
    if (Option.isNone(recognized)) {
      return yield* Effect.fail(invalidResponse());
    }
    return yield* Effect.fail(recognized.value);
  });

const execute = <Request, RequestEncoded, A, I>(input: {
  readonly client: HttpClient.HttpClient;
  readonly origin: URL;
  readonly path: string;
  readonly request: Request;
  readonly requestSchema: Schema.Schema<Request, RequestEncoded>;
  readonly schema: Schema.Schema<A, I>;
  readonly callerAgentId: AgentId;
  readonly signingAuthority: AgentSigningAuthority;
  readonly timeout: Duration.Duration;
}): Effect.Effect<A, RouterClientError> =>
  Effect.gen(function* () {
    const request = HttpClientRequest.post(new URL(input.path, input.origin));
    const encodedRequest = yield* Schema.encode(input.requestSchema)(
      input.request,
    ).pipe(
      Effect.catchTag("ParseError", () => Effect.fail(new AgentSigningError())),
    );
    const signedRequest = yield* signAgentRequest({
      httpRequest: request,
      callerAgentId: input.callerAgentId,
      encodedRequest,
      signingAuthority: input.signingAuthority,
    });
    const response = yield* input.client.execute(signedRequest).pipe(
      Effect.catchTags({
        RequestError: () => Effect.fail(new RouterConnectionError()),
        ResponseError: () => Effect.fail(new RouterConnectionError()),
      }),
    );
    return yield* decodeResponse(input.schema, response);
  }).pipe(
    Effect.timeoutFail({
      duration: input.timeout,
      onTimeout: () => new RouterRequestTimeoutError(),
    }),
  );

function snapshotDuration(duration: Duration.Duration): Duration.Duration {
  return Duration.match(duration, {
    onMillis: Duration.millis,
    onNanos: Duration.nanos,
  });
}

function makeClientService(
  client: HttpClient.HttpClient,
  settings: RouterClientSettings,
): RouterClientService {
  return {
    send: (call) =>
      execute({
        client,
        origin: settings.origin,
        path: routerSendPath,
        request: call.request,
        requestSchema: RouterSendRequest,
        schema: RouterSendResult,
        callerAgentId: call.callerAgentId,
        signingAuthority: call.signingAuthority,
        timeout: settings.sendTimeout,
      }),
    poll: (call) =>
      execute({
        client,
        origin: settings.origin,
        path: routerPollPath,
        request: call.request,
        requestSchema: RouterPollRequest,
        schema: RouterPollResult,
        callerAgentId: call.callerAgentId,
        signingAuthority: call.signingAuthority,
        timeout: settings.pollTimeout,
      }),
  };
}
