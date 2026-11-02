/** @file Authenticated Registry HTTP routes, bounded framing, and exact envelopes. */

import type { AgentSigningAuthority } from "@moltzap/social-harness";
import {
  HttpRouter,
  HttpServerRequest,
  HttpServerResponse,
} from "@effect/platform";
import { NodeHttpServerRequest } from "@effect/platform-node";
import {
  httpErrors,
  registryContract,
  wireVersions,
} from "@moltzap/social-harness/internal";
import { Cause, Effect, Option } from "effect";
import type { RegistryStorage } from "./storage.js";
import { verifyBootstrapRegistration } from "./admission.js";

/* eslint-disable @typescript-eslint/naming-convention -- Closed HTTP error constructors retain their class names when bound from the service capability. */
const {
  InternalServerError,
  MalformedRequestError,
  MethodNotAllowedError,
  OverloadedError,
  PayloadTooLargeError,
  RouteNotFoundError,
  UnsupportedMediaTypeError,
  VersionMismatchError,
} = httpErrors;
/* eslint-enable @typescript-eslint/naming-convention -- Restore variable naming after binding error constructors. */

type RegistryHttpAppInput = Readonly<{
  registrySigningAuthority: AgentSigningAuthority;
  storage: RegistryStorage;
  admissionCredential: Parameters<
    typeof verifyBootstrapRegistration
  >[0]["admissionCredential"];
  nonceCapacity: number;
  requestSemaphore: Effect.Semaphore;
}>;

/** Rejects requests immediately when the shared request semaphore is full. */
export function makeRegistryHttpApp(input: RegistryHttpAppInput) {
  return HttpRouter.empty.pipe(
    HttpRouter.post(registryContract.REGISTER_ROUTE, registerHandler(input)),
    HttpRouter.post(registryContract.LOOKUP_ROUTE, lookupHandler(input)),
    healthRoute(input.storage),
    fallbackRoute(),
  );
}

const JSON_CONTENT_TYPE = "application/json";
const SIGNATURE_HEADERS = [
  "content-digest",
  "signature-input",
  "signature",
  "authorization",
] as const;
const SINGLE_VALUE_HEADERS = [
  "host",
  "content-type",
  "content-encoding",
  "content-length",
  "transfer-encoding",
  "sharness-version",
  ...SIGNATURE_HEADERS,
] as const;

function healthRoute(storage: RegistryStorage) {
  return HttpRouter.get(
    registryContract.HEALTH_PATH,
    Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest;
      if (request.url !== registryContract.HEALTH_PATH) {
        return errorResponse(new RouteNotFoundError());
      }
      return yield* storage.checkReadiness().pipe(
        Effect.match({
          onFailure: () => HttpServerResponse.empty({ status: 503 }),
          onSuccess: () => HttpServerResponse.empty({ status: 204 }),
        }),
        Effect.catchAllCause(() =>
          Effect.succeed(HttpServerResponse.empty({ status: 503 })),
        ),
      );
    }),
  );
}

function fallbackRoute() {
  return HttpRouter.all(
    "*",
    Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest;
      if (request.url.includes("?")) {
        return errorResponse(new RouteNotFoundError());
      }
      const path = request.url.split("?", 1)[0] ?? "";
      return registryContract.KNOWN_PATHS.has(path)
        ? errorResponse(new MethodNotAllowedError())
        : errorResponse(new RouteNotFoundError());
    }),
  );
}

function registerHandler(input: RegistryHttpAppInput) {
  return recoverHttpResponse(
    Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest;
      if (request.url !== registryContract.REGISTER_PATH) {
        return yield* new RouteNotFoundError();
      }
      yield* validateRequestFraming({
        request,
        maximumBytes: registryContract.REGISTER_BODY_CAP,
        profile: "registration",
      });
      return yield* executeWithPermit(
        input.requestSemaphore,
        Effect.gen(function* () {
          const bodyBytes = yield* readBoundedBody(
            request,
            registryContract.REGISTER_BODY_CAP,
          );
          const registration = yield* verifyBootstrapRegistration({
            httpRequest: request,
            bodyBytes,
            admissionCredential: input.admissionCredential,
            nonceCapacity: input.nonceCapacity,
            storage: input.storage,
          });
          const requestBytes =
            yield* registryContract.registryRegisterCodec.request
              .encode(registration)
              .pipe(
                // eslint-disable-next-line agent-code-guard/no-effect-error-coalescing -- Business request serialization is an internal boundary with no client-actionable detail.
                Effect.mapError(() => new InternalServerError()),
              );
          const result = yield* input.storage.register({
            request: registration,
            requestBytes,
            registrySigningAuthority: input.registrySigningAuthority,
          });
          return yield* resultResponse(
            registryContract.registryRegisterCodec.response.encode(result),
          );
        }),
      );
    }),
  );
}

function lookupHandler(input: RegistryHttpAppInput) {
  return recoverHttpResponse(
    Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest;
      if (request.url !== registryContract.LOOKUP_PATH) {
        return yield* new RouteNotFoundError();
      }
      yield* validateRequestFraming({
        request,
        maximumBytes: registryContract.LOOKUP_BODY_CAP,
        profile: "public-read",
      });
      return yield* executeWithPermit(
        input.requestSemaphore,
        Effect.gen(function* () {
          const bodyBytes = yield* readBoundedBody(
            request,
            registryContract.LOOKUP_BODY_CAP,
          );
          const version = request.headers["sharness-version"];
          if (version === undefined || !(yield* wireVersions.live(version))) {
            return yield* new VersionMismatchError();
          }
          const decoded = yield* registryContract.registryLookupCodec.request
            .decode(bodyBytes)
            .pipe(
              // eslint-disable-next-line agent-code-guard/no-effect-error-coalescing -- Every canonical body or request-schema failure has the same public malformed-request meaning.
              Effect.mapError(() => new MalformedRequestError()),
            );
          const result = yield* input.storage.lookup(decoded);
          return yield* resultResponse(
            registryContract.registryLookupCodec.response.encode(result),
          );
        }),
      );
    }),
  );
}

type RequestProfile = "registration" | "public-read";

function recoverHttpResponse<R>(
  effect: Effect.Effect<
    HttpServerResponse.HttpServerResponse,
    HttpEnvelopeError,
    R
  >,
) {
  return effect.pipe(
    Effect.catchAll((error) => Effect.succeed(errorResponse(error))),
    Effect.catchAllCause((cause) =>
      Effect.logError("Registry request failed", Cause.squash(cause)).pipe(
        Effect.as(errorResponse(new InternalServerError())),
      ),
    ),
  );
}

function executeWithPermit<A, E, R>(
  semaphore: Effect.Semaphore,
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, E | InstanceType<typeof httpErrors.OverloadedError>, R> {
  return semaphore
    .withPermitsIfAvailable(1)(effect)
    .pipe(
      Effect.flatMap(
        Option.match({
          onNone: () => Effect.fail(new OverloadedError()),
          onSome: Effect.succeed,
        }),
      ),
    );
}

function validateRequestFraming(input: {
  readonly request: HttpServerRequest.HttpServerRequest;
  readonly maximumBytes: number;
  readonly profile: RequestProfile;
}): Effect.Effect<
  void,
  | InstanceType<typeof httpErrors.MalformedRequestError>
  | InstanceType<typeof httpErrors.UnsupportedMediaTypeError>
  | InstanceType<typeof httpErrors.PayloadTooLargeError>
> {
  if (requestFramingIsMalformed(input)) {
    return rejectBeforeBodyRead(input.request, new MalformedRequestError());
  }
  if (mediaTypeIsUnsupported(input.request)) {
    return rejectBeforeBodyRead(input.request, new UnsupportedMediaTypeError());
  }
  return validateDeclaredLength(input.request, input.maximumBytes);
}

function requestFramingIsMalformed(input: {
  readonly request: HttpServerRequest.HttpServerRequest;
  readonly profile: RequestProfile;
}): boolean {
  if (
    SINGLE_VALUE_HEADERS.some((name) => rawHeaderCount(input.request, name) > 1)
  ) {
    return true;
  }
  if (
    input.request.headers["content-length"] !== undefined &&
    input.request.headers["transfer-encoding"] !== undefined
  ) {
    return true;
  }
  return (
    input.profile === "public-read" &&
    SIGNATURE_HEADERS.some((name) => rawHeaderCount(input.request, name) !== 0)
  );
}

function rawHeaderCount(
  request: HttpServerRequest.HttpServerRequest,
  name: string,
): number {
  const incoming = NodeHttpServerRequest.toIncomingMessage(request);
  let count = 0;
  for (let index = 0; index < incoming.rawHeaders.length; index += 2) {
    if (incoming.rawHeaders[index]?.toLowerCase() === name) {
      count += 1;
    }
  }
  return count;
}

function mediaTypeIsUnsupported(
  request: HttpServerRequest.HttpServerRequest,
): boolean {
  return (
    request.headers["content-type"] !== JSON_CONTENT_TYPE ||
    request.headers["content-encoding"] !== undefined
  );
}

function validateDeclaredLength(
  request: HttpServerRequest.HttpServerRequest,
  maximumBytes: number,
): Effect.Effect<
  void,
  | InstanceType<typeof httpErrors.MalformedRequestError>
  | InstanceType<typeof httpErrors.PayloadTooLargeError>
> {
  const headerIsPresent = request.headers["content-length"] !== undefined;
  const declaredLength = canonicalContentLength(request);
  if (headerIsPresent && declaredLength === undefined) {
    return rejectBeforeBodyRead(request, new MalformedRequestError());
  }
  if (declaredLength !== undefined && declaredLength > maximumBytes) {
    return rejectBeforeBodyRead(request, new PayloadTooLargeError());
  }
  return Effect.void;
}

function canonicalContentLength(
  request: HttpServerRequest.HttpServerRequest,
): number | undefined {
  const value = request.headers["content-length"];
  if (value === undefined || !/^(?:0|[1-9]\d*)$/.test(value)) {
    return undefined;
  }
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) ? parsed : undefined;
}

function rejectBeforeBodyRead<E>(
  request: HttpServerRequest.HttpServerRequest,
  error: E,
): Effect.Effect<never, E> {
  return Effect.sync(() => {
    NodeHttpServerRequest.toIncomingMessage(request).resume();
  }).pipe(Effect.zipRight(Effect.fail(error)));
}

interface BodyReader {
  readonly cleanup: () => void;
  readonly onData: (chunk: Uint8Array) => void;
  readonly onEnd: () => void;
  readonly onError: () => void;
}

function readBoundedBody(
  request: HttpServerRequest.HttpServerRequest,
  maximumBytes: number,
): Effect.Effect<
  Uint8Array,
  | InstanceType<typeof httpErrors.PayloadTooLargeError>
  | InstanceType<typeof httpErrors.MalformedRequestError>
> {
  return Effect.async((resume) => {
    const incoming = NodeHttpServerRequest.toIncomingMessage(request);
    const body = new Uint8Array(maximumBytes);
    let length = 0;
    const reader: BodyReader = {
      cleanup: () => {
        incoming.off("data", reader.onData);
        incoming.off("end", reader.onEnd);
        incoming.off("error", reader.onError);
      },
      onData: (chunk) => {
        if (length + chunk.byteLength > maximumBytes) {
          reader.cleanup();
          incoming.resume();
          resume(Effect.fail(new PayloadTooLargeError()));
          return;
        }
        body.set(chunk, length);
        length += chunk.byteLength;
      },
      onEnd: () => {
        reader.cleanup();
        resume(Effect.succeed(body.slice(0, length)));
      },
      onError: () => {
        reader.cleanup();
        resume(Effect.fail(new MalformedRequestError()));
      },
    };
    incoming.on("data", reader.onData);
    incoming.once("end", reader.onEnd);
    incoming.once("error", reader.onError);
    return Effect.sync(() => {
      reader.cleanup();
      incoming.resume();
    });
  });
}

function resultResponse<E>(
  encoded: Effect.Effect<Uint8Array, E>,
): Effect.Effect<
  HttpServerResponse.HttpServerResponse,
  InstanceType<typeof httpErrors.InternalServerError>
> {
  return encoded.pipe(
    Effect.map((bytes) => jsonResponse(bytes, 200)),
    // eslint-disable-next-line agent-code-guard/no-effect-error-coalescing -- Response serialization is an internal closed boundary with no client-actionable detail.
    Effect.mapError(() => new InternalServerError()),
  );
}

const mappings = Object.freeze({
  MalformedRequestError: { status: 400, code: "malformed" },
  AuthenticationFailedError: { status: 401, code: "authentication_failed" },
  RouteNotFoundError: { status: 404, code: "not_found" },
  MethodNotAllowedError: { status: 405, code: "method_not_allowed" },
  VersionMismatchError: { status: 412, code: "version_mismatch" },
  PayloadTooLargeError: { status: 413, code: "payload_too_large" },
  UnsupportedMediaTypeError: {
    status: 415,
    code: "unsupported_media_type",
  },
  OverloadedError: { status: 429, code: "overloaded" },
  UnavailableError: { status: 503, code: "unavailable" },
  InternalServerError: { status: 500, code: "internal" },
} as const);

function errorResponse(
  error: HttpEnvelopeError,
): HttpServerResponse.HttpServerResponse {
  const envelope = mappings[error._tag];
  return jsonResponse(
    new TextEncoder().encode(
      JSON.stringify({
        error: envelope.code,
        ...(error._tag === "VersionMismatchError"
          ? { supported: error.supported }
          : {}),
      }),
    ),
    envelope.status,
  );
}

function jsonResponse(
  bodyBytes: Uint8Array,
  status: number,
): HttpServerResponse.HttpServerResponse {
  return HttpServerResponse.uint8Array(bodyBytes, {
    status,
    contentType: JSON_CONTENT_TYPE,
  });
}

/** Closed failures represented by the shared HTTP error envelope. */
type HttpEnvelopeError =
  | InstanceType<typeof httpErrors.MalformedRequestError>
  | InstanceType<typeof httpErrors.AuthenticationFailedError>
  | InstanceType<typeof httpErrors.RouteNotFoundError>
  | InstanceType<typeof httpErrors.MethodNotAllowedError>
  | InstanceType<typeof httpErrors.VersionMismatchError>
  | InstanceType<typeof httpErrors.PayloadTooLargeError>
  | InstanceType<typeof httpErrors.UnsupportedMediaTypeError>
  | InstanceType<typeof httpErrors.OverloadedError>
  | InstanceType<typeof httpErrors.UnavailableError>
  | InstanceType<typeof httpErrors.InternalServerError>;
