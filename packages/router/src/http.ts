/** @file Authenticated Router HTTP routes and exact envelope projection. */
import {
  HttpRouter,
  HttpServerRequest,
  HttpServerResponse,
} from "@effect/platform";
import { NodeHttpServerRequest } from "@effect/platform-node";
import {
  Authentication,
  type AuthenticationError,
  httpErrors,
  routerContract,
  type VerifiedAgentRequest,
} from "@moltzap/social-harness/internal";
import canonicalize from "canonicalize";
import { Cause, Effect, Option, Schema } from "effect";
import type { RouterFeed } from "./feed.js";
import type { RouterPoll } from "./poll.js";
import type { RouterSend } from "./send.js";

/* eslint-disable @typescript-eslint/naming-convention -- Closed HTTP error constructors retain their class names when bound from the service capability. */
const {
  AuthenticationFailedError,
  VersionMismatchError,
  UnavailableError,
  InternalServerError,
  MalformedRequestError,
  MethodNotAllowedError,
  OverloadedError,
  PayloadTooLargeError,
  RouteNotFoundError,
  UnsupportedMediaTypeError,
} = httpErrors;
/* eslint-enable @typescript-eslint/naming-convention -- Restore variable naming after binding error constructors. */

interface RouterHttpInput {
  readonly send: RouterSend;
  readonly poll: RouterPoll;
  readonly feed: RouterFeed;
  readonly requestSemaphore: Effect.Semaphore;
}

/** Rejects requests immediately when the shared request semaphore is full. */
export const makeRouterHttpApp = (input: RouterHttpInput) =>
  HttpRouter.empty.pipe(
    sendRoute(input),
    pollRoute(input),
    healthRoute(input),
    fallbackRoute(),
  );

/**
 * Returns an empty 204 while the feed can accept a fresh append, or 503
 * once its order range is exhausted.
 */
export const routerHealthResponse = (
  feed: RouterFeed,
): Effect.Effect<HttpServerResponse.HttpServerResponse> =>
  feed.freshAppendReady.pipe(
    Effect.map((ready) =>
      HttpServerResponse.empty({ status: ready ? 204 : 503 }),
    ),
  );

const rawPollRequest = Schema.Struct({
  pollCursor: Schema.optional(
    Schema.String.pipe(
      Schema.maxLength(routerContract.maximumPollCursorLength),
    ),
  ),
}).annotations({
  parseOptions: { exact: true, onExcessProperty: "error" },
});

const jsonText = (body: string, status: number) =>
  HttpServerResponse.text(body, {
    status,
    contentType: routerContract.routerJsonContentType,
  });

const errorResponse = (error: { readonly _tag: string }) => {
  const mapped = routerContract.routerHttpErrorEnvelope(error._tag);
  return mapped === undefined
    ? jsonText('{"error":"internal"}', 500)
    : jsonText(mapped.body, mapped.status);
};

const recoverUnexpectedFailure = <E, R>(
  effect: Effect.Effect<HttpServerResponse.HttpServerResponse, E, R>,
): Effect.Effect<HttpServerResponse.HttpServerResponse, never, R> =>
  effect.pipe(
    Effect.catchAllCause((cause) =>
      Effect.logError("Router route failed", Cause.squash(cause)).pipe(
        Effect.as(errorResponse(new InternalServerError())),
      ),
    ),
  );

const resultResponse = <A, I>(
  schema: Schema.Schema<A, I>,
  value: A,
): Effect.Effect<
  HttpServerResponse.HttpServerResponse,
  InstanceType<typeof httpErrors.InternalServerError>
> =>
  Schema.encode(schema)(value).pipe(
    Effect.catchTag("ParseError", () => Effect.fail(new InternalServerError())),
    Effect.flatMap((encoded) => {
      const body = canonicalize(encoded);
      return body === undefined
        ? Effect.fail(new InternalServerError())
        : Effect.succeed(jsonText(body, 200));
    }),
  );

const canonicalContentLength = (
  request: HttpServerRequest.HttpServerRequest,
): number | undefined => {
  const value = request.headers["content-length"];
  if (value === undefined || !/^(?:0|[1-9]\d*)$/.test(value)) {
    return undefined;
  }
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) ? parsed : undefined;
};

const rawHeaderCount = (
  request: HttpServerRequest.HttpServerRequest,
  name: string,
): number => {
  const incomingMessage = NodeHttpServerRequest.toIncomingMessage(request);
  let count = 0;
  for (let index = 0; index < incomingMessage.rawHeaders.length; index += 2) {
    if (incomingMessage.rawHeaders[index]?.toLowerCase() === name) {
      count += 1;
    }
  }
  return count;
};

const SINGLE_VALUE_HEADERS = [
  "host",
  "content-type",
  "content-encoding",
  "content-length",
  "transfer-encoding",
  "sharness-version",
  "content-digest",
  "signature-input",
  "signature",
] as const;

const framingIsMalformed = (
  request: HttpServerRequest.HttpServerRequest,
): boolean => {
  const duplicate = SINGLE_VALUE_HEADERS.some(
    (name) => rawHeaderCount(request, name) > 1,
  );
  const conflictingLength =
    request.headers["content-length"] !== undefined &&
    request.headers["transfer-encoding"] !== undefined;
  return duplicate || conflictingLength;
};

const mediaTypeIsUnsupported = (
  request: HttpServerRequest.HttpServerRequest,
): boolean =>
  request.headers["content-type"] !== routerContract.routerJsonContentType ||
  request.headers["content-encoding"] !== undefined;

const validateDeclaredLength = (
  request: HttpServerRequest.HttpServerRequest,
  maximumBytes: number,
): Effect.Effect<
  void,
  | InstanceType<typeof httpErrors.MalformedRequestError>
  | InstanceType<typeof httpErrors.PayloadTooLargeError>
> => {
  const headerIsPresent = request.headers["content-length"] !== undefined;
  const declaredLength = canonicalContentLength(request);
  if (headerIsPresent && declaredLength === undefined) {
    return Effect.fail(new MalformedRequestError());
  }
  if (declaredLength !== undefined && declaredLength > maximumBytes) {
    return Effect.fail(new PayloadTooLargeError());
  }
  return Effect.void;
};

const validateRequestFraming = (
  request: HttpServerRequest.HttpServerRequest,
  maximumBytes: number,
): Effect.Effect<
  void,
  | InstanceType<typeof httpErrors.PayloadTooLargeError>
  | InstanceType<typeof httpErrors.MalformedRequestError>
  | InstanceType<typeof httpErrors.UnsupportedMediaTypeError>
> => {
  if (framingIsMalformed(request)) {
    return Effect.fail(new MalformedRequestError());
  }
  if (mediaTypeIsUnsupported(request)) {
    return Effect.fail(new UnsupportedMediaTypeError());
  }
  return validateDeclaredLength(request, maximumBytes);
};

const readBoundedBody = (
  request: HttpServerRequest.HttpServerRequest,
  maximumBytes: number,
): Effect.Effect<
  Uint8Array,
  | InstanceType<typeof httpErrors.PayloadTooLargeError>
  | InstanceType<typeof httpErrors.MalformedRequestError>
> =>
  Effect.async((resume) => {
    const incoming = NodeHttpServerRequest.toIncomingMessage(request);
    const body = new Uint8Array(maximumBytes);
    let length = 0;
    function cleanup() {
      incoming.off("data", onData);
      incoming.off("end", onEnd);
      incoming.off("error", onError);
    }
    function onData(chunk: Uint8Array) {
      if (length + chunk.byteLength > maximumBytes) {
        cleanup();
        incoming.resume();
        resume(Effect.fail(new PayloadTooLargeError()));
        return;
      }
      body.set(chunk, length);
      length += chunk.byteLength;
    }
    function onEnd() {
      cleanup();
      resume(Effect.succeed(body.slice(0, length)));
    }
    function onError() {
      cleanup();
      resume(Effect.fail(new MalformedRequestError()));
    }
    incoming.on("data", onData);
    incoming.once("end", onEnd);
    incoming.once("error", onError);
    return Effect.sync(() => {
      cleanup();
      incoming.resume();
    });
  });

interface DomainRequestInput<Request, RequestEncoded, Result, ResultEncoded> {
  readonly routePath: string;
  readonly outerRequestSchema: Schema.Schema<Request, RequestEncoded>;
  readonly resultSchema: Schema.Schema<Result, ResultEncoded>;
  readonly bodyCap: number;
  readonly invoke: (
    request: Request,
    verifiedRequest: VerifiedAgentRequest,
  ) => Effect.Effect<Result, { readonly _tag: string }>;
  readonly requestSemaphore: Effect.Semaphore;
}

const runAdmittedDomainRequest = <
  Request,
  RequestEncoded,
  Result,
  ResultEncoded,
>(
  input: DomainRequestInput<Request, RequestEncoded, Result, ResultEncoded>,
  httpRequest: HttpServerRequest.HttpServerRequest,
) =>
  Effect.gen(function* () {
    const bodyBytes = yield* readBoundedBody(httpRequest, input.bodyCap);
    const authentication = yield* Authentication;
    const verifiedRequest = yield* authentication
      .verifyRequest({ request: httpRequest, body: bodyBytes })
      .pipe(Effect.mapError(authenticationFailure));
    const request = yield* Schema.decodeUnknown(input.outerRequestSchema)(
      verifiedRequest.request,
      {
        exact: true,
        onExcessProperty: "error",
      },
    ).pipe(
      Effect.catchTag("ParseError", () =>
        Effect.fail(new MalformedRequestError()),
      ),
    );
    const result = yield* input.invoke(request, verifiedRequest);
    return yield* resultResponse(input.resultSchema, result);
  });

const handleDomainRequest = <Request, RequestEncoded, Result, ResultEncoded>(
  input: DomainRequestInput<Request, RequestEncoded, Result, ResultEncoded>,
) =>
  Effect.gen(function* () {
    const httpRequest = yield* HttpServerRequest.HttpServerRequest;
    if (httpRequest.url !== input.routePath) {
      return errorResponse(new RouteNotFoundError());
    }
    yield* validateRequestFraming(httpRequest, input.bodyCap);
    const admitted = yield* input.requestSemaphore.withPermitsIfAvailable(1)(
      runAdmittedDomainRequest(input, httpRequest),
    );
    return yield* Option.match(admitted, {
      onNone: () => Effect.succeed(errorResponse(new OverloadedError())),
      onSome: Effect.succeed,
    });
  }).pipe(
    Effect.catchAll((error) => Effect.succeed(errorResponse(error))),
    recoverUnexpectedFailure,
  );

function sendRoute(input: RouterHttpInput) {
  return HttpRouter.post(
    routerContract.routerSendRoutePattern,
    handleDomainRequest({
      routePath: routerContract.routerSendPath,
      outerRequestSchema: routerContract.RawRouterSendRequest,
      resultSchema: routerContract.RouterSendResult,
      bodyCap: routerContract.routerRepresentationLimits.sendRequestBodyBytes,
      invoke: (request, verifiedRequest) =>
        input.send(request, verifiedRequest),
      requestSemaphore: input.requestSemaphore,
    }),
  );
}

function pollRoute(input: RouterHttpInput) {
  return HttpRouter.post(
    routerContract.routerPollRoutePattern,
    handleDomainRequest({
      routePath: routerContract.routerPollPath,
      outerRequestSchema: rawPollRequest,
      resultSchema: routerContract.RouterPollResult,
      bodyCap: routerContract.routerRepresentationLimits.pollRequestBodyBytes,
      invoke: (request, verifiedRequest) =>
        Schema.decodeUnknown(routerContract.RouterPollRequest)(request, {
          exact: true,
          onExcessProperty: "error",
        }).pipe(
          Effect.matchEffect({
            onFailure: () =>
              Effect.succeed({ kind: "cursor_invalid" } as const),
            onSuccess: (decoded) => input.poll(decoded, verifiedRequest),
          }),
        ),
      requestSemaphore: input.requestSemaphore,
    }),
  );
}

function healthRoute(input: RouterHttpInput) {
  return HttpRouter.get(
    routerContract.routerHealthPath,
    recoverUnexpectedFailure(
      Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        if (request.url !== routerContract.routerHealthPath) {
          return errorResponse(new RouteNotFoundError());
        }
        return yield* routerHealthResponse(input.feed);
      }),
    ),
  );
}

function fallbackRoute() {
  return HttpRouter.all(
    "*",
    recoverUnexpectedFailure(
      Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        if (request.url.includes("?")) {
          return errorResponse(new RouteNotFoundError());
        }
        const path = request.url.split("?", 1)[0];
        if (
          path === routerContract.routerSendPath ||
          path === routerContract.routerPollPath ||
          path === routerContract.routerHealthPath
        ) {
          return errorResponse(new MethodNotAllowedError());
        }
        return errorResponse(new RouteNotFoundError());
      }),
    ),
  );
}

/** HTTP alone projects authentication failures into the Router wire contract. */
function authenticationFailure(error: AuthenticationError) {
  switch (error.reason) {
    case "malformed":
      return new MalformedRequestError();
    case "authentication-failed":
      return new AuthenticationFailedError();
    case "version-mismatch":
      return new VersionMismatchError();
    case "overloaded":
      return new OverloadedError();
    case "unavailable":
      return new UnavailableError();
    default:
      return error.reason satisfies never;
  }
}
