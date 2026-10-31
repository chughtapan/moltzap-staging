/** @file Request admission for one owner's configured OAuth resource or local bearer. */

import { FetchHttpClient, HttpClient } from "@effect/platform";
import {
  getOAuthProtectedResourceMetadataUrl,
  isJSONRPCRequest,
  type McpHttpHandler,
} from "@modelcontextprotocol/server";
import { Data, Effect, Fiber, Redacted, Schema } from "effect";
import { createRemoteJWKSet, type JWTPayload, jwtVerify } from "jose";
import { createHash, timingSafeEqual } from "node:crypto";

/** Fixed provider metadata and owner authority for one public daemon. */
export interface DaemonOAuth {
  readonly resource: URL;
  readonly issuer: string;
  readonly ownerSubject: string;
  readonly scope: string;
  readonly allowedOrigins: readonly string[];
}

/** Exactly one admission mechanism is installed for an endpoint. */
export type McpAuthentication =
  | { readonly credential: Redacted.Redacted; readonly oauth?: never }
  | { readonly credential?: never; readonly oauth: DaemonOAuth };

class AuthenticationRefused extends Data.TaggedError("AuthenticationRefused") {}

/** Metadata and bearer challenges derive from the same fixed resource. */
export const protectedResourcePath = (oauth: DaemonOAuth): string =>
  new URL(getOAuthProtectedResourceMetadataUrl(oauth.resource)).pathname;

/** Native discovery does not grant access to any daemon operation. */
export const protectedResourceResponse = (oauth: DaemonOAuth): Response =>
  Response.json(
    {
      resource: oauth.resource.href,
      authorization_servers: [oauth.issuer],
      scopes_supported: [oauth.scope],
      bearer_methods_supported: ["header"],
    },
    { headers: { "cache-control": "no-store" } },
  );

/** One cached verifier serves every request, including Events and receipts. */
export function makeRequestAdmission(
  authentication: McpAuthentication,
): Effect.Effect<
  (
    request: Request,
  ) => Effect.Effect<number | undefined, AuthenticationRefused>,
  AuthenticationRefused
> {
  if (authentication.oauth === undefined) {
    return Effect.succeed(bearerAdmission(authentication.credential));
  }
  const oauth = authentication.oauth;
  return discoverKeys(oauth).pipe(
    Effect.map(
      (keys) => (request: Request) =>
        Effect.tryPromise({
          try: () =>
            jwtVerify(bearer(request), keys, {
              issuer: oauth.issuer,
              audience: oauth.resource.href,
              algorithms: ["RS256"],
              requiredClaims: ["exp", "sub", "scope"],
            }),
          catch: () => new AuthenticationRefused(),
        }).pipe(
          Effect.flatMap(({ payload }) =>
            ownsPayload(payload, oauth) && typeof payload.exp === "number"
              ? Effect.succeed(payload.exp * 1000)
              : Effect.fail(new AuthenticationRefused()),
          ),
        ),
    ),
  );
}

/** Rejected requests disclose only the standard discovery challenge. */
export function authenticationChallenge(
  authentication: McpAuthentication,
): Response {
  const challenge =
    authentication.oauth === undefined
      ? "Bearer"
      : `Bearer resource_metadata="${getOAuthProtectedResourceMetadataUrl(authentication.oauth.resource)}"`;
  return new Response("Unauthorized", {
    status: 401,
    headers: { "www-authenticate": challenge, "cache-control": "no-store" },
  });
}

/* eslint-disable agent-code-guard/async-keyword, agent-code-guard/promise-type -- The official MCP fetch boundary and Web Streams callbacks are Promise-native. */

/** Expiration aborts the SDK request, joining its existing consumer cleanup. */
export async function fetchWithAuthority(
  delegate: McpHttpHandler,
  request: Request,
  requestOptions: Parameters<McpHttpHandler["fetch"]>[1],
  expiresAt?: number,
): Promise<Response> {
  if (expiresAt === undefined) {
    return await delegate.fetch(request, requestOptions);
  }
  const body: unknown =
    requestOptions?.parsedBody === undefined
      ? await request
          .clone()
          .json()
          .catch(() => undefined)
      : requestOptions.parsedBody;
  const options =
    body === undefined
      ? requestOptions
      : { ...requestOptions, parsedBody: body };
  if (!containsEventStream(body)) {
    return await delegate.fetch(request, options);
  }
  const controller = new AbortController();
  const signal = AbortSignal.any([request.signal, controller.signal]);
  const deadline = Effect.runFork(
    Effect.sleep(Math.max(0, expiresAt - Date.now())).pipe(
      Effect.andThen(
        Effect.sync(() => {
          controller.abort();
        }),
      ),
    ),
  );
  const stop = () => Effect.runPromise(Fiber.interrupt(deadline));
  try {
    const response = await delegate.fetch(
      new Request(request, { signal }),
      options,
    );
    return scopedResponse(response, expiresAt, controller, stop);
  } catch (cause) {
    await stop();
    throw cause;
  }
}

function scopedResponse(
  response: Response,
  expiresAt: number,
  controller: AbortController,
  stop: () => Promise<unknown>,
): Response {
  if (response.body === null) {
    void stop().catch(() => undefined);
    return response;
  }
  const reader = response.body.getReader();
  const body = new ReadableStream<Uint8Array>({
    async pull(output) {
      try {
        const item = await reader.read();
        if (item.done || Date.now() >= expiresAt) {
          output.close();
          controller.abort();
          await reader.cancel();
          await stop();
        } else {
          output.enqueue(
            Schema.decodeUnknownSync(Schema.Uint8ArrayFromSelf)(item.value),
          );
        }
      } catch (cause) {
        controller.abort();
        output.error(cause);
        await stop();
      }
    },
    async cancel(reason) {
      controller.abort();
      try {
        await reader.cancel(reason);
      } finally {
        await stop();
      }
    },
  });
  return new Response(body, {
    status: response.status,
    statusText: response.statusText,
    headers: response.headers,
  });
}

/* eslint-enable agent-code-guard/async-keyword, agent-code-guard/promise-type -- Restore repository defaults beyond the SDK boundary. */

const digest = (value: string): Uint8Array =>
  createHash("sha256").update(value).digest();

function bearer(request: Request): string {
  const value = request.headers.get("authorization") ?? "";
  return /^Bearer +/iu.test(value) ? value.replace(/^Bearer +/iu, "") : "";
}

function bearerAdmission(credential: Redacted.Redacted) {
  return (
    request: Request,
  ): Effect.Effect<number | undefined, AuthenticationRefused> =>
    timingSafeEqual(digest(bearer(request)), digest(Redacted.value(credential)))
      ? Effect.succeed(undefined)
      : Effect.fail(new AuthenticationRefused());
}

function ownsPayload(payload: JWTPayload, oauth: DaemonOAuth): boolean {
  return (
    payload.sub === oauth.ownerSubject &&
    scopeIncludes(payload.scope, oauth.scope)
  );
}

function scopeIncludes(value: unknown, scope: string): boolean {
  return typeof value === "string" && value.split(" ").includes(scope);
}

const issuerKeys = Schema.Struct({
  issuer: Schema.String,
  jwks_uri: Schema.String,
});

/** Discover the key-set location from the configured issuer before opening the listener. */
function discoverKeys(oauth: DaemonOAuth) {
  return HttpClient.get(
    new URL(
      `${oauth.issuer.replace(/\/$/u, "")}/.well-known/openid-configuration`,
    ),
  ).pipe(
    Effect.filterOrFail(
      (response) => response.status >= 200 && response.status < 300,
      () => new AuthenticationRefused(),
    ),
    Effect.flatMap((response) => response.json),
    Effect.flatMap(Schema.decodeUnknown(issuerKeys)),
    Effect.filterOrFail(
      (metadata) =>
        metadata.issuer === oauth.issuer &&
        secureKeySet(metadata.jwks_uri, oauth.issuer),
      () => new AuthenticationRefused(),
    ),
    Effect.map((metadata) => createRemoteJWKSet(new URL(metadata.jwks_uri))),
    Effect.timeoutFail({
      duration: "10 seconds",
      onTimeout: () => new AuthenticationRefused(),
    }),
    Effect.catchTags({
      RequestError: () => Effect.fail(new AuthenticationRefused()),
      ResponseError: () => Effect.fail(new AuthenticationRefused()),
      ParseError: () => Effect.fail(new AuthenticationRefused()),
    }),
    Effect.provideService(FetchHttpClient.RequestInit, { redirect: "error" }),
    Effect.provide(FetchHttpClient.layer),
    Effect.scoped,
  );
}

/** Plaintext issuer fixtures cannot relax verification for an HTTPS provider. */
function secureKeySet(value: string, issuer: string): boolean {
  if (!URL.canParse(value)) {
    return false;
  }
  const url = new URL(value);
  const noCredentials =
    url.username === "" && url.password === "" && url.hash === "";
  return (
    noCredentials &&
    (url.protocol === "https:" || localKeySet(url, new URL(issuer)))
  );
}

function localKeySet(url: URL, issuer: URL): boolean {
  return (
    issuer.protocol === "http:" &&
    issuer.hostname === "127.0.0.1" &&
    url.origin === issuer.origin
  );
}

function containsEventStream(body: unknown): boolean {
  return Array.isArray(body) ? body.some(isEventStream) : isEventStream(body);
}

function isEventStream(body: unknown): boolean {
  return isJSONRPCRequest(body) && body.method === "events/stream";
}
