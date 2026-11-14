/**
 * @file The daemon's HTTP endpoint: the listener, which requests it accepts,
 * and who each one is admitted as.
 */

import {
  FetchHttpClient,
  type HttpApp,
  HttpBody,
  HttpClient,
  HttpServerRequest,
  HttpServerResponse,
} from "@effect/platform";
import { NodeHttpServer } from "@effect/platform-node";
import {
  Data,
  Effect,
  Match,
  Option,
  Redacted,
  Schema,
  type Scope,
  Stream,
} from "effect";
import { createRemoteJWKSet, jwtVerify } from "jose";
import { createHash, timingSafeEqual } from "node:crypto";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- NodeHttpServer accepts the native server constructor.
import { createServer } from "node:http";
import type { DaemonSettings } from "./config.js";
import { localOwner, type Owner, serverAgentOwner } from "./ownership.js";

const getOAuthProtectedResourceMetadataUrl = (resource: URL): string =>
  new URL(
    `/.well-known/oauth-protected-resource${resource.pathname}`,
    resource.origin,
  ).href;

/** Request admission uses the validated authentication settings without translating them. */
export type McpAuthentication = DaemonSettings["authentication"];

type OAuthSettings = Extract<McpAuthentication, { readonly kind: "oauth" }>;

/** The verified owner selects an agent; expiry bounds the admitted response stream. */
interface AdmittedCaller {
  readonly owner: Owner;
  readonly expiresAt?: number;
}

/** Admission failures expose no bearer, provider response or identity claims. */
export class AuthenticationRefused extends Data.TaggedError(
  "AuthenticationRefused",
) {}

/** An agent's MCP app, served to the requests admitted as its owner. */
type AppOfOwner = (
  owner: Owner,
) => Effect.Effect<HttpApp.Default<never, Scope.Scope>, unknown, Scope.Scope>;

/**
 * The app the listener serves. It answers only POST /mcp whose Host and
 * Origin headers it trusts, admits the caller, and hands the request to that
 * owner's agent. An agent that cannot serve answers 503, and the log names
 * what failed.
 */
export const makeMcpHttpApp = Effect.fn("makeMcpHttpApp")(function* (
  authentication: McpAuthentication,
  appOfOwner: AppOfOwner,
  serverAgentCredential: Option.Option<Redacted.Redacted>,
) {
  const admit = yield* makeRequestAdmission(
    authentication,
    serverAgentCredential,
  );
  const trusted = trustedCallers(authentication);
  return Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const { host, origin } = request.headers;
    if (
      host === undefined ||
      !trusted.host(host) ||
      (origin !== undefined && !trusted.origin(origin))
    ) {
      return HttpServerResponse.text("Forbidden", { status: 403 });
    }
    const path = new URL(request.url, "http://localhost").pathname;
    if (
      authentication.kind === "oauth" &&
      path === protectedResourcePath(authentication) &&
      request.method === "GET"
    ) {
      return HttpServerResponse.fromWeb(
        protectedResourceResponse(authentication),
      );
    }
    if (path !== "/mcp") {
      return HttpServerResponse.empty({ status: 404 });
    }
    if (request.method !== "POST") {
      return HttpServerResponse.empty({
        status: 405,
        headers: { allow: "POST" },
      });
    }
    return yield* admit(
      new Request(trusted.resource, { headers: request.headers }),
    ).pipe(
      Effect.matchEffect({
        onFailure: () =>
          Effect.succeed(
            HttpServerResponse.fromWeb(authenticationChallenge(authentication)),
          ),
        onSuccess: ({ owner, expiresAt }) =>
          appOfOwner(owner).pipe(
            Effect.flatMap((app) => app),
            Effect.flatMap((response) =>
              expiresAt === undefined
                ? response
                : limitResponseStream(
                    response,
                    Stream.interruptAfter(Math.max(0, expiresAt - Date.now())),
                  ),
            ),
            Effect.catchAll((error) =>
              Effect.logWarning("Agent unavailable").pipe(
                Effect.annotateLogs(failureKind(error)),
                Effect.as(
                  HttpServerResponse.text("Agent unavailable", { status: 503 }),
                ),
              ),
            ),
          ),
      }),
    );
  });
}, Effect.withSpan("makeMcpHttpApp"));

/** A local bearer daemon binds and advertises the same default address. */
const LOOPBACK_HOST = "127.0.0.1";

/** Returns after listening. Scope closure disconnects streaming requests before the listener closes. */
export const serveMcp = Effect.fn("Mcp.serve")(function* (
  port: number,
  app: HttpApp.Default<never, Scope.Scope>,
  host: string = LOOPBACK_HOST,
) {
  const listener = yield* Effect.sync(createServer);
  const http = yield* NodeHttpServer.make(() => listener, { host, port });
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      listener.closeAllConnections();
    }),
  );
  yield* http.serve(app);
});

/** Ends a streamed response body when `limit` says so. Other bodies are returned as they are. */
export function limitResponseStream(
  response: HttpServerResponse.HttpServerResponse,
  limit: <A, E>(body: Stream.Stream<A, E>) => Stream.Stream<A, E>,
) {
  return Match.value(response.body).pipe(
    Match.tag("Stream", (body) =>
      HttpServerResponse.setBody(
        response,
        HttpBody.stream(limit(body.stream), body.contentType),
      ),
    ),
    Match.orElse(() => response),
  );
}

/** The tag and reason of a failure, which name its kind and carry none of its content. */
function failureKind(error: unknown): Record<string, string> {
  if (typeof error !== "object" || error === null) {
    return { failure: "unknown" };
  }
  return {
    failure:
      "_tag" in error && typeof error._tag === "string"
        ? error._tag
        : "unknown",
    ...("reason" in error && typeof error.reason === "string"
      ? { reason: error.reason }
      : {}),
  };
}

/**
 * The Host and Origin headers a request may carry, and the resource its
 * bearer is checked against. A local bearer daemon is reached only through
 * loopback. An OAuth daemon is reached at its resource and from its allowed
 * origins.
 */
function trustedCallers(authentication: McpAuthentication) {
  if (authentication.kind === "oauth") {
    return {
      resource: authentication.resource.href,
      host: (host: string) =>
        host.toLowerCase() === authentication.resource.host.toLowerCase(),
      origin: (origin: string) =>
        authentication.allowedOrigins.includes(origin),
    };
  }
  return {
    resource: "http://127.0.0.1/mcp",
    host: (host: string) => isLoopbackUrl(`http://${host}`),
    origin: isLoopbackUrl,
  };
}

/** A plain http(s) URL on this machine, with no credentials or fragment. */
function isLoopbackUrl(value: string): boolean {
  const url = Schema.decodeUnknownOption(Schema.URL)(value);
  if (Option.isNone(url)) {
    return false;
  }
  const { username, password, hash, protocol, hostname } = url.value;
  const plain = !username && !password && !hash;
  return (
    plain &&
    ["http:", "https:"].includes(protocol) &&
    ["127.0.0.1", "[::1]", "localhost"].includes(hostname)
  );
}

/** Metadata and bearer challenges derive from the same fixed resource. */
function protectedResourcePath(oauth: OAuthSettings): string {
  return new URL(getOAuthProtectedResourceMetadataUrl(oauth.resource)).pathname;
}

/** Native discovery does not grant access to any daemon operation. */
function protectedResourceResponse(oauth: OAuthSettings): Response {
  return Response.json(
    {
      resource: oauth.resource.href,
      authorization_servers: [oauth.issuer],
      scopes_supported: [oauth.scope],
      bearer_methods_supported: ["header"],
    },
    { headers: { "cache-control": "no-store" } },
  );
}

/** Decides whether a request is admitted, and as which owner. */
type Admit = (
  request: Request,
) => Effect.Effect<AdmittedCaller, AuthenticationRefused>;

/**
 * One cached verifier serves every request, including Events and receipts. A
 * request carrying the server agent's credential is admitted as the server
 * agent; any other goes through the configured authentication.
 */
function makeRequestAdmission(
  authentication: McpAuthentication,
  serverAgentCredential: Option.Option<Redacted.Redacted>,
): Effect.Effect<Admit, AuthenticationRefused> {
  return Effect.map(configuredAdmission(authentication), (admit) => {
    if (Option.isNone(serverAgentCredential)) {
      return admit;
    }
    const admitServerAgent = bearerAdmission(
      serverAgentCredential.value,
      serverAgentOwner,
    );
    return (request: Request) =>
      Effect.orElse(admitServerAgent(request), () => admit(request));
  });
}

/** Rejected requests disclose only the standard discovery challenge. */
function authenticationChallenge(authentication: McpAuthentication): Response {
  const challenge =
    authentication.kind === "local"
      ? "Bearer"
      : `Bearer resource_metadata="${getOAuthProtectedResourceMetadataUrl(authentication.resource)}"`;
  return new Response("Unauthorized", {
    status: 401,
    headers: { "www-authenticate": challenge, "cache-control": "no-store" },
  });
}

/** Admission by the configured local bearer or OAuth resource. */
function configuredAdmission(
  authentication: McpAuthentication,
): Effect.Effect<Admit, AuthenticationRefused> {
  if (authentication.kind === "local") {
    return Effect.succeed(
      bearerAdmission(authentication.credential, localOwner),
    );
  }
  const oauth = authentication;
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
          Effect.filterOrFail(
            ({ payload }) => scopeIncludes(payload.scope, oauth.scope),
            () => new AuthenticationRefused(),
          ),
          Effect.flatMap(({ payload }) =>
            typeof payload.sub === "string" &&
            payload.sub.length > 0 &&
            typeof payload.exp === "number"
              ? Effect.succeed({
                  owner: { issuer: oauth.issuer, subject: payload.sub },
                  expiresAt: payload.exp * 1000,
                })
              : Effect.fail(new AuthenticationRefused()),
          ),
        ),
    ),
  );
}

const digest = (value: string): Uint8Array =>
  createHash("sha256").update(value).digest();

function bearer(request: Request): string {
  const value = request.headers.get("authorization") ?? "";
  return /^Bearer +/iu.test(value) ? value.replace(/^Bearer +/iu, "") : "";
}

/** Admits a request whose bearer is `credential`, as `owner`. */
function bearerAdmission(credential: Redacted.Redacted, owner: Owner) {
  return (
    request: Request,
  ): Effect.Effect<AdmittedCaller, AuthenticationRefused> =>
    timingSafeEqual(digest(bearer(request)), digest(Redacted.value(credential)))
      ? Effect.succeed({ owner })
      : Effect.fail(new AuthenticationRefused());
}

function scopeIncludes(value: unknown, scope: string): boolean {
  return typeof value === "string" && value.split(" ").includes(scope);
}

const issuerMetadata = Schema.Struct({
  issuer: Schema.String,
  jwks_uri: Schema.String,
});

/** Discover the key-set location from the configured issuer before opening the listener. */
function discoverKeys(oauth: OAuthSettings) {
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
    Effect.flatMap(Schema.decodeUnknown(issuerMetadata)),
    Effect.filterOrFail(
      (metadata) =>
        metadata.issuer === oauth.issuer &&
        isTrustedKeySetUrl(metadata.jwks_uri, oauth.issuer),
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
function isTrustedKeySetUrl(value: string, issuer: string): boolean {
  if (!URL.canParse(value)) {
    return false;
  }
  const url = new URL(value);
  const noCredentials =
    url.username === "" && url.password === "" && url.hash === "";
  return (
    noCredentials &&
    (url.protocol === "https:" || isLocalIssuerKeySet(url, new URL(issuer)))
  );
}

function isLocalIssuerKeySet(url: URL, issuer: URL): boolean {
  return (
    issuer.protocol === "http:" &&
    issuer.hostname === "127.0.0.1" &&
    url.origin === issuer.origin
  );
}
