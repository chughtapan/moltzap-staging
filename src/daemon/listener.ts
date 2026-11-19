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
import { getOAuthProtectedResourceMetadataUrl } from "@modelcontextprotocol/server";
import { Data, Effect, Match, Schema, type Scope, Stream } from "effect";
import { createRemoteJWKSet, jwtVerify } from "jose";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- NodeHttpServer accepts the native server constructor.
import { createServer } from "node:http";
import type { Owner } from "./owners.js";
import type { DaemonSettings } from "./settings.js";
import { plainUrl } from "../encoding.js";
import { packageVersion } from "../version.js";
import { type AgentCalls, makeMcpHandler } from "./tool-server.js";

/** What a request gets when its agent cannot serve it. */
const agentUnavailable = HttpServerResponse.text("Agent unavailable", {
  status: 503,
});

/** Request admission uses the validated authentication settings without translating them. */
export type McpAuthentication = DaemonSettings["authentication"];

/** Admission failures expose no bearer, provider response or identity claims. */
class AuthenticationRefused extends Data.TaggedError("AuthenticationRefused") {}

/** The running agent of an owner: its tools and its Events calls. */
type AgentOfOwner<E> = (
  owner: Owner,
) => Effect.Effect<AgentCalls, E, Scope.Scope>;

/**
 * The app the listener serves. It answers only POST /mcp whose Host and
 * Origin headers it trusts, admits the caller, and has the MCP handler answer
 * the request from that owner's agent. An agent that cannot start answers 503,
 * and the log names what failed.
 */
export const makeMcpHttpApp = Effect.fn("makeMcpHttpApp")(function* <
  E extends { readonly _tag: string },
>(authentication: McpAuthentication, agentOfOwner: AgentOfOwner<E>) {
  const mcp = makeMcpHandler({ name: "sharnessd", version: packageVersion });
  const admit = yield* tokenAdmission(authentication);
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
    return yield* admit(request.headers.authorization ?? "").pipe(
      Effect.matchEffect({
        onFailure: () =>
          Effect.succeed(
            HttpServerResponse.fromWeb(authenticationChallenge(authentication)),
          ),
        onSuccess: ({ owner, expiresAt }) =>
          agentOfOwner(owner).pipe(
            Effect.flatMap(mcp),
            Effect.flatMap((response) =>
              limitResponseStream(
                response,
                Stream.interruptAfter(Math.max(0, expiresAt - Date.now())),
              ),
            ),
            Effect.catchAll((error) =>
              Effect.logWarning("Agent unavailable").pipe(
                Effect.annotateLogs({ failure: error._tag }),
                Effect.as(agentUnavailable),
              ),
            ),
          ),
      }),
    );
  });
});

/** Returns after listening. Scope closure disconnects streaming requests before the listener closes. */
export const serveMcp = Effect.fn("Mcp.serve")(function* (
  port: number,
  app: HttpApp.Default<never, Scope.Scope>,
  host: string,
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
function limitResponseStream(
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

/**
 * The Host and Origin headers a request may carry: the daemon is reached at
 * its resource and from its allowed origins.
 */
function trustedCallers(authentication: McpAuthentication) {
  return {
    host: (host: string) =>
      host.toLowerCase() === authentication.resource.host.toLowerCase(),
    origin: (origin: string) => authentication.allowedOrigins.includes(origin),
  };
}

/** Metadata and bearer challenges derive from the same fixed resource. */
function protectedResourcePath(oauth: McpAuthentication): string {
  return new URL(getOAuthProtectedResourceMetadataUrl(oauth.resource)).pathname;
}

/** Native discovery does not grant access to any daemon operation. */
function protectedResourceResponse(oauth: McpAuthentication): Response {
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

/** Rejected requests disclose only the standard discovery challenge. */
function authenticationChallenge(authentication: McpAuthentication): Response {
  return new Response("Unauthorized", {
    status: 401,
    headers: {
      "www-authenticate": `Bearer resource_metadata="${getOAuthProtectedResourceMetadataUrl(authentication.resource)}"`,
      "cache-control": "no-store",
    },
  });
}

/**
 * Admission of the owner an access token for the resource names, until the
 * token expires. One cached verifier serves every request, including Events
 * and receipts.
 */
function tokenAdmission(oauth: McpAuthentication) {
  return discoverKeys(oauth).pipe(
    Effect.map(
      (keys) => (authorization: string) =>
        Effect.tryPromise({
          try: () =>
            jwtVerify(bearer(authorization), keys, {
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

function bearer(authorization: string): string {
  return /^Bearer +/iu.test(authorization)
    ? authorization.replace(/^Bearer +/iu, "")
    : "";
}

function scopeIncludes(value: unknown, scope: string): boolean {
  return typeof value === "string" && value.split(" ").includes(scope);
}

const issuerMetadata = Schema.Struct({
  issuer: Schema.String,
  jwks_uri: Schema.String,
});

/** Discover the key-set location from the configured issuer before opening the listener. */
function discoverKeys(oauth: McpAuthentication) {
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
  const url = plainUrl(value, ["https:", "http:"]);
  return (
    url !== undefined &&
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
