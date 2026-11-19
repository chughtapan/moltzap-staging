/**
 * @file The daemon's HTTP endpoint: the listener, which requests it accepts,
 * and who each one is admitted as.
 */

import {
  FetchHttpClient,
  type HttpApp,
  HttpBody,
  HttpClient,
  HttpClientResponse,
  HttpServerRequest,
  HttpServerResponse,
} from "@effect/platform";
import { NodeHttpServer } from "@effect/platform-node";
import {
  bearerAuthChallengeResponse,
  getOAuthProtectedResourceMetadataUrl,
  hostHeaderValidationResponse,
  OAuthError,
  OAuthErrorCode,
  oauthMetadataResponse,
  type OAuthTokenVerifier,
  originValidationResponse,
  verifyBearerToken,
} from "@modelcontextprotocol/server";
import { Data, Effect, Match, Schema, type Scope, Stream } from "effect";
import { createRemoteJWKSet, jwtVerify } from "jose";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- NodeHttpServer accepts the native server constructor.
import { createServer } from "node:http";
import type { Owner } from "./owners.js";
import type { DaemonSettings } from "./settings.js";
import { packageVersion } from "../version.js";
import { type AgentCalls, makeMcpHandler } from "./tool-server.js";

/** What a request gets when its agent cannot serve it. */
const agentUnavailable = HttpServerResponse.text("Agent unavailable", {
  status: 503,
});

/** What the SDK refuses a token with when the daemon's verifier rejects it. */
const invalidToken = new OAuthError(
  OAuthErrorCode.InvalidToken,
  "Invalid access token",
);

/** Request admission uses the validated authentication settings without translating them. */
type McpAuthentication = DaemonSettings["authentication"];

/** The issuer's metadata could not be read, or names another issuer. */
class AuthenticationRefused extends Data.TaggedError("AuthenticationRefused") {}

/** The running agent of an owner: its tools and its Events calls. */
type AgentOfOwner<E> = (
  owner: Owner,
) => Effect.Effect<AgentCalls, E, Scope.Scope>;

/**
 * The app the listener serves. The MCP SDK validates the Host and Origin
 * headers against the resource's hostname, serves the OAuth discovery
 * documents, and checks the bearer token's scope and expiry; a refusal is the
 * SDK's response. Any path but `/mcp` answers 404. The MCP handler answers an
 * admitted request from the agent of the token's owner, on a stream that ends
 * when the token expires. An agent that cannot start answers 503, and the log
 * names what failed.
 */
export const makeMcpHttpApp = Effect.fn("makeMcpHttpApp")(function* <
  E extends { readonly _tag: string },
>(authentication: McpAuthentication, agentOfOwner: AgentOfOwner<E>) {
  const mcp = makeMcpHandler({ name: "sharnessd", version: packageVersion });
  const issuer = yield* issuerMetadataOf(authentication);
  const hostnames = [authentication.resource.hostname];
  const discovery = {
    oauthMetadata: issuer,
    resourceServerUrl: authentication.resource,
    scopesSupported: [authentication.scope],
  };
  const bearer = {
    verifier: tokenVerifier(authentication, issuer.jwks_uri),
    requiredScopes: [authentication.scope],
    resourceMetadataUrl: getOAuthProtectedResourceMetadataUrl(
      authentication.resource,
    ),
  };
  return Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const web = new Request(new URL(request.url, authentication.resource), {
      method: request.method,
      headers: request.headers,
    });
    const answered =
      hostHeaderValidationResponse(web, hostnames) ??
      originValidationResponse(web, hostnames) ??
      oauthMetadataResponse(web, discovery);
    if (answered !== undefined) {
      return HttpServerResponse.fromWeb(answered);
    }
    if (new URL(web.url).pathname !== "/mcp") {
      return HttpServerResponse.empty({ status: 404 });
    }
    return yield* Effect.tryPromise({
      try: () => verifyBearerToken(request.headers.authorization, bearer),
      catch: (error) =>
        bearerAuthChallengeResponse(
          error instanceof OAuthError ? error : invalidToken,
          bearer,
        ),
    }).pipe(
      Effect.matchEffect({
        onFailure: (challenge) =>
          Effect.succeed(HttpServerResponse.fromWeb(challenge)),
        onSuccess: (admitted) =>
          agentOfOwner({
            issuer: authentication.issuer,
            subject: String(admitted.extra?.subject),
          }).pipe(
            Effect.flatMap(mcp),
            Effect.flatMap((response) =>
              limitResponseStream(
                response,
                Stream.interruptAfter(
                  Math.max(0, (admitted.expiresAt ?? 0) * 1000 - Date.now()),
                ),
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
 * Verifies an access token as a JWT the issuer signed for the resource, and
 * reports its non-empty subject as `extra.subject`. One cached key set serves
 * every request. The promise of a token that fails rejects.
 */
function tokenVerifier(
  oauth: McpAuthentication,
  keySet: URL,
): OAuthTokenVerifier {
  const keys = createRemoteJWKSet(keySet);
  return {
    verifyAccessToken: (token) =>
      Effect.tryPromise(() =>
        jwtVerify(token, keys, {
          issuer: oauth.issuer,
          audience: oauth.resource.href,
          algorithms: ["RS256"],
        }),
      ).pipe(
        Effect.filterOrFail(({ payload }) => Boolean(payload.sub)),
        Effect.map(({ payload }) => ({
          token,
          clientId: "",
          scopes:
            typeof payload.scope === "string" ? payload.scope.split(" ") : [],
          expiresAt: payload.exp,
          extra: { subject: payload.sub },
        })),
        Effect.runPromise,
      ),
  };
}

/** The issuer's metadata, which the daemon also serves as its authorization-server document. */
const issuerMetadata = Schema.Struct(
  {
    issuer: Schema.String,
    jwks_uri: Schema.URL,
    authorization_endpoint: Schema.String,
    token_endpoint: Schema.String,
    response_types_supported: Schema.mutable(Schema.Array(Schema.String)),
  },
  Schema.Record({ key: Schema.String, value: Schema.Unknown }),
);

/** Reads the configured issuer's metadata, which must name that issuer, before the listener opens. */
function issuerMetadataOf(oauth: McpAuthentication) {
  return HttpClient.get(
    new URL(
      `${oauth.issuer.replace(/\/$/u, "")}/.well-known/openid-configuration`,
    ),
  ).pipe(
    Effect.flatMap(HttpClientResponse.filterStatusOk),
    Effect.flatMap(HttpClientResponse.schemaBodyJson(issuerMetadata)),
    Effect.filterOrFail(
      (metadata) => metadata.issuer === oauth.issuer,
      () => new AuthenticationRefused(),
    ),
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
