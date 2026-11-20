/**
 * @file The daemon's HTTP endpoint. It admits each request as the agent of the
 * owner its OAuth token names and hands it to the MCP server `tools.ts`
 * builds for that agent.
 */

import {
  HttpApp,
  HttpBody,
  HttpServerRequest,
  HttpServerResponse,
} from "@effect/platform";
import { NodeHttpServer } from "@effect/platform-node";
import { SqlClient } from "@effect/sql";
import { discoverAuthorizationServerMetadata } from "@modelcontextprotocol/client";
import {
  type AuthInfo,
  createMcpHandler,
  getOAuthProtectedResourceMetadataUrl,
  hostHeaderValidationResponse,
  OAuthError,
  OAuthErrorCode,
  oauthMetadataResponse,
  originValidationResponse,
  requireBearerAuth,
} from "@modelcontextprotocol/server";
import { Data, Effect, Match, type Scope, Stream } from "effect";
import { createRemoteJWKSet, jwtVerify } from "jose";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- NodeHttpServer accepts the native server constructor.
import { createServer } from "node:http";
import type { DaemonSettings } from "./settings.js";
import { type Daemon, getOrCreateOwnedAgent } from "./agent.js";
import {
  inRequest,
  makeInboxEvents,
  requestRuntime,
  requestServer,
} from "./tools.js";

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

/** The issuer's metadata could not be read, names another issuer or names no key set. */
class AuthenticationRefused extends Data.TaggedError("AuthenticationRefused") {}

/**
 * The app the listener serves. The MCP SDK validates the Host and Origin
 * headers against the resource's hostname, serves the OAuth discovery
 * documents, and checks the bearer token's scope and expiry; a refusal is the
 * SDK's response. Any path but `/mcp` answers 404. A token is a JWT the issuer
 * signed for the resource with a subject, verified against the key set the
 * issuer's metadata names. The SDK's one MCP handler answers an admitted
 * request: it parses the request, negotiates the protocol revision, builds an
 * MCP server for the agent of the token's subject, dispatches the method and
 * frames the response, on a stream that ends when the token expires. No MCP
 * server outlives a request, and an aborted request ends a call in flight.
 */
export const makeMcpHttpApp = Effect.fn("makeMcpHttpApp")(function* (
  { issuer, resource, scope }: DaemonSettings["authentication"],
  daemon: Daemon,
) {
  const sql = yield* SqlClient.SqlClient;
  const events = yield* makeInboxEvents(admittedAgent);
  const mcp = createMcpHandler(({ authInfo }) =>
    inRequest(
      Effect.map(admittedAgent(authInfo), (agent) =>
        requestServer(daemon, agent, events),
      ),
    ),
  );
  const metadata = yield* Effect.tryPromise({
    try: () => discoverAuthorizationServerMetadata(issuer),
    catch: () => new AuthenticationRefused(),
  });
  if (typeof metadata?.jwks_uri !== "string") {
    return yield* new AuthenticationRefused();
  }
  const keys = createRemoteJWKSet(new URL(metadata.jwks_uri));
  const hostnames = [resource.hostname];
  const discovery = {
    oauthMetadata: metadata,
    resourceServerUrl: resource,
    scopesSupported: [scope],
  };
  const bearer = requireBearerAuth({
    requiredScopes: [scope],
    resourceMetadataUrl: getOAuthProtectedResourceMetadataUrl(resource),
    verifier: {
      // eslint-disable-next-line agent-code-guard/async-keyword -- The MCP SDK's authorization API is promise-based.
      verifyAccessToken: async (token) => {
        const verified = await jwtVerify(token, keys, {
          issuer,
          audience: resource.href,
          algorithms: ["RS256"],
        }).catch(() => undefined);
        if (!verified?.payload.sub) {
          throw new OAuthError(OAuthErrorCode.InvalidToken, "Invalid token");
        }
        const { sub, scope: granted, exp } = verified.payload;
        return {
          token,
          clientId: "",
          scopes: typeof granted === "string" ? granted.split(" ") : [],
          expiresAt: exp,
          extra: { issuer, subject: sub },
        };
      },
    },
  });
  return Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const web = new Request(new URL(request.url, resource), {
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
    // eslint-disable-next-line agent-code-guard/effect-promise -- The SDK's gate resolves to its refusal and does not reject.
    const authInfo = yield* Effect.promise(() => bearer(web));
    if (authInfo instanceof Response) {
      return HttpServerResponse.fromWeb(authInfo);
    }
    const runtime = yield* Effect.runtime<SqlClient.SqlClient>();
    const response = yield* HttpApp.fromWebHandler((mcpRequest) =>
      requestRuntime.run(runtime, () => mcp.fetch(mcpRequest, { authInfo })),
    ).pipe(Effect.interruptible, Effect.orDie);
    return yield* limitResponseStream(
      response,
      Stream.interruptAfter(
        Math.max(0, (authInfo.expiresAt ?? 0) * 1000 - Date.now()),
      ),
    );
  }).pipe(Effect.provideService(SqlClient.SqlClient, sql));
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

/** The agent of the owner an admitted request's token names. */
function admittedAgent(authInfo?: AuthInfo) {
  return getOrCreateOwnedAgent({
    issuer: String(authInfo?.extra?.issuer),
    subject: String(authInfo?.extra?.subject),
  }).pipe(Effect.orDie);
}
