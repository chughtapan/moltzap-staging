/**
 * @file The daemon's HTTP endpoint: the listener, which requests it accepts,
 * and who each one is admitted as.
 */

import {
  type HttpApp,
  HttpBody,
  HttpServerRequest,
  HttpServerResponse,
} from "@effect/platform";
import { NodeHttpServer } from "@effect/platform-node";
import { discoverAuthorizationServerMetadata } from "@modelcontextprotocol/client";
import {
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
import type { Owner } from "./agent.js";
import type { DaemonSettings } from "./settings.js";
import { packageVersion } from "../version.js";
import { type AgentCalls, makeMcpHandler } from "./tool-server.js";

/** What a request gets when its agent cannot serve it. */
const agentUnavailable = HttpServerResponse.text("Agent unavailable", {
  status: 503,
});

/** Request admission uses the validated authentication settings without translating them. */
type McpAuthentication = DaemonSettings["authentication"];

/** The issuer's metadata could not be read, names another issuer or names no key set. */
class AuthenticationRefused extends Data.TaggedError("AuthenticationRefused") {}

/** The agent of an owner: its tools and its Events calls. */
type AgentOfOwner<E> = (
  owner: Owner,
) => Effect.Effect<AgentCalls, E, Scope.Scope>;

/**
 * The app the listener serves. The MCP SDK validates the Host and Origin
 * headers against the resource's hostname, serves the OAuth discovery
 * documents, and checks the bearer token's scope and expiry; a refusal is the
 * SDK's response. Any path but `/mcp` answers 404. A token is a JWT the issuer
 * signed for the resource with a subject, verified against the key set the
 * issuer's metadata names. The MCP handler answers an admitted request from
 * the agent of the token's subject, on a stream that ends when the token
 * expires. A request whose agent cannot be read answers 503, and the log
 * names what failed.
 */
export const makeMcpHttpApp = Effect.fn("makeMcpHttpApp")(function* <
  E extends { readonly _tag: string },
>(authentication: McpAuthentication, agentOfOwner: AgentOfOwner<E>) {
  const { issuer, resource, scope } = authentication;
  const mcp = makeMcpHandler({ name: "sharnessd", version: packageVersion });
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
          extra: { subject: sub },
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
    const admitted = yield* Effect.promise(() => bearer(web));
    if (admitted instanceof Response) {
      return HttpServerResponse.fromWeb(admitted);
    }
    return yield* agentOfOwner({
      issuer,
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
