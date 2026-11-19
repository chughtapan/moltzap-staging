/** @file An OAuth issuer on this machine whose tokens sign owners in to a daemon under test or in development. */
import { HttpServerRequest, HttpServerResponse } from "@effect/platform";
import { NodeHttpServer } from "@effect/platform-node";
import { Effect } from "effect";
import {
  calculateJwkThumbprint,
  exportJWK,
  generateKeyPair,
  SignJWT,
} from "jose";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- NodeHttpServer owns the issuer listener's lifetime.
import { createServer } from "node:http";

/** The scope a token carries unless told otherwise, for the daemon to require. */
export const requiredScope = "use:mcp";

/**
 * An OpenID issuer on `127.0.0.1`, at `port` or a free one, with a key of its
 * own, and a signer of access tokens for its subjects. A token selects its
 * subject's agent on a daemon whose resource is `audience`, for an hour and
 * with `requiredScope` unless `claims` say otherwise. The scope closes the
 * listener.
 */
export const issuerFixture = Effect.fn("issuerFixture")(function* (
  port?: number,
) {
  const keys = yield* Effect.tryPromise(() => generateKeyPair("RS256"));
  const publicKey = yield* Effect.tryPromise(() => exportJWK(keys.publicKey));
  const kid = yield* Effect.tryPromise(() => calculateJwkThumbprint(publicKey));
  const server = yield* NodeHttpServer.make(createServer, {
    host: "127.0.0.1",
    port: port ?? 0,
  });
  if (server.address._tag !== "TcpAddress") {
    return yield* Effect.dieMessage("Expected issuer TCP listener");
  }
  const issuer = `http://127.0.0.1:${server.address.port}/`;
  yield* server.serve(
    Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest;
      return yield* HttpServerResponse.json(
        request.url === "/.well-known/openid-configuration"
          ? {
              issuer,
              jwks_uri: `${issuer}jwks`,
              authorization_endpoint: `${issuer}authorize`,
              token_endpoint: `${issuer}token`,
              response_types_supported: ["code"],
            }
          : { keys: [{ ...publicKey, kid, alg: "RS256" }] },
      ).pipe(Effect.orDie);
    }),
  );
  const token = (
    subject: string,
    audience: string,
    claims: { readonly scope?: string; readonly secondsLeft?: number } = {},
  ) =>
    Effect.tryPromise(() =>
      new SignJWT({ scope: claims.scope ?? requiredScope })
        .setProtectedHeader({ alg: "RS256", kid })
        .setIssuer(issuer)
        .setAudience(audience)
        .setSubject(subject)
        .setExpirationTime(
          Math.floor(Date.now() / 1000) + (claims.secondsLeft ?? 3600),
        )
        .sign(keys.privateKey),
    );
  return { issuer, token };
});
