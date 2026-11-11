/** @file An OAuth issuer whose tokens sign owners in to a test daemon. */
import { HttpServerRequest, HttpServerResponse } from "@effect/platform";
import { NodeHttpServer } from "@effect/platform-node";
import { Effect } from "effect";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- NodeHttpServer owns the issuer listener's lifetime.
import { createServer } from "node:http";

/** The scope every test token carries and the daemon requires. */
export const requiredScope = "use:mcp";

/**
 * A local OpenID issuer and a signer of access tokens for its subjects, each
 * selecting that subject's own agent on a daemon whose resource is `audience`.
 */
export const issuerFixture = Effect.gen(function* () {
  const keys = yield* Effect.tryPromise(() => generateKeyPair("RS256"));
  const publicKey = yield* Effect.tryPromise(() => exportJWK(keys.publicKey));
  const server = yield* NodeHttpServer.make(createServer, {
    host: "127.0.0.1",
    port: 0,
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
          ? { issuer, jwks_uri: `${issuer}jwks` }
          : { keys: [{ ...publicKey, kid: "shared-test", alg: "RS256" }] },
      ).pipe(Effect.orDie);
    }),
  );
  const token = (subject: string, audience: string) =>
    Effect.tryPromise(() =>
      new SignJWT({ scope: requiredScope })
        .setProtectedHeader({ alg: "RS256", kid: "shared-test" })
        .setIssuer(issuer)
        .setAudience(audience)
        .setSubject(subject)
        .setExpirationTime("1h")
        .sign(keys.privateKey),
    );
  return { issuer, token };
}).pipe(Effect.withSpan("issuerFixture"));
