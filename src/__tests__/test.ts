/** @file Fixtures for tests of the daemon: a PostgreSQL schema of its own for each test on the server `SHARNESS_PG_TEST_URL` names, and an OAuth issuer on this machine. */

import { HttpServerRequest, HttpServerResponse } from "@effect/platform";
import { NodeHttpServer } from "@effect/platform-node";
import { SqlClient } from "@effect/sql";
import { Config, Context, Effect, Layer, Redacted } from "effect";
import {
  calculateJwkThumbprint,
  exportJWK,
  generateKeyPair,
  SignJWT,
} from "jose";
import { randomBytes } from "node:crypto";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- NodeHttpServer owns the issuer listener's lifetime.
import { createServer } from "node:http";
import { databaseConnection, prepareDatabase } from "../database.js";

const serverUrl = Config.redacted("SHARNESS_PG_TEST_URL").pipe(
  Effect.catchTag("ConfigError", () =>
    Effect.dieMessage(
      "Tests that use a database need a PostgreSQL server: start one, for example with `docker run --detach --publish 127.0.0.1:5432:5432 --env POSTGRES_HOST_AUTH_METHOD=trust postgres:17`, and set SHARNESS_PG_TEST_URL to a login on it that can create schemas and roles, for example postgres://postgres@127.0.0.1:5432/postgres.",
    ),
  ),
);

/**
 * Creates a schema that only the caller's scope uses and connects to it as the
 * login of `SHARNESS_PG_TEST_URL`; closing the scope drops the schema. The
 * schema holds the daemon's tables unless `prepared` is false. `url` reaches
 * the same schema from another connection or process.
 */
export const openTestDatabase = Effect.fn("openTestDatabase")(function* (
  options: { readonly prepared?: boolean } = {},
) {
  const schema = `test_${randomBytes(8).toString("hex")}`;
  const located = new URL(Redacted.value(yield* serverUrl));
  located.searchParams.set(
    "options",
    `-c search_path=${schema} -c synchronous_commit=off`,
  );
  const url = Redacted.make(located.href);
  const sql = Context.get(
    yield* Layer.build(databaseConnection(url)),
    SqlClient.SqlClient,
  );
  yield* sql`CREATE SCHEMA ${sql(schema)}`;
  yield* Effect.addFinalizer(() =>
    sql`DROP SCHEMA ${sql(schema)} CASCADE`.pipe(Effect.orDie),
  );
  if (options.prepared !== false) {
    yield* prepareDatabase.pipe(
      Effect.provideService(SqlClient.SqlClient, sql),
    );
  }
  return {
    schema,
    url,
    sql,
    run: <A, E, R>(effect: Effect.Effect<A, E, R>) =>
      effect.pipe(Effect.provideService(SqlClient.SqlClient, sql)),
  };
}, Effect.orDie);

/** A test's database. A fixture given one serves it again, as a restart does. */
export type TestDatabase = Effect.Effect.Success<
  ReturnType<typeof openTestDatabase>
>;

/**
 * Creates a login of `database`'s tables that can read and write rows but can
 * neither create objects nor bypass row policies, as the login a daemon serves
 * through cannot. Returns the URL that reaches the schema as that login;
 * closing the scope drops it.
 */
export const restrictedLogin = Effect.fn("restrictedLogin")(function* ({
  schema,
  url,
  sql,
}: TestDatabase) {
  const role = `runtime_${schema}`;
  const password = randomBytes(24).toString("hex");
  yield* sql`CREATE ROLE ${sql(role)} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS PASSWORD ${sql.literal("'" + password + "'")}`;
  yield* Effect.addFinalizer(() =>
    sql`DROP OWNED BY ${sql(role)}`.pipe(
      Effect.zipRight(sql`DROP ROLE ${sql(role)}`),
      Effect.orDie,
    ),
  );
  yield* sql`GRANT USAGE ON SCHEMA ${sql(schema)} TO ${sql(role)}`;
  yield* sql`GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA ${sql(schema)} TO ${sql(role)}`;
  yield* sql`GRANT USAGE ON ALL SEQUENCES IN SCHEMA ${sql(schema)} TO ${sql(role)}`;
  const restricted = new URL(Redacted.value(url));
  restricted.username = role;
  restricted.password = password;
  return Redacted.make(restricted.href);
}, Effect.orDie);

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
        request.url.startsWith("/.well-known/")
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
