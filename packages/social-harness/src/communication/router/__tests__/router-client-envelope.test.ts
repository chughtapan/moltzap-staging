/** @file Public Router client validation of exact version-mismatch envelopes. */
import { HttpClient, HttpClientResponse } from "@effect/platform";
import { Duration, Effect, Either, Layer, Redacted, Schema } from "effect";
import { generateKeyPairSync } from "node:crypto";
import { expect, it } from "vitest";
import { AgentId } from "../../../identity/agent.js";
import { VersionMismatchError } from "../../../identity/http-signatures.js";
import { AgentSigningAuthority } from "../../../identity/keys.js";
import { Router } from "../api.js";

const supportedVersions = ["2099.101.0", "2099.102.0"];
const invalidResponseTag = "RouterInvalidResponseError";
const callerAgentId = Schema.decodeUnknownSync(AgentId)(
  "agt_AAAAAAAAAAAAAAAAAAAAAA",
);

const pollResponse = (body: string) =>
  Effect.gen(function* () {
    const signingAuthority = yield* AgentSigningAuthority.fromPkcs8(
      Redacted.make(
        generateKeyPairSync("ed25519").privateKey.export({
          format: "pem",
          type: "pkcs8",
        }),
      ),
    );
    const client = HttpClient.make((request) =>
      Effect.succeed(
        HttpClientResponse.fromWeb(
          request,
          new Response(body, {
            status: 412,
            headers: { "content-type": "application/json" },
          }),
        ),
      ),
    );
    return yield* Router.poll({
      request: {},
      callerAgentId,
      signingAuthority,
    }).pipe(
      Effect.provide(
        Router.layer({
          origin: new URL("http://router.example:3010"),
          sendTimeout: Duration.seconds(5),
          pollTimeout: Duration.seconds(5),
        }).pipe(Layer.provide(Layer.succeed(HttpClient.HttpClient, client))),
      ),
      Effect.either,
    );
  });

it("preserves the supported versions from a canonical 412", () =>
  Effect.runPromise(
    pollResponse(
      '{"error":"version_mismatch","supported":["2099.101.0","2099.102.0"]}',
    ).pipe(
      Effect.map((outcome) => {
        Either.match(outcome, {
          onLeft: (error) => {
            expect(error).toBeInstanceOf(VersionMismatchError);
            expect(error).toMatchObject({
              _tag: "VersionMismatchError",
              supported: supportedVersions,
            });
          },
          onRight: () => expect.fail("412 response was accepted as success"),
        });
        return undefined;
      }),
    ),
  ));

it.each([
  ["reordered keys", '{"supported":["2099.101.0"],"error":"version_mismatch"}'],
  ["whitespace", '{ "error":"version_mismatch","supported":["2099.101.0"]}'],
  [
    "duplicate key",
    '{"error":"version_mismatch","error":"version_mismatch","supported":["2099.101.0"]}',
  ],
  [
    "JSON escape",
    '{"error":"version\\u005fmismatch","supported":["2099.101.0"]}',
  ],
  ["empty supported", '{"error":"version_mismatch","supported":[]}'],
  [
    "invalid wire version",
    '{"error":"version_mismatch","supported":["invalid"]}',
  ],
  ["lone surrogate", '{"error":"version_mismatch","supported":["\\ud800"]}'],
  [
    "BOM prefix",
    '\ufeff{"error":"version_mismatch","supported":["2099.101.0"]}',
  ],
  ["missing supported", '{"error":"version_mismatch"}'],
  [
    "extra member",
    '{"error":"version_mismatch","extra":true,"supported":["2099.101.0"]}',
  ],
  ["invalid supported", '{"error":"version_mismatch","supported":[1]}'],
])("rejects a 412 with %s", (name, body) =>
  Effect.runPromise(
    pollResponse(body).pipe(
      Effect.map((outcome) => {
        Either.match(outcome, {
          onLeft: (error) => {
            expect(error._tag, name).toBe(invalidResponseTag);
          },
          onRight: () => expect.fail("invalid 412 response was accepted"),
        });
        return undefined;
      }),
    ),
  ),
);
