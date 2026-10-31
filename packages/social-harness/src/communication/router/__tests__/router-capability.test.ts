/** @file Public Router capability tests across HTTP success and typed failure paths. */
import {
  HttpClient,
  HttpClientError,
  HttpClientResponse,
} from "@effect/platform";
import { it as effectIt } from "@effect/vitest";
import {
  Brand,
  Duration,
  Effect,
  Either,
  Encoding,
  Layer,
  Option,
  Redacted,
  Schema,
} from "effect";
import { generateKeyPairSync } from "node:crypto";
import { describe, expect, it } from "vitest";
import {
  AgentId,
  type AgentId as AgentIdValue,
} from "../../../identity/identifiers.js";
import {
  AgentSigningAuthority,
  type AgentSigningAuthority as AgentSigningAuthorityValue,
} from "../../../identity/keys.js";
import { AgentSigningError } from "../../../identity/signing/http-signature.js";
import { Router } from "../api/client.js";
import {
  type PollCursor as PollCursorValue,
  RouterConnectionError,
  RouterInvalidResponseError,
  RouterRequestTimeoutError,
} from "../api/contract.js";

/* eslint-disable max-lines-per-function, agent-code-guard/no-hardcoded-assertion-literals -- Each scenario observes the deep capability at its public or private call boundary. */
const utf8Encoder = new TextEncoder();

const identifierPayload = (seed: number): string =>
  Encoding.encodeBase64Url(new Uint8Array(16).fill(seed));

const makeAgentId = (seed: number): AgentIdValue =>
  Schema.decodeUnknownSync(AgentId)(`agt_${identifierPayload(seed)}`);

const withUtf8Bom = (text: string): Uint8Array => {
  const bytes = utf8Encoder.encode(text);
  const marked = new Uint8Array(bytes.byteLength + 3);
  marked.set([0xef, 0xbb, 0xbf]);
  marked.set(bytes, 3);
  return marked;
};

const withMalformedUtf8Suffix = (text: string): Uint8Array => {
  const bytes = utf8Encoder.encode(text);
  const malformed = new Uint8Array(bytes.byteLength + 1);
  malformed.set(bytes);
  malformed[bytes.byteLength] = 0xff;
  return malformed;
};

const recipient = makeAgentId(2);
const makePrivateKeyPem = (): string =>
  generateKeyPairSync("ed25519").privateKey.export({
    format: "pem",
    type: "pkcs8",
  });

const makeSigningAuthority = () => {
  return AgentSigningAuthority.fromPkcs8(Redacted.make(makePrivateKeyPem()));
};

const runPollWithClient = (
  httpClient: HttpClient.HttpClient,
  signingAuthority: AgentSigningAuthorityValue,
  pollTimeout: Duration.Duration,
) =>
  Router.poll({
    request: {},
    callerAgentId: recipient,
    signingAuthority,
  }).pipe(
    Effect.provide(
      Router.layer({
        origin: new URL("http://router.example:3010"),
        sendTimeout: Duration.seconds(5),
        pollTimeout,
      }).pipe(Layer.provide(Layer.succeed(HttpClient.HttpClient, httpClient))),
    ),
    Effect.either,
  );

describe("public client behavior", () => {
  effectIt.effect(
    "snapshots its origin and rejects the wrong response media type",
    () =>
      Effect.gen(function* () {
        let requestedUrl = "";
        const httpClient = HttpClient.make((request, url) => {
          requestedUrl = url.href;
          return Effect.succeed(
            HttpClientResponse.fromWeb(
              request,
              new Response('{"kind":"cursor_invalid"}', {
                status: 200,
                headers: { "content-type": "text/plain" },
              }),
            ),
          );
        });
        const origin = new URL("http://router.example:3010");
        const routerLayer = Router.layer({
          origin,
          sendTimeout: Duration.seconds(5),
          pollTimeout: Duration.seconds(5),
        }).pipe(
          Layer.provide(Layer.succeed(HttpClient.HttpClient, httpClient)),
        );
        origin.hostname = "mutated.example";
        const signingAuthority = yield* makeSigningAuthority();
        const outcome = yield* Router.poll({
          request: {},
          callerAgentId: recipient,
          signingAuthority,
        }).pipe(Effect.provide(routerLayer), Effect.either);
        expect(requestedUrl).toBe(
          "http://router.example:3010/v1/messages:poll",
        );
        Either.match(outcome, {
          onLeft: (error) => {
            expect(error).toBeInstanceOf(RouterInvalidResponseError);
          },
          onRight: () => {
            expect.fail("wrong response media type was accepted");
          },
        });
      }),
  );

  it("keeps server, invalid-response, connection, and timeout failures distinct", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const signingAuthority = yield* makeSigningAuthority();
        const responseCases = [
          {
            name: "malformed request",
            status: 400,
            body: '{"error":"malformed"}',
            expectedTag: "MalformedRequestError",
          },
          {
            name: "authentication failure",
            status: 401,
            body: '{"error":"authentication_failed"}',
            expectedTag: "AuthenticationFailedError",
          },
          {
            name: "route not found",
            status: 404,
            body: '{"error":"not_found"}',
            expectedTag: "RouteNotFoundError",
          },
          {
            name: "method not allowed",
            status: 405,
            body: '{"error":"method_not_allowed"}',
            expectedTag: "MethodNotAllowedError",
          },
          {
            name: "payload too large",
            status: 413,
            body: '{"error":"payload_too_large"}',
            expectedTag: "PayloadTooLargeError",
          },
          {
            name: "unsupported media type",
            status: 415,
            body: '{"error":"unsupported_media_type"}',
            expectedTag: "UnsupportedMediaTypeError",
          },
          {
            name: "overloaded",
            status: 429,
            body: '{"error":"overloaded"}',
            expectedTag: "OverloadedError",
          },
          {
            name: "internal failure",
            status: 500,
            body: '{"error":"internal"}',
            expectedTag: "InternalServerError",
          },
          {
            name: "unavailable",
            status: 503,
            body: '{"error":"unavailable"}',
            expectedTag: "UnavailableError",
          },
          {
            name: "mismatched server envelope",
            status: 503,
            body: '{"error":"internal"}',
            expectedTag: "RouterInvalidResponseError",
          },
          {
            name: "undeclared status",
            status: 418,
            body: '{"error":"unavailable"}',
            expectedTag: "RouterInvalidResponseError",
          },
          {
            name: "noncanonical success",
            status: 200,
            body: '{ "kind":"cursor_invalid" }',
            expectedTag: "RouterInvalidResponseError",
          },
        ] as const;
        for (const scenario of responseCases) {
          const client = HttpClient.make((request) =>
            Effect.succeed(
              HttpClientResponse.fromWeb(
                request,
                new Response(scenario.body, {
                  status: scenario.status,
                  headers: { "content-type": "application/json" },
                }),
              ),
            ),
          );
          const outcome = yield* runPollWithClient(
            client,
            signingAuthority,
            Duration.seconds(5),
          );
          const tag = Either.match(outcome, {
            onLeft: (error) => error._tag,
            onRight: () => "unexpected_success",
          });
          expect(tag, scenario.name).toBe(scenario.expectedTag);
        }

        const invalidByteResponseCases = [
          {
            name: "success response with a UTF-8 byte-order mark",
            status: 200,
            body: withUtf8Bom('{"kind":"cursor_invalid"}'),
          },
          {
            name: "success response with malformed UTF-8",
            status: 200,
            body: withMalformedUtf8Suffix('{"kind":"cursor_invalid"}'),
          },
          {
            name: "error response with a UTF-8 byte-order mark",
            status: 503,
            body: withUtf8Bom('{"error":"unavailable"}'),
          },
          {
            name: "error response with malformed UTF-8",
            status: 503,
            body: withMalformedUtf8Suffix('{"error":"unavailable"}'),
          },
        ] as const;
        for (const scenario of invalidByteResponseCases) {
          const client = HttpClient.make((request) =>
            Effect.succeed(
              HttpClientResponse.fromWeb(
                request,
                new Response(scenario.body, {
                  status: scenario.status,
                  headers: { "content-type": "application/json" },
                }),
              ),
            ),
          );
          const outcome = yield* runPollWithClient(
            client,
            signingAuthority,
            Duration.seconds(5),
          );
          const tag = Either.match(outcome, {
            onLeft: (error) => error._tag,
            onRight: () => "unexpected_success",
          });
          expect(tag, scenario.name).toBe("RouterInvalidResponseError");
        }

        for (const status of [502, 503, 504]) {
          const client = HttpClient.make((request) =>
            Effect.succeed(
              HttpClientResponse.fromWeb(
                request,
                new Response("<html><body>Bad Gateway</body></html>", {
                  status,
                  headers: { "content-type": "text/html" },
                }),
              ),
            ),
          );
          const outcome = yield* runPollWithClient(
            client,
            signingAuthority,
            Duration.seconds(5),
          );
          const tag = Either.match(outcome, {
            onLeft: (error) => error._tag,
            onRight: () => "unexpected_success",
          });
          expect(tag, `gateway ${String(status)} page`).toBe(
            "RouterConnectionError",
          );
        }

        const connectionOutcome = yield* runPollWithClient(
          HttpClient.make((request) =>
            Effect.fail(
              new HttpClientError.RequestError({
                request,
                reason: "Transport",
              }),
            ),
          ),
          signingAuthority,
          Duration.seconds(5),
        );
        expect(
          Option.getOrThrow(Either.getLeft(connectionOutcome)),
        ).toBeInstanceOf(RouterConnectionError);

        const timeoutOutcome = yield* runPollWithClient(
          HttpClient.make(() => Effect.never),
          signingAuthority,
          Duration.millis(5),
        );
        expect(
          Option.getOrThrow(Either.getLeft(timeoutOutcome)),
        ).toBeInstanceOf(RouterRequestTimeoutError);

        let requestCount = 0;
        const signingOutcome = yield* Router.poll({
          request: {
            pollCursor: Brand.nominal<PollCursorValue>()("not-a-poll-cursor"),
          },
          callerAgentId: recipient,
          signingAuthority,
        }).pipe(
          Effect.provide(
            Router.layer({
              origin: new URL("http://router.example:3010"),
              sendTimeout: Duration.seconds(5),
              pollTimeout: Duration.seconds(5),
            }).pipe(
              Layer.provide(
                Layer.succeed(
                  HttpClient.HttpClient,
                  HttpClient.make(() => {
                    requestCount += 1;
                    return Effect.die("invalid request reached HTTP");
                  }),
                ),
              ),
            ),
          ),
          Effect.either,
        );
        Either.match(signingOutcome, {
          onLeft: (error) => {
            expect(error).toBeInstanceOf(AgentSigningError);
          },
          onRight: () => {
            expect.fail("invalid local request was signed");
          },
        });
        expect(requestCount).toBe(0);
      }),
    ));
});
/* eslint-enable max-lines-per-function, agent-code-guard/no-hardcoded-assertion-literals -- Restore production defaults after the scenario suite. */
