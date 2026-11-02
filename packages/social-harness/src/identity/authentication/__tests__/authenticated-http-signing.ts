/** @file RFC 9421 signing through the signature library's public API. */
import { Headers, HttpClientRequest } from "@effect/platform";
import { Clock, Effect } from "effect";
import { httpbis } from "http-message-signatures";
import { randomBytes, webcrypto } from "node:crypto";
import type { IdentityFixture } from "./authenticated-http-fixtures.js";
import {
  AgentSigningAuthority,
  agentSigningPrivateKey,
  ed25519PublicKeyThumbprintUri,
} from "../../keys.js";
import {
  type PreparedRequest,
  toServerRequest,
} from "./authenticated-http-requests.js";

/** Covered components shared by the signing helper and independent verifier. */
export const NORMAL_FIELDS = Object.freeze([
  "@method",
  "@authority",
  "@path",
  "@query",
  "content-digest",
  "content-type",
  "sharness-version",
]);
/** Ordered parameters used to construct complete RFC 9421 signature bases. */
export const SIGNATURE_PARAMETERS = Object.freeze([
  "created",
  "expires",
  "keyid",
  "nonce",
  "alg",
  "tag",
]);
const SIGNATURE_INTERVAL_MILLISECONDS = 300_000;

interface TestSignatureParameters {
  readonly created: Date;
  readonly expires: Date;
  readonly keyid: string;
  readonly nonce: string;
}

const signStandardBytes = (authority: AgentSigningAuthority, bytes: Buffer) =>
  Effect.runPromise(
    Effect.tryPromise({
      try: () =>
        webcrypto.subtle.sign(
          "Ed25519",
          agentSigningPrivateKey(authority),
          Uint8Array.from(bytes),
        ),
      catch: (cause) =>
        new Error("could not sign RFC 9421 test request", { cause }),
    }).pipe(Effect.map((signature) => Buffer.from(signature))),
  );

const makeSignatureParameters = (fixture: IdentityFixture) =>
  Effect.gen(function* () {
    const now = Math.floor((yield* Clock.currentTimeMillis) / 1_000) * 1_000;
    const keyid = yield* ed25519PublicKeyThumbprintUri(
      AgentSigningAuthority.publicKey(fixture.signingAuthority),
    );
    const nonce = yield* Effect.try({
      try: () => randomBytes(16).toString("base64url"),
      catch: (cause) =>
        new Error("could not create RFC 9421 test nonce", { cause }),
    });
    return {
      created: new Date(now),
      expires: new Date(now + SIGNATURE_INTERVAL_MILLISECONDS),
      keyid,
      nonce,
    } satisfies TestSignatureParameters;
  });

const signVersionedRequest = (
  request: HttpClientRequest.HttpClientRequest,
  fixture: IdentityFixture,
  parameters: TestSignatureParameters,
  profileParameters: { readonly alg?: string; readonly tag?: string },
) =>
  Effect.tryPromise({
    try: () =>
      httpbis.signMessage(
        {
          name: "sharness",
          fields: [...NORMAL_FIELDS],
          params: [...SIGNATURE_PARAMETERS],
          paramValues: {
            ...parameters,
            alg: "ed25519",
            tag: "sharness-request-v1",
            ...profileParameters,
          },
          key: {
            id: parameters.keyid,
            alg: "ed25519",
            sign: (bytes) => signStandardBytes(fixture.signingAuthority, bytes),
          },
        },
        {
          method: request.method,
          url: request.url,
          headers: { ...request.headers },
        },
      ),
    catch: (cause) =>
      new Error("could not re-sign version test request", { cause }),
  });

/** Re-signs the complete request, including caller-selected alg and tag parameters. */
export const resignWithVersion = (
  prepared: PreparedRequest,
  fixture: IdentityFixture,
  version: string,
  profileParameters: { readonly alg?: string; readonly tag?: string } = {},
) =>
  Effect.gen(function* () {
    const unsignedHeaders = Headers.remove(prepared.clientRequest.headers, [
      "signature",
      "signature-input",
    ]);
    const request = HttpClientRequest.post(prepared.clientRequest.url, {
      body: prepared.clientRequest.body,
      headers: unsignedHeaders,
    }).pipe(HttpClientRequest.setHeader("sharness-version", version));
    const parameters = yield* makeSignatureParameters(fixture);
    const signed = yield* signVersionedRequest(
      request,
      fixture,
      parameters,
      profileParameters,
    );
    const clientRequest = request.pipe(
      HttpClientRequest.setHeader(
        "signature-input",
        String(signed.headers["Signature-Input"]),
      ),
      HttpClientRequest.setHeader(
        "signature",
        String(signed.headers.Signature),
      ),
    );
    return {
      bodyBytes: prepared.bodyBytes,
      clientRequest,
      serverRequest: toServerRequest(clientRequest),
    } satisfies PreparedRequest;
  }).pipe(Effect.withSpan("resignWithVersion"));
