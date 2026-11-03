/** @file Signed cards and HTTP requests for identity service behavior tests. */

import {
  Headers,
  HttpClientRequest,
  HttpServerRequest,
} from "@effect/platform";
import { Clock, Effect, Encoding, Match, Redacted, Schema } from "effect";
import { httpbis } from "http-message-signatures";
import { GeneralSign } from "jose";
import { generateKeyPairSync, randomBytes, webcrypto } from "node:crypto";
import type { RegistryLookupResult } from "../registry.js";
import {
  decodeCanonicalJson,
  encodeCanonicalJson,
} from "../../encoding/canonical-json.js";
import { wireVersions } from "../../version.js";
import {
  AgentId,
  AgentIdentityCard,
  type AgentIdentityCardClaims,
  agentIdentityCardSigning,
  type AgentIdentityCardVerificationError,
  AgentName,
  PrincipalId,
  type VerifiedAgentIdentityCard,
} from "../agent-cards.js";
import {
  Authentication,
  AuthenticationFailedError,
  signAgentRequest,
} from "../http-authentication.js";
import {
  AgentSigningAuthority,
  agentSigningPrivateKey,
  ed25519PublicKeyThumbprintUri,
} from "../keys.js";

/** Whole-second UTC issuance evidence carried by an AgentIdentityCard. */
// eslint-disable-next-line @typescript-eslint/naming-convention -- Effect Schemas share the package-owned domain name they decode.
export const AgentIdentityCardIssuedAt = agentIdentityCardSigning.issuedAt;
/** Validated nominal value decoded by AgentIdentityCardIssuedAt. */
// eslint-disable-next-line @typescript-eslint/no-redeclare -- The same-named Schema and type form one package boundary model.
export type AgentIdentityCardIssuedAt = typeof AgentIdentityCardIssuedAt.Type;

interface CardFixtureInput
  extends Omit<AgentIdentityCardClaims, "kind" | "sharnessVersion"> {
  readonly registrySigningAuthority: AgentSigningAuthority;
}

/**
 * Signs immutable card claims for Identity fixtures.
 *
 * @param input Complete identity fields and Registry signing authority.
 * @returns A Registry-verified immutable AgentIdentityCard.
 */
export const makeVerifiedCard = (
  input: CardFixtureInput,
): Effect.Effect<
  VerifiedAgentIdentityCard,
  AgentIdentityCardVerificationError
> =>
  Effect.gen(function* () {
    const { registrySigningAuthority, ...claims } = input;
    return yield* agentIdentityCardSigning.sign(
      {
        kind: "agentCard",
        sharnessVersion: yield* wireVersions.emitted,
        ...claims,
      },
      registrySigningAuthority,
    );
  }).pipe(Effect.withSpan("makeVerifiedCard"));

/** Independent Registry and agent signing keys bound by the verified card. */
export interface IdentityFixture {
  readonly registryAuthority: AgentSigningAuthority;
  readonly identityCard: VerifiedAgentIdentityCard;
  readonly signingAuthority: AgentSigningAuthority;
}

const identifier = (prefix: string, byte: number): string =>
  `${prefix}${Encoding.encodeBase64Url(new Uint8Array(16).fill(byte))}`;
const makeAgentId = (byte: number) =>
  Schema.decodeUnknownSync(AgentId)(identifier("agt_", byte));

const makeSigningAuthority = () => {
  const { privateKey } = generateKeyPairSync("ed25519");
  return AgentSigningAuthority.fromPkcs8(
    Redacted.make(privateKey.export({ format: "pem", type: "pkcs8" })),
  );
};

/** Current-version card and both signing authorities for authentic requests. */
export const makeIdentityFixture = Effect.gen(function* () {
  const registryAuthority = yield* makeSigningAuthority();
  const signingAuthority = yield* makeSigningAuthority();
  const identityCard = yield* makeVerifiedCard({
    agentId: makeAgentId(1),
    principalId: Schema.decodeUnknownSync(PrincipalId)(identifier("prn_", 2)),
    agentName: Schema.decodeUnknownSync(AgentName)("http-signer"),
    publicKey: AgentSigningAuthority.publicKey(signingAuthority),
    issuedAt: Schema.decodeUnknownSync(AgentIdentityCardIssuedAt)(
      "2026-07-30T12:00:00Z",
    ),
    registrySigningAuthority: registryAuthority,
  });
  return {
    identityCard,
    signingAuthority,
    registryAuthority,
  } satisfies IdentityFixture;
}).pipe(Effect.withSpan("makeIdentityFixture"));

/** The Registry attests a history-only card without changing the agent key. */
export const retainedIdentityFixture = Effect.gen(function* () {
  const fixture = yield* makeIdentityFixture;
  const encoded = yield* Schema.encode(AgentIdentityCard)(fixture.identityCard);
  const representation = yield* Schema.decodeUnknown(
    Schema.Struct({ payload: Schema.String }),
  )(encoded);
  const bytes = yield* Encoding.decodeBase64Url(representation.payload);
  const fields = yield* decodeCanonicalJson(
    Schema.Record({ key: Schema.String, value: Schema.Unknown }),
    bytes,
  );
  const payload = yield* encodeCanonicalJson({
    ...fields,
    sharnessVersion: "2099.101.0",
  });
  const registrySignerPublicKey = AgentSigningAuthority.publicKey(
    fixture.registryAuthority,
  );
  const kid = yield* ed25519PublicKeyThumbprintUri(registrySignerPublicKey);
  const signed = yield* Effect.tryPromise({
    try: () =>
      new GeneralSign(payload)
        .addSignature(agentSigningPrivateKey(fixture.registryAuthority))
        .setProtectedHeader({
          alg: "Ed25519",
          kid,
          typ: "application/vnd.sharness.agent-card+jws",
        })
        .sign(),
    catch: () => new AuthenticationFailedError(),
  });
  const identityCard = yield* Schema.decodeUnknown(AgentIdentityCard)(
    signed,
  ).pipe(
    Effect.flatMap((identityCard) =>
      AgentIdentityCard.verify({ identityCard, registrySignerPublicKey }),
    ),
  );
  return { ...fixture, identityCard };
}).pipe(Effect.withSpan("retainedIdentityFixture"));

/** The URL every signed test request targets. */
const REQUEST_URL = new URL(
  "http://identity.test/v1/messages:poll?view=current",
);

/** One signed request, as the client sent it and as the server receives it. */
export interface PreparedRequest {
  readonly bodyBytes: Uint8Array;
  readonly clientRequest: HttpClientRequest.HttpClientRequest;
  readonly serverRequest: HttpServerRequest.HttpServerRequest;
}

/** The byte body a signed client request carries. */
const requestBodyBytes = (request: HttpClientRequest.HttpClientRequest) =>
  Match.value(request.body).pipe(
    Match.tag("Uint8Array", (body) =>
      Effect.succeed(Uint8Array.from(body.body)),
    ),
    Match.tag("Empty", "Raw", "FormData", "Stream", () =>
      Effect.dieMessage("signed request did not retain byte body"),
    ),
    Match.exhaustive,
  );

/** The server-side view of a signed client request. */
const toServerRequest = (
  request: HttpClientRequest.HttpClientRequest,
): HttpServerRequest.HttpServerRequest => {
  const url = new URL(request.url);
  const webRequest = new Request(url, {
    method: request.method,
    headers: {
      ...request.headers,
      host: url.host,
    },
  });
  return HttpServerRequest.fromWeb(webRequest);
};

/**
 * Signs a request as `fixture`, claiming `callerAgentId` when given and the
 * fixture card's AgentId otherwise.
 */
export const prepareRequest = (input: {
  readonly fixture: IdentityFixture;
  readonly callerAgentId?: AgentId;
  readonly encodedRequest?: unknown;
}) =>
  Effect.gen(function* () {
    const clientRequest = yield* signAgentRequest({
      httpRequest: HttpClientRequest.post(REQUEST_URL),
      callerAgentId: input.callerAgentId ?? input.fixture.identityCard.agentId,
      encodedRequest: input.encodedRequest ?? { pollCursor: "cursor" },
      signingAuthority: input.fixture.signingAuthority,
    });
    const bodyBytes = yield* requestBodyBytes(clientRequest);
    return {
      bodyBytes,
      clientRequest,
      serverRequest: toServerRequest(clientRequest),
    } satisfies PreparedRequest;
  }).pipe(Effect.withSpan("prepareRequest"));

interface LookupService {
  readonly lookup: Parameters<typeof Authentication.layer>[0]["lookup"];
}

/** A Registry whose only operation under test is `lookup`. */
export const registryWithLookup = (
  lookup: LookupService["lookup"],
): LookupService => ({
  lookup,
});

/** Authentication over `registry`, with small test capacities. */
export const authenticatedHttpLayerWithRegistry = (
  registry: LookupService,
  options: {
    readonly registryLookupConcurrencyLimit?: number;
    readonly identityCardCacheCapacity?: number;
  } = {},
) =>
  Authentication.layer({
    liveNonceCapacity: 64,
    identityCardCacheCapacity: options.identityCardCacheCapacity ?? 8,
    registryLookupConcurrencyLimit: options.registryLookupConcurrencyLimit ?? 4,
    lookup: registry.lookup,
  });

/** Verifies `prepared` as the server would. */
export const verifyAgentRequest = (prepared: PreparedRequest) =>
  Authentication.verifyRequest({
    request: prepared.serverRequest,
    body: prepared.bodyBytes,
  });

/**
 * Verifies a request from a fresh identity through a Registry whose lookup
 * returns what `lookupResult` builds from that identity's verified card.
 */
export const verifyThroughLookupResult = (
  lookupResult: (
    identityCard: VerifiedAgentIdentityCard,
  ) => RegistryLookupResult,
) =>
  Effect.gen(function* () {
    const fixture = yield* makeIdentityFixture;
    const prepared = yield* prepareRequest({ fixture });
    const verified = yield* verifyAgentRequest(prepared).pipe(
      Effect.provide(
        authenticatedHttpLayerWithRegistry(
          registryWithLookup(() =>
            Effect.succeed(lookupResult(fixture.identityCard)),
          ),
        ),
      ),
    );
    return { fixture, verified };
  }).pipe(Effect.withSpan("verifyThroughLookupResult"));

/** Keeps signature time fixed while deadlines use live timers. */
export const clockAt = (milliseconds: number): Clock.Clock => {
  const live = Clock.make();
  return {
    [Clock.ClockTypeId]: Clock.ClockTypeId,
    unsafeCurrentTimeMillis: () => milliseconds,
    currentTimeMillis: Effect.succeed(milliseconds),
    unsafeCurrentTimeNanos: () => BigInt(milliseconds) * 1_000_000n,
    currentTimeNanos: Effect.succeed(BigInt(milliseconds) * 1_000_000n),
    sleep: (duration) => live.sleep(duration),
  };
};

/** Reads the signed expiry so boundary tests follow the request's interval. */
export const signatureExpiry = (prepared: PreparedRequest): number => {
  const signatureInput =
    prepared.serverRequest.headers["signature-input"] ?? "";
  const match = /;expires=(\d+)/u.exec(signatureInput);
  const encodedExpiry = match?.[1];
  if (encodedExpiry === undefined) {
    throw new Error("signed request is missing its expiry");
  }
  const expiry = Schema.NumberFromString.pipe(
    Schema.int(),
    Schema.nonNegative(),
  );
  return Schema.decodeUnknownSync(expiry)(encodedExpiry);
};

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
