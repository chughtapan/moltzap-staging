/** @file HTTP proof, replay protection, and bounded identity resolution through Authentication. */

import type { Headers, HttpServerRequest } from "@effect/platform";
import {
  Deferred,
  Duration,
  Effect,
  Either,
  Encoding,
  Fiber,
  Ref,
  Schema,
} from "effect";
import { httpbis, type VerifyingKey } from "http-message-signatures";
import { importJWK } from "jose";
import { webcrypto } from "node:crypto";
import { expect, it } from "vitest";
import { WIRE_VERSION, WireVersions } from "../version.js";
import {
  AgentIdentityCardIssuedAt,
  authenticatedHttpLayerWithRegistry,
  clockAt,
  type IdentityFixture,
  makeIdentityFixture,
  makeVerifiedCard,
  NORMAL_FIELDS,
  type PreparedRequest,
  prepareRequest,
  registryWithLookup,
  resignWithVersion,
  retainedIdentityFixture,
  SIGNATURE_PARAMETERS,
  signatureExpiry,
  verifyAgentRequest,
  verifyThroughLookupResult,
} from "./__tests__/authentication-fixtures.js";
import {
  AgentId,
  AgentName,
  type VerifiedAgentIdentityCard,
} from "./agent-cards.js";
import {
  type Authentication,
  AuthenticationError,
  contentDigest,
  verifyHttpRequestSignature,
} from "./http-authentication.js";
import {
  AgentSigningAuthority,
  ed25519PublicKeyThumbprintUri,
} from "./keys.js";
import { RegistryError, type RegistryLookupResult } from "./registry.js";

/* eslint-disable agent-code-guard/no-example-only-tests -- regression-only suite: each case exercises a distinct HTTP proof, replay-ordering, cache, or cancellation contract. The concurrency timelines are not an input domain to generate over. */

const WRONG_VERSION = "wrong-version";
/** The preceding reset's emitted version must fail live HTTP admission. */
const PRIOR_WIRE_VERSION = "2026.1026.5";
const CONCURRENT_REQUEST_COUNT = 4;
const REGISTRY_LOOKUP_DEADLINE = Duration.millis(20);
const RESOLVER_REGRESSION_DEADLINE = Duration.seconds(2);
const VERIFIED_PROOF_FIELDS = Object.freeze([
  "identityCard",
  "callerAgentId",
  "request",
]);

interface RequestMutation {
  readonly name: string;
  readonly prepared: PreparedRequest;
}

interface ResolverRequests {
  readonly cached: PreparedRequest;
  readonly concurrent: readonly PreparedRequest[];
  readonly unseenAfterRefusal: readonly [PreparedRequest, PreparedRequest];
  readonly unseenDuringOutage: PreparedRequest;
}

type LookupMode =
  | Readonly<{ kind: "found"; identityCard: VerifiedAgentIdentityCard }>
  | Readonly<{ kind: "not_found" }>
  | Readonly<{ kind: "unavailable" }>;

interface LookupController {
  readonly count: Ref.Ref<number>;
  readonly mode: Ref.Ref<LookupMode>;
  readonly release: Deferred.Deferred<undefined>;
  readonly started: Deferred.Deferred<undefined>;
}

const identifier = (prefix: string, byte: number): string =>
  `${prefix}${Encoding.encodeBase64Url(new Uint8Array(16).fill(byte))}`;

const makeAgentId = (byte: number) =>
  Schema.decodeUnknownSync(AgentId)(identifier("agt_", byte));

const verifySignature = (prepared: PreparedRequest, fixture: IdentityFixture) =>
  verifyHttpRequestSignature({
    httpRequest: prepared.serverRequest,
    bodyBytes: prepared.bodyBytes,
    publicKey: AgentSigningAuthority.publicKey(fixture.signingAuthority),
    profile: "normal",
  });

const makeLookupController = (mode: LookupMode) =>
  Effect.gen(function* () {
    const count = yield* Ref.make(0);
    const modeRef = yield* Ref.make(mode);
    const release = yield* Deferred.make<undefined>();
    const started = yield* Deferred.make<undefined>();
    return {
      count,
      mode: modeRef,
      release,
      started,
    } satisfies LookupController;
  });

const lookupAgentCard = (controller: LookupController, agentId: AgentId) =>
  Effect.gen(function* () {
    yield* Ref.update(controller.count, (count) => count + 1);
    yield* Deferred.succeed(controller.started, undefined);
    yield* Deferred.await(controller.release);
    const mode = yield* Ref.get(controller.mode);
    if (mode.kind === "unavailable") {
      return yield* new RegistryError({ reason: "connection" });
    }
    if (mode.kind === "not_found" || mode.identityCard.agentId !== agentId) {
      return { kind: "not_found" } as const;
    }
    return { kind: "found", identityCard: mode.identityCard } as const;
  });

const registryService = (controller: LookupController) =>
  registryWithLookup((request) =>
    "agentId" in request
      ? lookupAgentCard(controller, request.agentId)
      : Effect.dieMessage("the request under test must select an AgentId"),
  );

const deadlineRegistryService = (controller: LookupController) =>
  registryWithLookup((request) =>
    "agentId" in request
      ? Effect.gen(function* () {
          yield* Ref.update(controller.count, (count) => count + 1);
          yield* Deferred.succeed(controller.started, undefined);
          yield* Deferred.await(controller.release);
          return yield* Effect.never.pipe(
            Effect.timeoutFail({
              duration: REGISTRY_LOOKUP_DEADLINE,
              onTimeout: () => new RegistryError({ reason: "connection" }),
            }),
          );
        })
      : Effect.dieMessage("the request under test must select an AgentId"),
  );

const authenticatedHttpLayer = (controller: LookupController) =>
  authenticatedHttpLayerWithRegistry(registryService(controller));

const releaseLookups = (controller: LookupController) =>
  Deferred.succeed(controller.release, undefined).pipe(Effect.asVoid);

const makeOracleKey = (keyid: string, publicKey: CryptoKey): VerifyingKey => ({
  id: keyid,
  algs: ["ed25519"],
  verify: (bytes, signature) =>
    Effect.runPromise(
      Effect.tryPromise({
        try: () =>
          webcrypto.subtle.verify(
            "Ed25519",
            publicKey,
            Uint8Array.from(signature),
            Uint8Array.from(bytes),
          ),
        catch: (cause) =>
          new Error("RFC 9421 oracle verification failed", { cause }),
      }),
    ),
});

const oracleVerifies = (prepared: PreparedRequest, fixture: IdentityFixture) =>
  Effect.gen(function* () {
    const publicJwk = AgentSigningAuthority.publicKey(fixture.signingAuthority);
    const [publicKey, keyid] = yield* Effect.all(
      [
        Effect.tryPromise({
          try: () => importJWK(publicJwk, "Ed25519"),
          catch: (cause) =>
            new Error("could not import oracle public key", { cause }),
        }),
        ed25519PublicKeyThumbprintUri(publicJwk),
      ] as const,
      { concurrency: 2 },
    );
    const key = makeOracleKey(keyid, publicKey);
    const verified = yield* Effect.tryPromise({
      try: () =>
        httpbis.verifyMessage(
          {
            all: true,
            requiredFields: [...NORMAL_FIELDS],
            requiredParams: [...SIGNATURE_PARAMETERS],
            keyLookup: () => Effect.runPromise(Effect.succeed(key)),
          },
          {
            method: prepared.clientRequest.method,
            url: new URL(prepared.clientRequest.url),
            headers: { ...prepared.clientRequest.headers },
          },
        ),
      catch: (cause) =>
        new Error("RFC 9421 oracle rejected the request", { cause }),
    });
    return verified === true;
  });

const withServerRequest = (
  prepared: PreparedRequest,
  serverRequest: HttpServerRequest.HttpServerRequest,
): PreparedRequest => ({ ...prepared, serverRequest });

const changeSignature = (value: string): string => {
  const firstByteIndex = value.indexOf(":") + 1;
  if (firstByteIndex <= 0 || firstByteIndex >= value.length) {
    return `${value}:changed`;
  }
  const current = value[firstByteIndex];
  const replacement = current === "A" ? "B" : "A";
  return (
    value.slice(0, firstByteIndex) +
    replacement +
    value.slice(firstByteIndex + 1)
  );
};

const universalSignatureHeader = (): string => {
  const signature = new Uint8Array(64);
  signature[0] = 1;
  return `sharness=:${Buffer.from(signature).toString("base64")}:`;
};

const signatureMutations = (
  prepared: PreparedRequest,
  headers: Headers.Headers,
): readonly RequestMutation[] => [
  {
    name: "signature bytes",
    prepared: withServerRequest(
      prepared,
      prepared.serverRequest.modify({
        headers: {
          ...headers,
          signature: changeSignature(headers.signature ?? ""),
        },
      }),
    ),
  },
  {
    name: "identity-point signature",
    prepared: withServerRequest(
      prepared,
      prepared.serverRequest.modify({
        headers: {
          ...headers,
          signature: universalSignatureHeader(),
        },
      }),
    ),
  },
];

const withSignatureInput = (
  prepared: PreparedRequest,
  signatureInput: string,
): PreparedRequest =>
  withServerRequest(
    prepared,
    prepared.serverRequest.modify({
      headers: {
        ...prepared.serverRequest.headers,
        "signature-input": signatureInput,
      },
    }),
  );

const withSignature = (
  prepared: PreparedRequest,
  signature: string,
): PreparedRequest =>
  withServerRequest(
    prepared,
    prepared.serverRequest.modify({
      headers: {
        ...prepared.serverRequest.headers,
        signature,
      },
    }),
  );

const requiredHeader = (headers: Headers.Headers, name: string): string => {
  const value = headers[name];
  if (value === undefined) {
    throw new Error(`signed request is missing ${name}`);
  }
  return value;
};

const structuredFieldDuplicateMutations = (
  prepared: PreparedRequest,
): readonly RequestMutation[] => {
  const headers = prepared.serverRequest.headers;
  const signatureInput = requiredHeader(headers, "signature-input");
  const signature = requiredHeader(headers, "signature");
  return [
    {
      name: "created parameter",
      prepared: withSignatureInput(
        prepared,
        signatureInput.replace(";created=", ";created=0;created="),
      ),
    },
    {
      name: "nonce parameter",
      prepared: withSignatureInput(
        prepared,
        signatureInput.replace(
          ";nonce=",
          ';nonce="AAAAAAAAAAAAAAAAAAAAAA";nonce=',
        ),
      ),
    },
    {
      name: "Signature-Input sharness label",
      prepared: withSignatureInput(
        prepared,
        `sharness=("@method");created=0, ${signatureInput}`,
      ),
    },
    {
      name: "Signature sharness label",
      prepared: withSignature(
        prepared,
        `${universalSignatureHeader()}, ${signature}`,
      ),
    },
  ];
};

const coveredMutations = (
  prepared: PreparedRequest,
): ReadonlyArray<
  Readonly<{
    name: string;
    prepared: PreparedRequest;
  }>
> => {
  const headers = prepared.serverRequest.headers;
  return [
    {
      name: "covered path",
      prepared: withServerRequest(
        prepared,
        prepared.serverRequest.modify({
          url: "/v1/messages:changed?view=current",
        }),
      ),
    },
    {
      name: "content digest",
      prepared: withServerRequest(
        prepared,
        prepared.serverRequest.modify({
          headers: {
            ...headers,
            "content-digest": contentDigest(Uint8Array.from([9])),
          },
        }),
      ),
    },
    ...signatureMutations(prepared, headers),
  ];
};

const assertCoveredMutationRejected = (
  mutation: RequestMutation,
  fixture: IdentityFixture,
) =>
  Effect.gen(function* () {
    const result = yield* verifySignature(mutation.prepared, fixture).pipe(
      Effect.either,
    );
    const rejected = Either.match(result, {
      onLeft: () => true,
      onRight: () => false,
    });
    expect(rejected, mutation.name).toBe(true);
  });

const assertAuthenticationMutationRejected = (mutation: RequestMutation) =>
  Effect.gen(function* () {
    const failure = yield* verifyAgentRequest(mutation.prepared).pipe(
      Effect.flip,
    );
    expect(failure, mutation.name).toStrictEqual(
      new AuthenticationError({ reason: "authentication-failed" }),
    );
  });

it("produces an RFC 9421 proof and rejects covered-field mutations", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const fixture = yield* makeIdentityFixture;
      const controller = yield* makeLookupController({
        kind: "found",
        identityCard: fixture.identityCard,
      });
      yield* releaseLookups(controller);
      const encodedRequest = Object.freeze({ pollCursor: "cursor" });
      const prepared = yield* prepareRequest({
        fixture,
        encodedRequest,
      });

      expect(yield* oracleVerifies(prepared, fixture)).toBe(true);
      yield* verifySignature(prepared, fixture);
      yield* Effect.forEach(
        coveredMutations(prepared),
        (mutation) => assertCoveredMutationRejected(mutation, fixture),
        { concurrency: 1, discard: true },
      );

      const proof = yield* verifyAgentRequest(prepared).pipe(
        Effect.provide(authenticatedHttpLayer(controller)),
      );
      expect(new Set(Reflect.ownKeys(proof).map(String))).toStrictEqual(
        new Set(VERIFIED_PROOF_FIELDS),
      );
      expect(Object.isFrozen(proof)).toBe(true);
      expect(proof.callerAgentId).toBe(fixture.identityCard.agentId);
      expect(proof.identityCard).toBe(fixture.identityCard);
      expect(proof.request).toEqual(encodedRequest);
    }),
  ));

it("rejects duplicate structured signature members without claiming the nonce", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const fixture = yield* makeIdentityFixture;
      const controller = yield* makeLookupController({
        kind: "found",
        identityCard: fixture.identityCard,
      });
      yield* releaseLookups(controller);
      const prepared = yield* prepareRequest({ fixture });
      const verifyDuplicates = Effect.gen(function* () {
        yield* Effect.forEach(
          structuredFieldDuplicateMutations(prepared),
          assertAuthenticationMutationRejected,
          { concurrency: 1, discard: true },
        );
        return yield* verifyAgentRequest(prepared);
      });
      const proof = yield* verifyDuplicates.pipe(
        Effect.provide(authenticatedHttpLayer(controller)),
      );
      expect(proof.callerAgentId).toBe(fixture.identityCard.agentId);
    }),
  ));

it("refuses a validly signed request tagged for the registration profile", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const fixture = yield* makeIdentityFixture;
      const prepared = yield* prepareRequest({ fixture });
      const signed = yield* resignWithVersion(prepared, fixture, WIRE_VERSION, {
        tag: "sharness-registration-v1",
      });
      const refusal = yield* verifyAgentRequest(signed).pipe(
        Effect.flip,
        Effect.provide(
          authenticatedHttpLayerWithRegistry(
            registryWithLookup(() =>
              Effect.succeed({
                kind: "found",
                identityCard: fixture.identityCard,
              }),
            ),
          ),
        ),
      );
      expect(refusal).toStrictEqual(
        new AuthenticationError({ reason: "authentication-failed" }),
      );
    }),
  ));

/** The signature library also refuses a foreign alg even without the package guard. */
it("refuses a validly signed request whose alg is not ed25519", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const fixture = yield* makeIdentityFixture;
      const prepared = yield* prepareRequest({ fixture });
      const signed = yield* resignWithVersion(prepared, fixture, WIRE_VERSION, {
        alg: "rsa-pss-sha512",
      });
      const refusal = yield* verifyAgentRequest(signed).pipe(
        Effect.flip,
        Effect.provide(
          authenticatedHttpLayerWithRegistry(
            registryWithLookup(() =>
              Effect.succeed({
                kind: "found",
                identityCard: fixture.identityCard,
              }),
            ),
          ),
        ),
      );
      expect(refusal).toStrictEqual(
        new AuthenticationError({ reason: "authentication-failed" }),
      );
    }),
  ));

it("claims a valid wrong-version nonce before returning version mismatch", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const fixture = yield* makeIdentityFixture;
      const controller = yield* makeLookupController({
        kind: "found",
        identityCard: fixture.identityCard,
      });
      yield* releaseLookups(controller);
      const prepared = yield* prepareRequest({ fixture });
      const wrongVersion = yield* resignWithVersion(
        prepared,
        fixture,
        WRONG_VERSION,
      );
      const verifyTwice = Effect.gen(function* () {
        const first = yield* verifyAgentRequest(wrongVersion).pipe(Effect.flip);
        expect(first).toMatchObject({ reason: "version-mismatch" });
        const replay = yield* verifyAgentRequest(wrongVersion).pipe(
          Effect.flip,
        );
        expect(replay).toMatchObject({ reason: "authentication-failed" });
      });
      yield* verifyTwice.pipe(
        Effect.provide(authenticatedHttpLayer(controller)),
      );
    }),
  ));

it("returns version mismatch to a peer still on the prior Social Harness version", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const fixture = yield* makeIdentityFixture;
      const controller = yield* makeLookupController({
        kind: "found",
        identityCard: fixture.identityCard,
      });
      yield* releaseLookups(controller);
      const prepared = yield* prepareRequest({ fixture });
      const priorVersion = yield* resignWithVersion(
        prepared,
        fixture,
        PRIOR_WIRE_VERSION,
      );

      const refusal = yield* verifyAgentRequest(priorVersion).pipe(
        Effect.flip,
        Effect.provide(authenticatedHttpLayer(controller)),
      );

      expect(refusal).toMatchObject({ reason: "version-mismatch" });
      expect(refusal).toMatchObject({ reason: "version-mismatch" });
    }),
  ));

const verifyInvalidLookupRecovery = (
  lookupResult: (
    identityCard: VerifiedAgentIdentityCard,
  ) => RegistryLookupResult,
) =>
  Effect.gen(function* () {
    const fixture = yield* makeIdentityFixture;
    const prepared = yield* prepareRequest({ fixture });
    const result = yield* Ref.make(lookupResult(fixture.identityCard));
    yield* Effect.gen(function* () {
      const failures = yield* Effect.all(
        [
          verifyAgentRequest(prepared).pipe(Effect.flip),
          verifyAgentRequest(prepared).pipe(Effect.flip),
        ],
        { concurrency: 2 },
      );
      expect(failures).toStrictEqual([
        new AuthenticationError({ reason: "unavailable" }),
        new AuthenticationError({ reason: "unavailable" }),
      ]);

      yield* Ref.set(result, {
        kind: "found",
        identityCard: fixture.identityCard,
      });
      const verified = yield* verifyAgentRequest(prepared);
      expect(verified.identityCard).toBe(fixture.identityCard);
    }).pipe(
      Effect.provide(
        authenticatedHttpLayerWithRegistry(
          registryWithLookup(() => Ref.get(result)),
          { registryLookupConcurrencyLimit: 1 },
        ),
      ),
      Effect.timeoutFail({
        duration: RESOLVER_REGRESSION_DEADLINE,
        onTimeout: () => new Error("Registry recovery did not settle"),
      }),
    );
  });

/** A readable card copy has the caller's key but lacks package verification. */
it.each([
  {
    result: "a copy of an AgentIdentityCard this package did not verify",
    lookupResult: (
      identityCard: VerifiedAgentIdentityCard,
    ): RegistryLookupResult => ({
      kind: "found",
      identityCard: Object.freeze(Object.assign({}, identityCard)),
    }),
  },
  {
    result: "an unknown result kind",
    lookupResult: (
      identityCard: VerifiedAgentIdentityCard,
    ): RegistryLookupResult =>
      Object.defineProperty({ kind: "found", identityCard }, "kind", {
        value: "unexpected",
      }),
  },
])(
  "reports unavailable and recovers after the Registry returns $result",
  ({ lookupResult }) =>
    Effect.runPromise(verifyInvalidLookupRecovery(lookupResult)),
);

it("reports unavailable when the Registry answers with another agent's verified card", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const other = yield* makeIdentityFixture;
      const prepared = yield* prepareRequest({
        fixture: other,
        callerAgentId: makeAgentId(9),
      });

      const refusal = yield* Effect.flip(
        verifyAgentRequest(prepared).pipe(
          Effect.provide(
            authenticatedHttpLayerWithRegistry(
              registryWithLookup(() =>
                Effect.succeed({
                  kind: "found",
                  identityCard: other.identityCard,
                }),
              ),
            ),
          ),
        ),
      );

      expect(refusal).toStrictEqual(
        new AuthenticationError({ reason: "unavailable" }),
      );
    }),
  ));

it("authenticates when the Registry's result carries members beyond its type", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const { fixture, verified } = yield* verifyThroughLookupResult(
        (identityCard) => ({ kind: "found", identityCard, etag: "1" }),
      );

      expect(verified.identityCard).toBe(fixture.identityCard);
    }),
  ));

it.each([-1, 0])("accepts at %i s from the signature expiry", (offsetSeconds) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const fixture = yield* makeIdentityFixture;
      const prepared = yield* prepareRequest({ fixture });
      const verification = verifyAgentRequest(prepared).pipe(
        Effect.provide(
          authenticatedHttpLayerWithRegistry(
            registryWithLookup(() =>
              Effect.succeed({
                kind: "found",
                identityCard: fixture.identityCard,
              }),
            ),
          ),
        ),
        Effect.withClock(
          clockAt((signatureExpiry(prepared) + offsetSeconds) * 1_000),
        ),
      );
      expect((yield* verification).callerAgentId).toBe(
        fixture.identityCard.agentId,
      );
    }),
  ),
);

it("refuses one second after the signature expiry", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const fixture = yield* makeIdentityFixture;
      const prepared = yield* prepareRequest({ fixture });
      const verification = verifyAgentRequest(prepared).pipe(
        Effect.provide(
          authenticatedHttpLayerWithRegistry(
            registryWithLookup(() =>
              Effect.succeed({
                kind: "found",
                identityCard: fixture.identityCard,
              }),
            ),
          ),
        ),
        Effect.withClock(clockAt((signatureExpiry(prepared) + 1) * 1_000)),
      );
      expect(yield* Effect.flip(verification)).toStrictEqual(
        new AuthenticationError({ reason: "authentication-failed" }),
      );
    }),
  ));

it("refuses a nonce replay at exactly the signature expiry second", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const fixture = yield* makeIdentityFixture;
      const prepared = yield* prepareRequest({ fixture });
      yield* Effect.gen(function* () {
        const accepted = yield* verifyAgentRequest(prepared);
        expect(accepted.callerAgentId).toBe(fixture.identityCard.agentId);
        expect(yield* Effect.flip(verifyAgentRequest(prepared))).toStrictEqual(
          new AuthenticationError({ reason: "authentication-failed" }),
        );
      }).pipe(
        Effect.provide(
          authenticatedHttpLayerWithRegistry(
            registryWithLookup(() =>
              Effect.succeed({
                kind: "found",
                identityCard: fixture.identityCard,
              }),
            ),
          ),
        ),
        Effect.withClock(clockAt(signatureExpiry(prepared) * 1_000)),
      );
    }),
  ));

const prepareResolverRequests = (fixture: IdentityFixture) =>
  Effect.gen(function* () {
    const concurrent = yield* Effect.all(
      Array.from({ length: CONCURRENT_REQUEST_COUNT }, () =>
        prepareRequest({ fixture }),
      ),
      { concurrency: CONCURRENT_REQUEST_COUNT },
    );
    const cached = yield* prepareRequest({ fixture });
    const unseenDuringOutage = yield* prepareRequest({
      fixture,
      callerAgentId: makeAgentId(20),
    });
    const unseenAfterRefusal = yield* Effect.all(
      [
        prepareRequest({
          fixture,
          callerAgentId: makeAgentId(21),
        }),
        prepareRequest({
          fixture,
          callerAgentId: makeAgentId(21),
        }),
      ] as const,
      { concurrency: 2 },
    );
    return {
      cached,
      concurrent,
      unseenAfterRefusal,
      unseenDuringOutage,
    } satisfies ResolverRequests;
  });

const assertInitialSingleFlight = (
  controller: LookupController,
  requests: readonly PreparedRequest[],
) =>
  Effect.gen(function* () {
    const fiber = yield* Effect.fork(
      Effect.forEach(requests, verifyAgentRequest, {
        concurrency: CONCURRENT_REQUEST_COUNT,
      }),
    );
    yield* Deferred.await(controller.started);
    yield* Effect.yieldNow();
    yield* releaseLookups(controller);
    expect(yield* Fiber.join(fiber)).toHaveLength(requests.length);
    expect(yield* Ref.get(controller.count)).toBe(1);
  });

const assertCacheAndFailureBehavior = (
  controller: LookupController,
  requests: ResolverRequests,
) =>
  Effect.gen(function* () {
    yield* Ref.set(controller.mode, { kind: "unavailable" });
    yield* verifyAgentRequest(requests.cached);
    expect(yield* Ref.get(controller.count)).toBe(1);
    const unavailable = yield* verifyAgentRequest(
      requests.unseenDuringOutage,
    ).pipe(Effect.flip);
    expect(unavailable).toStrictEqual(
      new AuthenticationError({ reason: "unavailable" }),
    );

    yield* Ref.set(controller.mode, { kind: "not_found" });
    const countBeforeRefusals = yield* Ref.get(controller.count);
    for (const request of requests.unseenAfterRefusal) {
      const refused = yield* verifyAgentRequest(request).pipe(Effect.flip);
      expect(refused).toStrictEqual(
        new AuthenticationError({ reason: "authentication-failed" }),
      );
    }
    expect(yield* Ref.get(controller.count)).toBe(
      countBeforeRefusals + requests.unseenAfterRefusal.length,
    );
  });

const assertResolverBehavior = (
  controller: LookupController,
  requests: ResolverRequests,
) =>
  Effect.gen(function* () {
    yield* assertInitialSingleFlight(controller, requests.concurrent);
    yield* assertCacheAndFailureBehavior(controller, requests);
  });

const assertLookupDeadlineBehavior = (
  controller: LookupController,
  requests: readonly [PreparedRequest, PreparedRequest, PreparedRequest],
) =>
  Effect.gen(function* () {
    const leadingFiber = yield* Effect.fork(
      verifyAgentRequest(requests[0]).pipe(Effect.flip),
    );
    yield* Deferred.await(controller.started);
    const joiningFiber = yield* Effect.fork(
      verifyAgentRequest(requests[1]).pipe(Effect.flip),
    );
    yield* Effect.yieldNow();
    yield* releaseLookups(controller);

    const [leadingFailure, joiningFailure] = yield* Effect.all(
      [Fiber.join(leadingFiber), Fiber.join(joiningFiber)] as const,
      { concurrency: 2 },
    );
    expect(leadingFailure).toStrictEqual(
      new AuthenticationError({ reason: "unavailable" }),
    );
    expect(joiningFailure).toStrictEqual(
      new AuthenticationError({ reason: "unavailable" }),
    );
    expect(yield* Ref.get(controller.count)).toBe(1);

    const laterFailure = yield* verifyAgentRequest(requests[2]).pipe(
      Effect.flip,
    );
    expect(laterFailure).toStrictEqual(
      new AuthenticationError({ reason: "unavailable" }),
    );
    expect(yield* Ref.get(controller.count)).toBe(2);
  });

it("single-flights positive resolution without caching failures", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const fixture = yield* makeIdentityFixture;
      const controller = yield* makeLookupController({
        kind: "found",
        identityCard: fixture.identityCard,
      });
      const requests = yield* prepareResolverRequests(fixture);
      yield* assertResolverBehavior(controller, requests).pipe(
        Effect.provide(authenticatedHttpLayer(controller)),
      );
    }),
  ));

it("propagates Registry lookup deadlines and releases resolver state", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const fixture = yield* makeIdentityFixture;
      const controller = yield* makeLookupController({
        kind: "unavailable",
      });
      const requests = yield* Effect.all(
        [
          prepareRequest({ fixture }),
          prepareRequest({ fixture }),
          prepareRequest({ fixture }),
        ] as const,
        { concurrency: 3 },
      );
      yield* assertLookupDeadlineBehavior(controller, requests).pipe(
        Effect.provide(
          authenticatedHttpLayerWithRegistry(
            deadlineRegistryService(controller),
            { registryLookupConcurrencyLimit: 1 },
          ),
        ),
        Effect.timeoutFail({
          duration: RESOLVER_REGRESSION_DEADLINE,
          onTimeout: () => new Error("Registry lookup deadline did not settle"),
        }),
      );
    }),
  ));

const increment = (count: number) => count + 1;

const registryThrowingOnFirstLookup = (
  identityCard: VerifiedAgentIdentityCard,
  calls: Ref.Ref<number>,
) => {
  return registryWithLookup(() => {
    const call = Effect.runSync(Ref.getAndUpdate(calls, increment));
    if (call === 0) {
      throw new Error("lookup threw before returning an Effect");
    }
    return Effect.succeed({ kind: "found", identityCard });
  });
};

it("resolves a caller again after a Registry lookup throws synchronously", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const fixture = yield* makeIdentityFixture;
      const calls = yield* Ref.make(0);
      const registry = registryThrowingOnFirstLookup(
        fixture.identityCard,
        calls,
      );
      const [first, second] = yield* Effect.all([
        prepareRequest({ fixture }),
        prepareRequest({ fixture }),
      ]);
      const outcome = yield* Effect.gen(function* () {
        const refusal = yield* Effect.flip(verifyAgentRequest(first));
        const verified = yield* verifyAgentRequest(second);
        return { refusal, verified };
      }).pipe(
        Effect.provide(authenticatedHttpLayerWithRegistry(registry)),
        Effect.timeoutFail({
          duration: RESOLVER_REGRESSION_DEADLINE,
          onTimeout: () => new Error("the caller's resolution never settled"),
        }),
      );

      expect(outcome.refusal).toStrictEqual(
        new AuthenticationError({ reason: "unavailable" }),
      );
      expect(outcome.verified.identityCard).toBe(fixture.identityCard);
    }),
  ));

it("keeps a shared card lookup alive when its first waiter is interrupted", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const fixture = yield* makeIdentityFixture;
      const controller = yield* makeLookupController({
        kind: "found",
        identityCard: fixture.identityCard,
      });
      const [leadingRequest, joiningRequest, cachedRequest] = yield* Effect.all(
        [
          prepareRequest({ fixture }),
          prepareRequest({ fixture }),
          prepareRequest({ fixture }),
        ] as const,
        { concurrency: 3 },
      );
      const verifyCancellationBehavior = Effect.gen(function* () {
        const leadingFiber = yield* Effect.fork(
          verifyAgentRequest(leadingRequest),
        );
        yield* Deferred.await(controller.started);
        const joiningFiber = yield* Effect.fork(
          verifyAgentRequest(joiningRequest),
        );
        yield* Effect.yieldNow();

        yield* Fiber.interrupt(leadingFiber);
        yield* releaseLookups(controller);

        const joined = yield* Fiber.join(joiningFiber);
        expect(joined.identityCard).toBe(fixture.identityCard);
        const cached = yield* verifyAgentRequest(cachedRequest);
        expect(cached.identityCard).toBe(fixture.identityCard);
        expect(yield* Ref.get(controller.count)).toBe(1);
      });
      yield* verifyCancellationBehavior.pipe(
        Effect.provide(authenticatedHttpLayer(controller)),
      );
    }),
  ));

/* eslint-enable agent-code-guard/no-example-only-tests -- Restore strict defaults after the scoped file-level exception. */

/** The Registry signs the retained card version; the live HTTP proof uses the current version. */
const authenticatesRetainedCard = Effect.gen(function* () {
  const fixture = yield* retainedIdentityFixture.pipe(
    Effect.provideService(WireVersions, {
      live: [WIRE_VERSION],
      history: ["2099.101.0", WIRE_VERSION],
    }),
  );
  const identityCard = fixture.identityCard;
  const controller = yield* makeLookupController({
    kind: "found",
    identityCard,
  });
  yield* releaseLookups(controller);
  const prepared = yield* prepareRequest({
    fixture: { ...fixture, identityCard },
  });
  const verified = yield* verifyAgentRequest(prepared).pipe(
    Effect.provide(authenticatedHttpLayer(controller)),
  );
  return { verified, identityCard };
});

it("authenticates a history-only AgentIdentityCard on a live HTTP request", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const { verified, identityCard } = yield* authenticatesRetainedCard;
      expect(verified.callerAgentId).toBe(identityCard.agentId);
    }),
  ));

const makeDistinctFixture = (index: number) =>
  Effect.gen(function* () {
    const fixture = yield* makeIdentityFixture;
    const identityCard = yield* makeVerifiedCard({
      agentId: yield* Schema.decodeUnknown(AgentId)(
        `agt_${Encoding.encodeBase64Url(new Uint8Array(16).fill(index))}`,
      ),
      principalId: fixture.identityCard.principalId,
      agentName: yield* Schema.decodeUnknown(AgentName)(
        `cache-signer-${index}`,
      ),
      publicKey: fixture.identityCard.publicKey,
      issuedAt: yield* Schema.decodeUnknown(AgentIdentityCardIssuedAt)(
        fixture.identityCard.issuedAt,
      ),
      registrySigningAuthority: fixture.registryAuthority,
    });
    return { ...fixture, identityCard };
  });

const lookupCards =
  (
    fixtures: readonly IdentityFixture[],
    lookups: Ref.Ref<readonly AgentId[]>,
  ): Parameters<typeof Authentication.layer>[0]["lookup"] =>
  (request) => {
    if (!("agentId" in request)) {
      return Effect.dieMessage("LRU lookup requires an AgentId");
    }
    const { agentId } = request;
    return Ref.update(lookups, (ids) => [...ids, agentId]).pipe(
      Effect.map(() => {
        const fixture = fixtures.find(
          (candidate) => candidate.identityCard.agentId === agentId,
        );
        return fixture === undefined
          ? ({ kind: "not_found" } as const)
          : ({ kind: "found", identityCard: fixture.identityCard } as const);
      }),
    );
  };

const makeCacheFixtures = Effect.gen(function* () {
  const cacheCapacity = 8;
  const fixtures = yield* Effect.forEach(
    Array.from({ length: cacheCapacity + 1 }).keys(),
    (index) => makeDistinctFixture(index),
    { concurrency: 1 },
  );
  const first = fixtures[0];
  const second = fixtures[1];
  const newest = fixtures[cacheCapacity];
  if (first === undefined || second === undefined || newest === undefined) {
    return yield* Effect.dieMessage("LRU fixtures are missing");
  }
  return { cacheCapacity, fixtures, first, second, newest };
});

it("retains a recently used card and looks up the least recently used card after eviction", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const { cacheCapacity, fixtures, first, second, newest } =
        yield* makeCacheFixtures;
      const lookups = yield* Ref.make<readonly AgentId[]>([]);
      const verifyFresh = (fixture: IdentityFixture) =>
        prepareRequest({ fixture }).pipe(Effect.flatMap(verifyAgentRequest));
      yield* Effect.gen(function* () {
        yield* Effect.forEach(fixtures.slice(0, cacheCapacity), verifyFresh, {
          concurrency: 1,
        });
        yield* verifyFresh(first);
        yield* verifyFresh(newest);
        yield* Ref.set(lookups, []);
        expect((yield* verifyFresh(first)).identityCard).toBe(
          first.identityCard,
        );
        expect(yield* Ref.get(lookups)).toStrictEqual([]);
        expect((yield* verifyFresh(second)).identityCard).toBe(
          second.identityCard,
        );
        expect(yield* Ref.get(lookups)).toStrictEqual([
          second.identityCard.agentId,
        ]);
      }).pipe(
        Effect.provide(
          authenticatedHttpLayerWithRegistry(
            registryWithLookup(lookupCards(fixtures, lookups)),
            { identityCardCacheCapacity: cacheCapacity },
          ),
        ),
      );
    }),
  ));
