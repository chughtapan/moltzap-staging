/** @file Signed-request helpers shared by the Authentication tests. */
import { HttpClientRequest, HttpServerRequest } from "@effect/platform";
import { Clock, Effect, Match, Schema } from "effect";
import type { AgentId, VerifiedAgentIdentityCard } from "../../agent.js";
import type { RegistryLookupResult } from "../../registry/contract.js";
import { signAgentRequest } from "../proof.js";
import { Authentication } from "../service.js";
import {
  type IdentityFixture,
  makeIdentityFixture,
} from "./authenticated-http-fixtures.js";

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
export const toServerRequest = (
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

/** A view of `value` that throws on every member read. */
export const unreadable = <Value extends object>(value: Value): Value =>
  new Proxy(value, {
    get: () => {
      throw new Error("unreadable");
    },
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
