/** @file Signs and authenticates HTTP requests made by registered agents. */

import type { HttpServerRequest } from "@effect/platform";
import {
  Brand,
  Cause,
  Context,
  Data,
  Deferred,
  Effect,
  Layer,
  Option,
  Ref,
  Schema,
} from "effect";
import type { RegistryLookupResult } from "./registry.js";
import {
  decodeCanonicalJson,
  exactStruct,
} from "../encoding/canonical-json.js";
import {
  AgentId,
  type VerifiedAgentIdentityCard,
  verifiedAgentIdentityCardSchema,
} from "./agent.js";
import { type BoundedCache, makeBoundedCache } from "./bounded-cache.js";
import {
  AuthenticationFailedError,
  MalformedRequestError,
  OverloadedError,
  UnavailableError,
  verifyHttpRequestSignature,
  VersionMismatchError,
} from "./http-signatures.js";
import { WireVersions } from "./wire-version.js";

/** Request body and caller identity established by Authentication. */
export type VerifiedAgentRequest = Readonly<{
  readonly callerAgentId: AgentId;
  readonly identityCard: VerifiedAgentIdentityCard;
  readonly request: unknown;
}> &
  Brand.Brand<"VerifiedAgentRequest">;

type RequestAuthenticationFailure =
  | MalformedRequestError
  | AuthenticationFailedError
  | VersionMismatchError
  | OverloadedError
  | UnavailableError;

/** Authentication fails before any operation receives a trusted caller. */
export class AuthenticationError extends Data.TaggedError(
  "AuthenticationError",
)<{
  readonly reason:
    | "malformed"
    | "authentication-failed"
    | "version-mismatch"
    | "overloaded"
    | "unavailable";
}> {}

interface AuthenticationService {
  readonly verifyRequest: (input: {
    readonly request: HttpServerRequest.HttpServerRequest;
    readonly body: Uint8Array;
  }) => Effect.Effect<VerifiedAgentRequest, AuthenticationError>;
}

/** Verifies signed request attribution and admits each nonce once. */
export class Authentication extends Context.Tag("SocialHarness/Authentication")<
  Authentication,
  AuthenticationService
>() {
  /** Returns verified caller claims before operation-specific decoding. */
  static readonly verifyRequest = Effect.serviceFunctionEffect(
    Authentication,
    (service) => service.verifyRequest,
  );
  /** Captures a card lookup and finite replay/lookup capacities without acquiring a Registry client. */
  static readonly layer = (
    input: AuthenticationSettings,
  ): Layer.Layer<Authentication> =>
    Layer.effect(Authentication, makeService(input));
}

interface AuthenticationSettings {
  readonly liveNonceCapacity: number;
  readonly identityCardCacheCapacity: number;
  readonly registryLookupConcurrencyLimit: number;
  readonly lookup: RegistryLookup;
}

function authenticationFailure(
  error: RequestAuthenticationFailure,
): AuthenticationError {
  switch (error._tag) {
    case "MalformedRequestError":
      return new AuthenticationError({ reason: "malformed" });
    case "AuthenticationFailedError":
      return new AuthenticationError({ reason: "authentication-failed" });
    case "VersionMismatchError":
      return new AuthenticationError({ reason: "version-mismatch" });
    case "OverloadedError":
      return new AuthenticationError({ reason: "overloaded" });
    case "UnavailableError":
      return new AuthenticationError({ reason: "unavailable" });
    default:
      return error satisfies never;
  }
}

const brandVerifiedAgentRequest = Brand.nominal<VerifiedAgentRequest>();

const registeredAgentBody = exactStruct({
  callerAgentId: AgentId,
  request: Schema.Unknown,
});

function makeService(
  input: AuthenticationSettings,
): Effect.Effect<AuthenticationService> {
  return Effect.gen(function* () {
    const nonces = yield* Ref.make<ReadonlyMap<string, number>>(new Map());
    const resolveCard = yield* makeCardResolver(
      {
        cacheCapacity: input.identityCardCacheCapacity,
        lookupConcurrencyLimit: input.registryLookupConcurrencyLimit,
      },
      input.lookup,
    );
    return Authentication.of({
      verifyRequest: (request) =>
        verifyRequest(request, {
          liveNonceCapacity: input.liveNonceCapacity,
          nonces,
          resolveCard,
        }).pipe(Effect.mapError(authenticationFailure)),
    });
  });
}

function verifyRequest(
  input: {
    readonly body: Uint8Array;
    readonly request: HttpServerRequest.HttpServerRequest;
  },
  dependencies: {
    readonly liveNonceCapacity: number;
    readonly nonces: Ref.Ref<ReadonlyMap<string, number>>;
    readonly resolveCard: CardResolver;
  },
): Effect.Effect<VerifiedAgentRequest, RequestAuthenticationFailure> {
  return Effect.gen(function* () {
    const body = yield* decodeCanonicalJson(
      registeredAgentBody,
      new Uint8Array(input.body),
    ).pipe(
      Effect.catchTag("CanonicalJsonError", () =>
        Effect.fail(new MalformedRequestError()),
      ),
    );
    const identityCard = yield* dependencies.resolveCard(body.callerAgentId);
    const signature = yield* verifyHttpRequestSignature({
      httpRequest: input.request,
      bodyBytes: input.body,
      publicKey: identityCard.publicKey,
      profile: "normal",
    });
    const claim = yield* claimNonce(dependencies.nonces, {
      capacity: dependencies.liveNonceCapacity,
      nonce: signature.nonce,
      expires: signature.expires,
      now: signature.verifiedAt,
    });
    if (claim === "replayed") {
      return yield* new AuthenticationFailedError();
    }
    if (claim === "full") {
      return yield* new OverloadedError();
    }
    const version = input.request.headers["sharness-version"];
    if (version === undefined || !(yield* WireVersions.live(version))) {
      return yield* new VersionMismatchError();
    }
    return makeVerifiedAgentRequest({
      callerAgentId: body.callerAgentId,
      identityCard: identityCard,
      request: body.request,
    });
  });
}

type CardResolutionError =
  | AuthenticationFailedError
  | OverloadedError
  | UnavailableError;

/** In-flight card lookups, keyed by agent, each awaited by later callers. */
type PendingResolutions = ReadonlyMap<
  AgentId,
  Deferred.Deferred<VerifiedAgentIdentityCard, CardResolutionError>
>;

type RegistryLookup = (request: {
  readonly agentId: AgentId;
}) => Effect.Effect<RegistryLookupResult, unknown>;

type CacheAccess =
  | Readonly<{
      readonly kind: "cached";
      readonly card: VerifiedAgentIdentityCard;
    }>
  | Readonly<{
      readonly kind: "join";
      readonly deferred: Deferred.Deferred<
        VerifiedAgentIdentityCard,
        CardResolutionError
      >;
    }>
  | Readonly<{
      readonly kind: "lead";
      readonly deferred: Deferred.Deferred<
        VerifiedAgentIdentityCard,
        CardResolutionError
      >;
    }>;

type RestoreInterruptibility = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
) => Effect.Effect<A, E, R>;

interface CardResolutionSettings {
  readonly cacheCapacity: number;
  readonly lookupConcurrencyLimit: number;
}

interface CardResolverDependencies {
  readonly pendingResolutions: Ref.Ref<PendingResolutions>;
  readonly cards: BoundedCache<AgentId, VerifiedAgentIdentityCard>;
  readonly lookup: RegistryLookup;
  readonly lookupPermits: Effect.Semaphore;
}

function makeCardResolver(
  settings: CardResolutionSettings,
  lookup: RegistryLookup,
): Effect.Effect<
  (
    agentId: AgentId,
  ) => Effect.Effect<VerifiedAgentIdentityCard, CardResolutionError>
> {
  return Effect.gen(function* () {
    const pendingResolutions = yield* Ref.make<PendingResolutions>(new Map());
    const lookupPermits = yield* Effect.makeSemaphore(
      settings.lookupConcurrencyLimit,
    );
    const dependencies: CardResolverDependencies = {
      pendingResolutions,
      cards: makeBoundedCache<AgentId, VerifiedAgentIdentityCard>(
        settings.cacheCapacity,
      ),
      lookup,
      lookupPermits,
    };
    return (
      agentId: AgentId,
    ): Effect.Effect<VerifiedAgentIdentityCard, CardResolutionError> =>
      Effect.uninterruptibleMask((restoreInterruptibility) =>
        resolveCard(dependencies, agentId, restoreInterruptibility),
      );
  });
}

type CardResolver = Effect.Effect.Success<ReturnType<typeof makeCardResolver>>;

function resolveCard(
  dependencies: CardResolverDependencies,
  agentId: AgentId,
  restoreInterruptibility: RestoreInterruptibility,
): Effect.Effect<VerifiedAgentIdentityCard, CardResolutionError> {
  return Effect.gen(function* () {
    const candidate = yield* Deferred.make<
      VerifiedAgentIdentityCard,
      CardResolutionError
    >();
    const access = yield* beginCardResolution(
      dependencies.pendingResolutions,
      dependencies.cards,
      agentId,
      candidate,
    );
    return yield* resolveCacheAccess(
      dependencies,
      agentId,
      access,
      restoreInterruptibility,
    );
  });
}

function resolveCacheAccess(
  dependencies: CardResolverDependencies,
  agentId: AgentId,
  access: CacheAccess,
  restoreInterruptibility: RestoreInterruptibility,
): Effect.Effect<VerifiedAgentIdentityCard, CardResolutionError> {
  // eslint-disable-next-line default-case -- the switch is exhaustive
  switch (access.kind) {
    case "cached":
      return Effect.succeed(access.card);
    case "join":
      return restoreInterruptibility(Deferred.await(access.deferred));
    case "lead":
      return Effect.forkDaemon(
        runLeadingResolution(dependencies, agentId, access.deferred).pipe(
          Effect.interruptible,
        ),
      ).pipe(
        Effect.zipRight(
          restoreInterruptibility(Deferred.await(access.deferred)),
        ),
      );
  }
}

function runLeadingResolution(
  dependencies: CardResolverDependencies,
  agentId: AgentId,
  deferred: Deferred.Deferred<VerifiedAgentIdentityCard, CardResolutionError>,
): Effect.Effect<VerifiedAgentIdentityCard, CardResolutionError> {
  return lookupCard(dependencies, agentId).pipe(
    Effect.tap((card) =>
      retainResolvedCard(dependencies.cards, {
        agentId,
        card,
      }),
    ),
    Effect.onExit((exit) =>
      removePendingResolution(dependencies.pendingResolutions, agentId).pipe(
        Effect.zipRight(Deferred.done(deferred, exit)),
      ),
    ),
  );
}

/**
 * Looks up `agentId` through the supplied resolver. Lookup failures have
 * one recovery at this boundary, so a lookup that fails, throws or dies fails
 * with an unavailable reason and still settles pending resolution. An
 * interrupted lookup stays interrupted.
 * @param dependencies Bounded Registry lookup and shared card-resolution state.
 * @param agentId Immutable identity whose verified card is required.
 * @returns The verified card or a closed resolution failure.
 */
function lookupCard(
  dependencies: CardResolverDependencies,
  agentId: AgentId,
): Effect.Effect<VerifiedAgentIdentityCard, CardResolutionError> {
  return dependencies.lookupPermits
    .withPermitsIfAvailable(1)(
      Effect.suspend(() => dependencies.lookup({ agentId })).pipe(
        Effect.catchAllCause((cause) =>
          Cause.isInterrupted(cause)
            ? Effect.interrupt
            : Effect.fail(new UnavailableError()),
        ),
      ),
    )
    .pipe(
      Effect.flatMap((attempted) => resolveLookupAttempt(attempted, agentId)),
    );
}

const isVerifiedIdentityCard = Schema.is(verifiedAgentIdentityCardSchema);

/**
 * A Registry provider must return a card verified by this package for the
 * requested identity. Its failure to supply that evidence leaves authentication
 * unavailable. Only an explicit missing identity refuses the caller here.
 * Provider-owned properties are read under a try because injected providers can
 * return objects whose getters throw.
 * @param attempted Registry outcome, absent when no lookup permit was available.
 * @param agentId Requested identity that the returned card must bind.
 * @returns The verified card or a closed resolution failure.
 */
function resolveLookupAttempt(
  attempted: Option.Option<RegistryLookupResult>,
  agentId: AgentId,
): Effect.Effect<VerifiedAgentIdentityCard, CardResolutionError> {
  if (Option.isNone(attempted)) {
    return Effect.fail(new OverloadedError());
  }
  const result = attempted.value;
  return Effect.try({
    try: (): Effect.Effect<
      VerifiedAgentIdentityCard,
      AuthenticationFailedError | UnavailableError
    > => {
      const kind = result.kind;
      if (kind === "not_found") {
        return Effect.fail(new AuthenticationFailedError());
      }
      const identityCard = result.identityCard;
      return kind === "found" &&
        isVerifiedIdentityCard(identityCard) &&
        identityCard.agentId === agentId
        ? Effect.succeed(identityCard)
        : Effect.fail(new UnavailableError());
    },
    catch: () => new UnavailableError(),
  }).pipe(Effect.flatten);
}

/**
 * Bounded-cache reads and writes are synchronous and never span a yield, so
 * cache access and pending-resolution decisions cannot interleave.
 * @param pendingResolutions Shared in-flight resolutions whose entries must be preserved.
 * @param cards Bounded cache of verified immutable cards.
 * @param agentId Identity used as the cache and pending-resolution key.
 * @param candidate Deferred value installed only if this lookup wins acquisition.
 * @returns The cached card or shared pending lookup selected atomically.
 */
function beginCardResolution(
  pendingResolutions: Ref.Ref<PendingResolutions>,
  cards: BoundedCache<AgentId, VerifiedAgentIdentityCard>,
  agentId: AgentId,
  candidate: Deferred.Deferred<VerifiedAgentIdentityCard, CardResolutionError>,
): Effect.Effect<CacheAccess> {
  return Ref.modify(
    pendingResolutions,
    (current): readonly [CacheAccess, PendingResolutions] => {
      const card = cards.get(agentId);
      if (Option.isSome(card)) {
        return [{ kind: "cached", card: card.value }, current];
      }
      const pending = current.get(agentId);
      if (pending !== undefined) {
        return [{ kind: "join", deferred: pending }, current];
      }
      return [
        { kind: "lead", deferred: candidate },
        new Map(current).set(agentId, candidate),
      ];
    },
  );
}

function removePendingResolution(
  pendingResolutions: Ref.Ref<PendingResolutions>,
  agentId: AgentId,
): Effect.Effect<void> {
  return Ref.update(pendingResolutions, (current) => {
    const pending = new Map(current);
    pending.delete(agentId);
    return pending;
  });
}

function retainResolvedCard(
  cards: BoundedCache<AgentId, VerifiedAgentIdentityCard>,
  input: {
    readonly agentId: AgentId;
    readonly card: VerifiedAgentIdentityCard;
  },
): Effect.Effect<void> {
  return Effect.sync(() => {
    cards.set(input.agentId, input.card);
  });
}

type NonceClaim = "claimed" | "replayed" | "full";

interface NonceClaimInput {
  readonly capacity: number;
  readonly nonce: string;
  readonly expires: number;
  readonly now: number;
}

function claimNonce(
  nonces: Ref.Ref<ReadonlyMap<string, number>>,
  input: NonceClaimInput,
): Effect.Effect<NonceClaim> {
  return Ref.modify(nonces, (current) => updateNonces(current, input));
}

function updateNonces(
  current: ReadonlyMap<string, number>,
  input: NonceClaimInput,
): readonly [NonceClaim, ReadonlyMap<string, number>] {
  const live = new Map<string, number>();
  for (const [retainedNonce, retainedExpiry] of current) {
    if (retainedExpiry >= input.now) {
      live.set(retainedNonce, retainedExpiry);
    }
  }
  if (live.has(input.nonce)) {
    return ["replayed", live];
  }
  if (live.size >= input.capacity) {
    return ["full", live];
  }
  live.set(input.nonce, input.expires);
  return ["claimed", live];
}

function makeVerifiedAgentRequest(input: {
  readonly callerAgentId: AgentId;
  readonly identityCard: VerifiedAgentIdentityCard;
  readonly request: unknown;
}): VerifiedAgentRequest {
  return brandVerifiedAgentRequest(Object.freeze({ ...input }));
}
