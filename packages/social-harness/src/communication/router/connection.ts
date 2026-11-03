/** @file Maintains the Router attachment and supervises polling and delivery. */

import { SqlClient } from "@effect/sql/SqlClient";
import {
  Clock,
  type Context,
  Data,
  Effect,
  Encoding,
  Option,
  Ref,
  Schedule,
  Schema,
  Stream,
  SubscriptionRef,
} from "effect";
import { createHash } from "node:crypto";
import type {
  AgentId,
  VerifiedAgentIdentityCard,
} from "../../identity/agent-cards.js";
import type { AgentSigningAuthority } from "../../identity/keys.js";
import { decodeCanonical } from "../../encoding/canonical-json.js";
import {
  type LiveSignedMessage,
  SignedMessage,
  type VerifiedSignedMessage,
} from "../../identity/message-security.js";
import { Registry } from "../../identity/registry.js";
import {
  beginOutbound,
  completeOutbound,
  type StoredOutboundMessage,
} from "../store.js";
import {
  type PollCursor,
  Router,
  type RouterInstanceId,
  type RouterPollResult,
  type RouterSendResult,
  SignedMessageDigest,
} from "./client.js";

/** An outer sender card could not be resolved, pinned, or authenticated. */
class RouterWorkerAuthenticationError extends Data.TaggedError(
  "RouterWorkerAuthenticationError",
) {}

/** Endpoint state needed for cursor advancement could not be persisted. */
export class RouterWorkerPersistenceError extends Data.TaggedError(
  "RouterWorkerPersistenceError",
) {}

/** An authenticated outer body is not one closed Client protocol value. */
export class RouterWorkerPayloadInvalidError extends Data.TaggedError(
  "RouterWorkerPayloadInvalidError",
) {}

/**
 * Quick transport retries or retry-identity resends exhausted their bound.
 * Work remains pending for another attempt.
 */
export class RouterWorkerTransportError extends Data.TaggedError(
  "RouterWorkerTransportError",
) {}

/** A Router outcome or local envelope violated the private worker contract. */
export class RouterWorkerProtocolError extends Data.TaggedError(
  "RouterWorkerProtocolError",
) {}

/** Certified-history recovery did not complete for the current Router tail. */
export class RouterWorkerRecoveryError extends Data.TaggedError(
  "RouterWorkerRecoveryError",
) {}

/** A caller must resume protocol work from the recovered certified position. */
export class RouterWorkerDiscontinuityError extends Data.TaggedError(
  "RouterWorkerDiscontinuityError",
) {}

/**
 * The Router refused this endpoint's request itself: its authentication, its
 * Social Harness version, its representation, or its local signature. Retrying
 * cannot succeed, so the failure is fatal rather than an outage.
 */
class RouterWorkerRejectedError extends Data.TaggedError(
  "RouterWorkerRejectedError",
)<{
  readonly reason:
    | "authentication"
    | "version"
    | "request"
    | "response"
    | "signing";
}> {}

/** A normal send was attempted while the worker is detached or recovering. */
export class RouterWorkerUnavailableError extends Data.TaggedError(
  "RouterWorkerUnavailableError",
) {}

interface TransmitInput {
  readonly outbound: StoredOutboundMessage;
  readonly message: LiveSignedMessage;
  readonly instance: RouterInstanceId;
  readonly mode: "initial" | "retry";
  readonly attemptsRemaining: number;
}

type OutboundTransportError =
  | RouterWorkerPersistenceError
  | RouterCallFailure
  | RouterWorkerProtocolError;

type AnchoredRecoveringState = RouterWorkerRecoveringState & {
  readonly anchor: NonNullable<RouterWorkerRecoveringState["anchor"]>;
};

/** Closed reason for abandoning process-local protocol folds. */
export type RouterDiscontinuityReason =
  | "feed_gap"
  | "cursor_invalid"
  | "router_restarted";

/** Empty-tail Router position learned from an omitted-cursor poll. */
export interface RouterTailAnchor {
  readonly routerInstanceId: RouterInstanceId;
  readonly pollCursor: PollCursor;
}

/** A verified protocol input either changed durable state or was conclusively irrelevant. */
export type RouterIngressDisposition = "accepted" | "ignored";

/** Closed failures returned by a send. */
export type RouterWorkerSendError =
  | RouterWorkerAuthenticationError
  | RouterWorkerPersistenceError
  | RouterWorkerRejectedError
  | RouterWorkerTransportError
  | RouterWorkerProtocolError
  | RouterWorkerRecoveryError
  | RouterWorkerDiscontinuityError
  | RouterWorkerUnavailableError;

/** Closed failures returned by one poll/recovery transition. */
export type RouterWorkerPollError =
  | RouterWorkerAuthenticationError
  | RouterWorkerPersistenceError
  | RouterWorkerRejectedError
  | RouterWorkerTransportError
  | RouterWorkerProtocolError
  | RouterWorkerRecoveryError
  | RouterWorkerDiscontinuityError
  | RouterWorkerUnavailableError;

/** Verified outer message and its decoded private Client payload. */
export interface RouterWorkerIngress<Payload> {
  readonly routerInstanceId: RouterInstanceId;
  readonly message: LiveSignedMessage & VerifiedSignedMessage;
  readonly senderCard: VerifiedAgentIdentityCard;
  readonly payload: Payload;
}

/** Discontinuity recovery input after learning the new empty tail. */
export interface RouterWorkerRecovery {
  readonly reason: RouterDiscontinuityReason;
  readonly anchor: RouterTailAnchor;
}

/** Private endpoint callbacks around the Router worker's ordering boundary. */
interface RouterWorkerCallbacks<Payload> {
  readonly decodePayload: (
    message: LiveSignedMessage & VerifiedSignedMessage,
  ) => Effect.Effect<Payload, RouterWorkerPayloadInvalidError>;
  readonly acceptPayload: (
    input: RouterWorkerIngress<Payload>,
  ) => Effect.Effect<RouterIngressDisposition, RouterWorkerPersistenceError>;
  readonly abandonVolatileFolds: (
    reason: RouterDiscontinuityReason,
  ) => Effect.Effect<void>;
  readonly recoverCertifiedHistory: (
    input: RouterWorkerRecovery,
  ) => Effect.Effect<void, RouterWorkerRecoveryError>;
  /** The worker reattached to the same Router instance after an outage. */
  readonly reattached: () => Effect.Effect<void>;
}

/** Complete private construction input for one registered endpoint. */
export interface RouterWorkerInput<Payload> {
  readonly agentKey: string;
  readonly callerAgentId: AgentId;
  /** Durable membership cards available before Registry connectivity. */
  readonly pinnedSenderCards: readonly VerifiedAgentIdentityCard[];
  readonly signingAuthority: AgentSigningAuthority;
  readonly callbacks: RouterWorkerCallbacks<Payload>;
}

/** One endpoint-wide Router worker. */
export interface RouterWorker {
  readonly currentAnchor: Effect.Effect<
    RouterTailAnchor,
    RouterWorkerUnavailableError
  >;
  /**
   * Returns the current anchor while active. Otherwise waits for the next poll
   * or completed recovery to publish one. Callers bound the wait because the
   * worker never gives up.
   */
  readonly awaitAnchor: Effect.Effect<RouterTailAnchor>;
  readonly pollOnce: Effect.Effect<void, RouterWorkerPollError>;
  /**
   * Polls for the endpoint's lifetime. A transient failure detaches the
   * worker and backs off until Router answers. Only a non-transient failure
   * ends the loop.
   */
  readonly run: Effect.Effect<never, RouterWorkerPollError>;
  readonly send: (
    envelopeId: string,
  ) => Effect.Effect<void, RouterWorkerSendError>;
}

/** Volatile cursor state for an active Router instance. */
interface RouterWorkerActiveState {
  readonly kind: "active";
  readonly generation: number;
  readonly anchor: RouterTailAnchor;
}

/** Volatile discontinuity state while certified history is reconciled. */
interface RouterWorkerRecoveringState {
  readonly kind: "recovering";
  readonly generation: number;
  readonly reason: RouterDiscontinuityReason;
  readonly priorRouterInstanceId?: RouterInstanceId;
  readonly volatileFoldsAbandoned: boolean;
  readonly anchor?: RouterTailAnchor;
  /**
   * Records when recovery first lost Router in epoch milliseconds. The next
   * answered tail poll clears it.
   */
  readonly unreachableSince: Option.Option<number>;
}

/**
 * Volatile cursor state while the Router stops answering an active worker.
 * The next answered poll either reattaches at the same anchor or starts
 * recovery, so no send runs until the Router's instance is known again.
 */
interface RouterWorkerDetachedState {
  readonly kind: "detached";
  readonly generation: number;
  readonly anchor: RouterTailAnchor;
  /** Epoch milliseconds of the Clock when the worker detached. */
  readonly detachedAt: number;
}

/** Closed volatile lifecycle of the Router worker. */
type RouterWorkerState =
  | RouterWorkerActiveState
  | RouterWorkerDetachedState
  | RouterWorkerRecoveringState;

/** Resolved public capabilities consumed by private worker mechanics. */
interface RouterWorkerServices {
  readonly router: Context.Tag.Service<typeof Router>;
  readonly registry: Context.Tag.Service<typeof Registry>;
}

/** Shared mutable worker runtime, guarded by its three semaphores. */
interface RouterWorkerRuntime<Payload> extends RouterWorkerServices {
  readonly input: RouterWorkerInput<Payload>;
  readonly cards: Map<AgentId, VerifiedAgentIdentityCard>;
  readonly state: SubscriptionRef.SubscriptionRef<RouterWorkerState>;
  readonly pollGate: Effect.Semaphore;
  readonly stateGate: Effect.Semaphore;
  readonly recoveryGate: Effect.Semaphore;
}

/** Outer message with both sender card and signature verified. */
interface RouterWorkerVerifiedIngress {
  readonly message: LiveSignedMessage & VerifiedSignedMessage;
  readonly senderCard: VerifiedAgentIdentityCard;
}

/** Router send outcome relevant above transport representation. */
type RouterWorkerSendOutcome =
  | Readonly<{ kind: "accepted" }>
  | Readonly<{ kind: "restarted" }>;

/** Attempts one send spends on transport retries and identity-loss resends. */
const routerWorkerRetryAttempts = 3;

/** Fixed interruptible spacing between bounded attempts. */
const routerWorkerRetryDelay = "25 millis";

/**
 * Quick retries of one poll before a transport failure detaches the worker,
 * spanning about 350 ms, so a connection reset or a single overloaded answer
 * does not block sends.
 */
const routerWorkerBlipSchedule = Schedule.exponential("50 millis").pipe(
  Schedule.intersect(Schedule.recurs(3)),
);

/**
 * Jittered delays grow from 100 ms to 5 s without exhaustion. Router outages
 * pause progress rather than end the endpoint.
 */
export const routerWorkerReconnectSchedule = Schedule.exponential(
  "100 millis",
).pipe(Schedule.union(Schedule.spaced("5 seconds")), Schedule.jittered);

/**
 * How often a worker that cannot reach the Router, detached or recovering,
 * repeats its warning.
 */
const routerWorkerDetachedReportInterval = "60 seconds";

const transientByTag = {
  RouterWorkerAuthenticationError: false,
  RouterWorkerDiscontinuityError: true,
  RouterWorkerPersistenceError: false,
  RouterWorkerProtocolError: false,
  RouterWorkerRecoveryError: false,
  RouterWorkerRejectedError: false,
  RouterWorkerTransportError: true,
  RouterWorkerUnavailableError: true,
} as const satisfies Readonly<Record<RouterWorkerPollError["_tag"], boolean>>;

/** Poll and send share the Router client failure union. */
type RouterCallError = Effect.Effect.Error<
  ReturnType<RouterWorkerServices["router"]["poll"]>
>;

/** Any tagged worker failure. */
type TaggedFailure = Readonly<{ _tag: string }>;

/** Worker failure a Router call maps to. */
type RouterCallFailure = RouterWorkerTransportError | RouterWorkerRejectedError;

/**
 * Retries outage failures: unreachable, timeout, 429 and 5xx. Authentication,
 * version, malformed responses, signing failures and other 4xx are rejections.
 */
const routerFailureByTag = {
  AgentSigningError: rejectedFailure("signing"),
  AuthenticationFailedError: rejectedFailure("authentication"),
  InternalServerError: transportFailure,
  MalformedRequestError: rejectedFailure("request"),
  MethodNotAllowedError: rejectedFailure("request"),
  OverloadedError: transportFailure,
  PayloadTooLargeError: rejectedFailure("request"),
  RouteNotFoundError: rejectedFailure("request"),
  RouterConnectionError: transportFailure,
  RouterInvalidResponseError: rejectedFailure("response"),
  RouterRequestTimeoutError: transportFailure,
  UnavailableError: transportFailure,
  UnsupportedMediaTypeError: rejectedFailure("request"),
  VersionMismatchError: rejectedFailure("version"),
} as const satisfies Readonly<
  Record<RouterCallError["_tag"], () => RouterCallFailure>
>;

/** Quick retries of one Router poll, for transport failures only. */
const pollBlipRetry = routerWorkerBlipSchedule.pipe(
  Schedule.whileInput(isTransportFailure),
);

/** A worker waiting on an unreachable Router, detached or recovering. */
interface Stall {
  readonly kind: "detached" | "recovering";
  readonly generation: number;
  readonly since: number;
}

const sameStall = Option.getEquivalence(
  (left: Stall, right: Stall) =>
    left.kind === right.kind &&
    left.generation === right.generation &&
    left.since === right.since,
);

const stallText = {
  detached: "worker detached",
  recovering: "recovery waiting",
} as const satisfies Readonly<Record<Stall["kind"], string>>;

/**
 * Build a recovering worker without starting its polling loop.
 * @param input Registered identity and durable endpoint callbacks.
 * @returns A worker that remains unavailable until certified recovery completes.
 */
export function makeRouterWorker<Payload>(
  input: RouterWorkerInput<Payload>,
): Effect.Effect<
  RouterWorker,
  RouterWorkerTransportError | RouterWorkerProtocolError,
  Registry | Router | SqlClient
> {
  return Effect.gen(function* () {
    const services = {
      router: yield* Router,
      registry: yield* Registry,
    };
    const cards = new Map(
      input.pinnedSenderCards.map((card) => [card.agentId, card]),
    );
    const runtime: RouterWorkerRuntime<Payload> = {
      ...services,
      input,
      cards,
      state: yield* SubscriptionRef.make<RouterWorkerState>({
        kind: "recovering",
        generation: 0,
        reason: "router_restarted",
        volatileFoldsAbandoned: false,
        unreachableSince: Option.none(),
      }),
      pollGate: yield* Effect.makeSemaphore(1),
      stateGate: yield* Effect.makeSemaphore(1),
      recoveryGate: yield* Effect.makeSemaphore(1),
    };
    return makeWorker(runtime, yield* SqlClient);
  }).pipe(Effect.withSpan("makeRouterWorker"));
}

/**
 * Transport loss, pending recovery and discontinuity can recover. Protocol,
 * persistence and rejection failures remain fatal.
 */
export function isTransientRouterWorkerError(
  error: RouterWorkerPollError | RouterWorkerSendError,
): boolean {
  return transientByTag[error._tag];
}

/**
 * Names why the worker or outbound drain failed. Router rejection includes
 * what Router refused.
 */
export function describeRouterWorkerFailure(
  error: RouterWorkerPollError | RouterWorkerSendError,
): string {
  return error._tag === "RouterWorkerRejectedError"
    ? `the Router rejected this endpoint's ${error.reason}`
    : error._tag;
}

/**
 * Classify one Router client failure as an outage or a rejection.
 * @param error Failure the Router client raised.
 * @returns The worker failure it maps to.
 */
function mapRouterFailure(error: RouterCallError): RouterCallFailure {
  return routerFailureByTag[error._tag]();
}

function mapAuthenticationError() {
  return new RouterWorkerAuthenticationError();
}

function mapProtocolError() {
  return new RouterWorkerProtocolError();
}

function mapPersistenceError() {
  return new RouterWorkerPersistenceError();
}

function anchorFromOmittedResult(
  result: RouterPollResult,
): Effect.Effect<RouterTailAnchor, RouterWorkerProtocolError> {
  return result.kind === "batch" && result.signedMessages.length === 0
    ? Effect.succeed({
        routerInstanceId: result.routerInstanceId,
        pollCursor: result.pollCursor,
      })
    : Effect.fail(new RouterWorkerProtocolError());
}

function activeAnchor(
  state: RouterWorkerState,
): Option.Option<RouterTailAnchor> {
  return state.kind === "active" ? Option.some(state.anchor) : Option.none();
}

function makeWorker<Payload>(
  runtime: RouterWorkerRuntime<Payload>,
  sql: SqlClient,
): RouterWorker {
  const provideSql = Effect.provideService(SqlClient, sql);
  const pollOnce = makePollOnce(runtime).pipe(provideSql);
  return Object.freeze({
    currentAnchor: Ref.get(runtime.state).pipe(
      Effect.flatMap((state) =>
        state.kind === "active"
          ? Effect.succeed(state.anchor)
          : Effect.fail(new RouterWorkerUnavailableError()),
      ),
    ),
    /**
     * State replay covers attachment between a caller's probe and this wait.
     * The stream never ends, so an empty head is a defect.
     */
    awaitAnchor: runtime.state.changes.pipe(
      Stream.filterMap(activeAnchor),
      Stream.runHead,
      Effect.flatMap(Effect.orDie),
    ),
    pollOnce,
    run: Effect.zipRight(
      reportUnreachable(runtime),
      pollOnce.pipe(
        Effect.tapError((error) => noteRunFailure(runtime, error)),
        Effect.retry(
          routerWorkerReconnectSchedule.pipe(
            Schedule.whileInput(isTransientRouterWorkerError),
          ),
        ),
        Effect.forever,
      ),
      { concurrent: true },
    ).pipe(Effect.interruptible),
    send: (envelopeId: string) =>
      makeSend(runtime, envelopeId).pipe(provideSql),
  });
}

/**
 * Repeats outage warnings at `routerWorkerDetachedReportInterval` for the
 * worker's lifetime.
 */
function reportUnreachable<Payload>(
  runtime: RouterWorkerRuntime<Payload>,
): Effect.Effect<void> {
  return runtime.state.changes.pipe(
    Stream.map(stallOf),
    Stream.changesWith(sameStall),
    Stream.flatMap(
      Option.match({
        onNone: () => Stream.empty,
        onSome: (stall) =>
          Stream.tick(routerWorkerDetachedReportInterval).pipe(
            Stream.drop(1),
            Stream.mapEffect(() => logStillStalled(stall)),
          ),
      }),
      { switch: true },
    ),
    Stream.runDrain,
  );
}

/**
 * Fatal failures log the exit reason. Recovery failures warn and retain the
 * first unreachable time.
 */
function noteRunFailure<Payload>(
  runtime: RouterWorkerRuntime<Payload>,
  error: RouterWorkerPollError,
): Effect.Effect<void> {
  return isTransientRouterWorkerError(error)
    ? Clock.currentTimeMillis.pipe(
        Effect.flatMap((now) =>
          runtime.stateGate.withPermits(1)(
            Ref.updateAndGet(runtime.state, (state) =>
              markUnreachable(state, error, now),
            ),
          ),
        ),
        Effect.flatMap((state) => logRecoveryAttemptFailed(state, error)),
      )
    : Effect.logError(
        `Router worker stopping, daemon exits: ${describeRouterWorkerFailure(error)}`,
      );
}

function makePollOnce<Payload>(
  runtime: RouterWorkerRuntime<Payload>,
): Effect.Effect<void, RouterWorkerPollError> {
  return runtime.pollGate.withPermits(1)(
    Ref.get(runtime.state).pipe(
      Effect.flatMap((state) => {
        switch (state.kind) {
          case "active":
            return pollActiveOnce(runtime, state);
          case "detached":
            return probeDetached(runtime, state);
          case "recovering":
            return runtime.recoveryGate.withPermits(1)(
              finishRouterRecovery(runtime, state),
            );
          default: {
            const exhaustive: never = state;
            return exhaustive;
          }
        }
      }),
    ),
  );
}

function pollActiveOnce<Payload>(
  runtime: RouterWorkerRuntime<Payload>,
  snapshot: RouterWorkerActiveState,
): Effect.Effect<void, RouterWorkerPollError> {
  return runtime.router
    .poll({
      request: { pollCursor: snapshot.anchor.pollCursor },
      callerAgentId: runtime.input.callerAgentId,
      signingAuthority: runtime.input.signingAuthority,
    })
    .pipe(
      Effect.mapError(mapRouterFailure),
      Effect.retry(pollBlipRetry),
      Effect.tapErrorTag("RouterWorkerTransportError", () =>
        detach(runtime, snapshot.generation),
      ),
      Effect.flatMap((result) => {
        switch (result.kind) {
          case "batch":
            if (result.routerInstanceId !== snapshot.anchor.routerInstanceId) {
              return triggerDiscontinuity(
                runtime,
                "router_restarted",
                snapshot.generation,
              );
            }
            return verifyBatch(runtime, result.signedMessages).pipe(
              Effect.flatMap((verified) =>
                commitBatch(runtime, snapshot, result, verified),
              ),
            );
          case "feed_gap":
            return triggerDiscontinuity(
              runtime,
              "feed_gap",
              snapshot.generation,
            );
          case "cursor_invalid":
            return triggerDiscontinuity(
              runtime,
              "cursor_invalid",
              snapshot.generation,
            );
          default:
            return Effect.fail(new RouterWorkerProtocolError());
        }
      }),
      Effect.interruptible,
    );
}

/**
 * Detaches only the generation that lost its poll. Newer recovery state
 * remains untouched, and sends wait for an answered poll.
 */
function detach<Payload>(
  runtime: RouterWorkerRuntime<Payload>,
  generation: number,
): Effect.Effect<void> {
  return Clock.currentTimeMillis.pipe(
    Effect.flatMap((detachedAt) =>
      runtime.stateGate.withPermits(1)(
        Ref.modify(
          runtime.state,
          (
            state,
          ): readonly [
            Option.Option<RouterWorkerDetachedState>,
            RouterWorkerState,
          ] => {
            if (state.kind !== "active" || state.generation !== generation) {
              return [Option.none(), state];
            }
            const detached: RouterWorkerDetachedState = {
              ...state,
              kind: "detached",
              detachedAt,
            };
            return [Option.some(detached), detached];
          },
        ),
      ),
    ),
    Effect.flatMap(
      Option.match({ onNone: () => Effect.void, onSome: logDetached }),
    ),
  );
}

function verifyBatch<Payload>(
  runtime: RouterWorkerRuntime<Payload>,
  messages: readonly LiveSignedMessage[],
): Effect.Effect<
  readonly RouterWorkerVerifiedIngress[],
  RouterWorkerAuthenticationError | RouterWorkerPersistenceError
> {
  return Effect.forEach(messages, (message) => verifyOuter(runtime, message), {
    concurrency: 1,
  });
}

function verifyOuter<Payload>(
  runtime: RouterWorkerRuntime<Payload>,
  signedMessage: LiveSignedMessage,
): Effect.Effect<
  RouterWorkerVerifiedIngress,
  RouterWorkerAuthenticationError | RouterWorkerPersistenceError
> {
  return Effect.gen(function* () {
    const senderCard = yield* resolveSenderCard(
      runtime,
      signedMessage.senderAgentId,
    );
    const message = yield* SignedMessage.verify({
      signedMessage,
      identityCard: senderCard,
    }).pipe(Effect.mapError(mapAuthenticationError));
    return { message, senderCard };
  });
}

function resolveSenderCard<Payload>(
  runtime: RouterWorkerRuntime<Payload>,
  agentId: AgentId,
): Effect.Effect<
  VerifiedAgentIdentityCard,
  RouterWorkerAuthenticationError | RouterWorkerPersistenceError
> {
  const cached = runtime.cards.get(agentId);
  if (cached !== undefined) {
    return Effect.succeed(cached);
  }
  return runtime.registry.lookup({ agentId }).pipe(
    Effect.mapError(mapAuthenticationError),
    Effect.flatMap((result) =>
      result.kind === "found" && result.identityCard.agentId === agentId
        ? Effect.succeed(result.identityCard)
        : Effect.fail(new RouterWorkerAuthenticationError()),
    ),
    Effect.tap((card) => Effect.sync(() => runtime.cards.set(agentId, card))),
    Effect.interruptible,
  );
}

/**
 * Accepts the entire verified batch durably before advancing the cursor.
 * Callbacks queue network work without transmitting under the state gate.
 * An obsolete generation makes no change.
 */
function commitBatch<Payload>(
  runtime: RouterWorkerRuntime<Payload>,
  snapshot: RouterWorkerActiveState,
  result: Extract<
    RouterPollResult,
    {
      readonly kind: "batch";
    }
  >,
  verified: readonly RouterWorkerVerifiedIngress[],
): Effect.Effect<void, RouterWorkerPersistenceError> {
  return runtime.stateGate.withPermits(1)(
    Effect.gen(function* () {
      const current = yield* Ref.get(runtime.state);
      if (
        !stateMatches(current, snapshot.generation, result.routerInstanceId)
      ) {
        return;
      }
      yield* Effect.forEach(
        verified,
        (ingress) => acceptVerified(runtime, ingress, result.routerInstanceId),
        { concurrency: 1 },
      );
      yield* Ref.set(runtime.state, {
        ...current,
        anchor: { ...current.anchor, pollCursor: result.pollCursor },
      });
    }),
  );
}

function acceptVerified<Payload>(
  runtime: RouterWorkerRuntime<Payload>,
  ingress: RouterWorkerVerifiedIngress,
  routerInstanceId: RouterInstanceId,
): Effect.Effect<void, RouterWorkerPersistenceError> {
  return runtime.input.callbacks.decodePayload(ingress.message).pipe(
    Effect.flatMap((payload) =>
      runtime.input.callbacks.acceptPayload({
        ...ingress,
        payload,
        routerInstanceId,
      }),
    ),
    Effect.asVoid,
    Effect.catchTag("RouterWorkerPayloadInvalidError", () => Effect.void),
  );
}

function stateMatches(
  state: RouterWorkerState,
  generation: number,
  routerInstanceId: RouterInstanceId,
): state is RouterWorkerActiveState {
  return (
    state.kind === "active" &&
    state.generation === generation &&
    state.anchor.routerInstanceId === routerInstanceId
  );
}

/**
 * An omitted-cursor probe answers immediately. The same Router resumes the
 * retained cursor and another instance starts recovery.
 */
function probeDetached<Payload>(
  runtime: RouterWorkerRuntime<Payload>,
  snapshot: RouterWorkerDetachedState,
): Effect.Effect<void, RouterWorkerPollError> {
  return pollRouterTail(runtime, runtime.input).pipe(
    Effect.flatMap((tail) =>
      tail.routerInstanceId === snapshot.anchor.routerInstanceId
        ? reattach(runtime, snapshot)
        : triggerDiscontinuity(
            runtime,
            "router_restarted",
            snapshot.generation,
          ),
    ),
    Effect.interruptible,
  );
}

/** Reattaches only the probed generation, then notifies the engine to resume paused work. */
function reattach<Payload>(
  runtime: RouterWorkerRuntime<Payload>,
  snapshot: RouterWorkerDetachedState,
): Effect.Effect<void> {
  return runtime.stateGate
    .withPermits(1)(
      Ref.get(runtime.state).pipe(
        Effect.flatMap((current) =>
          current.kind === "detached" &&
          current.generation === snapshot.generation
            ? Ref.set(runtime.state, {
                kind: "active",
                generation: current.generation,
                anchor: current.anchor,
              }).pipe(Effect.zipRight(logReattached(current)), Effect.as(true))
            : Effect.succeed(false),
        ),
      ),
    )
    .pipe(
      Effect.flatMap((reattached) =>
        reattached ? runtime.input.callbacks.reattached() : Effect.void,
      ),
    );
}

function makeSend<Payload>(
  runtime: RouterWorkerRuntime<Payload>,
  envelopeId: string,
): Effect.Effect<void, RouterWorkerSendError, SqlClient> {
  return Effect.gen(function* () {
    const result = yield* runtime.recoveryGate.withPermits(1)(
      Effect.gen(function* () {
        const state = yield* Ref.get(runtime.state);
        if (state.kind !== "active") {
          return yield* Effect.fail(new RouterWorkerUnavailableError());
        }
        const outcome = yield* transmitOutbound(
          runtime,
          envelopeId,
          state.anchor.routerInstanceId,
        );
        return { outcome, generation: state.generation };
      }),
    );
    if (result.outcome.kind === "restarted") {
      yield* triggerDiscontinuity(
        runtime,
        "router_restarted",
        result.generation,
      );
      return yield* Effect.fail(new RouterWorkerDiscontinuityError());
    }
  });
}

/**
 * Sends the current durable envelope by stable outbox identity under the
 * Router-instance fence. Returns acceptance or Router restart.
 */
function transmitOutbound<Payload>(
  runtime: RouterWorkerRuntime<Payload>,
  envelopeId: string,
  instance: RouterInstanceId,
): Effect.Effect<RouterWorkerSendOutcome, OutboundTransportError, SqlClient> {
  const agentKey = runtime.input.agentKey;

  return beginOutbound(agentKey, envelopeId).pipe(
    Effect.mapError(mapPersistenceError),
    Effect.flatMap((attempt) => {
      switch (attempt.kind) {
        case "inactive":
          return Effect.succeed({ kind: "accepted" } as const);
        case "pending":
          return decodeStoredOutbound(attempt.outbound).pipe(
            Effect.flatMap((message) =>
              message.senderAgentId === runtime.input.callerAgentId
                ? transmitOuter(runtime, {
                    outbound: attempt.outbound,
                    message,
                    instance,
                    mode: attempt.mode,
                    attemptsRemaining: routerWorkerRetryAttempts,
                  })
                : Effect.fail(new RouterWorkerProtocolError()),
            ),
          );
        default: {
          const exhaustive: never = attempt;
          return exhaustive;
        }
      }
    }),
  );
}

function interpretSendResult<Payload>(
  runtime: RouterWorkerRuntime<Payload>,
  input: TransmitInput,
  result: RouterSendResult,
): Effect.Effect<RouterWorkerSendOutcome, OutboundTransportError, SqlClient> {
  const agentKey = runtime.input.agentKey;

  switch (result.kind) {
    case "accepted":
      return completeAcceptedOutbound(agentKey, input, result);
    case "router_restarted":
      return Effect.succeed({ kind: "restarted" });
    case "retry_identity_unknown":
      return resendInMode(runtime, input, "initial", 1);
    case "idempotency_conflict":
      return resendInMode(runtime, input, "retry", 0);
    case "message_invalid":
      return Effect.fail(new RouterWorkerProtocolError());
    default: {
      const exhaustive: never = result;
      return exhaustive;
    }
  }
}

/**
 * Resends identical bytes as `initial` when retry identity was evicted, and
 * as `retry` after an initial conflict. Other mode/result pairings fail closed.
 *
 * Conflicts spend no attempt so a slower copy can succeed on the last one.
 * Unknown identities spend an attempt to bound repeated eviction. Exhaustion
 * leaves the envelope pending for the next drain.
 */
function resendInMode<Payload>(
  runtime: RouterWorkerRuntime<Payload>,
  input: TransmitInput,
  mode: TransmitInput["mode"],
  spent: 0 | 1,
): Effect.Effect<RouterWorkerSendOutcome, OutboundTransportError, SqlClient> {
  const attemptsRemaining = input.attemptsRemaining - spent;
  if (input.mode === mode) {
    return Effect.fail(new RouterWorkerProtocolError());
  }
  return attemptsRemaining < 1
    ? Effect.logWarning(
        "Router kept losing the retry identity; the envelope waits for the next drain",
      ).pipe(Effect.zipRight(Effect.fail(new RouterWorkerTransportError())))
    : transmitOuter(runtime, { ...input, mode, attemptsRemaining });
}

/** Preserves exact stored bytes and EnvelopeId across ambiguous outcomes. */
function transmitOuter<Payload>(
  runtime: RouterWorkerRuntime<Payload>,
  input: TransmitInput,
): Effect.Effect<RouterWorkerSendOutcome, OutboundTransportError, SqlClient> {
  return runtime.router
    .send({
      request: {
        expectedRouterInstanceId: input.instance,
        mode: input.mode,
        signedMessage: input.message,
      },
      callerAgentId: runtime.input.callerAgentId,
      signingAuthority: runtime.input.signingAuthority,
    })
    .pipe(
      Effect.mapError(mapRouterFailure),
      Effect.matchEffect({
        onFailure: (error) =>
          !isTransportFailure(error) || input.attemptsRemaining <= 1
            ? Effect.fail(error)
            : Effect.sleep(routerWorkerRetryDelay).pipe(
                Effect.zipRight(
                  transmitOuter(runtime, {
                    ...input,
                    mode: "retry",
                    attemptsRemaining: input.attemptsRemaining - 1,
                  }),
                ),
              ),
        onSuccess: (result) => interpretSendResult(runtime, input, result),
      }),
      Effect.interruptible,
    );
}

function completeAcceptedOutbound(
  agentKey: string,
  input: TransmitInput,
  result: Extract<RouterSendResult, { readonly kind: "accepted" }>,
): Effect.Effect<RouterWorkerSendOutcome, OutboundTransportError, SqlClient> {
  return Effect.gen(function* () {
    if (result.routerInstanceId !== input.instance) {
      return yield* Effect.fail(new RouterWorkerProtocolError());
    }
    const expectedDigest = yield* digestCanonicalSignedMessage(
      input.outbound.canonicalSignedMessage,
    );
    if (result.signedMessageDigest !== expectedDigest) {
      return yield* Effect.fail(new RouterWorkerProtocolError());
    }
    yield* completeOutbound(agentKey, input.outbound).pipe(
      Effect.mapError(mapPersistenceError),
    );
    return { kind: "accepted" };
  });
}

function digestCanonicalSignedMessage(
  canonicalSignedMessage: Uint8Array,
): Effect.Effect<typeof SignedMessageDigest.Type, RouterWorkerProtocolError> {
  return Effect.try({
    try: () => createHash("sha256").update(canonicalSignedMessage).digest(),
    catch: mapProtocolError,
  }).pipe(
    Effect.map((digest) => `smd_${Encoding.encodeBase64Url(digest)}`),
    Effect.flatMap(Schema.decodeUnknown(SignedMessageDigest)),
    Effect.mapError(mapProtocolError),
  );
}

function decodeStoredOutbound(
  outbound: StoredOutboundMessage,
): Effect.Effect<LiveSignedMessage, RouterWorkerProtocolError> {
  return decodeCanonical(
    SignedMessage.live,
    outbound.canonicalSignedMessage,
  ).pipe(
    Effect.mapError(mapProtocolError),
    Effect.flatMap((message) =>
      message.envelopeId === outbound.envelopeId
        ? Effect.succeed(message)
        : Effect.fail(new RouterWorkerProtocolError()),
    ),
  );
}

function triggerDiscontinuity<Payload>(
  runtime: RouterWorkerRuntime<Payload>,
  reason: RouterDiscontinuityReason,
  observedGeneration: number,
): Effect.Effect<void, RouterWorkerPollError> {
  return runtime.recoveryGate.withPermits(1)(
    Effect.gen(function* () {
      const current = yield* Ref.get(runtime.state);
      if (
        current.kind !== "recovering" &&
        current.generation !== observedGeneration
      ) {
        return;
      }
      if (current.kind === "recovering") {
        return yield* finishRouterRecovery(runtime, current);
      }
      const recovering: RouterWorkerRecoveringState = {
        kind: "recovering",
        generation: current.generation + 1,
        reason,
        priorRouterInstanceId: current.anchor.routerInstanceId,
        volatileFoldsAbandoned: false,
        unreachableSince: Option.none(),
      };
      yield* runtime.stateGate.withPermits(1)(
        Ref.set(runtime.state, recovering),
      );
      if (current.kind === "detached") {
        yield* logRecoveryAfterLoss(recovering);
      }
      return yield* finishRouterRecovery(runtime, recovering);
    }),
  );
}

/**
 * Warn that a worker that had lost the Router now recovers from a
 * discontinuity its first answered probe revealed.
 * @param state The new recovering state.
 * @returns Completion once logged.
 */
function logRecoveryAfterLoss(
  state: RouterWorkerRecoveringState,
): Effect.Effect<void> {
  return Effect.logWarning(
    `Router answered after a loss with ${state.reason}: recovering`,
  ).pipe(Effect.annotateLogs({ generation: state.generation }));
}

/**
 * Recovery fences conversations and queues catch-up without waiting for
 * answers. Polling starts only after activation, so retained traffic reaches
 * the active path.
 */
function finishRouterRecovery<Payload>(
  runtime: RouterWorkerRuntime<Payload>,
  recovering: RouterWorkerRecoveringState,
): Effect.Effect<void, RouterWorkerPollError> {
  return Effect.gen(function* () {
    const anchor = yield* pollRouterTail(runtime, runtime.input);
    const anchored = yield* anchorRecovery(runtime, recovering, anchor);
    const prepared = yield* prepareRecovery(runtime, anchored);
    yield* runtime.input.callbacks.recoverCertifiedHistory({
      reason: prepared.reason,
      anchor: prepared.anchor,
    });
    yield* runtime.stateGate.withPermits(1)(
      Effect.gen(function* () {
        const current = yield* Ref.get(runtime.state);
        if (
          !matchesRecovery(
            current,
            prepared.generation,
            prepared.anchor.routerInstanceId,
          )
        ) {
          return yield* Effect.fail(new RouterWorkerDiscontinuityError());
        }
        yield* Ref.set(runtime.state, {
          kind: "active",
          generation: prepared.generation,
          anchor: current.anchor,
        });
        yield* logRecoveryComplete(current);
      }),
    );
  }).pipe(Effect.withSpan("finishRouterRecovery"));
}

/**
 * Note that certified-history recovery finished and the worker is active.
 * @param state The recovering state recovery completed.
 * @returns Completion once logged.
 */
function logRecoveryComplete(
  state: RouterWorkerRecoveringState,
): Effect.Effect<void> {
  return Effect.logInfo(`Router recovery complete after ${state.reason}`).pipe(
    Effect.annotateLogs({ generation: state.generation }),
  );
}

/**
 * Learn one Router instance and empty-tail cursor without replay.
 * @param services Resolved Router and Registry capabilities.
 * @param caller Authenticated local endpoint identity.
 * @returns The immediate empty-tail Router position.
 */
function pollRouterTail(
  services: RouterWorkerServices,
  caller: Pick<
    RouterWorkerInput<unknown>,
    "callerAgentId" | "signingAuthority"
  >,
): Effect.Effect<
  RouterTailAnchor,
  RouterCallFailure | RouterWorkerProtocolError
> {
  return services.router
    .poll({
      request: {},
      callerAgentId: caller.callerAgentId,
      signingAuthority: caller.signingAuthority,
    })
    .pipe(
      Effect.mapError(mapRouterFailure),
      Effect.retry(pollBlipRetry),
      Effect.flatMap(anchorFromOmittedResult),
      Effect.interruptible,
    );
}

function matchesRecovery(
  state: RouterWorkerState,
  generation: number,
  routerInstanceId: RouterInstanceId,
): state is AnchoredRecoveringState {
  return (
    state.kind === "recovering" &&
    state.generation === generation &&
    state.anchor?.routerInstanceId === routerInstanceId
  );
}

function prepareRecovery<Payload>(
  runtime: RouterWorkerRuntime<Payload>,
  recovering: AnchoredRecoveringState,
): Effect.Effect<AnchoredRecoveringState, RouterWorkerPollError> {
  if (recovering.volatileFoldsAbandoned) {
    return Effect.succeed(recovering);
  }
  return runtime.input.callbacks.abandonVolatileFolds(recovering.reason).pipe(
    Effect.zipRight(
      runtime.stateGate.withPermits(1)(
        Effect.gen(function* () {
          const current = yield* Ref.get(runtime.state);
          if (
            current.kind !== "recovering" ||
            current.generation !== recovering.generation ||
            current.anchor === undefined
          ) {
            return yield* Effect.fail(new RouterWorkerDiscontinuityError());
          }
          const prepared: AnchoredRecoveringState = {
            ...current,
            anchor: current.anchor,
            volatileFoldsAbandoned: true,
          };
          yield* Ref.set(runtime.state, prepared);
          return prepared;
        }),
      ),
    ),
  );
}

function anchorRecovery<Payload>(
  runtime: RouterWorkerRuntime<Payload>,
  recovering: RouterWorkerRecoveringState,
  anchor: RouterTailAnchor,
): Effect.Effect<AnchoredRecoveringState, RouterWorkerPollError> {
  return runtime.stateGate.withPermits(1)(
    Effect.gen(function* () {
      const current = yield* Ref.get(runtime.state);
      if (
        current.kind !== "recovering" ||
        current.generation !== recovering.generation
      ) {
        return yield* Effect.fail(new RouterWorkerDiscontinuityError());
      }
      const reason =
        current.priorRouterInstanceId !== undefined &&
        current.priorRouterInstanceId !== anchor.routerInstanceId
          ? "router_restarted"
          : current.reason;
      const anchored: AnchoredRecoveringState = {
        ...current,
        reason,
        priorRouterInstanceId:
          current.priorRouterInstanceId ?? anchor.routerInstanceId,
        volatileFoldsAbandoned:
          current.reason === reason && current.volatileFoldsAbandoned,
        anchor,
        unreachableSince: Option.none(),
      };
      yield* Ref.set(runtime.state, anchored);
      return anchored;
    }),
  );
}

function transportFailure() {
  return new RouterWorkerTransportError();
}

function rejectedFailure(reason: RouterWorkerRejectedError["reason"]) {
  return () => new RouterWorkerRejectedError({ reason });
}

function logDetached(state: RouterWorkerDetachedState): Effect.Effect<void> {
  return Effect.logWarning(
    "Router unreachable: worker detached; sends wait until it answers",
  ).pipe(
    Effect.annotateLogs({
      generation: state.generation,
      routerInstanceId: state.anchor.routerInstanceId,
    }),
  );
}

function logReattached(state: RouterWorkerDetachedState): Effect.Effect<void> {
  return secondsSince(state.detachedAt).pipe(
    Effect.flatMap((seconds) =>
      Effect.logInfo(
        `Router answered: worker reattached after ${String(seconds)} s`,
      ).pipe(
        Effect.annotateLogs({
          generation: state.generation,
          routerInstanceId: state.anchor.routerInstanceId,
          detachedSeconds: seconds,
        }),
      ),
    ),
  );
}

function stallOf(state: RouterWorkerState): Option.Option<Stall> {
  switch (state.kind) {
    case "active":
      return Option.none();
    case "detached":
      return Option.some({
        kind: "detached",
        generation: state.generation,
        since: state.detachedAt,
      });
    case "recovering":
      return Option.map(state.unreachableSince, (since) => ({
        kind: "recovering",
        generation: state.generation,
        since,
      }));
    default: {
      const exhaustive: never = state;
      return exhaustive;
    }
  }
}

function logStillStalled(stall: Stall): Effect.Effect<void> {
  return secondsSince(stall.since).pipe(
    Effect.flatMap((seconds) =>
      Effect.logWarning(
        `Router still unreachable: ${stallText[stall.kind]} for ${String(seconds)} s`,
      ).pipe(
        Effect.annotateLogs({
          generation: stall.generation,
          unreachableSeconds: seconds,
        }),
      ),
    ),
  );
}

function secondsSince(since: number): Effect.Effect<number> {
  return Clock.currentTimeMillis.pipe(
    Effect.map((now) => Math.round((now - since) / 1000)),
  );
}

function markUnreachable(
  state: RouterWorkerState,
  error: RouterWorkerPollError,
  now: number,
): RouterWorkerState {
  return state.kind === "recovering" &&
    isTransportFailure(error) &&
    Option.isNone(state.unreachableSince)
    ? { ...state, unreachableSince: Option.some(now) }
    : state;
}

/**
 * Whether a worker failure is a Router outage rather than a rejection.
 * @param error Any tagged worker failure.
 * @returns True for `RouterWorkerTransportError`.
 */
function isTransportFailure(error: TaggedFailure): boolean {
  return error._tag === "RouterWorkerTransportError";
}

function logRecoveryAttemptFailed(
  state: RouterWorkerState,
  error: RouterWorkerPollError,
): Effect.Effect<void> {
  return state.kind === "recovering"
    ? Effect.logWarning(
        `Router recovery attempt failed, retrying: ${describeRouterWorkerFailure(error)}`,
      ).pipe(
        Effect.annotateLogs({
          generation: state.generation,
          reason: state.reason,
        }),
      )
    : Effect.void;
}
