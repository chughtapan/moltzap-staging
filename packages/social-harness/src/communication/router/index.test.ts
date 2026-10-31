/** @file Scripted Router worker ordering, cursor, replay, and recovery laws. */

import type { SqlClient } from "@effect/sql";
import { live as it } from "@effect/vitest";
import {
  type Context,
  Deferred,
  Effect,
  Encoding,
  Fiber,
  Layer,
  Logger,
  Option,
  Ref,
  Schema,
  TestContext,
} from "effect";
import { createHash } from "node:crypto";
import { describe, expect } from "vitest";
import type { Registry } from "../../identity/registry/index.js";
import type {
  ConversationFoundation,
  StoredOutboundMessage,
} from "../store/index.js";
import { advanceClock } from "../../__tests__/advance-clock.js";
import {
  batch,
  corruptSignature,
  emptyBatch,
  type Fixture,
  makeFixture,
  makeIdentityFixture,
  makeScriptedRouter,
  pollCursor,
  registryLayer,
  routerInstanceId,
  type ScriptedSendAnswer,
  signMessage,
  type TestPayload,
  unavailableRegistryLayer,
} from "../../__tests__/router-worker-fixtures.js";
import {
  stateDirectory,
  type TestStore,
  withDaemonStore,
} from "../../__tests__/store-schema-fixtures.js";
import { encodeCanonical } from "../../encoding/canonical-json.js";
import {
  SignedMessage,
  type SignedMessage as SignedMessageValue,
} from "../../identity/envelope/signed-message.js";
import {
  AuthenticationFailedError,
  OverloadedError,
  UnavailableError,
  VersionMismatchError,
} from "../../identity/signing/http-errors.js";
import { Router } from "./api/client.js";
import {
  RouterConnectionError,
  type RouterSendRequest,
  type RouterSendResult,
  SignedMessageDigest,
} from "./api/contract.js";
import {
  makeRouterWorker,
  RouterWorkerAuthenticationError,
  type RouterWorkerCallbacks,
  RouterWorkerDiscontinuityError,
  type RouterWorkerInput,
  RouterWorkerPayloadInvalidError,
  RouterWorkerPersistenceError,
  RouterWorkerProtocolError,
  type RouterWorkerRecovery,
  type RouterWorkerRecoveryError,
  RouterWorkerRejectedError,
  RouterWorkerTransportError,
  RouterWorkerUnavailableError,
} from "./index.js";

/** A Router client failure a scripted poll can raise. */
type RouterClientFailure = Effect.Effect.Error<
  ReturnType<Context.Tag.Service<typeof Router>["poll"]>
>;

/* eslint-disable max-lines, max-lines-per-function, @typescript-eslint/no-invalid-void-type -- The scripted scenarios keep each Router trace and its exact ordering assertions together, and their latch Deferreds carry no value. */

const callbacks = (input?: {
  readonly accepted?: Ref.Ref<string[]>;
  readonly acceptedRouterInstances?: Ref.Ref<string[]>;
  readonly events?: Ref.Ref<string[]>;
  readonly invalidText?: string;
  readonly failAcceptText?: string;
  readonly recover?: (
    input: RouterWorkerRecovery,
  ) => Effect.Effect<void, RouterWorkerRecoveryError>;
}): RouterWorkerCallbacks<TestPayload> => ({
  pinSenderCard: () => Effect.void,
  decodePayload: (message) => {
    const text = new TextDecoder().decode(message.body);
    return text === input?.invalidText
      ? Effect.fail(new RouterWorkerPayloadInvalidError())
      : Effect.succeed({ text });
  },
  acceptPayload: (acceptedInput) =>
    acceptedInput.payload.text === input?.failAcceptText
      ? Effect.fail(new RouterWorkerPersistenceError())
      : Effect.gen(function* () {
          if (input?.accepted !== undefined) {
            yield* Ref.update(input.accepted, (values) => [
              ...values,
              acceptedInput.payload.text,
            ]);
          }
          if (input?.acceptedRouterInstances !== undefined) {
            yield* Ref.update(input.acceptedRouterInstances, (values) => [
              ...values,
              acceptedInput.routerInstanceId,
            ]);
          }
          return "accepted" as const;
        }),
  abandonVolatileFolds: (reason) =>
    input?.events === undefined
      ? Effect.void
      : Ref.update(input.events, (events) => [...events, `abandon:${reason}`]),
  recoverCertifiedHistory: (recoveryInput) => {
    const record =
      input?.events === undefined
        ? Effect.void
        : Ref.update(input.events, (events) => [
            ...events,
            `recover:${recoveryInput.reason}`,
          ]);
    return record.pipe(
      Effect.zipRight(
        input?.recover === undefined
          ? Effect.void
          : input.recover(recoveryInput),
      ),
    );
  },
  reattached: () =>
    input?.events === undefined
      ? Effect.void
      : Ref.update(input.events, (events) => [...events, "reattached"]),
});

const makeInput = (
  fixture: Fixture,
  workerCallbacks: RouterWorkerCallbacks<TestPayload>,
): RouterWorkerInput<TestPayload> => ({
  callerAgentId: fixture.localCard.agentId,
  callerAgentCard: fixture.localCard,
  pinnedSenderCards: [fixture.localCard],
  signingAuthority: fixture.localAuthority,
  callbacks: workerCallbacks,
});

/**
 * Build a worker and bring it to `active` through its cold-start recovery
 * without spending the scenario's script or callbacks on that recovery. The
 * recovery's omitted-cursor tail probe consumes the first scripted poll.
 * Until the worker is active, the cold-start `abandonVolatileFolds` and
 * `recoverCertifiedHistory` callbacks do nothing, so they record no events.
 * Afterwards every callback passes through.
 * @param input Worker input whose callbacks the scenario observes.
 * @returns An active worker that has consumed exactly one scripted poll.
 */
const makeActiveRouterWorker = (input: RouterWorkerInput<TestPayload>) =>
  Effect.gen(function* () {
    const activating = yield* Ref.make(true);
    const configured = input.callbacks;
    const worker = yield* makeRouterWorker({
      ...input,
      callbacks: {
        ...configured,
        abandonVolatileFolds: (reason) =>
          configured
            .abandonVolatileFolds(reason)
            .pipe(Effect.unlessEffect(Ref.get(activating)), Effect.asVoid),
        recoverCertifiedHistory: (recovery) =>
          configured
            .recoverCertifiedHistory(recovery)
            .pipe(Effect.unlessEffect(Ref.get(activating)), Effect.asVoid),
      },
    });
    yield* worker.pollOnce;
    yield* Ref.set(activating, false);
    return worker;
  });

const provide = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
  routerLayer: Layer.Layer<Router>,
  fixture: Fixture,
) =>
  effect.pipe(
    Effect.provide(routerLayer),
    Effect.provide(registryLayer([fixture.localCard])),
  );

const withOutbox = <Value, Failure, Requirements>(
  use: (store: TestStore) => Effect.Effect<Value, Failure, Requirements>,
) => withDaemonStore(stateDirectory(), use);

function prepareOutbound(
  store: TestStore,
  conversationId: string,
  message: SignedMessageValue,
): Effect.Effect<StoredOutboundMessage, never, SqlClient.SqlClient> {
  return store.putConversationFoundation(routerFoundation(conversationId)).pipe(
    Effect.orDie,
    Effect.zipRight(encodeCanonical(SignedMessage, message).pipe(Effect.orDie)),
    Effect.flatMap((canonicalSignedMessage) =>
      store
        .enqueueOutbound({
          conversationId,
          envelopeId: message.envelopeId,
          canonicalSignedMessage,
          wireVersion: SignedMessage.wireVersion(message),
        })
        .pipe(Effect.orDie),
    ),
  );
}

function routerFoundation(conversationId: string): ConversationFoundation {
  return {
    conversationId,
    membershipHash: `mbr_${conversationId}`,
    canonicalMembership: new TextEncoder().encode(
      `membership:${conversationId}`,
    ),
    anchorHash: `anc_${conversationId}`,
    canonicalAnchor: new TextEncoder().encode(`anchor:${conversationId}`),
  };
}

function acceptedResult(
  instance: ReturnType<typeof routerInstanceId>,
  message: SignedMessageValue,
): Effect.Effect<RouterSendResult> {
  return encodeCanonical(SignedMessage, message).pipe(
    Effect.orDie,
    Effect.map((canonicalSignedMessage) =>
      createHash("sha256").update(canonicalSignedMessage).digest(),
    ),
    Effect.map((digest) =>
      Schema.decodeUnknownSync(SignedMessageDigest)(
        `smd_${Encoding.encodeBase64Url(digest)}`,
      ),
    ),
    Effect.map((signedMessageDigest) => ({
      kind: "accepted" as const,
      routerInstanceId: instance,
      signedMessageDigest,
    })),
  );
}

/** Router instance of every scripted-send scenario. */
const scriptedSendInstance = routerInstanceId(50);

const connectionLost: ScriptedSendAnswer = () =>
  Effect.fail(new RouterConnectionError());

const identityUnknown: ScriptedSendAnswer = () =>
  Effect.succeed({ kind: "retry_identity_unknown" });

const identityConflict: ScriptedSendAnswer = () =>
  Effect.succeed({ kind: "idempotency_conflict" });

const messageInvalid: ScriptedSendAnswer = () =>
  Effect.succeed({ kind: "message_invalid" });

const acceptsSentBytes: ScriptedSendAnswer = (request) =>
  acceptedResult(scriptedSendInstance, request.signedMessage);

const acceptsDifferentBytes: ScriptedSendAnswer = () =>
  Effect.succeed({
    kind: "accepted",
    routerInstanceId: scriptedSendInstance,
    signedMessageDigest: Schema.decodeUnknownSync(SignedMessageDigest)(
      `smd_${createHash("sha256").update("different bytes").digest("base64url")}`,
    ),
  });

/**
 * Answers for a Router that keeps retry identities as the real feed does and
 * whose first `initial` is slow. The worker loses that connection while the
 * `initial` is still in flight, and the original appends only once the
 * worker's resent `initial` has reached Router as well, so the original wins
 * the race. A `retry` answers with the retained original, so the worker's
 * digest check compares the original's bytes with its stored ones.
 * @returns The four answers in arrival order and the identities Router keeps.
 */
function slowOriginalRouter() {
  return Effect.gen(function* () {
    const retained = yield* Ref.make(new Map<string, SignedMessageValue>());
    const resendArrived = yield* Deferred.make<void>();
    const originalAppended = yield* Deferred.make<void>();
    const answerFromRetained: ScriptedSendAnswer = (request) =>
      Ref.modify(
        retained,
        (
          entries: Map<string, SignedMessageValue>,
        ): readonly [
          Effect.Effect<RouterSendResult>,
          Map<string, SignedMessageValue>,
        ] => {
          const message = request.signedMessage;
          const entry = entries.get(message.envelopeId);
          if (request.mode === "retry") {
            return entry === undefined
              ? [Effect.succeed({ kind: "retry_identity_unknown" }), entries]
              : [acceptedResult(scriptedSendInstance, entry), entries];
          }
          return entry === undefined
            ? [
                acceptedResult(scriptedSendInstance, message),
                new Map(entries).set(message.envelopeId, message),
              ]
            : [Effect.succeed({ kind: "idempotency_conflict" }), entries];
        },
      ).pipe(Effect.flatten);
    const slowOriginal: ScriptedSendAnswer = (request) =>
      Deferred.await(resendArrived).pipe(
        Effect.zipRight(answerFromRetained(request)),
        Effect.zipRight(Deferred.succeed(originalAppended, undefined)),
        Effect.forkDaemon,
        Effect.zipRight(Effect.fail(new RouterConnectionError())),
      );
    const resendBehindOriginal: ScriptedSendAnswer = (request) =>
      Deferred.succeed(resendArrived, undefined).pipe(
        Effect.zipRight(Deferred.await(originalAppended)),
        Effect.zipRight(answerFromRetained(request)),
      );
    return {
      answers: [
        slowOriginal,
        answerFromRetained,
        resendBehindOriginal,
        answerFromRetained,
      ],
      retained,
    };
  });
}

/**
 * Sends one stored envelope through a scripted Router and reports what went
 * out and what the outbox retains afterwards.
 * @param answers Send answers in arrival order; a send past them dies.
 * @returns The stored envelope, every request's mode and canonical bytes, the
 *   send's failure if any, and the outbox's pending rows.
 */
function sendThroughScript(answers: readonly ScriptedSendAnswer[]) {
  return withOutbox((store) =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture;
      const outgoing = yield* signMessage({
        card: fixture.localCard,
        authority: fixture.localAuthority,
        recipient: fixture.localCard.agentId,
        id: 50,
        body: "stored-envelope",
      });
      const outbound = yield* prepareOutbound(
        store,
        "conversation:scripted-send",
        outgoing,
      );
      const router = yield* makeScriptedRouter({
        polls: [],
        fallbackPoll: Effect.succeed(
          emptyBatch(scriptedSendInstance, pollCursor(8)),
        ),
        sends: answers,
      });
      const worker = yield* provide(
        makeActiveRouterWorker(makeInput(fixture, callbacks())),
        router.layer,
        fixture,
      );
      const failure = yield* provide(
        worker.send(outbound.envelopeId).pipe(Effect.flip, Effect.option),
        router.layer,
        fixture,
      );
      const sent = yield* Ref.get(router.scripted.sendCalls);
      const sentBytes = yield* Effect.forEach(
        sent,
        ({ request }) =>
          encodeCanonical(SignedMessage, request.signedMessage).pipe(
            Effect.orDie,
          ),
        { concurrency: 1 },
      );
      return {
        outbound,
        modes: sent.map(({ request }) => request.mode),
        sentBytes,
        failure,
        pending: (yield* store.recover()).outboundMessages,
      };
    }),
  );
}

const batchVerificationIsAtomic = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const valid = yield* signMessage({
      card: fixture.localCard,
      authority: fixture.localAuthority,
      recipient: fixture.localCard.agentId,
      id: 10,
      body: "valid",
    });
    const invalid = yield* signMessage({
      card: fixture.localCard,
      authority: fixture.localAuthority,
      recipient: fixture.localCard.agentId,
      id: 11,
      body: "invalid-signature",
    }).pipe(
      Effect.flatMap(corruptSignature),
      Effect.flatMap(Schema.encode(SignedMessage)),
      Effect.flatMap(Schema.decodeUnknown(SignedMessage.live)),
      Effect.orDie,
    );
    const accepted = yield* Ref.make<string[]>([]);
    const firstInstance = routerInstanceId(10);
    const router = yield* makeScriptedRouter({
      polls: [
        emptyBatch(firstInstance, pollCursor(1)),
        batch(firstInstance, pollCursor(2), [valid, invalid]),
      ],
    });
    const worker = yield* provide(
      makeActiveRouterWorker(makeInput(fixture, callbacks({ accepted }))),
      router.layer,
      fixture,
    );
    const error = yield* provide(
      worker.pollOnce.pipe(Effect.flip),
      router.layer,
      fixture,
    );
    expect(error).toStrictEqual(new RouterWorkerAuthenticationError());
    expect(yield* Ref.get(accepted)).toEqual([]);
    expect((yield* worker.currentAnchor).pollCursor).toBe(pollCursor(1));
  });

const orderedCursorCommit = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const accepted = yield* Ref.make<string[]>([]);
    const acceptedRouterInstances = yield* Ref.make<string[]>([]);
    const messages = yield* Effect.forEach(
      ["one", "invalid", "two"],
      (text, index) =>
        signMessage({
          card: fixture.localCard,
          authority: fixture.localAuthority,
          recipient: fixture.localCard.agentId,
          id: 20 + index,
          body: text,
        }),
      { concurrency: 1 },
    );
    const instance = routerInstanceId(20);
    const router = yield* makeScriptedRouter({
      polls: [
        emptyBatch(instance, pollCursor(3)),
        batch(instance, pollCursor(4), messages),
      ],
    });
    const worker = yield* provide(
      makeActiveRouterWorker(
        makeInput(
          fixture,
          callbacks({
            accepted,
            acceptedRouterInstances,
            invalidText: "invalid",
          }),
        ),
      ),
      router.layer,
      fixture,
    );
    yield* provide(worker.pollOnce, router.layer, fixture);
    expect(yield* Ref.get(accepted)).toEqual(["one", "two"]);
    expect(yield* Ref.get(acceptedRouterInstances)).toEqual([
      instance,
      instance,
    ]);
    expect((yield* worker.currentAnchor).pollCursor).toBe(pollCursor(4));
  });

const persistenceRetainsCursor = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const message = yield* signMessage({
      card: fixture.localCard,
      authority: fixture.localAuthority,
      recipient: fixture.localCard.agentId,
      id: 30,
      body: "persist-fails",
    });
    const instance = routerInstanceId(30);
    const router = yield* makeScriptedRouter({
      polls: [
        emptyBatch(instance, pollCursor(5)),
        batch(instance, pollCursor(6), [message]),
      ],
    });
    const worker = yield* provide(
      makeActiveRouterWorker(
        makeInput(fixture, callbacks({ failAcceptText: "persist-fails" })),
      ),
      router.layer,
      fixture,
    );
    const error = yield* provide(
      worker.pollOnce.pipe(Effect.flip),
      router.layer,
      fixture,
    );
    expect(error).toStrictEqual(new RouterWorkerPersistenceError());
    expect((yield* worker.currentAnchor).pollCursor).toBe(pollCursor(5));
  });

const ambiguousSendRetriesSameBytes = () =>
  Effect.gen(function* () {
    const result = yield* sendThroughScript([connectionLost, acceptsSentBytes]);
    const stored = result.outbound.canonicalSignedMessage;
    expect(result.failure).toEqual(Option.none());
    expect(result.modes).toEqual(["initial", "retry"]);
    expect(result.sentBytes).toEqual([stored, stored]);
    expect(result.pending).toEqual([]);
  });

const retryUnknownResendsStoredBytes = () =>
  Effect.gen(function* () {
    const result = yield* sendThroughScript([
      connectionLost,
      identityUnknown,
      acceptsSentBytes,
    ]);
    const stored = result.outbound.canonicalSignedMessage;
    expect(result.failure).toEqual(Option.none());
    expect(result.modes).toEqual(["initial", "retry", "initial"]);
    expect(result.sentBytes).toEqual([stored, stored, stored]);
    expect(result.pending).toEqual([]);
  });

const resentInitialLosesRaceToSlowOriginal = () =>
  Effect.gen(function* () {
    const router = yield* slowOriginalRouter();
    const result = yield* sendThroughScript(router.answers);
    const retained = yield* Ref.get(router.retained);
    const stored = result.outbound.canonicalSignedMessage;
    expect(result.failure).toEqual(Option.none());
    expect(result.modes).toEqual(["initial", "retry", "initial", "retry"]);
    expect(result.sentBytes).toEqual([stored, stored, stored, stored]);
    expect([...retained.keys()]).toEqual([result.outbound.envelopeId]);
    expect(result.pending).toEqual([]);
  });

/** A Router result the worker must not resend after, and its scenario. */
interface FailClosedRow {
  readonly result: string;
  readonly answers: readonly ScriptedSendAnswer[];
  readonly modes: ReadonlyArray<RouterSendRequest["mode"]>;
}

/**
 * A conflict to a retry, identity loss to an initial, or an invalid message
 * fails the send closed: the worker sends nothing more and keeps the envelope
 * pending. An acceptance the worker must never reach follows that result, so
 * a resend shows in the asserted modes rather than as an exhausted script.
 * @param row Send answers in arrival order and the modes the worker sends
 *   before it fails closed.
 * @returns Completion once the send has failed closed.
 */
const resultFailsClosed = (row: FailClosedRow) =>
  Effect.gen(function* () {
    const result = yield* sendThroughScript(row.answers);
    expect(result.failure).toEqual(
      Option.some(new RouterWorkerProtocolError()),
    );
    expect(result.modes).toEqual(row.modes);
    expect(result.pending).toEqual([result.outbound]);
  });

/**
 * Each identity loss spends one of the three attempts, so a Router that
 * alternates conflict and identity loss gets six sends and never reaches the
 * acceptance scripted seventh. Running out is a transport failure, so the
 * envelope waits for the next drain.
 */
const alternatingConflictAndLossStopsAtTheBound = () =>
  Effect.gen(function* () {
    const result = yield* sendThroughScript([
      identityConflict,
      identityUnknown,
      identityConflict,
      identityUnknown,
      identityConflict,
      identityUnknown,
      acceptsSentBytes,
    ]);
    expect(result.failure).toEqual(
      Option.some(new RouterWorkerTransportError()),
    );
    expect(result.modes).toEqual([
      "initial",
      "retry",
      "initial",
      "retry",
      "initial",
      "retry",
    ]);
    expect(result.pending).toEqual([result.outbound]);
  });

const mismatchedAcceptedDigestRetainsOutbound = () =>
  Effect.gen(function* () {
    const result = yield* sendThroughScript([acceptsDifferentBytes]);
    expect(result.failure).toEqual(
      Option.some(new RouterWorkerProtocolError()),
    );
    expect(result.pending).toEqual([result.outbound]);
  });

const restartedSendRecoversBeforeReturning = () =>
  withOutbox((store) =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture;
      const outgoing = yield* signMessage({
        card: fixture.localCard,
        authority: fixture.localAuthority,
        recipient: fixture.localCard.agentId,
        id: 55,
        body: "restart-fenced",
      });
      const outbound = yield* prepareOutbound(
        store,
        "conversation:restart-send",
        outgoing,
      );
      const events = yield* Ref.make<string[]>([]);
      const oldInstance = routerInstanceId(55);
      const newInstance = routerInstanceId(56);
      const router = yield* makeScriptedRouter({
        polls: [
          emptyBatch(oldInstance, pollCursor(18)),
          emptyBatch(newInstance, pollCursor(19)),
        ],
        sends: [{ kind: "router_restarted", routerInstanceId: newInstance }],
      });
      const worker = yield* provide(
        makeActiveRouterWorker(makeInput(fixture, callbacks({ events }))),
        router.layer,
        fixture,
      );
      const error = yield* provide(
        worker.send(outbound.envelopeId).pipe(Effect.flip),
        router.layer,
        fixture,
      );
      expect(error).toStrictEqual(new RouterWorkerDiscontinuityError());
      expect(yield* Ref.get(events)).toEqual([
        "abandon:router_restarted",
        "recover:router_restarted",
      ]);
      expect(yield* worker.currentAnchor).toEqual({
        routerInstanceId: newInstance,
        pollCursor: pollCursor(19),
      });
      expect((yield* store.recover()).outboundMessages).toEqual([outbound]);
    }),
  );

const restartOrdersTailBeforeRecovery = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const events = yield* Ref.make<string[]>([]);
    const recoveryStarted = yield* Deferred.make<RouterWorkerRecovery>();
    const releaseRecovery = yield* Deferred.make<void>();
    const oldInstance = routerInstanceId(60);
    const newInstance = routerInstanceId(61);
    const router = yield* makeScriptedRouter({
      polls: [
        emptyBatch(oldInstance, pollCursor(9)),
        { kind: "cursor_invalid" },
        emptyBatch(newInstance, pollCursor(10)),
      ],
      fallbackPoll: Effect.never,
    });
    const worker = yield* provide(
      makeActiveRouterWorker(
        makeInput(
          fixture,
          callbacks({
            events,
            recover: (recovery) =>
              Deferred.succeed(recoveryStarted, recovery).pipe(
                Effect.zipRight(Deferred.await(releaseRecovery)),
              ),
          }),
        ),
      ),
      router.layer,
      fixture,
    );
    const polling = yield* provide(
      Effect.fork(worker.pollOnce),
      router.layer,
      fixture,
    );
    const recovery = yield* Deferred.await(recoveryStarted);
    const unavailable = yield* worker.currentAnchor.pipe(Effect.flip);
    expect(unavailable).toStrictEqual(new RouterWorkerUnavailableError());
    expect(recovery).toMatchObject({
      reason: "router_restarted",
      anchor: {
        routerInstanceId: newInstance,
        pollCursor: pollCursor(10),
      },
    });
    expect(yield* Ref.get(events)).toEqual([
      "abandon:router_restarted",
      "recover:router_restarted",
    ]);
    yield* Deferred.succeed(releaseRecovery, undefined);
    yield* Fiber.join(polling);
    expect(yield* worker.currentAnchor).toEqual({
      routerInstanceId: newInstance,
      pollCursor: pollCursor(10),
    });
  });

const recoveryRetryPromotesChangedTailToRestart = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const events = yield* Ref.make<string[]>([]);
    const recoveryAttempts = yield* Ref.make(0);
    const recoveryStarted = yield* Deferred.make<RouterWorkerRecovery>();
    const releaseRecovery = yield* Deferred.make<void>();
    const oldInstance = routerInstanceId(64);
    const newInstance = routerInstanceId(65);
    const router = yield* makeScriptedRouter({
      polls: [
        emptyBatch(oldInstance, pollCursor(26)),
        { kind: "feed_gap", routerInstanceId: oldInstance },
        emptyBatch(oldInstance, pollCursor(27)),
        emptyBatch(newInstance, pollCursor(28)),
        emptyBatch(newInstance, pollCursor(29)),
      ],
      fallbackPoll: Effect.never,
    });
    const worker = yield* provide(
      makeActiveRouterWorker(
        makeInput(
          fixture,
          callbacks({
            events,
            recover: (recovery) =>
              Ref.getAndUpdate(
                recoveryAttempts,
                (attempts) => attempts + 1,
              ).pipe(
                Effect.flatMap((attempt) =>
                  attempt === 0
                    ? Effect.void
                    : Deferred.succeed(recoveryStarted, recovery).pipe(
                        Effect.zipRight(Deferred.await(releaseRecovery)),
                      ),
                ),
              ),
          }),
        ),
      ),
      router.layer,
      fixture,
    );

    yield* provide(worker.pollOnce, router.layer, fixture);
    expect(yield* worker.currentAnchor).toEqual({
      routerInstanceId: oldInstance,
      pollCursor: pollCursor(27),
    });

    const restart = yield* provide(
      Effect.fork(worker.pollOnce),
      router.layer,
      fixture,
    );
    const recovery = yield* Deferred.await(recoveryStarted);
    const unavailable = yield* worker.currentAnchor.pipe(Effect.flip);
    expect(unavailable).toStrictEqual(new RouterWorkerUnavailableError());
    expect(recovery).toMatchObject({
      reason: "router_restarted",
      anchor: {
        routerInstanceId: newInstance,
        pollCursor: pollCursor(29),
      },
    });
    expect(yield* Ref.get(events)).toEqual([
      "abandon:feed_gap",
      "recover:feed_gap",
      "abandon:router_restarted",
      "recover:router_restarted",
    ]);

    yield* Deferred.succeed(releaseRecovery, undefined);
    yield* Fiber.join(restart);
    expect(yield* worker.currentAnchor).toEqual({
      routerInstanceId: newInstance,
      pollCursor: pollCursor(29),
    });
  });

const normalSendWaitsForRecovery = () =>
  withOutbox((store) =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture;
      const outgoing = yield* signMessage({
        card: fixture.localCard,
        authority: fixture.localAuthority,
        recipient: fixture.localCard.agentId,
        id: 63,
        body: "recovery-gated-send",
      });
      const outbound = yield* prepareOutbound(
        store,
        "conversation:recovery-gated-send",
        outgoing,
      );
      const recoveryStarted = yield* Deferred.make<void>();
      const releaseRecovery = yield* Deferred.make<void>();
      const instance = routerInstanceId(63);
      const accepted = yield* acceptedResult(instance, outgoing);
      const router = yield* makeScriptedRouter({
        polls: [
          emptyBatch(instance, pollCursor(24)),
          { kind: "cursor_invalid" },
          emptyBatch(instance, pollCursor(25)),
        ],
        sends: [accepted],
        fallbackPoll: Effect.never,
      });
      const worker = yield* provide(
        makeActiveRouterWorker(
          makeInput(
            fixture,
            callbacks({
              recover: () =>
                Deferred.succeed(recoveryStarted, undefined).pipe(
                  Effect.zipRight(Deferred.await(releaseRecovery)),
                ),
            }),
          ),
        ),
        router.layer,
        fixture,
      );
      const recovering = yield* provide(
        Effect.fork(worker.pollOnce),
        router.layer,
        fixture,
      );
      yield* Deferred.await(recoveryStarted);
      const sending = yield* provide(
        Effect.fork(worker.send(outbound.envelopeId)),
        router.layer,
        fixture,
      );
      yield* Effect.yieldNow();
      expect(yield* Ref.get(router.scripted.sendCalls)).toEqual([]);
      yield* Deferred.succeed(releaseRecovery, undefined);
      yield* Fiber.join(recovering);
      yield* Fiber.join(sending);
      expect(yield* Ref.get(router.scripted.sendCalls)).toHaveLength(1);
      expect((yield* store.recover()).outboundMessages).toEqual([]);
    }),
  );

const ingressHeldDuringRecoveryIsAcceptedOnceActive = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const heldMessage = yield* signMessage({
      card: fixture.localCard,
      authority: fixture.localAuthority,
      recipient: fixture.localCard.agentId,
      id: 70,
      body: "action-during-recovery",
    });
    const accepted = yield* Ref.make<string[]>([]);
    const callbackStarted = yield* Deferred.make<void>();
    const recoveryDurable = yield* Deferred.make<void>();
    const oldInstance = routerInstanceId(70);
    const newInstance = routerInstanceId(71);
    const router = yield* makeScriptedRouter({
      polls: [
        emptyBatch(oldInstance, pollCursor(11)),
        { kind: "cursor_invalid" },
        emptyBatch(newInstance, pollCursor(12)),
        batch(newInstance, pollCursor(13), [heldMessage]),
      ],
      fallbackPoll: Effect.never,
    });
    const worker = yield* provide(
      makeActiveRouterWorker(
        makeInput(
          fixture,
          callbacks({
            accepted,
            recover: () =>
              Deferred.succeed(callbackStarted, undefined).pipe(
                Effect.zipRight(Deferred.await(recoveryDurable)),
              ),
          }),
        ),
      ),
      router.layer,
      fixture,
    );
    const recovering = yield* provide(
      Effect.fork(worker.pollOnce),
      router.layer,
      fixture,
    );
    yield* Deferred.await(callbackStarted);
    expect(yield* Ref.get(accepted)).toEqual([]);
    const unavailable = yield* worker.currentAnchor.pipe(Effect.flip);
    expect(unavailable).toStrictEqual(new RouterWorkerUnavailableError());
    yield* Deferred.succeed(recoveryDurable, undefined);
    yield* Fiber.join(recovering);
    expect(yield* worker.currentAnchor).toEqual({
      routerInstanceId: newInstance,
      pollCursor: pollCursor(12),
    });
    yield* provide(worker.pollOnce, router.layer, fixture);
    expect(yield* Ref.get(accepted)).toEqual(["action-during-recovery"]);
    expect(yield* worker.currentAnchor).toEqual({
      routerInstanceId: newInstance,
      pollCursor: pollCursor(13),
    });
  });

const pinnedCardsSurviveRegistryOutage = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const remote = yield* makeIdentityFixture(2, "worker-remote");
    const accepted = yield* Ref.make<string[]>([]);
    const instance = routerInstanceId(90);
    const message = yield* signMessage({
      card: remote.localCard,
      authority: remote.localAuthority,
      recipient: fixture.localCard.agentId,
      id: 90,
      body: "durable-history",
    });
    const router = yield* makeScriptedRouter({
      polls: [
        emptyBatch(instance, pollCursor(16)),
        batch(instance, pollCursor(17), [message]),
      ],
    });
    const worker = yield* makeActiveRouterWorker({
      ...makeInput(fixture, callbacks({ accepted })),
      pinnedSenderCards: [fixture.localCard, remote.localCard],
    }).pipe(
      Effect.provide(router.layer),
      Effect.provide(unavailableRegistryLayer),
    );
    yield* worker.pollOnce.pipe(
      Effect.provide(router.layer),
      Effect.provide(unavailableRegistryLayer),
    );
    expect(yield* Ref.get(accepted)).toEqual(["durable-history"]);
    expect((yield* worker.currentAnchor).pollCursor).toBe(pollCursor(17));
  });

const coldStartRecoversBeforeActivation = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const events = yield* Ref.make<string[]>([]);
    const instance = routerInstanceId(100);
    const router = yield* makeScriptedRouter({
      polls: [emptyBatch(instance, pollCursor(18))],
      fallbackPoll: Effect.never,
    });
    const worker = yield* provide(
      makeRouterWorker(makeInput(fixture, callbacks({ events }))),
      router.layer,
      fixture,
    );

    const unavailable = yield* worker.currentAnchor.pipe(Effect.flip);
    expect(unavailable).toStrictEqual(new RouterWorkerUnavailableError());

    yield* provide(worker.pollOnce, router.layer, fixture);

    expect(yield* Ref.get(events)).toEqual([
      "abandon:router_restarted",
      "recover:router_restarted",
    ]);
    expect(yield* worker.currentAnchor).toEqual({
      routerInstanceId: instance,
      pollCursor: pollCursor(18),
    });
  });

const awaitAnchorResolvesOnActivation = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const instance = routerInstanceId(101);
    const router = yield* makeScriptedRouter({
      polls: [emptyBatch(instance, pollCursor(19))],
      fallbackPoll: Effect.never,
    });
    const worker = yield* provide(
      makeRouterWorker(makeInput(fixture, callbacks())),
      router.layer,
      fixture,
    );

    const attached = {
      routerInstanceId: instance,
      pollCursor: pollCursor(19),
    };
    const waiting = yield* Effect.fork(worker.awaitAnchor);
    expect(yield* Fiber.poll(waiting)).toEqual(Option.none());

    yield* provide(worker.pollOnce, router.layer, fixture);

    expect(yield* Fiber.join(waiting)).toEqual(attached);
    expect(yield* worker.awaitAnchor).toEqual(attached);
  });

/** Log lines `captureLogs` records, as `LEVEL: message`. */
type LogLines = string[];

const logText = (message: unknown): string =>
  (Array.isArray(message) ? message : [message])
    .map((part) => (typeof part === "string" ? part : JSON.stringify(part)))
    .join(" ");

/**
 * Replace the default logger with one that records each line.
 * @param lines Receives every logged line.
 * @returns A logger layer for the scenario.
 */
const captureLogs = (lines: LogLines) =>
  Logger.replace(
    Logger.defaultLogger,
    Logger.make(({ logLevel, message }) => {
      lines.push(`${logLevel.label}: ${logText(message)}`);
    }),
  );

const countLogged = (lines: LogLines, text: string): number =>
  lines.filter((line) => line.includes(text)).length;

/**
 * A worker scenario on the TestClock with captured logs.
 * @param lines Receives every logged line.
 * @param scenario Scenario to run.
 * @param routerLayer Router the worker talks to.
 * @param fixture Local identity the Registry resolves.
 * @returns The scenario with its services provided.
 */
const onTestClock = (
  lines: LogLines,
  scenario: Effect.Effect<
    void,
    unknown,
    Router | Registry | SqlClient.SqlClient
  >,
  routerLayer: Layer.Layer<Router>,
  fixture: Fixture,
): Effect.Effect<void, never, SqlClient.SqlClient> =>
  provide(scenario, routerLayer, fixture).pipe(
    Effect.orDie,
    Effect.provide(captureLogs(lines)),
    Effect.provide(TestContext.TestContext),
  );

const outageCursor = pollCursor(40);

/**
 * A Router that answers an omitted-cursor poll at once with `tail` and holds
 * a continuation poll for a second, failing both while unreachable.
 */
const outageRouter = (input: {
  readonly reachable: Ref.Ref<boolean>;
  readonly tail: Ref.Ref<ReturnType<typeof routerInstanceId>>;
}) =>
  Layer.succeed(Router, {
    poll: (call) => {
      const answer = Effect.all([
        Ref.get(input.reachable),
        Ref.get(input.tail),
      ]).pipe(
        Effect.flatMap(([reachable, instance]) =>
          reachable
            ? Effect.succeed(
                emptyBatch(instance, call.request.pollCursor ?? outageCursor),
              )
            : Effect.fail(new RouterConnectionError()),
        ),
      );
      return call.request.pollCursor === undefined
        ? answer
        : Effect.sleep("1 second").pipe(Effect.zipRight(answer));
    },
    send: (call) =>
      Ref.get(input.reachable).pipe(
        Effect.flatMap((reachable) =>
          reachable
            ? Ref.get(input.tail).pipe(
                Effect.flatMap((instance) =>
                  acceptedResult(instance, call.request.signedMessage),
                ),
              )
            : Effect.fail(new RouterConnectionError()),
        ),
      ),
  });

/**
 * A Router outage far longer than any bounded retry detaches the worker and
 * keeps its poll loop running, warns again while it lasts, and the first
 * answered probe reattaches at the retained anchor so a held send goes out
 * and the endpoint hears of the reattach.
 */
const outageDetachesAndReattaches = () =>
  Effect.gen(function* () {
    const lines: LogLines = [];
    const events = yield* Ref.make<string[]>([]);
    const fixture = yield* makeFixture;
    const instance = routerInstanceId(110);
    const reachable = yield* Ref.make(true);
    const tail = yield* Ref.make(instance);
    yield* withOutbox((store) =>
      onTestClock(
        lines,
        Effect.gen(function* () {
          const outgoing = yield* signMessage({
            card: fixture.localCard,
            authority: fixture.localAuthority,
            recipient: fixture.localCard.agentId,
            id: 110,
            body: "sent-after-outage",
          });
          const outbound = yield* prepareOutbound(
            store,
            "conversation:router-outage",
            outgoing,
          );
          const worker = yield* makeActiveRouterWorker(
            makeInput(fixture, callbacks({ events })),
          );
          const polling = yield* Effect.fork(worker.run);

          yield* Ref.set(reachable, false);
          yield* advanceClock("70 seconds");
          expect(yield* Fiber.poll(polling)).toEqual(Option.none());
          expect(lines).toEqual(
            expect.arrayContaining([
              expect.stringContaining("worker detached"),
            ]),
          );
          expect(lines).toEqual(
            expect.arrayContaining([
              expect.stringContaining("still unreachable"),
            ]),
          );
          expect(yield* worker.currentAnchor.pipe(Effect.flip)).toStrictEqual(
            new RouterWorkerUnavailableError(),
          );
          expect(
            yield* worker.send(outbound.envelopeId).pipe(Effect.flip),
          ).toStrictEqual(new RouterWorkerUnavailableError());

          yield* Ref.set(reachable, true);
          yield* advanceClock("6 seconds");
          expect(yield* worker.currentAnchor).toEqual({
            routerInstanceId: instance,
            pollCursor: outageCursor,
          });
          expect(lines).toEqual(
            expect.arrayContaining([expect.stringContaining("reattached")]),
          );
          expect(yield* Ref.get(events)).toEqual(["reattached"]);
          yield* worker.send(outbound.envelopeId);
          expect((yield* store.recover()).outboundMessages).toEqual([]);
          yield* Fiber.interrupt(polling);
        }),
        outageRouter({ reachable, tail }),
        fixture,
      ),
    );
  });

/**
 * A detached worker whose probe finds another Router instance recovers from
 * the restart instead of reattaching at the stale anchor.
 */
const detachedRestartRecovers = () =>
  Effect.gen(function* () {
    const lines: LogLines = [];
    const events = yield* Ref.make<string[]>([]);
    const fixture = yield* makeFixture;
    const reachable = yield* Ref.make(true);
    const tail = yield* Ref.make(routerInstanceId(111));
    const restarted = routerInstanceId(112);
    yield* onTestClock(
      lines,
      Effect.gen(function* () {
        const worker = yield* makeActiveRouterWorker(
          makeInput(fixture, callbacks({ events })),
        );
        const polling = yield* Effect.fork(worker.run);
        yield* Ref.set(reachable, false);
        yield* advanceClock("5 seconds");
        expect(lines).toEqual(
          expect.arrayContaining([expect.stringContaining("worker detached")]),
        );

        yield* Ref.set(tail, restarted);
        yield* Ref.set(reachable, true);
        yield* advanceClock("6 seconds");
        expect(yield* worker.currentAnchor).toEqual({
          routerInstanceId: restarted,
          pollCursor: outageCursor,
        });
        expect(yield* Ref.get(events)).toEqual([
          "abandon:router_restarted",
          "recover:router_restarted",
        ]);
        expect(lines).not.toEqual(
          expect.arrayContaining([expect.stringContaining("reattached")]),
        );
        yield* Fiber.interrupt(polling);
      }),
      outageRouter({ reachable, tail }),
      fixture,
    );
  });

/**
 * A continuation poll that fails with the given Router answers and then
 * succeeds is retried within the poll: the worker never detaches.
 */
const shortBlipKeepsWorkerAttached = (
  failures: readonly RouterClientFailure[],
) =>
  Effect.gen(function* () {
    const lines: LogLines = [];
    const fixture = yield* makeFixture;
    const instance = routerInstanceId(113);
    const remaining = yield* Ref.make([...failures]);
    const pollCalls = yield* Ref.make(0);
    const routerLayer = Layer.succeed(Router, {
      poll: (call) =>
        call.request.pollCursor === undefined
          ? Effect.succeed(emptyBatch(instance, pollCursor(41)))
          : Ref.update(pollCalls, (count) => count + 1).pipe(
              Effect.zipRight(
                Ref.modify(remaining, ([head, ...rest]) => [head, rest]),
              ),
              Effect.flatMap((failure) =>
                failure === undefined
                  ? Effect.succeed(emptyBatch(instance, pollCursor(42)))
                  : Effect.fail(failure),
              ),
            ),
      send: () => Effect.die("no send in this scenario"),
    });
    yield* onTestClock(
      lines,
      Effect.gen(function* () {
        const worker = yield* makeActiveRouterWorker(
          makeInput(fixture, callbacks()),
        );
        const polling = yield* Effect.fork(worker.pollOnce);
        yield* advanceClock("1 second");
        yield* Fiber.join(polling);
        expect(yield* Ref.get(pollCalls)).toBe(failures.length + 1);
        expect(yield* worker.currentAnchor).toEqual({
          routerInstanceId: instance,
          pollCursor: pollCursor(42),
        });
        expect(lines).not.toEqual(
          expect.arrayContaining([expect.stringContaining("worker detached")]),
        );
      }),
      routerLayer,
      fixture,
    );
  });

/**
 * A poll issued at one generation that fails only after a send-triggered
 * recovery activated the next generation leaves the new generation attached.
 */
const staleFailureKeepsNewGenerationAttached = () =>
  Effect.gen(function* () {
    const lines: LogLines = [];
    const fixture = yield* makeFixture;
    const first = routerInstanceId(114);
    const second = routerInstanceId(115);
    const tails = yield* Ref.make([
      emptyBatch(first, pollCursor(43)),
      emptyBatch(second, pollCursor(44)),
    ]);
    const releaseStale = yield* Deferred.make<void>();
    const staleIssued = yield* Deferred.make<void>();
    const routerLayer = Layer.succeed(Router, {
      poll: (call) => {
        switch (call.request.pollCursor) {
          case undefined:
            return Ref.modify(tails, ([head, ...rest]) => [
              head ?? emptyBatch(second, pollCursor(44)),
              rest,
            ]);
          case pollCursor(43):
            return Deferred.succeed(staleIssued, undefined).pipe(
              Effect.zipRight(Deferred.await(releaseStale)),
              Effect.zipRight(Effect.fail(new RouterConnectionError())),
            );
          default:
            return Effect.never;
        }
      },
      send: () =>
        Effect.succeed({
          kind: "router_restarted" as const,
          routerInstanceId: second,
        }),
    });
    yield* withOutbox((store) =>
      onTestClock(
        lines,
        Effect.gen(function* () {
          const outgoing = yield* signMessage({
            card: fixture.localCard,
            authority: fixture.localAuthority,
            recipient: fixture.localCard.agentId,
            id: 114,
            body: "observes-restart",
          });
          const outbound = yield* prepareOutbound(
            store,
            "conversation:stale-generation",
            outgoing,
          );
          const worker = yield* makeActiveRouterWorker(
            makeInput(fixture, callbacks()),
          );
          const polling = yield* Effect.fork(worker.run);
          yield* Deferred.await(staleIssued);
          expect(
            yield* worker.send(outbound.envelopeId).pipe(Effect.flip),
          ).toStrictEqual(new RouterWorkerDiscontinuityError());
          const recovered = {
            routerInstanceId: second,
            pollCursor: pollCursor(44),
          };
          expect(yield* worker.currentAnchor).toEqual(recovered);

          yield* Deferred.succeed(releaseStale, undefined);
          yield* advanceClock("2 seconds");
          expect(yield* worker.currentAnchor).toEqual(recovered);
          expect(lines).not.toEqual(
            expect.arrayContaining([
              expect.stringContaining("worker detached"),
            ]),
          );
          yield* Fiber.interrupt(polling);
        }),
        routerLayer,
        fixture,
      ),
    );
  });

/**
 * A Router lost while a recovery runs does not interrupt it: the recovery
 * completes, the next poll detaches the worker, the loop stays up and warns,
 * and once the Router answers the worker reattaches and a held send goes out.
 */
const outageDuringRecoveryRecovers = () =>
  Effect.gen(function* () {
    const lines: LogLines = [];
    const fixture = yield* makeFixture;
    const reachable = yield* Ref.make(true);
    const tail = yield* Ref.make(routerInstanceId(117));
    const restarted = routerInstanceId(118);
    const recoveryStarted = yield* Deferred.make<void>();
    const recover = () =>
      Deferred.succeed(recoveryStarted, undefined).pipe(
        Effect.zipRight(Effect.sleep("20 seconds")),
      );
    yield* withOutbox((store) =>
      onTestClock(
        lines,
        Effect.gen(function* () {
          const outbound = yield* prepareOutbound(
            store,
            "conversation:outage-during-recovery",
            yield* signMessage({
              card: fixture.localCard,
              authority: fixture.localAuthority,
              recipient: fixture.localCard.agentId,
              id: 117,
              body: "sent-after-recovery",
            }),
          );
          const worker = yield* makeActiveRouterWorker(
            makeInput(fixture, callbacks({ recover })),
          );
          const polling = yield* Effect.fork(worker.run);
          yield* Ref.set(tail, restarted);
          yield* advanceClock("1250 millis");
          expect(yield* Deferred.isDone(recoveryStarted)).toBe(true);

          yield* Ref.set(reachable, false);
          yield* advanceClock("70 seconds");
          expect(yield* Fiber.poll(polling)).toEqual(Option.none());
          expect(countLogged(lines, "recovery complete")).toBe(2);
          expect(lines).toEqual(
            expect.arrayContaining([
              expect.stringContaining("Router unreachable"),
            ]),
          );

          yield* Ref.set(reachable, true);
          yield* advanceClock("30 seconds");
          expect(yield* worker.currentAnchor).toEqual({
            routerInstanceId: restarted,
            pollCursor: outageCursor,
          });
          yield* worker.send(outbound.envelopeId);
          expect((yield* store.recover()).outboundMessages).toEqual([]);
          yield* Fiber.interrupt(polling);
        }),
        outageRouter({ reachable, tail }),
        fixture,
      ),
    );
  });

/**
 * A daemon cold-started while its Router is down keeps its recovering worker
 * running and warning, then recovers and sends once the Router answers.
 */
const coldStartWithRouterDownRecovers = () =>
  Effect.gen(function* () {
    const lines: LogLines = [];
    const fixture = yield* makeFixture;
    const instance = routerInstanceId(119);
    const reachable = yield* Ref.make(false);
    const tail = yield* Ref.make(instance);
    yield* withOutbox((store) =>
      onTestClock(
        lines,
        Effect.gen(function* () {
          const outbound = yield* prepareOutbound(
            store,
            "conversation:cold-start-router-down",
            yield* signMessage({
              card: fixture.localCard,
              authority: fixture.localAuthority,
              recipient: fixture.localCard.agentId,
              id: 119,
              body: "sent-after-cold-start",
            }),
          );
          const worker = yield* makeRouterWorker(
            makeInput(fixture, callbacks()),
          );
          const polling = yield* Effect.fork(worker.run);
          const attached = yield* Effect.fork(worker.awaitAnchor);
          yield* advanceClock("70 seconds");
          expect(yield* Fiber.poll(polling)).toEqual(Option.none());
          expect(yield* Fiber.poll(attached)).toEqual(Option.none());
          expect(lines).toEqual(
            expect.arrayContaining([
              expect.stringContaining("recovery attempt failed"),
            ]),
          );
          expect(lines).toEqual(
            expect.arrayContaining([
              expect.stringContaining("recovery waiting for"),
            ]),
          );
          expect(lines).not.toEqual(
            expect.arrayContaining([
              expect.stringContaining("recovery complete"),
            ]),
          );

          yield* Ref.set(reachable, true);
          yield* advanceClock("6 seconds");
          expect(yield* Fiber.join(attached)).toEqual({
            routerInstanceId: instance,
            pollCursor: outageCursor,
          });
          expect(lines).toEqual(
            expect.arrayContaining([
              expect.stringContaining("recovery complete"),
            ]),
          );
          yield* worker.send(outbound.envelopeId);
          expect((yield* store.recover()).outboundMessages).toEqual([]);
          yield* Fiber.interrupt(polling);
        }),
        outageRouter({ reachable, tail }),
        fixture,
      ),
    );
  });

/**
 * A Router rejection of the request itself is not an outage: the poll loop
 * ends at once with a typed rejection and logs what the Router refused. The
 * worker leaves what that ending does to the daemon to its supervisor.
 */
const rejectionEndsThePollLoop = (
  rejection: RouterClientFailure,
  reason: RouterWorkerRejectedError["reason"],
) =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const instance = routerInstanceId(116);
    const pollCalls = yield* Ref.make(0);
    const routerLayer = Layer.succeed(Router, {
      poll: (call) =>
        call.request.pollCursor === undefined
          ? Effect.succeed(emptyBatch(instance, pollCursor(45)))
          : Ref.update(pollCalls, (count) => count + 1).pipe(
              Effect.zipRight(Effect.fail(rejection)),
            ),
      send: () => Effect.die("no send in this scenario"),
    });
    const lines: LogLines = [];
    yield* onTestClock(
      lines,
      Effect.gen(function* () {
        const worker = yield* makeActiveRouterWorker(
          makeInput(fixture, callbacks()),
        );
        const failure = yield* worker.run.pipe(Effect.flip);
        expect(failure).toStrictEqual(
          new RouterWorkerRejectedError({ reason }),
        );
        expect(yield* Ref.get(pollCalls)).toBe(1);
        expect(lines).toContain(
          `ERROR: Router worker stopping, daemon exits: the Router rejected this endpoint's ${reason}`,
        );
      }),
      routerLayer,
      fixture,
    );
  });

/** A persistence fault is not an outage: the poll loop ends at once. */
const persistenceFaultEndsPollLoop = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const message = yield* signMessage({
      card: fixture.localCard,
      authority: fixture.localAuthority,
      recipient: fixture.localCard.agentId,
      id: 111,
      body: "persist-fails",
    });
    const instance = routerInstanceId(111);
    const router = yield* makeScriptedRouter({
      polls: [
        emptyBatch(instance, pollCursor(21)),
        batch(instance, pollCursor(22), [message]),
      ],
      fallbackPoll: Effect.never,
    });
    const worker = yield* provide(
      makeActiveRouterWorker(
        makeInput(fixture, callbacks({ failAcceptText: "persist-fails" })),
      ),
      router.layer,
      fixture,
    );
    const error = yield* provide(
      worker.run.pipe(Effect.flip, Effect.timeout("2 seconds")),
      router.layer,
      fixture,
    );
    expect(error).toStrictEqual(new RouterWorkerPersistenceError());
  });

// @agent-code-guard/regression-only: these scenarios pin the endpoint cursor and recovery safety boundary.
describe("private Router worker", () => {
  it("recovers certified history before activating a cold worker", () =>
    withDaemonStore(stateDirectory(), () =>
      coldStartRecoversBeforeActivation(),
    ));
  it("verifies a complete batch before dispatching any item", () =>
    withDaemonStore(stateDirectory(), () => batchVerificationIsAtomic()));
  it("accepts or ignores in order before advancing the cursor", () =>
    withDaemonStore(stateDirectory(), () => orderedCursorCommit()));
  it("retains the prior cursor when durable acceptance fails", () =>
    withDaemonStore(stateDirectory(), () => persistenceRetainsCursor()));
  it("retries an ambiguous send with the original envelope", () =>
    withDaemonStore(stateDirectory(), () => ambiguousSendRetriesSameBytes()));
  it("resends the stored envelope as initial after retry identity loss", () =>
    withDaemonStore(stateDirectory(), () => retryUnknownResendsStoredBytes()));
  it("asks again as retry when a resent initial loses the race to its slow original", () =>
    withDaemonStore(stateDirectory(), () =>
      resentInitialLosesRaceToSlowOriginal(),
    ));
  it.each<FailClosedRow>([
    {
      result: "a retry conflicts",
      answers: [connectionLost, identityConflict, acceptsSentBytes],
      modes: ["initial", "retry"],
    },
    {
      result: "an initial reports retry identity loss",
      answers: [identityUnknown, acceptsSentBytes],
      modes: ["initial"],
    },
    {
      result: "Router reports the message invalid",
      answers: [messageInvalid, acceptsSentBytes],
      modes: ["initial"],
    },
  ])(
    "fails closed and retains the envelope when $result",
    (...args: Parameters<typeof resultFailsClosed>) =>
      withDaemonStore(stateDirectory(), () => resultFailsClosed(...args)),
  );
  it("leaves the envelope for the next drain once Router alternation spends every attempt", () =>
    withDaemonStore(stateDirectory(), () =>
      alternatingConflictAndLossStopsAtTheBound(),
    ));
  it("retains an envelope when Router acceptance names different bytes", () =>
    withDaemonStore(stateDirectory(), () =>
      mismatchedAcceptedDigestRetainsOutbound(),
    ));
  it("recovers a fresh tail before returning a Router-restarted send", () =>
    withDaemonStore(stateDirectory(), () =>
      restartedSendRecoversBeforeReturning(),
    ));
  it("promotes an omitted-tail instance change before recovery", () =>
    withDaemonStore(stateDirectory(), () => restartOrdersTailBeforeRecovery()));
  it("recovers again on the new instance when the next poll after a recovery names one", () =>
    withDaemonStore(stateDirectory(), () =>
      recoveryRetryPromotesChangedTailToRestart(),
    ));
  it("holds normal sends until recovery activates its Router generation", () =>
    withDaemonStore(stateDirectory(), () => normalSendWaitsForRecovery()));
  it("accepts traffic the Router held during recovery once the worker is active", () =>
    withDaemonStore(stateDirectory(), () =>
      ingressHeldDuringRecoveryIsAcceptedOnceActive(),
    ));
  it("authenticates pinned history senders while Registry is unavailable", () =>
    withDaemonStore(stateDirectory(), () =>
      pinnedCardsSurviveRegistryOutage(),
    ));
  it("answers awaitAnchor once cold-start recovery activates the worker", () =>
    withDaemonStore(stateDirectory(), () => awaitAnchorResolvesOnActivation()));
  it(
    "detaches through a Router outage, warns while it lasts, and reattaches",
    () =>
      withDaemonStore(stateDirectory(), () => outageDetachesAndReattaches()),
    30_000,
  );
  it(
    "stays up and warns when the Router drops out after a recovery",
    () =>
      withDaemonStore(stateDirectory(), () => outageDuringRecoveryRecovers()),
    30_000,
  );
  it(
    "cold-starts with the Router down, warns, and recovers when it answers",
    () =>
      withDaemonStore(stateDirectory(), () =>
        coldStartWithRouterDownRecovers(),
      ),
    30_000,
  );
  it(
    "recovers from a restart a detached worker's probe discovers",
    () => withDaemonStore(stateDirectory(), () => detachedRestartRecovers()),
    30_000,
  );
  it("retries a 503 within the poll without detaching", () =>
    withDaemonStore(stateDirectory(), () =>
      shortBlipKeepsWorkerAttached([
        new UnavailableError(),
        new UnavailableError(),
      ]),
    ));
  it("retries a 429 within the poll without detaching", () =>
    withDaemonStore(stateDirectory(), () =>
      shortBlipKeepsWorkerAttached([
        new OverloadedError(),
        new OverloadedError(),
      ]),
    ));
  it("retries a dropped connection within the poll without detaching", () =>
    withDaemonStore(stateDirectory(), () =>
      shortBlipKeepsWorkerAttached([new RouterConnectionError()]),
    ));
  it(
    "keeps a generation recovery re-activated attached after a stale failure",
    () =>
      withDaemonStore(stateDirectory(), () =>
        staleFailureKeepsNewGenerationAttached(),
      ),
    30_000,
  );
  it("logs why and ends the poll loop when the Router rejects the endpoint's authentication", () =>
    withDaemonStore(stateDirectory(), () =>
      rejectionEndsThePollLoop(
        new AuthenticationFailedError(),
        "authentication",
      ),
    ));
  it("logs why and ends the poll loop when the Router rejects the endpoint's version", () =>
    withDaemonStore(stateDirectory(), () =>
      rejectionEndsThePollLoop(new VersionMismatchError(), "version"),
    ));
  it("ends the poll loop on a persistence fault without retrying", () =>
    withDaemonStore(stateDirectory(), () => persistenceFaultEndsPollLoop()));
});

/* eslint-enable max-lines, max-lines-per-function, @typescript-eslint/no-invalid-void-type -- Restore repository defaults. */
