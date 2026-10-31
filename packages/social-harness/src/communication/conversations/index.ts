/**
 * @file Certifies immutable messages and restores accepted work after restart.
 */

import * as SqlClient from "@effect/sql/SqlClient";
import { Data, Deferred, Duration, Effect, type Scope } from "effect";
import type { Message } from "../message.js";
import type { DecodedOuterBody, MsgId, RecordHash } from "../records/index.js";
import type {
  RouterDiscontinuityReason,
  RouterIngressDisposition,
  RouterWorkerIngress,
  RouterWorkerPersistenceError,
  RouterWorkerRecovery,
  RouterWorkerRecoveryError,
  RouterWorkerSendError,
} from "../router/index.js";
import type { EndpointStoreError } from "../store/index.js";
import type {
  EndpointEngineInput,
  EnginePhases,
  EngineRuntime,
} from "./engine-context.js";
import { AgentSigningAuthority } from "../../identity/keys.js";
import { wireVersions } from "../../identity/wire-version.js";
import * as Store from "../store/index.js";
import {
  acceptEngineIngress,
  acceptEngineRecoveryIngress,
  resumeEngineFolds,
} from "./certification/index.js";
import { DeliveryAcknowledgeError, ListenError, SendError } from "./errors.js";
import { readStoredHistory } from "./history/index.js";
import { makeOutbox } from "./outbox.js";
import { recoverEngineState } from "./restore.js";
import {
  acceptEngineIngressWithRecovery,
  installRecoveryBarrier,
  isRecovering,
  rearmPausedCatchUp,
  recoverCertifiedHistory,
  recoveryHeadAdvanced,
} from "./router-recovery/index.js";
import {
  acceptMessage,
  type EngineSentMessage,
  proposeIntent,
} from "./send.js";

/** Private engine dependencies retained behind the daemon boundary. */
export type { EndpointEngineInput } from "./engine-context.js";
/** Certification results remain private to the communication implementation. */
export type { EngineSentMessage } from "./send.js";
/** The marker spans that bracket daemon work in flight, shared with the layers above. */
export {
  exitOutcome,
  markRetries,
  retryPending,
  retrySettled,
} from "./retry-markers.js";
/**
 * The one verifier of a stored membership row, and the stored-history reader
 * the owner tools page through, shared with the daemon.
 */
export {
  readRecentMessages,
  readStoredHistory,
  verifyStoredMembership,
} from "./history/index.js";

/** Engine acquisition could not establish one coherent durable endpoint. */
export class EngineInitializationError extends Data.TaggedError(
  "EngineInitializationError",
)<{
  readonly reason:
    | "identity"
    | "persistence"
    | "representation"
    | "outbox-too-old";
}> {}

/** Sending queued protocol traffic could not complete safely. */
export class EngineOutboundError extends Data.TaggedError(
  "EngineOutboundError",
)<{
  readonly reason: "network" | "persistence" | "representation" | "version";
}> {}

/** Verified messages remain pending until the Communication handler finishes. */
interface EnginePendingMessage {
  readonly recordHash: RecordHash;
  readonly message: Message;
}

/** Stable private engine capability consumed by daemon composition. */
export interface EndpointEngine {
  /** Observe one durable local intent without rechecking, rebinding, or sending a replacement. */
  readonly observeMessage: (
    msgId: MsgId,
  ) => Effect.Effect<EngineSentMessage, SendError>;

  /** Prepares accepted work without waiting for certification. */
  readonly prepareMessage: (message: Message) => Effect.Effect<void, SendError>;

  /** Accepts and certifies the caller's immutable message identity. */
  readonly send: (
    message: Message,
  ) => Effect.Effect<EngineSentMessage, SendError>;
  readonly readPendingMessages: () => Effect.Effect<
    readonly EnginePendingMessage[],
    ListenError
  >;
  readonly acknowledgeMessage: (
    recordHash: RecordHash,
  ) => Effect.Effect<void, DeliveryAcknowledgeError>;
  readonly acceptRouterIngress: (
    ingress: RouterWorkerIngress<DecodedOuterBody>,
  ) => Effect.Effect<RouterIngressDisposition, RouterWorkerPersistenceError>;
  readonly recoverCertifiedHistory: (
    recovery: RouterWorkerRecovery,
  ) => Effect.Effect<void, RouterWorkerRecoveryError>;
  readonly drainOutbound: Effect.Effect<void, EngineOutboundError>;
  readonly runOutbound: Effect.Effect<never, EngineOutboundError>;
  readonly abandonVolatileFolds: (
    reason: RouterDiscontinuityReason,
  ) => Effect.Effect<void>;
  /**
   * Start catch-up again for every conversation recovery still holds whose
   * retries ran out; the Router worker calls it when it reattaches.
   */
  readonly rearmCatchUp: Effect.Effect<void>;
}

type RecoveredStateError = Effect.Effect.Error<
  ReturnType<typeof recoverEngineState>
>;

const initializationReasonByStoreReason = {
  closed: "persistence",
  conflict: "identity",
  corrupt: "persistence",
  incompatible: "persistence",
  "invalid-continuation": "persistence",
  "invalid-input": "persistence",
  "not-found": "persistence",
  persistence: "persistence",
} as const satisfies Readonly<
  Record<EndpointStoreError["reason"], EngineInitializationError["reason"]>
>;

const listenReasonByStoreReason = {
  closed: "transport-failed",
  conflict: "decode-failed",
  corrupt: "decode-failed",
  incompatible: "incompatible-daemon",
  "invalid-continuation": "decode-failed",
  "invalid-input": "decode-failed",
  "not-found": "decode-failed",
  persistence: "transport-failed",
} as const satisfies Readonly<
  Record<EndpointStoreError["reason"], ListenError["reason"]>
>;

const acknowledgeReasonByStoreReason = {
  closed: "persistence-failed",
  conflict: "delivery-conflict",
  corrupt: "persistence-failed",
  incompatible: "persistence-failed",
  "invalid-continuation": "persistence-failed",
  "invalid-input": "persistence-failed",
  "not-found": "unknown-delivery",
  persistence: "persistence-failed",
} as const satisfies Readonly<
  Record<EndpointStoreError["reason"], DeliveryAcknowledgeError["reason"]>
>;

type ResumeIntentDisposition =
  | "fail-persistence"
  | "fail-representation"
  | "ignore";

const resumeDispositionBySendReason = {
  "outcome-unknown": "fail-representation",
  "certification-unavailable": "ignore",
  "content-invalid": "fail-representation",
  "message-conflict": "fail-representation",
  "invalid-address": "fail-representation",
  "membership-invalid": "fail-representation",
  "network-unavailable": "ignore",
  "delivery-pending": "ignore",
  "not-registered": "fail-representation",
  "persistence-failed": "fail-persistence",
  "unknown-agent": "fail-representation",
  "version-mismatch": "fail-representation",
} as const satisfies Readonly<
  Record<SendError["reason"], ResumeIntentDisposition>
>;

const outboundReasonByTag = {
  RouterWorkerAuthenticationError: "representation",
  RouterWorkerDiscontinuityError: "network",
  RouterWorkerPersistenceError: "persistence",
  RouterWorkerProtocolError: "representation",
  RouterWorkerRecoveryError: "network",
  RouterWorkerTransportError: "network",
  RouterWorkerUnavailableError: "network",
} as const satisfies Readonly<
  Record<
    Exclude<RouterWorkerSendError["_tag"], "RouterWorkerRejectedError">,
    EngineOutboundError["reason"]
  >
>;

const outboundFailure = (error: RouterWorkerSendError): EngineOutboundError => {
  if (error._tag !== "RouterWorkerRejectedError") {
    return new EngineOutboundError({ reason: outboundReasonByTag[error._tag] });
  }
  return new EngineOutboundError({
    reason: error.reason === "version" ? "version" : "representation",
  });
};

function listenStoreFailure(error: EndpointStoreError): ListenError {
  return new ListenError({ reason: listenReasonByStoreReason[error.reason] });
}

function listenRepresentationFailure(): ListenError {
  return new ListenError({ reason: "decode-failed" });
}

function acknowledgeStoreFailure(
  error: EndpointStoreError,
): DeliveryAcknowledgeError {
  return new DeliveryAcknowledgeError({
    reason: acknowledgeReasonByStoreReason[error.reason],
  });
}

function recoveryInitializationFailure(
  error: RecoveredStateError,
): EngineInitializationError {
  switch (error._tag) {
    case "ClientRepresentationError":
    case "ParseError":
      return representationInitializationFailure();
    case "OutboxTooOldError":
      return new EngineInitializationError({ reason: "outbox-too-old" });
    case "RouterWorkerPersistenceError":
      return new EngineInitializationError({ reason: "persistence" });
    default: {
      const exhaustive: never = error;
      return exhaustive;
    }
  }
}

function resumeIntentFailure(
  error: SendError,
): Effect.Effect<void, EngineInitializationError> {
  const disposition = resumeDispositionBySendReason[error.reason];
  switch (disposition) {
    case "fail-persistence":
      return Effect.fail(
        new EngineInitializationError({ reason: "persistence" }),
      );
    case "fail-representation":
      return Effect.fail(representationInitializationFailure());
    case "ignore":
      return Effect.void;
    default: {
      const exhaustive: never = disposition;
      return exhaustive;
    }
  }
}

function initializationStoreFailure(
  error: EndpointStoreError,
): EngineInitializationError {
  return new EngineInitializationError({
    reason: initializationReasonByStoreReason[error.reason],
  });
}

function representationInitializationFailure(): EngineInitializationError {
  return new EngineInitializationError({ reason: "representation" });
}

function resumeFoldFailure(): EngineInitializationError {
  return new EngineInitializationError({ reason: "persistence" });
}

/**
 * How long a local send's own drain may run before the send fails as
 * `delivery-pending`. With `ROUTER_ATTACH_TIMEOUT` it stays under the MCP
 * SDK's `DEFAULT_REQUEST_TIMEOUT_MSEC`.
 */
const LOCAL_DRAIN_TIMEOUT = Duration.seconds(10);

/** Accepted messages remain queued after the caller's bounded certification wait ends. */
const finishAcceptedMessage = (
  runtime: EngineRuntime,
  prepared: Effect.Effect.Success<ReturnType<typeof acceptMessage>>,
) =>
  Effect.gen(function* () {
    if (!(yield* Deferred.isDone(prepared.completion))) {
      yield* runtime.outbox.drain.pipe(
        Effect.timeout(LOCAL_DRAIN_TIMEOUT),
        // eslint-disable-next-line agent-code-guard/no-effect-error-coalescing -- Every drain failure, the timeout included, leaves the message queued and still delivered, as this function's JSDoc explains.
        Effect.mapError(() => new SendError({ reason: "delivery-pending" })),
      );
    }
    const recordHash = yield* Deferred.await(prepared.completion);
    return { msgId: prepared.msgId, recordHash };
  });

const send = (
  runtime: EngineRuntime,
  message: Message,
): Effect.Effect<EngineSentMessage, SendError, SqlClient.SqlClient> =>
  acceptMessage(runtime, message).pipe(
    Effect.flatMap((accepted) => finishAcceptedMessage(runtime, accepted)),
    Effect.withSpan("EndpointEngine.send"),
  );

const readPendingMessages = (
  runtime: EngineRuntime,
): Effect.Effect<
  readonly EnginePendingMessage[],
  ListenError,
  SqlClient.SqlClient
> =>
  Store.readUnconsumedRecords().pipe(
    Effect.mapError(listenStoreFailure),
    Effect.flatMap((history) =>
      readStoredHistory(
        runtime.input.registrySignerPublicKey,
        history,
        history.records,
      ).pipe(Effect.mapError(listenRepresentationFailure)),
    ),
  );

const acknowledgeMessage = (recordHash: RecordHash) =>
  Store.consumeRecord(recordHash).pipe(
    Effect.mapError(acknowledgeStoreFailure),
  );

const validateLocalIdentity = (
  input: EndpointEngineInput,
): Effect.Effect<void, EngineInitializationError> =>
  AgentSigningAuthority.publicKey(input.signingAuthority).x ===
  input.localAgentCard.publicKey.x
    ? Effect.void
    : Effect.fail(new EngineInitializationError({ reason: "identity" }));

/** Each engine phase that another phase starts, bound once for every runtime. */
const enginePhases: EnginePhases = {
  proposeIntent,
  acceptIngress: acceptEngineIngress,
  acceptRecoveryIngress: acceptEngineRecoveryIngress,
  resumeFolds: resumeEngineFolds,
  rearmCatchUp: rearmPausedCatchUp,
  headAdvanced: recoveryHeadAdvanced,
  isRecovering,
};

const makeRuntime = (
  input: EndpointEngineInput,
  recovered: Effect.Effect.Success<ReturnType<typeof recoverEngineState>>,
  scope: Scope.Scope,
): Effect.Effect<EngineRuntime, never, SqlClient.SqlClient> =>
  Effect.gen(function* () {
    return {
      input,
      wireVersion: yield* wireVersions.emitted,
      conversations: recovered.conversations,
      intents: new Map(),
      completedMessages: recovered.completedMessages,
      actionFolds: recovered.actionFolds,
      recordFolds: recovered.recordFolds,
      gate: yield* Effect.makeSemaphore(1),
      outbox: yield* makeOutbox(
        input,
        recovered.outboundMessages.map((message) => message.envelopeId),
      ),
      phases: enginePhases,
      scope,
    };
  });

const hydrateMessageIntents = (
  runtime: EngineRuntime,
  messageIntents: Effect.Effect.Success<
    ReturnType<typeof recoverEngineState>
  >["messageIntents"],
): Effect.Effect<void, EngineInitializationError, SqlClient.SqlClient> =>
  Effect.forEach(
    messageIntents,
    (stored) =>
      Effect.gen(function* () {
        const { intent } = stored;
        const completion = yield* Deferred.make<RecordHash, SendError>();
        yield* Effect.sync(() => {
          runtime.intents.set(intent.msgId, {
            intent,
            canonicalIntent: stored.canonicalIntent,
            completion,
          });
        });
        const completedRecordHash = runtime.completedMessages.get(intent.msgId);
        if (completedRecordHash !== undefined) {
          yield* Deferred.succeed(completion, completedRecordHash);
        }
      }),
    { concurrency: 1, discard: true },
  );

const initializeRuntime = (
  input: EndpointEngineInput,
): Effect.Effect<
  EngineRuntime,
  EngineInitializationError,
  Scope.Scope | SqlClient.SqlClient
> =>
  Effect.gen(function* () {
    yield* validateLocalIdentity(input);
    const recovery = yield* Store.recover().pipe(
      Effect.mapError(initializationStoreFailure),
    );
    const recovered = yield* recoverEngineState(input, recovery).pipe(
      Effect.mapError(recoveryInitializationFailure),
    );
    const runtime = yield* makeRuntime(input, recovered, yield* Effect.scope);
    yield* hydrateMessageIntents(runtime, recovered.messageIntents);
    return runtime;
  });

const resumeRuntime = (runtime: EngineRuntime) =>
  Effect.gen(function* () {
    yield* resumeEngineFolds(runtime).pipe(Effect.mapError(resumeFoldFailure));
    yield* Effect.forEach(
      runtime.intents.values(),
      (intent) =>
        runtime.completedMessages.has(intent.intent.msgId)
          ? Effect.void
          : proposeIntent(runtime, intent).pipe(
              Effect.asVoid,
              Effect.catchTag("SendError", resumeIntentFailure),
            ),
      { concurrency: 1, discard: true },
    );
  });

const abandonVolatileFolds = (
  runtime: EngineRuntime,
  reason: RouterDiscontinuityReason,
): Effect.Effect<void, never, SqlClient.SqlClient> =>
  runtime.gate
    .withPermits(1)(
      Effect.gen(function* () {
        yield* installRecoveryBarrier(runtime);
        yield* Effect.sync(() => {
          runtime.outbox.clear();
          if (reason !== "router_restarted") {
            return;
          }
          for (const fold of runtime.actionFolds.values()) {
            fold.localActionEvidenceQueued = false;
            fold.localDurabilityEvidenceQueued = false;
          }
        });
      }),
    )
    .pipe(Effect.withSpan("abandonVolatileFolds", { attributes: { reason } }));

const endpointEngine = (
  runtime: EngineRuntime,
  sql: SqlClient.SqlClient,
): EndpointEngine => {
  const provideSql = Effect.provideService(SqlClient.SqlClient, sql);
  return Object.freeze({
    observeMessage: (msgId: MsgId) => observeMessage(runtime, msgId),
    prepareMessage: (message: Message) =>
      acceptMessage(runtime, message).pipe(Effect.asVoid, provideSql),
    send: (message: Message) => send(runtime, message).pipe(provideSql),
    readPendingMessages: () => readPendingMessages(runtime).pipe(provideSql),
    acknowledgeMessage: (recordHash: RecordHash) =>
      acknowledgeMessage(recordHash).pipe(provideSql),
    acceptRouterIngress: (ingress: RouterWorkerIngress<DecodedOuterBody>) =>
      acceptEngineIngressWithRecovery(runtime, ingress).pipe(provideSql),
    recoverCertifiedHistory: (recovery: RouterWorkerRecovery) =>
      recoverCertifiedHistory(runtime, recovery).pipe(provideSql),
    drainOutbound: runtime.outbox.drain.pipe(
      Effect.mapError(outboundFailure),
      provideSql,
    ),
    runOutbound: runtime.outbox.run.pipe(
      Effect.mapError(outboundFailure),
      provideSql,
    ),
    abandonVolatileFolds: (reason: RouterDiscontinuityReason) =>
      abandonVolatileFolds(runtime, reason).pipe(provideSql),
    rearmCatchUp: rearmPausedCatchUp(runtime).pipe(provideSql),
  });
};

/**
 * Build one coherent private engine over registered identity and durable state.
 * @param input Verified identity, Registry and Router dependencies.
 * @returns A scoped engine whose recovered state is ready for protocol work.
 */
export const makeEndpointEngine = (
  input: EndpointEngineInput,
): Effect.Effect<
  EndpointEngine,
  EngineInitializationError,
  Scope.Scope | SqlClient.SqlClient
> =>
  Effect.gen(function* () {
    const runtime = yield* initializeRuntime(input);
    yield* resumeRuntime(runtime);
    return endpointEngine(runtime, yield* SqlClient.SqlClient);
  }).pipe(Effect.withSpan("makeEndpointEngine"));

/**
 * Observe the retained local intent without preparing or binding a replacement.
 * @param runtime The active engine whose completion latches retain local ownership.
 * @param msgId The already-bound local message to observe.
 * @returns The original message and its certificate, or its retained send failure.
 */
function observeMessage(runtime: EngineRuntime, msgId: MsgId) {
  return Effect.suspend(() => {
    const complete = runtime.completedMessages.get(msgId);
    if (complete !== undefined) {
      return Effect.succeed({ msgId, recordHash: complete });
    }
    const retained = runtime.intents.get(msgId);
    return retained === undefined
      ? Effect.fail(new SendError({ reason: "certification-unavailable" }))
      : Deferred.await(retained.completion).pipe(
          Effect.map((recordHash) => ({ msgId, recordHash })),
        );
  });
}

/** Assessment context asks communication whether accepted outgoing work remains. */
export { hasPendingMessages } from "../store/index.js";
