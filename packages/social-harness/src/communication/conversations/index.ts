/**
 * @file Certifies immutable messages and restores accepted work after restart.
 */

import * as SqlClient from "@effect/sql/SqlClient";
import { Data, Effect, type Scope } from "effect";
import type { Message } from "../message.js";
import type { DecodedOuterBody, RecordHash } from "../records/index.js";
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
  AcceptedMessage,
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
import {
  DeliveryAcknowledgeError,
  ListenError,
  type SendError,
} from "./errors.js";
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
import { prepareMessage, proposeIntent } from "./send.js";

/** Private engine dependencies retained behind the daemon boundary. */
export type { EndpointEngineInput } from "./engine-context.js";
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
  /** Prepares accepted work without waiting for certification. */
  readonly prepareMessage: (
    accepted: AcceptedMessage,
  ) => Effect.Effect<void, SendError>;
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
      intents: new Map(
        recovered.messageIntents.map(({ intent, canonicalIntent }) => [
          intent.msgId,
          { intent, canonicalIntent },
        ]),
      ),
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
    return yield* makeRuntime(input, recovered, yield* Effect.scope);
  });

const resumeRuntime = (runtime: EngineRuntime) =>
  Effect.gen(function* () {
    yield* resumeEngineFolds(runtime).pipe(Effect.mapError(resumeFoldFailure));
    yield* Effect.forEach(
      runtime.intents.values(),
      (intent) =>
        proposeIntent(runtime, intent).pipe(
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
    prepareMessage: (accepted: AcceptedMessage) =>
      prepareMessage(runtime, accepted).pipe(provideSql),
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

/** Assessment context asks communication whether accepted outgoing work remains. */
export { hasPendingMessages } from "../store/index.js";
