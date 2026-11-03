/** @file One conversation engine owns restored state and its signed outbound queue. */

import { SqlClient } from "@effect/sql/SqlClient";
import {
  type Context,
  Data,
  Effect,
  Queue,
  Schedule,
  type Scope,
} from "effect";
import type {
  AgentId,
  VerifiedAgentIdentityCard,
} from "../../identity/agent-cards.js";
import type { Registry } from "../../identity/registry.js";
import type { SendError } from "../errors.js";
import type { Message } from "../messages.js";
import {
  type ClientRepresentationError,
  encodeCanonical,
} from "../../encoding/canonical-json.js";
import {
  type AgentPublicKey,
  AgentSigningAuthority,
} from "../../identity/keys.js";
import { SignedMessage } from "../../identity/message-security.js";
import { exitOutcome, markRetries } from "../../tracing.js";
import { wireVersions } from "../../version.js";
import {
  type ActionCertifiedRecord,
  type ActionCore,
  type ActionHash,
  type CertifiedRecord,
  type ConversationId,
  type DecodedOuterBody,
  type DirectPacket,
  type MessageIntent,
  type RecordHash,
  type RouterAnchor,
  signOuterEvidence,
  signOuterPacket,
  type VerifiedMembership,
} from "../records.js";
import {
  describeRouterWorkerFailure,
  isTransientRouterWorkerError,
  type RouterDiscontinuityReason,
  type RouterWorker,
  type RouterWorkerIngress,
  routerWorkerReconnectSchedule,
  type RouterWorkerRecovery,
  type RouterWorkerSendError,
} from "../router/connection.js";
import {
  consumeRecord,
  type EndpointStoreError,
  enqueueOutbound,
  readRecordOutbound,
  readUnconsumedRecords,
  recoverStoredState,
  type StagedRecord,
  stageRecordAndOutbound,
} from "../store.js";
import {
  acceptEngineIngress,
  acceptEngineRecoveryIngress,
  resumeEngineFolds,
} from "./certification.js";
import {
  readStoredHistory,
  recoverEngineState,
  type StoredRowError,
} from "./history.js";
import { prepareMessage, proposeIntent } from "./proposals.js";
import {
  acceptEngineIngressWithRecovery,
  installRecoveryBarrier,
  isRecovering,
  rearmPausedCatchUp,
  recoverCertifiedHistory,
  recoveryHeadAdvanced,
} from "./recovery.js";

// safer-arch-ignore no-fat-orchestrator: Conversation protocol composition remains encapsulated inside Communication.

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
/** The private engine shape comes from the operations it actually exposes. */
export type EndpointEngine = ReturnType<typeof endpointEngine>;

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

const readPendingMessages = (
  runtime: EngineRuntime,
): Effect.Effect<
  readonly EnginePendingMessage[],
  EndpointStoreError | StoredRowError,
  SqlClient
> =>
  readUnconsumedRecords().pipe(
    Effect.flatMap((history) =>
      readStoredHistory(
        runtime.input.registrySignerPublicKey,
        history,
        history.records,
      ),
    ),
  );

const acknowledgeMessage = (recordHash: RecordHash) =>
  consumeRecord(recordHash);

const validateLocalIdentity = (
  input: EndpointEngineInput,
): Effect.Effect<void, EngineInitializationError> =>
  AgentSigningAuthority.publicKey(input.signingAuthority).x ===
  input.localAgentCard.publicKey.x
    ? Effect.void
    : Effect.fail(new EngineInitializationError({ reason: "identity" }));

/** Each engine phase that another phase starts, bound once for every runtime. */
const enginePhases = {
  proposeIntent,
  acceptIngress: acceptEngineIngress,
  acceptRecoveryIngress: acceptEngineRecoveryIngress,
  resumeFolds: resumeEngineFolds,
  rearmCatchUp: rearmPausedCatchUp,
  headAdvanced: recoveryHeadAdvanced,
  isRecovering,
};

/** Retained outbox identities resume in durable order and wake the outbound loop. */
export const makeOutbox = (
  input: EndpointEngineInput,
  retained: readonly string[],
): Effect.Effect<ReturnType<typeof bindOutbox>> =>
  Effect.gen(function* () {
    const state: OutboxState = {
      input,
      queued: [...retained],
      signal: yield* Queue.unbounded<undefined>(),
      gate: yield* Effect.makeSemaphore(1),
    };
    if (state.queued.length > 0) {
      yield* Queue.offer(state.signal, undefined);
    }
    return bindOutbox(state);
  }).pipe(Effect.withSpan("makeOutbox"));

const makeRuntime = (
  input: EndpointEngineInput,
  recovered: Effect.Effect.Success<ReturnType<typeof recoverEngineState>>,
  scope: Scope.Scope,
): Effect.Effect<EngineRuntime, never, SqlClient> =>
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
  Scope.Scope | SqlClient
> =>
  Effect.gen(function* () {
    yield* validateLocalIdentity(input);
    const recovery = yield* recoverStoredState().pipe(
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
): Effect.Effect<void, never, SqlClient> =>
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

const endpointEngine = (runtime: EngineRuntime, sql: SqlClient) => {
  const provideSql = Effect.provideService(SqlClient, sql);
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
  Scope.Scope | SqlClient
> =>
  Effect.gen(function* () {
    const runtime = yield* initializeRuntime(input);
    yield* resumeRuntime(runtime);
    return endpointEngine(runtime, yield* SqlClient);
  }).pipe(Effect.withSpan("makeEndpointEngine"));

/** RouterWorker operations consumed by the engine's outbound queue. */
type EngineRouterPort = Pick<
  RouterWorker,
  "currentAnchor" | "awaitAnchor" | "send"
>;

/** Stable private dependencies for one endpoint protocol engine. */
export interface EndpointEngineInput {
  readonly localAgentCard: VerifiedAgentIdentityCard;
  readonly signingAuthority: AgentSigningAuthority;
  readonly registrySignerPublicKey: AgentPublicKey;
  readonly registry: Pick<Context.Tag.Service<typeof Registry>, "lookup">;
  readonly routerWorker: EngineRouterPort;
  /**
   * Stops the host after a store failure may leave an accepted intent absent
   * from memory or unproposed. Restart recovery resumes it. Waiting for the head
   * to move cannot recover a new conversation.
   */
  readonly reportStorageFault: (
    error: EndpointStoreError,
  ) => Effect.Effect<void>;
}

/** Preparation uses the exact payload already retained by Communication. */
export interface AcceptedMessage {
  readonly message: Message;
  readonly canonicalMessage: Uint8Array;
}

/** One locally authored immutable message intent awaiting certification. */
export interface EngineMessageIntent {
  readonly intent: MessageIntent;
  readonly canonicalIntent: Uint8Array;
  proposedActionHash?: ActionHash;
}

/** One locally complete history head. */
export interface EngineCertifiedHead {
  readonly recordHash: RecordHash;
  readonly record: CertifiedRecord;
}

/** Immutable membership with the endpoint's current durable position. */
export interface EngineConversation {
  readonly conversationId: ConversationId;
  readonly membership: VerifiedMembership;
  currentAnchor: RouterAnchor;
  head?: EngineCertifiedHead;
}

/** Volatile evidence fold for the selected action at one predecessor. */
export interface EngineActionFold {
  readonly conversation: EngineConversation;
  readonly action: ActionCore;
  readonly actionHash: ActionHash;
  readonly routerAnchor: RouterAnchor;
  readonly actionEvidence: Map<AgentId, SignedMessage>;
  readonly durabilityEvidence: Map<AgentId, SignedMessage>;
  localActionEvidenceQueued: boolean;
  localDurabilityEvidenceQueued: boolean;
  recordHash?: RecordHash;
  certified: boolean;
}

/** Shared acquired state used by addressed send and protocol ingress. */
export interface EngineRuntime {
  readonly wireVersion: string;
  readonly input: EndpointEngineInput;
  readonly conversations: Map<ConversationId, EngineConversation>;
  /** Pending local intents, released after durable certification. */
  readonly intents: Map<string, EngineMessageIntent>;
  /** Locally complete message ids retained for the engine's lifetime. */
  readonly completedMessages: Set<string>;
  readonly actionFolds: Map<ActionHash, EngineActionFold>;
  readonly recordFolds: Map<RecordHash, EngineActionFold>;
  readonly gate: Effect.Semaphore;
  readonly outbox: ReturnType<typeof bindOutbox>;
  readonly phases: typeof enginePhases;
  /**
   * The engine's lifetime. Recovery work that outlives one Router callback,
   * such as a conversation's catch-up retries, runs in a child of it, so it
   * ends with the engine.
   */
  readonly scope: Scope.Scope;
}

/**
 * Why the outbox could not stage an envelope: signing or canonical encoding
 * failed, or the store refused the row. Each phase maps it to its own error.
 */
export type EngineOutboxError = ClientRepresentationError | EndpointStoreError;

/**
 * Queue mutation is synchronous. The gate orders drain reads and removals
 * against serialized callers and must not cover network sends.
 */
interface OutboxState {
  readonly input: EndpointEngineInput;
  readonly queued: string[];
  readonly signal: Queue.Queue<undefined>;
  readonly gate: Effect.Semaphore;
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

function bindOutbox(state: OutboxState) {
  return {
    sign: (membership: VerifiedMembership, body: DecodedOuterBody) =>
      sign(state, membership, body),
    queuePacket: (conversation: EngineConversation, packet: DirectPacket) =>
      queueBody(state, conversation, { kind: "direct", packet }),
    queueEvidence: (
      conversation: EngineConversation,
      evidence: SignedMessage,
    ) =>
      queueBody(state, conversation, { kind: "evidence", message: evidence }),
    stageRecord: (
      conversation: EngineConversation,
      packet: ActionCertifiedRecord,
      record: StagedRecord,
    ) =>
      Effect.gen(function* () {
        const retained = yield* readRecordOutbound(
          conversation.conversationId,
          record.recordHash,
        );
        if (retained !== undefined && retained.disposition !== "discarded") {
          yield* push(state, [retained.outbound.envelopeId]);
          return;
        }
        const message = yield* sign(state, conversation.membership, {
          kind: "direct",
          packet,
        });
        const canonicalSignedMessage = yield* encodeCanonical(
          SignedMessage,
          message,
        );
        const outbound = yield* stageRecordAndOutbound(record, {
          conversationId: conversation.conversationId,
          envelopeId: message.envelopeId,
          wireVersion: SignedMessage.wireVersion(message),
          canonicalSignedMessage,
        });
        yield* push(state, [outbound.envelopeId]);
      }),
    enqueueSigned: (conversationId: ConversationId, message: SignedMessage) =>
      enqueueSigned(state, conversationId, message),
    resume: (envelopeIds: readonly string[]) => push(state, envelopeIds),
    clear: () => {
      state.queued.length = 0;
    },
    serialized: state.gate.withPermits(1),
    drain: drain(state),
    run: run(state),
  };
}

function queueBody(
  state: OutboxState,
  conversation: EngineConversation,
  body: DecodedOuterBody,
): Effect.Effect<void, EngineOutboxError, SqlClient> {
  return sign(state, conversation.membership, body).pipe(
    Effect.flatMap((message) =>
      enqueueSigned(state, conversation.conversationId, message),
    ),
  );
}

function sign(
  state: OutboxState,
  membership: VerifiedMembership,
  body: DecodedOuterBody,
): Effect.Effect<SignedMessage, ClientRepresentationError> {
  const signer = {
    membership,
    identityCard: state.input.localAgentCard,
    signingAuthority: state.input.signingAuthority,
  };
  return body.kind === "direct"
    ? signOuterPacket({ ...signer, packet: body.packet })
    : signOuterEvidence({ ...signer, evidence: body.message });
}

function enqueueSigned(
  state: OutboxState,
  conversationId: ConversationId,
  message: SignedMessage,
): Effect.Effect<void, EngineOutboxError, SqlClient> {
  return encodeCanonical(SignedMessage, message).pipe(
    Effect.flatMap((canonicalSignedMessage) => {
      const input = {
        conversationId,
        envelopeId: message.envelopeId,
        wireVersion: SignedMessage.wireVersion(message),
        canonicalSignedMessage,
      };
      return enqueueOutbound(input);
    }),
    Effect.flatMap((outbound) => push(state, [outbound.envelopeId])),
  );
}

function push(
  state: OutboxState,
  envelopeIds: Iterable<string>,
): Effect.Effect<void> {
  return Effect.sync(() => {
    for (const envelopeId of envelopeIds) {
      if (!state.queued.includes(envelopeId)) {
        state.queued.push(envelopeId);
      }
    }
  }).pipe(Effect.zipRight(Queue.offer(state.signal, undefined)), Effect.asVoid);
}

/**
 * After transient failure, waits for reattachment and rereads the queue head
 * because recovery may replace the queue. Outbound trace spans cover outage
 * waits. Fatal failures end the daemon.
 */
function run(state: OutboxState): Effect.Effect<never, RouterWorkerSendError> {
  const attachedDrain = state.input.routerWorker.awaitAnchor.pipe(
    Effect.zipRight(drain(state)),
    Effect.retry(
      routerWorkerReconnectSchedule.pipe(
        Schedule.whileInput(isTransientRouterWorkerError),
      ),
    ),
    markRetries("outbound", "outbox", (exit) => exitOutcome(exit, "sent")),
  );
  const drainWhenAttached = Effect.suspend(() =>
    state.queued.length === 0 ? Effect.void : attachedDrain,
  ).pipe(
    Effect.tapError((error) =>
      Effect.logError(
        `Outbound drain stopping, daemon exits: ${describeRouterWorkerFailure(error)}`,
      ),
    ),
  );
  return Queue.take(state.signal).pipe(
    Effect.zipRight(drainWhenAttached),
    Effect.forever,
  );
}

/**
 * Never holds the queue gate across worker sends. Those sends can wait for
 * or run recovery, which resumes intents through the same gate. The worker
 * serializes transmission. Remove only the head actually sent.
 */
function drain(state: OutboxState): Effect.Effect<void, RouterWorkerSendError> {
  return Effect.gen(function* () {
    let envelopeId = yield* peek(state);
    while (envelopeId !== undefined) {
      yield* state.input.routerWorker.send(envelopeId);
      yield* shift(state, envelopeId);
      envelopeId = yield* peek(state);
    }
  });
}

function peek(state: OutboxState): Effect.Effect<string | undefined> {
  return state.gate.withPermits(1)(Effect.sync(() => state.queued[0]));
}

function shift(state: OutboxState, envelopeId: string): Effect.Effect<void> {
  return state.gate.withPermits(1)(
    Effect.sync(() => {
      if (state.queued[0] === envelopeId) {
        state.queued.shift();
      }
    }),
  );
}
