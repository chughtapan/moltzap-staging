/** @file Queues signed conversation messages and sends them through the Router across retries. */

import type * as SqlClient from "@effect/sql/SqlClient";
import { Effect, Queue, Schedule } from "effect";
import type {
  EndpointEngineInput,
  EngineConversation,
  EngineOutbox,
  EngineOutboxError,
} from "./engine-context.js";
import {
  type ClientRepresentationError,
  encodeCanonical,
} from "../../encoding/canonical-json.js";
import { SignedMessage } from "../../identity/envelope/signed-message.js";
import {
  type ConversationId,
  type DecodedOuterBody,
  signOuterEvidence,
  signOuterPacket,
  type VerifiedMembership,
} from "../records/index.js";
import {
  describeRouterWorkerFailure,
  isTransientRouterWorkerError,
  routerWorkerReconnectSchedule,
  type RouterWorkerSendError,
} from "../router/index.js";
import * as Store from "../store/index.js";
import { exitOutcome, markRetries } from "./retry-markers.js";

/**
 * The outbox's mutable state. `queued` holds outbox identities in durable
 * order and `signal` wakes the outbound loop. `gate` orders a drain's reads
 * and removals of the queue head against `serialized` callers; `push` and
 * `clear` change the queue in one synchronous step without it.
 */
interface OutboxState {
  readonly input: EndpointEngineInput;
  readonly queued: string[];
  readonly signal: Queue.Queue<undefined>;
  readonly gate: Effect.Semaphore;
}

/**
 * Build the outbox of one engine.
 * @param input Signing identity and Router worker.
 * @param retained Outbox identities the store retains at startup, in durable
 *   order; when there are any, the outbound loop wakes to send them.
 * @returns The engine's outbox port.
 */
export const makeOutbox = (
  input: EndpointEngineInput,
  retained: readonly string[],
): Effect.Effect<EngineOutbox> =>
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

function bindOutbox(state: OutboxState): EngineOutbox {
  return {
    sign: (membership, body) => sign(state, membership, body),
    queuePacket: (conversation, packet) =>
      queueBody(state, conversation, { kind: "direct", packet }),
    queueEvidence: (conversation, evidence) =>
      queueBody(state, conversation, { kind: "evidence", message: evidence }),
    stageRecord: (conversation, packet, record) =>
      Effect.gen(function* () {
        const retained = yield* Store.readRecordOutbound(
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
        const outbound = yield* Store.stageRecordAndOutbound(record, {
          conversationId: conversation.conversationId,
          envelopeId: message.envelopeId,
          wireVersion: SignedMessage.wireVersion(message),
          canonicalSignedMessage,
        });
        yield* push(state, [outbound.envelopeId]);
      }),
    enqueueSigned: (conversationId, message) =>
      enqueueSigned(state, conversationId, message),
    resume: (envelopeIds) => push(state, envelopeIds),
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
): Effect.Effect<void, EngineOutboxError, SqlClient.SqlClient> {
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
    agentCard: state.input.localAgentCard,
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
): Effect.Effect<void, EngineOutboxError, SqlClient.SqlClient> {
  return encodeCanonical(SignedMessage, message).pipe(
    Effect.flatMap((canonicalSignedMessage) => {
      const input = {
        conversationId,
        envelopeId: message.envelopeId,
        wireVersion: SignedMessage.wireVersion(message),
        canonicalSignedMessage,
      };
      return Store.enqueueOutbound(input);
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
 * Wake on each signal and drain once the worker is attached. After a
 * transient worker failure, back off, wait for the worker to re-anchor, and
 * drain again; recovery may have reset the queue meanwhile, so each attempt
 * re-reads its head. A drain that starts with queued identities is marked as
 * `outbound` work, since its waits for the Router can last the whole outage
 * and emit nothing. A fatal failure is logged with its reason before it ends
 * the daemon.
 * @param state Outbox whose queued identities the worker sends.
 * @returns An effect that drains until a fatal worker failure.
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
 * Send queued outbox identities in order until the queue is empty.
 *
 * The gate covers only reading and removing the queue head, never the worker
 * send. A worker send queues behind a running recovery on the worker's
 * recovery gate, and may run that recovery on its own fiber after it observes
 * a Router restart; recovery resumes intents through `serialized`, so holding
 * the gate across the send would deadlock either way. The worker serializes
 * transmissions and a sent outbox identity is inactive, so concurrent drains
 * stay ordered; a drain removes the head only when it is still the identity
 * that drain sent.
 * @param state Outbox whose queued identities are sent.
 * @returns Completion once no queued identity remains.
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
