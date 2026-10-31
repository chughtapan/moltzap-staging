/** @file Address resolution, immutable intent binding, and proposal creation. */

import type * as SqlClient from "@effect/sql/SqlClient";
import { Deferred, Duration, Effect, Exit, Schedule, Schema } from "effect";
import type { RouterWorkerUnavailableError } from "../router/index.js";
import type {
  ConversationFoundation,
  EndpointStoreError,
  MessageIntent as StoredMessageIntent,
} from "../store/index.js";
import type {
  EngineConversation,
  EngineMessageIntent,
  EngineOutboxError,
  EngineRuntime,
} from "./engine-context.js";
import { encodeCanonical, sameBytes } from "../../encoding/canonical-json.js";
import {
  AgentCard,
  agentViewOf,
  type VerifiedAgentCard,
} from "../../identity/cards.js";
import { Message, messageFromIntent, messageMembers } from "../message.js";
import {
  type ActionCore,
  type ActionHash,
  deriveConversationId,
  hashAction,
  hashAnchor,
  hashMessageIntent,
  InitialAnchorBody,
  MembershipDescriptor as MembershipDescriptorSchema,
  type MessageIntent,
  MessageIntent as MessageIntentSchema,
  type MsgId,
  type RecordHash,
  type VerifiedMembership,
  verifyMembershipDescriptor,
} from "../records/index.js";
import { compareAgentIds } from "../records/values.js";
import * as Store from "../store/index.js";
import { isTerminalSendFailure, SendError } from "./errors.js";
import { anchorHashAtHead } from "./history/index.js";
import { markRetries, type RetryOutcome } from "./retry-markers.js";
import {
  catchUpRetrySchedule,
  pendingRecoveryFence,
} from "./router-recovery/index.js";

// safer-arch-ignore no-fat-orchestrator: Immutable intent acceptance and protocol proposal share the engine gate and durable conversation binding.

/** Certification records the caller's original message identity. */
export interface EngineSentMessage {
  readonly msgId: MsgId;
  readonly recordHash: RecordHash;
}

/**
 * Fail a send whose intent bind did not complete. A store refusal is raised
 * before the bind commits and rolls it back, so nothing was queued. A raw
 * persistence failure leaves durability unknown and is reported as a storage
 * fault, since a durable intent outside `runtime.intents` waits for a restart.
 * @param runtime The engine whose daemon hears of the fault.
 * @param error The store's failure.
 * @returns The send's failure.
 */
function failBind(
  runtime: EngineRuntime,
  error: EndpointStoreError,
): Effect.Effect<never, SendError> {
  return error.reason === "persistence"
    ? runtime.input
        .reportStorageFault(error)
        .pipe(
          Effect.zipRight(
            Effect.fail(new SendError({ reason: "outcome-unknown" })),
          ),
        )
    : Effect.fail(storeFailure(error));
}

const sendReasonByStoreReason = {
  closed: "persistence-failed",
  conflict: "message-conflict",
  corrupt: "persistence-failed",
  incompatible: "persistence-failed",
  "invalid-continuation": "persistence-failed",
  "invalid-input": "persistence-failed",
  "not-found": "persistence-failed",
  persistence: "persistence-failed",
} as const satisfies Readonly<
  Record<EndpointStoreError["reason"], SendError["reason"]>
>;

function storeFailure(error: EndpointStoreError): SendError {
  return new SendError({ reason: sendReasonByStoreReason[error.reason] });
}

const representationFailure = (): SendError =>
  new SendError({ reason: "certification-unavailable" });

/**
 * How long a send waits for the Router worker to attach before failing as
 * `network-unavailable`.
 *
 * A cold-start attach can span a Router tail hold, `HOLD_DURATION` in
 * `@moltzap/router`, and the daemon's own `ROUTER_POLL_TIMEOUT`, so a bound
 * near either one fails sends that are merely early. It stays under the MCP
 * SDK's `DEFAULT_REQUEST_TIMEOUT_MSEC`, the deadline a host client applies to
 * the tool call, so the host receives this closed failure rather than a
 * transport timeout it cannot classify, and the remaining margin covers the
 * send itself.
 */
const ROUTER_ATTACH_TIMEOUT = Duration.seconds(45);

const conversationIdOf = (
  cards: readonly [
    VerifiedAgentCard,
    VerifiedAgentCard,
    ...VerifiedAgentCard[],
  ],
) =>
  deriveConversationId([
    cards[0].agentId,
    cards[1].agentId,
    ...cards.slice(2).map((card) => card.agentId),
  ]).pipe(Effect.mapError(representationFailure));

const membershipFor = (
  runtime: EngineRuntime,
  cards: readonly [
    VerifiedAgentCard,
    VerifiedAgentCard,
    ...VerifiedAgentCard[],
  ],
) =>
  Effect.gen(function* () {
    const conversationId = yield* conversationIdOf(cards);
    const stored = runtime.conversations.get(conversationId);
    if (stored !== undefined) {
      return stored.membership;
    }
    const descriptor = yield* Schema.decodeUnknown(MembershipDescriptorSchema)({
      sharnessVersion: AgentCard.wireVersion(cards[0]),
      kind: "membership_descriptor",
      conversationId,
      members: yield* Effect.forEach(
        cards,
        (card) => Schema.encode(AgentCard)(card),
        { concurrency: 1 },
      ).pipe(Effect.catchTag("ParseError", representationFailure)),
    }).pipe(Effect.mapError(representationFailure));
    return yield* verifyMembershipDescriptor(
      descriptor,
      runtime.input.registrySignerPublicKey,
    ).pipe(Effect.mapError(representationFailure));
  });

function resolveMembership(runtime: EngineRuntime, message: Message) {
  return Effect.gen(function* () {
    const members = messageMembers(message);
    const cards = yield* Effect.forEach(
      members,
      (member) => resolveMemberCard(runtime, member),
      { concurrency: 1 },
    );
    cards.sort((left, right) => compareAgentIds(left.agentId, right.agentId));
    const [first, second, ...rest] = cards;
    if (first === undefined || second === undefined) {
      return yield* new SendError({ reason: "membership-invalid" });
    }
    return yield* membershipFor(runtime, [first, second, ...rest]);
  });
}

function resolveMemberCard(runtime: EngineRuntime, member: Message["author"]) {
  const local = runtime.input.localAgentCard;
  const pinned =
    member.agentId === local.agentId
      ? local
      : [...runtime.conversations.values()]
          .flatMap((conversation) => conversation.membership.members)
          .find((card) => card.agentId === member.agentId);
  const resolved =
    pinned === undefined
      ? runtime.input.registry.lookup({ agentId: member.agentId }).pipe(
          Effect.mapError(registryFailure),
          Effect.flatMap((result) =>
            result.kind === "found"
              ? Effect.succeed(result.agentCard)
              : Effect.fail(new SendError({ reason: "unknown-agent" })),
          ),
        )
      : Effect.succeed(pinned);
  return resolved.pipe(
    Effect.filterOrFail(
      (card) => {
        const view = agentViewOf(card);
        return (
          view.agentId === member.agentId &&
          view.agentName === member.agentName &&
          view.principalId === member.principalId
        );
      },
      () => new SendError({ reason: "membership-invalid" }),
    ),
  );
}

function registryFailure() {
  return new SendError({ reason: "network-unavailable" });
}

const proposalAction = (
  conversation: EngineConversation,
  intent: MessageIntent,
): Effect.Effect<ActionCore, SendError, SqlClient.SqlClient> =>
  Effect.gen(function* () {
    const messageIntentHash = yield* hashMessageIntent(intent).pipe(
      Effect.mapError(representationFailure),
    );
    const head = conversation.head;
    if (head === undefined) {
      const anchor = conversation.currentAnchor;
      if (anchor.kind !== "initial_anchor_body") {
        return yield* Effect.fail(representationFailure());
      }
      const action: ActionCore = {
        sharnessVersion: intent.sharnessVersion,
        kind: "FIRST_MESSAGE",
        conversationId: conversation.conversationId,
        membership: conversation.membership.descriptor,
        anchor,
        previousRecordHash: null,
        messageIntent: intent,
        messageIntentHash,
      };
      return action;
    }
    const action: ActionCore = {
      sharnessVersion: intent.sharnessVersion,
      kind: "MESSAGE",
      conversationId: conversation.conversationId,
      membershipHash: conversation.membership.hash,
      anchorHash: anchorHashAtHead(conversation, head),
      previousRecordHash: head.recordHash,
      messageIntent: intent,
      messageIntentHash,
    };
    return action;
  });

/**
 * Rebase one unchanged local intent against the conversation's current head.
 * @param runtime Current engine state and protocol dependencies.
 * @param localIntent Durable immutable message intent awaiting certification.
 * @returns The predecessor-bound action hash proposed for this attempt.
 */
export const proposeIntent = (
  runtime: EngineRuntime,
  localIntent: EngineMessageIntent,
): Effect.Effect<ActionHash, SendError, SqlClient.SqlClient> =>
  Effect.gen(function* () {
    const conversation = runtime.conversations.get(
      localIntent.intent.conversationId,
    );
    if (conversation === undefined) {
      return yield* Effect.fail(representationFailure());
    }
    const action = yield* proposalAction(conversation, localIntent.intent);
    const actionHash = yield* hashAction(action).pipe(
      Effect.mapError(representationFailure),
    );
    if (localIntent.proposedActionHash === actionHash) {
      return actionHash;
    }
    yield* queueLocalProposal(runtime, {
      conversation,
      localIntent,
      action,
      actionHash,
    });
    return actionHash;
  }).pipe(
    Effect.tapError((error) =>
      isTerminalSendFailure(error.reason)
        ? Store.failMessage(
            {
              authorAgentId: localIntent.intent.authorAgentId,
              msgId: localIntent.intent.msgId,
            },
            error.reason,
          ).pipe(
            Effect.mapError(storeFailure),
            Effect.zipRight(Deferred.fail(localIntent.completion, error)),
            Effect.tap(() =>
              Effect.sync(() =>
                runtime.intents.delete(localIntent.intent.msgId),
              ),
            ),
            Effect.asVoid,
          )
        : Effect.void,
    ),
    Effect.withSpan("proposeIntent"),
  );

interface LocalProposal {
  readonly conversation: EngineConversation;
  readonly localIntent: EngineMessageIntent;
  readonly action: ActionCore;
  readonly actionHash: ActionHash;
}

/**
 * A proposal's resends: the catch-up retry delays, until a resend finds none
 * due.
 */
const proposalResendSchedule = catchUpRetrySchedule.pipe(
  Schedule.whileInput((due: boolean) => due),
);

/**
 * Queue a proposal behind the certified record it extends, then resend both
 * while its action certificate is incomplete. A store failure while
 * queueing is reported as a storage fault: the intent stays bound but not
 * proposed until recovery after a restart proposes it. Activation proposes
 * inside an uninterruptible region and a forked fiber inherits it, so the
 * resends run interruptibly and end with the engine.
 * @param runtime Engine that sends the proposal.
 * @param proposal The local proposal and the intent it proposes.
 * @returns Completion once both envelopes are queued, the intent records the
 *     proposed action, and its resends are scheduled.
 */
function queueLocalProposal(
  runtime: EngineRuntime,
  proposal: LocalProposal,
): Effect.Effect<void, SendError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    yield* Effect.uninterruptible(
      queueProposal(runtime, proposal).pipe(
        Effect.catchTags({
          EndpointStoreError: (error) =>
            runtime.input
              .reportStorageFault(error)
              .pipe(Effect.zipRight(Effect.fail(storeFailure(error)))),
          ClientRepresentationError: () => Effect.fail(representationFailure()),
        }),
        Effect.zipRight(
          Effect.sync(() => {
            proposal.localIntent.proposedActionHash = proposal.actionHash;
          }),
        ),
      ),
    );
    yield* Effect.scheduleFrom(
      resendProposal(runtime, proposal),
      true,
      proposalResendSchedule,
    ).pipe(
      markRetries("proposal", proposal.action.messageIntent.msgId, (exit) =>
        resendOutcome(runtime, proposal, exit),
      ),
      Effect.interruptible,
      Effect.forkIn(runtime.scope),
    );
  });
}

/**
 * How a proposal's resends ended: the engine ended them; a defect did; they
 * ran out while the proposal still awaited its action certificate; its message
 * was certified; or a later proposal of the intent, or a moved head,
 * replaced it.
 */
function resendOutcome(
  runtime: EngineRuntime,
  proposal: LocalProposal,
  exit: Exit.Exit<unknown>,
): RetryOutcome {
  const { action, actionHash } = proposal;
  if (Exit.isFailure(exit)) {
    return Exit.isInterrupted(exit) ? "interrupted" : "failed";
  }
  if (
    runtime.completedMessages.has(action.messageIntent.msgId) ||
    runtime.actionFolds.get(actionHash)?.recordHash !== undefined
  ) {
    return "certified";
  }
  const conversation = runtime.conversations.get(action.conversationId);
  return conversation !== undefined &&
    proposalIsOpen(runtime, proposal, conversation)
    ? "gave-up"
    : "superseded";
}

/**
 * Queue a proposal and, for a MESSAGE, the certified record of the head it
 * extends ahead of it. A faulty member can seal its durability vote so that
 * only some members open it, so a member may not have certified the head the
 * proposer certified when the proposal arrives, and it would drop the
 * proposal as not gap-free. The proposer's own copy of that record, which
 * every member can open, reaches each member first.
 * @param runtime Engine that sends the proposal.
 * @param proposal The proposal and the conversation it extends.
 * @returns Completion once both envelopes are queued.
 */
function queueProposal(
  runtime: EngineRuntime,
  proposal: LocalProposal,
): Effect.Effect<void, EngineOutboxError, SqlClient.SqlClient> {
  const head = proposal.conversation.head;
  const queueHead =
    head === undefined
      ? Effect.void
      : runtime.outbox.queuePacket(proposal.conversation, head.record);
  return queueHead.pipe(
    Effect.zipRight(
      runtime.outbox.queuePacket(proposal.conversation, {
        sharnessVersion: proposal.action.sharnessVersion,
        kind: "action_proposal",
        action: proposal.action,
      }),
    ),
  );
}

/**
 * Send a proposal again, with its head, while it is still the intent's
 * proposal at the conversation's head and its action certificate is
 * incomplete. Members that dropped it, such as one still catching up while
 * fenced, sign the new copy; members that signed already deduplicate it by
 * `ActionHash`. A fenced conversation sends nothing, and its resends wait for
 * it to recover without consuming attempts. The wait releases the engine
 * gate so recovery can complete. A store failure ends the resends and
 * reports a storage fault.
 * @param runtime Engine that sends the proposal.
 * @param proposal The proposal, the conversation it extends, and its intent.
 * @returns Whether a later resend is still due.
 */
function resendProposal(
  runtime: EngineRuntime,
  proposal: LocalProposal,
): Effect.Effect<boolean, never, SqlClient.SqlClient> {
  const { action } = proposal;
  return runtime.gate
    .withPermits(1)(
      Effect.suspend(
        (): Effect.Effect<
          boolean | Deferred.Deferred<undefined>,
          never,
          SqlClient.SqlClient
        > => {
          const conversation = runtime.conversations.get(action.conversationId);
          if (
            conversation === undefined ||
            !proposalIsOpen(runtime, proposal, conversation)
          ) {
            return Effect.succeed(false);
          }
          const barrier = pendingRecoveryFence(runtime, action.conversationId);
          if (barrier !== undefined) {
            return Effect.succeed(barrier);
          }
          return Effect.uninterruptible(
            queueProposal(runtime, { ...proposal, conversation }),
          ).pipe(
            Effect.as(true),
            Effect.catchTags({
              EndpointStoreError: (error) =>
                runtime.input.reportStorageFault(error).pipe(Effect.as(false)),
              ClientRepresentationError: () => Effect.succeed(false),
            }),
          );
        },
      ),
    )
    .pipe(
      Effect.flatMap((due) =>
        typeof due === "boolean"
          ? Effect.succeed(due)
          : Deferred.await(due).pipe(
              Effect.zipRight(resendProposal(runtime, proposal)),
            ),
      ),
    );
}

/**
 * Whether a proposal still awaits its action certificate: it is still its
 * intent's proposal, its message has not completed, no record certifies it, and
 * the conversation's head is still the one it extends.
 * @param runtime Engine that sent the proposal.
 * @param proposal The proposal and the intent it proposes.
 * @param conversation The conversation's current state.
 * @returns Whether a resend of the proposal is still useful.
 */
function proposalIsOpen(
  runtime: EngineRuntime,
  proposal: LocalProposal,
  conversation: EngineConversation,
): boolean {
  const { action, actionHash, localIntent } = proposal;
  const head = conversation.head?.recordHash ?? null;
  return (
    localIntent.proposedActionHash === actionHash &&
    !runtime.completedMessages.has(action.messageIntent.msgId) &&
    runtime.actionFolds.get(actionHash)?.recordHash === undefined &&
    head === action.previousRecordHash
  );
}

const createConversation = (
  runtime: EngineRuntime,
  membership: VerifiedMembership,
): Effect.Effect<
  Readonly<{
    conversation: EngineConversation;
    foundation: ConversationFoundation;
  }>,
  SendError
> =>
  Effect.gen(function* () {
    const routerAnchor = yield* runtime.input.routerWorker.currentAnchor.pipe(
      Effect.mapError(currentAnchorFailure),
    );
    const anchor = yield* Schema.decodeUnknown(InitialAnchorBody)({
      sharnessVersion: membership.descriptor.sharnessVersion,
      kind: "initial_anchor_body",
      conversationId: membership.descriptor.conversationId,
      membershipHash: membership.hash,
      routerInstanceId: routerAnchor.routerInstanceId,
    }).pipe(Effect.mapError(representationFailure));
    const anchorHash = yield* hashAnchor(anchor).pipe(
      Effect.mapError(representationFailure),
    );
    const foundation: ConversationFoundation = {
      conversationId: membership.descriptor.conversationId,
      membershipHash: membership.hash,
      canonicalMembership: yield* encodeCanonical(
        MembershipDescriptorSchema,
        membership.descriptor,
      ).pipe(Effect.mapError(representationFailure)),
      anchorHash,
      canonicalAnchor: yield* encodeCanonical(InitialAnchorBody, anchor).pipe(
        Effect.mapError(representationFailure),
      ),
    };
    return {
      foundation,
      conversation: {
        conversationId: membership.descriptor.conversationId,
        membership,
        currentAnchor: anchor,
      },
    };
  });

function currentAnchorFailure(error: RouterWorkerUnavailableError): SendError {
  const reasonByTag = {
    RouterWorkerUnavailableError: "network-unavailable",
  } as const satisfies Readonly<
    Record<RouterWorkerUnavailableError["_tag"], SendError["reason"]>
  >;
  return new SendError({ reason: reasonByTag[error._tag] });
}

/**
 * Wait before taking the engine gate: worker recovery needs that same gate
 * before attachment can finish, including immediately after registration.
 */
const awaitRouterAttachment = (
  runtime: EngineRuntime,
): Effect.Effect<void, SendError> =>
  runtime.input.routerWorker.currentAnchor.pipe(
    Effect.catchTag("RouterWorkerUnavailableError", () =>
      runtime.input.routerWorker.awaitAnchor.pipe(
        Effect.timeoutFail({
          duration: ROUTER_ATTACH_TIMEOUT,
          onTimeout: () => new SendError({ reason: "network-unavailable" }),
        }),
      ),
    ),
    Effect.asVoid,
  );

/**
 * One durably bound send: its minted identity and its completion latch, which
 * yields the hash of the message's locally stored certified record.
 */
export interface AcceptedMessage {
  readonly msgId: MsgId;
  readonly completion: Deferred.Deferred<RecordHash, SendError>;
}

interface PreparedIntent {
  readonly membership: VerifiedMembership;
  readonly intent: MessageIntent;
  readonly canonicalIntent: Uint8Array;
  readonly canonicalMessage: Uint8Array;
}

/** Captured message identity survives preparation and every subsequent retry. */
export const acceptMessage = (
  runtime: EngineRuntime,
  message: Message,
): Effect.Effect<AcceptedMessage, SendError, SqlClient.SqlClient> =>
  Effect.gen(function* () {
    if (message.author.agentId !== runtime.input.localAgentCard.agentId) {
      return yield* new SendError({ reason: "membership-invalid" });
    }
    const sorted = messageMembers(message)
      .map((member) => member.agentId)
      .sort(compareAgentIds);
    const [first, second, ...rest] = sorted;
    if (first === undefined || second === undefined) {
      return yield* new SendError({ reason: "membership-invalid" });
    }
    const conversationId = yield* deriveConversationId([
      first,
      second,
      ...rest,
    ]).pipe(Effect.mapError(representationFailure));
    const canonicalMessage = yield* encodeCanonical(Message, message).pipe(
      Effect.mapError(representationFailure),
    );
    yield* Store.acceptMessage({
      authorAgentId: message.author.agentId,
      msgId: message.msgId,
      conversationId,
      canonicalMessage,
    }).pipe(Effect.mapError(storeFailure));
    const retained = runtime.intents.get(message.msgId);
    if (retained !== undefined) {
      yield* requireSameMessage(runtime, retained, message);
      return { msgId: message.msgId, completion: retained.completion };
    }
    yield* awaitRouterAttachment(runtime);
    const membership = yield* resolveMembership(runtime, message);
    const intent = yield* Schema.decodeUnknown(MessageIntentSchema)({
      sharnessVersion: runtime.wireVersion,
      kind: "message_intent",
      conversationId: membership.descriptor.conversationId,
      membershipHash: membership.hash,
      authorAgentId: message.author.agentId,
      msgId: message.msgId,
      content: message.content,
    }).pipe(Effect.mapError(representationFailure));
    const canonicalIntent = yield* encodeCanonical(
      MessageIntentSchema,
      intent,
    ).pipe(Effect.mapError(representationFailure));
    const activation = yield* activateIntent(runtime, {
      membership,
      intent,
      canonicalIntent,
      canonicalMessage,
    });
    return { msgId: message.msgId, completion: activation.completion };
  }).pipe(Effect.withSpan("acceptMessage"));

function requireSameMessage(
  runtime: EngineRuntime,
  retained: EngineMessageIntent,
  message: Message,
) {
  return Effect.gen(function* () {
    const membership = runtime.conversations.get(
      retained.intent.conversationId,
    )?.membership;
    if (membership === undefined) {
      return yield* representationFailure();
    }
    const original = yield* messageFromIntent(
      membership.members.map(agentViewOf),
      retained.intent,
    ).pipe(Effect.mapError(representationFailure));
    const bytes = yield* Effect.all([
      encodeCanonical(Message, original),
      encodeCanonical(Message, message),
    ]).pipe(Effect.mapError(representationFailure));
    if (!sameBytes(bytes[0], bytes[1])) {
      return yield* new SendError({ reason: "message-conflict" });
    }
  });
}

function storedIntent(prepared: PreparedIntent): StoredMessageIntent {
  return {
    conversationId: prepared.intent.conversationId,
    membershipHash: prepared.intent.membershipHash,
    authorAgentId: prepared.intent.authorAgentId,
    msgId: prepared.intent.msgId,
    canonicalIntent: prepared.canonicalIntent,
    canonicalMessage: prepared.canonicalMessage,
  };
}

function bindPreparedIntent(
  runtime: EngineRuntime,
  prepared: PreparedIntent,
): Effect.Effect<void, SendError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const message = {
      intent: storedIntent(prepared),
    };
    const retained = runtime.conversations.get(prepared.intent.conversationId);
    if (retained !== undefined) {
      if (retained.membership.hash !== prepared.membership.hash) {
        return yield* new SendError({ reason: "certification-unavailable" });
      }
      return yield* Store.bindMessageIntent({
        kind: "existing-conversation",
        ...message,
      }).pipe(
        Effect.catchTag("EndpointStoreError", (error) =>
          failBind(runtime, error),
        ),
        Effect.asVoid,
      );
    }
    const created = yield* createConversation(runtime, prepared.membership);
    yield* Store.bindMessageIntent({
      kind: "new-conversation",
      foundation: created.foundation,
      ...message,
    }).pipe(
      Effect.catchTag("EndpointStoreError", (error) =>
        failBind(runtime, error),
      ),
    );
    runtime.conversations.set(
      created.conversation.conversationId,
      created.conversation,
    );
  });
}

type IntentActivation =
  | Readonly<{
      kind: "ready";
      completion: Deferred.Deferred<RecordHash, SendError>;
    }>
  | Readonly<{ kind: "waiting"; barrier: Deferred.Deferred<undefined> }>;

function activateIntent(
  runtime: EngineRuntime,
  prepared: PreparedIntent,
): Effect.Effect<
  Exclude<IntentActivation, { kind: "waiting" }>,
  SendError,
  SqlClient.SqlClient
> {
  return runtime.gate
    .withPermits(1)(
      Effect.uninterruptible(activateIntentOnce(runtime, prepared)),
    )
    .pipe(
      Effect.flatMap((activation) => {
        switch (activation.kind) {
          case "ready":
            return Effect.succeed(activation);
          case "waiting":
            return runtime.phases
              .rearmCatchUp(runtime, prepared.intent.conversationId)
              .pipe(
                Effect.zipRight(Deferred.await(activation.barrier)),
                Effect.zipRight(activateIntent(runtime, prepared)),
              );
          default: {
            const exhaustive: never = activation;
            return exhaustive;
          }
        }
      }),
    );
}

function activateIntentOnce(
  runtime: EngineRuntime,
  prepared: PreparedIntent,
): Effect.Effect<IntentActivation, SendError, SqlClient.SqlClient> {
  const { intent } = prepared;
  return Effect.gen(function* () {
    const barrier = pendingRecoveryFence(runtime, intent.conversationId);
    if (barrier !== undefined) {
      return { kind: "waiting", barrier } satisfies IntentActivation;
    }
    yield* bindPreparedIntent(runtime, prepared);
    return yield* activateBoundIntent(runtime, prepared);
  });
}

/**
 * Propose a message whose intent `bindPreparedIntent` durably stored, or complete
 * it when it already certified. The intent is in the store and
 * `runtime.intents` before any proposal, so a failure here does not mean the
 * message was not sent: `rebasePendingIntents` proposes it again when the
 * conversation's head moves, and recovery proposes it after a restart. So
 * the activation stays ready after any failure here, and its completion
 * latch fails the send as `delivery-pending`.
 * @param runtime Engine whose conversation and outbox take the proposal.
 * @param prepared The stored intent with its canonical bytes.
 * @returns The ready activation and its completion latch.
 */
function activateBoundIntent(
  runtime: EngineRuntime,
  prepared: PreparedIntent,
): Effect.Effect<IntentActivation, SendError, SqlClient.SqlClient> {
  const { canonicalIntent, intent } = prepared;
  return Effect.gen(function* () {
    const retained = runtime.intents.get(intent.msgId);
    if (retained !== undefined) {
      const completedRecordHash = runtime.completedMessages.get(intent.msgId);
      if (completedRecordHash !== undefined) {
        yield* Deferred.succeed(retained.completion, completedRecordHash);
      } else {
        yield* proposeIntent(runtime, retained);
      }
      return {
        kind: "ready",
        completion: retained.completion,
      } satisfies IntentActivation;
    }
    const completion = yield* Deferred.make<RecordHash, SendError>();
    const localIntent: EngineMessageIntent = {
      intent,
      canonicalIntent,
      completion,
    };
    const completedRecordHash = runtime.completedMessages.get(intent.msgId);
    yield* Effect.sync(() => {
      runtime.intents.set(intent.msgId, localIntent);
    });
    if (completedRecordHash !== undefined) {
      yield* Deferred.succeed(completion, completedRecordHash);
    } else {
      yield* proposeIntent(runtime, localIntent);
    }
    return { kind: "ready", completion } satisfies IntentActivation;
  }).pipe(Effect.catchAll(() => pendingActivation()));
}

/**
 * A bound intent's activation after its proposal failed: still ready, with a
 * latch that fails as `delivery-pending`.
 */
function pendingActivation(): Effect.Effect<IntentActivation> {
  return Deferred.make<RecordHash, SendError>().pipe(
    Effect.tap((completion) =>
      Deferred.fail(completion, new SendError({ reason: "delivery-pending" })),
    ),
    Effect.map(
      (completion): IntentActivation => ({ kind: "ready", completion }),
    ),
  );
}
