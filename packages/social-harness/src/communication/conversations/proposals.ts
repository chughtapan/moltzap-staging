/** @file Address resolution, immutable intent binding, and proposal creation. */

import type { SqlClient } from "@effect/sql/SqlClient";
import { Deferred, Duration, Effect, Exit, Schedule, Schema } from "effect";
import type { RouterWorkerUnavailableError } from "../router/connection.js";
import type {
  AcceptedMessage,
  EngineConversation,
  EngineMessageIntent,
  EngineOutboxError,
  EngineRuntime,
} from "./conversation.js";
import { encodeCanonical } from "../../encoding/canonical-json.js";
import {
  agentCardOf,
  AgentIdentityCard,
  type VerifiedAgentIdentityCard,
} from "../../identity/agent-cards.js";
import { markRetries, type RetryOutcome } from "../../tracing.js";
import { isTerminalSendFailure, SendError } from "../errors.js";
import { compareAgentIds, type Message, messageMembers } from "../messages.js";
import {
  type ActionCore,
  type ActionHash,
  deriveConversationId,
  hashAction,
  hashAnchor,
  hashMembershipDescriptor,
  hashMessageIntent,
  InitialAnchorBody,
  MembershipDescriptor as MembershipDescriptorSchema,
  type MessageIntent,
  MessageIntent as MessageIntentSchema,
  type VerifiedMembership,
} from "../records.js";
import {
  bindMessageIntent,
  type ConversationFoundation,
  type EndpointStoreError,
  failMessage,
  type MessageIntent as StoredMessageIntent,
} from "../store.js";
import { anchorHashAtHead } from "./history.js";
import { catchUpRetrySchedule, pendingRecoveryFence } from "./recovery.js";

// safer-arch-ignore no-fat-orchestrator: Immutable intent binding and protocol proposal share the engine gate and durable conversation binding.

/**
 * A semantic store refusal rolls back acceptance. A persistence failure leaves
 * it uncertain, so report a storage fault: a durable intent absent from memory
 * requires restart to resume.
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
 * Allows cold attachment to span a Router tail hold and daemon poll timeout.
 * Stays below the MCP client's default timeout, leaving time to report a
 * classified failure and complete the send.
 */
const ROUTER_ATTACH_TIMEOUT = Duration.seconds(45);

const conversationIdOf = (
  cards: readonly [
    VerifiedAgentIdentityCard,
    VerifiedAgentIdentityCard,
    ...VerifiedAgentIdentityCard[],
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
    VerifiedAgentIdentityCard,
    VerifiedAgentIdentityCard,
    ...VerifiedAgentIdentityCard[],
  ],
) =>
  Effect.gen(function* () {
    const conversationId = yield* conversationIdOf(cards);
    const stored = runtime.conversations.get(conversationId);
    if (stored !== undefined) {
      return stored.membership;
    }
    const descriptor: typeof MembershipDescriptorSchema.Type = {
      sharnessVersion: AgentIdentityCard.wireVersion(cards[0]),
      kind: "membership_descriptor",
      conversationId,
      members: [
        yield* Schema.encode(AgentIdentityCard)(cards[0]),
        yield* Schema.encode(AgentIdentityCard)(cards[1]),
        ...(yield* Effect.forEach(
          cards.slice(2),
          (card) => Schema.encode(AgentIdentityCard)(card),
          { concurrency: 1 },
        )),
      ],
    };
    return {
      descriptor,
      hash: yield* hashMembershipDescriptor(descriptor),
      members: cards,
    };
  }).pipe(Effect.mapError(representationFailure));

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
              ? Effect.succeed(result.identityCard)
              : Effect.fail(new SendError({ reason: "unknown-agent" })),
          ),
        )
      : Effect.succeed(pinned);
  return resolved.pipe(
    Effect.filterOrFail(
      (card) => {
        const view = agentCardOf(card);
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
): Effect.Effect<ActionCore, SendError, SqlClient> =>
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
): Effect.Effect<ActionHash, SendError, SqlClient> => {
  const agentKey = runtime.input.agentKey;
  return Effect.gen(function* () {
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
        ? failMessage(
            agentKey,
            {
              authorAgentId: localIntent.intent.authorAgentId,
              msgId: localIntent.intent.msgId,
            },
            error.reason,
          ).pipe(
            Effect.mapError(storeFailure),
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
};

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
 * Persistence failure after binding requires restart to resume the intent.
 * Resends run interruptibly in the engine scope even when activation inherits
 * an uninterruptible region.
 */
function queueLocalProposal(
  runtime: EngineRuntime,
  proposal: LocalProposal,
): Effect.Effect<void, SendError, SqlClient> {
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
 * Queues the certified predecessor before a MESSAGE proposal. A faulty
 * member may seal evidence so another member cannot open it and certify the
 * head. The proposer supplies a readable copy to fill that gap.
 */
function queueProposal(
  runtime: EngineRuntime,
  proposal: LocalProposal,
): Effect.Effect<void, EngineOutboxError, SqlClient> {
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
 * Fenced conversations release the engine gate and wait without spending
 * attempts. Members deduplicate repeated proposals by ActionHash. Store failure
 * ends resends and reports a storage fault.
 */
function resendProposal(
  runtime: EngineRuntime,
  proposal: LocalProposal,
): Effect.Effect<boolean, never, SqlClient> {
  const { action } = proposal;
  return runtime.gate
    .withPermits(1)(
      Effect.suspend(
        (): Effect.Effect<
          boolean | Deferred.Deferred<undefined>,
          never,
          SqlClient
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
 * Resends are useful only for the intent's current, uncertified proposal at
 * the unchanged conversation head.
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

interface PreparedIntent {
  readonly membership: VerifiedMembership;
  readonly intent: MessageIntent;
  readonly canonicalIntent: Uint8Array;
  readonly canonicalMessage: Uint8Array;
}

/** Uses the immutable payload retained at Communication acceptance. */
export const prepareMessage = (
  runtime: EngineRuntime,
  accepted: AcceptedMessage,
): Effect.Effect<void, SendError, SqlClient> =>
  Effect.gen(function* () {
    const { message, canonicalMessage } = accepted;
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
    yield* activateIntent(runtime, {
      membership,
      intent,
      canonicalIntent,
      canonicalMessage,
    });
  }).pipe(Effect.withSpan("prepareMessage"));

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
): Effect.Effect<void, SendError, SqlClient> {
  const agentKey = runtime.input.agentKey;

  return Effect.gen(function* () {
    const intent = storedIntent(prepared);
    const retained = runtime.conversations.get(prepared.intent.conversationId);
    if (retained !== undefined) {
      if (retained.membership.hash !== prepared.membership.hash) {
        return yield* new SendError({ reason: "certification-unavailable" });
      }
      return yield* bindMessageIntent(agentKey, {
        kind: "existing-conversation",
        intent,
      }).pipe(
        Effect.catchTag("EndpointStoreError", (error) =>
          failBind(runtime, error),
        ),
        Effect.asVoid,
      );
    }
    const created = yield* createConversation(runtime, prepared.membership);
    yield* bindMessageIntent(agentKey, {
      kind: "new-conversation",
      foundation: created.foundation,
      intent,
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

function activateIntent(
  runtime: EngineRuntime,
  prepared: PreparedIntent,
): Effect.Effect<void, SendError, SqlClient> {
  return runtime.gate
    .withPermits(1)(
      Effect.uninterruptible(
        Effect.gen(function* () {
          const barrier = pendingRecoveryFence(
            runtime,
            prepared.intent.conversationId,
          );
          if (barrier !== undefined) {
            return barrier;
          }
          yield* bindPreparedIntent(runtime, prepared);
          yield* activateBoundIntent(runtime, prepared);
          return undefined;
        }),
      ),
    )
    .pipe(
      Effect.flatMap((barrier) =>
        barrier === undefined
          ? Effect.void
          : runtime.phases
              .rearmCatchUp(runtime, prepared.intent.conversationId)
              .pipe(
                Effect.zipRight(Deferred.await(barrier)),
                Effect.zipRight(activateIntent(runtime, prepared)),
              ),
      ),
    );
}

/**
 * A bound intent remains recoverable when its initial proposal fails.
 * Retain its identity before proposing so certification and recovery find it.
 */
function activateBoundIntent(
  runtime: EngineRuntime,
  prepared: PreparedIntent,
): Effect.Effect<void, SendError, SqlClient> {
  const { canonicalIntent, intent } = prepared;
  return Effect.gen(function* () {
    if (runtime.completedMessages.has(intent.msgId)) {
      return;
    }
    const localIntent = runtime.intents.get(intent.msgId) ?? {
      intent,
      canonicalIntent,
    };
    runtime.intents.set(intent.msgId, localIntent);
    yield* proposeIntent(runtime, localIntent);
  }).pipe(Effect.catchAll(() => Effect.void));
}
