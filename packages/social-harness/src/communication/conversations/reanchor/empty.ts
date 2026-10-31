/** @file Empty-conversation foundation replacement after a Router restart. */

import type * as SqlClient from "@effect/sql/SqlClient";
import { Effect } from "effect";
import type {
  ConversationFoundation,
  EndpointRecovery,
  MessageIntent as StoredMessageIntent,
} from "../../store/index.js";
import type { EngineRuntime } from "../engine-context.js";
import {
  decodeCanonical,
  encodeCanonical,
  sameBytes,
} from "../../../encoding/canonical-json.js";
import {
  type ConversationId as ConversationIdValue,
  hashAnchor,
  InitialAnchorBody,
  type InitialAnchorBody as InitialAnchorBodyValue,
  type VerifiedMembership,
} from "../../records/index.js";
import { RouterWorkerPersistenceError } from "../../router/index.js";
import * as Store from "../../store/index.js";

interface EmptyPositionRestartInput {
  readonly runtime: EngineRuntime;
  /** The Router instance the replacement initial anchor binds. */
  readonly routerInstanceId: InitialAnchorBodyValue["routerInstanceId"];
  readonly membership: VerifiedMembership;
  readonly recovery: EndpointRecovery;
  readonly position: EndpointRecovery["positions"][number];
}

/**
 * Replace an initial foundation only while its certified history is empty. The
 * caller marks the conversation recovered. The durable replacement and the
 * engine's adoption of its anchor are one uninterruptible step: a retry reads
 * the durable anchor as already restarted and never resets the engine, so an
 * interruption between them would leave actions binding the old anchor.
 * @param input Durable and recovered state for one empty conversation.
 * @returns Completion after the new foundation is durable and active.
 */
export function restartEmptyPosition(
  input: EmptyPositionRestartInput,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient.SqlClient> {
  const { membership, position, recovery, routerInstanceId, runtime } = input;
  return Effect.gen(function* () {
    const expectedFoundation = yield* emptyConversationFoundation(
      recovery,
      membership,
      position,
    );
    const replacement = yield* replacementEmptyFoundation(
      routerInstanceId,
      membership,
      expectedFoundation,
    );
    yield* Effect.uninterruptible(
      Effect.gen(function* () {
        const restarted = yield* Store.restartEmptyConversation({
          expectedFoundation,
          replacementFoundation: replacement.foundation,
        }).pipe(Effect.mapError(persistenceFailure));
        if (
          !restartedEmptyStateMatches({
            runtime,
            conversationId: membership.descriptor.conversationId,
            expected: replacement.foundation,
            retained: restarted.foundation,
            messageIntents: restarted.messageIntents,
          })
        ) {
          return yield* Effect.fail(persistenceFailure());
        }
        yield* resetEmptyConversationRuntime(
          runtime,
          membership.descriptor.conversationId,
          replacement.anchor,
          restarted.messageIntents,
        );
      }),
    );
  }).pipe(Effect.withSpan("restartEmptyPosition"));
}

function emptyConversationFoundation(
  recovery: EndpointRecovery,
  membership: VerifiedMembership,
  position: EndpointRecovery["positions"][number],
): Effect.Effect<
  ConversationFoundation,
  RouterWorkerPersistenceError,
  SqlClient.SqlClient
> {
  const conversationId = membership.descriptor.conversationId;
  const storedMemberships = recovery.memberships.filter(
    (stored) => stored.conversationId === conversationId,
  );
  const storedAnchors = recovery.anchors.filter(
    (stored) => stored.conversationId === conversationId,
  );
  const storedMembership = storedMemberships[0];
  const storedAnchor = storedAnchors[0];
  if (
    storedMemberships.length !== 1 ||
    storedAnchors.length !== 1 ||
    storedMembership === undefined ||
    storedAnchor === undefined
  ) {
    return Effect.fail(persistenceFailure());
  }
  const foundationMatches = [
    position.conversationId === conversationId,
    position.membershipHash === membership.hash,
    position.currentAnchorHash === storedAnchor.anchorHash,
    storedMembership.membershipHash === membership.hash,
    storedAnchor.previousAnchorHash === undefined,
    storedAnchor.selectedRecordHash === undefined,
  ].every((matches) => matches);
  if (!foundationMatches) {
    return Effect.fail(persistenceFailure());
  }
  return Effect.succeed({
    conversationId,
    membershipHash: membership.hash,
    canonicalMembership: storedMembership.canonicalMembership,
    anchorHash: storedAnchor.anchorHash,
    canonicalAnchor: storedAnchor.canonicalAnchor,
  });
}

interface ReplacementEmptyFoundation {
  readonly anchor: InitialAnchorBodyValue;
  readonly foundation: ConversationFoundation;
}

function replacementEmptyFoundation(
  routerInstanceId: InitialAnchorBodyValue["routerInstanceId"],
  membership: VerifiedMembership,
  expected: ConversationFoundation,
): Effect.Effect<
  ReplacementEmptyFoundation,
  RouterWorkerPersistenceError,
  SqlClient.SqlClient
> {
  return Effect.gen(function* () {
    const preceding = yield* decodeCanonical(
      InitialAnchorBody,
      expected.canonicalAnchor,
    ).pipe(Effect.mapError(persistenceFailure));
    const anchor: InitialAnchorBodyValue = {
      sharnessVersion: preceding.sharnessVersion,
      kind: "initial_anchor_body",
      conversationId: membership.descriptor.conversationId,
      membershipHash: membership.hash,
      routerInstanceId,
    };
    return yield* Effect.all({
      anchorHash: hashAnchor(anchor),
      canonicalAnchor: encodeCanonical(InitialAnchorBody, anchor),
    }).pipe(
      Effect.mapError(persistenceFailure),
      Effect.map(({ anchorHash, canonicalAnchor }) => ({
        anchor,
        foundation: {
          conversationId: expected.conversationId,
          membershipHash: expected.membershipHash,
          canonicalMembership: expected.canonicalMembership,
          anchorHash,
          canonicalAnchor,
        },
      })),
    );
  });
}

interface RestartedEmptyState {
  readonly runtime: EngineRuntime;
  readonly conversationId: ConversationIdValue;
  readonly expected: ConversationFoundation;
  readonly retained: ConversationFoundation;
  readonly messageIntents: readonly StoredMessageIntent[];
}

function restartedEmptyStateMatches(input: RestartedEmptyState): boolean {
  const { conversationId, expected, messageIntents, retained, runtime } = input;
  const conversation = runtime.conversations.get(conversationId);
  const runtimeIntents = [...runtime.intents.values()].filter(
    (pending) => pending.intent.conversationId === expected.conversationId,
  );
  if (conversation === undefined || conversation.head !== undefined) {
    return false;
  }
  const foundationMatches = [
    expected.conversationId === retained.conversationId,
    expected.membershipHash === retained.membershipHash,
    expected.anchorHash === retained.anchorHash,
    sameBytes(expected.canonicalMembership, retained.canonicalMembership),
    sameBytes(expected.canonicalAnchor, retained.canonicalAnchor),
  ].every((matches) => matches);
  return (
    foundationMatches &&
    runtimeIntents.length === messageIntents.length &&
    messageIntents.every((stored) => storedIntentMatches(runtime, stored))
  );
}

function storedIntentMatches(
  runtime: EngineRuntime,
  stored: StoredMessageIntent,
): boolean {
  const pending = runtime.intents.get(stored.msgId);
  if (pending === undefined || stored.completedRecordHash !== undefined) {
    return false;
  }
  return [
    pending.intent.conversationId === stored.conversationId,
    pending.intent.membershipHash === stored.membershipHash,
    pending.intent.authorAgentId === stored.authorAgentId,
    pending.intent.msgId === stored.msgId,
    sameBytes(pending.canonicalIntent, stored.canonicalIntent),
  ].every((matches) => matches);
}

function resetEmptyConversationRuntime(
  runtime: EngineRuntime,
  conversationId: ConversationIdValue,
  anchor: InitialAnchorBodyValue,
  messageIntents: readonly StoredMessageIntent[],
): Effect.Effect<void, never, SqlClient.SqlClient> {
  return Effect.sync(() => {
    replaceRuntimeAnchor(runtime, conversationId, anchor);
    discardConversationFolds(runtime, conversationId);
    clearIntentProposals(runtime, messageIntents);
  });
}

function replaceRuntimeAnchor(
  runtime: EngineRuntime,
  conversationId: ConversationIdValue,
  anchor: InitialAnchorBodyValue,
): void {
  const conversation = runtime.conversations.get(conversationId);
  if (conversation !== undefined) {
    conversation.currentAnchor = anchor;
    delete conversation.head;
  }
}

function discardConversationFolds(
  runtime: EngineRuntime,
  conversationId: ConversationIdValue,
): void {
  for (const [actionHash, fold] of runtime.actionFolds) {
    if (fold.conversation.conversationId === conversationId) {
      runtime.actionFolds.delete(actionHash);
    }
  }
  for (const [recordHash, fold] of runtime.recordFolds) {
    if (fold.conversation.conversationId === conversationId) {
      runtime.recordFolds.delete(recordHash);
    }
  }
}

function clearIntentProposals(
  runtime: EngineRuntime,
  messageIntents: readonly StoredMessageIntent[],
): void {
  for (const stored of messageIntents) {
    const pending = runtime.intents.get(stored.msgId);
    if (pending !== undefined) {
      pending.proposedActionHash = undefined;
    }
  }
}

function persistenceFailure(): RouterWorkerPersistenceError {
  return new RouterWorkerPersistenceError();
}
