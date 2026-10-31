/** @file Certified head updates and reproposal of pending local messages. */

import type * as SqlClient from "@effect/sql/SqlClient";
import { Effect } from "effect";
import type { CertifiedRecord } from "../../records/index.js";
import type {
  EngineActionFold,
  EngineConversation,
  EngineMessageIntent,
  EngineRuntime,
} from "../engine-context.js";
import type { SendError } from "../errors.js";
import { RouterWorkerPersistenceError } from "../../router/index.js";

/** Updates the certified head and local completion index after durable promotion. */
export function completePromotion(
  runtime: EngineRuntime,
  fold: EngineActionFold,
  record: CertifiedRecord,
): Effect.Effect<void> {
  return Effect.sync(() => {
    fold.certified = true;
    fold.conversation.head = {
      recordHash: record.actionCertifiedRecord.recordHash,
      record,
    };
    if (
      fold.action.messageIntent.authorAgentId !==
      runtime.input.localAgentCard.agentId
    ) {
      return;
    }
    runtime.completedMessages.add(fold.action.messageIntent.msgId);
  });
}

type ReproposalDisposition = "fail" | "ignore";

const reproposalDispositionByReason = {
  "outcome-unknown": "fail",
  "certification-unavailable": "ignore",
  "content-invalid": "fail",
  "message-conflict": "fail",
  "invalid-address": "fail",
  "membership-invalid": "fail",
  "network-unavailable": "ignore",
  "delivery-pending": "ignore",
  "not-registered": "fail",
  "persistence-failed": "fail",
  "unknown-agent": "fail",
  "version-mismatch": "fail",
} as const satisfies Readonly<
  Record<SendError["reason"], ReproposalDisposition>
>;

const reproposePendingIntent = (
  runtime: EngineRuntime,
  pending: EngineMessageIntent,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient.SqlClient> =>
  runtime.phases.proposeIntent(runtime, pending).pipe(
    Effect.asVoid,
    Effect.catchTag("SendError", (error) =>
      reproposalDispositionByReason[error.reason] === "ignore"
        ? Effect.void
        : Effect.fail(new RouterWorkerPersistenceError()),
    ),
  );

/**
 * Propose a conversation's uncertified messages again at its new head. A
 * conversation still recovering proposes nothing here: its endpoint ignores
 * the Router's echo of a proposal while fenced, so the proposal would be
 * signed by members but never by its author, and its recovery proposes the
 * messages at the head it settles on.
 * @param runtime Engine whose pending messages are rebased.
 * @param conversationId Conversation whose head moved.
 * @returns Completion once each pending message is proposed again.
 */
export const rebasePendingIntents = (
  runtime: EngineRuntime,
  conversationId: EngineConversation["conversationId"],
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient.SqlClient> =>
  runtime.phases.isRecovering(runtime, conversationId)
    ? Effect.void
    : Effect.forEach(
        runtime.intents.values(),
        (pending) =>
          pending.intent.conversationId === conversationId &&
          !runtime.completedMessages.has(pending.intent.msgId)
            ? reproposePendingIntent(runtime, pending)
            : Effect.void,
        { concurrency: 1, discard: true },
      );
