/** @file Captures questions and claims exact outgoing answers. */

import { Clock, Effect, Option, Queue, Schema } from "effect";
import type { Message } from "../message.js";
import type { Question } from "./model.js";
import { AgentView } from "../../identity/cards.js";
import {
  answerFieldsText,
  type CollectiveId,
  CollectivesError,
} from "./contracts.js";
import {
  type Environment,
  makeMessage,
  outgoingAddress,
  requestItem,
  sameParticipants,
  saveMessage,
  self,
  transaction,
} from "./operations.js";
import * as Store from "./store.js";
import { validateAnswer } from "./validation.js";
import {
  collectiveRequesterOf,
  encodeCollectiveContent,
  readCollectiveValue,
} from "./wire.js";

/** The fixed question and its request messages are accepted in one transaction. */
export const ask = Effect.fn("ask")(function* (
  environment: Environment,
  question: Question,
) {
  const author = yield* self(environment);
  if (
    !Schema.equivalence(AgentView)(question.author, author) ||
    collectiveRequesterOf(question.operationId) !== author.agentId ||
    (question.op === "all_gather" && question.recipients.length < 2)
  ) {
    return yield* new CollectivesError({ reason: "invalid-question" });
  }
  yield* transaction(saveQuestion(environment, question));
});

/** Request content stays fixed while liveness is rechecked when an answer is accepted. */
export const getRequest = Effect.fn("getRequest")(function* (
  operationId: CollectiveId,
) {
  const operation = yield* Store.readCollectiveOperation(operationId);
  if (operation === undefined || operation.origin !== "remote") {
    return yield* new CollectivesError({ reason: "request-none" });
  }
  return requestItem(operation.request.message, operation.request.value)
    .message;
});

/** Acceptance claims the answer until its original message completes or fails. */
export const answer = Effect.fn("answer")(function* (
  environment: Environment,
  message: Message,
) {
  const author = yield* self(environment);
  const response = yield* readResponse(message);
  if (!Schema.equivalence(AgentView)(message.author, author)) {
    return yield* new CollectivesError({ reason: "answer-invalid" });
  }
  const request = yield* getRequest(response.operationId);
  if (response.action === "accept") {
    yield* validateAnswer(
      request.content.requestedSchema,
      response.content,
    ).pipe(
      Effect.matchEffect({
        onFailure: (fields) =>
          new CollectivesError({
            reason: "answer-invalid",
            detail: answerFieldsText(fields),
          }),
        onSuccess: () => Effect.void,
      }),
    );
  }
  yield* transaction(
    claimAnswer(environment, message, response.operationId),
  ).pipe(
    Effect.flatMap((inserted) =>
      inserted
        ? Queue.offer(environment.observations, {
            operationId: response.operationId,
            role: "response",
            destination: outgoingAddress(message),
            reference: {
              authorAgentId: message.author.agentId,
              msgId: message.msgId,
            },
          })
        : Effect.void,
    ),
    Effect.uninterruptible,
  );
});

function saveQuestion(environment: Environment, question: Question) {
  return Effect.gen(function* () {
    const existing = yield* Store.readCollectiveOperation(question.operationId);
    if (
      existing === undefined &&
      question.deadlineAt <= (yield* Clock.currentTimeMillis)
    ) {
      return yield* new CollectivesError({ reason: "invalid-question" });
    }
    yield* Store.putCollectiveOperation({ origin: "local", request: question });
    if (existing !== undefined) {
      return;
    }
    const content = yield* encodeCollectiveContent(
      {
        kind: "operation",
        op: question.op,
        operationId: question.operationId,
        deadlineAt: question.deadlineAt,
        requestedSchema: question.requestedSchema,
      },
      question.text,
    ).pipe(
      Effect.catchTag(
        "CollectiveContentError",
        () => new CollectivesError({ reason: "invalid-question" }),
      ),
    );
    const audiences =
      question.op === "gather"
        ? question.recipients.map((recipient) => [recipient])
        : [question.recipients];
    for (const recipients of audiences) {
      const message = yield* makeMessage(question.author, recipients, content);
      yield* saveMessage(environment, question.operationId, "request", message);
    }
  });
}

function readResponse(message: Message) {
  return Effect.gen(function* () {
    const value = yield* readCollectiveValue(message.content).pipe(
      Effect.catchTag(
        "CollectivePartInvalidError",
        () => new CollectivesError({ reason: "answer-invalid" }),
      ),
    );
    if (Option.isNone(value) || value.value.kind !== "response") {
      return yield* new CollectivesError({ reason: "answer-invalid" });
    }
    return value.value;
  });
}

function claimAnswer(
  environment: Environment,
  message: Message,
  operationId: CollectiveId,
) {
  return Effect.gen(function* () {
    const operation = yield* Store.readCollectiveOperation(operationId);
    if (operation === undefined) {
      return yield* new CollectivesError({ reason: "request-none" });
    }
    const prior = (yield* Store.readCollectiveMessages(operationId)).find(
      (entry) => entry.role === "response",
    );
    if (prior !== undefined) {
      if (
        prior.reference.authorAgentId !== message.author.agentId ||
        prior.reference.msgId !== message.msgId
      ) {
        return yield* new CollectivesError({ reason: "request-answered" });
      }
      yield* saveMessage(environment, operationId, "response", message).pipe(
        Effect.catchTag("CollectivesError", (error) =>
          error.reason === "message-failed" &&
          error.detail === "message-conflict"
            ? new CollectivesError({ reason: "request-answered" })
            : Effect.fail(error),
        ),
      );
      return false;
    }
    if (operation.phase !== "collecting") {
      return yield* new CollectivesError({ reason: "request-none" });
    }
    if ((yield* Clock.currentTimeMillis) >= operation.deadlineAt) {
      return yield* new CollectivesError({ reason: "request-expired" });
    }
    if (operation.origin !== "remote") {
      return yield* new CollectivesError({ reason: "request-none" });
    }
    const source = operation.request.message;
    if (!sameParticipants(source, message)) {
      return yield* new CollectivesError({ reason: "answer-invalid" });
    }
    return yield* saveMessage(environment, operationId, "response", message);
  });
}
