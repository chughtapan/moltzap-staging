/** @file Freezes selected answers and publishes recoverable results. */

import { Clock, Deferred, Effect, Option, Schema } from "effect";
import type { AgentView } from "../../identity/cards.js";
import type { Message, MessageReference } from "../message.js";
import type { collectiveClosure, CollectiveId } from "./contracts.js";
import type { StoredCollectiveOperation } from "./store.js";
import { MAXIMUM_RUNTIME_ITEM_BYTES } from "../../encoding/limits.js";
import { encodeRuntimeValue, EndpointStoreError } from "../store/index.js";
import { ResultDelivery, type ResultSources } from "./model.js";
import {
  type Environment,
  makeMessage,
  operationParticipants,
  saveMessage,
  self,
  transaction,
} from "./operations.js";
import * as Store from "./store.js";
import { outcomeOfResponse } from "./validation.js";
import {
  type CollectiveValue,
  encodeCollectiveContent,
  type FormModeSchema,
  readCollectiveValue,
} from "./wire.js";

type Closure = typeof collectiveClosure.Type;
type Participants = ReturnType<typeof operationParticipants>;
type Close = Extract<CollectiveValue, { readonly kind: "close" }>;

/** Final fields are frozen before the close is accepted in the same transaction. */
export const finishLocal = Effect.fn("finishLocal")(function* (
  environment: Environment,
  operationId: CollectiveId,
) {
  return yield* transaction(freezeLocal(environment, operationId));
});

/** Close and own-answer events advance remote completion without a periodic scan. */
export const finishRemote = Effect.fn("finishRemote")(function* (
  environment: Environment,
  operationId: CollectiveId,
) {
  yield* transaction(completeRemote(environment, operationId));
});

/** Final item and assembled source bytes retain independent publication budgets. */
export function checkResultSize(result: {
  readonly item: ResultDelivery["item"];
  readonly sources: ResultSources;
}) {
  return Effect.gen(function* () {
    const item = yield* encodeRuntimeValue(result.item);
    const sources = yield* encodeRuntimeValue(result.sources);
    if (
      item.byteLength > MAXIMUM_RUNTIME_ITEM_BYTES ||
      sources.byteLength > MAXIMUM_RUNTIME_ITEM_BYTES
    ) {
      return yield* new EndpointStoreError({ reason: "invalid-input" });
    }
  }).pipe(Effect.withSpan("checkResultSize"));
}

/** Handler success and handoff retirement share the caller fiber transaction. */
export function consumeResults<E, R>(
  environment: Environment,
  handle: (result: ResultDelivery) => Effect.Effect<void, E, R>,
) {
  return deliverNextResult(handle).pipe(
    Effect.forever,
    Effect.raceFirst(Deferred.await(environment.failed)),
    Effect.withSpan("consumeResults"),
  );
}

function freezeLocal(environment: Environment, operationId: CollectiveId) {
  return Effect.gen(function* () {
    const operation = yield* Store.readCollectiveOperation(operationId);
    if (
      operation === undefined ||
      operation.origin !== "local" ||
      operation.phase !== "collecting"
    ) {
      return undefined;
    }
    const participants = operationParticipants(operation);
    const answers = yield* Store.readCollectiveAnswers(operationId);
    const closure = completionAt(
      operation.deadlineAt,
      participants.members.length,
      answers.length,
      yield* Clock.currentTimeMillis,
    );
    if (closure === undefined) {
      return undefined;
    }
    const result = yield* makeResult(operation, answers, closure);
    yield* checkResultSize(result);
    yield* Store.saveCollectiveResult({
      operationId,
      item: result.item,
      answers: answers.map((answer) => ({
        authorAgentId: answer.author.agentId,
        msgId: answer.msgId,
      })),
      complete: participants.op === "gather",
    });
    if (participants.op === "gather") {
      return undefined;
    }
    const close = yield* makeGroupClose(
      operation,
      participants,
      answers,
      closure,
    );
    yield* saveMessage(environment, operationId, "close", close);
    return {
      operationId,
      role: "close" as const,
      destination: participants.to,
      reference: { authorAgentId: close.author.agentId, msgId: close.msgId },
    };
  });
}

function completionAt(
  deadlineAt: number,
  members: number,
  answers: number,
  now: number,
): Closure | undefined {
  if (answers < members && now < deadlineAt) {
    return undefined;
  }
  return {
    closedAt: now,
    closedBy: answers === members ? "settled" : "deadline",
  };
}

function makeGroupClose(
  operation: StoredCollectiveOperation,
  participants: Participants,
  answers: readonly Message[],
  closure: Closure,
) {
  return Effect.gen(function* () {
    const operationId = operation.operationId;
    const content = yield* encodeCollectiveContent({
      kind: "close",
      operationId,
      ...closure,
      included: answers.map((answer) => ({
        authorAgentId: answer.author.agentId,
        msgId: answer.msgId,
      })),
    }).pipe(
      Effect.catchTag(
        "CollectiveContentError",
        () => new EndpointStoreError({ reason: "corrupt" }),
      ),
    );
    return yield* makeMessage(
      participants.request.author,
      participants.request.recipients,
      content,
    );
  });
}

function completeRemote(environment: Environment, operationId: CollectiveId) {
  return Effect.gen(function* () {
    const operation = yield* Store.readCollectiveOperation(operationId);
    if (
      operation === undefined ||
      operation.origin !== "remote" ||
      operation.phase !== "finalizing" ||
      operation.close === undefined
    ) {
      return;
    }
    const { value } = operation.close;
    const selected = yield* selectAnswers(environment, operationId, value);
    if (Option.isSome(selected)) {
      const result = yield* makeResult(operation, selected.value, {
        closedAt: value.closedAt,
        closedBy: value.closedBy,
      });
      yield* checkResultSize(result);
      yield* Store.saveCollectiveResult({
        operationId,
        item: result.item,
        answers: value.included,
        complete: true,
      });
    }
  });
}

function selectAnswers(
  environment: Environment,
  operationId: CollectiveId,
  close: Close,
) {
  return Effect.gen(function* () {
    if (
      new Set(close.included.map((reference) => reference.authorAgentId))
        .size !== close.included.length
    ) {
      yield* Store.retireCollectiveOperation(operationId);
      return Option.none<readonly Message[]>();
    }
    const answers = yield* Store.readCollectiveAnswers(operationId);
    const selected = answersFor(answers, close.included);
    if (selected.length === close.included.length) {
      return Option.some(selected);
    }
    if (
      yield* ownAnswerPending(environment, operationId, answers, close.included)
    ) {
      return Option.none<readonly Message[]>();
    }
    yield* Store.retireCollectiveOperation(operationId);
    return Option.none<readonly Message[]>();
  });
}

function answersFor(
  answers: readonly Message[],
  included: readonly MessageReference[],
) {
  return included.flatMap((reference) => {
    const answer = answers.find(
      (message) =>
        message.author.agentId === reference.authorAgentId &&
        message.msgId === reference.msgId,
    );
    return answer === undefined ? [] : [answer];
  });
}

function ownAnswerPending(
  environment: Environment,
  operationId: CollectiveId,
  answers: readonly Message[],
  included: readonly MessageReference[],
) {
  return Effect.gen(function* () {
    const local = yield* self(environment);
    const own = included.find(
      (reference) => reference.authorAgentId === local.agentId,
    );
    if (
      own === undefined ||
      answers.some(
        (answer) =>
          answer.author.agentId === own.authorAgentId &&
          answer.msgId === own.msgId,
      )
    ) {
      return false;
    }
    return (yield* Store.readCollectiveMessages(operationId)).some(
      (entry) =>
        entry.role === "response" &&
        entry.reference.authorAgentId === own.authorAgentId &&
        entry.reference.msgId === own.msgId,
    );
  });
}

function makeResult(
  operation: StoredCollectiveOperation,
  answers: readonly Message[],
  closure: Closure,
) {
  return Effect.gen(function* () {
    const participants = operationParticipants(operation);
    const outcomes = yield* Effect.forEach(
      participants.members,
      (member) =>
        memberResult(
          member,
          answers,
          participants.requestedSchema,
          operation.operationId,
        ),
      { concurrency: 1 },
    );
    const [first, ...rest] = outcomes;
    if (first === undefined) {
      return yield* new EndpointStoreError({ reason: "corrupt" });
    }
    const item = yield* Schema.decodeUnknown(ResultDelivery.fields.item)({
      kind: "collectiveResult",
      op: participants.op,
      operationId: operation.operationId,
      ...closure,
      to: participants.to,
      question: participants.question,
      outcomes: [first, ...rest],
    }).pipe(
      Effect.catchTag(
        "ParseError",
        () => new EndpointStoreError({ reason: "corrupt" }),
      ),
    );
    const request =
      operation.origin === "remote" ? operation.request.message : undefined;
    return {
      item,
      sources: { answers, ...(request === undefined ? {} : { request }) },
    };
  });
}

function memberResult(
  member: AgentView,
  answers: readonly Message[],
  requestedSchema: FormModeSchema,
  operationId: string,
) {
  return Effect.gen(function* () {
    const message = answers.find(
      (answer) => answer.author.agentId === member.agentId,
    );
    if (message === undefined) {
      return { member, outcome: { kind: "no-answer" as const } };
    }
    const value = yield* readCollectiveValue(message.content).pipe(
      Effect.catchTag(
        "CollectivePartInvalidError",
        () => new EndpointStoreError({ reason: "corrupt" }),
      ),
    );
    if (
      Option.isNone(value) ||
      value.value.kind !== "response" ||
      value.value.operationId !== operationId
    ) {
      return yield* new EndpointStoreError({ reason: "corrupt" });
    }
    return {
      member,
      outcome: yield* outcomeOfResponse(requestedSchema, value.value),
    };
  });
}

function deliverNextResult<E, R>(
  handle: (result: ResultDelivery) => Effect.Effect<void, E, R>,
) {
  return Effect.gen(function* () {
    const operation = yield* Store.readNextCollectiveResult();
    if (operation === undefined) {
      return yield* Effect.sleep("100 millis");
    }
    if (operation.result === undefined) {
      return yield* new EndpointStoreError({ reason: "corrupt" });
    }
    const answers = answersFor(
      yield* Store.readCollectiveAnswers(operation.operationId),
      operation.result.answers,
    );
    if (answers.length !== operation.result.answers.length) {
      return yield* new EndpointStoreError({ reason: "corrupt" });
    }
    const request =
      operation.origin === "remote" &&
      operation.result.item.kind === "collectiveResult"
        ? operation.request.message
        : undefined;
    const sources: ResultSources = {
      answers,
      ...(request === undefined ? {} : { request }),
    };
    const result: ResultDelivery = {
      operationId: operation.operationId,
      deliveryToken: operation.deliveryToken,
      item: operation.result.item,
      sources,
    };
    yield* checkResultSize(result);
    yield* transaction(
      handle(result).pipe(
        Effect.zipRight(Store.retireCollectiveResult(operation.operationId)),
      ),
    );
  });
}
