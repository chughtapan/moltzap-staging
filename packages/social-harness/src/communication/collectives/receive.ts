/** @file Interprets verified collective traffic in its delivery transaction. */

import type * as SqlClient from "@effect/sql/SqlClient";
import { Clock, Effect, Option, Queue, Schema } from "effect";
import type { EndpointStoreError } from "../store/index.js";
import type { CollectivesError } from "./contracts.js";
import type { InboundItem as IncomingItem } from "./inbound.js";
import { Message } from "../message.js";
import {
  type Environment,
  operationParticipants,
  requestItem,
  sameParticipants,
  self,
  transaction,
} from "./operations.js";
import { finishLocal, finishRemote } from "./results.js";
import * as Store from "./store.js";
import {
  collectiveRequesterOf,
  type CollectiveValue,
  readCollectiveValue,
  type ResponseValue,
} from "./wire.js";

/** Only durable interpretation runs in the communication handoff transaction. */
export function receive(environment: Environment, message: Message) {
  return readCollectiveValue(message.content).pipe(
    Effect.matchEffect({
      onFailure: () => Effect.succeed(Option.none<IncomingItem>()),
      onSuccess: (decoded) =>
        Option.match(decoded, {
          onNone: () =>
            Effect.succeed(
              Option.some<IncomingItem>({ kind: "multicast", message }),
            ),
          onSome: (value) =>
            transaction(receivePart(environment, message, value)),
        }),
    }),
    Effect.withSpan("receive"),
  );
}

function receivePart(
  environment: Environment,
  message: Message,
  value: CollectiveValue,
): Effect.Effect<
  Option.Option<IncomingItem>,
  EndpointStoreError | CollectivesError,
  SqlClient.SqlClient
> {
  switch (value.kind) {
    case "operation":
      return receiveRequest(environment, message, value);
    case "response":
      return receiveAnswer(environment, message, value).pipe(
        Effect.as(Option.none()),
      );
    case "close":
      return receiveClose(environment, message, value).pipe(
        Effect.as(Option.none()),
      );
    default: {
      const exhaustive: never = value;
      return exhaustive;
    }
  }
}

function receiveRequest(
  environment: Environment,
  message: Message,
  value: Extract<CollectiveValue, { readonly kind: "operation" }>,
) {
  return Effect.gen(function* () {
    const local = yield* self(environment);
    if (
      !requestEligible(
        message,
        value,
        local.agentId,
        yield* Clock.currentTimeMillis,
      )
    ) {
      return Option.none<IncomingItem>();
    }
    const existing = yield* Store.readCollectiveOperation(value.operationId);
    if (existing !== undefined) {
      if (existing.origin !== "remote") {
        return Option.none<IncomingItem>();
      }
      const saved = existing.request.message;
      return Schema.equivalence(Message)(saved, message)
        ? Option.some<IncomingItem>(requestItem(message, value))
        : Option.none<IncomingItem>();
    }
    yield* Store.putCollectiveOperation({
      origin: "remote",
      request: message,
      value,
    });
    return Option.some<IncomingItem>(requestItem(message, value));
  });
}

function receiveAnswer(
  environment: Environment,
  message: Message,
  response: ResponseValue,
) {
  return Effect.gen(function* () {
    const operation = yield* Store.readCollectiveOperation(
      response.operationId,
    );
    if (operation === undefined || operation.phase === "completed") {
      return;
    }
    const participants = operationParticipants(operation);
    if (
      !participants.members.some(
        (member) => member.agentId === message.author.agentId,
      )
    ) {
      return;
    }
    if (
      participants.op === "all_gather"
        ? !sameParticipants(participants.request, message)
        : message.recipients.length !== 1 ||
          message.recipients[0]?.agentId !== participants.request.author.agentId
    ) {
      return;
    }
    const recorded = yield* Store.recordCollectiveAnswer(
      response.operationId,
      message,
      yield* Clock.currentTimeMillis,
    );
    if (recorded && operation.origin === "local") {
      const close = yield* finishLocal(environment, operation.operationId);
      if (close !== undefined) {
        yield* Queue.offer(environment.observations, close);
      }
    }
  });
}

function receiveClose(
  environment: Environment,
  message: Message,
  value: Extract<CollectiveValue, { readonly kind: "close" }>,
) {
  return Effect.gen(function* () {
    const operation = yield* Store.readCollectiveOperation(value.operationId);
    if (
      operation === undefined ||
      operation.origin !== "remote" ||
      operation.phase !== "collecting" ||
      operation.request.message.author.agentId !== message.author.agentId
    ) {
      return;
    }
    const request = operation.request.message;
    const participants = operationParticipants(operation);
    if (
      participants.op !== "all_gather" ||
      !sameParticipants(request, message)
    ) {
      return;
    }
    yield* Store.saveRemoteClose(value.operationId, message);
    yield* finishRemote(environment, value.operationId);
    const own = (yield* Store.readCollectiveMessages(value.operationId)).find(
      (entry) => entry.role === "response",
    );
    if (own !== undefined) {
      yield* Queue.offer(environment.observations, own);
    }
  });
}

function requestEligible(
  message: Message,
  value: Extract<CollectiveValue, { readonly kind: "operation" }>,
  localAgentId: Message["author"]["agentId"],
  now: number,
) {
  const matchingAuthor =
    collectiveRequesterOf(value.operationId) === message.author.agentId;
  const addressedToLocal = message.recipients.some(
    (recipient) => recipient.agentId === localAgentId,
  );
  const audience =
    value.op === "gather"
      ? message.recipients.length === 1
      : message.recipients.length >= 2;
  return (
    matchingAuthor && addressedToLocal && audience && value.deadlineAt > now
  );
}
