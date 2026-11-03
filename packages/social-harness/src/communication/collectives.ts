/** @file Retained requests select replies and publish completed results. */

import { SqlClient } from "@effect/sql/SqlClient";
import { Cause, Clock, Deferred, Effect, Option, Schema, Scope } from "effect";
import {
  encodeCanonical,
  MAXIMUM_RUNTIME_ITEM_BYTES,
} from "../encoding/canonical-json.js";
import { AgentCard } from "../identity/agent-cards.js";
import { CommunicationError } from "./errors.js";
import {
  type collectiveClosure,
  type CollectiveId,
  collectiveRequesterOf,
  type CollectiveValue,
  encodeCollectiveContent,
  isReceivedRequest,
  Message,
  messageInterpretation,
  messageMembers,
  type MessageReference,
  outcomeOfResponse,
  type ReceivedRequest,
  type Request,
  type RequestedSchema,
  RequestError,
  type ResponseValue,
  type ResultDelivery,
  ResultSources,
  validateAnswer,
} from "./messages.js";
import {
  completeCollectiveOperation,
  encodeRuntimeValue,
  EndpointStoreError,
  failCollectiveClose,
  putCollectiveMessage,
  putCollectiveOperation,
  readAcceptedMessage,
  readCollectiveAnswers,
  readCollectiveMessages,
  readCollectiveOperation,
  readCollectiveOperationIds,
  readNextCollectiveResult,
  readReadyCollectiveMessages,
  recordCollectiveAnswer,
  retireCollectiveOperation,
  retireCollectiveResult,
  saveCollectiveResult,
  saveRemoteClose,
  type StoredCollectiveMessage,
  type StoredCollectiveOperation,
} from "./store.js";

// safer-arch-ignore no-fat-orchestrator: Request lifecycle and its workers share one private owner inside Communication.

type IncomingItem =
  | { readonly kind: "message"; readonly message: Message }
  | { readonly kind: "request"; readonly message: ReceivedRequest };

/** Construction captures shared resources without creating another public service. */
export const makeRequests = Effect.fn("Communication.requests")(function* (
  agentKey: string,
  input: {
    readonly self: AgentCard;
    readonly acceptMessage: (
      message: Message,
    ) => Effect.Effect<void, CommunicationError>;
  },
) {
  const environment: Environment = {
    agentKey,
    ...input,
    sql: yield* SqlClient,
    scope: yield* Scope.Scope,
    failed: yield* Deferred.make<never, RequestError | CommunicationError>(),
  };
  yield* start(environment, runWorker(environment));
  const capture = <A, E, R>(
    effect: Effect.Effect<A, E | EndpointStoreError, R>,
  ) =>
    effect.pipe(
      Effect.catchTag(
        "EndpointStoreError",
        () => new CommunicationError({ reason: "persistence-failed" }),
      ),
    );
  return {
    accept: (request: Request) => capture(ask(environment, request)),
    answer: (message: Message) => capture(answer(environment, message)),
    getRequest: (operationId: CollectiveId) =>
      capture(getRequest(agentKey, operationId)),
    receive: (message: Message) => capture(receive(environment, message)),
    deliverNextResult: <E, R>(
      handle: (
        result: ResultDelivery,
        operation: StoredCollectiveOperation,
      ) => Effect.Effect<void, E, R>,
    ) => deliverNextResult(agentKey, handle),
    failed: Deferred.await(environment.failed),
  };
});

/** One service scope supervises deadlines and retained message completions. */
interface Environment {
  readonly agentKey: string;
  readonly sql: SqlClient;
  readonly self: AgentCard;
  readonly acceptMessage: (
    message: Message,
  ) => Effect.Effect<void, CommunicationError>;
  readonly scope: Scope.Scope;
  readonly failed: Deferred.Deferred<never, RequestError | CommunicationError>;
}

type Closure = typeof collectiveClosure.Type;

type Close = Extract<CollectiveValue, { readonly kind: "close" }>;

/** Fixed request identity and every fanout message enter storage in one transaction. */
function ask(environment: Environment, request: Request) {
  const agentKey = environment.agentKey;

  return Effect.gen(function* () {
    if (
      !Schema.equivalence(AgentCard)(request.author, environment.self) ||
      collectiveRequesterOf(request.operationId) !== environment.self.agentId ||
      (request.op === "all_gather" && request.recipients.length < 2)
    ) {
      return yield* new CommunicationError({ reason: "membership-invalid" });
    }
    yield* transaction(
      Effect.gen(function* () {
        const existing = yield* readCollectiveOperation(
          agentKey,
          request.operationId,
        );
        if (
          existing === undefined &&
          request.deadlineAt <= (yield* Clock.currentTimeMillis)
        ) {
          return yield* new RequestError({
            operationId: request.operationId,
            failure: { kind: "deadline-passed" },
          });
        }
        yield* putCollectiveOperation(agentKey, { origin: "local", request });
        if (existing !== undefined) {
          return;
        }
        const content = yield* encodeCollectiveContent(
          {
            kind: "operation",
            op: request.op,
            operationId: request.operationId,
            deadlineAt: request.deadlineAt,
            requestedSchema: request.requestedSchema,
          },
          request.question,
        ).pipe(
          Effect.catchTag(
            "CollectiveContentError",
            () => new CommunicationError({ reason: "content-invalid" }),
          ),
        );
        const audiences =
          request.op === "gather"
            ? request.recipients.map((recipient): Message["recipients"] => [
                recipient,
              ])
            : [request.recipients];
        for (const recipients of audiences) {
          const message = yield* Message.create({
            author: request.author,
            recipients,
            content,
          });
          yield* saveMessage(
            environment,
            request.operationId,
            "request",
            message,
          );
        }
      }),
    );
  }).pipe(Effect.withSpan("Requests.accept"));
}

/** Looking up a request reserves nothing and reports its current answer eligibility. */
function getRequest(agentKey: string, operationId: CollectiveId) {
  return Effect.gen(function* () {
    const operation = yield* readCollectiveOperation(agentKey, operationId);
    if (operation === undefined || operation.origin !== "remote") {
      return yield* new RequestError({
        operationId,
        failure: { kind: "request-none" },
      });
    }
    if (
      (yield* readCollectiveMessages(agentKey, operationId)).some(
        (entry) => entry.role === "response",
      )
    ) {
      return yield* new RequestError({
        operationId,
        failure: { kind: "request-answered" },
      });
    }
    if (operation.phase !== "collecting") {
      return yield* new RequestError({
        operationId,
        failure: { kind: "request-none" },
      });
    }
    if ((yield* Clock.currentTimeMillis) >= operation.deadlineAt) {
      return yield* new RequestError({
        operationId,
        failure: { kind: "request-expired" },
      });
    }
    return yield* receivedRequest(operation.request.message);
  }).pipe(Effect.withSpan("Requests.getRequest"));
}

/** The response claim and original Message acceptance commit together, including retries. */
function answer(environment: Environment, message: Message) {
  const agentKey = environment.agentKey;

  return Effect.gen(function* () {
    const response = message.operation;
    if (
      response?.kind !== "response" ||
      !Schema.equivalence(AgentCard)(message.author, environment.self)
    ) {
      return yield* new CommunicationError({ reason: "content-invalid" });
    }
    yield* transaction(
      Effect.gen(function* () {
        const operation = yield* readCollectiveOperation(
          agentKey,
          response.operationId,
        );
        if (operation === undefined || operation.origin !== "remote") {
          return yield* new RequestError({
            operationId: response.operationId,
            failure: { kind: "request-none" },
          });
        }
        const source = yield* receivedRequest(operation.request.message);
        if (!sameParticipants(source, message)) {
          return yield* new CommunicationError({
            reason: "membership-invalid",
          });
        }
        if (response.action === "accept") {
          const fields = yield* validateAnswer(
            source.operation.requestedSchema,
            response.content,
          ).pipe(Effect.flip, Effect.option);
          if (Option.isSome(fields)) {
            return yield* new RequestError({
              operationId: response.operationId,
              failure: { kind: "answer-invalid", fields: fields.value },
            });
          }
        }
        const prior = (yield* readCollectiveMessages(
          agentKey,
          response.operationId,
        )).find((entry) => entry.role === "response");
        if (prior !== undefined) {
          if (
            prior.reference.authorAgentId !== message.author.agentId ||
            prior.reference.msgId !== message.msgId
          ) {
            return yield* new RequestError({
              operationId: response.operationId,
              failure: { kind: "request-answered" },
            });
          }
          yield* saveMessage(
            environment,
            response.operationId,
            "response",
            message,
          );
          return false;
        }
        if (operation.phase !== "collecting") {
          return yield* new RequestError({
            operationId: response.operationId,
            failure: { kind: "request-none" },
          });
        }
        if ((yield* Clock.currentTimeMillis) >= operation.deadlineAt) {
          return yield* new RequestError({
            operationId: response.operationId,
            failure: { kind: "request-expired" },
          });
        }
        return yield* saveMessage(
          environment,
          response.operationId,
          "response",
          message,
        );
      }),
    );
  }).pipe(Effect.withSpan("Requests.acceptAnswer"));
}

function receivedRequest(
  message: Message,
): Effect.Effect<ReceivedRequest, CommunicationError> {
  return isReceivedRequest(message)
    ? Effect.succeed(message)
    : new CommunicationError({ reason: "history-gap" });
}

/** Only durable interpretation runs in the communication handoff transaction. */
function receive(environment: Environment, message: Message) {
  return messageInterpretation(message).pipe(
    Effect.matchEffect({
      onFailure: () => Effect.succeed(Option.none<IncomingItem>()),
      onSuccess: (decoded) =>
        Option.match(decoded, {
          onNone: () =>
            Effect.succeed(
              Option.some<IncomingItem>({ kind: "message", message }),
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
  EndpointStoreError | RequestError | CommunicationError,
  SqlClient
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
  const agentKey = environment.agentKey;

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
    const existing = yield* readCollectiveOperation(
      agentKey,
      value.operationId,
    );
    if (existing !== undefined) {
      if (existing.origin !== "remote") {
        return Option.none<IncomingItem>();
      }
      const saved = existing.request.message;
      return Schema.equivalence(Message)(saved, message)
        ? Option.fromNullable(requestItem(message))
        : Option.none<IncomingItem>();
    }
    yield* putCollectiveOperation(agentKey, {
      origin: "remote",
      request: message,
      value,
    });
    return Option.fromNullable(requestItem(message));
  });
}

/** The recognized question keeps the original message identity and participants. */
function requestItem(
  message: Message,
): Extract<IncomingItem, { readonly kind: "request" }> | undefined {
  return isReceivedRequest(message) ? { kind: "request", message } : undefined;
}

function receiveAnswer(
  environment: Environment,
  message: Message,
  response: ResponseValue,
) {
  const agentKey = environment.agentKey;

  return Effect.gen(function* () {
    const operation = yield* readCollectiveOperation(
      agentKey,
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
    const recorded = yield* recordCollectiveAnswer(
      agentKey,
      response.operationId,
      message,
      yield* Clock.currentTimeMillis,
    );
    if (recorded && operation.origin === "local") {
      yield* finishLocal(environment, operation.operationId);
    }
  });
}

function receiveClose(
  environment: Environment,
  message: Message,
  value: Extract<CollectiveValue, { readonly kind: "close" }>,
) {
  const agentKey = environment.agentKey;

  return Effect.gen(function* () {
    const operation = yield* readCollectiveOperation(
      agentKey,
      value.operationId,
    );
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
    yield* saveRemoteClose(agentKey, value.operationId, message);
    yield* finishRemote(environment, value.operationId);
  });
}

/** Fixed identities, names and principals must agree before operation traffic is accepted. */
function sameParticipants(
  left: Pick<Message, "author" | "recipients">,
  right: Pick<Message, "author" | "recipients">,
) {
  return Schema.equivalence(Schema.Array(AgentCard))(
    messageMembers(left),
    messageMembers(right),
  );
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

function deliverNextResult<E, R>(
  agentKey: string,
  handle: (
    result: ResultDelivery,
    operation: StoredCollectiveOperation,
  ) => Effect.Effect<void, E, R>,
) {
  return Effect.gen(function* () {
    const operation = yield* readNextCollectiveResult(agentKey);
    if (operation === undefined) {
      return false;
    }
    if (operation.result === undefined) {
      return yield* new EndpointStoreError({ reason: "corrupt" });
    }
    const answers = answersFor(
      yield* readCollectiveAnswers(agentKey, operation.operationId),
      operation.result.answers,
    );
    if (answers.length !== operation.result.answers.length) {
      return yield* new EndpointStoreError({ reason: "corrupt" });
    }
    const request =
      operation.result.item.kind === "result"
        ? yield* sourceRequest(operation)
        : undefined;
    const sources: ResultSources = {
      answers,
      ...(request === undefined ? {} : { request }),
    };
    const result: ResultDelivery = {
      operationId: operation.operationId,
      item: operation.result.item,
      sources,
    };
    yield* checkResultSize(result);
    yield* transaction(
      handle(result, operation).pipe(
        Effect.zipRight(
          retireCollectiveResult(agentKey, operation.operationId),
        ),
      ),
    );
    return true;
  });
}

/** One worker reconciles persisted completions and request deadlines after every restart. */
function runWorker(environment: Environment) {
  const agentKey = environment.agentKey;

  return Effect.gen(function* () {
    for (const saved of yield* readReadyCollectiveMessages(agentKey)) {
      yield* observeMessage(environment, saved);
    }
    for (const operationId of yield* readCollectiveOperationIds(
      agentKey,
      "remote",
      "finalizing",
    )) {
      yield* finishRemote(environment, operationId);
    }
    yield* localDeadlines(environment);
  }).pipe(Effect.zipRight(Effect.sleep("100 millis")), Effect.forever);
}

/** The scoped worker runs outside caller transaction contexts and reports failures to consumers. */
function start(
  environment: Environment,
  work: Effect.Effect<
    void,
    RequestError | CommunicationError | EndpointStoreError,
    SqlClient
  >,
) {
  return work.pipe(
    Effect.provideService(SqlClient, environment.sql),
    Effect.catchTag("EndpointStoreError", () =>
      Deferred.fail(
        environment.failed,
        new CommunicationError({ reason: "persistence-failed" }),
      ),
    ),
    Effect.catchTag("RequestError", (error) =>
      Deferred.fail(environment.failed, error),
    ),
    Effect.catchTag("CommunicationError", (error) =>
      Deferred.fail(environment.failed, error),
    ),
    Effect.catchAllCause((cause) =>
      Cause.isInterruptedOnly(cause)
        ? Effect.interrupt
        : Deferred.failCause(environment.failed, cause),
    ),
    Effect.asVoid,
    Effect.interruptible,
    Effect.forkIn(environment.scope),
    Effect.asVoid,
  );
}

/** Certification state is read from Communication's retained message in the same local database. */
function observeMessage(
  environment: Environment,
  saved: StoredCollectiveMessage,
) {
  const agentKey = environment.agentKey;

  return transaction(
    Effect.gen(function* () {
      const operation = yield* readCollectiveOperation(
        agentKey,
        saved.operationId,
      );
      if (operation === undefined || operation.phase === "completed") {
        return;
      }
      const accepted = yield* readAcceptedMessage(agentKey, saved.reference);
      if (accepted === undefined) {
        return yield* new EndpointStoreError({ reason: "corrupt" });
      }
      if (accepted.failure !== undefined) {
        return yield* failOperationMessage(agentKey, saved, accepted.failure);
      }
      if (!accepted.completed) {
        return;
      }
      if (saved.role === "response") {
        yield* recordCollectiveAnswer(
          agentKey,
          saved.operationId,
          accepted.message,
          yield* Clock.currentTimeMillis,
        );
        yield* finishRemote(environment, saved.operationId);
      } else if (saved.role === "close") {
        yield* completeCollectiveOperation(agentKey, saved.operationId);
      }
    }),
  );
}

function failOperationMessage(
  agentKey: string,
  saved: StoredCollectiveMessage,
  reason: string,
) {
  return Effect.gen(function* () {
    if (saved.role === "close") {
      const operation = yield* readCollectiveOperation(
        agentKey,
        saved.operationId,
      );
      if (operation === undefined) {
        return yield* new EndpointStoreError({ reason: "corrupt" });
      }
      const item: ResultDelivery["item"] = {
        kind: "request-failed",
        request: yield* capturedRequest(operation),
        error: `The request result could not be shared: ${reason}`,
      };
      yield* checkResultSize({ item, sources: { answers: [] } });
      yield* transaction(
        failCollectiveClose(agentKey, saved.operationId, item),
      );
    } else if (saved.role === "response") {
      yield* retireUnusableClose(agentKey, saved);
    }
  });
}

/** A certified close and the selected answers complete the same retained result. */
function finishRemote(environment: Environment, operationId: CollectiveId) {
  const agentKey = environment.agentKey;

  return transaction(
    Effect.gen(function* () {
      const operation = yield* readCollectiveOperation(agentKey, operationId);
      if (
        operation === undefined ||
        operation.origin !== "remote" ||
        operation.phase !== "finalizing" ||
        operation.close === undefined
      ) {
        return;
      }
      const value = operation.close;
      const selected = yield* selectAnswers(environment, operationId, value);
      if (Option.isSome(selected)) {
        const result = yield* makeResult(operation, selected.value, {
          closedAt: value.closedAt,
          closedBy: value.closedBy,
        });
        yield* checkResultSize(result);
        yield* saveCollectiveResult(agentKey, {
          operationId,
          item: result.item,
          answers: value.included,
          complete: true,
        });
      }
    }),
  ).pipe(Effect.withSpan("finishRemote"));
}

function selectAnswers(
  environment: Environment,
  operationId: CollectiveId,
  close: Close,
) {
  const agentKey = environment.agentKey;

  return Effect.gen(function* () {
    if (
      new Set(close.included.map((reference) => reference.authorAgentId))
        .size !== close.included.length
    ) {
      yield* retireCollectiveOperation(agentKey, operationId);
      return Option.none<readonly Message[]>();
    }
    const answers = yield* readCollectiveAnswers(agentKey, operationId);
    const selected = answersFor(answers, close.included);
    if (selected.length === close.included.length) {
      return Option.some(selected);
    }
    if (
      yield* ownAnswerPending(environment, operationId, answers, close.included)
    ) {
      return Option.none<readonly Message[]>();
    }
    yield* retireCollectiveOperation(agentKey, operationId);
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
  const agentKey = environment.agentKey;

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
    return (yield* readCollectiveMessages(agentKey, operationId)).some(
      (entry) =>
        entry.role === "response" &&
        entry.reference.authorAgentId === own.authorAgentId &&
        entry.reference.msgId === own.msgId,
    );
  });
}

/** All operations use the local identity captured by Communication construction. */
function self(environment: Environment) {
  return Effect.succeed(environment.self);
}

function localDeadlines(environment: Environment) {
  const agentKey = environment.agentKey;

  return Effect.gen(function* () {
    for (const operationId of yield* readCollectiveOperationIds(
      agentKey,
      "local",
      "collecting",
    )) {
      yield* Effect.uninterruptible(
        Effect.gen(function* () {
          yield* finishLocal(environment, operationId);
        }),
      );
    }
  });
}

/** Final fields and the close are accepted in the same transaction. */
function finishLocal(environment: Environment, operationId: CollectiveId) {
  const agentKey = environment.agentKey;

  return transaction(
    Effect.gen(function* () {
      const operation = yield* readCollectiveOperation(agentKey, operationId);
      if (
        operation === undefined ||
        operation.origin !== "local" ||
        operation.phase !== "collecting"
      ) {
        return undefined;
      }
      const participants = operationParticipants(operation);
      const answers = yield* readCollectiveAnswers(agentKey, operationId);
      const now = yield* Clock.currentTimeMillis;
      if (
        answers.length < participants.members.length &&
        now < operation.deadlineAt
      ) {
        return undefined;
      }
      const closure: Closure = {
        closedAt: now,
        closedBy:
          answers.length === participants.members.length
            ? "settled"
            : "deadline",
      };
      const result = yield* makeResult(operation, answers, closure);
      yield* checkResultSize(result);
      const included = answers.map((answer) => ({
        authorAgentId: answer.author.agentId,
        msgId: answer.msgId,
      }));
      yield* saveCollectiveResult(agentKey, {
        operationId,
        item: result.item,
        answers: included,
        complete: participants.op === "gather",
      });
      if (participants.op === "gather") {
        return undefined;
      }
      const content = yield* encodeCollectiveContent({
        kind: "close",
        operationId,
        ...closure,
        included,
      }).pipe(
        Effect.catchTag(
          "CollectiveContentError",
          () => new EndpointStoreError({ reason: "corrupt" }),
        ),
      );
      const close = yield* Message.create({
        author: operation.request.author,
        recipients: operation.request.recipients,
        content,
      });
      yield* saveMessage(environment, operationId, "close", close);
    }),
  ).pipe(Effect.withSpan("finishLocal"));
}

/** Acceptance and the collective claim use the same caller transaction. */
function saveMessage(
  environment: Environment,
  operationId: CollectiveId,
  role: "request" | "response" | "close",
  message: Message,
) {
  const agentKey = environment.agentKey;

  return Effect.gen(function* () {
    const inserted = yield* putCollectiveMessage(agentKey, {
      operationId,
      role,
      destination: outgoingAddress(message),
      reference: {
        authorAgentId: message.author.agentId,
        msgId: message.msgId,
      },
    });
    yield* environment.acceptMessage(message);
    return inserted;
  }).pipe(Effect.withSpan("saveMessage"));
}

/** A direct outgoing message names its recipient rather than its author. */
function outgoingAddress(message: Pick<Message, "author" | "recipients">) {
  return messageMembers(message)
    .map((member) => member.agentId)
    .join(",");
}

/** Final item and assembled source bytes retain independent publication budgets. */
function checkResultSize(result: {
  readonly item: ResultDelivery["item"];
  readonly sources: ResultSources;
}) {
  return Effect.gen(function* () {
    const item = yield* encodeRuntimeValue(result.item);
    const sources = yield* encodeCanonical(ResultSources, result.sources).pipe(
      Effect.catchTag(
        "ClientRepresentationError",
        () => new EndpointStoreError({ reason: "invalid-input" }),
      ),
    );
    if (
      item.byteLength > MAXIMUM_RUNTIME_ITEM_BYTES ||
      sources.byteLength > MAXIMUM_RUNTIME_ITEM_BYTES
    ) {
      return yield* new EndpointStoreError({ reason: "invalid-input" });
    }
  }).pipe(Effect.withSpan("checkResultSize"));
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
    const item: ResultDelivery["item"] = {
      kind: "result",
      result: {
        request: yield* capturedRequest(operation),
        ...closure,
        outcomes: [first, ...rest],
      },
    };
    const request = yield* sourceRequest(operation);
    return {
      item,
      sources: { answers, ...(request === undefined ? {} : { request }) },
    };
  });
}

/** Stored questions are decoded once by their owning reader. */
function operationParticipants(operation: StoredCollectiveOperation) {
  if (operation.origin === "local") {
    const question = operation.request;
    return {
      op: question.op,
      members: question.recipients,
      question: question.question,
      requestedSchema: question.requestedSchema,
      request: question,
    };
  }
  const { message, value } = operation.request;
  return {
    op: value.op,
    members: message.recipients,
    question: questionText(message.content),
    requestedSchema: value.requestedSchema,
    request: message,
  };
}

function questionText(content: Message["content"]): string {
  return content
    .flatMap((part) => (part.type === "text" ? [part.text] : []))
    .join("\n");
}

function memberResult(
  member: AgentCard,
  answers: readonly Message[],
  requestedSchema: RequestedSchema,
  operationId: string,
) {
  return Effect.gen(function* () {
    const message = answers.find(
      (answer) => answer.author.agentId === member.agentId,
    );
    if (message === undefined) {
      return { member, outcome: { kind: "no-answer" as const } };
    }
    const value = yield* messageInterpretation(message).pipe(
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

function sourceRequest(
  operation: StoredCollectiveOperation,
): Effect.Effect<ReceivedRequest | undefined, EndpointStoreError> {
  if (operation.origin === "local") {
    return Effect.succeed(undefined);
  }
  const message = operation.request.message;
  return isReceivedRequest(message)
    ? Effect.succeed(message)
    : new EndpointStoreError({ reason: "corrupt" });
}

function retireUnusableClose(agentKey: string, saved: StoredCollectiveMessage) {
  return transaction(
    Effect.gen(function* () {
      const operation = yield* readCollectiveOperation(
        agentKey,
        saved.operationId,
      );
      if (
        operation?.origin === "remote" &&
        operation.phase === "finalizing" &&
        operation.close?.included.some(
          (reference) =>
            reference.authorAgentId === saved.reference.authorAgentId &&
            reference.msgId === saved.reference.msgId,
        )
      ) {
        yield* retireCollectiveOperation(agentKey, saved.operationId);
      }
    }),
  );
}

/** Business refusals and handler failures keep their type when SQL rolls back. */
function transaction<A, E, R>(work: Effect.Effect<A, E, R>) {
  return Effect.flatMap(SqlClient, (sql) => sql.withTransaction(work)).pipe(
    Effect.catchTag(
      "SqlError",
      () => new EndpointStoreError({ reason: "persistence" }),
    ),
  );
}

function capturedRequest(
  operation: StoredCollectiveOperation,
): Effect.Effect<Request, EndpointStoreError> {
  if (operation.origin === "local") {
    return Effect.succeed(operation.request);
  }
  const message = operation.request.message;
  const details = message.operation;
  return details?.kind === "request"
    ? Effect.succeed({
        ...details,
        kind: "request",
        author: message.author,
        recipients: message.recipients,
      })
    : new EndpointStoreError({ reason: "corrupt" });
}
