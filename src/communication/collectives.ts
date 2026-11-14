/** @file One agent's collectives: the collective requests it sends and receives, their responses and the results they complete. */

import { SqlClient } from "@effect/sql/SqlClient";
import {
  Clock,
  Duration,
  Effect,
  Option,
  Predicate,
  Schedule,
  Schema,
} from "effect";
import { afterCommit, transaction } from "../database/index.js";
import { encodeJson, MAXIMUM_RUNTIME_ITEM_BYTES } from "../encoding.js";
import { AgentCard } from "../identity.js";
import { CommunicationError } from "./errors.js";
import {
  type CollectiveClose,
  type collectiveClosure,
  type CollectivePart,
  collectivePartOf,
  type CollectiveRequest,
  collectiveRequestContent,
  type CollectiveRequestMessage,
  type CollectiveResponse,
  type CollectiveResult,
  CollectiveResultSources,
  encodeCollectiveContent,
  isCollectiveRequestMessage,
  Message,
  messageMembers,
  type MessageReference,
  messageWithId,
  type MsgId,
  outcomeOfResponse,
  RequestError,
  validateResponseContent,
} from "./messages.js";
import {
  discardCollective,
  EndpointStoreError,
  markCollectiveResultConsumed,
  readCollectingRequestIds,
  readCollective,
  readCollectiveOfCopy,
  readCollectiveResponses,
  readNextCollectiveResult,
  recordCollectiveCopy,
  recordCollectiveResponse,
  saveCollective,
  saveCollectiveResult,
  type StoredCollective,
} from "./store.js";

/** What the host should see of a message this agent received. */
type ReceivedMessage =
  | { readonly kind: "message"; readonly message: Message }
  | {
      readonly kind: "collectiveRequest";
      readonly message: CollectiveRequestMessage;
    };

/** A collective result with the messages its peer-controlled text came from. */
interface CompletedCollective {
  readonly result: CollectiveResult;
  readonly sources: CollectiveResultSources;
}

/**
 * One agent's collectives. `send` orders and saves a message inside the
 * caller's transaction.
 */
export const makeCollectives = Effect.fn("Communication.collectives")(
  function* (
    agentKey: string,
    input: {
      readonly card: AgentCard;
      readonly send: (
        message: Message,
      ) => Effect.Effect<void, CommunicationError>;
      /** Runs once a result is saved for this agent to consume. */
      readonly completed: Effect.Effect<void>;
    },
  ) {
    const agent: CollectiveAgent = {
      agentKey,
      ...input,
      sql: yield* SqlClient,
      requested: yield* Effect.makeLatch(),
    };
    yield* completeDueRequests(agent).pipe(
      Effect.provideService(SqlClient, agent.sql),
      restarting(agentKey, "deadline worker"),
      Effect.interruptible,
      Effect.forkScoped,
    );
    const storeFailureAsUnavailable = <A, E, R>(
      effect: Effect.Effect<A, E | EndpointStoreError, R>,
    ) =>
      effect.pipe(
        Effect.catchTag(
          "EndpointStoreError",
          () => new CommunicationError({ reason: "persistence-failed" }),
        ),
      );
    return {
      sendRequest: (request: CollectiveRequest) =>
        storeFailureAsUnavailable(sendRequest(agent, request)),
      sendResponse: (message: Message, response: CollectiveResponse) =>
        storeFailureAsUnavailable(sendResponse(agent, message, response)),
      getRequest: (requestId: MsgId) =>
        storeFailureAsUnavailable(getRequest(agent, requestId)),
      receive: (message: Message) =>
        storeFailureAsUnavailable(receive(agent, message)),
      deliverNextResult: <E, R>(
        handle: (completed: CompletedCollective) => Effect.Effect<void, E, R>,
      ) => deliverNextResult(agentKey, handle),
    };
  },
);

/**
 * Keeps a background loop of one agent running. A failure is logged and the
 * loop starts again after a pause that doubles from one second up to thirty.
 */
export function restarting(agentKey: string, loop: string) {
  return <E, R>(work: Effect.Effect<never, E, R>) =>
    work.pipe(
      Effect.tapError((error) =>
        Effect.logError(`The ${loop} failed and will start again`).pipe(
          Effect.annotateLogs({
            agentKey,
            failure: Predicate.hasProperty(error, "_tag")
              ? String(error._tag)
              : "unknown",
          }),
        ),
      ),
      Effect.retry(
        Schedule.union(
          Schedule.exponential("1 second"),
          Schedule.spaced("30 seconds"),
        ),
      ),
    );
}

/** The agent whose collectives these are, and what handling them uses for the life of its service. */
interface CollectiveAgent {
  readonly agentKey: string;
  readonly sql: SqlClient;
  readonly card: AgentCard;
  readonly send: (message: Message) => Effect.Effect<void, CommunicationError>;
  readonly completed: Effect.Effect<void>;
  /** Opened when this agent sends a collective request, which gives the worker a deadline to wait for. */
  readonly requested: Effect.Latch;
}

type Closure = typeof collectiveClosure.Type;

/**
 * Saves the collective and sends its request in one transaction: one group
 * message for an all_gather, one private copy per recipient for a gather. The
 * first copy takes the request's own message id, and every copy's id is
 * recorded against the collective. Copies go out in recipient-name order, so
 * two requests lock the conversations they share in the same order.
 */
function sendRequest(agent: CollectiveAgent, request: CollectiveRequest) {
  const { agentKey } = agent;

  return Effect.gen(function* () {
    if (
      !Schema.equivalence(AgentCard)(request.author, agent.card) ||
      (request.op === "all_gather" && request.recipients.length < 2)
    ) {
      return yield* new CommunicationError({ reason: "membership-invalid" });
    }
    yield* storeTransaction(
      Effect.gen(function* () {
        const existing = yield* readCollective(agentKey, request.msgId);
        if (
          existing === undefined &&
          request.deadlineAt <= (yield* Clock.currentTimeMillis)
        ) {
          return yield* new RequestError({
            requestId: request.msgId,
            failure: { kind: "deadline-passed" },
          });
        }
        yield* saveCollective(agentKey, { role: "requester", request });
        if (existing !== undefined) {
          return;
        }
        yield* afterCommit(agent.requested.open);
        const content = yield* collectiveRequestContent(request).pipe(
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
        for (const [index, recipients] of audiences.entries()) {
          const copy = yield* index === 0
            ? messageWithId(
                request.author,
                recipients,
                request.msgId,
                content,
              ).pipe(
                Effect.catchTag(
                  "ParseError",
                  () => new CommunicationError({ reason: "content-invalid" }),
                ),
              )
            : Message.create({ author: request.author, recipients, content });
          yield* recordCollectiveCopy(agentKey, request.msgId, copy.msgId);
          yield* agent.send(copy);
        }
      }),
    );
  }).pipe(Effect.withSpan("Collectives.sendRequest"));
}

/** The collective request this agent may still respond to. Looking it up reserves nothing. */
function getRequest(agent: CollectiveAgent, requestId: MsgId) {
  return Effect.gen(function* () {
    const stored = yield* readCollective(agent.agentKey, requestId);
    if (stored === undefined || stored.role !== "recipient") {
      return yield* refused(requestId, "request-none");
    }
    if ((yield* ownResponse(agent, requestId)) !== undefined) {
      return yield* refused(requestId, "request-answered");
    }
    if (stored.phase !== "collecting") {
      return yield* refused(requestId, "request-none");
    }
    if ((yield* Clock.currentTimeMillis) >= stored.deadlineAt) {
      return yield* refused(requestId, "request-expired");
    }
    return stored.request;
  }).pipe(Effect.withSpan("Collectives.getRequest"));
}

function refused(
  requestId: MsgId,
  kind: "request-none" | "request-answered" | "request-expired",
) {
  return new RequestError({ requestId, failure: { kind } });
}

/** The response this agent has sent to a collective request it received. */
function ownResponse(agent: CollectiveAgent, requestId: MsgId) {
  return readCollectiveResponses(agent.agentKey, requestId).pipe(
    Effect.map((responses) =>
      responses.find(
        (response) => response.author.agentId === agent.card.agentId,
      ),
    ),
  );
}

/**
 * Sends this agent's response and saves it as sent, in one transaction.
 * Repeating the same response changes nothing; a different one is refused.
 */
function sendResponse(
  agent: CollectiveAgent,
  message: Message,
  response: CollectiveResponse,
) {
  const { agentKey } = agent;
  const { requestId } = response;

  return Effect.gen(function* () {
    if (!Schema.equivalence(AgentCard)(message.author, agent.card)) {
      return yield* new CommunicationError({ reason: "content-invalid" });
    }
    yield* storeTransaction(
      Effect.gen(function* () {
        const stored = yield* readCollective(agentKey, requestId);
        if (stored === undefined || stored.role !== "recipient") {
          return yield* refused(requestId, "request-none");
        }
        if (!sameMembers(stored.request, message)) {
          return yield* new CommunicationError({
            reason: "membership-invalid",
          });
        }
        if (response.action === "accept") {
          const fields = yield* validateResponseContent(
            stored.request.collectiveRequest.requestedSchema,
            response.content,
          ).pipe(Effect.flip, Effect.option);
          if (Option.isSome(fields)) {
            return yield* new RequestError({
              requestId,
              failure: { kind: "answer-invalid", fields: fields.value },
            });
          }
        }
        const prior = yield* ownResponse(agent, requestId);
        if (prior !== undefined) {
          return prior.msgId === message.msgId
            ? yield* agent.send(message)
            : yield* refused(requestId, "request-answered");
        }
        if (stored.phase !== "collecting") {
          return yield* refused(requestId, "request-none");
        }
        const now = yield* Clock.currentTimeMillis;
        if (now >= stored.deadlineAt) {
          return yield* refused(requestId, "request-expired");
        }
        yield* agent.send(message);
        yield* recordCollectiveResponse(agentKey, requestId, message, now);
      }),
    );
  }).pipe(Effect.withSpan("Collectives.sendResponse"));
}

/**
 * What the host should see of a message this agent received. A collective
 * request yields itself as a request, a message with no collective part
 * yields itself, and a response, a close or an unreadable collective part
 * yields nothing.
 */
function receive(agent: CollectiveAgent, message: Message) {
  return collectivePartOf(message).pipe(
    Effect.matchEffect({
      onFailure: () => Effect.succeed(Option.none<ReceivedMessage>()),
      onSuccess: (decoded) =>
        Option.match(decoded, {
          onNone: () =>
            Effect.succeed(
              Option.some<ReceivedMessage>({ kind: "message", message }),
            ),
          onSome: (part) =>
            storeTransaction(receiveCollectivePart(agent, message, part)),
        }),
    }),
    Effect.withSpan("receive"),
  );
}

function receiveCollectivePart(
  agent: CollectiveAgent,
  message: Message,
  part: CollectivePart,
): Effect.Effect<
  Option.Option<ReceivedMessage>,
  EndpointStoreError | RequestError | CommunicationError,
  SqlClient
> {
  switch (part.kind) {
    case "request":
      return receiveRequest(agent, message);
    case "response":
      return receiveResponse(agent, message, part).pipe(
        Effect.as(Option.none()),
      );
    case "close":
      return receiveClose(agent, message, part).pipe(Effect.as(Option.none()));
    default: {
      const exhaustive: never = part;
      return exhaustive;
    }
  }
}

/**
 * Saves a collective request this agent received under the request's message
 * id and yields it for the host. A request that is not addressed as its `op`
 * requires, or whose deadline has passed, yields nothing.
 */
function receiveRequest(agent: CollectiveAgent, message: Message) {
  const { agentKey } = agent;

  return Effect.gen(function* () {
    if (
      !isCollectiveRequestMessage(message) ||
      !requestEligible(
        message,
        agent.card.agentId,
        yield* Clock.currentTimeMillis,
      )
    ) {
      return Option.none<ReceivedMessage>();
    }
    const received = Option.some<ReceivedMessage>({
      kind: "collectiveRequest",
      message,
    });
    const existing = yield* readCollective(agentKey, message.msgId);
    if (existing !== undefined) {
      return existing.role === "recipient" &&
        Schema.equivalence(Message)(existing.request, message)
        ? received
        : Option.none<ReceivedMessage>();
    }
    yield* saveCollective(agentKey, { role: "recipient", request: message });
    yield* recordCollectiveCopy(agentKey, message.msgId, message.msgId);
    return received;
  });
}

/**
 * Saves a recipient's first response to the collective whose request, or
 * whose private copy of it, the response names.
 */
function receiveResponse(
  agent: CollectiveAgent,
  message: Message,
  response: CollectiveResponse,
) {
  const { agentKey } = agent;

  return Effect.gen(function* () {
    const stored = yield* readCollectiveOfCopy(agentKey, response.requestId);
    if (stored === undefined || stored.phase === "completed") {
      return;
    }
    const request = requestOf(stored);
    if (
      !request.recipients.some(
        (recipient) => recipient.agentId === message.author.agentId,
      )
    ) {
      return;
    }
    if (
      request.op === "all_gather"
        ? !sameMembers(request, message)
        : message.recipients.length !== 1 ||
          message.recipients[0]?.agentId !== request.author.agentId
    ) {
      return;
    }
    const recorded = yield* recordCollectiveResponse(
      agentKey,
      stored.requestId,
      message,
      yield* Clock.currentTimeMillis,
    );
    if (recorded && stored.role === "requester") {
      yield* completeOwnRequest(agent, stored.requestId);
    }
  });
}

/**
 * Completes an all_gather this agent received with the result its
 * requester's close fixes. A close that names a response this agent does not
 * hold ends the collective without a result.
 */
function receiveClose(
  agent: CollectiveAgent,
  message: Message,
  close: CollectiveClose,
) {
  const { agentKey } = agent;

  return Effect.gen(function* () {
    const stored = yield* readCollective(agentKey, close.requestId);
    if (
      stored === undefined ||
      stored.role !== "recipient" ||
      stored.phase !== "collecting" ||
      stored.request.author.agentId !== message.author.agentId
    ) {
      return;
    }
    if (
      stored.request.collectiveRequest.op !== "all_gather" ||
      !sameMembers(stored.request, message)
    ) {
      return;
    }
    const responses = includedResponses(
      yield* readCollectiveResponses(agentKey, close.requestId),
      close.included,
    );
    if (
      responses.length !== close.included.length ||
      new Set(close.included.map((reference) => reference.authorAgentId))
        .size !== close.included.length
    ) {
      return yield* discardCollective(agentKey, close.requestId);
    }
    const completed = yield* makeResult(stored, responses, {
      closedAt: close.closedAt,
      closedBy: close.closedBy,
    });
    yield* checkResultSize(completed);
    yield* saveCollectiveResult(agentKey, close.requestId, {
      result: completed.result,
      responses: close.included,
    });
  });
}

/** Names, agent ids and principals must all agree before a response or a close is accepted. */
function sameMembers(
  left: Pick<Message, "author" | "recipients">,
  right: Pick<Message, "author" | "recipients">,
) {
  return Schema.equivalence(Schema.Array(AgentCard))(
    messageMembers(left),
    messageMembers(right),
  );
}

function requestEligible(
  message: CollectiveRequestMessage,
  localAgentId: AgentCard["agentId"],
  now: number,
) {
  const { op, deadlineAt } = message.collectiveRequest;
  const addressedToLocal = message.recipients.some(
    (recipient) => recipient.agentId === localAgentId,
  );
  const audience =
    op === "gather"
      ? message.recipients.length === 1
      : message.recipients.length >= 2;
  return addressedToLocal && audience && deadlineAt > now;
}

function deliverNextResult<E, R>(
  agentKey: string,
  handle: (completed: CompletedCollective) => Effect.Effect<void, E, R>,
) {
  return Effect.gen(function* () {
    const stored = yield* readNextCollectiveResult(agentKey);
    if (stored === undefined) {
      return false;
    }
    if (stored.completed === undefined) {
      return yield* new EndpointStoreError({ reason: "corrupt" });
    }
    const responses = includedResponses(
      yield* readCollectiveResponses(agentKey, stored.requestId),
      stored.completed.responses,
    );
    if (responses.length !== stored.completed.responses.length) {
      return yield* new EndpointStoreError({ reason: "corrupt" });
    }
    const completed: CompletedCollective = {
      result: stored.completed.result,
      sources: sourcesOf(stored, responses),
    };
    yield* checkResultSize(completed);
    yield* storeTransaction(
      handle(completed).pipe(
        Effect.zipRight(
          markCollectiveResultConsumed(agentKey, stored.requestId),
        ),
      ),
    );
    return true;
  });
}

/**
 * Completes each collective request this agent sent once its deadline has
 * passed, then waits for the earliest deadline still open or for a new
 * request.
 */
function completeDueRequests(agent: CollectiveAgent) {
  return Effect.gen(function* () {
    yield* agent.requested.close;
    const deadlines: number[] = [];
    for (const requestId of yield* readCollectingRequestIds(agent.agentKey)) {
      const deadline = yield* Effect.uninterruptible(
        completeOwnRequest(agent, requestId),
      );
      if (deadline !== undefined) {
        deadlines.push(deadline);
      }
    }
    const [first, ...rest] = deadlines;
    if (first === undefined) {
      return yield* agent.requested.await;
    }
    const remaining =
      Math.min(first, ...rest) - (yield* Clock.currentTimeMillis);
    yield* Effect.raceFirst(
      agent.requested.await,
      Effect.sleep(Duration.millis(Math.max(0, remaining))),
    );
  }).pipe(Effect.forever);
}

/** The saved responses a result counts, in the order the result lists them. */
function includedResponses(
  responses: readonly Message[],
  included: readonly MessageReference[],
) {
  return included.flatMap((reference) => {
    const response = responses.find(
      (message) =>
        message.author.agentId === reference.authorAgentId &&
        message.msgId === reference.msgId,
    );
    return response === undefined ? [] : [response];
  });
}

/**
 * Completes a collective request this agent sent once every recipient has
 * responded or the deadline has passed. An all_gather also sends the close
 * that tells the group which responses the result counts. Returns the
 * deadline of a request it leaves open.
 */
function completeOwnRequest(agent: CollectiveAgent, requestId: MsgId) {
  const { agentKey } = agent;

  return storeTransaction(
    Effect.gen(function* () {
      const stored = yield* readCollective(agentKey, requestId);
      if (
        stored === undefined ||
        stored.role !== "requester" ||
        stored.phase !== "collecting"
      ) {
        return undefined;
      }
      const { request } = stored;
      const responses = yield* readCollectiveResponses(agentKey, requestId);
      const now = yield* Clock.currentTimeMillis;
      if (
        responses.length < request.recipients.length &&
        now < stored.deadlineAt
      ) {
        return stored.deadlineAt;
      }
      const closure: Closure = {
        closedAt: now,
        closedBy:
          responses.length === request.recipients.length
            ? "settled"
            : "deadline",
      };
      const completed = yield* makeResult(stored, responses, closure);
      yield* checkResultSize(completed);
      const included = responses.map((response) => ({
        authorAgentId: response.author.agentId,
        msgId: response.msgId,
      }));
      yield* saveCollectiveResult(agentKey, requestId, {
        result: completed.result,
        responses: included,
      });
      yield* afterCommit(agent.completed);
      if (request.op === "gather") {
        return undefined;
      }
      const content = yield* encodeCollectiveContent({
        kind: "close",
        requestId,
        included,
        ...closure,
      }).pipe(
        Effect.catchTag(
          "CollectiveContentError",
          () => new EndpointStoreError({ reason: "corrupt" }),
        ),
      );
      const close = yield* Message.create({
        author: request.author,
        recipients: request.recipients,
        content,
      });
      yield* agent.send(close);
      return undefined;
    }),
  ).pipe(Effect.withSpan("completeOwnRequest"));
}

/** The result and its sources have independent size budgets. */
function checkResultSize(completed: CompletedCollective) {
  return Effect.gen(function* () {
    const sources = yield* encodeJson(
      CollectiveResultSources,
      completed.sources,
    ).pipe(
      Effect.catchTag(
        "ClientRepresentationError",
        () => new EndpointStoreError({ reason: "invalid-input" }),
      ),
    );
    if (
      Buffer.byteLength(JSON.stringify(completed.result)) >
        MAXIMUM_RUNTIME_ITEM_BYTES ||
      Buffer.byteLength(sources) > MAXIMUM_RUNTIME_ITEM_BYTES
    ) {
      return yield* new EndpointStoreError({ reason: "invalid-input" });
    }
  }).pipe(Effect.withSpan("checkResultSize"));
}

/** The result of a collective: one outcome for each recipient of its request, read from the counted responses. */
function makeResult(
  stored: StoredCollective,
  responses: readonly Message[],
  closure: Closure,
) {
  return Effect.gen(function* () {
    const request = requestOf(stored);
    const outcomes = yield* Effect.forEach(
      request.recipients,
      (recipient) => outcomeOf(recipient, responses, request.requestedSchema),
      { concurrency: 1 },
    );
    const [first, ...rest] = outcomes;
    if (first === undefined) {
      return yield* new EndpointStoreError({ reason: "corrupt" });
    }
    const completed: CompletedCollective = {
      result: { request, ...closure, outcomes: [first, ...rest] },
      sources: sourcesOf(stored, responses),
    };
    return completed;
  });
}

/**
 * The same view of a collective request this agent sent or received: the
 * request as its requester states it, with the recipients this agent knows.
 */
function requestOf(stored: StoredCollective): CollectiveRequest {
  if (stored.role === "requester") {
    return stored.request;
  }
  const { msgId, author, recipients, collectiveRequest } = stored.request;
  return {
    kind: "collectiveRequest",
    msgId,
    author,
    recipients,
    ...collectiveRequest,
  };
}

function outcomeOf(
  recipient: AgentCard,
  responses: readonly Message[],
  requestedSchema: CollectiveRequest["requestedSchema"],
) {
  return Effect.gen(function* () {
    const message = responses.find(
      (response) => response.author.agentId === recipient.agentId,
    );
    if (message === undefined) {
      return { recipient: recipient, outcome: { kind: "no-answer" as const } };
    }
    if (message.collectiveResponse === undefined) {
      return yield* new EndpointStoreError({ reason: "corrupt" });
    }
    return {
      recipient: recipient,
      outcome: yield* outcomeOfResponse(
        requestedSchema,
        message.collectiveResponse,
      ),
    };
  });
}

/** The messages a result's peer-controlled text came from. A request this agent sent is not among them. */
function sourcesOf(
  stored: StoredCollective,
  responses: readonly Message[],
): CollectiveResultSources {
  return {
    responses,
    ...(stored.role === "requester" ? {} : { request: stored.request }),
  };
}

/** Business refusals and handler failures keep their type when SQL rolls back. */
function storeTransaction<A, E, R>(work: Effect.Effect<A, E, R>) {
  return transaction(work).pipe(
    Effect.catchTag(
      "SqlError",
      () => new EndpointStoreError({ reason: "persistence" }),
    ),
  );
}
