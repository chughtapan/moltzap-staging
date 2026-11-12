/**
 * @file Delivers messages between agents hosted by one daemon.
 *
 * A send orders the message in its conversation and saves it for every member
 * in one transaction. Questions and answers are messages that the request
 * worker interprets.
 */
import { SqlClient } from "@effect/sql/SqlClient";
import { Effect, Option, Schema } from "effect";
import type {
  CommunicationService,
  CommunicationSettings,
  Incoming,
  Outgoing,
} from "./contract.js";
import { executeQuery } from "../database/sql.js";
import { AgentCard } from "../identity/agent-cards.js";
import { makeRequests } from "./collectives.js";
import { CommunicationError } from "./errors.js";
import { messageHistory } from "./history.js";
import {
  compareAgentIds,
  deriveConversationId,
  type Message,
  messageInterpretation,
  RequestError,
} from "./messages.js";
import {
  acknowledgeMessage,
  communicationQueries,
  hasMessage,
  readMessages,
  saveMessage,
} from "./store.js";

interface Environment {
  readonly sql: SqlClient;
  readonly settings: CommunicationSettings;
}
const unavailable = () =>
  new CommunicationError({ reason: "persistence-failed" });
const invalid = () => new CommunicationError({ reason: "membership-invalid" });
const agentRow = Schema.Struct({
  agentKey: Schema.String,
  ...AgentCard.fields,
});

/** One agent's sends, incoming work and history on this daemon. */
export const makeDelivery = Effect.fn("Communication.delivery")(function* (
  settings: CommunicationSettings,
) {
  const environment = { sql: yield* SqlClient, settings };
  const history = messageHistory({
    agentKey: settings.agentKey,
    local: settings.card,
    maximumHistoryRecords: settings.maximumHistoryRecords ?? 1024,
  });
  const requests = yield* makeRequests(settings.agentKey, {
    self: settings.card,
    send: (message) =>
      capture(environment, sendMessage(environment, message)).pipe(
        Effect.mapError((error) =>
          error instanceof CommunicationError ? error : unavailable(),
        ),
      ),
  });
  const consumer = yield* Effect.makeSemaphore(1);
  return {
    send: (value) => capture(environment, send(environment, requests, value)),
    getRequest: (operationId) =>
      capture(environment, requests.getRequest(operationId)),
    consume: (handle) =>
      consumer.withPermits(1)(
        consume(environment, requests, handle).pipe(
          Effect.raceFirst(requests.failed),
        ),
      ),
    getConversation: (names) =>
      capture(environment, history.getConversation(names)),
    listConversations: (query = {}) =>
      capture(environment, history.listConversations(query)),
    readHistory: (query) => capture(environment, history.readHistory(query)),
  } satisfies CommunicationService;
});

/**
 * Marks the database as serving `mode`. Fails with `membership-invalid` when
 * it already serves another.
 */
export const claimMode = Effect.fn("claimMode")(
  function* (mode: string) {
    const db = yield* communicationQueries;
    yield* executeQuery(
      db
        .insertInto("communication_mode")
        .values({ singleton: 1, kind: mode })
        .onConflict((conflict) => conflict.column("singleton").doNothing()),
    );
    const [row] = yield* executeQuery(
      db
        .selectFrom("communication_mode")
        .select("kind")
        .where("singleton", "=", 1),
    );
    if (row?.kind !== mode) {
      return yield* invalid();
    }
  },
  Effect.catchTag("SqlError", unavailable),
);

type Requests = Effect.Effect.Success<ReturnType<typeof makeRequests>>;

/** A question starts a request, an answer joins one, and anything else is sent as it is. */
function send(environment: Environment, requests: Requests, value: Outgoing) {
  if ("kind" in value) {
    return requests.accept(value);
  }
  return Effect.gen(function* () {
    const decoded = yield* messageInterpretation(value).pipe(
      Effect.catchTag("CollectivePartInvalidError", () =>
        Effect.succeed(Option.none()),
      ),
    );
    return yield* Option.isSome(decoded) && decoded.value.kind === "response"
      ? requests.answer({ ...value, operation: decoded.value })
      : sendMessage(environment, value);
  });
}

/** The key of the agent on this daemon that `recipient` names, which must match its stored card. */
function recipientKey(recipient: AgentCard) {
  return Effect.gen(function* () {
    const db = yield* communicationQueries;
    const [raw] = yield* executeQuery(
      db
        .selectFrom("agent_profiles")
        .select([
          "agent_key as agentKey",
          "agent_id as agentId",
          "agent_name as agentName",
          "principal_id as principalId",
        ])
        .where("agent_id", "=", recipient.agentId),
    );
    if (raw === undefined) {
      return yield* invalid();
    }
    const { agentKey, ...stored } = yield* Schema.decodeUnknown(agentRow)(raw);
    if (!Schema.equivalence(AgentCard)(recipient, stored)) {
      return yield* invalid();
    }
    return agentKey;
  });
}

/**
 * Orders one message and saves it for every member, or changes nothing. A
 * message this agent has already sent takes no new position. The sender locks
 * its own agent and then the conversation, as every other write of an agent
 * does, so senders cannot deadlock.
 */
function sendMessage(environment: Environment, message: Message) {
  const { sql, settings } = environment;
  return sql.withTransaction(
    Effect.gen(function* () {
      if (!Schema.equivalence(AgentCard)(message.author, settings.card)) {
        return yield* invalid();
      }
      const conversationId = yield* messageConversation(settings, message);
      const recipientKeys = yield* Effect.forEach(
        message.recipients,
        recipientKey,
        { concurrency: 1 },
      );
      if (yield* hasMessage(settings.agentKey, conversationId, message)) {
        return;
      }
      const db = yield* communicationQueries;
      const [next] = yield* executeQuery(
        db
          .insertInto("message_sequences")
          .values({ conversation_id: conversationId, next_ordinal: 1 })
          .onConflict((conflict) =>
            conflict.column("conversation_id").doUpdateSet((eb) => ({
              next_ordinal: eb("message_sequences.next_ordinal", "+", 1),
            })),
          )
          .returning((eb) => eb("next_ordinal", "-", 1).as("ordinal")),
      );
      if (next === undefined) {
        return yield* unavailable();
      }
      yield* Effect.forEach(
        [settings.agentKey, ...recipientKeys],
        (agentKey) =>
          saveMessage(agentKey, conversationId, next.ordinal, message),
        { concurrency: 1, discard: true },
      );
    }),
  );
}

/**
 * The conversation a message belongs to: its author and recipients, who must
 * be distinct, include this agent and be permitted to share a conversation.
 */
function messageConversation(
  settings: CommunicationSettings,
  message: Message,
) {
  return Effect.gen(function* () {
    const members = [message.author, ...message.recipients];
    const [first, second, ...rest] = members
      .map((member) => member.agentId)
      .sort(compareAgentIds);
    if (first === undefined || second === undefined) {
      return yield* invalid();
    }
    const memberIds = [first, second, ...rest] as const;
    const distinct = new Set(memberIds).size === memberIds.length;
    const local = members.some((member) =>
      Schema.equivalence(AgentCard)(member, settings.card),
    );
    if (!distinct || !local || settings.permits?.(members) === false) {
      return yield* invalid();
    }
    return yield* deriveConversationId(memberIds);
  });
}

/**
 * Hands each pending message and question to `handle` in the order they were
 * saved, and each result as soon as it is complete, so a result precedes any
 * message saved after the answer that completed it. An item is marked consumed
 * only after `handle` succeeds.
 */
function consume<E, R>(
  environment: Environment,
  requests: Requests,
  handle: (incoming: Incoming) => Effect.Effect<void, E, R>,
) {
  const { agentKey, card } = environment.settings;
  const results = capture(
    environment,
    requests.deliverNextResult((delivery) =>
      handle({ ...delivery.item, sources: delivery.sources }),
    ),
  ).pipe(Effect.repeat({ while: (delivered) => delivered }));
  return Effect.gen(function* () {
    while (true) {
      yield* results;
      const rows = yield* capture(
        environment,
        readMessages(agentKey, { pendingFor: card.agentId, limit: 50 }),
      );
      for (const entry of rows) {
        const classified = yield* capture(
          environment,
          requests.receive(entry.message),
        );
        if (Option.isSome(classified)) {
          yield* handle(classified.value);
        }
        yield* capture(
          environment,
          acknowledgeMessage(agentKey, {
            authorAgentId: entry.message.author.agentId,
            msgId: entry.message.msgId,
          }),
        );
        yield* results;
      }
      yield* Effect.sleep("100 millis");
    }
  });
}

function capture<A, E, R>({ sql }: Environment, work: Effect.Effect<A, E, R>) {
  return work.pipe(
    Effect.provideService(SqlClient, sql),
    Effect.mapError((error) => {
      if (
        error instanceof CommunicationError ||
        error instanceof RequestError
      ) {
        return error;
      }
      if (
        typeof error === "object" &&
        error !== null &&
        "reason" in error &&
        error.reason === "conflict"
      ) {
        return new CommunicationError({ reason: "message-conflict" });
      }
      return unavailable();
    }),
  );
}
