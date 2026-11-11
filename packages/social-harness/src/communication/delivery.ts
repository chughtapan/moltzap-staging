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
import { withAgentTransaction } from "../database/transactions.js";
import { encodeCanonical } from "../encoding/index.js";
import { AgentCard, type AgentName } from "../identity/agent-cards.js";
import { makeRequests } from "./collectives.js";
import { CommunicationError } from "./errors.js";
import { messageHistory } from "./history.js";
import {
  compareAgentIds,
  deriveConversationId,
  Message,
  messageInterpretation,
  RequestError,
} from "./messages.js";
import { bindCommunicationMode } from "./mode.js";
import {
  acceptMessage,
  acknowledgeMessage,
  completeMessage,
  readAcceptedMessage,
  readMessages,
} from "./store.js";

interface Environment {
  readonly sql: SqlClient;
  /** With a server agent named, a conversation is that agent and one other. */
  readonly settings: CommunicationSettings & {
    readonly serverAgentName?: AgentName;
  };
}
const unavailable = () =>
  new CommunicationError({ reason: "persistence-failed" });
const invalid = () => new CommunicationError({ reason: "membership-invalid" });
const agentRow = Schema.Struct({
  agentKey: Schema.String,
  ...AgentCard.fields,
});

/** One agent's sends, incoming work and history on this daemon. */
export function makeDelivery(settings: Environment["settings"]) {
  return Effect.gen(function* () {
    const environment = { sql: yield* SqlClient, settings };
    yield* claimMode(settings);
    const history = messageHistory({
      agentKey: settings.agentKey,
      local: settings.card,
      maximumHistoryRecords: settings.maximumHistoryRecords ?? 1024,
      check: checkHistory(environment),
    });
    const requests = yield* makeRequests(settings.agentKey, {
      self: settings.card,
      acceptMessage: (message) =>
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
  }).pipe(
    Effect.mapError((error) =>
      error instanceof CommunicationError ? error : unavailable(),
    ),
    Effect.withSpan("Communication.delivery"),
  );
}

type Requests = Effect.Effect.Success<ReturnType<typeof makeRequests>>;

/** A database serves one mode, and in client/server mode one server agent. */
function claimMode(settings: Environment["settings"]) {
  return settings.serverAgentName === undefined
    ? bindCommunicationMode("p2p")
    : bindCommunicationMode("client-server", settings.serverAgentName);
}

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
function recipientKey({ sql }: Environment, recipient: AgentCard) {
  return Effect.gen(function* () {
    const [raw] =
      yield* sql`SELECT agent_key AS "agentKey",agent_id AS "agentId",agent_name AS "agentName",principal_id AS "principalId" FROM agent_profiles WHERE agent_id=${recipient.agentId}`;
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
 * Orders one message and saves it for every member, or changes nothing.
 * Members' rows are locked in one fixed order, so sends in opposite directions
 * cannot hold each other's rows.
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
        (recipient) => recipientKey(environment, recipient),
        { concurrency: 1 },
      );
      yield* claimMode(settings);
      const agentKeys = [settings.agentKey, ...recipientKeys].sort((a, b) =>
        a.localeCompare(b),
      );
      for (const agentKey of agentKeys) {
        yield* sql.onDialectOrElse({
          pg: () =>
            sql`SELECT agent_key FROM agent_profiles WHERE agent_key=${agentKey} FOR UPDATE`.pipe(
              Effect.asVoid,
            ),
          orElse: () => Effect.void,
        });
      }
      const bytes = yield* encodeCanonical(Message, message);
      yield* acceptMessage(settings.agentKey, {
        authorAgentId: message.author.agentId,
        msgId: message.msgId,
        conversationId,
        canonicalMessage: bytes,
      });
      const accepted = yield* readAcceptedMessage(settings.agentKey, {
        authorAgentId: message.author.agentId,
        msgId: message.msgId,
      });
      if (accepted?.completed) {
        return;
      }
      yield* sql`INSERT INTO message_sequences(conversation_id,next_ordinal) VALUES(${conversationId},0) ON CONFLICT(conversation_id) DO NOTHING`;
      const [position] = yield* sql<{
        nextOrdinal: number;
      }>`UPDATE message_sequences SET next_ordinal=next_ordinal+1 WHERE conversation_id=${conversationId} RETURNING next_ordinal AS "nextOrdinal"`;
      if (position === undefined) {
        return yield* unavailable();
      }
      for (const agentKey of agentKeys) {
        yield* withAgentTransaction(
          agentKey,
          completeMessage(
            agentKey,
            conversationId,
            position.nextOrdinal - 1,
            message,
          ),
        );
      }
    }),
  );
}

/**
 * The conversation a message belongs to: its author and recipients, who must
 * be distinct and include this agent. With a server agent named, they must be
 * that agent and one other.
 */
function messageConversation(
  settings: Environment["settings"],
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
    const { serverAgentName } = settings;
    const allowed =
      serverAgentName === undefined ||
      (members.length === 2 &&
        members.some((member) => member.agentName === serverAgentName));
    if (!distinct || !local || !allowed) {
      return yield* invalid();
    }
    return yield* deriveConversationId(memberIds);
  });
}

/** Fails unless every conversation this agent holds has each position up to the conversation's latest. */
function checkHistory({ sql, settings }: Environment) {
  return withAgentTransaction(
    settings.agentKey,
    Effect.gen(function* () {
      const [missing] = yield* sql`SELECT 1 FROM message_log AS message
      LEFT JOIN message_sequences AS head USING(conversation_id)
      WHERE message.agent_key=${settings.agentKey}
      GROUP BY message.conversation_id,head.next_ordinal
      HAVING head.next_ordinal IS NULL OR COUNT(*)<>head.next_ordinal OR MIN(message.ordinal)<>0 OR MAX(message.ordinal)+1<>head.next_ordinal
      LIMIT 1`;
      if (missing !== undefined) {
        return yield* new CommunicationError({ reason: "history-gap" });
      }
    }),
  ).pipe(Effect.catchTag("SqlError", () => Effect.fail(unavailable())));
}

/**
 * Hands each pending message, question and result to `handle` in order, then
 * waits for more. A message is marked consumed only after `handle` succeeds.
 */
function consume<E, R>(
  environment: Environment,
  requests: Requests,
  handle: (incoming: Incoming) => Effect.Effect<void, E, R>,
) {
  return Effect.gen(function* () {
    while (true) {
      yield* capture(environment, checkHistory(environment));
      const rows = yield* capture(
        environment,
        readMessages(environment.settings.agentKey, {
          pendingFor: environment.settings.card.agentId,
          limit: 50,
        }),
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
          acknowledgeMessage(environment.settings.agentKey, {
            authorAgentId: entry.message.author.agentId,
            msgId: entry.message.msgId,
          }),
        );
      }
      while (
        yield* capture(
          environment,
          requests.deliverNextResult((delivery) =>
            delivery.item.kind === "result"
              ? handle({ ...delivery.item, sources: delivery.sources })
              : handle(delivery.item),
          ),
        )
      ) {
        continue;
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
