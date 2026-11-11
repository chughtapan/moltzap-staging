/**
 * @file Delivers messages between agents hosted by one daemon.
 *
 * A send orders the message in its conversation and saves it for every member
 * in one transaction. Questions and answers are messages that the request
 * worker interprets.
 */
import { SqlClient } from "@effect/sql/SqlClient";
import { Effect, Option, Schema } from "effect";
import { randomBytes } from "node:crypto";
import type {
  CommunicationService,
  CommunicationSettings,
  Incoming,
  Outgoing,
} from "./contract.js";
import { makeRequests } from "./collectives.js";
import { withAgentTransaction } from "../database/transactions.js";
import {
  decodeCanonical,
  encodeCanonical,
  sameBytes,
} from "../encoding/index.js";
import {
  AgentCard,
  agentCardOf,
  AgentIdentityCard,
  type AgentName,
} from "../identity/agent-cards.js";
import { EnvelopeId, SignedMessage } from "../identity/message-security.js";
import { CommunicationError } from "./errors.js";
import { messageHistory } from "./history.js";
import {
  compareAgentIds,
  Message,
  messageInterpretation,
  type MessageReference,
  RequestError,
  deriveConversationId,
} from "./messages.js";
import { bindCommunicationMode } from "./mode.js";
import {
  acceptMessage,
  acknowledgeMessage,
  completeMessage,
  initializeCommunication,
  readAcceptedMessage,
  readMessages,
  type StoredMessage,
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
  canonicalCard: Schema.Uint8ArrayFromSelf,
});
const signatureRow = Schema.Struct({
  signedMessage: Schema.Uint8ArrayFromSelf,
});

/** One agent's sends, incoming work and history on this daemon. */
export function makeDelivery(settings: Environment["settings"]) {
  return Effect.gen(function* () {
    const environment = { sql: yield* SqlClient, settings };
    yield* claimMode(settings);
    yield* initializeCommunication(settings.agentKey, settings.identityCard);
    yield* registerHosted(environment);
    const history = messageHistory({
      agentKey: settings.agentKey,
      local: settings.identityCard,
      maximumHistoryRecords: settings.maximumHistoryRecords ?? 1024,
      check: checkHistory(environment),
      verify: (entries) => verifyMessages(environment, entries),
    });
    const requests = yield* makeRequests(settings.agentKey, {
      self: agentCardOf(settings.identityCard),
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
      waitForMessage: (reference) =>
        capture(environment, waitForMessage(environment, reference)),
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
const claimMode = (settings: Environment["settings"]) =>
  settings.serverAgentName === undefined
    ? bindCommunicationMode("p2p")
    : bindCommunicationMode("client-server", settings.serverAgentName);

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

function registerHosted(environment: Environment) {
  const { sql, settings } = environment;
  return Effect.gen(function* () {
    const bytes = yield* encodeCanonical(
      AgentIdentityCard,
      settings.identityCard,
    );
    yield* sql`INSERT INTO communication_agents(agent_key,agent_id,canonical_card) VALUES(${settings.agentKey},${settings.identityCard.agentId},${bytes}) ON CONFLICT(agent_key) DO NOTHING`;
    const local = yield* hosted(environment, settings.identityCard.agentId);
    if (
      local.agentKey !== settings.agentKey ||
      !sameBytes(bytes, yield* encodeCanonical(AgentIdentityCard, local.card))
    ) {
      return yield* invalid();
    }
  });
}

function hosted({ sql, settings }: Environment, agentId: string) {
  return Effect.gen(function* () {
    const [raw] =
      yield* sql`SELECT agent_key AS "agentKey",canonical_card AS "canonicalCard" FROM communication_agents WHERE agent_id=${agentId}`;
    if (raw === undefined) {
      return yield* invalid();
    }
    const row = yield* Schema.decodeUnknown(agentRow)(raw);
    const identityCard = yield* decodeCanonical(
      AgentIdentityCard,
      row.canonicalCard,
    );
    const card = yield* AgentIdentityCard.verify({
      identityCard,
      registrySignerPublicKey: settings.registrySignerPublicKey,
    });
    if (card.agentId !== agentId) {
      return yield* invalid();
    }
    return { agentKey: row.agentKey, card };
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
      if (
        !Schema.equivalence(AgentCard)(
          message.author,
          agentCardOf(settings.identityCard),
        )
      ) {
        return yield* invalid();
      }
      const conversationId = yield* messageConversation(settings, message);
      const recipientKeys = yield* Effect.forEach(
        message.recipients,
        (recipient) =>
          hosted(environment, recipient.agentId).pipe(
            Effect.filterOrFail(
              (remote) =>
                Schema.equivalence(AgentCard)(
                  recipient,
                  agentCardOf(remote.card),
                ),
              () => invalid(),
            ),
            Effect.map((remote) => remote.agentKey),
          ),
        { concurrency: 1 },
      );
      yield* claimMode(settings);
      const agentKeys = [settings.agentKey, ...recipientKeys].sort((a, b) =>
        a.localeCompare(b),
      );
      for (const agentKey of agentKeys) {
        yield* withAgentTransaction(
          agentKey,
          sql.onDialectOrElse({
            pg: () =>
              sql`SELECT agent_key FROM communication_metadata WHERE agent_key=${agentKey} FOR UPDATE`.pipe(
                Effect.asVoid,
              ),
            orElse: () => Effect.void,
          }),
        );
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
      // eslint-disable-next-line no-restricted-properties -- The daemon keeps plaintext custody and signs the Message directly; it creates no outer envelope.
      const signed = yield* SignedMessage.sign({
        identityCard: settings.identityCard,
        signingAuthority: settings.signingAuthority,
        recipientAgentIds: new Set(recipientIds(message)),
        envelopeId: yield* Schema.decodeUnknown(EnvelopeId)(
          `env_${randomBytes(16).toString("base64url")}`,
        ),
        body: bytes,
      });
      const signedBytes = yield* encodeCanonical(SignedMessage, signed);
      for (const agentKey of agentKeys) {
        yield* withAgentTransaction(
          agentKey,
          completeMessage(
            agentKey,
            conversationId,
            position.nextOrdinal - 1,
            message,
          ).pipe(
            Effect.zipRight(
              sql`INSERT INTO message_signatures(agent_key,author_agent_id,msg_id,signed_message) VALUES(${agentKey},${message.author.agentId},${message.msgId},${signedBytes})`,
            ),
          ),
        );
      }
    }),
  );
}

const recipientIds = (message: Message) =>
  message.recipients.map((recipient) => recipient.agentId);

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
      Schema.equivalence(AgentCard)(member, agentCardOf(settings.identityCard)),
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

/** Retained signatures and sequence heads make an incomplete participant view observable. */
function checkHistory({ sql, settings }: Environment) {
  return withAgentTransaction(
    settings.agentKey,
    Effect.gen(function* () {
      const [missing] = yield* sql`SELECT 1 FROM message_signatures AS evidence
      LEFT JOIN message_log AS message USING(agent_key,author_agent_id,msg_id)
      WHERE evidence.agent_key=${settings.agentKey} AND message.sequence IS NULL
      UNION ALL
      SELECT 1 FROM message_log AS message
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

function verifyMessages(
  environment: Environment,
  entries: readonly StoredMessage[],
) {
  return Effect.forEach(entries, (entry) => verifyMessage(environment, entry), {
    concurrency: 1,
    discard: true,
  }).pipe(
    Effect.catchTags({
      CommunicationError: () =>
        Effect.fail(new CommunicationError({ reason: "history-gap" })),
      ParseError: () =>
        Effect.fail(new CommunicationError({ reason: "history-gap" })),
      ClientRepresentationError: () =>
        Effect.fail(new CommunicationError({ reason: "history-gap" })),
      AgentIdentityCardVerificationError: () =>
        Effect.fail(new CommunicationError({ reason: "history-gap" })),
      SignedMessageVerificationError: () =>
        Effect.fail(new CommunicationError({ reason: "history-gap" })),
      SqlError: () => Effect.fail(unavailable()),
    }),
  );
}

function verifyMessage(environment: Environment, entry: StoredMessage) {
  const { sql, settings } = environment;
  return Effect.gen(function* () {
    if (
      entry.conversationId !==
      (yield* messageConversation(settings, entry.message))
    ) {
      return yield* invalid();
    }
    const author = yield* hosted(environment, entry.message.author.agentId);
    if (
      !Schema.equivalence(AgentCard)(
        entry.message.author,
        agentCardOf(author.card),
      )
    ) {
      return yield* invalid();
    }
    const [raw] = yield* withAgentTransaction(
      settings.agentKey,
      sql`SELECT signed_message AS "signedMessage" FROM message_signatures WHERE agent_key=${settings.agentKey} AND author_agent_id=${entry.message.author.agentId} AND msg_id=${entry.message.msgId}`,
    );
    const row = yield* Schema.decodeUnknown(signatureRow)(raw);
    const signedMessage = yield* decodeCanonical(
      SignedMessage,
      row.signedMessage,
    );
    yield* SignedMessage.verify({ signedMessage, identityCard: author.card });
    const recipients = recipientIds(entry.message).sort(compareAgentIds);
    if (
      !sameBytes(
        signedMessage.body,
        yield* encodeCanonical(Message, entry.message),
      ) ||
      signedMessage.recipientAgentIds.length !== recipients.length ||
      signedMessage.recipientAgentIds.some(
        (agentId, index) => agentId !== recipients[index],
      )
    ) {
      return yield* invalid();
    }
  });
}

function waitForMessage(
  { settings }: Environment,
  reference: MessageReference,
) {
  return Effect.gen(function* () {
    if (reference.authorAgentId !== settings.identityCard.agentId) {
      return yield* new CommunicationError({ reason: "unknown-message" });
    }
    const accepted = yield* readAcceptedMessage(settings.agentKey, reference);
    if (accepted === undefined) {
      return yield* new CommunicationError({ reason: "unknown-message" });
    }
    if (!accepted.completed) {
      return yield* unavailable();
    }
    return accepted.message;
  });
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
          pending: true,
          limit: 50,
        }).pipe(Effect.tap((entries) => verifyMessages(environment, entries))),
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
