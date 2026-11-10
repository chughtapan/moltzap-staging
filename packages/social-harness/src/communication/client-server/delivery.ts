/** @file Publishes direct client/server messages through the shared message store. */
import { SqlClient } from "@effect/sql/SqlClient";
import { Effect, Option, Schema } from "effect";
import { randomBytes } from "node:crypto";
import type {
  CommunicationService,
  CommunicationSettings,
  Incoming,
} from "../contract.js";
import { withAgentTransaction } from "../../database/transactions.js";
import {
  decodeCanonical,
  encodeCanonical,
  sameBytes,
} from "../../encoding/canonical-json.js";
import {
  AgentCard,
  agentCardOf,
  AgentIdentityCard,
  type AgentName,
} from "../../identity/agent-cards.js";
import { EnvelopeId, SignedMessage } from "../../identity/message-security.js";
import { CommunicationError } from "../errors.js";
import { messageHistory } from "../history.js";
import {
  compareAgentIds,
  Message,
  messageInterpretation,
  type MessageReference,
  RequestError,
} from "../messages.js";
import { bindCommunicationMode } from "../mode.js";
import { deriveConversationId } from "../records.js";
import {
  acceptMessage,
  acknowledgeMessage,
  completeMessage,
  initializeCommunication,
  readAcceptedMessage,
  readMessages,
  type StoredMessage,
} from "../store.js";

interface Environment {
  readonly sql: SqlClient;
  readonly settings: CommunicationSettings & {
    readonly serverAgentName: AgentName;
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

/** Each caller owns an ordinary registered identity; only the configured server connects clients. */
export function makeClientServer(settings: Environment["settings"]) {
  return Effect.gen(function* () {
    const environment = { sql: yield* SqlClient, settings };
    yield* bindCommunicationMode("client-server", settings.serverAgentName);
    yield* initializeCommunication(settings.agentKey, settings.identityCard);
    yield* registerHosted(environment);
    const history = messageHistory({
      agentKey: settings.agentKey,
      local: settings.identityCard,
      maximumHistoryRecords: settings.maximumHistoryRecords ?? 1024,
      check: checkHistory(environment),
      verify: (entries) => verifyMessages(environment, entries),
    });
    const consumer = yield* Effect.makeSemaphore(1);
    return {
      send: (value) =>
        capture(
          environment,
          "kind" in value
            ? Effect.fail(new CommunicationError({ reason: "content-invalid" }))
            : sendMessage(environment, value),
        ),
      getRequest: (operationId) =>
        Effect.fail(
          new RequestError({ operationId, failure: { kind: "request-none" } }),
        ),
      waitForMessage: (reference) =>
        capture(environment, waitForMessage(environment, reference)),
      consume: (handle) =>
        consumer.withPermits(1)(consume(environment, handle)),
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
    Effect.withSpan("Communication.clientServerLayer"),
  );
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

/** Pair locks precede acceptance so opposite-direction sends cannot hold one another's tenant row. */
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
      if (Option.isSome(yield* messageInterpretation(message))) {
        return yield* new CommunicationError({ reason: "content-invalid" });
      }
      const recipient = message.recipients[0];
      const remote = yield* hosted(environment, recipient.agentId);
      if (!Schema.equivalence(AgentCard)(recipient, agentCardOf(remote.card))) {
        return yield* invalid();
      }
      yield* bindCommunicationMode("client-server", settings.serverAgentName);
      const agentKeys = [settings.agentKey, remote.agentKey].sort((a, b) =>
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
      // eslint-disable-next-line no-restricted-properties -- Client/server trusts the daemon for plaintext custody and signs the Message directly; it creates no P2P outer envelope.
      const signed = yield* SignedMessage.sign({
        identityCard: settings.identityCard,
        signingAuthority: settings.signingAuthority,
        recipientAgentIds: new Set([recipient.agentId]),
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

/** Signed membership binds retained rows to the local tenant, topology and conversation. */
function messageConversation(
  settings: Environment["settings"],
  message: Message,
) {
  return Effect.gen(function* () {
    const recipient = message.recipients[0];
    if (
      message.recipients.length !== 1 ||
      recipient.agentId === message.author.agentId
    ) {
      return yield* invalid();
    }
    const members = [message.author, recipient];
    if (
      !members.some(
        (member) => member.agentName === settings.serverAgentName,
      ) ||
      !members.some((member) =>
        Schema.equivalence(AgentCard)(
          member,
          agentCardOf(settings.identityCard),
        ),
      )
    ) {
      return yield* invalid();
    }
    const [first, second] = members
      .map((member) => member.agentId)
      .sort(compareAgentIds);
    if (first === undefined || second === undefined) {
      return yield* invalid();
    }
    return yield* deriveConversationId([first, second]);
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
    if (
      !sameBytes(
        signedMessage.body,
        yield* encodeCanonical(Message, entry.message),
      ) ||
      signedMessage.recipientAgentIds.length !== 1 ||
      signedMessage.recipientAgentIds[0] !== entry.message.recipients[0].agentId
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

function consume<E, R>(
  environment: Environment,
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
        yield* handle({ kind: "message", message: entry.message });
        yield* capture(
          environment,
          acknowledgeMessage(environment.settings.agentKey, {
            authorAgentId: entry.message.author.agentId,
            msgId: entry.message.msgId,
          }),
        );
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
