/** @file Reads the same retained message history for every delivery protocol. */
import { SqlClient } from "@effect/sql/SqlClient";
import { Effect, Option, Schema } from "effect";
import { randomBytes } from "node:crypto";
import type {
  Conversation,
  ConversationQuery,
  HistoryQuery,
} from "./contract.js";
import { withAgentTransaction } from "../database/transactions.js";
import { encodeCanonical } from "../encoding/canonical-json.js";
import {
  AgentCard,
  type AgentName,
  type VerifiedAgentIdentityCard,
} from "../identity/agent-cards.js";
import { CommunicationError } from "./errors.js";
import {
  compareAgentIds,
  conversationNameKey,
  Message,
  messageInterpretation,
  messageMembers,
  type MessageReference,
} from "./messages.js";
import { deriveConversationId } from "./records.js";
import {
  readMessages,
  readPendingMessages,
  type StoredMessage,
} from "./store.js";

interface HistoryInput {
  readonly agentKey: string;
  readonly local: VerifiedAgentIdentityCard;
  readonly maximumHistoryRecords: number;
  readonly check: Effect.Effect<void, CommunicationError, SqlClient>;
  readonly verify: (
    rows: readonly StoredMessage[],
  ) => Effect.Effect<void, CommunicationError, SqlClient>;
}
interface Environment extends HistoryInput {
  readonly session: string;
}
interface Window {
  readonly conversationId: string;
  readonly after: number;
  readonly through: number;
  readonly reference: string;
  readonly newest: boolean;
  readonly limit: number;
}
const cursorSchema = Schema.Struct({
  session: Schema.String,
  conversation: Schema.String,
  after: Schema.Int,
  through: Schema.Int,
  reference: Schema.String,
});
const fail = (reason: CommunicationError["reason"]) =>
  new CommunicationError({ reason });

/** Verification belongs to the selected protocol; paging and visibility use the shared log. */
export function messageHistory(input: HistoryInput) {
  const environment = {
    ...input,
    session: randomBytes(16).toString("base64url"),
  };
  return {
    getConversation: (names: readonly AgentName[]) =>
      capture(environment, getConversation(environment, names)).pipe(
        Effect.withSpan("getConversation"),
      ),
    listConversations: (query: ConversationQuery = {}) =>
      capture(environment, listConversations(environment, query)).pipe(
        Effect.withSpan("listConversations"),
      ),
    readHistory: (query: HistoryQuery) =>
      capture(environment, readHistory(environment, query)).pipe(
        Effect.withSpan("readHistory"),
      ),
  };
}

function getConversation(
  environment: Environment,
  names: readonly AgentName[],
) {
  return Effect.gen(function* () {
    if (
      !names.includes(environment.local.agentName) ||
      new Set(names).size !== names.length
    ) {
      return yield* fail("membership-invalid");
    }
    const wanted = conversationNameKey(names, environment.local.agentName);
    const conversations = yield* retainedConversations(environment);
    return conversations.find(
      (members) => conversationKey(environment, members) === wanted,
    );
  });
}

function listConversations(environment: Environment, query: ConversationQuery) {
  return Effect.gen(function* () {
    const conversations = yield* retainedConversations(environment);
    const after =
      query.cursor === undefined
        ? ""
        : yield* Schema.decodeUnknown(
            Schema.parseJson(
              Schema.Struct({
                session: Schema.Literal(environment.session),
                after: Schema.String,
              }),
            ),
          )(query.cursor).pipe(
            Effect.map((value) => value.after),
            Effect.mapError(() => fail("invalid-cursor")),
          );
    const selected = conversations.filter(
      (members) => conversationKey(environment, members) > after,
    );
    const page = selected.slice(0, 50);
    const last = page.at(-1);
    return {
      conversations: page,
      ...(selected.length > 50 && last !== undefined
        ? {
            nextCursor: JSON.stringify({
              session: environment.session,
              after: conversationKey(environment, last),
            }),
          }
        : {}),
    };
  });
}

function readHistory(environment: Environment, query: HistoryQuery) {
  return Effect.gen(function* () {
    const window = yield* historyWindow(environment, query);
    const selected = yield* scanWindow(environment, window);
    const page = selected.slice(0, window.limit);
    const last = page.at(-1);
    const nextCursor =
      !window.newest && selected.length > window.limit && last !== undefined
        ? JSON.stringify({
            session: environment.session,
            conversation: window.conversationId,
            after: last.ordinal,
            through: window.through,
            reference: window.reference,
          })
        : undefined;
    if (window.newest) {
      page.reverse();
    }
    const entries = yield* Effect.forEach(
      page,
      (row) =>
        encodeCanonical(Message, row.message).pipe(
          Effect.map((bytes) => ({
            message: row.message,
            messageBytes: bytes.byteLength,
          })),
        ),
      { concurrency: 1 },
    );
    const pendingOutgoing = yield* readPendingMessages(
      environment.agentKey,
      window.conversationId,
    );
    return {
      entries,
      pendingOutgoing,
      earlierMessagesOmitted: window.newest
        ? selected.length > window.limit
        : window.reference !== "",
      ...(nextCursor === undefined ? {} : { nextCursor }),
    };
  });
}

function retainedConversations(environment: Environment) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    const rows = yield* withAgentTransaction(
      environment.agentKey,
      sql<{
        conversationId: string;
      }>`SELECT DISTINCT conversation_id AS "conversationId" FROM message_log WHERE agent_key=${environment.agentKey}`,
    );
    const conversations: Conversation[] = [];
    for (const row of rows) {
      const [entry] = yield* read(environment, {
        conversationId: row.conversationId,
        limit: 1,
      });
      if (entry !== undefined) {
        conversations.push(yield* membersOf(entry.message));
      }
    }
    return conversations.sort((a, b) =>
      compareText(
        conversationKey(environment, a),
        conversationKey(environment, b),
      ),
    );
  });
}

function historyWindow(environment: Environment, query: HistoryQuery) {
  return Effect.gen(function* () {
    const conversationId = yield* historyConversation(
      environment,
      query.members,
    );
    const [head] = yield* read(environment, {
      conversationId,
      newest: true,
      limit: 1,
    });
    const orderedMembers = [...query.members].sort((a, b) =>
      compareText(a.agentName, b.agentName),
    );
    if (
      head !== undefined &&
      !Schema.equivalence(Schema.Array(AgentCard))(
        messageMembers(head.message),
        orderedMembers,
      )
    ) {
      return yield* fail("membership-invalid");
    }
    const through = head?.ordinal ?? -1;
    if (query.order === "newest") {
      if (
        !Number.isSafeInteger(query.limit) ||
        query.limit < 1 ||
        query.limit > environment.maximumHistoryRecords
      ) {
        return yield* fail("content-invalid");
      }
      return {
        conversationId,
        after: -1,
        through:
          query.beforeMessage === undefined
            ? through
            : (yield* position(
                environment,
                conversationId,
                query.beforeMessage,
              )) - 1,
        reference: "",
        newest: true,
        limit: query.limit,
      };
    }
    const after =
      query.afterMessage === undefined
        ? -1
        : yield* position(environment, conversationId, query.afterMessage);
    const reference =
      query.afterMessage === undefined
        ? ""
        : `${query.afterMessage.authorAgentId}:${query.afterMessage.msgId}`;
    const window = {
      conversationId,
      after,
      through,
      reference,
      newest: false,
      limit: 50,
    };
    return query.cursor === undefined
      ? window
      : yield* continueWindow(environment, window, query.cursor);
  });
}

function historyConversation(environment: Environment, members: Conversation) {
  return Effect.gen(function* () {
    if (
      !members.some((member) => member.agentId === environment.local.agentId)
    ) {
      return yield* fail("membership-invalid");
    }
    const ids = members.map((member) => member.agentId).sort(compareAgentIds);
    const [first, second, ...rest] = ids;
    if (
      first === undefined ||
      second === undefined ||
      new Set(ids).size !== ids.length
    ) {
      return yield* fail("membership-invalid");
    }
    return yield* deriveConversationId([first, second, ...rest]);
  });
}

function position(
  environment: Environment,
  conversationId: string,
  reference: MessageReference,
) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    const [row] = yield* withAgentTransaction(
      environment.agentKey,
      sql<{
        ordinal: number;
      }>`SELECT ordinal FROM message_log WHERE agent_key=${environment.agentKey} AND conversation_id=${conversationId} AND author_agent_id=${reference.authorAgentId} AND msg_id=${reference.msgId}`,
    );
    if (row === undefined) {
      return yield* fail("history-gap");
    }
    yield* read(environment, {
      conversationId,
      after: row.ordinal - 1,
      through: row.ordinal,
      limit: 1,
    });
    return row.ordinal;
  });
}

function continueWindow(
  environment: Environment,
  window: Window,
  value: string,
) {
  return Effect.gen(function* () {
    const cursor = yield* Schema.decodeUnknown(Schema.parseJson(cursorSchema))(
      value,
    ).pipe(Effect.mapError(() => fail("invalid-cursor")));
    const sameRequest =
      cursor.session === environment.session &&
      cursor.conversation === window.conversationId &&
      cursor.reference === window.reference;
    const validBounds =
      cursor.after >= window.after &&
      cursor.after <= cursor.through &&
      cursor.through <= window.through;
    if (!sameRequest || !validBounds) {
      return yield* fail("invalid-cursor");
    }
    return { ...window, after: cursor.after, through: cursor.through };
  });
}

/** Every ordinal is retained even when its collective control message is hidden from application history. */
function scanWindow(environment: Environment, window: Window) {
  return Effect.gen(function* () {
    const selected: StoredMessage[] = [];
    const step = window.newest ? -1 : 1;
    let next = window.newest ? window.through : window.after + 1;
    while (next > window.after && next <= window.through) {
      const rows = yield* readWindowChunk(environment, window, next, step);
      for (const row of rows) {
        if (yield* applicationMessage(row.message)) {
          selected.push(row);
        }
        next += step;
        if (selected.length > window.limit) {
          return selected;
        }
      }
    }
    return selected;
  });
}

/** A chunk must cover every expected ordinal even if application history hides control messages. */
function readWindowChunk(
  environment: Environment,
  window: Window,
  next: number,
  step: number,
) {
  return Effect.gen(function* () {
    const rows = yield* read(environment, {
      conversationId: window.conversationId,
      after: window.newest ? window.after : next - 1,
      through: window.newest ? next : window.through,
      newest: window.newest,
      limit: 50,
    });
    if (
      rows.length === 0 ||
      rows.some((row, index) => row.ordinal !== next + step * index)
    ) {
      return yield* fail("history-gap");
    }
    return rows;
  });
}

function applicationMessage(message: Message) {
  return messageInterpretation(message).pipe(
    Effect.map(
      Option.match({
        onNone: () => true,
        onSome: (value) => value.kind !== "close",
      }),
    ),
    Effect.catchAll(() => Effect.succeed(false)),
  );
}

function read(
  environment: Environment,
  query: Parameters<typeof readMessages>[1],
) {
  return readMessages(environment.agentKey, query).pipe(
    Effect.tap(environment.verify),
  );
}

function conversationKey(environment: Environment, members: Conversation) {
  return conversationNameKey(
    members.map((member) => member.agentName),
    environment.local.agentName,
  );
}

function compareText(a: string, b: string) {
  if (a < b) {
    return -1;
  }
  return a > b ? 1 : 0;
}

function membersOf(message: Message) {
  const [first, second, ...rest] = messageMembers(message);
  return first === undefined || second === undefined
    ? Effect.fail(fail("history-gap"))
    : Effect.succeed<Conversation>([first, second, ...rest]);
}

function capture<A, E, R>(
  environment: Environment,
  work: Effect.Effect<A, E, R>,
) {
  return environment.check.pipe(
    Effect.zipRight(work),
    Effect.mapError((error) => {
      if (error instanceof CommunicationError) {
        return error;
      }
      if (
        typeof error === "object" &&
        error !== null &&
        "_tag" in error &&
        error._tag === "SqlError"
      ) {
        return fail("persistence-failed");
      }
      return fail("history-gap");
    }),
  );
}
