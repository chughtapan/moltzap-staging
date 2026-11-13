/** @file Reads an agent's conversations and their ordered messages. */
import { Effect, Option, Schema } from "effect";
import { randomBytes } from "node:crypto";
import type {
  Conversation,
  ConversationQuery,
  HistoryQuery,
} from "./contract.js";
import { executeQuery, withAgentTransaction } from "../database/index.js";
import { AgentCard, type AgentName } from "../identity/index.js";
import { CommunicationError } from "./errors.js";
import {
  compareAgentIds,
  conversationNameKey,
  deriveConversationId,
  type Message,
  messageInterpretation,
  messageMembers,
  type MessageReference,
} from "./messages.js";
import {
  communicationQueries,
  readMessages,
  type StoredMessage,
} from "./store.js";

interface HistoryInput {
  readonly agentKey: string;
  readonly local: AgentCard;
  readonly maximumHistoryRecords: number;
}
interface Environment extends HistoryInput {
  readonly session: string;
}
interface Window {
  readonly conversationId: string;
  readonly after: number;
  readonly through: number;
  readonly newest: boolean;
  readonly limit: number;
}
const cursorSchema = Schema.Struct({
  session: Schema.String,
  conversation: Schema.String,
  after: Schema.Int,
  through: Schema.Int,
});
const fail = (reason: CommunicationError["reason"]) =>
  new CommunicationError({ reason });

/** Conversation lookup, listing and paged history for one agent. */
export function messageHistory(input: HistoryInput) {
  const environment = {
    ...input,
    session: randomBytes(16).toString("base64url"),
  };
  return {
    getConversation: (names: readonly AgentName[]) =>
      capture(getConversation(environment, names)).pipe(
        Effect.withSpan("getConversation"),
      ),
    listConversations: (query: ConversationQuery = {}) =>
      capture(listConversations(environment, query)).pipe(
        Effect.withSpan("listConversations"),
      ),
    readHistory: (query: HistoryQuery) =>
      capture(readHistory(environment, query)).pipe(
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
          })
        : undefined;
    if (window.newest) {
      page.reverse();
    }
    return {
      entries: page.map((row) => ({
        message: row.message,
        messageBytes: row.bytes,
      })),
      earlierMessagesOmitted: window.newest && selected.length > window.limit,
      ...(nextCursor === undefined ? {} : { nextCursor }),
    };
  });
}

function retainedConversations(environment: Environment) {
  return Effect.gen(function* () {
    const db = yield* communicationQueries;
    const rows = yield* withAgentTransaction(
      environment.agentKey,
      executeQuery(
        db
          .selectFrom("message_log")
          .select("conversation_id as conversationId")
          .distinct()
          .where("agent_key", "=", environment.agentKey),
      ),
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
        newest: true,
        limit: query.limit,
      };
    }
    const window = {
      conversationId,
      after: -1,
      through,
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

/** The position of a message this agent's history holds in the conversation. */
function position(
  environment: Environment,
  conversationId: string,
  reference: MessageReference,
) {
  return Effect.gen(function* () {
    const db = yield* communicationQueries;
    const [row] = yield* withAgentTransaction(
      environment.agentKey,
      executeQuery(
        db
          .selectFrom("message_log")
          .select("ordinal")
          .where("agent_key", "=", environment.agentKey)
          .where("conversation_id", "=", conversationId)
          .where("author_agent_id", "=", reference.authorAgentId)
          .where("msg_id", "=", reference.msgId),
      ),
    );
    return row === undefined ? yield* fail("content-invalid") : row.ordinal;
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
      cursor.conversation === window.conversationId;
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

/** The window's messages in its order, without the closes of questions, up to one past its limit. */
function scanWindow(environment: Environment, window: Window) {
  return Effect.gen(function* () {
    const selected: StoredMessage[] = [];
    let { after, through } = window;
    while (after < through) {
      const rows = yield* read(environment, {
        conversationId: window.conversationId,
        after,
        through,
        newest: window.newest,
        limit: 50,
      });
      const last = rows.at(-1);
      if (last === undefined) {
        break;
      }
      for (const row of rows) {
        if (yield* applicationMessage(row.message)) {
          selected.push(row);
        }
        if (selected.length > window.limit) {
          return selected;
        }
      }
      if (window.newest) {
        through = last.ordinal - 1;
      } else {
        after = last.ordinal;
      }
    }
    return selected;
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
  return readMessages(environment.agentKey, query);
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
    ? Effect.fail(fail("persistence-failed"))
    : Effect.succeed<Conversation>([first, second, ...rest]);
}

function capture<A, E, R>(work: Effect.Effect<A, E, R>) {
  return work.pipe(
    Effect.mapError((error) =>
      error instanceof CommunicationError ? error : fail("persistence-failed"),
    ),
  );
}
