/**
 * @file The messages of a daemon that hosts every agent, as each agent holds
 * them.
 *
 * A send orders a message in its conversation and saves it for its author and
 * its recipient in one transaction. The reads give an agent its inbox, its
 * conversations and their ordered history. An agent is named by the key its
 * rows are stored under, and every function runs on its caller's SqlClient.
 */

import { SqlClient } from "@effect/sql/SqlClient";
import { Effect } from "effect";
import { createHash } from "node:crypto";
import type { AgentCard } from "../identity.js";
import {
  executeQuery,
  type Generated,
  makeQueries,
  transaction,
  withAgentRows,
} from "../database.js";
import { compareAscii, decodeJson, encodeJson } from "../encoding.js";
import {
  Message,
  messageMembers,
  PAGE_SIZE,
  type PaginatedRequest,
} from "./messages.js";

/**
 * How the recipient of a message holds it: `readable` in its inbox, with the
 * guidance its firewall gave as text, or withheld from it.
 */
export interface Received {
  readonly readable: boolean;
  readonly guidance?: string;
}

/**
 * A message as the reading agent holds it, with the size of its stored JSON
 * text. A message the agent wrote is not readable.
 */
export interface HeldMessage extends Received {
  readonly message: Message;
  readonly messageBytes: number;
}

type CommunicationTables = Record<
  "message_log",
  Record<
    "agent_key" | "conversation_id" | "author_agent_id" | "msg_id" | "message",
    string
  > &
    Record<"ordinal", number> &
    Record<"sequence", Generated<number>> &
    Record<"read_number", number | null> &
    Record<"guidance", string | null>
> &
  Record<
    "message_sequences",
    Record<"conversation_id", string> & Record<"next_ordinal", number>
  >;

/** Builds queries over the tables that hold messages. */
const communicationQueries = makeQueries<CommunicationTables>();

/** The identifier of the conversation among `members`, given in any order. */
function conversationOf(members: readonly AgentCard[]) {
  const ids = members.map((member) => member.agentId).sort(compareAscii);
  return `cnv_${createHash("sha256").update(JSON.stringify(ids)).digest("base64url")}`;
}

/** The newest read number a message of the agent holds, or zero when it may read none. */
const newestReadNumber = Effect.fn("newestReadNumber")(function* (
  agentKey: string,
) {
  const db = yield* communicationQueries;
  return db
    .selectFrom("message_log")
    .where("agent_key", "=", agentKey)
    .select((eb) =>
      eb.fn.coalesce(eb.fn.max("read_number"), eb.val(0)).as("newest"),
    );
});

/**
 * Adds a message to one agent's history at its position. A message its agent
 * may read takes the agent's next read number. The advisory lock on the agent
 * is the last lock its transaction takes and is held until it ends, so
 * messages become readable in read-number order: a reader that has read to
 * one number has seen every message before it.
 */
const saveMessage = Effect.fn("saveMessage")(
  function* (
    agentKey: string,
    position: { readonly conversationId: string; readonly ordinal: number },
    message: Message,
    received: Received,
  ) {
    const db = yield* communicationQueries;
    const newest = yield* newestReadNumber(agentKey);
    if (received.readable) {
      const sql = yield* SqlClient;
      yield* sql`SELECT pg_advisory_xact_lock(hashtext(${agentKey}))`;
    }
    const encoded = yield* encodeJson(Message, message);
    yield* executeQuery(
      db.insertInto("message_log").values((eb) => ({
        agent_key: agentKey,
        conversation_id: position.conversationId,
        ordinal: position.ordinal,
        author_agent_id: message.author.agentId,
        msg_id: message.msgId,
        message: encoded,
        read_number: received.readable ? eb(newest, "+", 1) : null,
        guidance: received.guidance ?? null,
      })),
    );
  },
  (work, agentKey) => withAgentRows(agentKey, work),
);

/**
 * Orders one message and saves it for its author and its recipient, held by
 * the recipient as `received` says, or changes nothing. A message is sent
 * once. Every write of a conversation first takes the row of the
 * conversation's next position, so two sends cannot deadlock.
 */
export const sendMessage = Effect.fn("sendMessage")(function* (
  route: { readonly authorKey: string; readonly recipientKey: string },
  message: Message,
  received: Received,
) {
  const conversationId = conversationOf(messageMembers(message));
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
  const position = { conversationId, ordinal: next?.ordinal ?? 0 };
  yield* saveMessage(route.authorKey, position, message, { readable: false });
  yield* saveMessage(route.recipientKey, position, message, received);
}, transaction);

/**
 * A range of one agent's messages: those of a conversation in its order, or
 * with `afterReadNumber` those the agent may read after it in read order.
 * Each comes with its position in its conversation and its read number.
 */
const readMessages = Effect.fn("readMessages")(
  function* (
    agentKey: string,
    range: {
      readonly conversationId?: string;
      readonly after?: number;
      readonly newestFirst?: boolean;
      readonly afterReadNumber?: number;
      readonly limit?: number;
    },
  ) {
    const db = yield* communicationQueries;
    let query = db
      .selectFrom("message_log")
      .select(["ordinal", "message", "read_number", "guidance"])
      .where("agent_key", "=", agentKey);
    if (range.conversationId !== undefined) {
      query = query.where("conversation_id", "=", range.conversationId);
    }
    if (range.after !== undefined) {
      query = query.where("ordinal", ">", range.after);
    }
    if (range.afterReadNumber !== undefined) {
      query = query.where("read_number", ">", range.afterReadNumber);
    }
    if (range.limit !== undefined) {
      query = query.limit(range.limit);
    }
    const ascending =
      range.afterReadNumber === undefined ? "ordinal" : "read_number";
    const rows = yield* executeQuery(
      range.newestFirst
        ? query.orderBy("ordinal", "desc")
        : query.orderBy(ascending),
    );
    return yield* Effect.forEach(
      rows,
      (row) =>
        Effect.map(
          decodeJson(Message, row.message),
          (
            message,
          ): HeldMessage & {
            readonly ordinal: number;
            readonly readNumber: number | null;
          } => ({
            ordinal: row.ordinal,
            readNumber: row.read_number,
            message,
            messageBytes: Buffer.byteLength(row.message),
            readable: row.read_number !== null,
            ...(row.guidance === null ? {} : { guidance: row.guidance }),
          }),
        ),
      { concurrency: 1 },
    );
  },
  (work, agentKey) => withAgentRows(agentKey, work),
);

/**
 * At most `limit` of the messages the agent stored under `agentKey` may read
 * after `cursor`, in read order, with the cursor to read on from and whether
 * more follow. Without a cursor the read starts from the newest and returns
 * none.
 */
export const readInbox = Effect.fn("readInbox")(function* (
  agentKey: string,
  { limit, cursor }: { readonly limit: number; readonly cursor?: string },
) {
  const [row] = yield* withAgentRows(
    agentKey,
    Effect.flatMap(newestReadNumber(agentKey), executeQuery),
  );
  const from = Number(cursor ?? row?.newest ?? 0);
  const rows = yield* readMessages(agentKey, {
    afterReadNumber: from,
    limit: limit + 1,
  });
  const page = rows.slice(0, limit);
  return {
    messages: page,
    nextCursor: String(page.at(-1)?.readNumber ?? from),
    hasMore: rows.length > limit,
  };
});

/** Every conversation the agent has a message in, in no order, each as its members in agent-name order. */
export const listConversations = Effect.fn("listConversations")(function* (
  agentKey: string,
) {
  const db = yield* communicationQueries;
  const rows = yield* withAgentRows(
    agentKey,
    executeQuery(
      db
        .selectFrom("message_log")
        .select("conversation_id as conversationId")
        .distinct()
        .where("agent_key", "=", agentKey),
    ),
  );
  const conversations: Array<readonly AgentCard[]> = [];
  for (const row of rows) {
    const [stored] = yield* readMessages(agentKey, {
      conversationId: row.conversationId,
      limit: 1,
    });
    if (stored !== undefined) {
      conversations.push(messageMembers(stored.message));
    }
  }
  return conversations;
});

/**
 * One page of the agent's conversation among `members`, oldest first, after
 * the cursor an earlier page returned. A page's `nextCursor` is the position
 * of its last message.
 */
export const readHistory = Effect.fn("readHistory")(function* (
  agentKey: string,
  request: PaginatedRequest & { readonly members: readonly AgentCard[] },
) {
  const selected = yield* readMessages(agentKey, {
    conversationId: conversationOf(request.members),
    after: Number(request.cursor ?? -1),
    limit: PAGE_SIZE + 1,
  });
  const entries = selected.slice(0, PAGE_SIZE);
  const last = entries.at(-1);
  return {
    entries,
    ...(selected.length > PAGE_SIZE && last !== undefined
      ? { nextCursor: String(last.ordinal) }
      : {}),
  };
});

/**
 * The latest messages of the agent's conversation among `members`, oldest
 * first: at most `limit` of them, or every one without it.
 */
export const readLatest = Effect.fn("readLatest")(function* (
  agentKey: string,
  window: {
    readonly members: readonly AgentCard[];
    readonly limit?: number;
  },
) {
  const selected = yield* readMessages(agentKey, {
    conversationId: conversationOf(window.members),
    newestFirst: true,
    ...(window.limit === undefined ? {} : { limit: window.limit + 1 }),
  });
  const entries = selected.slice(0, window.limit).reverse();
  return {
    entries,
    earlierMessagesOmitted: selected.length > entries.length,
  };
});
