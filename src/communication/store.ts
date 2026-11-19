/** @file Stores each agent's ordered messages and which of them it has consumed. */

import { Cause, Data, Effect, Option, type Schema } from "effect";

import type { AgentId } from "../identity.js";
import {
  executeQuery,
  type Generated,
  makeQueries,
  withAgentRows,
} from "../database/index.js";
import { decodeJson, encodeJson, sameJson } from "../encoding.js";
import { Message, type MessageReference } from "./messages.js";

/** One closed store failure. */
export class EndpointStoreError extends Data.TaggedError("EndpointStoreError")<{
  readonly reason: EndpointStoreFailure;
}> {}

/** Closed storage failures that never expose database diagnostics. */
type EndpointStoreFailure = "conflict" | "corrupt" | "persistence";

type CommunicationTables = Record<
  "message_log",
  Record<
    "agent_key" | "conversation_id" | "author_agent_id" | "msg_id" | "message",
    string
  > &
    Record<"ordinal", number> &
    Record<"sequence" | "consumed", Generated<number>>
> &
  Record<
    "message_sequences",
    Record<"conversation_id", string> & Record<"next_ordinal", number>
  > &
  Record<
    "agent_cards",
    Record<"agent_key" | "agent_id" | "agent_name" | "principal_id", string>
  >;

/** Builds queries over the tables that hold messages and the agents they name. */
export const communicationQueries = makeQueries<CommunicationTables>();

const conflict = () => new EndpointStoreError({ reason: "conflict" });
const corrupt = () => new EndpointStoreError({ reason: "corrupt" });

/** Reads stored JSON text through its schema; text the schema refuses is `corrupt`. */
const decodeStored = <A, I>(
  schema: Schema.Schema<A, I>,
  text: string,
): Effect.Effect<A, EndpointStoreError> =>
  decodeJson(schema, text).pipe(
    Effect.catchTag("JsonRepresentationError", corrupt),
  );

/** A message at its position in a conversation, as one agent's history holds it. */
export interface StoredMessage {
  readonly conversationId: string;
  readonly ordinal: number;
  readonly sequence: number;
  readonly message: Message;
  /** The size of the message's stored JSON text. */
  readonly bytes: number;
}

/**
 * Whether this agent's history holds the message. A saved message with the
 * same author and identifier but another conversation or content is a
 * conflict.
 */
export const hasMessage = Effect.fn("hasMessage")(function* (
  agentKey: string,
  conversationId: string,
  message: Message,
) {
  const db = yield* communicationQueries;
  const [row] = yield* executeQuery(
    db
      .selectFrom("message_log")
      .select(["conversation_id", "message"])
      .where("agent_key", "=", agentKey)
      .where("author_agent_id", "=", message.author.agentId)
      .where("msg_id", "=", message.msgId),
  );
  if (row === undefined) {
    return false;
  }
  if (
    row.conversation_id !== conversationId ||
    !sameJson(row.message, yield* encodeJson(Message, message))
  ) {
    return yield* conflict();
  }
  return true;
}, withAgentLock);

/**
 * Adds a message to one agent's history at its position. It takes no lock on
 * the agent, so an author can save for every member while holding only its own
 * agent's lock and the conversation's.
 */
export const saveMessage = Effect.fn("saveMessage")(
  function* (
    agentKey: string,
    conversationId: string,
    ordinal: number,
    message: Message,
  ) {
    const db = yield* communicationQueries;
    yield* executeQuery(
      db.insertInto("message_log").values({
        agent_key: agentKey,
        conversation_id: conversationId,
        ordinal,
        author_agent_id: message.author.agentId,
        msg_id: message.msgId,
        message: yield* encodeJson(Message, message),
      }),
    );
  },
  (work, agentKey) => withAgentRows(agentKey, work),
  asStoreError,
);

/** A bounded range of one agent's messages, in conversation order or in the order they were saved. */
export const readMessages = Effect.fn("readMessages")(function* (
  agentKey: string,
  range: {
    readonly conversationId?: string;
    readonly after?: number;
    readonly through?: number;
    readonly newestFirst?: boolean;
    /** Selects messages this agent has not consumed and did not write. */
    readonly unconsumedBy?: AgentId;
    readonly limit: number;
  },
) {
  const db = yield* communicationQueries;
  let query = db
    .selectFrom("message_log")
    .select([
      "conversation_id",
      "ordinal",
      "sequence",
      "author_agent_id",
      "msg_id",
      "message",
    ])
    .where("agent_key", "=", agentKey)
    .limit(range.limit);
  if (range.conversationId !== undefined) {
    query = query.where("conversation_id", "=", range.conversationId);
  }
  if (range.after !== undefined) {
    query = query.where("ordinal", ">", range.after);
  }
  if (range.through !== undefined) {
    query = query.where("ordinal", "<=", range.through);
  }
  if (range.unconsumedBy !== undefined) {
    query = query
      .where("consumed", "=", 0)
      .where("author_agent_id", "<>", range.unconsumedBy);
  }
  const ascending = range.conversationId === undefined ? "sequence" : "ordinal";
  const rows = yield* executeQuery(
    range.newestFirst
      ? query.orderBy("ordinal", "desc")
      : query.orderBy(ascending),
  );
  return yield* Effect.forEach(
    rows,
    (row) =>
      decodeStored(Message, row.message).pipe(
        Effect.filterOrFail(
          (message) =>
            message.author.agentId === row.author_agent_id &&
            message.msgId === row.msg_id,
          conflict,
        ),
        Effect.map(
          (message): StoredMessage => ({
            conversationId: row.conversation_id,
            ordinal: row.ordinal,
            sequence: row.sequence,
            message,
            bytes: Buffer.byteLength(row.message),
          }),
        ),
      ),
    { concurrency: 1 },
  );
}, withAgentLock);

/** Marks a message consumed, so that it is not handed to the consumer again. */
export const markMessageConsumed = Effect.fn("markMessageConsumed")(function* (
  agentKey: string,
  reference: MessageReference,
) {
  const db = yield* communicationQueries;
  yield* executeQuery(
    db
      .updateTable("message_log")
      .set({ consumed: 1 })
      .where("agent_key", "=", agentKey)
      .where("author_agent_id", "=", reference.authorAgentId)
      .where("msg_id", "=", reference.msgId),
  );
}, withAgentLock);

/**
 * Locking the agent's own row serializes one agent's transactions, so a
 * transaction that reads before it writes sees every earlier write of the same
 * agent. Nested calls retain the lock until the enclosing transaction commits;
 * other agents use their own rows.
 */
function withAgentLock<A, E, R>(
  work: Effect.Effect<A, E, R>,
  agentKey: string,
) {
  return withAgentRows(
    agentKey,
    Effect.gen(function* () {
      const db = yield* communicationQueries;
      yield* executeQuery(
        db
          .selectFrom("agent_cards")
          .select("agent_key")
          .where("agent_key", "=", agentKey)
          .forUpdate(),
      );
      return yield* work;
    }),
  ).pipe(asStoreError, Effect.withSpan("withAgentLock"));
}

/** SQL failures, defects and row corruption leave through one closed storage error channel. */
function asStoreError<A, E, R>(
  work: Effect.Effect<A, E, R>,
): Effect.Effect<A, EndpointStoreError, R> {
  return work.pipe(
    Effect.catchAllCause((cause) => {
      if (Cause.isInterruptedOnly(cause)) {
        return Effect.failCause(Cause.stripFailures(cause));
      }
      const failure = Cause.failureOption(cause);
      return Effect.fail(
        Option.isSome(failure) && failure.value instanceof EndpointStoreError
          ? failure.value
          : new EndpointStoreError({ reason: "persistence" }),
      );
    }),
  );
}
