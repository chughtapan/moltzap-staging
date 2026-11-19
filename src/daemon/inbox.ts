/** @file Stores one agent's inbox entries in the order they arrived. */

import { SqlClient } from "@effect/sql";
import { Data, Effect, Encoding, Schema } from "effect";
import type { Message, MessageReference } from "../communication/index.js";
import type { LocalGuidance } from "../firewall/index.js";
import {
  type Compilable,
  executeQuery,
  type Generated,
  makeQueries,
  withAgentRows,
} from "../database.js";
import { decodeJson, encodeJson, exactStruct } from "../encoding.js";
import {
  type AgentAddress,
  type InboxCursor,
  InboxEntry,
} from "../mcp/tools.js";

const entryRow = exactStruct({
  read_number: Schema.Number,
  entry: Schema.parseJson(InboxEntry),
});

/** `read_number` orders an agent's entries by when each was stored, counting from one. */
type InboxTables = Record<
  "inbox_entries",
  Record<"agent_key" | "author" | "message_id" | "entry", string> &
    Record<"sequence", Generated<number>> &
    Record<"read_number", number>
>;

/** One agent's inbox: the storing of an entry, then the reads of what was stored. */
export type Inbox = Effect.Effect.Success<ReturnType<typeof makeInbox>>;

/** Captures the database. */
export const makeInbox = Effect.fn("SocialHarness.inbox")(function* (
  agentKey: string,
) {
  const sql = yield* SqlClient.SqlClient;
  const queries = yield* makeQueries<InboxTables>();
  const run = <Row extends object>(query: Compilable<Row>) =>
    withAgentRows(agentKey, executeQuery(query)).pipe(
      Effect.provideService(SqlClient.SqlClient, sql),
    );
  /** The rows of a query, each read through `row`. */
  const read = <A, I, Row extends object>(
    row: Schema.Schema<A, I>,
    query: Compilable<Row>,
  ) => run(query).pipe(Effect.flatMap(Schema.decodeUnknown(Schema.Array(row))));
  const inboxRows = () =>
    queries.selectFrom("inbox_entries").where("agent_key", "=", agentKey);
  /** The newest read number an entry holds, or zero for an inbox with no entry. */
  const newest = (rows: ReturnType<typeof inboxRows>) =>
    rows.select((eb) =>
      eb.fn.coalesce(eb.fn.max("read_number"), eb.val(0)).as("newest"),
    );
  return {
    /**
     * Stores a received message as the agent's next entry, with the guidance
     * its check gave. Storing a message again changes nothing. The lock on
     * the agent is held until the caller's transaction ends, so entries
     * become readable in read-number order and a reader that has read to one
     * number has seen every entry before it.
     */
    store: Effect.fn(function* (
      address: AgentAddress,
      message: Message,
      guidance?: LocalGuidance,
    ) {
      const entry = yield* encodeJson(InboxEntry, {
        address,
        message,
        ...(guidance === undefined ? {} : { guidance }),
      });
      yield* withAgentRows(
        agentKey,
        sql`SELECT pg_advisory_xact_lock(hashtext(${agentKey}))`.pipe(
          Effect.zipRight(
            executeQuery(
              queries
                .insertInto("inbox_entries")
                .values((eb) => ({
                  agent_key: agentKey,
                  author: message.author.agentId,
                  message_id: message.msgId,
                  entry,
                  read_number: eb(newest(inboxRows()), "+", 1).$notNull(),
                }))
                .onConflict((conflict) =>
                  conflict
                    .columns(["agent_key", "author", "message_id"])
                    .doNothing(),
                ),
            ),
          ),
        ),
      ).pipe(Effect.provideService(SqlClient.SqlClient, sql));
    }),
    /**
     * At most `limit` of the entries after `cursor`, in read order, with the
     * cursor to read on from and whether more entries follow. Without a
     * cursor the read starts from the newest entry and returns none.
     */
    listInbox: Effect.fn(function* (
      limit: number,
      cursor?: typeof InboxCursor.Type,
    ) {
      const from = Number(
        cursor ?? (yield* run(newest(inboxRows())))[0]?.newest ?? 0,
      );
      const rows = yield* read(
        entryRow,
        inboxRows()
          .select(["read_number", "entry"])
          .where("read_number", ">", from)
          .orderBy("read_number")
          .limit(limit + 1),
      );
      const page = rows.slice(0, limit);
      return {
        entries: page.map((row) => row.entry),
        nextCursor: String(page.at(-1)?.read_number ?? from),
        hasMore: rows.length > limit,
      };
    }),
    storedEntry: (reference: MessageReference) =>
      read(
        exactStruct({ entry: entryRow.fields.entry }),
        inboxRows()
          .select("entry")
          .where("author", "=", reference.authorAgentId)
          .where("message_id", "=", reference.msgId)
          .limit(1),
      ).pipe(Effect.map((rows) => rows[0]?.entry)),
  };
});

/** The text is not a cursor the read it was given to returned. */
export class CursorError extends Data.TaggedError("CursorError") {}

/** A cursor is its position as base64url JSON text. */
export function encodeCursor<A, I>(
  schema: Schema.Schema<A, I>,
  position: NoInfer<A>,
) {
  return encodeJson(schema, position).pipe(
    Effect.map((text) => Encoding.encodeBase64Url(text)),
  );
}

/** The position a cursor holds, when it is one `schema` describes. */
export function decodeCursor<A, I>(
  schema: Schema.Schema<A, I>,
  cursor: string,
) {
  return Encoding.decodeBase64UrlString(cursor).pipe(
    Effect.flatMap((text) => decodeJson(schema, text)),
    Effect.catchTags({
      DecodeException: () => new CursorError(),
      JsonRepresentationError: () => new CursorError(),
    }),
  );
}
