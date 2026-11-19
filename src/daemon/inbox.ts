/** @file Stores one agent's unchecked messages and its inbox entries in the order they were allowed. */

import { SqlClient } from "@effect/sql";
import { Effect, Schema } from "effect";
import type { IncomingDecision } from "../firewall/service.js";
import { Message, type MessageReference } from "../communication/index.js";
import {
  type Compilable,
  executeQuery,
  type Generated,
  makeQueries,
  withAgentRows,
} from "../database.js";
import {
  encodeJson,
  exactStruct,
  MAXIMUM_RUNTIME_ITEM_BYTES,
} from "../encoding.js";
import { AgentAddress, InboxEntry } from "../mcp/tools.js";

const uncheckedRow = exactStruct({
  address: AgentAddress,
  unchecked: Schema.parseJson(Message),
});
const entryRow = exactStruct({
  read_number: Schema.Number,
  entry: Schema.parseJson(InboxEntry),
});

/** A received message the firewall has not yet decided, with the address its inbox entry takes. */
export interface Unchecked {
  readonly address: AgentAddress;
  readonly message: Message;
}

/** `read_number` orders an agent's allowed entries by when each was allowed, counting from one. */
type InboxTables = Record<
  "inbox_entries",
  Record<"agent_key" | "address" | "author" | "message_id", string> &
    Record<"unchecked" | "entry", string | null> &
    Record<"sequence", Generated<number>> &
    Record<"read_number", number | null> &
    Record<"status", "unchecked" | "withheld" | "allowed">
>;

/** One agent's inbox: its operations in the order an entry lives through them, then the reads of what was stored. */
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
    storeUnchecked: Effect.fn(function* (
      address: AgentAddress,
      message: Message,
    ) {
      const encodedMessage = yield* encodeJson(Message, message);
      yield* run(
        queries
          .insertInto("inbox_entries")
          .values({
            agent_key: agentKey,
            address,
            author: message.author.agentId,
            message_id: message.msgId,
            unchecked: encodedMessage,
            entry: null,
            status: "unchecked",
          })
          .onConflict((conflict) =>
            conflict.columns(["agent_key", "author", "message_id"]).doNothing(),
          ),
      );
    }),
    unchecked: (limit: number) =>
      read(
        uncheckedRow,
        queries
          .selectFrom("inbox_entries as candidate")
          .select(["candidate.address", "candidate.unchecked"])
          .where("candidate.agent_key", "=", agentKey)
          .where("candidate.status", "=", "unchecked")
          .where((eb) =>
            eb.not(
              eb.exists(
                eb
                  .selectFrom("inbox_entries as earlier")
                  .select("earlier.sequence")
                  .whereRef("earlier.agent_key", "=", "candidate.agent_key")
                  .whereRef("earlier.address", "=", "candidate.address")
                  .where("earlier.status", "=", "unchecked")
                  .whereRef("earlier.sequence", "<", "candidate.sequence"),
              ),
            ),
          )
          .orderBy("candidate.sequence")
          .limit(limit),
      ).pipe(
        Effect.map((rows) =>
          rows.map(
            (row): Unchecked => ({
              address: row.address,
              message: row.unchecked,
            }),
          ),
        ),
      ),
    /**
     * An allowed entry takes the agent's next read number. The lock on the
     * agent is held until the transaction that records the decision ends, so
     * entries become readable in read-number order and a reader that has read
     * to one number has seen every entry before it.
     */
    recordDecision: Effect.fn(function* (
      unchecked: Unchecked,
      decision: IncomingDecision,
    ) {
      let encodedEntry: string | undefined;
      if (decision.kind === "allowed") {
        encodedEntry = yield* encodeJson(InboxEntry, {
          address: unchecked.address,
          message: unchecked.message,
          ...(decision.guidance === undefined
            ? {}
            : { guidance: decision.guidance }),
        });
        if (Buffer.byteLength(encodedEntry) > MAXIMUM_RUNTIME_ITEM_BYTES) {
          encodedEntry = undefined;
        }
      }
      const entry = encodedEntry ?? null;
      yield* withAgentRows(
        agentKey,
        sql`SELECT pg_advisory_xact_lock(hashtext(${agentKey}))`.pipe(
          Effect.zipRight(
            executeQuery(
              queries
                .updateTable("inbox_entries")
                .set((eb) => ({
                  unchecked: null,
                  entry,
                  status: entry === null ? "withheld" : "allowed",
                  read_number:
                    entry === null ? null : eb(newest(inboxRows()), "+", 1),
                }))
                .where("agent_key", "=", agentKey)
                .where("author", "=", unchecked.message.author.agentId)
                .where("message_id", "=", unchecked.message.msgId)
                .where("status", "=", "unchecked"),
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
    listInbox: Effect.fn(function* (limit: number, cursor?: string) {
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
          .where("entry", "is not", null)
          .limit(1),
      ).pipe(Effect.map((rows) => rows[0]?.entry)),
    visibleMessages: (messages: readonly Message[]) =>
      Effect.forEach(
        messages,
        (message) =>
          run(
            queries
              .selectFrom("inbox_entries")
              .select("sequence")
              .where("agent_key", "=", agentKey)
              .where("author", "=", message.author.agentId)
              .where("message_id", "=", message.msgId)
              .where("entry", "is not", null),
          ).pipe(
            Effect.map((rows) => ({
              message,
              visibleToAgent: rows.length > 0,
            })),
          ),
        { concurrency: 1 },
      ),
  };
});
