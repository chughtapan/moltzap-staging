/** @file Stores one agent's unchecked messages, its inbox entries in the order they became readable, and the position its host has read to. */

import { SqlClient } from "@effect/sql";
import { Data, Effect, Encoding, Schema } from "effect";
import type { IncomingDecision } from "../firewall/service.js";
import {
  Message,
  type MessageReference,
  PAGE_SIZE,
} from "../communication/index.js";
import {
  type Compilable,
  executeQuery,
  type Generated,
  makeQueries,
  withAgentRows,
} from "../database.js";
import {
  decodeJson,
  encodeJson,
  exactStruct,
  MAXIMUM_RUNTIME_ITEM_BYTES,
} from "../encoding.js";
import {
  InboxEntry,
  InboxError,
  type InboxPage,
  MessageAddress,
} from "../mcp/tools.js";

const uncheckedRow = exactStruct({
  address: MessageAddress,
  unchecked: Schema.parseJson(Message),
});
/** A pending entry as stored text, whose size counts toward a page's bytes. */
const pendingRow = exactStruct({
  read_number: Schema.Number,
  entry: Schema.String,
});
const positionRow = exactStruct({
  newest: Schema.Number,
  committed: Schema.Number,
});
const inboxCursor = Schema.Struct({
  operation: Schema.Literal("inbox"),
  after: Schema.NonNegativeInt,
});

/** A received message the firewall has not yet decided, with the address its inbox entry takes. */
export interface Unchecked {
  readonly address: MessageAddress;
  readonly message: Message;
}

/**
 * `read_number` orders an agent's allowed entries by when each became
 * readable, counting from one. `inbox_positions` holds, for one agent, the
 * newest read number given out and the one its host has read to.
 */
type InboxTables = Record<
  "inbox_entries",
  Record<"agent_key" | "address" | "author" | "message_id", string> &
    Record<"unchecked" | "entry", string | null> &
    Record<"sequence", Generated<number>> &
    Record<"read_number", number | null> &
    Record<"status", "unchecked" | "withheld" | "allowed">
> &
  Record<
    "inbox_positions",
    Record<"agent_key", string> & Record<"newest" | "committed", number>
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
  /**
   * The newest read number an entry holds and the one the host has read to.
   * The entries between them are the pending ones.
   */
  const position = read(
    positionRow,
    queries
      .selectFrom("inbox_positions")
      .select(["newest", "committed"])
      .where("agent_key", "=", agentKey),
  ).pipe(Effect.map((rows) => rows[0] ?? { newest: 0, committed: 0 }));
  return {
    storeUnchecked: Effect.fn(function* (
      address: MessageAddress,
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
     * An allowed entry takes the agent's next read number in the transaction
     * that stores it. Taking the number holds the agent's position row until
     * that transaction ends, so entries become readable in read-number order
     * and a reader that has read to one number has seen every entry before
     * it. A message that is no longer unchecked takes no number.
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
        Effect.gen(function* () {
          const [decided] = yield* executeQuery(
            queries
              .updateTable("inbox_entries")
              .set({
                unchecked: null,
                entry,
                status: entry === null ? "withheld" : "allowed",
              })
              .where("agent_key", "=", agentKey)
              .where("author", "=", unchecked.message.author.agentId)
              .where("message_id", "=", unchecked.message.msgId)
              .where("status", "=", "unchecked")
              .returning("sequence"),
          );
          if (decided === undefined || entry === null) {
            return;
          }
          yield* executeQuery(
            queries
              .with("position", (db) =>
                db
                  .insertInto("inbox_positions")
                  .values({ agent_key: agentKey, newest: 1, committed: 0 })
                  .onConflict((conflict) =>
                    conflict.column("agent_key").doUpdateSet((eb) => ({
                      newest: eb("inbox_positions.newest", "+", 1),
                    })),
                  )
                  .returning("newest"),
              )
              .updateTable("inbox_entries")
              .set((eb) => ({
                read_number: eb.selectFrom("position").select("newest"),
              }))
              .where("sequence", "=", decided.sequence),
          );
        }),
      ).pipe(Effect.provideService(SqlClient.SqlClient, sql));
    }),
    position,
    /**
     * Records that the host has handled every entry up to the one `cursor`
     * names. A cursor behind the committed position or ahead of the newest
     * entry is refused.
     */
    commitPosition: Effect.fn(function* (cursor: string) {
      const refused = new InboxError({ reason: "invalid-cursor" });
      const { after } = yield* decodeCursor(inboxCursor, cursor).pipe(
        Effect.catchTag("CursorError", () => refused),
      );
      const committed = yield* run(
        queries
          .updateTable("inbox_positions")
          .set({ committed: after })
          .where("agent_key", "=", agentKey)
          .where("committed", "<=", after)
          .where("newest", ">=", after)
          .returning("committed"),
      );
      if (committed.length === 0) {
        return yield* refused;
      }
    }),
    /** One page of the pending entries in read order, with the cursor that commits them. */
    listInbox: Effect.fn(function* () {
      const { committed } = yield* position;
      const rows = yield* read(
        pendingRow,
        inboxRows()
          .select(["read_number", "entry"])
          .where("read_number", ">", committed)
          .orderBy("read_number")
          .limit(PAGE_SIZE),
      );
      const entries: InboxEntry[] = [];
      let bytes = 0;
      let after: number | undefined;
      for (const row of rows) {
        const size = Buffer.byteLength(row.entry);
        if (bytes + size > MAXIMUM_RUNTIME_ITEM_BYTES) {
          break;
        }
        entries.push(yield* decodeJson(InboxEntry, row.entry));
        bytes += size;
        after = row.read_number;
      }
      return {
        entries,
        ...(after === undefined
          ? {}
          : {
              nextCursor: yield* encodeCursor(inboxCursor, {
                operation: "inbox",
                after,
              }),
            }),
      } satisfies InboxPage;
    }),
    storedEntry: (reference: MessageReference) =>
      read(
        exactStruct({ entry: Schema.parseJson(InboxEntry) }),
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
