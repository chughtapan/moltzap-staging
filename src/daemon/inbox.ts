/** @file Stores one agent's unchecked messages, its inbox entries in the order they became pending, and the position its host has read up to. */

import { SqlClient } from "@effect/sql";
import { Data, Effect, Encoding, Schema } from "effect";
import type { IncomingDecision } from "../firewall/service.js";
import {
  Message,
  type MessageReference,
  PAGE_SIZE,
  type PaginatedRequest,
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
import { InboxEntry, MessageAddress } from "../mcp/tools.js";

const uncheckedRow = exactStruct({
  sequence: Schema.Number,
  address: MessageAddress,
  unchecked: Schema.parseJson(Message),
});
/** A pending entry as stored text, whose size counts toward a page's bytes. */
const pendingRow = exactStruct({
  position: Schema.Number,
  entry: Schema.String,
});

/** Pending entries after a position, in the order they became pending, and the position of the last. */
export interface PendingEntries {
  readonly entries: ReadonlyArray<{
    readonly position: number;
    readonly entry: InboxEntry;
  }>;
  readonly position: number;
  readonly hasMore: boolean;
}

/**
 * A received message the firewall has not yet decided, with the address its
 * inbox entry takes.
 */
export interface Unchecked {
  readonly sequence: number;
  readonly address: MessageAddress;
  readonly message: Message;
}

interface InboxTables {
  readonly inbox_entries: Record<
    "agent_key" | "address" | "author" | "message_id",
    string
  > &
    Record<"unchecked" | "entry", string | null> &
    Record<"sequence", Generated<number>> &
    Record<"position", number | null> &
    Record<"status", "unchecked" | "withheld" | "pending">;
  readonly inbox_read: { agent_key: string; position: number };
}

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
          .select([
            "candidate.sequence",
            "candidate.address",
            "candidate.unchecked",
          ])
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
              sequence: row.sequence,
              address: row.address,
              message: row.unchecked,
            }),
          ),
        ),
      ),
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
      yield* withAgentRows(
        agentKey,
        Effect.gen(function* () {
          yield* sql`SELECT pg_advisory_xact_lock(hashtext(${agentKey}))`;
          yield* executeQuery(
            queries
              .updateTable("inbox_entries")
              .set((eb) => ({
                unchecked: null,
                entry: encodedEntry ?? null,
                status: encodedEntry === undefined ? "withheld" : "pending",
                position:
                  encodedEntry === undefined
                    ? null
                    : eb
                        .selectFrom("inbox_entries as placed")
                        .select((placed) =>
                          eb(
                            placed.fn.coalesce(
                              placed.fn.max<number>("placed.position"),
                              eb.val(0),
                            ),
                            "+",
                            1,
                          ).as("next"),
                        )
                        .where("placed.agent_key", "=", agentKey),
              }))
              .where("agent_key", "=", agentKey)
              .where("sequence", "=", unchecked.sequence)
              .where("status", "=", "unchecked"),
          );
        }),
      ).pipe(Effect.provideService(SqlClient.SqlClient, sql));
    }),
    /** At most a page of the entries that became pending after `after`. */
    pendingAfter: Effect.fn(function* (after: number, limit = PAGE_SIZE) {
      const rows = yield* read(
        pendingRow,
        inboxRows()
          .select(["position", "entry"])
          .where("status", "=", "pending")
          .where("position", ">", after)
          .orderBy("position")
          .limit(Math.min(limit, PAGE_SIZE) + 1),
      );
      const entries: Array<PendingEntries["entries"][number]> = [];
      let bytes = 0;
      for (const row of rows.slice(0, Math.min(limit, PAGE_SIZE))) {
        const size = Buffer.byteLength(row.entry);
        if (entries.length > 0 && bytes + size > MAXIMUM_RUNTIME_ITEM_BYTES) {
          break;
        }
        entries.push({
          position: row.position,
          entry: yield* decodeJson(InboxEntry, row.entry),
        });
        bytes += size;
      }
      return {
        entries,
        position: entries.at(-1)?.position ?? after,
        hasMore: rows.length > entries.length,
      } satisfies PendingEntries;
    }),
    /** The position the host has read up to; 0 before its first read. */
    readPosition: read(
      Schema.Struct({ position: Schema.Number }),
      queries
        .selectFrom("inbox_read")
        .select("position")
        .where("agent_key", "=", agentKey),
    ).pipe(Effect.map((rows) => rows[0]?.position ?? 0)),
    /** Records that the host has read up to `position`; an earlier position changes nothing. */
    markRead: (position: number) =>
      run(
        queries
          .insertInto("inbox_read")
          .values({ agent_key: agentKey, position })
          .onConflict((conflict) =>
            conflict
              .column("agent_key")
              .doUpdateSet({ position })
              .where("inbox_read.position", "<", position),
          ),
      ).pipe(Effect.asVoid),
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
