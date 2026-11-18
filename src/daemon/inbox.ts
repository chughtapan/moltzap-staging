/** @file Stores one agent's unchecked messages and collective results, its inbox entries and which of them are delivered. */

import { SqlClient } from "@effect/sql";
import { Effect, Encoding, Schema } from "effect";
import { randomBytes } from "node:crypto";
import type {
  Message,
  MessageReference,
  MsgId,
  PaginatedRequest,
} from "../communication/index.js";
import type { IncomingDecision } from "../firewall/service.js";
import { Incoming } from "../communication/service.js";
import {
  type Compilable,
  executeQuery,
  type Generated,
  makeQueries,
  withAgentRows,
} from "../database/index.js";
import {
  decodeJson,
  encodeJson,
  exactStruct,
  MAXIMUM_RUNTIME_ITEM_BYTES,
} from "../encoding.js";
import {
  DeliveryToken,
  type InboxEntry,
  InboxError,
  type InboxPage,
  MessageAddress,
  PAGE_SIZE,
  StoredInboxEntry,
} from "../mcp/tools.js";

const uncheckedRow = exactStruct({
  sequence: Schema.Number,
  delivery_token: DeliveryToken,
  address: MessageAddress,
  unchecked: Schema.parseJson(Incoming),
});
const position = { sequence: Schema.Number, delivery_token: DeliveryToken };
/** A pending entry as stored text, whose size counts toward a page's bytes. */
const pendingRow = exactStruct({ ...position, entry: Schema.String });
const entryRow = exactStruct({
  ...position,
  entry: Schema.parseJson(StoredInboxEntry),
});
const inboxCursor = Schema.Struct({
  operation: Schema.Literal("inbox"),
  after: Schema.NonNegativeInt,
  through: Schema.NonNegativeInt,
});

/**
 * A received message or collective result the firewall has not yet decided,
 * with the address and delivery token its inbox entry takes.
 */
export interface Unchecked {
  readonly sequence: number;
  readonly token: DeliveryToken;
  readonly address: MessageAddress;
  readonly incoming: Incoming;
}

type InboxTables = Record<
  "inbox_entries",
  Record<
    "agent_key" | "dedupe_key" | "delivery_token" | "address" | "kind",
    string
  > &
    Record<"author" | "message_id", string | null> &
    Record<"unchecked" | "entry", string | null> &
    Record<"sequence", Generated<number>> &
    Record<"status", "unchecked" | "withheld" | "pending" | "delivered">
> &
  Record<
    "inbox_visibility",
    Record<"agent_key" | "author" | "message_id", string> &
      Record<"visible", number>
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
  ) =>
    run(query).pipe(
      Effect.flatMap(Schema.decodeUnknown(Schema.Array(row))),
      Effect.mapError(persistenceFailure),
    );
  const inboxRows = () =>
    queries.selectFrom("inbox_entries").where("agent_key", "=", agentKey);
  const head = (rows: ReturnType<typeof inboxRows>) =>
    run(
      rows.select((eb) =>
        eb.fn.coalesce(eb.fn.max<number>("sequence"), eb.val(0)).as("head"),
      ),
    ).pipe(Effect.map((found) => found[0]?.head ?? 0));
  return {
    storeUnchecked: Effect.fn(function* (
      address: MessageAddress,
      incoming: Incoming,
    ) {
      const encodedIncoming = yield* encodeJson(Incoming, incoming);
      const message =
        incoming.kind === "collectiveResult" ? undefined : incoming.message;
      const dedupeKey =
        incoming.kind === "collectiveResult"
          ? `result:${incoming.result.request.msgId}`
          : `message:${incoming.message.author.agentId}:${incoming.message.msgId}`;
      const token = DeliveryToken.make(
        `dlv_${randomBytes(32).toString("base64url")}`,
      );
      yield* run(
        queries
          .insertInto("inbox_entries")
          .values({
            agent_key: agentKey,
            dedupe_key: dedupeKey,
            delivery_token: token,
            address,
            kind: incoming.kind,
            author: message?.author.agentId ?? null,
            message_id: message?.msgId ?? null,
            unchecked: encodedIncoming,
            entry: null,
            status: "unchecked",
          })
          .onConflict((conflict) =>
            conflict.columns(["agent_key", "dedupe_key"]).doNothing(),
          ),
      );
    }, Effect.mapError(persistenceFailure)),
    unchecked: (limit: number) =>
      read(
        uncheckedRow,
        queries
          .selectFrom("inbox_entries as candidate")
          .select([
            "candidate.sequence",
            "candidate.delivery_token",
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
              token: row.delivery_token,
              address: row.address,
              incoming: row.unchecked,
            }),
          ),
        ),
      ),
    recordDecision: Effect.fn(function* (
      unchecked: Unchecked,
      decision: IncomingDecision,
      { expired = false }: { readonly expired?: boolean } = {},
    ) {
      let encodedEntry: string | undefined;
      if (decision.kind === "allowed" && !expired) {
        encodedEntry = yield* encodeJson(StoredInboxEntry, {
          address: unchecked.address,
          item: decision.item,
          ...(decision.guidance === undefined
            ? {}
            : { guidance: decision.guidance }),
        });
        if (
          Buffer.byteLength(encodedEntry) > MAXIMUM_RUNTIME_ITEM_BYTES &&
          decision.item.kind === "collectiveResult"
        ) {
          encodedEntry = yield* encodeJson(StoredInboxEntry, {
            address: unchecked.address,
            item: decision.item,
          });
        }
        if (Buffer.byteLength(encodedEntry) > MAXIMUM_RUNTIME_ITEM_BYTES) {
          encodedEntry = undefined;
        }
      }
      yield* run(
        queries
          .updateTable("inbox_entries")
          .set({
            unchecked: null,
            entry: encodedEntry ?? null,
            status: encodedEntry === undefined ? "withheld" : "pending",
          })
          .where("agent_key", "=", agentKey)
          .where("delivery_token", "=", unchecked.token)
          .where("status", "=", "unchecked"),
      );
      for (const permission of decision.visibility) {
        const visible =
          !expired && encodedEntry !== undefined && permission.visibleToAgent;
        yield* run(
          queries
            .insertInto("inbox_visibility")
            .values({
              agent_key: agentKey,
              author: permission.message.authorAgentId,
              message_id: permission.message.msgId,
              visible: visible ? 1 : 0,
            })
            .onConflict((conflict) =>
              conflict
                .columns(["agent_key", "author", "message_id"])
                .doUpdateSet((eb) => ({
                  // eslint-disable-next-line agent-code-guard/then-chain -- Kysely CASE branches compile SQL; this is not Promise chaining.
                  visible: eb
                    .case()
                    .when(
                      "excluded.visible",
                      ">",
                      eb.ref("inbox_visibility.visible"),
                    )
                    .then(eb.ref("excluded.visible"))
                    .else(eb.ref("inbox_visibility.visible"))
                    .end(),
                })),
            ),
        );
      }
    }, Effect.mapError(persistenceFailure)),
    withholdExpired: (token: DeliveryToken) =>
      run(
        queries
          .updateTable("inbox_entries")
          .set({ unchecked: null, status: "withheld" })
          .where("agent_key", "=", agentKey)
          .where("delivery_token", "=", token)
          .where("status", "=", "unchecked"),
      ).pipe(Effect.asVoid, Effect.mapError(persistenceFailure)),
    listInbox: Effect.fn(
      function* (request: typeof PaginatedRequest.Type = {}) {
        const position =
          request.cursor === undefined
            ? { after: 0, through: yield* head(inboxRows()) }
            : yield* decodeCursor(inboxCursor, request.cursor);
        const rows = yield* read(
          pendingRow,
          inboxRows()
            .select(["sequence", "delivery_token", "entry"])
            .where("status", "=", "pending")
            .where("sequence", ">", position.after)
            .where("sequence", "<=", position.through)
            .orderBy("sequence")
            .limit(PAGE_SIZE + 1),
        );
        const entries: InboxEntry[] = [];
        let bytes = 0;
        let after = position.after;
        for (const row of rows.slice(0, PAGE_SIZE)) {
          const size = Buffer.byteLength(row.entry);
          if (bytes + size > MAXIMUM_RUNTIME_ITEM_BYTES) {
            break;
          }
          const stored = yield* decodeJson(StoredInboxEntry, row.entry);
          entries.push({ ...stored, deliveryToken: row.delivery_token });
          bytes += size;
          after = row.sequence;
        }
        return {
          entries,
          ...(rows.length > entries.length
            ? {
                nextCursor: yield* encodeCursor(inboxCursor, {
                  operation: "inbox",
                  after,
                  through: position.through,
                }),
              }
            : {}),
        } satisfies InboxPage;
      },
      Effect.mapError((error) =>
        error instanceof InboxError ? error : persistenceFailure(),
      ),
    ),
    /** The delivery token of each pending entry. */
    pendingTokens: read(
      Schema.Struct({ delivery_token: DeliveryToken }),
      inboxRows().select("delivery_token").where("status", "=", "pending"),
    ).pipe(Effect.map((rows) => rows.map((row) => row.delivery_token))),
    /** Marks the pending entries carrying these delivery tokens delivered, all or none. */
    markDelivered: (tokens: readonly DeliveryToken[]) =>
      tokens.length === 0
        ? Effect.void
        : run(
            queries
              .updateTable("inbox_entries")
              .set({ status: "delivered" })
              .where("agent_key", "=", agentKey)
              .where("delivery_token", "in", tokens)
              .where("status", "=", "pending"),
          ).pipe(Effect.asVoid, Effect.mapError(persistenceFailure)),
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
    collectiveResultHead: (address: string) =>
      head(
        inboxRows()
          .where("address", "=", address)
          .where("kind", "=", "collectiveResult")
          .where("entry", "is not", null),
      ).pipe(Effect.mapError(persistenceFailure)),
    collectiveResultPage: (
      address: string,
      after: number,
      through: number,
      limit: number,
    ) =>
      read(
        entryRow,
        inboxRows()
          .select(["sequence", "delivery_token", "entry"])
          .where("address", "=", address)
          .where("sequence", ">", after)
          .where("sequence", "<=", through)
          .where("kind", "=", "collectiveResult")
          .where("entry", "is not", null)
          .orderBy("sequence")
          .limit(limit + 1),
      ).pipe(
        Effect.map((rows) =>
          rows.map((row) => ({
            sequence: row.sequence,
            token: row.delivery_token,
            entry: row.entry,
          })),
        ),
      ),
    collectiveResultAddresses: (after = "") =>
      read(
        Schema.Struct({ address: MessageAddress }),
        inboxRows()
          .select("address")
          .distinct()
          .where("address", ">", after)
          .where("kind", "=", "collectiveResult")
          .where("entry", "is not", null)
          .orderBy("address")
          .limit(PAGE_SIZE + 1),
      ).pipe(Effect.map((rows) => rows.map((row) => row.address))),
    isCollectiveRequestVisible: (requestId: MsgId) =>
      run(
        inboxRows()
          .select("sequence")
          .where("kind", "=", "collectiveRequest")
          .where("message_id", "=", requestId)
          .where("entry", "is not", null)
          .limit(1),
      ).pipe(
        Effect.map((rows) => rows.length !== 0),
        Effect.mapError(persistenceFailure),
      ),
    visibleMessages: (messages: readonly Message[]) =>
      Effect.forEach(
        messages,
        (message) =>
          run(
            queries
              .selectFrom("inbox_visibility")
              .select("visible")
              .where("agent_key", "=", agentKey)
              .where("author", "=", message.author.agentId)
              .where("message_id", "=", message.msgId),
          ).pipe(
            Effect.map((rows) => ({
              message,
              visibleToAgent: rows[0]?.visible === 1,
            })),
          ),
        { concurrency: 1 },
      ).pipe(Effect.mapError(persistenceFailure)),
  };
});

function persistenceFailure() {
  return new InboxError({ reason: "persistence-failed" });
}

/** A cursor is its position as base64url JSON text. */
export function encodeCursor<A, I>(
  schema: Schema.Schema<A, I>,
  position: NoInfer<A>,
) {
  return encodeJson(schema, position).pipe(
    Effect.map((text) => Encoding.encodeBase64Url(text)),
    Effect.mapError(persistenceFailure),
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
      DecodeException: () => new InboxError({ reason: "invalid-cursor" }),
      JsonRepresentationError: () =>
        new InboxError({ reason: "invalid-cursor" }),
    }),
  );
}
