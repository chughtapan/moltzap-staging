/** @file Incoming items retain one host delivery identity through assessment and replay. */

import * as SqlClient from "@effect/sql/SqlClient";
import { Effect, Schema } from "effect";
import type { IncomingEntry, LocalAdmission } from "./types.js";
import {
  type MessageReference,
  MessageReference as MessageReferenceSchema,
} from "../../communication/message.js";
import {
  EndpointStoreError,
  readBytes,
  readInteger,
  readOptionalBytes,
  readOptionalText,
  readText,
  requireBytes,
  requireSameBytes,
  requireSameOptionalBytes,
  requireText,
  storeOperation,
} from "../../communication/store/database/index.js";
import { DeliveryToken } from "../../communication/store/index.js";
import { MAXIMUM_RUNTIME_ITEM_BYTES } from "../../encoding/limits.js";
import { saveCheckDecision } from "../../firewall/store/firewall.js";

const columns =
  "delivery_token,conversation,canonical_item,canonical_sources,author_agent_id,msg_id";

const incomingSchema = `
CREATE TABLE incoming_items (
 sequence INTEGER PRIMARY KEY AUTOINCREMENT,
 delivery_token TEXT NOT NULL UNIQUE,
 conversation TEXT NOT NULL,
 canonical_item BLOB NOT NULL,
 canonical_sources BLOB,
 author_agent_id TEXT,
 msg_id TEXT,
 check_id TEXT,
 published_sequence INTEGER UNIQUE,
 published_item BLOB,
 canonical_guidance BLOB,
 acknowledged INTEGER NOT NULL DEFAULT 0 CHECK(acknowledged IN(0,1)),
 disposition TEXT NOT NULL DEFAULT 'pending' CHECK(disposition IN ('pending','admitted','withheld')),
 CHECK((author_agent_id IS NULL)=(msg_id IS NULL)),
 UNIQUE(author_agent_id,msg_id)
) STRICT;
CREATE INDEX incoming_frontiers ON incoming_items(conversation,sequence) WHERE disposition='pending';
CREATE INDEX incoming_pending_sequence ON incoming_items(sequence) WHERE disposition='pending';
CREATE TABLE message_visibility (
 author_agent_id TEXT NOT NULL,msg_id TEXT NOT NULL,admitted INTEGER NOT NULL CHECK(admitted IN(0,1)),delivery_token TEXT NOT NULL,
 PRIMARY KEY(author_agent_id,msg_id)
) STRICT;
CREATE INDEX incoming_unread ON incoming_items(published_sequence) WHERE acknowledged=0 AND published_item IS NOT NULL;
`;

/** Host queue, visibility and projected history share the inbox transaction lifetime. */
export function createIncomingTables() {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    for (const statement of incomingSchema
      .split(";")
      .map((part) => part.trim())
      .filter((part) => part.length > 0)) {
      yield* sql.unsafe<Readonly<Record<string, unknown>>>(statement);
    }
  }).pipe(storeOperation, Effect.withSpan("createIncomingTables"));
}

/** A replay finds the host identity retained for the same author and message id. */
export function findIncomingMessage(message: MessageReference) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const row = (yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      `SELECT ${columns} FROM incoming_items WHERE author_agent_id=? AND msg_id=?`,
      [message.authorAgentId, message.msgId],
    ))[0];
    return row === undefined ? undefined : yield* incomingEntry(row);
  }).pipe(storeOperation, Effect.withSpan("findIncomingMessage"));
}

/** A delivery token and source message bind immutable public and private bytes independently. */
export function queueIncomingItem(item: IncomingEntry) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const values = yield* queueValues(item);
    const prior = (yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      `SELECT ${columns} FROM incoming_items WHERE delivery_token=? OR (author_agent_id=? AND msg_id=?)`,
      [item.deliveryToken, values.author, values.msgId],
    ))[0];
    if (prior !== undefined) {
      yield* requireSameItem(yield* incomingEntry(prior), item);
      return;
    }
    yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      "INSERT INTO incoming_items(delivery_token,conversation,canonical_item,canonical_sources,author_agent_id,msg_id) VALUES(?,?,?,?,?,?)",
      [
        item.deliveryToken,
        item.conversation,
        item.canonicalItem,
        values.sources,
        values.author,
        values.msgId,
      ],
    );
  }).pipe(storeOperation, Effect.withSpan("queueIncomingItem"));
}

/** At most one unsettled predecessor per conversation is eligible for assessment. */
export function readNextItemsToCheck() {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      `SELECT ${columns} FROM incoming_items item WHERE item.disposition='pending' AND NOT EXISTS(SELECT 1 FROM incoming_items earlier WHERE earlier.conversation=item.conversation AND earlier.disposition='pending' AND earlier.sequence<item.sequence) ORDER BY item.sequence LIMIT 32`,
    );
    return yield* Effect.forEach(rows, incomingEntry, { concurrency: 1 });
  }).pipe(storeOperation, Effect.withSpan("readNextItemsToCheck"));
}

/** Result judgments and their saved host history settle in the publication transaction. */
export function settleLocalItem(admission: LocalAdmission) {
  return Effect.gen(function* () {
    yield* requireNextItemToCheck(
      admission.item.deliveryToken,
      admission.queuedItem,
    );
    for (const completion of admission.completions) {
      if (completion.direction !== "incoming") {
        return yield* new EndpointStoreError({ reason: "conflict" });
      }
      requireSameBytes(completion.canonicalCandidate, admission.queuedItem);
      yield* saveCheckDecision(completion);
    }
    for (const visibility of admission.visibility) {
      yield* markMessageVisibility(
        visibility.message,
        visibility.admitted,
        admission.item.deliveryToken,
      );
    }
    yield* settleQueuedItem(admission.item.deliveryToken, "admitted");
  }).pipe(storeOperation, Effect.withSpan("settleLocalItem"));
}

/** Expiry retires a request without inventing a policy judgment. */
export function retireQueuedItem(deliveryToken: DeliveryToken) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const row = (yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      "SELECT canonical_item FROM incoming_items WHERE delivery_token=? AND disposition='pending'",
      [deliveryToken],
    ))[0];
    if (row === undefined) {
      return yield* new EndpointStoreError({ reason: "not-found" });
    }
    yield* requireNextItemToCheck(
      deliveryToken,
      readBytes(row, "canonical_item"),
    );
    yield* settleQueuedItem(deliveryToken, "withheld");
  }).pipe(storeOperation, Effect.withSpan("retireQueuedItem"));
}

/** An item can finish only after earlier items in its conversation. */
export function requireNextItemToCheck(
  deliveryToken: DeliveryToken,
  canonicalItem: Uint8Array,
) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const row = (yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      "SELECT conversation,sequence,canonical_item FROM incoming_items WHERE delivery_token=? AND disposition='pending'",
      [deliveryToken],
    ))[0];
    if (row === undefined) {
      return yield* new EndpointStoreError({ reason: "conflict" });
    }
    requireSameBytes(readBytes(row, "canonical_item"), canonicalItem);
    const earlier = (yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      "SELECT 1 AS pending FROM incoming_items WHERE conversation=? AND sequence<? AND disposition='pending' LIMIT 1",
      [readText(row, "conversation"), readInteger(row, "sequence")],
    ))[0];
    if (earlier !== undefined) {
      return yield* new EndpointStoreError({ reason: "conflict" });
    }
  }).pipe(storeOperation, Effect.withSpan("requireNextItemToCheck"));
}

/** Settlement preserves the source identity after inbox acknowledgment or withholding. */
export function settleQueuedItem(
  deliveryToken: DeliveryToken,
  disposition: "admitted" | "withheld",
  checkId?: string,
) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const row = (yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      `UPDATE incoming_items SET disposition=?,check_id=? WHERE delivery_token=? RETURNING ${columns}`,
      [disposition, checkId ?? null, deliveryToken],
    ))[0];
    if (row === undefined) {
      return yield* new EndpointStoreError({ reason: "not-found" });
    }
    const entry = yield* incomingEntry(row);
    if (entry.message !== undefined) {
      yield* markMessageVisibility(
        entry.message,
        disposition === "admitted",
        deliveryToken,
      );
    }
  }).pipe(storeOperation, Effect.withSpan("settleQueuedItem"));
}

/** A settled source keeps its past visibility even when a later result withholds the same text. */
export function markMessageVisibility(
  message: MessageReference,
  admitted: boolean,
  deliveryToken: DeliveryToken,
) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      `INSERT INTO message_visibility(author_agent_id,msg_id,admitted,delivery_token) VALUES(?,?,?,?) ON CONFLICT(author_agent_id,msg_id) DO UPDATE SET admitted=MAX(message_visibility.admitted,excluded.admitted),delivery_token=CASE WHEN message_visibility.admitted=1 THEN message_visibility.delivery_token ELSE excluded.delivery_token END`,
      [message.authorAgentId, message.msgId, admitted ? 1 : 0, deliveryToken],
    );
  }).pipe(storeOperation, Effect.withSpan("markMessageVisibility"));
}

function requireSameItem(prior: IncomingEntry, item: IncomingEntry) {
  return Effect.gen(function* () {
    requireSameBytes(prior.canonicalItem, item.canonicalItem);
    requireSameOptionalBytes(prior.canonicalSources, item.canonicalSources);
    if (
      prior.deliveryToken !== item.deliveryToken ||
      prior.conversation !== item.conversation ||
      !sameReference(prior.message, item.message)
    ) {
      return yield* new EndpointStoreError({ reason: "conflict" });
    }
  });
}

function incomingEntry(row: Readonly<Record<string, unknown>>) {
  return Effect.gen(function* () {
    const author = readOptionalText(row, "author_agent_id");
    const id = readOptionalText(row, "msg_id");
    const sources = readOptionalBytes(row, "canonical_sources");
    return {
      deliveryToken: yield* Schema.decodeUnknown(DeliveryToken)(
        readText(row, "delivery_token"),
      ),
      conversation: readText(row, "conversation"),
      canonicalItem: readBytes(row, "canonical_item"),
      ...(sources === undefined ? {} : { canonicalSources: sources }),
      ...(author === undefined || id === undefined
        ? {}
        : {
            message: yield* Schema.decodeUnknown(MessageReferenceSchema)({
              authorAgentId: author,
              msgId: id,
            }),
          }),
    } satisfies IncomingEntry;
  }).pipe(
    Effect.catchTag(
      "ParseError",
      () => new EndpointStoreError({ reason: "corrupt" }),
    ),
  );
}

function queueValues(item: IncomingEntry) {
  return Effect.gen(function* () {
    requireText(item.conversation);
    requireBytes(item.canonicalItem);
    if (
      item.canonicalItem.byteLength > MAXIMUM_RUNTIME_ITEM_BYTES ||
      (item.canonicalSources?.byteLength ?? 0) > MAXIMUM_RUNTIME_ITEM_BYTES
    ) {
      return yield* new EndpointStoreError({ reason: "invalid-input" });
    }
    const source =
      item.message === undefined
        ? { author: null, msgId: null }
        : { author: item.message.authorAgentId, msgId: item.message.msgId };
    return { ...source, sources: item.canonicalSources ?? null };
  });
}

function sameReference(left?: MessageReference, right?: MessageReference) {
  return left === undefined || right === undefined
    ? left === right
    : left.authorAgentId === right.authorAgentId && left.msgId === right.msgId;
}
