/** @file Stores host inbox items and tracks delivery and acknowledgment. */

import * as SqlClient from "@effect/sql/SqlClient";
import { Effect, Schema } from "effect";

import type { MessageReference } from "../../communication/message.js";
import type { InboxSummary } from "../../host-protocol/operations.js";
import type { ItemProjection } from "./types.js";
import {
  EndpointStoreError,
  readBytes,
  readInteger,
  readOptionalBytes,
  readText,
  requireBytes,
  requireSameBytes,
  requireSameOptionalBytes,
  storeOperation,
  StoreSignal,
  transaction,
} from "../../communication/store/database/index.js";
import { decodeRuntimeValue } from "../../communication/store/index.js";
import {
  DeliveryToken,
  type StoreMutation,
} from "../../communication/store/types.js";
import { MAXIMUM_RUNTIME_ITEM_BYTES } from "../../encoding/limits.js";
import {
  type LocalGuidance,
  localGuidanceSchema,
} from "../../host-protocol/advice.js";
import { InboundItem } from "../../host-protocol/items.js";
// safer-arch-ignore file-implicit-boundary-module: Daemon publication and host delivery share inbox operations within the caller’s SQL transaction.
import { createIncomingTables } from "./pending.js";
const MAXIMUM_PAGE_ITEMS = 50;
interface InboxBounds {
  readonly after?: number;
  readonly through?: number;
  readonly limit?: number;
}
/** Immutable classified item retained until the host accepts its delivery. */
export interface InboxEntry extends ItemProjection, RetainedItem {
  readonly sequence: number;
}
/** Stored content and guidance are validated together before callers receive them. */
export interface RetainedItem {
  readonly item: InboundItem;
  readonly guidance?: LocalGuidance;
}
/** A bounded view below a fixed inbox sequence, independent of later arrivals. */
export interface InboxPage {
  readonly items: readonly InboxEntry[];
  readonly through: number;
  readonly nextAfter?: number;
}
/** Callback progress and retained content share the inbox's persistence lifetime. */
export interface EventStore {
  readonly readEventState: () => Effect.Effect<
    Uint8Array | undefined,
    EndpointStoreError
  >;
  readonly writeEventState: (
    value: Uint8Array,
  ) => Effect.Effect<void, EndpointStoreError>;
  readonly readInbox: (
    input?: InboxBounds,
  ) => Effect.Effect<InboxPage, EndpointStoreError>;
  readonly readInboxItem: (
    token: DeliveryToken,
  ) => Effect.Effect<
    (InboxEntry & { readonly acknowledged: boolean }) | undefined,
    EndpointStoreError
  >;
  readonly readInboxSummary: () => Effect.Effect<
    InboxSummary,
    EndpointStoreError
  >;
  readonly completeWebhookDelivery: (
    token: DeliveryToken,
    state: Uint8Array,
  ) => Effect.Effect<void, EndpointStoreError>;
}
/** Called only inside the daemon's initial schema transaction. */
export function createInboxTables() {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql.unsafe<Readonly<Record<string, unknown>>>(`
  CREATE TABLE runtime_events (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    canonical_state BLOB NOT NULL
  ) STRICT;
`);
    yield* createIncomingTables();
  }).pipe(storeOperation, Effect.withSpan("createInboxTables"));
}
/** Retained bytes remain readable after handoff or request-context retirement. */
export function readInboxItem(deliveryToken: DeliveryToken) {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    const row = (yield* database.unsafe<Readonly<Record<string, unknown>>>(
      "SELECT published_sequence AS sequence, published_item AS canonical_item, canonical_guidance, acknowledged FROM incoming_items WHERE delivery_token = ? AND published_item IS NOT NULL",
      [deliveryToken],
    ))[0];
    if (row === undefined) {
      return undefined;
    }
    const projection = {
      canonicalItem: readBytes(row, "canonical_item"),
      ...guidanceBytes(row),
    };
    return {
      deliveryToken,
      sequence: readInteger(row, "sequence"),
      ...projection,
      ...(yield* decodeItemProjection(projection)),
      acknowledged: readInteger(row, "acknowledged") === 1,
    };
  }).pipe(storeOperation, Effect.withSpan("readInboxItem"));
}
/**
 * Bind a delivery token to exactly one classified payload, including after ack.
 * @param item Immutable classified delivery.
 * @returns Whether this call inserted the binding.
 */
export function putInboxItem(
  item: ItemProjection,
): Effect.Effect<StoreMutation, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    requireBytes(item.canonicalItem);
    if (
      item.canonicalItem.byteLength +
        (item.canonicalGuidance?.byteLength ?? 0) >
      MAXIMUM_RUNTIME_ITEM_BYTES
    ) {
      return yield* new EndpointStoreError({ reason: "invalid-input" });
    }
    const retained = (yield* database.unsafe<Readonly<Record<string, unknown>>>(
      "SELECT published_item AS canonical_item, canonical_guidance FROM incoming_items WHERE delivery_token = ? AND published_item IS NOT NULL",
      [item.deliveryToken],
    ))[0];
    if (retained !== undefined) {
      requireSameBytes(
        readBytes(retained, "canonical_item"),
        item.canonicalItem,
      );
      const priorGuidance = readOptionalBytes(retained, "canonical_guidance");
      requireSameOptionalBytes(priorGuidance, item.canonicalGuidance);
      return "existing";
    }
    const published = yield* database.unsafe<Readonly<Record<string, unknown>>>(
      "UPDATE incoming_items SET published_item=?,canonical_guidance=?,published_sequence=(SELECT COALESCE(MAX(published_sequence),0)+1 FROM incoming_items) WHERE delivery_token=? RETURNING delivery_token",
      [item.canonicalItem, item.canonicalGuidance ?? null, item.deliveryToken],
    );
    if (published.length === 0) {
      return yield* new EndpointStoreError({ reason: "not-found" });
    }
    return "inserted";
  }).pipe(storeOperation, Effect.withSpan("putInboxItem"));
}
/** Acknowledgment preserves the immutable host binding after the host accepts it. */
export function acknowledgeInboxItem(
  deliveryToken: DeliveryToken,
): Effect.Effect<void, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    const [retained] = yield* database.unsafe<
      Readonly<Record<string, unknown>>
    >(
      "UPDATE incoming_items SET acknowledged=1 WHERE delivery_token=? AND published_item IS NOT NULL RETURNING delivery_token",
      [deliveryToken],
    );
    if (retained === undefined) {
      return yield* new EndpointStoreError({ reason: "not-found" });
    }
  }).pipe(storeOperation, Effect.withSpan("acknowledgeInboxItem"));
}
/** Receipt retirement and callback progress commit together across crashes. */
export function completeWebhookDelivery(
  deliveryToken: DeliveryToken,
  canonicalState: Uint8Array,
): Effect.Effect<void, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    yield* transaction(
      Effect.gen(function* () {
        yield* acknowledgeInboxItem(deliveryToken);
        yield* writeEventState(canonicalState);
      }),
    );
  }).pipe(storeOperation, Effect.withSpan("completeWebhookDelivery"));
}
/**
 * Read an inbox page without allowing concurrent arrivals to extend its snapshot.
 * @param input Sequence bounds and maximum item count.
 * @returns Pending items and an optional next position within the same snapshot.
 */
export function readInbox(
  input: InboxBounds = {},
): Effect.Effect<InboxPage, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    const after = requireSequence(input.after ?? 0);
    const through = requireSequence(input.through ?? (yield* newestSequence()));
    const limit = pageLimit(input, after, through);
    const rows = yield* database.unsafe<Readonly<Record<string, unknown>>>(
      "SELECT published_sequence AS sequence, delivery_token, published_item AS canonical_item, canonical_guidance FROM incoming_items WHERE acknowledged=0 AND published_item IS NOT NULL AND published_sequence>? AND published_sequence<=? ORDER BY published_sequence LIMIT ?",
      [after, through, limit + 1],
    );
    const items = yield* Effect.forEach(
      collectPage(rows, limit),
      (entry) =>
        decodeItemProjection(entry).pipe(
          Effect.map((view) => ({ ...entry, ...view })),
        ),
      { concurrency: 1 },
    );
    const last = items.at(-1);
    return {
      items,
      through,
      ...(last !== undefined && rows.length > items.length
        ? { nextAfter: last.sequence }
        : {}),
    };
  }).pipe(storeOperation, Effect.withSpan("readInbox"));
}
/**
 * Read counters for notification coalescing without disclosing inbox contents.
 * @returns Current unread count and monotonic insertion position.
 */
export function readInboxSummary(): Effect.Effect<
  InboxSummary,
  EndpointStoreError,
  SqlClient.SqlClient
> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    const row = (yield* database.unsafe<Readonly<Record<string, unknown>>>(
      "SELECT COUNT(*) AS pending FROM incoming_items WHERE acknowledged=0 AND published_item IS NOT NULL",
      [],
    ))[0];
    if (row === undefined) {
      return yield* new EndpointStoreError({ reason: "corrupt" });
    }
    return {
      pendingCount: readInteger(row, "pending"),
      newestSequence: yield* newestSequence(),
    };
  }).pipe(storeOperation, Effect.withSpan("readInboxSummary"));
}
/**
 * Read the runtime-owned event state without interpreting its private format.
 * @returns Persisted registration and callback delivery state, when present.
 */
export function readEventState(): Effect.Effect<
  Uint8Array | undefined,
  EndpointStoreError,
  SqlClient.SqlClient
> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    const row = (yield* database.unsafe<Readonly<Record<string, unknown>>>(
      "SELECT canonical_state FROM runtime_events WHERE singleton = 1",
      [],
    ))[0];
    return row === undefined ? undefined : readBytes(row, "canonical_state");
  }).pipe(storeOperation, Effect.withSpan("readEventState"));
}
/**
 * Atomically replace the one runtime consumer's durable event state.
 * @param canonicalState Runtime-validated registration and delivery state.
 */
export function writeEventState(
  canonicalState: Uint8Array,
): Effect.Effect<void, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    requireBytes(canonicalState);
    yield* database.unsafe<Readonly<Record<string, unknown>>>(
      "INSERT INTO runtime_events (singleton, canonical_state) VALUES (1, ?) ON CONFLICT(singleton) DO UPDATE SET canonical_state = excluded.canonical_state",
      [canonicalState],
    );
  }).pipe(storeOperation, Effect.withSpan("writeEventState"));
}
/** Reply authority follows text that the host has actually been shown. */
export function isMessageVisible(message: MessageReference) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const row = (yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      "SELECT admitted FROM message_visibility WHERE author_agent_id=? AND msg_id=?",
      [message.authorAgentId, message.msgId],
    ))[0];
    return row !== undefined && readInteger(row, "admitted") === 1;
  }).pipe(storeOperation, Effect.withSpan("isMessageVisible"));
}
/** Saved host projections remain available after acknowledgment and later assessments. */
export function readHistoryViews(
  messages: readonly MessageReference[],
): Effect.Effect<
  readonly HistoryView[],
  EndpointStoreError,
  SqlClient.SqlClient
> {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const views: HistoryView[] = [];
    const retained = new Map<DeliveryToken, Omit<HistoryView, "message">>();
    for (const message of messages) {
      const row = (yield* sql.unsafe<Readonly<Record<string, unknown>>>(
        "SELECT i.delivery_token,i.published_item AS canonical_item,i.canonical_guidance FROM message_visibility v JOIN incoming_items i USING(delivery_token) WHERE v.author_agent_id=? AND v.msg_id=? AND i.published_item IS NOT NULL",
        [message.authorAgentId, message.msgId],
      ))[0];
      if (row !== undefined) {
        const deliveryToken = yield* Schema.decodeUnknown(DeliveryToken)(
          readText(row, "delivery_token"),
        );
        let view = retained.get(deliveryToken);
        if (view === undefined) {
          const projection = {
            canonicalItem: readBytes(row, "canonical_item"),
            ...guidanceBytes(row),
          };
          view = {
            deliveryToken,
            ...(yield* decodeItemProjection(projection)),
          };
          retained.set(deliveryToken, view);
        }
        views.push({ message, ...view });
      }
    }
    return views;
  }).pipe(storeOperation, Effect.withSpan("readHistoryViews"));
}
/** Retained projections share one item and guidance decoder across delivery and history. */
export function decodeItemProjection(
  projection: Pick<ItemProjection, "canonicalItem" | "canonicalGuidance">,
): Effect.Effect<RetainedItem, EndpointStoreError> {
  return Effect.gen(function* () {
    return {
      item: yield* decodeRuntimeValue(InboundItem, projection.canonicalItem),
      ...(projection.canonicalGuidance === undefined
        ? {}
        : {
            guidance: yield* decodeRuntimeValue(
              localGuidanceSchema,
              projection.canonicalGuidance,
            ),
          }),
    };
  }).pipe(Effect.withSpan("decodeItemProjection"));
}

function requireSequence(value: number): number {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new StoreSignal("invalid-continuation");
  }
  return value;
}
function newestSequence(): Effect.Effect<
  number,
  EndpointStoreError,
  SqlClient.SqlClient
> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    const row = (yield* database.unsafe<Readonly<Record<string, unknown>>>(
      "SELECT COALESCE(MAX(published_sequence), 0) AS newest FROM incoming_items",
      [],
    ))[0];
    if (row === undefined) {
      return yield* new EndpointStoreError({ reason: "corrupt" });
    }
    return readInteger(row, "newest");
  }).pipe(storeOperation);
}
function pageLimit(input: InboxBounds, after: number, through: number): number {
  const limit = input.limit ?? MAXIMUM_PAGE_ITEMS;
  if (
    !Number.isInteger(limit) ||
    limit < 1 ||
    limit > MAXIMUM_PAGE_ITEMS ||
    after > through
  ) {
    throw new StoreSignal("invalid-continuation");
  }
  return limit;
}
function collectPage(
  rows: ReadonlyArray<Readonly<Record<string, unknown>>>,
  limit: number,
): ReadonlyArray<Omit<InboxEntry, "item" | "guidance">> {
  const items: Array<Omit<InboxEntry, "item" | "guidance">> = [];
  let bytes = 0;
  for (const row of rows) {
    const canonicalItem = readBytes(row, "canonical_item");
    const guidance = guidanceBytes(row);
    const itemBytes =
      canonicalItem.byteLength + (guidance.canonicalGuidance?.byteLength ?? 0);
    if (
      items.length === limit ||
      (items.length > 0 && bytes + itemBytes > MAXIMUM_RUNTIME_ITEM_BYTES)
    ) {
      break;
    }
    items.push({
      sequence: readInteger(row, "sequence"),
      deliveryToken: Schema.decodeUnknownSync(DeliveryToken)(
        readText(row, "delivery_token"),
      ),
      canonicalItem,
      ...guidance,
    });
    bytes += itemBytes;
  }
  return items;
}
function guidanceBytes(
  row: Readonly<Record<string, unknown>>,
): Pick<InboxEntry, "canonicalGuidance"> {
  const value = readOptionalBytes(row, "canonical_guidance");
  return value === undefined ? {} : { canonicalGuidance: value };
}
/** A saved permitted projection of the same authored message. */
export interface HistoryView extends RetainedItem {
  readonly deliveryToken: DeliveryToken;
  readonly message: MessageReference;
}
