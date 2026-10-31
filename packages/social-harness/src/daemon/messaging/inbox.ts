/** @file Reads retained host items and shares acknowledgment state with MCP delivery. */

import { SqlClient } from "@effect/sql";
import { DateTime, Effect } from "effect";
import type { DeliveryToken } from "../../communication/store/types.js";
import type { InboundItem } from "../../host-protocol/items.js";
import type { DeliveryOperations } from "../../host-protocol/operations.js";
import type { HistoryExportPort } from "../inbox/host-traffic-log.js";
import { DeliveryAcknowledgeError } from "../../communication/conversations/errors.js";
import { readRuntimeEvent, readRuntimeInbox } from "../inbox/inbox.js";
import {
  acknowledgeInboxItem,
  completeWebhookDelivery,
  type EventStore,
  readEventState,
  readInbox,
  readInboxItem,
  readInboxSummary,
  writeEventState,
} from "../inbox/store.js";

/** Exporting once is process-local; content and acknowledgment remain durable. */
export const makeInboxOperations = Effect.fn("makeInboxOperations")(function* (
  sql: SqlClient.SqlClient,
  historyExport: HistoryExportPort,
) {
  const record = yield* historyRecorder(historyExport);
  const provideSql = Effect.provideService(SqlClient.SqlClient, sql);
  const listInbox: DeliveryOperations["readInbox"] = (request) =>
    readRuntimeInbox(request).pipe(
      Effect.tap((page) =>
        Effect.forEach(
          page.items,
          (entry) => record(entry.deliveryToken, entry.item),
          { concurrency: 1, discard: true },
        ),
      ),
      provideSql,
    );
  return {
    listInbox,
    getInboxItem: (token: DeliveryToken) =>
      readRuntimeEvent(token).pipe(provideSql),
    acknowledgeDelivery: (token: DeliveryToken) =>
      acknowledgeDelivery(token).pipe(provideSql),
    events: eventOperations(sql, record),
  };
});

function historyRecorder(historyExport: HistoryExportPort) {
  return Effect.gen(function* () {
    const gate = yield* Effect.makeSemaphore(1);
    const exported = new Set<DeliveryToken>();
    return (token: DeliveryToken, item: InboundItem) =>
      gate.withPermits(1)(
        Effect.gen(function* () {
          if (exported.has(token)) {
            return;
          }
          yield* historyExport.record({
            kind: "inbound",
            item,
            at: yield* DateTime.now,
          });
          exported.add(token);
        }),
      );
  });
}

function eventOperations(
  sql: SqlClient.SqlClient,
  record: Effect.Effect.Success<ReturnType<typeof historyRecorder>>,
): EventStore {
  const provideSql = Effect.provideService(SqlClient.SqlClient, sql);
  return {
    readEventState: () => readEventState().pipe(provideSql),
    writeEventState: (bytes) => writeEventState(bytes).pipe(provideSql),
    readInboxItem: (token) => readInboxItem(token).pipe(provideSql),
    readInboxSummary: () => readInboxSummary().pipe(provideSql),
    completeWebhookDelivery: (token, bytes) =>
      completeWebhookDelivery(token, bytes).pipe(provideSql),
    readInbox: (bounds) =>
      readInbox(bounds).pipe(
        Effect.tap((page) =>
          Effect.forEach(
            page.items,
            (entry) => record(entry.deliveryToken, entry.item),
            { concurrency: 1, discard: true },
          ),
        ),
        provideSql,
      ),
  };
}

function acknowledgeDelivery(token: DeliveryToken) {
  return acknowledgeInboxItem(token).pipe(
    Effect.catchTag(
      "EndpointStoreError",
      (error) =>
        new DeliveryAcknowledgeError({
          reason:
            error.reason === "not-found"
              ? "unknown-delivery"
              : "persistence-failed",
        }),
    ),
  );
}
