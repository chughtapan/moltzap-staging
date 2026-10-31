/** @file A real daemon inbox with scoped SQL and helpers for adding test messages. */

import { SqlClient } from "@effect/sql";
import { Effect, Layer } from "effect";
import type { DeliveryOperations } from "../host-protocol/operations.js";
import { noHistoryExport } from "../daemon/inbox/host-traffic-log.js";
import { queueIncomingItem } from "../daemon/inbox/pending.js";
import { acknowledgeInboxItem } from "../daemon/inbox/store.js";
import { makeInboxOperations } from "../daemon/messaging/inbox.js";
import { layer } from "../daemon/store.js";
import { LIVE_WIRE_VERSIONS } from "../identity/wire-version.js";
import { publishTestInboxItem as putInboxItem } from "./store-schema-fixtures.js";

/** Test events use the production inbox and receiver callbacks over one database. */
export const openInboxFixture = Effect.fn("openInboxFixture")(function* (
  stateDirectory: string,
) {
  const persistence = yield* Layer.build(
    layer({
      stateDirectory,
      liveWireVersions: LIVE_WIRE_VERSIONS,
    }),
  );
  const sql = yield* SqlClient.SqlClient.pipe(Effect.provide(persistence));
  const run = Effect.provideService(SqlClient.SqlClient, sql);
  const inbox = yield* makeInboxOperations(sql, noHistoryExport);
  return {
    ...inbox.events,
    operations: {
      readInbox: inbox.listInbox,
      readEvent: ({ eventId }) => inbox.getInboxItem(eventId),
      acknowledgeDelivery: inbox.acknowledgeDelivery,
    } satisfies Pick<
      DeliveryOperations,
      "readInbox" | "readEvent" | "acknowledgeDelivery"
    >,
    sql,
    run,
    putInboxItem: (item: Parameters<typeof putInboxItem>[0]) =>
      putInboxItem(item).pipe(run),
    acknowledgeInboxItem: (token: Parameters<typeof acknowledgeInboxItem>[0]) =>
      acknowledgeInboxItem(token).pipe(run),
    queueIncomingItem: (item: Parameters<typeof queueIncomingItem>[0]) =>
      queueIncomingItem(item).pipe(run),
  };
});
