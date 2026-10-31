/** @file Native and webhook reads share retained content, export deduplication and acknowledgment. */

import { SqlClient } from "@effect/sql";
import { scopedLive as it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { expect } from "vitest";
import type { HistoryExportRecord } from "../../src/daemon/inbox/host-traffic-log.js";
import {
  digest,
  testAgentView,
} from "../../src/__tests__/agent-card-fixtures.js";
import { collectiveOperationId } from "../../src/__tests__/collective-id-fixtures.js";
import {
  publishTestInboxItem as putInboxItem,
  stateDirectory,
  withDaemonStore,
} from "../../src/__tests__/store-schema-fixtures.js";
import {
  DeliveryToken,
  encodeRuntimeValue,
} from "../../src/communication/store/index.js";
import { makeInboxOperations } from "../../src/daemon/messaging/inbox.js";
import { InboundItem } from "../../src/host-protocol/items.js";

const token = Schema.decodeUnknownSync(DeliveryToken)(digest("dlv_", 9));
const item = Schema.decodeUnknownSync(InboundItem)({
  kind: "operationFailed",
  operationId: collectiveOperationId(testAgentView("agent:alice").agentId),
  to: "agent:bob",
  error: "request unavailable",
});

function inboxFixture() {
  return Effect.gen(function* () {
    const records: HistoryExportRecord[] = [];
    const sql = yield* SqlClient.SqlClient;
    yield* putInboxItem({
      deliveryToken: token,
      canonicalItem: yield* encodeRuntimeValue(item),
    });
    const inbox = yield* makeInboxOperations(sql, {
      record: (record) =>
        Effect.sync(() => {
          records.push(record);
        }),
    });
    return { inbox, records, sql };
  });
}

it("exports a retained item once across webhook and native reads and keeps its acknowledged content", () =>
  withDaemonStore(stateDirectory(), () =>
    Effect.gen(function* () {
      const { inbox, records } = yield* inboxFixture();
      yield* inbox.events.readInbox({ limit: 1 });
      expect((yield* inbox.listInbox({})).items).toEqual([
        { deliveryToken: token, item },
      ]);
      yield* inbox.events.readInbox({ limit: 1 });
      expect(records).toMatchObject([{ kind: "inbound", item }]);
      yield* inbox.acknowledgeDelivery(token);
      expect((yield* inbox.listInbox({})).items).toEqual([]);
      expect(yield* inbox.getInboxItem(token)).toMatchObject({ item });
    }),
  ));

it("keeps an unread item when acknowledgment cannot be persisted", () =>
  withDaemonStore(stateDirectory(), () =>
    Effect.gen(function* () {
      const { inbox, sql } = yield* inboxFixture();
      yield* sql.unsafe(
        "CREATE TRIGGER fail_receipt BEFORE UPDATE ON incoming_items BEGIN SELECT RAISE(ABORT, 'receipt write failed'); END",
      );
      const failure = yield* inbox.acknowledgeDelivery(token).pipe(Effect.flip);
      // eslint-disable-next-line agent-code-guard/no-hardcoded-assertion-literals -- The closed acknowledgment failure tells callers to retry retained delivery.
      expect(failure.reason).toBe("persistence-failed");
      expect((yield* inbox.listInbox({})).items).toEqual([
        { deliveryToken: token, item },
      ]);
    }),
  ));
