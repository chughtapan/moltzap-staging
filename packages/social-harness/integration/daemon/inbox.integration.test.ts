/** @file Classified delivery, stable token bindings, and acknowledgment durability. */

import { scopedLive as it } from "@effect/vitest";
import { Effect, Array as EffectArray, Schema } from "effect";
import { describe, expect } from "vitest";
import type { HarnessMessageReadyEvent } from "../../src/host-protocol/operations.js";
import {
  digest,
  testAgentView,
} from "../../src/__tests__/agent-card-fixtures.js";
import { collectiveOperationId } from "../../src/__tests__/collective-id-fixtures.js";
import { openTestDatabase } from "../../src/__tests__/communication-test-database.js";
import { localGuidance } from "../../src/__tests__/message-policy-fixtures.js";
import {
  publishTestInboxEvent,
  stateDirectory,
  withDaemonStore,
} from "../../src/__tests__/store-schema-fixtures.js";
import { Message } from "../../src/communication/message.js";
import {
  DeliveryToken,
  encodeRuntimeValue,
} from "../../src/communication/store/index.js";
import { noHistoryExport } from "../../src/daemon/inbox/host-traffic-log.js";
import { readRuntimeInbox } from "../../src/daemon/inbox/inbox.js";
import { makeInboxOperations } from "../../src/daemon/messaging/inbox.js";
import { InboundItem } from "../../src/host-protocol/items.js";

/* eslint-disable agent-code-guard/no-hardcoded-assertion-literals -- Durable tombstone and snapshot fixtures pin exact store outcomes. */

const token = (byte: number) =>
  Schema.decodeUnknownSync(DeliveryToken)(digest("dlv_", byte));
const failure = Schema.decodeUnknownSync(InboundItem)({
  kind: "operationFailed",
  operationId: collectiveOperationId(testAgentView("agent:alice").agentId, 1),
  to: "agent:bob",
  error: "request unavailable",
});
const request = Schema.decodeUnknownSync(InboundItem)({
  kind: "collectiveRequest",
  message: {
    msgId: digest("msg_", 2),
    author: testAgentView("agent:bob"),
    recipients: ["alice", "bob", "carol"]
      .map(testAgentView)
      .filter(
        (member) => member.agentId !== testAgentView("agent:bob").agentId,
      ),
    content: {
      op: "all_gather",
      operationId: collectiveOperationId(
        testAgentView("agent:alice").agentId,
        2,
      ),
      question: "Answer independently",
      requestedSchema: {
        type: "object",
        properties: { answer: { type: "string" } },
        required: ["answer"],
      },
      deadlineAt: 4_000_000_000_000,
    },
  },
});

const preservesTokenBindings = () => {
  const path = stateDirectory();
  return Effect.gen(function* () {
    yield* Effect.scoped(
      withDaemonStore(path, (store) =>
        Effect.gen(function* () {
          yield* store.putInboxItem({
            deliveryToken: token(1),
            canonicalItem: yield* encodeRuntimeValue(failure),
            canonicalGuidance: yield* encodeRuntimeValue(localGuidance(1)),
          });
          yield* store.acknowledgeInboxItem(token(1));
        }),
      ),
    );
    yield* Effect.scoped(
      withDaemonStore(path, (store) =>
        Effect.gen(function* () {
          yield* store.acknowledgeInboxItem(token(1));
          yield* publishTestInboxEvent({
            deliveryToken: token(1),
            item: failure,
          });
          expect((yield* readRuntimeInbox({})).items).toEqual([]);
          const changed = yield* publishTestInboxEvent({
            deliveryToken: token(1),
            item: request,
          }).pipe(Effect.flip);
          expect(changed.reason).toBe("conflict");
        }),
      ),
    );
  });
};

const freezesPagesAcrossArrivalsAndAcknowledgments = () =>
  withDaemonStore(stateDirectory(), (store) =>
    Effect.gen(function* () {
      yield* Effect.forEach(
        EffectArray.range(1, 51),
        (index) =>
          publishTestInboxEvent({
            deliveryToken: token(index),
            item: failure,
          }),
        { concurrency: 1, discard: true },
      );
      const first = yield* readRuntimeInbox({});
      expect(first.items).toHaveLength(50);
      expect(first.nextCursor).toBeDefined();
      yield* publishTestInboxEvent({
        deliveryToken: token(52),
        item: failure,
      });
      yield* store.acknowledgeInboxItem(token(1));
      const second = yield* readRuntimeInbox({
        cursor: first.nextCursor,
      });
      expect(second.items.map((item) => item.deliveryToken)).toEqual([
        token(51),
      ]);
      expect(second.nextCursor).toBeUndefined();
      expect((yield* store.readInboxSummary()).pendingCount).toBe(51);
      const invalid = yield* readRuntimeInbox({
        cursor: "invalid",
      }).pipe(Effect.flip);
      expect(invalid.reason).toBe("invalid-continuation");
    }),
  );

const commitsReceiptAtomically = () => {
  const path = stateDirectory();
  const state = new TextEncoder().encode('{"registration":null}');
  return Effect.gen(function* () {
    yield* Effect.scoped(
      withDaemonStore(path, (store) =>
        Effect.gen(function* () {
          yield* publishTestInboxEvent({
            deliveryToken: token(1),
            item: failure,
          });
          yield* store.writeEventState(state);
          const error = yield* store
            .completeWebhookDelivery(token(1), new Uint8Array())
            .pipe(Effect.flip);
          expect(error.reason).toBe("invalid-input");
          expect((yield* store.readInboxSummary()).pendingCount).toBe(1);
          expect(yield* store.readEventState()).toEqual(state);
          const next = new TextEncoder().encode(
            '{"registration":{"outbox":null}}',
          );
          yield* store.completeWebhookDelivery(token(1), next);
          expect((yield* store.readInboxSummary()).pendingCount).toBe(0);
          expect(yield* store.readEventState()).toEqual(next);
        }),
      ),
    );
    yield* Effect.scoped(
      withDaemonStore(path, (store) =>
        Effect.gen(function* () {
          expect((yield* store.readInboxItem(token(1)))?.acknowledged).toBe(
            true,
          );
          expect((yield* store.readInboxSummary()).pendingCount).toBe(0);
        }),
      ),
    );
  });
};

it("retains an admitted envelope and guidance after acknowledgment and a store restart", () => {
  const path = stateDirectory();
  const message = Schema.decodeUnknownSync(Message)({
    msgId: digest("msg_", 1),
    author: testAgentView("agent:bob"),
    recipients: [testAgentView("agent:alice")],
    content: [{ type: "text", text: "Retained content" }],
  });
  const item: InboundItem = { kind: "multicast", message };
  const event: HarnessMessageReadyEvent = {
    deliveryToken: token(1),
    item,
    guidance: localGuidance(1),
  };
  return Effect.gen(function* () {
    const canonicalItem = yield* encodeRuntimeValue(item);
    const canonicalGuidance = yield* encodeRuntimeValue(event.guidance);
    yield* withDaemonStore(path, (store) =>
      store.putInboxItem({
        deliveryToken: event.deliveryToken,
        canonicalItem,
        canonicalGuidance,
      }),
    );
    const database = yield* openTestDatabase(path);
    const operations = yield* makeInboxOperations(
      database.sql,
      noHistoryExport,
    );
    expect(yield* operations.listInbox({})).toEqual({ items: [event] });
    yield* operations.acknowledgeDelivery(event.deliveryToken);
    expect(yield* operations.listInbox({})).toEqual({ items: [] });
    expect(yield* operations.getInboxItem(event.deliveryToken)).toEqual({
      item,
      guidance: event.guidance,
    });
  });
});

describe("durable runtime inbox", () => {
  it(
    "rolls back a failed receipt commit and retains a successful retirement across reopen",
    commitsReceiptAtomically,
  );
  it(
    "retains acknowledgment tombstones and rejects token payload collisions",
    preservesTokenBindings,
  );
  it(
    "bounds a snapshot while new arrivals and acknowledgments change unread state",
    freezesPagesAcrossArrivalsAndAcknowledgments,
  );
});

/* eslint-enable agent-code-guard/no-hardcoded-assertion-literals -- Restore repository defaults. */
