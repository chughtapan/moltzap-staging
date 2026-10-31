/** @file Incoming replay keeps one durable host identity and preserves already presented history. */

import { scopedLive as it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { expect } from "vitest";
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
import { Message } from "../../src/communication/message.js";
import {
  EndpointStoreError,
  transaction,
} from "../../src/communication/store/database/index.js";
import {
  DeliveryToken,
  encodeRuntimeValue,
} from "../../src/communication/store/index.js";
import {
  findIncomingMessage,
  markMessageVisibility,
  queueIncomingItem,
  settleQueuedItem,
} from "../../src/daemon/inbox/pending.js";
import {
  acknowledgeInboxItem,
  isMessageVisible,
  readHistoryViews,
} from "../../src/daemon/inbox/store.js";
import { MAXIMUM_RUNTIME_ITEM_BYTES } from "../../src/encoding/limits.js";
import { InboundItem } from "../../src/host-protocol/items.js";

const message = Schema.decodeUnknownSync(Message)({
  msgId: digest("msg_", 1),
  author: testAgentView("agent:bob"),
  recipients: [testAgentView("agent:alice")],
  content: [{ type: "text", text: "A retained message" }],
});
const reference = {
  authorAgentId: message.author.agentId,
  msgId: message.msgId,
};
const deliveryToken = Schema.decodeUnknownSync(DeliveryToken)(
  digest("dlv_", 1),
);
const laterToken = Schema.decodeUnknownSync(DeliveryToken)(digest("dlv_", 2));
const incoming = Schema.decodeUnknown(InboundItem)({
  kind: "multicast",
  message,
}).pipe(
  Effect.flatMap(encodeRuntimeValue),
  Effect.map((canonicalItem) => ({
    deliveryToken,
    conversation: "agent:bob",
    canonicalItem,
    message: reference,
  })),
);

// @agent-code-guard/regression-only: Finite SQL transitions cover replay, acknowledgment, independent byte budgets, and preserved visibility.
it("reuses the incoming identity after host acknowledgment and restart", () => {
  const directory = stateDirectory();
  return Effect.gen(function* () {
    const item = yield* incoming;
    yield* withDaemonStore(directory, () => retainItem(item));
    const retained = yield* withDaemonStore(directory, () =>
      queueIncomingItem(item).pipe(
        Effect.zipRight(findIncomingMessage(reference)),
      ),
    );
    expect(retained).toMatchObject({
      deliveryToken,
      message: reference,
      canonicalItem: item.canonicalItem,
    });
  });
});

it("keeps an already presented history view when another result withholds its source", () =>
  withDaemonStore(stateDirectory(), () =>
    Effect.gen(function* () {
      const item = yield* incoming;
      yield* retainItem(item);
      yield* markMessageVisibility(reference, false, laterToken);
      const views = yield* readHistoryViews([reference]);
      expect(views).toMatchObject([
        { deliveryToken, item: { kind: "multicast", message } },
      ]);
    }),
  ));

it("refuses different content under the same incoming message identity", () =>
  withDaemonStore(stateDirectory(), () =>
    Effect.gen(function* () {
      const item = yield* incoming;
      yield* queueIncomingItem(item);
      const changed = yield* Schema.decodeUnknown(InboundItem)({
        kind: "multicast",
        message: {
          ...message,
          content: [{ type: "text", text: "Changed message" }],
        },
      }).pipe(Effect.flatMap(encodeRuntimeValue));
      expect(
        yield* queueIncomingItem({ ...item, canonicalItem: changed }).pipe(
          Effect.flip,
        ),
      ).toEqual(new EndpointStoreError({ reason: "conflict" }));
      expect((yield* findIncomingMessage(reference))?.canonicalItem).toEqual(
        item.canonicalItem,
      );
    }),
  ));

it("retains public item bytes and private source bytes under independent budgets", () =>
  withDaemonStore(stateDirectory(), () =>
    Effect.gen(function* () {
      const item = yield* incoming;
      const large = yield* encodeRuntimeValue({
        text: "x".repeat(MAXIMUM_RUNTIME_ITEM_BYTES / 2 + 100),
      });
      yield* queueIncomingItem({
        ...item,
        canonicalItem: large,
        canonicalSources: large,
      });
      const retained = yield* findIncomingMessage(reference);
      expect(retained?.canonicalItem.byteLength).toBe(large.byteLength);
      expect(retained?.canonicalSources?.byteLength).toBe(large.byteLength);
    }),
  ));

it("retains a public invalid-answer result without granting visibility to the original answer", () =>
  withDaemonStore(stateDirectory(), () =>
    Effect.gen(function* () {
      const result = yield* Schema.decodeUnknown(InboundItem)({
        kind: "collectiveResult",
        op: "gather",
        operationId: collectiveOperationId(
          testAgentView("agent:alice").agentId,
        ),
        closedBy: "settled",
        closedAt: Date.now(),
        to: "agent:bob",
        question: "Which day?",
        outcomes: [
          {
            member: message.author,
            outcome: { kind: "invalid", reason: "Expected a day" },
          },
        ],
      });
      const canonicalItem = yield* encodeRuntimeValue(result);
      yield* putInboxItem({ deliveryToken: laterToken, canonicalItem });
      yield* markMessageVisibility(reference, false, laterToken);
      const [view] = yield* readHistoryViews([reference]);
      expect(view?.deliveryToken).toBe(laterToken);
      expect(view?.item).toEqual(result);
      expect(yield* isMessageVisible(reference)).toBe(false);
    }),
  ));

function retainItem(item: Effect.Effect.Success<typeof incoming>) {
  return transaction(
    Effect.gen(function* () {
      yield* queueIncomingItem(item);
      yield* putInboxItem(item);
      yield* settleQueuedItem(deliveryToken, "admitted");
      yield* acknowledgeInboxItem(deliveryToken);
    }),
  );
}
