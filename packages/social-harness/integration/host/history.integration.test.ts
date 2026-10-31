/** @file Host history retains permitted projections across acknowledgment and restart. */

import { SqlClient } from "@effect/sql";
import { scopedLive as it } from "@effect/vitest";
import { Array, Effect, Schema } from "effect";
import { expect } from "vitest";
import {
  digest,
  testAgentView,
} from "../../src/__tests__/agent-card-fixtures.js";
import { collectiveOperationId } from "../../src/__tests__/collective-id-fixtures.js";
import { mcpIdentity } from "../../src/__tests__/mcp-http-fixtures.js";
import { localGuidance } from "../../src/__tests__/message-policy-fixtures.js";
import {
  publishTestInboxItem as putInboxItem,
  stateDirectory,
  withDaemonStore,
} from "../../src/__tests__/store-schema-fixtures.js";
import { Communication } from "../../src/communication/index.js";
import { Message } from "../../src/communication/message.js";
import { AgentAddress } from "../../src/communication/records/values.js";
import { transaction } from "../../src/communication/store/database/index.js";
import {
  DeliveryToken,
  encodeRuntimeValue,
} from "../../src/communication/store/index.js";
import {
  queueIncomingItem,
  settleQueuedItem,
} from "../../src/daemon/inbox/pending.js";
import { acknowledgeInboxItem } from "../../src/daemon/inbox/store.js";
import { makeHistoryOperations } from "../../src/daemon/messaging/history.js";
import { managementReadConversationRequestSchema } from "../../src/host-protocol/index.js";
import { InboundItem } from "../../src/host-protocol/items.js";

const alice = testAgentView("agent:alice");
const bob = testAgentView("agent:bob");
const peerMessage = Schema.decodeUnknownSync(Message)({
  msgId: digest("msg_", 1),
  author: bob,
  recipients: [alice],
  content: [{ type: "text", text: "Permitted peer content" }],
});
const hiddenMessage = Schema.decodeUnknownSync(Message)({
  ...peerMessage,
  msgId: digest("msg_", 2),
  content: [{ type: "text", text: "Unchecked peer content" }],
});
const ownMessage = Schema.decodeUnknownSync(Message)({
  msgId: digest("msg_", 3),
  author: alice,
  recipients: [bob],
  content: [{ type: "text", text: "Alice's reply" }],
});
const reference = { authorAgentId: bob.agentId, msgId: peerMessage.msgId };
const deliveryToken = Schema.decodeUnknownSync(DeliveryToken)(
  digest("dlv_", 1),
);
const guidance = localGuidance(9);
const historyRequest = Schema.decodeUnknownSync(
  managementReadConversationRequestSchema,
)({ address: "agent:bob" });
const unused = () =>
  Effect.dieMessage("History does not submit or consume messages");
const communication = Communication.of({
  send: unused,
  waitForMessage: unused,
  consume: unused,
  listConversations: () =>
    Effect.succeed({
      conversations: [
        { conversationId: "alice-and-bob", members: [alice, bob] },
      ],
      hasMore: false,
    }),
  readHistory: () =>
    Effect.succeed({
      messages: [peerMessage, hiddenMessage, ownMessage],
      continuation: null,
    }),
});

function retainView() {
  return transaction(
    Effect.gen(function* () {
      const canonicalItem = yield* encodeRuntimeValue({
        kind: "multicast",
        message: peerMessage,
      });
      yield* queueIncomingItem({
        deliveryToken,
        canonicalItem,
        conversation: "agent:bob",
        message: reference,
      });
      yield* putInboxItem({
        deliveryToken,
        canonicalItem,
        canonicalGuidance: yield* encodeRuntimeValue(guidance),
      });
      yield* settleQueuedItem(deliveryToken, "admitted");
      yield* acknowledgeInboxItem(deliveryToken);
    }),
  );
}

// @agent-code-guard/regression-only: Exercises SQLite restart, fixed history heads and the 50-address page boundary.
// eslint-disable-next-line sonarjs/assertions-in-tests -- The reopened SQL fixture performs the retained-view assertions in checkRetainedHistory.
it("retains acknowledged permitted views after restart without exposing unchecked peer messages", () => {
  const directory = stateDirectory();
  return Effect.gen(function* () {
    yield* withDaemonStore(directory, retainView);
    yield* withDaemonStore(directory, checkRetainedHistory);
  });
});

it("requires registration before listing or reading conversation history", () =>
  withDaemonStore(stateDirectory(), () =>
    Effect.gen(function* () {
      const history = makeHistoryOperations(
        mcpIdentity(false),
        communication,
        yield* SqlClient.SqlClient,
      );
      expect(
        yield* history.readHistory(historyRequest).pipe(Effect.flip),
      ).toMatchObject({ reason: "not-registered" });
      expect(
        yield* history.listConversations({}).pipe(Effect.flip),
      ).toMatchObject({ reason: "not-registered" });
    }),
  ));

function checkRetainedHistory() {
  return Effect.gen(function* () {
    const history = makeHistoryOperations(
      mcpIdentity(),
      communication,
      yield* SqlClient.SqlClient,
    );
    const page = yield* history.readHistory(historyRequest);
    expect(page.records).toEqual([
      {
        kind: "received",
        message: reference,
        item: { kind: "multicast", message: peerMessage },
        guidance,
      },
      { kind: "sent", message: ownMessage },
    ]);
    expect(yield* history.listConversations({})).toEqual({
      kind: "page",
      addresses: ["agent:bob"],
      hasMore: false,
    });
  });
}

it("retains acknowledged result-only history after restart within its original page boundary", () => {
  const directory = stateDirectory();
  return Effect.gen(function* () {
    const continuation = yield* withDaemonStore(directory, firstResultPage);
    yield* withDaemonStore(directory, () => remainingResultPage(continuation));
  });
});

const localOnly = Communication.of({
  ...communication,
  listConversations: () =>
    Effect.succeed({ conversations: [], hasMore: false }),
  readHistory: unused,
});

function saveLocalResult(byte: number, address = "agent:bob") {
  return Effect.gen(function* () {
    const token = Schema.decodeUnknownSync(DeliveryToken)(digest("dlv_", byte));
    const item = yield* Schema.decodeUnknown(InboundItem)({
      kind: "collectiveResult",
      op: "gather",
      operationId: collectiveOperationId(alice.agentId, byte),
      to: address,
      closedBy: "deadline",
      closedAt: Date.now(),
      question: `Question ${byte}`,
      outcomes: [
        { member: testAgentView(address), outcome: { kind: "no-answer" } },
      ],
    });
    const canonicalItem = yield* encodeRuntimeValue(item);
    yield* queueIncomingItem({
      deliveryToken: token,
      conversation: address,
      canonicalItem,
      canonicalSources: yield* encodeRuntimeValue({ answers: [] }),
    });
    yield* putInboxItem({ deliveryToken: token, canonicalItem });
    yield* settleQueuedItem(token, "admitted");
    return { kind: "result" as const, deliveryToken: token, item };
  });
}

it("pages the merged result and message addresses without repeating shared conversations", () =>
  withDaemonStore(stateDirectory(), () =>
    Effect.gen(function* () {
      const addresses = Array.range(0, 50).map(
        (index) => `agent:peer-${String(index).padStart(3, "0")}`,
      );
      yield* Effect.forEach(
        addresses,
        (address, index) => saveLocalResult(index + 1, address),
        {
          concurrency: 1,
          discard: true,
        },
      );
      const combined = Communication.of({
        ...localOnly,
        listConversations: () =>
          Effect.succeed({
            conversations: [
              {
                conversationId: "shared",
                members: [alice, testAgentView("agent:peer-000")],
              },
              {
                conversationId: "messages-only",
                members: [alice, testAgentView("agent:peer-999")],
              },
            ],
            hasMore: false,
          }),
      });
      const history = makeHistoryOperations(
        mcpIdentity(),
        combined,
        yield* SqlClient.SqlClient,
      );
      expect(yield* history.listConversations({})).toEqual({
        kind: "page",
        addresses: addresses.slice(0, 50),
        hasMore: true,
      });
      expect(
        yield* history.listConversations({
          afterAddress: AgentAddress.make("agent:peer-049"),
        }),
      ).toEqual({
        kind: "page",
        addresses: ["agent:peer-050", "agent:peer-999"],
        hasMore: false,
      });
    }),
  ));

function firstResultPage() {
  return Effect.gen(function* () {
    const saved = yield* Effect.forEach(
      Array.range(0, 50),
      (index) => saveLocalResult(index + 1),
      { concurrency: 1 },
    );
    const history = makeHistoryOperations(
      mcpIdentity(),
      localOnly,
      yield* SqlClient.SqlClient,
    );
    expect(yield* history.listConversations({})).toEqual({
      kind: "page",
      addresses: ["agent:bob"],
      hasMore: false,
    });
    const page = yield* history.readHistory(historyRequest);
    expect(page.records).toEqual(saved.slice(0, 50));
    const first = saved[0];
    if (first === undefined || page.continuation === null) {
      return yield* Effect.dieMessage("Expected a paginated result history");
    }
    yield* acknowledgeInboxItem(first.deliveryToken);
    return page.continuation;
  });
}

function remainingResultPage(continuation: string) {
  return Effect.gen(function* () {
    yield* saveLocalResult(52);
    const history = makeHistoryOperations(
      mcpIdentity(),
      localOnly,
      yield* SqlClient.SqlClient,
    );
    const page = yield* history.readHistory({ continuation });
    expect(page.records).toMatchObject([
      { kind: "result", deliveryToken: digest("dlv_", 51) },
    ]);
    expect(page.continuation).toBeNull();
    const restarted = yield* history.readHistory(historyRequest);
    expect(restarted.records[0]).toMatchObject({
      kind: "result",
      deliveryToken: digest("dlv_", 1),
    });
  });
}
