/** @file A policy decision and its host view commit together and survive acknowledgment and restart. */

import { live as it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { expect } from "vitest";
import type { firewallDecisionSummarySchema } from "../firewall/config.js";
import type { FirewallCompletion } from "../firewall/store/index.js";
import { digest, testAgentView } from "../__tests__/agent-card-fixtures.js";
import { localGuidance } from "../__tests__/message-policy-fixtures.js";
import {
  bytes,
  stateDirectory,
  type TestStore,
  withDaemonStore,
} from "../__tests__/store-schema-fixtures.js";
import {
  DeliveryToken,
  EndpointStoreError,
} from "../communication/store/index.js";
import { queueIncomingItem, readNextItemsToCheck } from "./inbox/pending.js";

const item = {
  deliveryToken: Schema.decodeUnknownSync(DeliveryToken)(digest("dlv_", 1)),
  canonicalItem: bytes(
    JSON.stringify({
      kind: "multicast",
      message: {
        msgId: digest("msg_", 1),
        author: testAgentView("agent:bob"),
        recipients: [testAgentView("agent:alice")],
        content: [{ type: "text", text: "Retained host item" }],
      },
    }),
  ),
  canonicalGuidance: bytes(JSON.stringify(localGuidance(1))),
};
const summary: typeof firewallDecisionSummarySchema.Type = {
  direction: "incoming",
  peer: testAgentView("agent:bob"),
  outcome: "allow",
  cause: "default",
  decidedBy: [],
  checkedRuleIds: [],
  activeRevision: "retained-policy",
};
const completion: FirewallCompletion = {
  checkId: "completed-check",
  direction: "incoming",
  canonicalCandidate: item.canonicalItem,
  canonicalInput: bytes("captured policy and evidence"),
  canonicalResult: bytes(JSON.stringify({ summaries: [summary] })),
  admit: true,
};

it("rolls back failed publication and retains the successful decision and view after reopen", () => {
  const directory = stateDirectory();
  return Effect.gen(function* () {
    yield* withDaemonStore(directory, publish);
    expect(yield* withDaemonStore(directory, expectRetained)).toMatchObject({
      ...item,
      acknowledged: true,
    });
  });
});

function publish(store: TestStore) {
  return Effect.gen(function* () {
    yield* queueIncomingItem({ ...item, conversation: "agent:bob" });
    const rejected = yield* store
      .saveMessageDecision(
        {
          ...item,
          canonicalGuidance: new Uint8Array(3 * 1024 * 1024),
        },
        completion,
      )
      .pipe(Effect.flip);
    expect(rejected).toEqual(
      new EndpointStoreError({ reason: "invalid-input" }),
    );
    expect((yield* store.readDecisions({})).decisions).toEqual([]);
    expect(yield* store.readInboxItem(item.deliveryToken)).toBeUndefined();
    expect(yield* readNextItemsToCheck()).toHaveLength(1);
    yield* store.saveMessageDecision(item, completion);
    yield* store.acknowledgeInboxItem(item.deliveryToken);
  });
}

function expectRetained(store: TestStore) {
  return Effect.gen(function* () {
    expect(yield* readNextItemsToCheck()).toEqual([]);
    expect((yield* store.readDecisions({})).decisions).toMatchObject([
      { ...summary, checkId: completion.checkId, disposition: "admitted" },
    ]);
    yield* store.saveCheckDecision(completion);
    expect(
      yield* store
        .saveCheckDecision({
          ...completion,
          canonicalCandidate: bytes("different candidate"),
        })
        .pipe(Effect.flip),
    ).toEqual(new EndpointStoreError({ reason: "conflict" }));
    expect(
      yield* store
        .saveCheckDecision({
          ...completion,
          canonicalInput: bytes("different policy and evidence"),
        })
        .pipe(Effect.flip),
    ).toEqual(new EndpointStoreError({ reason: "conflict" }));
    expect(
      yield* store
        .saveCheckDecision({
          ...completion,
          canonicalResult: bytes("different judgment"),
        })
        .pipe(Effect.flip),
    ).toEqual(new EndpointStoreError({ reason: "conflict" }));
    return yield* store.readInboxItem(item.deliveryToken);
  });
}
