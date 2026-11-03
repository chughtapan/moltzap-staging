/** @file Saved peer work, permitted views and delivery tokens stay scoped to their agent. */
import { scopedLive as it } from "@effect/vitest";
import { Effect, Exit, Redacted, Schema } from "effect";
import { expect } from "vitest";
import { testAgentCard } from "../__tests__/agents.js";
import { Message } from "../communication/service.js";
import { migrateDaemon } from "../database/migrations.js";
import { databaseConnection } from "../database/sql.js";
import { makeInbox, MessageAddress } from "./inbox.js";

it("retains the same peer message independently and rejects foreign event and acknowledgment tokens", () =>
  Effect.gen(function* () {
    yield* migrateDaemon;
    const first = yield* makeInbox("first");
    const second = yield* makeInbox("second");
    const address = yield* Schema.decodeUnknown(MessageAddress)("agent:alice");
    const message = yield* Message.create({
      author: testAgentCard("agent:alice"),
      recipients: [testAgentCard("agent:bob"), testAgentCard("agent:carol")],
      content: [{ type: "text", text: "Can we meet tomorrow?" }],
    });
    const incoming = { kind: "message", message } as const;
    yield* first.queueIncoming(address, incoming);
    yield* second.queueIncoming(address, incoming);
    const firstPending = yield* first.pending(10);
    const secondPending = yield* second.pending(10);
    expect(firstPending).toHaveLength(1);
    expect(secondPending).toHaveLength(1);
    const firstItem = firstPending[0];
    const secondItem = secondPending[0];
    if (firstItem === undefined || secondItem === undefined) {
      return yield* Effect.dieMessage(
        "Both agents must retain their own incoming work",
      );
    }
    const reference = {
      authorAgentId: message.author.agentId,
      msgId: message.msgId,
    };
    const decision = {
      kind: "allowed",
      item: { kind: "multicast", message },
      visibility: [{ message: reference, visibleToAgent: true }],
      save: Effect.void,
    } as const;
    yield* first.publish(firstItem, decision);
    expect(
      (yield* first.visibleMessages([message]))[0]?.visibleToAgent,
    ).toBeTruthy();
    expect(
      (yield* second.visibleMessages([message]))[0]?.visibleToAgent,
    ).toBeFalsy();
    const foreignRead = yield* second
      .getInboxEvent(firstItem.token)
      .pipe(Effect.exit);
    const foreignAck = yield* second
      .acknowledgeDelivery(firstItem.token)
      .pipe(Effect.exit);
    expect(Exit.isFailure(foreignRead)).toBeTruthy();
    expect(Exit.isFailure(foreignAck)).toBeTruthy();
    yield* second.publish(secondItem, decision);
    yield* first.acknowledgeDelivery(firstItem.token);
    const reopened = yield* makeInbox("second");
    const retained = yield* reopened.listInbox();
    expect(retained.items.map((item) => item.deliveryToken)).toEqual([
      secondItem.token,
    ]);
    expect(yield* reopened.getInboxEvent(secondItem.token)).toEqual(
      yield* first.getInboxEvent(firstItem.token),
    );
    expect((yield* first.listInbox()).items).toHaveLength(0);
  }).pipe(
    Effect.provide(databaseConnection(Redacted.make("sqlite::memory:"))),
  ));
