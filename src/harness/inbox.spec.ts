/** @file Unchecked messages, inbox entries and delivery tokens stay scoped to their agent. */
import { scopedLive as it } from "@effect/vitest";
import { Effect, Exit, Redacted, Schema } from "effect";
import { expect } from "vitest";
import { testAgentCard } from "../__tests__/agents.js";
import { Message } from "../communication/index.js";
import { databaseConnection, migrateDaemon } from "../database/index.js";
import { makeInbox } from "./inbox.js";
import { MessageAddress } from "./message-format.js";

it("stores the same peer message for each agent and refuses another agent's delivery token", () =>
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
    yield* first.storeUnchecked(address, incoming);
    yield* second.storeUnchecked(address, incoming);
    const uncheckedByFirst = yield* first.unchecked(10);
    const uncheckedBySecond = yield* second.unchecked(10);
    expect(uncheckedByFirst).toHaveLength(1);
    expect(uncheckedBySecond).toHaveLength(1);
    const firstUnchecked = uncheckedByFirst[0];
    const secondUnchecked = uncheckedBySecond[0];
    if (firstUnchecked === undefined || secondUnchecked === undefined) {
      return yield* Effect.dieMessage(
        "Both agents must store their own unchecked message",
      );
    }
    const reference = {
      authorAgentId: message.author.agentId,
      msgId: message.msgId,
    };
    const decision = {
      kind: "allowed",
      item: { kind: "message", message },
      visibility: [{ message: reference, visibleToAgent: true }],
      save: Effect.void,
    } as const;
    yield* first.recordDecision(firstUnchecked, decision);
    expect(
      (yield* first.visibleMessages([message]))[0]?.visibleToAgent,
    ).toBeTruthy();
    expect(
      (yield* second.visibleMessages([message]))[0]?.visibleToAgent,
    ).toBeFalsy();
    const foreignAck = yield* second
      .acknowledgeDelivery(firstUnchecked.token)
      .pipe(Effect.exit);
    expect(Exit.isFailure(foreignAck)).toBeTruthy();
    yield* second.recordDecision(secondUnchecked, decision);
    yield* first.acknowledgeDelivery(firstUnchecked.token);
    const reopened = yield* makeInbox("second");
    const pending = yield* reopened.listInbox();
    expect(pending.entries.map((entry) => entry.deliveryToken)).toEqual([
      secondUnchecked.token,
    ]);
    expect((yield* first.listInbox()).entries).toHaveLength(0);
  }).pipe(
    Effect.provide(databaseConnection(Redacted.make("sqlite::memory:"))),
  ));
