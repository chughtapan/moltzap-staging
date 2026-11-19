/** @file Unchecked messages, inbox entries and delivery tokens stay scoped to their agent. */
import { scopedLive as it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { expect } from "vitest";
import { testAgentCard } from "../__tests__/agents.js";
import { Message } from "../communication/index.js";
import { AgentAddress } from "../mcp/tools.js";
import { openTestDatabase } from "../test.js";
import { makeInbox } from "./inbox.js";

it("stores the same message for each agent and leaves another agent's delivery token pending", () =>
  Effect.gen(function* () {
    const first = yield* makeInbox("first");
    const second = yield* makeInbox("second");
    const address = yield* Schema.decodeUnknown(AgentAddress)("agent:alice");
    const message = yield* Message.create({
      author: testAgentCard("agent:alice"),
      recipients: [testAgentCard("agent:bob")],
      content: [{ type: "text", text: "Can we meet tomorrow?" }],
    });
    yield* first.storeUnchecked(address, message);
    yield* second.storeUnchecked(address, message);
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
    const decision = {
      kind: "allowed",
      save: Effect.void,
    } as const;
    yield* first.recordDecision(firstUnchecked, decision);
    expect(
      (yield* first.visibleMessages([message]))[0]?.visibleToAgent,
    ).toBeTruthy();
    expect(
      (yield* second.visibleMessages([message]))[0]?.visibleToAgent,
    ).toBeFalsy();
    yield* second.markDelivered([firstUnchecked.token]);
    expect((yield* first.listInbox()).entries).toHaveLength(1);
    yield* second.recordDecision(secondUnchecked, decision);
    yield* first.markDelivered([firstUnchecked.token]);
    const reopened = yield* makeInbox("second");
    const pending = yield* reopened.listInbox();
    expect(pending.entries.map((entry) => entry.deliveryToken)).toEqual([
      secondUnchecked.token,
    ]);
    expect((yield* first.listInbox()).entries).toHaveLength(0);
  }).pipe((test) =>
    Effect.flatMap(openTestDatabase(), (database) => database.run(test)),
  ));
