/** @file A message to several recipients names each once. */

import { effect as it } from "@effect/vitest";
import { Effect, Exit, Schema } from "effect";
import { expect } from "vitest";
import { digest, testAgentCard } from "../__tests__/agents.js";
import { Message } from "./messages.js";

const bob = testAgentCard("bob");

it.each([
  { reason: "a repeated recipient", recipients: [bob, bob] },
  {
    reason: "one identity under two recipient names",
    recipients: [bob, { ...testAgentCard("carol"), agentId: bob.agentId }],
  },
])("rejects $reason", ({ recipients }) =>
  Effect.gen(function* () {
    const decoded = yield* Schema.decodeUnknown(Message)({
      msgId: digest("msg_", 1),
      author: testAgentCard("alice"),
      recipients,
      content: [{ type: "text", text: "Choose a meeting time." }],
    }).pipe(Effect.exit);

    expect(Exit.isFailure(decoded)).toBe(true);
  }),
);
