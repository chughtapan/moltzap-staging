/** @file Messages preserve author-scoped identity and distinct participants through reconstruction. */

import { effect as it } from "@effect/vitest";
import { Effect, Exit, Schema } from "effect";
import { expect } from "vitest";
import { digest, testAgentView } from "../__tests__/agent-card-fixtures.js";
import {
  Message,
  messageFromIntent,
  messageFromParticipants,
} from "./message.js";
import { MsgId } from "./records/values.js";

const alice = testAgentView("alice");
const bob = testAgentView("bob");
const carol = testAgentView("carol");
const msgId = Schema.decodeUnknownSync(MsgId)(digest("msg_", 1));
const content = [{ type: "text", text: "Choose a meeting time." }] as const;

// @agent-code-guard/regression-only: Retried messages retain attribution while ambiguous participant identities are rejected.
it("preserves identity, participants and content when member enumeration changes", () =>
  Effect.gen(function* () {
    const intent = { authorAgentId: alice.agentId, msgId, content };
    const first = yield* messageFromIntent([carol, alice, bob], intent);
    const recovered = yield* messageFromIntent([bob, carol, alice], intent);

    expect(first).toEqual({
      msgId,
      author: alice,
      recipients: [bob, carol],
      content,
    });
    expect(recovered).toEqual(first);
  }));

it.each([
  { reason: "the author as a recipient", recipients: [alice] },
  { reason: "a repeated recipient", recipients: [bob, bob] },
  {
    reason: "one identity under two recipient names",
    recipients: [bob, { ...carol, agentId: bob.agentId }],
  },
])("rejects $reason", ({ recipients }) =>
  Effect.gen(function* () {
    const decoded = yield* Schema.decodeUnknown(Message)({
      msgId,
      author: alice,
      recipients,
      content,
    }).pipe(Effect.exit);

    expect(Exit.isFailure(decoded)).toBe(true);
  }),
);

it("captures nested content independently of the caller's objects", () =>
  Effect.gen(function* () {
    const data = { times: ["monday"] };
    const message = yield* messageFromParticipants(alice, [bob], {
      msgId,
      content: [{ type: "data", value: data }],
    });
    data.times.push("tuesday");
    expect(message.content).toEqual([
      { type: "data", value: { times: ["monday"] } },
    ]);
  }));
