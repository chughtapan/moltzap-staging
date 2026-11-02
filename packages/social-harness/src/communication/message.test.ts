/** @file Messages preserve author-scoped identity and distinct participants through reconstruction. */

import { effect as it } from "@effect/vitest";
import { Effect, Exit, JSONSchema, Schema } from "effect";
import { expect } from "vitest";
import { digest, testAgentView } from "../__tests__/agent-card-fixtures.js";
import { Answer, Incoming, Request, RequestedSchema } from "./index.js";
import {
  Message,
  messageFromIntent,
  messageFromParticipants,
} from "./message.js";
import { MsgId } from "./records/values.js";
import { encodeCollectiveContent } from "./requests/answers.js";

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

it("captures a request and an answer while keeping decoded operation metadata out of stored message bytes", () =>
  Effect.gen(function* () {
    const request = yield* Request.create({
      author: alice,
      recipients: [bob, carol],
      op: "all_gather",
      question: "Which day works?",
      deadline: 30,
      requestedSchema: Schema.decodeUnknownSync(RequestedSchema)({
        type: "object",
        properties: { day: { type: "string" } },
        required: ["day"],
      }),
    });
    const question = yield* Message.create({
      author: request.author,
      recipients: request.recipients,
      content: yield* encodeCollectiveContent(
        {
          kind: "operation",
          op: request.op,
          operationId: request.operationId,
          deadlineAt: request.deadlineAt,
          requestedSchema: request.requestedSchema,
        },
        request.question,
      ),
    });
    if (question.operation?.kind !== "request") {
      return yield* Effect.dieMessage("question did not decode");
    }
    const answer = yield* Answer.create({
      author: bob,
      request: { ...question, operation: question.operation },
      response: { action: "accept", content: { day: "monday" } },
    });
    expect(answer.recipients).toEqual([alice, carol]);
    expect(answer.operation).toMatchObject({
      kind: "response",
      operationId: request.operationId,
      content: { day: "monday" },
    });
    const incoming: Incoming = { kind: "message", message: answer };
    const stored = yield* Schema.encode(Incoming)(incoming);
    expect(stored).toMatchObject({ kind: "message", message: { author: bob } });
    if (stored.kind !== "message") {
      return yield* Effect.dieMessage("stored handoff changed kind");
    }
    expect(stored.message).not.toHaveProperty("operation");
    expect(yield* Schema.decodeUnknown(Incoming)(stored)).toEqual(incoming);
    expect(JSONSchema.make(Schema.typeSchema(Incoming))).toHaveProperty(
      "anyOf",
    );
  }));
