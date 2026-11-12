/** @file Host requests select conversations and decode as one closed set of sends. */

import { live as effectIt, it } from "@effect/vitest";
import { Effect, Either, Schema } from "effect";
import { describe, expect } from "vitest";
import { testAgentCard } from "../__tests__/agents.js";
import { collectiveOperationId } from "../communication/__tests__/request-ids.js";
import {
  maximumMembers,
  MessageAddressInput,
} from "../communication/messages.js";
import { SendRequest } from "../index.js";
import { canonicalMessageAddress } from "./message-format.js";

const localAgentName = testAgentCard("agent:agent-1").agentName;

effectIt.each([
  { input: "agent:agent-2", expected: "agent:agent-2" },
  { input: "group:agent-2", expected: "agent:agent-2" },
  { input: "group:agent-1,agent-2", expected: "agent:agent-2" },
  {
    input: "group:agent-2,agent-10",
    expected: "group:agent-1,agent-10,agent-2",
  },
])("canonicalizes $input as $expected", ({ input, expected }) =>
  Effect.gen(function* () {
    const to = yield* Schema.decodeUnknown(MessageAddressInput)(input);
    expect((yield* canonicalMessageAddress(to, localAgentName)).address).toBe(
      expected,
    );
  }),
);

const oversized = Array.from(
  Array(maximumMembers).keys(),
  (index) => `remote-${String(index)}`,
).join(",");

effectIt.each([
  "agent:agent-1",
  "group:agent-1",
  "group:agent-2,agent-2",
  `group:${oversized}`,
])("refuses invalid membership in %s", (input) =>
  Effect.gen(function* () {
    const to = yield* Schema.decodeUnknown(MessageAddressInput)(input);
    const rejected = yield* canonicalMessageAddress(to, localAgentName).pipe(
      Effect.flip,
    );
    expect(rejected).toMatchObject({ reason: "membership-invalid" });
  }),
);

const decodingFails = (
  schema: Schema.Schema.AnyNoContext,
  value: unknown,
): boolean =>
  Either.match(Schema.decodeUnknownEither(schema)(value), {
    onLeft: () => true,
    onRight: () => false,
  });

// @agent-code-guard/regression-only: SendRequest is a closed operation boundary with no host retry identity.
describe("public send input", () => {
  it("decodes a send without a collective operation", () => {
    const input = { to: "agent:bob-agent", text: "Hello" };

    expect(Schema.decodeUnknownSync(SendRequest)(input)).toEqual(input);
  });

  it("decodes an explicit multicast operation", () => {
    const input = {
      to: "agent:bob-agent",
      text: "Hello",
      collective: { op: "multicast" },
    };

    expect(Schema.decodeUnknownSync(SendRequest)(input)).toEqual(input);
  });

  it("decodes a collective operation that omits op", () => {
    const input = { to: "agent:bob-agent", text: "Hello", collective: {} };

    expect(Schema.decodeUnknownSync(SendRequest)(input)).toEqual(input);
  });

  it("rejects an operation this endpoint does not define", () => {
    expect(
      decodingFails(SendRequest, {
        to: "agent:bob-agent",
        text: "Hello",
        collective: { op: "broadcast" },
      }),
    ).toBe(true);
  });

  it("rejects unexpected fields", () => {
    const input = { to: "agent:bob-agent", text: "Hello", extra: true };
    expect(decodingFails(SendRequest, input)).toBe(true);
  });
});

const collectiveId = collectiveOperationId(
  testAgentCard("agent:alice").agentId,
  3,
);
const slotSchema = {
  type: "object",
  properties: { slot: { type: "string", enum: ["mon", "tue"] } },
  required: ["slot"],
};

// @agent-code-guard/regression-only: gather inputs pin the collective send grammar.
describe.each(["gather", "all_gather"] as const)("public %s input", (op) => {
  it("decodes a question with a deadline in seconds and a schema", () => {
    const input = {
      to: "group:bob-agent,carol-agent",
      text: "Which day?",
      collective: { op, deadline: 300, requestedSchema: slotSchema },
    };

    expect(Schema.decodeUnknownSync(SendRequest)(input)).toEqual(input);
  });

  it("accepts a deadline of up to 30 days", () => {
    const input = {
      to: "agent:bob-agent",
      text: "Which day?",
      collective: { op, deadline: 2_592_000, requestedSchema: slotSchema },
    };

    expect(Schema.decodeUnknownSync(SendRequest)(input)).toEqual(input);
  });

  it.each([
    { case: "zero", deadline: 0 },
    { case: "fractional", deadline: 1.5 },
    { case: "beyond 30 days", deadline: 2_592_001 },
  ])("rejects a $case deadline", ({ deadline }) => {
    expect(
      decodingFails(SendRequest, {
        to: "agent:bob-agent",
        text: "Which day?",
        collective: { op, deadline, requestedSchema: slotSchema },
      }),
    ).toBe(true);
  });

  it("rejects a question without a requested schema", () => {
    expect(
      decodingFails(SendRequest, {
        to: "agent:bob-agent",
        text: "Which day?",
        collective: { op, deadline: 60 },
      }),
    ).toBe(true);
  });
});

// @agent-code-guard/regression-only: response inputs pin how a member answers a collective request.
describe("public collective response input", () => {
  it("rejects a response that omits its request identity", () => {
    const input = {
      to: "agent:bob-agent",
      collectiveResponse: { action: "accept", content: { slot: "mon" } },
    };

    expect(decodingFails(SendRequest, input)).toBe(true);
  });

  it("decodes a response that names the request it answers", () => {
    const input = {
      to: "agent:bob-agent",
      collectiveResponse: { operationId: collectiveId, action: "decline" },
    };

    expect(Schema.decodeUnknownSync(SendRequest)(input)).toEqual(input);
  });

  it("rejects a response without a conversation", () => {
    expect(
      decodingFails(SendRequest, {
        collectiveResponse: { action: "decline" },
      }),
    ).toBe(true);
  });

  it("rejects content on a decline", () => {
    expect(
      decodingFails(SendRequest, {
        to: "agent:bob-agent",
        collectiveResponse: {
          operationId: collectiveId,
          action: "decline",
          content: { slot: "mon" },
        },
      }),
    ).toBe(true);
  });
});
