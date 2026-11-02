/** @file Public addressed-message schema behavior. */

import { Either, Encoding, Schema } from "effect";
import { describe, expect, it } from "vitest";
import { digest, testAgentView } from "./__tests__/agent-card-fixtures.js";
import { collectiveOperationId } from "./__tests__/collective-id-fixtures.js";
import {
  AgentAddress,
  Content,
  GroupAddress,
  MessageAddressInput,
  MsgId,
  SendRequest,
} from "./index.js";

const canonicalGroup32 = `group:${Array.from(
  Array(32).keys(),
  (index) => `member-${String(index).padStart(2, "0")}`,
).join(",")}`;

const canonicalGroup33 = `group:${Array.from(
  Array(33).keys(),
  (index) => `member-${String(index).padStart(2, "0")}`,
).join(",")}`;

const decodingFails = (
  schema: Schema.Schema.AnyNoContext,
  value: unknown,
): boolean =>
  Either.match(Schema.decodeUnknownEither(schema)(value), {
    onLeft: () => true,
    onRight: () => false,
  });

// @agent-code-guard/regression-only: Address schemas pin the accepted public runtime grammar.
describe("public address schemas", () => {
  it("accepts exact agent addresses and arbitrary group input order", () => {
    const direct = "agent:alice-agent";
    const unorderedGroup = "group:carol-agent,bob-agent";

    expect(Schema.decodeUnknownSync(AgentAddress)(direct)).toBe(direct);
    expect(Schema.decodeUnknownSync(MessageAddressInput)(unorderedGroup)).toBe(
      unorderedGroup,
    );
  });

  it("accepts canonical 3-member and 32-member groups", () => {
    const threeMembers = "group:alice-agent,bob-agent,carol-agent";

    expect(Schema.decodeUnknownSync(GroupAddress)(threeMembers)).toBe(
      threeMembers,
    );
    expect(Schema.decodeUnknownSync(GroupAddress)(canonicalGroup32)).toBe(
      canonicalGroup32,
    );
  });

  it.each([
    {
      case: "noncanonical member order",
      address: "group:carol-agent,alice-agent,bob-agent",
    },
    { case: "2 members", address: "group:alice-agent,bob-agent" },
    { case: "33 members", address: canonicalGroup33 },
  ])("rejects a group output with $case", ({ address }) => {
    expect(decodingFails(GroupAddress, address)).toBe(true);
  });
});

const messageHash = (byteLength: number) =>
  Encoding.encodeBase64Url(new Uint8Array(byteLength).fill(7));

// @agent-code-guard/regression-only: Identifier schemas pin the closed durable send grammar.
describe("public message identifiers", () => {
  it("accepts the exact message identifier grammar", () => {
    const msgId = digest("msg_", 7);

    expect(Schema.decodeUnknownSync(MsgId)(msgId)).toBe(msgId);
  });

  // A MsgId is `msg_` plus the canonical base64url of exactly 32 bytes, so
  // each case breaks one of the prefix, the length, or the round trip.
  it.each([
    { case: "another prefix", candidate: `cnv_${messageHash(32)}` },
    { case: "31 bytes", candidate: `msg_${messageHash(31)}` },
    { case: "33 bytes", candidate: `msg_${messageHash(33)}` },
    {
      case: "a noncanonical final character",
      candidate: `msg_${messageHash(32).slice(0, -1)}d`,
    },
    { case: "padding", candidate: `msg_${messageHash(32)}=` },
  ])("rejects a message identifier with $case", ({ candidate }) => {
    expect(decodingFails(MsgId, candidate)).toBe(true);
  });
});

// @agent-code-guard/regression-only: Content schemas pin the exact semantic boundary and canonical byte cap.
describe("public content", () => {
  it("accepts exact nonempty semantic content", () => {
    const content = [
      { type: "text", text: "Meet at 10?" },
      { type: "data", value: { available: true, hour: 10 } },
    ];

    expect(Schema.decodeUnknownSync(Content)(content)).toEqual(content);
  });

  it("accepts content at the exact canonical byte cap", () => {
    const text = "a".repeat(32_741);
    const content = [{ type: "text", text }];

    expect(Schema.decodeUnknownSync(Content)(content)).toEqual(content);
  });

  it.each([
    { case: "empty", content: [] },
    { case: "non-JSON", content: [{ type: "data", value: Number.NaN }] },
    { case: "ill-formed", content: [{ type: "text", text: "\ud800" }] },
    {
      case: "oversized",
      content: [{ type: "text", text: "a".repeat(32_742) }],
    },
    { case: "open", content: [{ type: "text", text: "hello", extra: true }] },
  ])("rejects $case content", ({ content }) => {
    expect(decodingFails(Content, content)).toBe(true);
  });
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

  it.each([
    {
      case: "an inherited field",
      input: { to: "agent:bob-agent", text: "Hello", inherited: true },
    },
    {
      case: "an idempotency key",
      input: {
        to: "agent:bob-agent",
        text: "Hello",
        idempotencyKey: "outbox-43",
      },
    },
    {
      case: "content in place of text",
      input: {
        to: "agent:bob-agent",
        content: [{ type: "text", text: "Hello" }],
      },
    },
  ])("rejects an operation with $case", ({ input }) => {
    expect(decodingFails(SendRequest, input)).toBe(true);
  });
});

const collectiveId = collectiveOperationId(
  testAgentView("agent:alice").agentId,
  3,
);
const slotSchema = {
  type: "object",
  properties: { slot: { type: "string", enum: ["mon", "tue"] } },
  required: ["slot"],
};

// @agent-code-guard/regression-only: gather inputs pin the collective send grammar.
describe("public gather input", () => {
  it("decodes a gather with a deadline in seconds and a schema", () => {
    const input = {
      to: "group:bob-agent,carol-agent",
      text: "Which day?",
      collective: { op: "gather", deadline: 300, requestedSchema: slotSchema },
    };

    expect(Schema.decodeUnknownSync(SendRequest)(input)).toEqual(input);
  });

  it("accepts a gather deadline of up to 30 days", () => {
    const input = {
      to: "agent:bob-agent",
      text: "Which day?",
      collective: {
        op: "gather",
        deadline: 2_592_000,
        requestedSchema: slotSchema,
      },
    };

    expect(Schema.decodeUnknownSync(SendRequest)(input)).toEqual(input);
  });

  it.each([
    { case: "zero", deadline: 0 },
    { case: "fractional", deadline: 1.5 },
    { case: "beyond 30 days", deadline: 2_592_001 },
  ])("rejects a $case gather deadline", ({ deadline }) => {
    expect(
      decodingFails(SendRequest, {
        to: "agent:bob-agent",
        text: "Which day?",
        collective: { op: "gather", deadline, requestedSchema: slotSchema },
      }),
    ).toBe(true);
  });

  it("rejects a gather without a requested schema", () => {
    expect(
      decodingFails(SendRequest, {
        to: "agent:bob-agent",
        text: "Which day?",
        collective: { op: "gather", deadline: 60 },
      }),
    ).toBe(true);
  });
});

// @agent-code-guard/regression-only: all_gather inputs pin the group collective send grammar.
describe("public all_gather input", () => {
  it("decodes an all_gather with a deadline in seconds and a schema", () => {
    const input = {
      to: "group:bob-agent,carol-agent",
      text: "Which day?",
      collective: {
        op: "all_gather",
        deadline: 300,
        requestedSchema: slotSchema,
      },
    };

    expect(Schema.decodeUnknownSync(SendRequest)(input)).toEqual(input);
  });

  it("rejects an all_gather deadline beyond 30 days", () => {
    expect(
      decodingFails(SendRequest, {
        to: "group:bob-agent,carol-agent",
        text: "Which day?",
        collective: {
          op: "all_gather",
          deadline: 2_592_001,
          requestedSchema: slotSchema,
        },
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
