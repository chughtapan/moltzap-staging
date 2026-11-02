/** @file Conversation pages use one opaque cursor and canonical address order. */
import { Schema } from "effect";
import { describe, expect, it } from "vitest";
import {
  managementSearchConversationsRequestSchema,
  managementSearchConversationsResultSchema,
} from "./daemon-tools.js";

describe("conversation pagination", () => {
  it("accepts a cursor without interpreting its contents", () => {
    expect(
      Schema.decodeUnknownSync(managementSearchConversationsRequestSchema)({
        cursor: "opaque-position",
      }),
    ).toEqual({ cursor: "opaque-position" });
  });
  it("preserves canonical address order and the next cursor", () => {
    const page = {
      addresses: ["agent:bob", "group:alice,bob,carol"],
      nextCursor: "opaque-position",
    };
    expect(
      Schema.decodeUnknownSync(managementSearchConversationsResultSchema)(page),
    ).toEqual(page);
    expect(() =>
      Schema.decodeUnknownSync(managementSearchConversationsResultSchema)({
        addresses: [...page.addresses].reverse(),
      }),
    ).toThrow();
  });
});
