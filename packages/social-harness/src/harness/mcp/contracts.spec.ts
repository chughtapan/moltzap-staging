/** @file MCP values validate their public message and management behavior. */

import { Effect, Either, Schema } from "effect";
import { describe, expect, it } from "vitest";
import { testAgentCard } from "../../__tests__/agents.js";
import { collectiveOperationId } from "../../communication/__tests__/request-ids.js";
import {
  decodeHarnessSendErrorData,
  managementSearchConversationsRequestSchema,
  managementSearchConversationsResultSchema,
} from "./contracts.js";

describe("message operation contracts", () => {
  describe("MCP send failures", () => {
    it.each(["membership-invalid", "network-unavailable"])(
      "retains a member's closed refusal %s",
      (reason) => {
        const failure = {
          reason: "collective-failed",
          operationId: collectiveOperationId(testAgentCard("alice").agentId),
          failure: {
            kind: "members-unreachable",
            members: [{ member: testAgentCard("bob"), reason }],
          },
        };
        expect(Effect.runSync(decodeHarnessSendErrorData(failure))).toEqual(
          failure,
        );
      },
    );
    it.each([
      { reason: "firewall-refused", checkId: "incomplete" },
      { reason: "collective-failed" },
    ])("requires a complete domain refusal: %j", (value) => {
      expect(
        Either.match(
          Effect.runSync(decodeHarnessSendErrorData(value).pipe(Effect.either)),
          { onLeft: () => true, onRight: () => false },
        ),
      ).toBe(true);
    });
    it("reads an additive plain failure without retaining transport data", () => {
      expect(
        Effect.runSync(
          decodeHarnessSendErrorData({
            reason: "network-unavailable",
            addedLater: "private transport",
          }),
        ),
      ).toEqual({ reason: "network-unavailable" });
    });
  });
});

describe("management and history contracts", () => {
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
        Schema.decodeUnknownSync(managementSearchConversationsResultSchema)(
          page,
        ),
      ).toEqual(page);
      expect(() =>
        Schema.decodeUnknownSync(managementSearchConversationsResultSchema)({
          addresses: [...page.addresses].reverse(),
        }),
      ).toThrow();
    });
  });
});
