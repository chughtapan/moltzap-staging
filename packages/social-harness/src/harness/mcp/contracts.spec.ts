/** @file MCP values validate their public message and management behavior. */

import { Option, Schema } from "effect";
import { describe, expect, it } from "vitest";
import { testAgentCard } from "../../__tests__/agents.js";
import { collectiveOperationId } from "../../communication/__tests__/request-ids.js";
import {
  managementListConversationsRequestSchema,
  managementListConversationsResultSchema,
  toolDefinitions,
} from "./contracts.js";

describe("message operation contracts", () => {
  describe("a refused send", () => {
    const read = (data: unknown) =>
      Option.getOrThrow(
        toolDefinitions.send_message.failure.read({ kind: "refused", data }),
      );

    it.each(["membership-invalid", "network-unavailable"])(
      "keeps a member's closed refusal %s",
      (reason) => {
        const failure = {
          kind: "members-unreachable",
          members: [{ member: testAgentCard("bob"), reason }],
        };
        const operationId = collectiveOperationId(
          testAgentCard("alice").agentId,
        );
        expect(
          read({ reason: "collective-failed", operationId, failure }),
        ).toMatchObject({ _tag: "RequestError", operationId, failure });
      },
    );
    it.each([
      { reason: "firewall-refused", checkId: "incomplete" },
      { reason: "collective-failed" },
    ])("treats an incomplete refusal as an unknown outcome: %j", (value) => {
      expect(read(value)).toMatchObject({
        _tag: "SendError",
        reason: "outcome-unknown",
      });
    });
    it("reads a plain failure without keeping fields it does not know", () => {
      const unknownField = "private transport";
      const failure = read({
        reason: "network-unavailable",
        addedLater: unknownField,
      });
      expect(failure).toMatchObject({ reason: "network-unavailable" });
      expect(JSON.stringify(failure)).not.toContain(unknownField);
    });
  });

  describe("a send with no readable answer", () => {
    it("is unavailable when the request cannot have reached the daemon", () => {
      expect(
        Option.getOrThrow(
          toolDefinitions.send_message.failure.read({
            kind: "unanswered",
            sent: false,
          }),
        ),
      ).toMatchObject({ reason: "network-unavailable" });
    });
    it.each([
      { kind: "unanswered", sent: true },
      { kind: "unreadable" },
    ] as const)(
      "has an unknown outcome when it may have been received: $kind",
      (failure) => {
        expect(
          Option.getOrThrow(toolDefinitions.send_message.failure.read(failure)),
        ).toMatchObject({ reason: "outcome-unknown" });
      },
    );
  });
});

describe("management and history contracts", () => {
  describe("conversation pagination", () => {
    it("accepts a cursor without interpreting its contents", () => {
      expect(
        Schema.decodeUnknownSync(managementListConversationsRequestSchema)({
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
        Schema.decodeUnknownSync(managementListConversationsResultSchema)(page),
      ).toEqual(page);
      expect(() =>
        Schema.decodeUnknownSync(managementListConversationsResultSchema)({
          addresses: [...page.addresses].reverse(),
        }),
      ).toThrow();
    });
  });
});
