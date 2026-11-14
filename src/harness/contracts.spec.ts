/** @file MCP values validate their public message and management behavior. */

import { Option, Schema } from "effect";
import { describe, expect, it } from "vitest";
import { digest } from "../__tests__/agents.js";
import { toolDefinitions } from "./contracts.js";

describe("message operation contracts", () => {
  describe("a refused send", () => {
    const toError = (data: unknown) =>
      Option.getOrThrow(
        toolDefinitions.send_message.failure.toError({ kind: "refused", data }),
      );

    it.each([{ kind: "request-expired" }, { kind: "request-answered" }])(
      "keeps a collective request's closed refusal %j",
      (failure) => {
        const requestId = digest("msg_", 1);
        expect(
          toError({ reason: "collective-failed", requestId, failure }),
        ).toMatchObject({ _tag: "RequestError", requestId, failure });
      },
    );

    it.each([
      { reason: "firewall-refused", checkId: "incomplete" },
      { reason: "collective-failed" },
    ])("treats an incomplete refusal as an unknown outcome: %j", (value) => {
      expect(toError(value)).toMatchObject({
        _tag: "SendError",
        reason: "outcome-unknown",
      });
    });
    it("reads a plain failure without keeping fields it does not know", () => {
      const unknownField = "private transport";
      const failure = toError({
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
          toolDefinitions.send_message.failure.toError({
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
          Option.getOrThrow(
            toolDefinitions.send_message.failure.toError(failure),
          ),
        ).toMatchObject({ reason: "outcome-unknown" });
      },
    );
  });
});

describe("management and history contracts", () => {
  describe("conversation pagination", () => {
    it("preserves canonical address order and the next cursor", () => {
      const page = {
        addresses: ["agent:bob", "group:alice,bob,carol"],
        nextCursor: "opaque-position",
      };
      const decode = Schema.decodeUnknownSync(
        toolDefinitions.list_conversations.output,
      );
      expect(decode(page)).toEqual(page);
      expect(() =>
        decode({ addresses: [...page.addresses].reverse() }),
      ).toThrow();
    });
  });
});
