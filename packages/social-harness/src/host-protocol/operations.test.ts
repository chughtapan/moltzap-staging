/** @file MCP readers validate saved inbox content and closed send refusals. */
import { Effect, Either, Schema } from "effect";
import { describe, expect, it } from "vitest";
import { digest, testAgentView } from "../__tests__/agent-card-fixtures.js";
import { collectiveOperationId } from "../__tests__/collective-id-fixtures.js";
import { DeliveryToken } from "../harness/inbox.js";
import {
  decodeHarnessMessageReadyEvent,
  decodeHarnessReadInboxResult,
  decodeHarnessSendErrorData,
} from "./operations.js";

const deliveryToken = Schema.decodeUnknownSync(DeliveryToken)(
  digest("dlv_", 1),
);
const item = {
  kind: "multicast",
  message: {
    msgId: digest("msg_", 1),
    content: [{ type: "text", text: "hello" }],
    author: testAgentView("alice"),
    recipients: [testAgentView("bob")],
  },
};
const entry = { deliveryToken, address: "agent:alice", item };

describe("MCP inbox values", () => {
  it("retains the conversation address beside content and the lookup token", () => {
    expect(Effect.runSync(decodeHarnessMessageReadyEvent(entry))).toEqual(
      entry,
    );
  });
  it("reads extension fields without treating a read as acknowledgment", () => {
    expect(
      Effect.runSync(
        decodeHarnessReadInboxResult({
          items: [{ ...entry, addedLater: true }],
          nextCursor: "opaque",
          addedLater: true,
        }),
      ),
    ).toEqual({ items: [entry], nextCursor: "opaque" });
  });
  it.each([
    { items: [{ address: "agent:alice", item }] },
    { items: [{ deliveryToken, address: "agent:alice" }] },
    { items: [{ deliveryToken, item }] },
  ])("requires the complete saved entry: %j", (value) => {
    expect(
      Either.match(
        Effect.runSync(decodeHarnessReadInboxResult(value).pipe(Effect.either)),
        { onLeft: () => true, onRight: () => false },
      ),
    ).toBe(true);
  });
});

describe("MCP send failures", () => {
  it.each(["membership-invalid", "network-unavailable"])(
    "retains a member's closed refusal %s",
    (reason) => {
      const failure = {
        reason: "collective-failed",
        operationId: collectiveOperationId(testAgentView("alice").agentId),
        failure: {
          kind: "members-unreachable",
          members: [{ member: testAgentView("bob"), reason }],
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
