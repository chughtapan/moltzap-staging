/** @file MCP clients retain complete domain refusals without leaking transport data. */
import { Effect, Either } from "effect";
import { describe, expect, it } from "vitest";
import { testAgentView } from "../__tests__/agent-card-fixtures.js";
import { collectiveOperationId } from "../__tests__/collective-id-fixtures.js";
import { decodeHarnessSendErrorData } from "./operations.js";

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
