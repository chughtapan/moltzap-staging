/** @file Accepted messages, questions and answers expose one receipt shape. */
import { Effect, Either } from "effect";
import { describe, expect, it } from "vitest";
import { digest, testAgentView } from "../__tests__/agent-card-fixtures.js";
import { collectiveOperationId } from "../__tests__/collective-id-fixtures.js";
import {
  checkHarnessSendResult,
  decodeHarnessSendResult,
} from "./operations.js";

const address = "agent:bob";
const ids = [
  digest("msg_", 11),
  collectiveOperationId(testAgentView("alice").agentId, 12),
];

describe("send receipts", () => {
  it.each(ids)("identifies accepted work %s and its conversation", (id) => {
    expect(Effect.runSync(decodeHarnessSendResult({ id, address }))).toEqual({
      id,
      address,
    });
  });
  it("accepts reader extensions while checking the writer's complete result", () => {
    const result = { id: ids[0], address, addedLater: true };
    expect(Effect.runSync(decodeHarnessSendResult(result))).toEqual({
      id: ids[0],
      address,
    });
    expect(
      Either.match(
        Effect.runSync(checkHarnessSendResult(result).pipe(Effect.either)),
        { onLeft: () => true, onRight: () => false },
      ),
    ).toBe(true);
  });
});
