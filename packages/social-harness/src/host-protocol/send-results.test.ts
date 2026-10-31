/** @file Send results describe complete successful cases across the host boundary. */

import { scoped as it } from "@effect/vitest";
import { Effect, Exit } from "effect";
import { describe, expect } from "vitest";
import { digest, testAgentView } from "../__tests__/agent-card-fixtures.js";
import { collectiveOperationId } from "../__tests__/collective-id-fixtures.js";
import {
  checkHarnessSendResult,
  decodeHarnessSendResult,
} from "./operations.js";

const msgId = digest("msg_", 11);
const operationId = collectiveOperationId(
  testAgentView("agent:alice").agentId,
  12,
);
const address = "agent:bob";

const results = [
  { kind: "multicast", msgId, address },
  { kind: "response", operationId, msgId, address },
  { kind: "collective", operationId },
];

// @agent-code-guard/regression-only: Exact writer cases cover all finite success variants, rejected combinations, reader evolution, and absent optional fields.
describe("send results", () => {
  it.each(results)("decodes a complete $kind result", (result) =>
    Effect.gen(function* () {
      expect(yield* decodeHarnessSendResult(result)).toEqual(result);
    }),
  );

  it.each([
    {},
    { kind: "multicast", msgId },
    { kind: "response", msgId, address },
    { kind: "collective", operationId, msgId },
    { kind: "multicast", msgId, address, operationId },
  ])("rejects an incomplete or contradictory result: %j", (result) =>
    Effect.gen(function* () {
      const decoded = yield* Effect.either(decodeHarnessSendResult(result));
      expect(decoded).toMatchObject({ _tag: "Left" });
    }),
  );

  it("ignores a future result field at the reader and refuses it at the writer", () =>
    Effect.gen(function* () {
      const result = { kind: "multicast", msgId, address, future: true };
      expect(yield* decodeHarnessSendResult(result)).toEqual({
        kind: "multicast",
        msgId,
        address,
      });
      expect(
        Exit.isFailure(yield* Effect.exit(checkHarnessSendResult(result))),
      ).toBe(true);
    }));
});
