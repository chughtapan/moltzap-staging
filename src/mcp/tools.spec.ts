/** @file MCP values validate their public message and management behavior. */

import { Effect, FastCheck as fc, Schema } from "effect";
import { describe, expect, it } from "vitest";
import { testAgentCard } from "../__tests__/agents.js";
import { MessageAddressInput } from "../communication/index.js";
import { canonicalMessageAddress, toolDefinitions } from "./tools.js";

const localAgentName = testAgentCard("agent:agent-1").agentName;

it("addresses one agent as agent:<name>, and refuses the address that names only the caller", () => {
  fc.assert(
    fc.property(
      fc.oneof(
        fc.stringMatching(/^[a-z0-9]{3,8}$/),
        fc.constant(localAgentName),
      ),
      (name) => {
        const address = Schema.decodeUnknownSync(MessageAddressInput)(
          `agent:${name}`,
        );
        expect(
          Effect.runSync(
            canonicalMessageAddress(address, localAgentName).pipe(
              Effect.match({
                onFailure: ({ reason }) => reason,
                onSuccess: (canonical) => canonical.address,
              }),
            ),
          ),
        ).toBe(name === localAgentName ? "membership-invalid" : address);
      },
    ),
  );
});

describe("management and history contracts", () => {
  describe("conversation pagination", () => {
    it("preserves canonical address order and the next cursor", () => {
      const page = {
        addresses: ["agent:bob", "agent:carol"],
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
