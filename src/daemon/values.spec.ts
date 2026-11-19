/** @file The address grammar. */

import { Effect, FastCheck as fc, Schema } from "effect";
import { expect, it } from "vitest";
import { testAgentCard } from "../__tests__/agents.js";
import { addressedAgent, AgentAddress } from "./values.js";

const localAgentName = testAgentCard("agent:agent-1").agentName;

it("addresses one agent as agent:<name>, and refuses the address that names only the caller", () => {
  fc.assert(
    fc.property(
      fc.oneof(
        fc.stringMatching(/^[a-z0-9]{3,8}$/),
        fc.constant(localAgentName),
      ),
      (name) => {
        const address = Schema.decodeUnknownSync(AgentAddress)(`agent:${name}`);
        expect(
          Effect.runSync(
            addressedAgent(address, localAgentName).pipe(
              Effect.match({
                onFailure: ({ reason }) => reason,
                onSuccess: (named) => named,
              }),
            ),
          ),
        ).toBe(name === localAgentName ? "membership-invalid" : name);
      },
    ),
  );
});
