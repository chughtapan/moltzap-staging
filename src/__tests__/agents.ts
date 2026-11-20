/** @file Shared test agents. */

import { Effect, Encoding, Schema } from "effect";
import { createHash } from "node:crypto";
import { getOrCreateOwnedAgent } from "../daemon/agent.js";
import { AgentCard, AgentName } from "../messages.js";

/** The same card for the same name on every call; Alice, Bob, Carol and Dave have ids of one repeated byte. */
export function testAgentCard(address: string): AgentCard {
  const name = address.startsWith("agent:")
    ? address.slice("agent:".length)
    : address;
  const index = ["alice", "bob", "carol", "dave"].indexOf(name);
  const suffix =
    index < 0
      ? createHash("sha256")
          .update(name)
          .digest()
          .subarray(0, 16)
          .toString("base64url")
      : Encoding.encodeBase64Url(new Uint8Array(16).fill(index + 1));
  return Schema.decodeUnknownSync(AgentCard)({
    agentId: `agt_${suffix}`,
    agentName: name,
    principalId: `prn_${suffix}`,
  });
}

/** Creates an agent named `name` the way the daemon does for a new owner. */
export const createTestAgent = Effect.fn("createTestAgent")(
  (name: string) =>
    getOrCreateOwnedAgent(
      { issuer: "urn:social-harness:test", subject: name },
      Schema.decodeUnknownSync(AgentName)(name),
    ),
  Effect.orDie,
);
