/** @file Shared test agents. */

import { Encoding, Schema } from "effect";
import { createHash } from "node:crypto";
import { AgentCard } from "../messages.js";

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
