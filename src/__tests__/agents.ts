/** @file Shared test agents and deterministic identifiers. */

import { Context, Effect, Encoding, Layer, Schema } from "effect";
import { createHash } from "node:crypto";
import { getOrCreateOwnedAgentKey } from "../harness/ownership.js";
import { AgentCard, AgentName, Identity } from "../identity.js";

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

/**
 * A prefixed identifier over 32 repeated bytes, the form of MsgId and
 * DeliveryToken.
 * @param prefix Identifier prefix, such as `msg_`.
 * @param byte Value repeated across all 32 bytes.
 * @returns The identifier text, before schema decoding.
 */
export function digest(prefix: string, byte: number): string {
  return `${prefix}${Encoding.encodeBase64Url(new Uint8Array(32).fill(byte))}`;
}

/**
 * Creates an agent named `name` the way the daemon does for a new owner.
 * Returns the key its rows are stored under and the card the daemon gave it.
 */
export const createTestAgent = Effect.fn("createTestAgent")(
  function* (name: string) {
    const agentKey = yield* getOrCreateOwnedAgentKey(
      { issuer: "urn:social-harness:test", subject: name },
      Schema.decodeUnknownSync(AgentName)(name),
    );
    const identity = yield* Layer.build(Identity.layer(agentKey));
    return { agentKey, card: Context.get(identity, Identity).me };
  },
  Effect.scoped,
  Effect.orDie,
);
