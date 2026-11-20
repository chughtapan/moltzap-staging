/** @file Agent identifiers, names and cards, and the agent a firewall acts for. */

import { Schema } from "effect";
import { canonicalIdentifier } from "./encoding.js";

const IDENTIFIER_BYTE_LENGTH = 16;

/** Identifies one agent. The daemon mints it when it creates the agent. */
export const AgentId = canonicalIdentifier(
  "AgentId",
  "agt_",
  IDENTIFIER_BYTE_LENGTH,
);
/** Validated nominal value decoded by {@link AgentId}. */
export type AgentId = typeof AgentId.Type;

/** Opaque identity of the principal represented by an agent. */
export const PrincipalId = canonicalIdentifier(
  "PrincipalId",
  "prn_",
  IDENTIFIER_BYTE_LENGTH,
);
/** Validated nominal value decoded by {@link PrincipalId}. */
export type PrincipalId = typeof PrincipalId.Type;

/** The rule every agent name meets, as validation errors state it. */
export const AGENT_NAME_RULE =
  "3 to 32 lowercase letters, digits or single hyphens, starting and ending with a letter or digit";

/** The name other agents address an agent by, unique on its daemon. */
export const AgentName = Schema.String.pipe(
  Schema.minLength(3),
  Schema.maxLength(32),
  Schema.pattern(/^[a-z0-9]+(-[a-z0-9]+)*$/),
  Schema.brand("AgentName"),
  Schema.annotations({
    identifier: "AgentName",
    description: `an agent name: ${AGENT_NAME_RULE}`,
  }),
);
/** Validated nominal value decoded by {@link AgentName}. */
export type AgentName = typeof AgentName.Type;

/**
 * Identifies an agent and the principal it represents. Messages, results and
 * policy decisions name their agents with this value.
 */
export const AgentCard = Schema.Struct({
  agentId: AgentId,
  agentName: AgentName,
  principalId: PrincipalId,
}).annotations({ identifier: "AgentCard" });
/**
 * An agent's identifier, name and principal, as its daemon recorded them.
 * @useDeclaredType
 */
export type AgentCard = typeof AgentCard.Type;
