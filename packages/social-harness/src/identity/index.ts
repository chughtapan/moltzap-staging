/** @file Local registration and immutable identity values used across the harness. */
export {
  AgentCard,
  agentCardOf,
  AgentId,
  AgentIdentityCard,
  AgentIdentityCardVerificationError,
  AgentName,
  AgentPublicKey,
  AgentSigningAuthority,
  PrincipalId,
} from "./agent.js";
export type { VerifiedAgentIdentityCard } from "./agent.js";
export { Authentication, AuthenticationError } from "./authentication/index.js";
export type { VerifiedAgentRequest } from "./authentication/index.js";
export { Registry, RegistryError } from "./registry/index.js";
export { Identity, IdentityError, RegistrationError } from "./service.js";
export type { IdentityState, RegistrationRequest } from "./service.js";
