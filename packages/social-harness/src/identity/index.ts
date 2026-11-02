/** @file Local registration and immutable identity values used across the harness. */
/** Agent cards name participants; identity cards retain the Registry's attestation. */
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
  type VerifiedAgentIdentityCard,
} from "./agent.js";
/** Request authentication verifies attribution and rejects replay. */
export {
  Authentication,
  AuthenticationError,
  type VerifiedAgentRequest,
} from "./authentication/index.js";
/** Registry calls bind admission and lookup to a trusted signer. */
export { Registry, RegistryError } from "./registry/index.js";
/** Local registration becomes visible after its binding is saved. */
export {
  Identity,
  IdentityError,
  type IdentityState,
  RegistrationError,
  type RegistrationRequest,
} from "./service.js";
