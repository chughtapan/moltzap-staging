/** @file Agent identity: cards and keys, signed messages, HTTP request authentication, the Registry client and the local identity service. */

/** Agent names, ids and cards, and the Registry-signed identity card with its signing. */
export {
  AGENT_NAME_RULE,
  AgentCard,
  agentCardOf,
  AgentId,
  AgentIdentityCard,
  AgentIdentityCardDigest,
  agentIdentityCardSigning,
  AgentName,
  PrincipalId,
  type VerifiedAgentIdentityCard,
} from "./agent-cards.js";
/** An agent's public key and its signing authority, which holds the private key. */
export { AgentPublicKey, AgentSigningAuthority } from "./keys.js";
/** Signs and verifies HTTP requests between agents and network services, with the failures a service returns. */
export {
  AgentSigningError,
  Authentication,
  AuthenticationError,
  AuthenticationFailedError,
  httpErrors,
  InternalServerError,
  MalformedRequestError,
  MethodNotAllowedError,
  OverloadedError,
  PayloadTooLargeError,
  type RegistryLookupResult,
  RouteNotFoundError,
  signAgentRequest,
  UnavailableError,
  UnsupportedMediaTypeError,
  type VerifiedAgentRequest,
  type VerifiedHttpSignature,
  verifyBootstrapRequest,
  VersionMismatchError,
} from "./http-authentication.js";
/** Signed and sealed messages bound to their sender and recipients. */
export {
  EnvelopeId,
  type LiveSignedMessage,
  SealedBody,
  SignedMessage,
  type SignedMessageVerificationError,
  type VerifiedSignedMessage,
} from "./message-security.js";
/** The Registry client and its wire schemas. */
export {
  RegistrationId,
  Registry,
  registryContract,
  RegistryError,
} from "./registry.js";
/** A Registry kept in the daemon's own database. */
export { localRegistry } from "./local-registry.js";
/** The service that saves the local registration and resolves other agents to verified cards. */
export {
  Identity,
  IdentityError,
  type RegistrationRequest,
} from "./service.js";
