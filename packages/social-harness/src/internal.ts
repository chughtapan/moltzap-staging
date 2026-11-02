/** @file Condition-only network service contracts and identity verification capabilities. */

/** Bootstrap verification returns the retained proof accepted by Registry. */
export {
  httpErrors,
  type VerifiedHttpSignature,
  verifyBootstrapRequest,
} from "./identity/authentication/proof.js";
/** Registry issuance and history verification use the complete identity document. */
export {
  type AgentIdentityCardClaims,
  AgentIdentityCardDigest,
  agentIdentityCardSigning,
  AgentIdentityCardVerificationError,
} from "./identity/agent.js";
/** Registered HTTP requests expose verified attribution before operation decoding. */
export {
  Authentication,
  AuthenticationError,
  type VerifiedAgentRequest,
} from "./identity/authentication/service.js";
/** Service construction uses the same Registry client as local identity. */
export { Registry } from "./identity/registry/index.js";
/** Router operations and their exact wire contract belong to Communication. */
export { Router, routerContract } from "./communication/router/api.js";
/** Registry HTTP and storage adapters share their owning wire contract. */
export { registryContract } from "./identity/registry/contract.js";
/** Service processes apply the installed compatibility policy. */
export {
  HISTORY_WIRE_VERSIONS,
  LIVE_WIRE_VERSIONS,
  WIRE_VERSION,
  type WireVersion,
  wireVersions,
} from "./identity/wire-version.js";
