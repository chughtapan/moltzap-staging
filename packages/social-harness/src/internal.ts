/** @file Network service contracts available under the internal import condition. */

/** HTTP authentication for Registry bootstrap requests. */
export {
  httpErrors,
  type VerifiedHttpSignature,
  verifyBootstrapRequest,
} from "./identity/http-authentication.js";
/** Signing and verification of Registry-issued identity cards. */
export {
  type AgentIdentityCardClaims,
  AgentIdentityCardDigest,
  agentIdentityCardSigning,
  AgentIdentityCardVerificationError,
} from "./identity/agent-cards.js";
/** HTTP authentication for registered agents. */
export {
  Authentication,
  AuthenticationError,
  type VerifiedAgentRequest,
} from "./identity/http-authentication.js";
/** Registry lookup for network services. */
export { Registry } from "./identity/registry.js";
/** Router client and wire schemas shared with its server. */
export { Router, routerContract } from "./communication/router/client.js";
/** Registry wire schemas shared by its client, server and storage. */
export { registryContract } from "./identity/registry.js";
/** Wire versions emitted and accepted by this release. */
export {
  HISTORY_WIRE_VERSIONS,
  LIVE_WIRE_VERSIONS,
  WIRE_VERSION,
  type WireVersion,
  wireVersions,
} from "./version.js";
