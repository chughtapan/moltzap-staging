/** @file Network service contracts available under the internal import condition. */

/** HTTP authentication for Registry bootstrap requests. */
export {
  httpErrors,
  type VerifiedHttpSignature,
  verifyBootstrapRequest,
} from "./identity/index.js";
/** Signing and verification of Registry-issued identity cards. */
export {
  AgentIdentityCardDigest,
  agentIdentityCardSigning,
} from "./identity/index.js";
/** HTTP authentication for registered agents. */
export {
  Authentication,
  AuthenticationError,
  type VerifiedAgentRequest,
} from "./identity/index.js";
/** Registry lookup for network services. */
export { Registry } from "./identity/index.js";
/** Router client and wire schemas shared with its server. */
export { Router, routerContract } from "./communication/index.js";
/** Registry wire schemas shared by its client, server and storage. */
export { registryContract } from "./identity/index.js";
/** Wire versions emitted and accepted by this release. */
export {
  HISTORY_WIRE_VERSIONS,
  LIVE_WIRE_VERSIONS,
  WIRE_VERSION,
  wireVersions,
} from "./version.js";
