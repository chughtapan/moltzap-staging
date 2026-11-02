/** @file Condition-only offline evaluation and process-test configuration. */

/** Process tests supply the same compatibility policy as production services. */
export {
  HISTORY_WIRE_VERSIONS,
  WireVersions,
} from "./identity/wire-version.js";
/** Test processes create signed HTTP attempts with production signing authority. */
export { signAgentRequest } from "./identity/authentication/proof.js";
/** Offline model evaluation remains independent of storage and daemon startup. */
export {
  CheckInputError,
  checkMessage,
  type DecisionProvider,
  DecisionProviderError,
  type DecisionRequest,
  type DecisionResponse,
  type Message,
  type MessageContext,
} from "./firewall/check.js";
