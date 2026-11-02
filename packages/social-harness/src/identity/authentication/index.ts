/** @file Request attribution and replay admission for authenticated services. */
/** Authentication verifies attribution and reserves each accepted proof against replay. */
export {
  Authentication,
  AuthenticationError,
  type VerifiedAgentRequest,
} from "./service.js";
