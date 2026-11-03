/** @file Offline evaluation and process fixtures under the test import condition. */

/** Wire-version overrides for process tests. */
export { HISTORY_WIRE_VERSIONS, WireVersions } from "./version.js";
/** HTTP signing for process fixtures. */
export { signAgentRequest } from "./identity/http-authentication.js";
/** Offline firewall checks with a caller-supplied provider. */
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

/** Trusted database qualification uses the same schema, ownership and inbox operations as the daemon. */
export { migrateDaemon } from "./database/migrations.js";
/** Database selection uses the production driver composition. */
export { databaseConnection } from "./database/sql.js";
/** Restricted-role tests exercise production transaction scoping. */
export { withAgentTransaction } from "./database/transactions.js";
/** Process qualification persists deliveries through the daemon inbox. */
export { makeInbox } from "./harness/inbox.js";
/** Concurrent admission tests retain production ownership and key generation. */
export { getOrCreateOwnedAgent } from "./harness/ownership.js";
