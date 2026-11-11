/** @file Offline evaluation and process fixtures under the test import condition. */

/** Replaces a configured decision provider around the daemon's own composition. */
export { DecisionProviderOverride } from "./firewall/index.js";
/** Offline firewall checks with a caller-supplied provider. */
export {
  type DecisionProvider,
  DecisionProviderError,
  type DecisionRequest,
  type Message,
} from "./firewall/index.js";

/** Trusted database qualification uses the same schema, ownership and inbox operations as the daemon. */
export {
  checkDatabase,
  migrateDaemon,
  prepareDatabase,
} from "./database/migrations.js";
/** Database selection uses the production driver composition. */
export { databaseAccess, databaseConnection } from "./database/sql.js";
/** Restricted-role tests exercise production transaction scoping. */
export { withAgentTransaction } from "./database/transactions.js";
/** Process qualification persists deliveries through the daemon inbox. */
export { makeInbox } from "./harness/inbox.js";
/** Creates agents for owners the way the daemon does. */
export { getOrCreateOwnedAgent, localOwner } from "./harness/ownership.js";

/** Database qualification drives the daemon's acceptance and request recovery operations. */
export {
  acceptMessage,
  encodeRuntimeValue,
  putCollectiveOperation,
  readAcceptedMessage,
  readCollectiveOperation,
  readCollectiveOperationIds,
  retireCollectiveOperation,
  transaction,
} from "./communication/store.js";

/** Integration fixtures compose the existing domain services without startup overrides. */
export { loadDaemonSettings } from "./harness/config.js";

/** Policy process fixtures use the same per-agent webhook capability as startup. */
export { makeWebhookEvents } from "./harness/mcp/webhook.js";

/** Trusted process fixtures supply the production callback HTTP client. */
export { webhookHttpClientLayer } from "./harness/mcp/webhook-client.js";
