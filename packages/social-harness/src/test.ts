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
/** Concurrent admission tests retain production ownership and key generation. */
export { getOrCreateOwnedAgent } from "./harness/ownership.js";

/** Database qualification drives the daemon's acceptance and request recovery operations. */
export {
  acceptMessage,
  encodeRuntimeValue,
  failMessage,
  initializeCommunication,
  putCollectiveOperation,
  readAcceptedMessage,
  readCollectiveOperation,
  readCollectiveOperationIds,
  readUnpreparedMessages,
  retireCollectiveOperation,
  transaction,
} from "./communication/store.js";

/** Integration fixtures compose the existing domain services without startup overrides. */
export { loadDaemonSettings, spanExport } from "./harness/config.js";
/** Retained registration inputs are shared with process fixtures. */
export { loadAgentRegistration, localOwner } from "./harness/ownership.js";
/** Policy fixtures use the production MCP catalogue and authentication guard. */
export {
  makeAgentMcpApp,
  makeMcpHttpApp,
  serveMcp,
} from "./harness/mcp/server.js";
/** Real Router connections are retained in deterministic provider traces. */
export { Router } from "./communication/router/client.js";

/** Policy process fixtures use the same per-agent webhook capability as startup. */
export { makeWebhookEvents } from "./harness/mcp/webhook.js";
/** Trusted process fixtures supply the production callback HTTP client. */
export { webhookHttpClientLayer } from "./harness/mcp/webhook-client.js";
