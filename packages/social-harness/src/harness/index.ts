/** @file The harness: host message formats, the saved inbox, history, agent ownership, daemon settings and the MCP tool contracts. */

/** Parses a host's send request and renders messages as the text an agent reads. */
export {
  groupMembers,
  MessageTextError,
  parseMessageText,
  renderInboxItem,
  replyAddress,
  SendRequest,
  type SendResult,
} from "./message-format.js";
/** The saved inbox, its entries and events, and delivery acknowledgment. */
export {
  DeliveryAcknowledgeError,
  DeliveryToken,
  InboxEntry,
  InboxError,
  InboxEvent,
  makeInbox,
} from "./inbox.js";
/** History records as the agent's policy let it see them. */
export { HistoryError, type HistoryRecord } from "./social-harness.js";
/** Binds an owner to an agent and loads its retained registration. */
export {
  getOrCreateOwnedAgent,
  loadAgentRegistration,
  localOwner,
} from "./ownership.js";
/** Loads daemon settings with the policy and norms they reference. */
export { loadDaemonSettings } from "./config.js";
/** MCP tool arguments, results and failures, and the inbox notification name. */
export {
  INBOX_PENDING_EVENT,
  MAXIMUM_WAIT_SECONDS,
  type ToolCallFailure,
  toolDefinitions,
  type ToolName,
  type ToolRequest,
  type ToolResult,
} from "./mcp/contracts.js";
/** Wakes a host's subscribed callback when its inbox has something to read. */
export { makeWebhookEvents } from "./mcp/webhook.js";
/** The HTTP client that signs and sends those callbacks. */
export { webhookHttpClientLayer } from "./mcp/webhook-client.js";
