/** @file What a host needs to talk to the daemon: the MCP tool contracts, a typed MCP connection, and the values and failures a tool call carries. */

/** The tools and their types, a host's MCP connection with its typed calls, and the values and failures a tool call carries. */
export {
  acquireMcpConnection,
  callTool,
  callToolAsWritten,
  connectFromEnvironment,
  DeliveryAcknowledgeError,
  DeliveryToken,
  HistoryError,
  type HistoryRecord,
  INBOX_PENDING_EVENT,
  InboxEntry,
  InboxError,
  MAXIMUM_WAIT_SECONDS,
  McpConnectError,
  type McpConnection,
  type McpConnectionSettings,
  McpReceiveError,
  McpSettingsError,
  McpToolError,
  SendRequest,
  type SendResult,
  StoredInboxEntry,
  type ToolCallFailure,
  toolDefinitions,
  type ToolFailure,
  type ToolName,
  type ToolRequest,
  type ToolResult,
} from "./mcp/index.js";
/** The content policy permits an agent to read, and why policy refused a send or an edit. */
export {
  FirewallPolicyError,
  FirewallRefusal,
  PermittedItem,
} from "./firewall/index.js";
/** Messages, their identifiers and addresses, and why a send, a collective request or a collective response was refused. */
export {
  AgentAddress,
  GroupAddress,
  Message,
  MessageAddressInput,
  MsgId,
  RequestError,
  SendError,
} from "./communication/index.js";
/** The identifiers and name an agent is known by. */
export { AgentCard, AgentId, AgentName } from "./identity.js";
/** Installed package version. */
export { packageVersion } from "./version.js";
