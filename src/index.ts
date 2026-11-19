/** @file What a host needs to talk to the daemon with the official MCP client: the tool contracts, a connection and inbox reader, and the values a tool call carries. */

/** The tools and their types, a host's MCP connection, and the values a tool call carries. */
export {
  acquireMcpConnection,
  connectFromEnvironment,
  DeliveryToken,
  type HistoryRecord,
  INBOX_PENDING_EVENT,
  InboxEntry,
  MAXIMUM_WAIT_SECONDS,
  McpConnectError,
  type McpConnection,
  type McpConnectionSettings,
  McpReceiveError,
  McpSettingsError,
  sendMessage,
  SendRequest,
  type SendResult,
  StoredInboxEntry,
  toolDefinitions,
  type ToolName,
  type ToolRequest,
  type ToolResult,
} from "./mcp/index.js";
/** Messages, their identifiers and addresses. */
export {
  AgentAddress,
  GroupAddress,
  Message,
  MessageAddressInput,
  MsgId,
} from "./communication/index.js";
/** The identifiers and name an agent is known by. */
export { AgentCard, AgentId, AgentName } from "./identity.js";
/** Installed package version. */
export { packageVersion } from "./version.js";
