/** @file What crosses MCP between a host and the daemon: the tool contracts and a host's typed connection. It loads no database. */

/** The tools, the types derived from their schemas, the longest wait a read accepts and the name of the event a wake-up carries. */
export {
  INBOX_PENDING_EVENT,
  MAXIMUM_WAIT_SECONDS,
  type ToolCallFailure,
  toolDefinitions,
  type ToolName,
  type ToolRequest,
  type ToolResult,
} from "./tools.js";
/** A host's MCP connection to the daemon, its typed tool calls and inbox reception. */
export {
  acquireMcpConnection,
  callTool,
  callToolAsWritten,
  connectFromEnvironment,
  McpConnectError,
  type McpConnection,
  type McpConnectionSettings,
  McpReceiveError,
  McpSettingsError,
  McpToolError,
  type ToolFailure,
} from "./connection.js";
/** What a host sends, the inbox entries and history records it reads, and why a read or an acknowledgment failed. */
export {
  DeliveryAcknowledgeError,
  DeliveryToken,
  HistoryError,
  type HistoryRecord,
  InboxEntry,
  InboxError,
  SendRequest,
  type SendResult,
  StoredInboxEntry,
} from "./tools.js";
