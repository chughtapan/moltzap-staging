/** @file What crosses MCP between a host and the daemon: the tool contracts, and what a host needs beyond the official MCP client. It loads no database. */

/** The tools, the types derived from their schemas and the longest wait a read accepts. */
export {
  MAXIMUM_WAIT_SECONDS,
  toolDefinitions,
  type ToolName,
  type ToolRequest,
  type ToolResult,
} from "./tools.js";
/** A host's MCP connection to the daemon, its inbox reception, and a send whose outcome is always stated. */
export {
  acquireMcpConnection,
  connectFromEnvironment,
  McpConnectError,
  type McpConnection,
  McpReceiveError,
  McpSettingsError,
  sendMessage,
} from "./connection.js";
/** What a host sends, and the inbox entries and history records it reads. */
export {
  DeliveryToken,
  InboxEntry,
  SendRequest,
  type SendResult,
} from "./tools.js";
