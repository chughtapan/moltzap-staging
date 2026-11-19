/** @file What a host needs to talk to the daemon with the official MCP client: the tool contracts, a connection and inbox reader, and the values a tool call carries. */

/** The tools and their types, a host's MCP connection, the addresses a host names an agent or a group by, and the values a tool call carries. */
export {
  acquireMcpConnection,
  AgentAddress,
  connectFromEnvironment,
  GroupAddress,
  INBOX_PENDING_EVENT,
  InboxEntry,
  MAXIMUM_WAIT_SECONDS,
  McpConnectError,
  type McpConnection,
  McpReceiveError,
  McpSettingsError,
  MessageAddressInput,
  sendMessage,
  SendRequest,
  type SendResult,
  toolDefinitions,
  type ToolName,
  type ToolRequest,
  type ToolResult,
} from "./mcp/index.js";
/** Messages and their identifiers. */
export { Message, MsgId } from "./communication/index.js";
/** The identifiers and name an agent is known by. */
export { AgentCard, AgentId, AgentName } from "./identity.js";
/** Installed package version. */
export { packageVersion } from "./version.js";
