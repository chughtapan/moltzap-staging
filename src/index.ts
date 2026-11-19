/** @file What a host needs to talk to the daemon with the official MCP client: the tool contracts, a connection and inbox reader, and the values a tool call carries. */

/** The tools and their types, the addresses a host names an agent or a group by, and the values a tool call carries. */
export {
  AgentAddress,
  DeliveryToken,
  InboxEntry,
  MAXIMUM_WAIT_SECONDS,
  MessageAddressInput,
  SendRequest,
  type SendResult,
  toolDefinitions,
  type ToolName,
  type ToolRequest,
  type ToolResult,
} from "./mcp/tools.js";
/** A host's MCP connection to the daemon, its inbox reception, and a send whose outcome is always stated. */
export {
  acquireMcpConnection,
  connectFromEnvironment,
  McpConnectError,
  type McpConnection,
  McpReceiveError,
  McpSettingsError,
  sendMessage,
} from "./mcp/connection.js";
/** Messages and their identifiers. */
export { Message, MsgId } from "./communication/index.js";
/** The address of a group, which only P2P mode offers. */
export { GroupAddress } from "./mcp/groups.js";
/** The identifiers and name an agent is known by. */
export { AgentCard, AgentId, AgentName } from "./identity.js";
/** Installed package version. */
export { packageVersion } from "./version.js";
