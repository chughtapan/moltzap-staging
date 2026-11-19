/** @file What a host needs to talk to the daemon with the official MCP client: the tool contracts and the values a tool call carries. */

/** The tools and their types, the addresses a host names an agent or a group by, and the values a tool call carries. */
export {
  AgentAddress,
  GroupAddress,
  InboxEntry,
  MAXIMUM_WAIT_SECONDS,
  MessageAddressInput,
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
