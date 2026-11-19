/** @file What crosses MCP between a host and the daemon: the tool contracts. It loads no database. */

/** The tools, the types derived from their schemas and the longest wait a read accepts. */
export {
  MAXIMUM_WAIT_SECONDS,
  toolDefinitions,
  type ToolName,
  type ToolRequest,
  type ToolResult,
} from "./tools.js";
/** What a host sends, the address it names an agent by, and the inbox entries it reads. */
export {
  AgentAddress,
  DeliveryToken,
  InboxEntry,
  SendRequest,
  type SendResult,
} from "./tools.js";
