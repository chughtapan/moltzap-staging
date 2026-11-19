/** @file What crosses MCP between a host and the daemon: the tool contracts. It loads no database. */

/** The tools, the types derived from their schemas and the longest wait a read accepts. */
export {
  MAXIMUM_WAIT_SECONDS,
  toolDefinitions,
  type ToolName,
  type ToolRequest,
  type ToolResult,
} from "./tools.js";
/** What a host sends, the addresses it names an agent or a group by, and the inbox entries it reads. */
export {
  AgentAddress,
  DeliveryToken,
  InboxEntry,
  MessageAddressInput,
  SendRequest,
  type SendResult,
} from "./tools.js";
/** The address of a group, which only P2P mode offers. */
export { GroupAddress } from "./groups.js";
