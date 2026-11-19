/** @file What crosses MCP between a host and the daemon: the tool contracts. It loads no database. */

/** The tools and the types derived from their schemas. */
export {
  toolDefinitions,
  type ToolName,
  type ToolRequest,
  type ToolResult,
} from "./tools.js";
/** What a host sends, the addresses it names an agent or a group by, the inbox entries it reads and the event that carries them. */
export {
  AgentAddress,
  INBOX_EVENT,
  InboxEntry,
  MessageAddressInput,
  SendRequest,
  type SendResult,
} from "./tools.js";
/** The address of a group, which only P2P mode offers. */
export { GroupAddress } from "./groups.js";
