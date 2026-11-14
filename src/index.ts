/** @file What a host needs to talk to the daemon: the MCP tool contracts, a typed client, and the values and failures a tool call carries. */

/** The tools, the types derived from their schemas, the longest wait a read accepts and the notification that the inbox has something to read. */
export {
  INBOX_PENDING_EVENT,
  MAXIMUM_WAIT_SECONDS,
  type ToolCallFailure,
  toolDefinitions,
  type ToolName,
  type ToolRequest,
  type ToolResult,
} from "./harness/contracts.js";
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
} from "./harness/client.js";
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
} from "./harness/message-format.js";
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
