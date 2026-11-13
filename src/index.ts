/** @file MCP contracts and the values clients use to send messages and read their saved inbox. */
// safer-arch-ignore no-folder-cycle: Folder grouping combines pure contracts and scoped implementations; configured service domains enforce dependency direction.

/** MCP tool arguments, results and failures derive from these schemas. */
export {
  MAXIMUM_WAIT_SECONDS,
  type ToolCallFailure,
  toolDefinitions,
  type ToolName,
  type ToolRequest,
  type ToolResult,
} from "./harness/mcp/contracts.js";
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
} from "./harness/mcp/client.js";
/** Send requests and accepted-work receipts. */
export { SendRequest, type SendResult } from "./harness/message-format.js";
/** Results and policy feedback that a host may show its agent. */
export { FirewallRefusal, PermittedItem, Withheld } from "./firewall/index.js";
/** Policy failures retain actionable reasons and bounded field information. */
export { FirewallPolicyError } from "./firewall/index.js";
/** Guidance contains only the source text local policy permits the agent to read. */
export type { CheckedGuidance } from "./firewall/index.js";
/** Inbox items retain their content and token after acknowledgment. */
export {
  DeliveryAcknowledgeError,
  DeliveryToken,
  InboxEntry,
  InboxError,
  InboxEvent,
} from "./harness/inbox.js";
/** History uses the saved policy view and never rechecks old content. */
export { HistoryError, type HistoryRecord } from "./harness/social-harness.js";
/** Message, question, answer and collective result values. */
export {
  Answer,
  Message,
  Request,
  RequestedSchema,
  RequestError,
} from "./communication/index.js";
/** Names and addresses used in host messages. */
export {
  AgentAddress,
  Content,
  GroupAddress,
  MessageAddressInput,
  MsgId,
} from "./communication/index.js";
/** A send error classifies correction, retained work and uncertain acceptance. */
export { SendError } from "./communication/index.js";
/** The identifiers and name an agent is known by. */
export {
  AgentCard,
  AgentId,
  AgentName,
  PrincipalId,
} from "./identity/index.js";
/** Installed package version. */
export { packageVersion } from "./version.js";
/** Notification name that prompts clients to read their saved inbox. */
export { INBOX_PENDING_EVENT } from "./harness/mcp/contracts.js";
