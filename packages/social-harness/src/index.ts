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
/** Send requests, accepted-work receipts and agent-facing text for host integrations. */
export {
  groupMembers,
  MessageTextError,
  parseMessageText,
  renderInboxItem,
  replyAddress,
  SendRequest,
  type SendResult,
} from "./harness/message-format.js";
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
/** Agent attribution and Registry-signed identity cards. */
export {
  AgentCard,
  AgentId,
  AgentIdentityCard,
  AgentName,
  PrincipalId,
  type VerifiedAgentIdentityCard,
} from "./identity/agent-cards.js";
/** Signing authority exposes no private key bytes. */
export { AgentPublicKey, AgentSigningAuthority } from "./identity/keys.js";
/** Signed network messages and their verification results. */
export {
  EnvelopeId,
  type LiveSignedMessage,
  SignedMessage,
  type VerifiedSignedMessage,
} from "./identity/message-security.js";
/** Installed package version and the wire versions this release emits and accepts. */
export { LIVE_WIRE_VERSIONS, packageVersion, WIRE_VERSION } from "./version.js";
/** Notification name that prompts clients to read their saved inbox. */
export { INBOX_PENDING_EVENT } from "./harness/mcp/contracts.js";
