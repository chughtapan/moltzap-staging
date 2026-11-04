/** @file MCP contracts and the values clients use to send messages and read their saved inbox. */
// safer-arch-ignore no-folder-cycle: Folder grouping combines pure contracts and scoped implementations; configured service domains enforce dependency direction.

/** MCP tool arguments, results and failures derive from these schemas. */
export {
  toolDefinitions,
  toolFailureSchemas,
  type ToolName,
  type ToolRequest,
  type ToolResult,
} from "./harness/mcp/contracts.js";
/** Clients distinguish a refused send from an unknown outcome using this decoder. */
export { decodeHarnessSendErrorData } from "./harness/mcp/contracts.js";
/** Send requests, accepted-work receipts and agent-facing text for host integrations. */
export {
  groupMembers,
  type GuidanceBodyOrigin,
  MessageTextError,
  parseMessageText,
  renderCollectiveRequest,
  renderCollectiveResult,
  renderContent,
  renderLocalGuidance,
  replyAddress,
  SendRequest,
  type SendResult,
  SendResultSchema,
} from "./harness/message-format.js";
/** Results and policy feedback that a host may show its agent. */
export {
  CollectiveResult,
  FirewallRefusal,
  LocalGuidance,
  PermittedItem,
  Withheld,
} from "./firewall/service.js";
/** Policy failures retain actionable reasons and bounded field information. */
export { FirewallPolicyError } from "./firewall/rules.js";
/** Guidance contains only the source text local policy permits the agent to read. */
export type { CheckedGuidance } from "./firewall/check.js";
/** Inbox items retain their content and token after acknowledgment. */
export {
  DeliveryAcknowledgeError,
  DeliveryToken,
  InboxEntry,
  InboxError,
  InboxEvent,
  InboxPage,
  MessageAddress,
} from "./harness/inbox.js";
/** History uses the saved policy view and never rechecks old content. */
export {
  HistoryError,
  type HistoryRecord,
  type ListConversationsRequest,
  type ListConversationsResult,
  type ReadHistoryRequest,
  type ReadHistoryResult,
} from "./harness/social-harness.js";
/** Registration failures are distinct from communication startup. */
export { RegistrationError } from "./identity/service.js";
/** Message, question, answer and collective result values. */
export {
  Answer,
  AnswerContent,
  type AnswerInput,
  AnswerOutcome,
  CollectiveId,
  Message,
  MessageOperation,
  MessageReference,
  PaginatedRequest,
  PaginatedResult,
  ReceivedRequest,
  Request,
  RequestedSchema,
  RequestError,
  RequestResult,
  ResultSources,
} from "./communication/service.js";
/** Names and addresses used in host messages. */
export {
  AgentAddress,
  Content,
  ContentPart,
  GroupAddress,
  JsonValue,
  MessageAddressInput,
  MsgId,
} from "./communication/messages.js";
/** A send error classifies correction, retained work and uncertain acceptance. */
export { SendError } from "./communication/errors.js";
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
