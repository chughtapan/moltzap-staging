/** @file MCP contracts and the values clients use to send messages and read their saved inbox. */
// safer-arch-ignore no-folder-cycle: Folder grouping combines pure contracts and scoped implementations; configured service domains enforce dependency direction.

/** A tool's schemas own its arguments, successful result and declared failures. */
export {
  toolDefinitions,
  toolFailureSchemas,
  type ToolName,
  type ToolRequest,
  type ToolResult,
} from "./harness/mcp/contracts.js";
/** Clients distinguish a refused send from an unknown outcome using this decoder. */
export { decodeHarnessSendErrorData } from "./harness/mcp/contracts.js";
/** Host syntax is decoded once before resolving its destination. */
export { SendRequest } from "./host-protocol/messages.js";
/** Native hosts can translate a text command through the same request grammar. */
export { MessageTextError, parseMessageText } from "./host-protocol/render.js";
/** Host-neutral display keeps local advice separate from peer content. */
export {
  type GuidanceBodyOrigin,
  renderLocalGuidance,
} from "./host-protocol/render.js";
/** Presentation helpers use the delivered content and its saved policy view. */
export {
  groupMembers,
  renderCollectiveRequest,
  renderCollectiveResult,
  renderContent,
  replyAddress,
} from "./host-protocol/render.js";
/** The same refusal and policy-error schemas serve the server and clients. */
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
/** Accepted sends have one receipt shape for messages, replies and questions. */
export { type SendResult, SendResultSchema } from "./host-protocol/messages.js";
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
} from "./harness/service.js";
/** Registration failures are distinct from communication startup. */
export { RegistrationError } from "./identity/service.js";
/** Optional traffic export records admitted incoming content and completed sends. */
export { HistoryExportRecord } from "./harness/message-log.js";
/** Shared values carry message identity and decoded request/reply meaning. */
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
} from "./communication/index.js";
/** Explicit host destinations are independent of transport routing. */
export {
  AgentAddress,
  Content,
  ContentPart,
  GroupAddress,
  JsonValue,
  MessageAddressInput,
  MsgId,
} from "./communication/records/values.js";
/** A send error classifies correction, retained work and uncertain acceptance. */
export { SendError } from "./communication/conversations/errors.js";
/** Public identity and its complete signed document have distinct names. */
export {
  AgentCard,
  AgentId,
  AgentIdentityCard,
  AgentName,
  PrincipalId,
  type VerifiedAgentIdentityCard,
} from "./identity/agent.js";
/** Signing authority exposes no private key bytes. */
export { AgentPublicKey, AgentSigningAuthority } from "./identity/keys.js";
/** Services and verification tools use attributed envelopes at the network boundary. */
export {
  EnvelopeId,
  type LiveSignedMessage,
  SignedMessage,
  type VerifiedSignedMessage,
} from "./identity/envelope/signed-message.js";
/** Emission and compatibility use the installed release's wire policy. */
export { LIVE_WIRE_VERSIONS, WIRE_VERSION } from "./identity/wire-version.js";
/** Clients report the shipped core release without another version copy. */
export { packageVersion } from "./release.js";
/** Native notifications signal that the client should traverse its inbox again. */
export { INBOX_PENDING_EVENT } from "./harness/mcp/contracts.js";
