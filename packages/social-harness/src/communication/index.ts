/** @file Communication: the service that accepts messages and questions, the message values other layers read, and the Router client. */

/** The communication service, with one layer per delivery mode. */
export { Communication } from "./service.js";
/** Conversations, history pages and the incoming and outgoing work the service hands over. */
export {
  type Conversation,
  type ConversationQuery,
  type HistoryPage,
  type HistoryQuery,
  Incoming,
  type Outgoing,
} from "./contract.js";
/** Send and service failures, with the reasons and field issues a caller can show. */
export {
  CommunicationError,
  issuesText,
  SendError,
  sendFailureReasons,
  type ValidationIssue,
} from "./errors.js";
/** Messages, addresses, questions and answers, with their validation and paging values. */
export {
  AcceptResponse,
  AGENT_ADDRESS_PREFIX,
  AgentAddress,
  agentAddressOf,
  Answer,
  type AnswerContent,
  type AnswerFieldFailures,
  answerFieldsText,
  AnswerOutcome,
  CollectiveId,
  Content,
  DeadlineSeconds,
  DeclineResponse,
  decodeCollectiveFailure,
  encodeCollectiveContent,
  GROUP_ADDRESS_PREFIX,
  GroupAddress,
  MAXIMUM_DEADLINE_SECONDS,
  maximumMembers,
  Message,
  MessageAddressInput,
  messageFromParticipants,
  MessageReference,
  MsgId,
  PaginatedRequest,
  PaginatedResult,
  ReceivedRequest,
  Request,
  RequestedSchema,
  RequestError,
  RequestOperation,
  type ResultSources,
  validateAnswer,
  wellFormedString,
} from "./messages.js";
/** The Router client and the wire schemas it shares with the Router server. */
export { Router, routerContract } from "./p2p/router/client.js";
/** Stored messages and collective operations, which the package's test entry drives directly. */
export {
  acceptMessage,
  encodeRuntimeValue,
  failMessage,
  initializeCommunication,
  putCollectiveOperation,
  readAcceptedMessage,
  readCollectiveOperation,
  readCollectiveOperationIds,
  retireCollectiveOperation,
  transaction,
} from "./store.js";
/** P2P messages a peer has received but not yet prepared, for the package's test entry. */
export { readUnpreparedMessages } from "./p2p/conversations/store.js";
