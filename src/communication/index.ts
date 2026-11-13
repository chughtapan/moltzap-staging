/** @file Messages, questions and answers, the service that delivers them, and their failures. */

/** Messages, addresses, questions, answers and results as validated values. */
export {
  AcceptResponse,
  AGENT_ADDRESS_PREFIX,
  AgentAddress,
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
/** What the service accepts, hands over and returns. */
export {
  type Conversation,
  type ConversationQuery,
  type HistoryPage,
  type HistoryQuery,
  Incoming,
  type Outgoing,
} from "./service.js";
/** Delivers messages, completes questions and reads ordered history for one agent. */
export { Communication } from "./service.js";
/** Why a send, a read or a question was refused. */
export {
  CommunicationError,
  issuesText,
  SendError,
  sendFailureReasons,
  type ValidationIssue,
} from "./errors.js";
/** Keeps a background loop of one agent running after a failure. */
export { restarting } from "./collectives.js";
