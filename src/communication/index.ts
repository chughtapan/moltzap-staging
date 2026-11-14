/** @file Messages, collective requests and collective responses as values, and their failures. The service that delivers them is `service.ts`. */

/** Messages, addresses, collective requests, responses and results as validated values. */
export {
  AGENT_ADDRESS_PREFIX,
  AgentAddress,
  CollectiveOp,
  CollectiveOutcome,
  CollectiveRequest,
  CollectiveRequestMessage,
  CollectiveResponse,
  type CollectiveResultSources,
  DeadlineSeconds,
  decodeCollectiveFailure,
  encodeCollectiveContent,
  GROUP_ADDRESS_PREFIX,
  GroupAddress,
  maximumMembers,
  Message,
  MessageAddressInput,
  MessageReference,
  messageWithId,
  MsgId,
  PaginatedRequest,
  PaginatedResult,
  RequestedSchema,
  RequestError,
  type ResponseContent,
  type ResponseFieldFailures,
  responseFieldsText,
  validateResponseContent,
  wellFormedString,
} from "./messages.js";
/** Why a send, a read, a collective request or a collective response was refused. */
export {
  CommunicationError,
  issuesText,
  SendError,
  sendFailureReasons,
  type ValidationIssue,
} from "./errors.js";
