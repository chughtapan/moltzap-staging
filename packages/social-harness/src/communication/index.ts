/** @file Reliable messages and request results, without protocol or host policy details. */
/** Acceptance and recovery use a closed failure vocabulary. */
export { CommunicationError } from "./errors.js";
/** Messages carry application content and decoded request meaning. */
export {
  Message,
  MessageOperation,
  PaginatedRequest,
  PaginatedResult,
} from "./message.js";
/** Message identities stay valid across retries and retained history. */
export { MessageReference } from "./records/values.js";
/** Requests normalize the response form before policy assessment. */
export { RequestedSchema } from "./requests/answers.js";
/** Request outcomes retain the original operation identity and deadline. */
export {
  AcceptResponse,
  AnswerContent,
  CollectiveId,
  DeadlineSeconds,
  DeclineResponse,
  decodeCollectiveFailure,
  RequestError,
  RequestOperation,
} from "./requests/contracts.js";
/** Constructors fix outgoing work and schemas preserve complete result evidence. */
export {
  Answer,
  type AnswerInput,
  AnswerOutcome,
  ReceivedRequest,
  Request,
  RequestResult,
  ResultSources,
} from "./requests/index.js";
/** One scoped service owns delivery, request completion and offline history. */
export {
  Communication,
  type Conversation,
  type ConversationQuery,
  type HistoryPage,
  type HistoryQuery,
  Incoming,
  Outgoing,
} from "./service.js";
