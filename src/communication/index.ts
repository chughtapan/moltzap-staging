/** @file Messages as values, and their failures. The service that carries them between agents is `service.ts`. */

/** Messages as validated values, the largest membership of a conversation, and the form a filled set of fields is checked against. */
export {
  maximumMembers,
  Message,
  MessageReference,
  messageWithId,
  MsgId,
  Page,
  PaginatedRequest,
  RequestedSchema,
  validateResponseContent,
  wellFormedString,
} from "./messages.js";
/** Why a send or a read was refused. */
export {
  CommunicationError,
  issuesText,
  SendError,
  sendFailureReasons,
  type ValidationIssue,
} from "./errors.js";
