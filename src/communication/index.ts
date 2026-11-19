/** @file Messages as values, and their failures. The service that carries them between agents is `service.ts`. */

/** Messages and addresses as validated values, and the form a filled set of fields is checked against. */
export {
  AGENT_ADDRESS_PREFIX,
  AgentAddress,
  Message,
  MessageAddressInput,
  MessageReference,
  messageWithId,
  MsgId,
  Page,
  PaginatedRequest,
  RequestedSchema,
  validateResponseContent,
  wellFormedString,
} from "./messages.js";
/** Group addresses, which only P2P mode offers. */
export {
  GroupAddress,
  groupAddressOf,
  groupMembers,
  maximumMembers,
} from "./groups.js";
/** Why a send or a read was refused. */
export {
  CommunicationError,
  issuesText,
  SendError,
  sendFailureReasons,
  type ValidationIssue,
} from "./errors.js";
