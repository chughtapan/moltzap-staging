/** @file Messages as values, and their failures. The service that carries them between agents is `service.ts`. */

/** Messages as validated values, why a send or a read was refused, the largest membership of a conversation, and the form a filled set of fields is checked against. */
export {
  CommunicationError,
  maximumMembers,
  Message,
  MessageReference,
  messageWithId,
  MsgId,
  Page,
  PAGE_SIZE,
  PaginatedRequest,
  RequestedSchema,
  validateResponseContent,
  wellFormedString,
} from "./messages.js";
