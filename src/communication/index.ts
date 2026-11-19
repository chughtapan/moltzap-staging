/** @file Messages as values, and their failures. The service that carries them between agents is `service.ts`. */

/** Messages as validated values, why a send or a read was refused and the largest membership of a conversation. */
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
  wellFormedString,
} from "./messages.js";
