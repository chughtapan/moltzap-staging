/** @file Messages as values, and their failures. `service.ts` stores and reads them for each agent. */

/** Messages as validated values, why a send or a read was refused and the largest membership of a conversation. */
export {
  CommunicationError,
  Cursor,
  Message,
  messageWithId,
  MsgId,
  Page,
  PAGE_SIZE,
  PaginatedRequest,
  wellFormedString,
} from "./messages.js";
