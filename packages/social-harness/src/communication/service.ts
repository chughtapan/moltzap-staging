/** @file The communication service: accepts messages and questions, and supplies ordered history and incoming work. */

import { Context, Layer } from "effect";
import type {
  CommunicationService,
  CommunicationSettings,
} from "./contract.js";
import { makeDelivery } from "./delivery.js";
import { claimMode } from "./mode.js";

/** The values the service accepts, hands over and returns in both modes. */
export {
  type Conversation,
  type ConversationQuery,
  type HistoryPage,
  type HistoryQuery,
  Incoming,
  Outgoing,
} from "./contract.js";

/** Delivers messages, completes questions and reads ordered history for one agent. */
export class Communication extends Context.Tag("SocialHarness/Communication")<
  Communication,
  CommunicationService
>() {
  /**
   * Marks the database as serving one mode before any agent starts, and fails
   * with `membership-invalid` when it already serves another.
   */
  static readonly claim = claimMode;

  /** The service of the agent `settings` names. */
  static readonly layer = (settings: CommunicationSettings) =>
    Layer.scoped(Communication, makeDelivery(settings));
}

/** Caller failures distinguish refusal from retained or uncertain progress. */
export { CommunicationError } from "./errors.js";
/** Messages and requests retain the same semantic values at every service handoff. */
export {
  AcceptResponse,
  Answer,
  AnswerOutcome,
  CollectiveId,
  DeadlineSeconds,
  DeclineResponse,
  decodeCollectiveFailure,
  Message,
  MessageReference,
  PaginatedRequest,
  PaginatedResult,
  ReceivedRequest,
  Request,
  RequestedSchema,
  RequestError,
  RequestOperation,
  ResultSources,
} from "./messages.js";
