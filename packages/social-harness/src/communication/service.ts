/** @file The communication service: accepts messages and questions, and supplies ordered history and incoming work. */

import { Context, Layer } from "effect";
import type { AgentName } from "../identity/agent-cards.js";
import type {
  CommunicationService,
  CommunicationSettings,
} from "./contract.js";
import { makeDelivery } from "./delivery.js";
import { bindCommunicationMode } from "./mode.js";

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
   * P2P: any agent on the daemon messages any other, alone or in a group.
   * `claim` marks the database as P2P before any agent starts and fails if it
   * already belongs to client/server mode.
   */
  static readonly p2p = {
    claim: bindCommunicationMode("p2p"),
    layer: (settings: CommunicationSettings) =>
      Layer.scoped(Communication, makeDelivery(settings)),
  };

  /**
   * Client/server: every conversation is the server agent and one client.
   * `claim` marks the database as belonging to this mode and this server
   * agent, and fails if it belongs to P2P mode or, once messages exist, to
   * another server agent.
   */
  static readonly clientServer = {
    claim: (serverAgentName: AgentName) =>
      bindCommunicationMode("client-server", serverAgentName),
    layer: (
      settings: CommunicationSettings & { readonly serverAgentName: AgentName },
    ) => Layer.scoped(Communication, makeDelivery(settings)),
  };
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
