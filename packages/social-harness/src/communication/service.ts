/** @file The communication service: accepts messages and questions, and supplies ordered history and incoming work. */

import { Context, Layer } from "effect";
import type { AgentName } from "../identity/agent-cards.js";
import type {
  CommunicationService,
  CommunicationSettings,
} from "./contract.js";
import { makeClientServer } from "./client-server/delivery.js";
import { bindCommunicationMode } from "./mode.js";
import { makeP2p } from "./p2p/index.js";

/** Semantic handoffs are shared by every delivery mode. */
export {
  type Conversation,
  type ConversationQuery,
  type HistoryPage,
  type HistoryQuery,
  Incoming,
  Outgoing,
} from "./contract.js";

/** Owns durable message delivery, request completion and verified history. */
export class Communication extends Context.Tag("SocialHarness/Communication")<
  Communication,
  CommunicationService
>() {
  /**
   * P2P: an agent certifies each message with its recipients and exchanges it
   * through the Router. `claim` marks the database as P2P before any agent
   * starts and fails if it already belongs to client/server mode. The layer
   * runs delivery and request workers until its scope closes; reopening the
   * same store with the same identity resumes accepted work.
   */
  static readonly p2p = {
    claim: bindCommunicationMode("p2p"),
    layer: (settings: CommunicationSettings) =>
      Layer.scoped(Communication, makeP2p(settings)),
  };

  /**
   * Client/server: the daemon orders and stores the messages between the
   * server agent and each client itself. `claim` marks the database as
   * belonging to this mode and this server agent, and fails if it belongs to
   * P2P mode or, once messages exist, to another server agent.
   */
  static readonly clientServer = {
    claim: (serverAgentName: AgentName) =>
      bindCommunicationMode("client-server", serverAgentName),
    layer: (
      settings: CommunicationSettings & { readonly serverAgentName: AgentName },
    ) => Layer.scoped(Communication, makeClientServer(settings)),
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
