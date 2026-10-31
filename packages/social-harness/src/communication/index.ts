/** @file Reliable ordered messages retain one identity through sending, recovery and consumption. */

import { Context, Data, type Effect } from "effect";
import type { AgentView } from "../identity/cards.js";
import type { Message, MessageReference } from "./message.js";

/** Communication failures distinguish refused work from unknown acceptance. */
export class CommunicationError extends Data.TaggedError("CommunicationError")<{
  readonly reason:
    | "invalid-address"
    | "unknown-agent"
    | "membership-invalid"
    | "content-invalid"
    | "not-registered"
    | "version-mismatch"
    | "certification-unavailable"
    | "persistence-failed"
    | "network-unavailable"
    | "outcome-unknown"
    | "unknown-message"
    | "message-conflict"
    | "invalid-continuation"
    | "history-gap"
    | "outbox-too-old"
    | "agent-not-admitted";
}> {}

/** Conversation pages continue in conversation-id order. */
export interface ConversationQuery {
  readonly afterConversationId?: string;
}

/** Fixed participants identify a conversation independently of its displayed address. */
export interface ConversationPage {
  readonly conversations: ReadonlyArray<{
    readonly conversationId: string;
    readonly members: readonly AgentView[];
  }>;
  readonly hasMore: boolean;
}

/** A continuation keeps the first page's fixed history boundary. */
export type HistoryQuery =
  | Readonly<{ conversationId: string; afterMessage?: MessageReference }>
  | Readonly<{ continuation: string }>;

/** Verified messages stop at the head captured when traversal began. */
export interface MessagePage {
  readonly messages: readonly Message[];
  readonly continuation: string | null;
}

/** Callers work with messages while the live layer owns protocol workers and recovery. */
export class Communication extends Context.Tag("@moltzap/Communication")<
  Communication,
  {
    /** Durably accepts the immutable message before background certification begins. */
    readonly send: (
      message: Message,
    ) => Effect.Effect<void, CommunicationError>;
    /** Observes accepted work without submitting a replacement. */
    readonly waitForMessage: (
      message: MessageReference,
    ) => Effect.Effect<Message, CommunicationError>;
    /** Retires a handoff after durable handler success and stops before later work on failure. */
    readonly consume: <E, R>(
      handle: (message: Message) => Effect.Effect<void, E, R>,
    ) => Effect.Effect<never, CommunicationError | E, R>;
    /** Lists at most fifty conversations without reading recovery state. */
    readonly listConversations: (
      query: ConversationQuery,
    ) => Effect.Effect<ConversationPage, CommunicationError>;
    /** Reads at most fifty verified messages from a fixed traversal. */
    readonly readHistory: (
      query: HistoryQuery,
    ) => Effect.Effect<MessagePage, CommunicationError>;
  }
>() {}
