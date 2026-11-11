/** @file What the communication service accepts, hands to the host and returns as history. */
import { type Effect, Schema } from "effect";
import type { AgentCard, AgentName } from "../identity/agent-cards.js";
import type { CommunicationError } from "./errors.js";
import { exactStruct } from "../encoding/index.js";
import {
  type CollectiveId,
  Message,
  type MessageReference,
  type PaginatedRequest,
  type PaginatedResult,
  ReceivedRequest,
  Request,
  type RequestError,
  RequestResult,
  ResultSources,
} from "./messages.js";
/** All conversation members, in ascending agent-name order. */
export type Conversation = readonly [
  Message["author"],
  Message["author"],
  ...Array<Message["author"]>,
];
/** Omit cursor to begin a live participant traversal. */
export interface ConversationQuery extends PaginatedRequest {}
/** A missing nextCursor ends the live traversal. */
interface ConversationPage extends PaginatedResult {
  readonly conversations: readonly Conversation[];
}
/** Selects a paged forward traversal or one bounded recent window. */
export type HistoryQuery =
  | (PaginatedRequest & {
      readonly members: Conversation;
      readonly order?: "oldest";
    })
  | {
      readonly members: Conversation;
      readonly order: "newest";
      readonly limit: number;
      readonly beforeMessage?: MessageReference;
    };
/** Messages retain conversation order and count only their encoded value bytes. */
export interface HistoryPage extends PaginatedResult {
  readonly entries: ReadonlyArray<{
    readonly message: Message;
    readonly messageBytes: number;
  }>;
  readonly earlierMessagesOmitted: boolean;
}
/* eslint-disable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Named Effect schemas and their decoded values share one public contract. */
/** Encodes the fixed semantic work that Communication accepts. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- The owning handoff grammar lets Main persist Message values through their encoder.
export const Outgoing = Schema.Union(Message, Request);
/** Final outgoing work is fixed before policy assessment. */
export type Outgoing = typeof Outgoing.Type;
/** Incoming work retains encoded Message values at storage boundaries. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- The owning handoff grammar lets Main persist Message values through their encoder.
export const Incoming = Schema.Union(
  exactStruct({ kind: Schema.Literal("message"), message: Message }),
  exactStruct({ kind: Schema.Literal("request"), message: ReceivedRequest }),
  exactStruct({
    kind: Schema.Literal("result"),
    result: RequestResult,
    sources: ResultSources,
  }),
);
/** Incoming work awaits local policy before the host receives it. */
export type Incoming = typeof Incoming.Type;

/** One agent's sends, incoming work and history. */
export interface CommunicationService {
  /**
   * Returns once a message is ordered and saved for every member, or a
   * question is saved. Retry the same `Message` or `Request`, retaining its
   * `msgId` or `operationId`.
   */
  readonly send: (
    value: Outgoing,
  ) => Effect.Effect<void, CommunicationError | RequestError>;
  /** Checks current answer eligibility without reserving the request. */
  readonly getRequest: (
    operationId: CollectiveId,
  ) => Effect.Effect<ReceivedRequest, CommunicationError | RequestError>;
  /**
   * Calls the handler for incoming work and retires the item after success.
   * Failed or interrupted handling leaves it pending. Failed retirement can
   * replay a successfully handled item, so handlers must tolerate repeats.
   */
  readonly consume: <E, R>(
    handle: (incoming: Incoming) => Effect.Effect<void, E, R>,
  ) => Effect.Effect<never, CommunicationError | RequestError | E, R>;
  /** Finds a retained conversation by all member names, including self. */
  readonly getConversation: (
    names: readonly AgentName[],
  ) => Effect.Effect<Conversation | undefined, CommunicationError>;
  /** Lists retained conversations. Cursors expire when this service instance ends. */
  readonly listConversations: (
    query?: ConversationQuery,
  ) => Effect.Effect<ConversationPage, CommunicationError>;
  /** Reads a conversation's messages in order. Continuation cursors require this service instance. */
  readonly readHistory: (
    query: HistoryQuery,
  ) => Effect.Effect<HistoryPage, CommunicationError>;
}

/* eslint-enable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Restore naming after schema/type pairs. */

/** The agent a communication service acts for. */
export interface CommunicationSettings {
  /** The key the agent's rows are stored under. */
  readonly agentKey: string;
  readonly card: AgentCard;
  /** The most messages one newest-first history read may ask for. */
  readonly maximumHistoryRecords?: number;
  /** Whether these agents may share a conversation. Without it any may. */
  readonly permits?: (members: readonly AgentCard[]) => boolean;
}
