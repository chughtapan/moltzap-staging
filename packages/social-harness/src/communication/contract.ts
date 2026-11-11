/** @file Semantic communication handoffs shared by delivery implementations. */
import { type Effect, Schema } from "effect";
import type {
  AgentName,
  AgentPublicKey,
  AgentSigningAuthority,
  VerifiedAgentIdentityCard,
} from "../identity/index.js";
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
      readonly afterMessage?: MessageReference;
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
  /**
   * This agent's accepted messages that delivery has not ordered yet, in the
   * order they were accepted. They are not in `entries`.
   */
  readonly pendingOutgoing: readonly Message[];
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
  exactStruct({
    kind: Schema.Literal("request-failed"),
    request: Request,
    error: Schema.String,
  }),
);
/** Incoming work awaits local policy before the host receives it. */
export type Incoming = typeof Incoming.Type;

/** The delivery mode owns acceptance, ordering and verification behind this handoff. */
export interface CommunicationService {
  /**
   * Returns after durable acceptance. P2P certification continues in the
   * layer's scope; client/server ordering completes before returning. Retry the same `Message` or `Request`,
   * retaining its `msgId` or `operationId`.
   */
  readonly send: (
    value: Outgoing,
  ) => Effect.Effect<void, CommunicationError | RequestError>;
  /** Checks current answer eligibility without reserving the request. */
  readonly getRequest: (
    operationId: CollectiveId,
  ) => Effect.Effect<ReceivedRequest, CommunicationError | RequestError>;
  /**
   * Waits for a locally authored message to complete, or reports its retained
   * terminal failure. Unknown references fail with `unknown-message`.
   * Cancellation stops this wait and leaves accepted work running.
   */
  readonly waitForMessage: (
    reference: MessageReference,
  ) => Effect.Effect<Message, CommunicationError>;
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
  /** Reads verified application messages. Continuation cursors require this service instance. */
  readonly readHistory: (
    query: HistoryQuery,
  ) => Effect.Effect<HistoryPage, CommunicationError>;
}

/* eslint-enable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Restore naming after schema/type pairs. */

/** Signing and Registry authority belong to the one configured local agent. */
export interface CommunicationSettings {
  readonly agentKey: string;
  readonly identityCard: VerifiedAgentIdentityCard;
  readonly maximumHistoryRecords?: number;
  readonly signingAuthority: AgentSigningAuthority;
  readonly registrySignerPublicKey: AgentPublicKey;
}
