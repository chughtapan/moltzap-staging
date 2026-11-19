/**
 * @file One agent's communication service on a daemon that hosts every agent.
 *
 * The file opens with what the service accepts, hands to the host and
 * returns, then reads an agent's conversations and their ordered history. A
 * send orders the message in its conversation and saves it for every member
 * in one transaction.
 */

import { SqlClient } from "@effect/sql/SqlClient";
import { Context, Effect, Layer, Predicate, Schedule, Schema } from "effect";
import { randomBytes } from "node:crypto";
import {
  afterCommit,
  executeQuery,
  transaction,
  withAgentRows,
} from "../database/index.js";
import { compareAgentIds } from "../encoding.js";
import { AgentCard } from "../identity.js";
import {
  CommunicationError,
  deriveConversationId,
  type Message,
  messageMembers,
  type MessageReference,
  type Page,
  type PaginatedRequest,
} from "./messages.js";
import {
  communicationQueries,
  hasMessage,
  markMessageConsumed,
  readMessages,
  saveMessage,
} from "./store.js";

/** All conversation members, in ascending agent-name order. */
export type Conversation = readonly [AgentCard, AgentCard, ...AgentCard[]];
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
export interface HistoryPage extends Page {
  readonly entries: ReadonlyArray<{
    readonly message: Message;
    readonly messageBytes: number;
  }>;
  readonly earlierMessagesOmitted: boolean;
}

/** One agent's sends, what it consumes and its history. */
export interface CommunicationService {
  /**
   * Returns once the message is ordered and saved for every member. Sending
   * the same `Message` again, with its `msgId`, changes nothing.
   */
  readonly send: (message: Message) => Effect.Effect<void, CommunicationError>;
  /**
   * Calls the handler for each message this agent receives and marks it
   * consumed after success. Failed or interrupted handling leaves it to be
   * handed over again. One that was handled can also be handed over again
   * when marking it fails, so handlers must tolerate repeats.
   */
  readonly consume: <E, R>(
    handle: (message: Message) => Effect.Effect<void, E, R>,
  ) => Effect.Effect<never, CommunicationError | E, R>;
  /** Every conversation this agent has a message in, in no order. */
  readonly listConversations: Effect.Effect<
    readonly Conversation[],
    CommunicationError
  >;
  /** Reads a conversation's messages in order. Continuation cursors require this service instance. */
  readonly readHistory: (
    query: HistoryQuery,
  ) => Effect.Effect<HistoryPage, CommunicationError>;
}

/** The agent a communication service acts for. */
export interface CommunicationSettings {
  /** The key the agent's rows are stored under. */
  readonly agentKey: string;
  readonly card: AgentCard;
  /**
   * The latch each agent's consumer waits on, by agent key. Every agent of a
   * daemon is given the same map, so a send opens its recipients' latches.
   */
  readonly consumers: Map<string, Effect.Latch>;
}

/**
 * Keeps a background loop of one agent running. A failure is logged and the
 * loop starts again after a pause that doubles from one second up to thirty.
 */
export function restarting(agentKey: string, loop: string) {
  return <E, R>(work: Effect.Effect<never, E, R>) =>
    work.pipe(
      Effect.tapError((error) =>
        Effect.logError(`The ${loop} failed and will start again`).pipe(
          Effect.annotateLogs({
            agentKey,
            failure: Predicate.hasProperty(error, "_tag")
              ? String(error._tag)
              : "unknown",
          }),
        ),
      ),
      Effect.retry(
        Schedule.union(
          Schedule.exponential("1 second"),
          Schedule.spaced("30 seconds"),
        ),
      ),
    );
}

/** The agent whose history is read, and the session its cursors belong to. */
interface HistoryAgent {
  readonly agentKey: string;
  readonly card: AgentCard;
  readonly session: string;
}
interface HistoryWindow {
  readonly conversationId: string;
  readonly after: number;
  readonly through: number;
  readonly newestFirst: boolean;
  readonly limit: number;
}
const cursorSchema = Schema.Struct({
  session: Schema.String,
  conversation: Schema.String,
  after: Schema.Int,
  through: Schema.Int,
});
const refused = (reason: CommunicationError["reason"]) =>
  new CommunicationError({ reason });

/** Conversation lookup, listing and paged history for one agent. */
function messageHistory(input: Omit<HistoryAgent, "session">) {
  const agent = {
    ...input,
    session: randomBytes(16).toString("base64url"),
  };
  return {
    listConversations: historyFailure(listConversations(agent)).pipe(
      Effect.withSpan("listConversations"),
    ),
    readHistory: (query: HistoryQuery) =>
      historyFailure(readHistory(agent, query)).pipe(
        Effect.withSpan("readHistory"),
      ),
  };
}

function readHistory(agent: HistoryAgent, query: HistoryQuery) {
  return Effect.gen(function* () {
    const window = yield* historyWindow(agent, query);
    const selected = yield* read(agent, {
      conversationId: window.conversationId,
      after: window.after,
      through: window.through,
      newestFirst: window.newestFirst,
      limit: window.limit + 1,
    });
    const page = selected.slice(0, window.limit);
    const last = page.at(-1);
    const nextCursor =
      !window.newestFirst &&
      selected.length > window.limit &&
      last !== undefined
        ? JSON.stringify({
            session: agent.session,
            conversation: window.conversationId,
            after: last.ordinal,
            through: window.through,
          })
        : undefined;
    if (window.newestFirst) {
      page.reverse();
    }
    return {
      entries: page.map((row) => ({
        message: row.message,
        messageBytes: row.bytes,
      })),
      earlierMessagesOmitted:
        window.newestFirst && selected.length > window.limit,
      ...(nextCursor === undefined ? {} : { nextCursor }),
    };
  });
}

function listConversations(agent: HistoryAgent) {
  return Effect.gen(function* () {
    const db = yield* communicationQueries;
    const rows = yield* withAgentRows(
      agent.agentKey,
      executeQuery(
        db
          .selectFrom("message_log")
          .select("conversation_id as conversationId")
          .distinct()
          .where("agent_key", "=", agent.agentKey),
      ),
    );
    const conversations: Conversation[] = [];
    for (const row of rows) {
      const [stored] = yield* read(agent, {
        conversationId: row.conversationId,
        limit: 1,
      });
      if (stored !== undefined) {
        conversations.push(yield* membersOf(stored.message));
      }
    }
    return conversations;
  });
}

function historyWindow(agent: HistoryAgent, query: HistoryQuery) {
  return Effect.gen(function* () {
    const conversationId = yield* historyConversation(agent, query.members);
    const [newestStored] = yield* read(agent, {
      conversationId,
      newestFirst: true,
      limit: 1,
    });
    const orderedMembers = [...query.members].sort((a, b) =>
      compareText(a.agentName, b.agentName),
    );
    if (
      newestStored !== undefined &&
      !Schema.equivalence(Schema.Array(AgentCard))(
        messageMembers(newestStored.message),
        orderedMembers,
      )
    ) {
      return yield* refused("membership-invalid");
    }
    const through = newestStored?.ordinal ?? -1;
    if (query.order === "newest") {
      return {
        conversationId,
        after: -1,
        through:
          query.beforeMessage === undefined
            ? through
            : (yield* ordinalOf(agent, conversationId, query.beforeMessage)) -
              1,
        newestFirst: true,
        limit: query.limit,
      };
    }
    const window = {
      conversationId,
      after: -1,
      through,
      newestFirst: false,
      limit: 50,
    };
    return query.cursor === undefined
      ? window
      : yield* continueWindow(agent, window, query.cursor);
  });
}

function historyConversation(agent: HistoryAgent, members: Conversation) {
  return Effect.gen(function* () {
    if (!members.some((member) => member.agentId === agent.card.agentId)) {
      return yield* refused("membership-invalid");
    }
    const ids = members.map((member) => member.agentId).sort(compareAgentIds);
    const [first, second, ...rest] = ids;
    if (
      first === undefined ||
      second === undefined ||
      new Set(ids).size !== ids.length
    ) {
      return yield* refused("membership-invalid");
    }
    return yield* deriveConversationId([first, second, ...rest]);
  });
}

/** The ordinal of a message this agent's history holds in the conversation. */
function ordinalOf(
  agent: HistoryAgent,
  conversationId: string,
  reference: MessageReference,
) {
  return Effect.gen(function* () {
    const db = yield* communicationQueries;
    const [row] = yield* withAgentRows(
      agent.agentKey,
      executeQuery(
        db
          .selectFrom("message_log")
          .select("ordinal")
          .where("agent_key", "=", agent.agentKey)
          .where("conversation_id", "=", conversationId)
          .where("author_agent_id", "=", reference.authorAgentId)
          .where("msg_id", "=", reference.msgId),
      ),
    );
    return row === undefined ? yield* refused("content-invalid") : row.ordinal;
  });
}

function continueWindow(
  agent: HistoryAgent,
  window: HistoryWindow,
  encodedCursor: string,
) {
  return Effect.gen(function* () {
    const cursor = yield* Schema.decodeUnknown(Schema.parseJson(cursorSchema))(
      encodedCursor,
    ).pipe(Effect.mapError(() => refused("invalid-cursor")));
    const sameRequest =
      cursor.session === agent.session &&
      cursor.conversation === window.conversationId;
    const validBounds =
      cursor.after >= window.after &&
      cursor.after <= cursor.through &&
      cursor.through <= window.through;
    if (!sameRequest || !validBounds) {
      return yield* refused("invalid-cursor");
    }
    return { ...window, after: cursor.after, through: cursor.through };
  });
}

function read(agent: HistoryAgent, query: Parameters<typeof readMessages>[1]) {
  return readMessages(agent.agentKey, query);
}

function compareText(a: string, b: string) {
  if (a < b) {
    return -1;
  }
  return a > b ? 1 : 0;
}

function membersOf(message: Message) {
  const [first, second, ...rest] = messageMembers(message);
  return first === undefined || second === undefined
    ? Effect.fail(refused("persistence-failed"))
    : Effect.succeed<Conversation>([first, second, ...rest]);
}

/** Reports any failure of a history read that is not already the service's as `persistence-failed`. */
function historyFailure<A, E, R>(work: Effect.Effect<A, E, R>) {
  return work.pipe(
    Effect.mapError((error) =>
      error instanceof CommunicationError
        ? error
        : refused("persistence-failed"),
    ),
  );
}

/** One hosted agent's database client, settings and arrival latch. */
interface HostedAgent {
  readonly sql: SqlClient;
  readonly settings: CommunicationSettings;
  /** Opened when a message is saved for this agent. */
  readonly arrived: Effect.Latch;
}
const agentRow = Schema.Struct({
  agentKey: Schema.String,
  ...AgentCard.fields,
});

/** One agent's sends, what it consumes and its history on this daemon. */
const makeCommunication = Effect.fn("makeCommunication")(function* (
  settings: CommunicationSettings,
) {
  const arrived = yield* Effect.makeLatch();
  settings.consumers.set(settings.agentKey, arrived);
  const agent = { sql: yield* SqlClient, settings, arrived };
  const history = messageHistory({
    agentKey: settings.agentKey,
    card: settings.card,
  });
  const oneConsumer = yield* Effect.makeSemaphore(1);
  return {
    send: (message) =>
      withSqlAndServiceErrors(agent, sendMessage(agent, message)),
    consume: (handle) => oneConsumer.withPermits(1)(consume(agent, handle)),
    listConversations: withSqlAndServiceErrors(
      agent,
      history.listConversations,
    ),
    readHistory: (query) =>
      withSqlAndServiceErrors(agent, history.readHistory(query)),
  } satisfies CommunicationService;
});

/** Carries one agent's messages and reads its ordered history. */
export class Communication extends Context.Tag("SocialHarness/Communication")<
  Communication,
  CommunicationService
>() {
  /** The service of the agent `settings` names. */
  static readonly layer = (settings: CommunicationSettings) =>
    Layer.scoped(Communication, makeCommunication(settings));
}

/** The key of the agent on this daemon that `recipient` names, which must match its stored card. */
function recipientKey(recipient: AgentCard) {
  return Effect.gen(function* () {
    const db = yield* communicationQueries;
    const [raw] = yield* executeQuery(
      db
        .selectFrom("agent_cards")
        .select([
          "agent_key as agentKey",
          "agent_id as agentId",
          "agent_name as agentName",
          "principal_id as principalId",
        ])
        .where("agent_id", "=", recipient.agentId),
    );
    if (raw === undefined) {
      return yield* refused("membership-invalid");
    }
    const { agentKey, ...stored } = yield* Schema.decodeUnknown(agentRow)(raw);
    if (!Schema.equivalence(AgentCard)(recipient, stored)) {
      return yield* refused("membership-invalid");
    }
    return agentKey;
  });
}

/**
 * Orders one message and saves it for every member, or changes nothing. A
 * message this agent has already sent takes no new position. The author locks
 * its own agent and then the conversation, as every other write of an agent
 * does, so two sends cannot deadlock. Each recipient's consumer wakes once the
 * message is committed.
 */
function sendMessage({ settings }: HostedAgent, message: Message) {
  return transaction(
    Effect.gen(function* () {
      if (!Schema.equivalence(AgentCard)(message.author, settings.card)) {
        return yield* refused("membership-invalid");
      }
      const conversationId = yield* messageConversation(settings, message);
      const recipientKeys = yield* Effect.forEach(
        message.recipients,
        recipientKey,
        { concurrency: 1 },
      );
      if (yield* hasMessage(settings.agentKey, conversationId, message)) {
        return;
      }
      const db = yield* communicationQueries;
      const [next] = yield* executeQuery(
        db
          .insertInto("message_sequences")
          .values({ conversation_id: conversationId, next_ordinal: 1 })
          .onConflict((conflict) =>
            conflict.column("conversation_id").doUpdateSet((eb) => ({
              next_ordinal: eb("message_sequences.next_ordinal", "+", 1),
            })),
          )
          .returning((eb) => eb("next_ordinal", "-", 1).as("ordinal")),
      );
      if (next === undefined) {
        return yield* refused("persistence-failed");
      }
      yield* Effect.forEach(
        [settings.agentKey, ...recipientKeys],
        (agentKey) =>
          saveMessage(agentKey, conversationId, next.ordinal, message),
        { concurrency: 1, discard: true },
      );
      yield* afterCommit(
        Effect.forEach(
          recipientKeys,
          (agentKey) => settings.consumers.get(agentKey)?.open ?? Effect.void,
          { concurrency: 1, discard: true },
        ),
      );
    }),
  );
}

/**
 * The conversation a message belongs to: its author and recipients, who must
 * be distinct, include this agent and be permitted to share a conversation.
 */
function messageConversation(
  settings: CommunicationSettings,
  message: Message,
) {
  return Effect.gen(function* () {
    const members = [message.author, ...message.recipients];
    const [first, second, ...rest] = members
      .map((member) => member.agentId)
      .sort(compareAgentIds);
    if (first === undefined || second === undefined) {
      return yield* refused("membership-invalid");
    }
    const memberIds = [first, second, ...rest] as const;
    const distinct = new Set(memberIds).size === memberIds.length;
    const local = members.some((member) =>
      Schema.equivalence(AgentCard)(member, settings.card),
    );
    if (!distinct || !local) {
      return yield* refused("membership-invalid");
    }
    return yield* deriveConversationId(memberIds);
  });
}

/**
 * Hands each message this agent has not consumed to `handle` in the order
 * they were saved, and marks each consumed only after `handle` succeeds. With
 * nothing to consume it waits until a message is saved for this agent.
 */
function consume<E, R>(
  agent: HostedAgent,
  handle: (message: Message) => Effect.Effect<void, E, R>,
) {
  const { agentKey, card } = agent.settings;
  return Effect.gen(function* () {
    while (true) {
      yield* agent.arrived.close;
      const unconsumed = yield* withSqlAndServiceErrors(
        agent,
        readMessages(agentKey, { unconsumedBy: card.agentId, limit: 50 }),
      );
      for (const { message } of unconsumed) {
        yield* handle(message);
        yield* withSqlAndServiceErrors(
          agent,
          markMessageConsumed(agentKey, {
            authorAgentId: message.author.agentId,
            msgId: message.msgId,
          }),
        );
      }
      if (unconsumed.length === 0) {
        yield* agent.arrived.await;
      }
    }
  });
}

/**
 * Gives `work` the agent's database client and reports its failures as the
 * service's: a conflict as `message-conflict`, anything else that is not
 * already the service's as `persistence-failed`.
 */
function withSqlAndServiceErrors<A, E, R>(
  { sql }: HostedAgent,
  work: Effect.Effect<A, E, R>,
) {
  return work.pipe(
    Effect.provideService(SqlClient, sql),
    Effect.mapError((error) => {
      if (error instanceof CommunicationError) {
        return error;
      }
      const conflict =
        typeof error === "object" &&
        error !== null &&
        "reason" in error &&
        error.reason === "conflict";
      return refused(conflict ? "message-conflict" : "persistence-failed");
    }),
  );
}
