/**
 * @file One agent's communication service on a daemon that hosts every agent.
 *
 * The file opens with what the service accepts, hands to the host and
 * returns, and the queries that store each agent's ordered messages and which
 * of them it has consumed. It then reads an agent's conversations and their
 * ordered history. A send orders the message in its conversation and saves
 * it for every member in one transaction.
 */

import { SqlClient } from "@effect/sql/SqlClient";
import { Cause, Context, Effect, Layer, Option, Schema } from "effect";
import { randomBytes } from "node:crypto";
import {
  afterCommit,
  executeQuery,
  type Generated,
  makeQueries,
  transaction,
  withAgentRows,
} from "../database/index.js";
import {
  compareAgentIds,
  compareAscii,
  decodeJson,
  encodeJson,
  sameJson,
} from "../encoding.js";
import { AgentCard, type AgentId } from "../identity.js";
import {
  CommunicationError,
  deriveConversationId,
  Message,
  messageMembers,
  type MessageReference,
  type Page,
  PAGE_SIZE,
  type PaginatedRequest,
} from "./messages.js";

/** Selects a paged forward traversal or one bounded recent window of the conversation among `members`, given in any order. */
export type HistoryQuery =
  | (PaginatedRequest & {
      readonly members: readonly AgentCard[];
      readonly order?: "oldest";
    })
  | {
      readonly members: readonly AgentCard[];
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
  /** Every conversation this agent has a message in, in no order, each as its members in agent-name order. */
  readonly listConversations: Effect.Effect<
    ReadonlyArray<readonly AgentCard[]>,
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

type CommunicationTables = Record<
  "message_log",
  Record<
    "agent_key" | "conversation_id" | "author_agent_id" | "msg_id" | "message",
    string
  > &
    Record<"ordinal", number> &
    Record<"sequence" | "consumed", Generated<number>>
> &
  Record<
    "message_sequences",
    Record<"conversation_id", string> & Record<"next_ordinal", number>
  > &
  Record<
    "agent_cards",
    Record<"agent_key" | "agent_id" | "agent_name" | "principal_id", string>
  >;

/** Builds queries over the tables that hold messages and the agents they name. */
const communicationQueries = makeQueries<CommunicationTables>();

/**
 * Whether this agent's history holds the message. A saved message with the
 * same author and identifier but another conversation or content is a
 * conflict.
 */
const hasMessage = Effect.fn("hasMessage")(function* (
  agentKey: string,
  conversationId: string,
  message: Message,
) {
  const db = yield* communicationQueries;
  const [row] = yield* executeQuery(
    db
      .selectFrom("message_log")
      .select(["conversation_id", "message"])
      .where("agent_key", "=", agentKey)
      .where("author_agent_id", "=", message.author.agentId)
      .where("msg_id", "=", message.msgId),
  );
  if (row === undefined) {
    return false;
  }
  if (
    row.conversation_id !== conversationId ||
    !sameJson(row.message, yield* encodeJson(Message, message))
  ) {
    return yield* refused("message-conflict");
  }
  return true;
}, withAgentLock);

/**
 * Adds a message to one agent's history at its position. It takes no lock on
 * the agent, so an author can save for every member while holding only its own
 * agent's lock and the conversation's.
 */
const saveMessage = Effect.fn("saveMessage")(
  function* (
    agentKey: string,
    conversationId: string,
    ordinal: number,
    message: Message,
  ) {
    const db = yield* communicationQueries;
    yield* executeQuery(
      db.insertInto("message_log").values({
        agent_key: agentKey,
        conversation_id: conversationId,
        ordinal,
        author_agent_id: message.author.agentId,
        msg_id: message.msgId,
        message: yield* encodeJson(Message, message),
      }),
    );
  },
  (work, agentKey) => withAgentRows(agentKey, work),
);

/**
 * A bounded range of one agent's messages, in conversation order or in the
 * order they were saved, each with its position in its conversation and the
 * size of its stored JSON text. A row whose message names another author or
 * identifier than the row does is a conflict.
 */
const readMessages = Effect.fn("readMessages")(function* (
  agentKey: string,
  range: {
    readonly conversationId?: string;
    readonly after?: number;
    readonly through?: number;
    readonly newestFirst?: boolean;
    /** Selects messages this agent has not consumed and did not write. */
    readonly unconsumedBy?: AgentId;
    readonly limit: number;
  },
) {
  const db = yield* communicationQueries;
  let query = db
    .selectFrom("message_log")
    .select(["ordinal", "author_agent_id", "msg_id", "message"])
    .where("agent_key", "=", agentKey)
    .limit(range.limit);
  if (range.conversationId !== undefined) {
    query = query.where("conversation_id", "=", range.conversationId);
  }
  if (range.after !== undefined) {
    query = query.where("ordinal", ">", range.after);
  }
  if (range.through !== undefined) {
    query = query.where("ordinal", "<=", range.through);
  }
  if (range.unconsumedBy !== undefined) {
    query = query
      .where("consumed", "=", 0)
      .where("author_agent_id", "<>", range.unconsumedBy);
  }
  const ascending = range.conversationId === undefined ? "sequence" : "ordinal";
  const rows = yield* executeQuery(
    range.newestFirst
      ? query.orderBy("ordinal", "desc")
      : query.orderBy(ascending),
  );
  return yield* Effect.forEach(
    rows,
    (row) =>
      decodeJson(Message, row.message).pipe(
        Effect.filterOrFail(
          (message) =>
            message.author.agentId === row.author_agent_id &&
            message.msgId === row.msg_id,
          () => refused("message-conflict"),
        ),
        Effect.map((message) => ({
          ordinal: row.ordinal,
          message,
          bytes: Buffer.byteLength(row.message),
        })),
      ),
    { concurrency: 1 },
  );
}, withAgentLock);

/** Marks a message consumed, so that it is not handed to the consumer again. */
const markMessageConsumed = Effect.fn("markMessageConsumed")(function* (
  agentKey: string,
  reference: MessageReference,
) {
  const db = yield* communicationQueries;
  yield* executeQuery(
    db
      .updateTable("message_log")
      .set({ consumed: 1 })
      .where("agent_key", "=", agentKey)
      .where("author_agent_id", "=", reference.authorAgentId)
      .where("msg_id", "=", reference.msgId),
  );
}, withAgentLock);

function readHistory(agent: HostedAgent, query: HistoryQuery) {
  return Effect.gen(function* () {
    const window = yield* historyWindow(agent, query);
    const selected = yield* readMessages(agent.settings.agentKey, {
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

function listConversations(agent: HostedAgent) {
  return Effect.gen(function* () {
    const { agentKey } = agent.settings;
    const db = yield* communicationQueries;
    const rows = yield* withAgentRows(
      agentKey,
      executeQuery(
        db
          .selectFrom("message_log")
          .select("conversation_id as conversationId")
          .distinct()
          .where("agent_key", "=", agentKey),
      ),
    );
    const conversations: Array<readonly AgentCard[]> = [];
    for (const row of rows) {
      const [stored] = yield* readMessages(agentKey, {
        conversationId: row.conversationId,
        limit: 1,
      });
      if (stored !== undefined) {
        conversations.push(messageMembers(stored.message));
      }
    }
    return conversations;
  });
}

function historyWindow(agent: HostedAgent, query: HistoryQuery) {
  return Effect.gen(function* () {
    const { agentKey, card } = agent.settings;
    const conversationId = yield* conversationOf(card, query.members);
    const [newestStored] = yield* readMessages(agentKey, {
      conversationId,
      newestFirst: true,
      limit: 1,
    });
    const orderedMembers = [...query.members].sort((a, b) =>
      compareAscii(a.agentName, b.agentName),
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
            : (yield* ordinalOf(
                agentKey,
                conversationId,
                query.beforeMessage,
              )) - 1,
        newestFirst: true,
        limit: query.limit,
      };
    }
    const window = {
      conversationId,
      after: -1,
      through,
      newestFirst: false,
      limit: PAGE_SIZE,
    };
    return query.cursor === undefined
      ? window
      : yield* continueWindow(agent.session, window, query.cursor);
  });
}

/**
 * The conversation among `members`, in any order: at least two distinct
 * agents, `self` among them.
 */
function conversationOf(self: AgentCard, members: readonly AgentCard[]) {
  return Effect.gen(function* () {
    const ids = members.map((member) => member.agentId).sort(compareAgentIds);
    const [first, second, ...rest] = ids;
    if (
      first === undefined ||
      second === undefined ||
      new Set(ids).size !== ids.length ||
      !ids.includes(self.agentId)
    ) {
      return yield* refused("membership-invalid");
    }
    return yield* deriveConversationId([first, second, ...rest]);
  });
}

/** The ordinal of a message this agent's history holds in the conversation. */
function ordinalOf(
  agentKey: string,
  conversationId: string,
  reference: MessageReference,
) {
  return Effect.gen(function* () {
    const db = yield* communicationQueries;
    const [row] = yield* withAgentRows(
      agentKey,
      executeQuery(
        db
          .selectFrom("message_log")
          .select("ordinal")
          .where("agent_key", "=", agentKey)
          .where("conversation_id", "=", conversationId)
          .where("author_agent_id", "=", reference.authorAgentId)
          .where("msg_id", "=", reference.msgId),
      ),
    );
    return row === undefined ? yield* refused("content-invalid") : row.ordinal;
  });
}

function continueWindow(
  session: string,
  window: HistoryWindow,
  encodedCursor: string,
) {
  return Effect.gen(function* () {
    const cursor = yield* Schema.decodeUnknown(Schema.parseJson(cursorSchema))(
      encodedCursor,
    ).pipe(Effect.mapError(() => refused("invalid-cursor")));
    const sameRequest =
      cursor.session === session &&
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

/** One hosted agent's database client, settings, arrival latch and cursor session. */
interface HostedAgent {
  readonly sql: SqlClient;
  readonly settings: CommunicationSettings;
  /** Opened when a message is saved for this agent. */
  readonly arrived: Effect.Latch;
  /** Names this service instance in the history cursors it returns. */
  readonly session: string;
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
  const agent = {
    sql: yield* SqlClient,
    settings,
    arrived,
    session: randomBytes(16).toString("base64url"),
  };
  const oneConsumer = yield* Effect.makeSemaphore(1);
  return {
    send: (message) =>
      withSqlAndServiceErrors(agent, sendMessage(agent, message)),
    consume: (handle) => oneConsumer.withPermits(1)(consume(agent, handle)),
    listConversations: withSqlAndServiceErrors(
      agent,
      listConversations(agent),
    ).pipe(Effect.withSpan("listConversations")),
    readHistory: (query) =>
      withSqlAndServiceErrors(agent, readHistory(agent, query)).pipe(
        Effect.withSpan("readHistory"),
      ),
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
      const conversationId = yield* conversationOf(settings.card, [
        message.author,
        ...message.recipients,
      ]);
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
 * Gives `work` the agent's database client and reports as `persistence-failed`
 * every way it ends, a defect included, other than success, interruption and a
 * failure that is already the service's.
 */
function withSqlAndServiceErrors<A, E, R>(
  { sql }: HostedAgent,
  work: Effect.Effect<A, E, R>,
) {
  return work.pipe(
    Effect.provideService(SqlClient, sql),
    Effect.catchAllCause((cause) => {
      if (Cause.isInterruptedOnly(cause)) {
        return Effect.failCause(Cause.stripFailures(cause));
      }
      const failure = Cause.failureOption(cause);
      return Effect.fail(
        Option.isSome(failure) && failure.value instanceof CommunicationError
          ? failure.value
          : refused("persistence-failed"),
      );
    }),
  );
}

/**
 * Locking the agent's own row serializes one agent's transactions, so a
 * transaction that reads before it writes sees every earlier write of the same
 * agent. Nested calls retain the lock until the enclosing transaction commits;
 * other agents use their own rows.
 */
function withAgentLock<A, E, R>(
  work: Effect.Effect<A, E, R>,
  agentKey: string,
) {
  return withAgentRows(
    agentKey,
    Effect.gen(function* () {
      const db = yield* communicationQueries;
      yield* executeQuery(
        db
          .selectFrom("agent_cards")
          .select("agent_key")
          .where("agent_key", "=", agentKey)
          .forUpdate(),
      );
      return yield* work;
    }),
  ).pipe(Effect.withSpan("withAgentLock"));
}
