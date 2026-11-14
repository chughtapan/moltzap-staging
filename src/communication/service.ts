/**
 * @file One agent's communication service on a daemon that hosts every agent.
 *
 * The file opens with what the service accepts, hands to the host and
 * returns, then reads an agent's conversations and their ordered history. A
 * send orders the message in its conversation and saves it for every member
 * in one transaction. Collective requests and responses are messages that
 * `collectives.ts` interprets.
 */

import { SqlClient } from "@effect/sql/SqlClient";
import { Context, Effect, Layer, Option, Schema } from "effect";
import { randomBytes } from "node:crypto";
import {
  afterCommit,
  executeQuery,
  transaction,
  withAgentTransaction,
} from "../database/index.js";
import { compareAgentIds, exactStruct } from "../encoding.js";
import { AgentCard, type AgentName } from "../identity.js";
import { makeCollectives } from "./collectives.js";
import { CommunicationError } from "./errors.js";
import {
  collectivePartOf,
  CollectiveRequest,
  CollectiveRequestMessage,
  CollectiveResult,
  CollectiveResultSources,
  conversationNameKey,
  deriveConversationId,
  Message,
  messageMembers,
  type MessageReference,
  type MsgId,
  type PaginatedRequest,
  type PaginatedResult,
  RequestError,
} from "./messages.js";
import {
  communicationQueries,
  hasMessage,
  markMessageConsumed,
  readMessages,
  saveMessage,
  type StoredMessage,
} from "./store.js";

/** All conversation members, in ascending agent-name order. */
export type Conversation = readonly [AgentCard, AgentCard, ...AgentCard[]];
/** Omit cursor to begin listing conversations. */
export interface ConversationQuery extends PaginatedRequest {}
/** A missing nextCursor ends the listing. */
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

/** What Communication sends: a message, or a collective request with every recipient. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- The owning handoff grammar lets Main persist Message values through their encoder.
export const Outgoing = Schema.Union(Message, CollectiveRequest);
/** What an agent sends, fixed before policy assessment. */
export type Outgoing = typeof Outgoing.Type;
/** What an agent consumes: a message, a collective request, or a collective result with its sources. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- The owning handoff grammar lets Main persist Message values through their encoder.
export const Incoming = Schema.Union(
  exactStruct({ kind: Schema.Literal("message"), message: Message }),
  exactStruct({
    kind: Schema.Literal("collectiveRequest"),
    message: CollectiveRequestMessage,
  }),
  exactStruct({
    kind: Schema.Literal("collectiveResult"),
    result: CollectiveResult,
    sources: CollectiveResultSources,
  }),
);
/** What an agent consumes, before local policy decides what its host reads. */
export type Incoming = typeof Incoming.Type;

/** One agent's sends, what it consumes and its history. */
export interface CommunicationService {
  /**
   * Returns once a message is ordered and saved for every member, or a
   * collective request is saved with its copies. Sending the same `Message`
   * or `CollectiveRequest` again, with its `msgId`, changes nothing.
   */
  readonly send: (
    value: Outgoing,
  ) => Effect.Effect<void, CommunicationError | RequestError>;
  /**
   * The collective request with this message id that this agent received and
   * may still respond to. Looking it up reserves nothing.
   */
  readonly getCollectiveRequest: (
    requestId: MsgId,
  ) => Effect.Effect<
    CollectiveRequestMessage,
    CommunicationError | RequestError
  >;
  /**
   * Calls the handler for each message, collective request and collective
   * result, and marks it consumed after success. Failed or interrupted
   * handling leaves it to be handed over again. A handled item can also be
   * handed over again when marking it fails, so handlers must tolerate
   * repeats.
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

/** The agent a communication service acts for. */
export interface CommunicationSettings {
  /** The key the agent's rows are stored under. */
  readonly agentKey: string;
  readonly card: AgentCard;
  /** The most messages one newest-first history read may ask for. */
  readonly maximumHistoryRecords?: number;
  /** Whether these agents may share a conversation. Without it any may. */
  readonly permits?: (members: readonly AgentCard[]) => boolean;
  /**
   * The latch each agent's consumer waits on, by agent key. Every agent of a
   * daemon is given the same map, so a send opens its recipients' latches.
   */
  readonly consumers: Map<string, Effect.Latch>;
}

/** The agent whose history is read, and the session its cursors belong to. */
interface HistoryAgent {
  readonly agentKey: string;
  readonly card: AgentCard;
  readonly maximumHistoryRecords: number;
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
    getConversation: (names: readonly AgentName[]) =>
      historyFailure(getConversation(agent, names)).pipe(
        Effect.withSpan("getConversation"),
      ),
    listConversations: (query: ConversationQuery = {}) =>
      historyFailure(listConversations(agent, query)).pipe(
        Effect.withSpan("listConversations"),
      ),
    readHistory: (query: HistoryQuery) =>
      historyFailure(readHistory(agent, query)).pipe(
        Effect.withSpan("readHistory"),
      ),
  };
}

function getConversation(agent: HistoryAgent, names: readonly AgentName[]) {
  return Effect.gen(function* () {
    if (
      !names.includes(agent.card.agentName) ||
      new Set(names).size !== names.length
    ) {
      return yield* refused("membership-invalid");
    }
    const wanted = conversationNameKey(names, agent.card.agentName);
    const conversations = yield* retainedConversations(agent);
    return conversations.find(
      (members) => conversationKey(agent, members) === wanted,
    );
  });
}

function listConversations(agent: HistoryAgent, query: ConversationQuery) {
  return Effect.gen(function* () {
    const conversations = yield* retainedConversations(agent);
    const after =
      query.cursor === undefined
        ? ""
        : yield* Schema.decodeUnknown(
            Schema.parseJson(
              Schema.Struct({
                session: Schema.Literal(agent.session),
                after: Schema.String,
              }),
            ),
          )(query.cursor).pipe(
            Effect.map((value) => value.after),
            Effect.mapError(() => refused("invalid-cursor")),
          );
    const selected = conversations.filter(
      (members) => conversationKey(agent, members) > after,
    );
    const page = selected.slice(0, 50);
    const last = page.at(-1);
    return {
      conversations: page,
      ...(selected.length > 50 && last !== undefined
        ? {
            nextCursor: JSON.stringify({
              session: agent.session,
              after: conversationKey(agent, last),
            }),
          }
        : {}),
    };
  });
}

function readHistory(agent: HistoryAgent, query: HistoryQuery) {
  return Effect.gen(function* () {
    const window = yield* historyWindow(agent, query);
    const selected = yield* scanWindow(agent, window);
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

function retainedConversations(agent: HistoryAgent) {
  return Effect.gen(function* () {
    const db = yield* communicationQueries;
    const rows = yield* withAgentTransaction(
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
      const [entry] = yield* read(agent, {
        conversationId: row.conversationId,
        limit: 1,
      });
      if (entry !== undefined) {
        conversations.push(yield* membersOf(entry.message));
      }
    }
    return conversations.sort((a, b) =>
      compareText(conversationKey(agent, a), conversationKey(agent, b)),
    );
  });
}

function historyWindow(agent: HistoryAgent, query: HistoryQuery) {
  return Effect.gen(function* () {
    const conversationId = yield* historyConversation(agent, query.members);
    const [newestStored] = yield* read(agent, {
      conversationId,
      newest: true,
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
      if (
        !Number.isSafeInteger(query.limit) ||
        query.limit < 1 ||
        query.limit > agent.maximumHistoryRecords
      ) {
        return yield* refused("content-invalid");
      }
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
    const [row] = yield* withAgentTransaction(
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
  value: string,
) {
  return Effect.gen(function* () {
    const cursor = yield* Schema.decodeUnknown(Schema.parseJson(cursorSchema))(
      value,
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

/** The window's messages in its order, without collective closes, up to one past its limit. */
function scanWindow(agent: HistoryAgent, window: HistoryWindow) {
  return Effect.gen(function* () {
    const selected: StoredMessage[] = [];
    let { after, through } = window;
    while (after < through) {
      const rows = yield* read(agent, {
        conversationId: window.conversationId,
        after,
        through,
        newest: window.newestFirst,
        limit: 50,
      });
      const last = rows.at(-1);
      if (last === undefined) {
        break;
      }
      for (const row of rows) {
        if (yield* hostReadable(row.message)) {
          selected.push(row);
        }
        if (selected.length > window.limit) {
          return selected;
        }
      }
      if (window.newestFirst) {
        through = last.ordinal - 1;
      } else {
        after = last.ordinal;
      }
    }
    return selected;
  });
}

/** Whether a host may read the message: every message but a collective close or one whose collective part is unreadable. */
function hostReadable(message: Message) {
  return collectivePartOf(message).pipe(
    Effect.map(
      Option.match({
        onNone: () => true,
        onSome: (part) => part.kind !== "close",
      }),
    ),
    Effect.catchAll(() => Effect.succeed(false)),
  );
}

function read(agent: HistoryAgent, query: Parameters<typeof readMessages>[1]) {
  return readMessages(agent.agentKey, query);
}

function conversationKey(agent: HistoryAgent, members: Conversation) {
  return conversationNameKey(
    members.map((member) => member.agentName),
    agent.card.agentName,
  );
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

/** Keeps a background loop of one agent running after a failure. */
export { restarting } from "./collectives.js";

/** One hosted agent's database client, settings and arrival latch. */
interface HostedAgent {
  readonly sql: SqlClient;
  readonly settings: CommunicationSettings;
  /** Opened when a message or a collective result is saved for this agent. */
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
    maximumHistoryRecords: settings.maximumHistoryRecords ?? 1024,
  });
  const collectives = yield* makeCollectives(settings.agentKey, {
    card: settings.card,
    send: (message) =>
      withSqlAndServiceErrors(agent, sendMessage(agent, message)).pipe(
        Effect.mapError((error) =>
          error instanceof CommunicationError
            ? error
            : refused("persistence-failed"),
        ),
      ),
    completed: arrived.open,
  });
  const consumer = yield* Effect.makeSemaphore(1);
  return {
    send: (value) =>
      withSqlAndServiceErrors(agent, send(agent, collectives, value)),
    getCollectiveRequest: (requestId) =>
      withSqlAndServiceErrors(agent, collectives.getRequest(requestId)),
    consume: (handle) =>
      consumer.withPermits(1)(consume(agent, collectives, handle)),
    getConversation: (names) =>
      withSqlAndServiceErrors(agent, history.getConversation(names)),
    listConversations: (query = {}) =>
      withSqlAndServiceErrors(agent, history.listConversations(query)),
    readHistory: (query) =>
      withSqlAndServiceErrors(agent, history.readHistory(query)),
  } satisfies CommunicationService;
});

/** Delivers messages, completes collective requests and reads ordered history for one agent. */
export class Communication extends Context.Tag("SocialHarness/Communication")<
  Communication,
  CommunicationService
>() {
  /** The service of the agent `settings` names. */
  static readonly layer = (settings: CommunicationSettings) =>
    Layer.scoped(Communication, makeCommunication(settings));
}

type Collectives = Effect.Effect.Success<ReturnType<typeof makeCollectives>>;

/** A collective request and a collective response go through the collective layer; any other message is sent as it is. */
function send(agent: HostedAgent, collectives: Collectives, value: Outgoing) {
  if ("kind" in value) {
    return collectives.sendRequest(value);
  }
  return Effect.gen(function* () {
    const part = yield* collectivePartOf(value).pipe(
      Effect.catchTag("CollectivePartInvalidError", () =>
        Effect.succeed(Option.none()),
      ),
    );
    return yield* Option.isSome(part) && part.value.kind === "response"
      ? collectives.sendResponse(value, part.value)
      : sendMessage(agent, value);
  });
}

/** The key of the agent on this daemon that `recipient` names, which must match its stored card. */
function recipientKey(recipient: AgentCard) {
  return Effect.gen(function* () {
    const db = yield* communicationQueries;
    const [raw] = yield* executeQuery(
      db
        .selectFrom("agent_profiles")
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
 * message this agent has already sent takes no new position. The sender locks
 * its own agent and then the conversation, as every other write of an agent
 * does, so senders cannot deadlock. Each recipient's consumer wakes once the
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
    if (!distinct || !local || settings.permits?.(members) === false) {
      return yield* refused("membership-invalid");
    }
    return yield* deriveConversationId(memberIds);
  });
}

/**
 * Hands each message and collective request this agent has not consumed to
 * `handle` in the order they were saved, and each collective result as soon
 * as it is complete, so a result precedes any message saved after the
 * response that completed it. Each is marked consumed only after `handle`
 * succeeds. With nothing to consume it waits until a message or a result is
 * saved for this agent.
 */
function consume<E, R>(
  agent: HostedAgent,
  collectives: Collectives,
  handle: (incoming: Incoming) => Effect.Effect<void, E, R>,
) {
  const { agentKey, card } = agent.settings;
  const results = withSqlAndServiceErrors(
    agent,
    collectives.deliverNextResult((completed) =>
      handle({ kind: "collectiveResult", ...completed }),
    ),
  ).pipe(Effect.repeat({ while: (delivered) => delivered }));
  return Effect.gen(function* () {
    while (true) {
      yield* agent.arrived.close;
      yield* results;
      const rows = yield* withSqlAndServiceErrors(
        agent,
        readMessages(agentKey, { pendingFor: card.agentId, limit: 50 }),
      );
      for (const entry of rows) {
        const received = yield* withSqlAndServiceErrors(
          agent,
          collectives.receive(entry.message),
        );
        if (Option.isSome(received)) {
          yield* handle(received.value);
        }
        yield* withSqlAndServiceErrors(
          agent,
          markMessageConsumed(agentKey, {
            authorAgentId: entry.message.author.agentId,
            msgId: entry.message.msgId,
          }),
        );
        yield* results;
      }
      if (rows.length === 0) {
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
      if (
        error instanceof CommunicationError ||
        error instanceof RequestError
      ) {
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
