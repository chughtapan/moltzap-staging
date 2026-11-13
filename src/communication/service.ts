/**
 * @file One agent's communication service on a daemon that hosts every agent.
 *
 * The file opens with what the service accepts, hands to the host and
 * returns, then reads an agent's conversations and their ordered history. A
 * send orders the message in its conversation and saves it for every member
 * in one transaction. Questions and answers are messages that the request
 * worker interprets.
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
import { exactStruct } from "../encoding.js";
import { AgentCard, type AgentName } from "../identity.js";
import { makeRequests } from "./collectives.js";
import { CommunicationError } from "./errors.js";
import {
  type CollectiveId,
  compareAgentIds,
  conversationNameKey,
  deriveConversationId,
  Message,
  messageInterpretation,
  messageMembers,
  type MessageReference,
  type PaginatedRequest,
  type PaginatedResult,
  ReceivedRequest,
  Request,
  RequestError,
  RequestResult,
  ResultSources,
} from "./messages.js";
import {
  acknowledgeMessage,
  communicationQueries,
  hasMessage,
  readMessages,
  saveMessage,
  type StoredMessage,
} from "./store.js";

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

interface HistoryInput {
  readonly agentKey: string;
  readonly local: AgentCard;
  readonly maximumHistoryRecords: number;
}
interface HistoryEnvironment extends HistoryInput {
  readonly session: string;
}
interface Window {
  readonly conversationId: string;
  readonly after: number;
  readonly through: number;
  readonly newest: boolean;
  readonly limit: number;
}
const cursorSchema = Schema.Struct({
  session: Schema.String,
  conversation: Schema.String,
  after: Schema.Int,
  through: Schema.Int,
});
const fail = (reason: CommunicationError["reason"]) =>
  new CommunicationError({ reason });

/** Conversation lookup, listing and paged history for one agent. */
function messageHistory(input: HistoryInput) {
  const environment = {
    ...input,
    session: randomBytes(16).toString("base64url"),
  };
  return {
    getConversation: (names: readonly AgentName[]) =>
      capturePersistence(getConversation(environment, names)).pipe(
        Effect.withSpan("getConversation"),
      ),
    listConversations: (query: ConversationQuery = {}) =>
      capturePersistence(listConversations(environment, query)).pipe(
        Effect.withSpan("listConversations"),
      ),
    readHistory: (query: HistoryQuery) =>
      capturePersistence(readHistory(environment, query)).pipe(
        Effect.withSpan("readHistory"),
      ),
  };
}

function getConversation(
  environment: HistoryEnvironment,
  names: readonly AgentName[],
) {
  return Effect.gen(function* () {
    if (
      !names.includes(environment.local.agentName) ||
      new Set(names).size !== names.length
    ) {
      return yield* fail("membership-invalid");
    }
    const wanted = conversationNameKey(names, environment.local.agentName);
    const conversations = yield* retainedConversations(environment);
    return conversations.find(
      (members) => conversationKey(environment, members) === wanted,
    );
  });
}

function listConversations(
  environment: HistoryEnvironment,
  query: ConversationQuery,
) {
  return Effect.gen(function* () {
    const conversations = yield* retainedConversations(environment);
    const after =
      query.cursor === undefined
        ? ""
        : yield* Schema.decodeUnknown(
            Schema.parseJson(
              Schema.Struct({
                session: Schema.Literal(environment.session),
                after: Schema.String,
              }),
            ),
          )(query.cursor).pipe(
            Effect.map((value) => value.after),
            Effect.mapError(() => fail("invalid-cursor")),
          );
    const selected = conversations.filter(
      (members) => conversationKey(environment, members) > after,
    );
    const page = selected.slice(0, 50);
    const last = page.at(-1);
    return {
      conversations: page,
      ...(selected.length > 50 && last !== undefined
        ? {
            nextCursor: JSON.stringify({
              session: environment.session,
              after: conversationKey(environment, last),
            }),
          }
        : {}),
    };
  });
}

function readHistory(environment: HistoryEnvironment, query: HistoryQuery) {
  return Effect.gen(function* () {
    const window = yield* historyWindow(environment, query);
    const selected = yield* scanWindow(environment, window);
    const page = selected.slice(0, window.limit);
    const last = page.at(-1);
    const nextCursor =
      !window.newest && selected.length > window.limit && last !== undefined
        ? JSON.stringify({
            session: environment.session,
            conversation: window.conversationId,
            after: last.ordinal,
            through: window.through,
          })
        : undefined;
    if (window.newest) {
      page.reverse();
    }
    return {
      entries: page.map((row) => ({
        message: row.message,
        messageBytes: row.bytes,
      })),
      earlierMessagesOmitted: window.newest && selected.length > window.limit,
      ...(nextCursor === undefined ? {} : { nextCursor }),
    };
  });
}

function retainedConversations(environment: HistoryEnvironment) {
  return Effect.gen(function* () {
    const db = yield* communicationQueries;
    const rows = yield* withAgentTransaction(
      environment.agentKey,
      executeQuery(
        db
          .selectFrom("message_log")
          .select("conversation_id as conversationId")
          .distinct()
          .where("agent_key", "=", environment.agentKey),
      ),
    );
    const conversations: Conversation[] = [];
    for (const row of rows) {
      const [entry] = yield* read(environment, {
        conversationId: row.conversationId,
        limit: 1,
      });
      if (entry !== undefined) {
        conversations.push(yield* membersOf(entry.message));
      }
    }
    return conversations.sort((a, b) =>
      compareText(
        conversationKey(environment, a),
        conversationKey(environment, b),
      ),
    );
  });
}

function historyWindow(environment: HistoryEnvironment, query: HistoryQuery) {
  return Effect.gen(function* () {
    const conversationId = yield* historyConversation(
      environment,
      query.members,
    );
    const [head] = yield* read(environment, {
      conversationId,
      newest: true,
      limit: 1,
    });
    const orderedMembers = [...query.members].sort((a, b) =>
      compareText(a.agentName, b.agentName),
    );
    if (
      head !== undefined &&
      !Schema.equivalence(Schema.Array(AgentCard))(
        messageMembers(head.message),
        orderedMembers,
      )
    ) {
      return yield* fail("membership-invalid");
    }
    const through = head?.ordinal ?? -1;
    if (query.order === "newest") {
      if (
        !Number.isSafeInteger(query.limit) ||
        query.limit < 1 ||
        query.limit > environment.maximumHistoryRecords
      ) {
        return yield* fail("content-invalid");
      }
      return {
        conversationId,
        after: -1,
        through:
          query.beforeMessage === undefined
            ? through
            : (yield* position(
                environment,
                conversationId,
                query.beforeMessage,
              )) - 1,
        newest: true,
        limit: query.limit,
      };
    }
    const window = {
      conversationId,
      after: -1,
      through,
      newest: false,
      limit: 50,
    };
    return query.cursor === undefined
      ? window
      : yield* continueWindow(environment, window, query.cursor);
  });
}

function historyConversation(
  environment: HistoryEnvironment,
  members: Conversation,
) {
  return Effect.gen(function* () {
    if (
      !members.some((member) => member.agentId === environment.local.agentId)
    ) {
      return yield* fail("membership-invalid");
    }
    const ids = members.map((member) => member.agentId).sort(compareAgentIds);
    const [first, second, ...rest] = ids;
    if (
      first === undefined ||
      second === undefined ||
      new Set(ids).size !== ids.length
    ) {
      return yield* fail("membership-invalid");
    }
    return yield* deriveConversationId([first, second, ...rest]);
  });
}

/** The position of a message this agent's history holds in the conversation. */
function position(
  environment: HistoryEnvironment,
  conversationId: string,
  reference: MessageReference,
) {
  return Effect.gen(function* () {
    const db = yield* communicationQueries;
    const [row] = yield* withAgentTransaction(
      environment.agentKey,
      executeQuery(
        db
          .selectFrom("message_log")
          .select("ordinal")
          .where("agent_key", "=", environment.agentKey)
          .where("conversation_id", "=", conversationId)
          .where("author_agent_id", "=", reference.authorAgentId)
          .where("msg_id", "=", reference.msgId),
      ),
    );
    return row === undefined ? yield* fail("content-invalid") : row.ordinal;
  });
}

function continueWindow(
  environment: HistoryEnvironment,
  window: Window,
  value: string,
) {
  return Effect.gen(function* () {
    const cursor = yield* Schema.decodeUnknown(Schema.parseJson(cursorSchema))(
      value,
    ).pipe(Effect.mapError(() => fail("invalid-cursor")));
    const sameRequest =
      cursor.session === environment.session &&
      cursor.conversation === window.conversationId;
    const validBounds =
      cursor.after >= window.after &&
      cursor.after <= cursor.through &&
      cursor.through <= window.through;
    if (!sameRequest || !validBounds) {
      return yield* fail("invalid-cursor");
    }
    return { ...window, after: cursor.after, through: cursor.through };
  });
}

/** The window's messages in its order, without the closes of questions, up to one past its limit. */
function scanWindow(environment: HistoryEnvironment, window: Window) {
  return Effect.gen(function* () {
    const selected: StoredMessage[] = [];
    let { after, through } = window;
    while (after < through) {
      const rows = yield* read(environment, {
        conversationId: window.conversationId,
        after,
        through,
        newest: window.newest,
        limit: 50,
      });
      const last = rows.at(-1);
      if (last === undefined) {
        break;
      }
      for (const row of rows) {
        if (yield* applicationMessage(row.message)) {
          selected.push(row);
        }
        if (selected.length > window.limit) {
          return selected;
        }
      }
      if (window.newest) {
        through = last.ordinal - 1;
      } else {
        after = last.ordinal;
      }
    }
    return selected;
  });
}

function applicationMessage(message: Message) {
  return messageInterpretation(message).pipe(
    Effect.map(
      Option.match({
        onNone: () => true,
        onSome: (value) => value.kind !== "close",
      }),
    ),
    Effect.catchAll(() => Effect.succeed(false)),
  );
}

function read(
  environment: HistoryEnvironment,
  query: Parameters<typeof readMessages>[1],
) {
  return readMessages(environment.agentKey, query);
}

function conversationKey(
  environment: HistoryEnvironment,
  members: Conversation,
) {
  return conversationNameKey(
    members.map((member) => member.agentName),
    environment.local.agentName,
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
    ? Effect.fail(fail("persistence-failed"))
    : Effect.succeed<Conversation>([first, second, ...rest]);
}

function capturePersistence<A, E, R>(work: Effect.Effect<A, E, R>) {
  return work.pipe(
    Effect.mapError((error) =>
      error instanceof CommunicationError ? error : fail("persistence-failed"),
    ),
  );
}

/** Keeps a background loop of one agent running after a failure. */
export { restarting } from "./collectives.js";

interface Environment {
  readonly sql: SqlClient;
  readonly settings: CommunicationSettings;
  /** Opened when a message or a result is saved for this agent. */
  readonly arrived: Effect.Latch;
}
const unavailable = () =>
  new CommunicationError({ reason: "persistence-failed" });
const invalid = () => new CommunicationError({ reason: "membership-invalid" });
const agentRow = Schema.Struct({
  agentKey: Schema.String,
  ...AgentCard.fields,
});

/** One agent's sends, incoming work and history on this daemon. */
const makeCommunication = Effect.fn("makeCommunication")(function* (
  settings: CommunicationSettings,
) {
  const arrived = yield* Effect.makeLatch();
  settings.consumers.set(settings.agentKey, arrived);
  const environment = { sql: yield* SqlClient, settings, arrived };
  const history = messageHistory({
    agentKey: settings.agentKey,
    local: settings.card,
    maximumHistoryRecords: settings.maximumHistoryRecords ?? 1024,
  });
  const requests = yield* makeRequests(settings.agentKey, {
    self: settings.card,
    send: (message) =>
      capture(environment, sendMessage(environment, message)).pipe(
        Effect.mapError((error) =>
          error instanceof CommunicationError ? error : unavailable(),
        ),
      ),
    completed: arrived.open,
  });
  const consumer = yield* Effect.makeSemaphore(1);
  return {
    send: (value) => capture(environment, send(environment, requests, value)),
    getRequest: (operationId) =>
      capture(environment, requests.getRequest(operationId)),
    consume: (handle) =>
      consumer.withPermits(1)(consume(environment, requests, handle)),
    getConversation: (names) =>
      capture(environment, history.getConversation(names)),
    listConversations: (query = {}) =>
      capture(environment, history.listConversations(query)),
    readHistory: (query) => capture(environment, history.readHistory(query)),
  } satisfies CommunicationService;
});

/**
 * Marks the database as serving `mode`. Fails with `membership-invalid` when
 * it already serves another.
 */
const claimMode = Effect.fn("claimMode")(
  function* (mode: string) {
    const db = yield* communicationQueries;
    yield* executeQuery(
      db
        .insertInto("communication_mode")
        .values({ singleton: 1, kind: mode })
        .onConflict((conflict) => conflict.column("singleton").doNothing()),
    );
    const [row] = yield* executeQuery(
      db
        .selectFrom("communication_mode")
        .select("kind")
        .where("singleton", "=", 1),
    );
    if (row?.kind !== mode) {
      return yield* invalid();
    }
  },
  Effect.catchTag("SqlError", unavailable),
);

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
    Layer.scoped(Communication, makeCommunication(settings));
}

type Requests = Effect.Effect.Success<ReturnType<typeof makeRequests>>;

/** A question starts a request, an answer joins one, and anything else is sent as it is. */
function send(environment: Environment, requests: Requests, value: Outgoing) {
  if ("kind" in value) {
    return requests.accept(value);
  }
  return Effect.gen(function* () {
    const decoded = yield* messageInterpretation(value).pipe(
      Effect.catchTag("CollectivePartInvalidError", () =>
        Effect.succeed(Option.none()),
      ),
    );
    return yield* Option.isSome(decoded) && decoded.value.kind === "response"
      ? requests.answer({ ...value, operation: decoded.value })
      : sendMessage(environment, value);
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
      return yield* invalid();
    }
    const { agentKey, ...stored } = yield* Schema.decodeUnknown(agentRow)(raw);
    if (!Schema.equivalence(AgentCard)(recipient, stored)) {
      return yield* invalid();
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
function sendMessage({ settings }: Environment, message: Message) {
  return transaction(
    Effect.gen(function* () {
      if (!Schema.equivalence(AgentCard)(message.author, settings.card)) {
        return yield* invalid();
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
        return yield* unavailable();
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
      return yield* invalid();
    }
    const memberIds = [first, second, ...rest] as const;
    const distinct = new Set(memberIds).size === memberIds.length;
    const local = members.some((member) =>
      Schema.equivalence(AgentCard)(member, settings.card),
    );
    if (!distinct || !local || settings.permits?.(members) === false) {
      return yield* invalid();
    }
    return yield* deriveConversationId(memberIds);
  });
}

/**
 * Hands each pending message and question to `handle` in the order they were
 * saved, and each result as soon as it is complete, so a result precedes any
 * message saved after the answer that completed it. An item is marked consumed
 * only after `handle` succeeds. With nothing pending it waits until a message
 * or a result is saved for this agent.
 */
function consume<E, R>(
  environment: Environment,
  requests: Requests,
  handle: (incoming: Incoming) => Effect.Effect<void, E, R>,
) {
  const { agentKey, card } = environment.settings;
  const results = capture(
    environment,
    requests.deliverNextResult((delivery) =>
      handle({ ...delivery.item, sources: delivery.sources }),
    ),
  ).pipe(Effect.repeat({ while: (delivered) => delivered }));
  return Effect.gen(function* () {
    while (true) {
      yield* environment.arrived.close;
      yield* results;
      const rows = yield* capture(
        environment,
        readMessages(agentKey, { pendingFor: card.agentId, limit: 50 }),
      );
      for (const entry of rows) {
        const classified = yield* capture(
          environment,
          requests.receive(entry.message),
        );
        if (Option.isSome(classified)) {
          yield* handle(classified.value);
        }
        yield* capture(
          environment,
          acknowledgeMessage(agentKey, {
            authorAgentId: entry.message.author.agentId,
            msgId: entry.message.msgId,
          }),
        );
        yield* results;
      }
      if (rows.length === 0) {
        yield* environment.arrived.await;
      }
    }
  });
}

function capture<A, E, R>({ sql }: Environment, work: Effect.Effect<A, E, R>) {
  return work.pipe(
    Effect.provideService(SqlClient, sql),
    Effect.mapError((error) => {
      if (
        error instanceof CommunicationError ||
        error instanceof RequestError
      ) {
        return error;
      }
      if (
        typeof error === "object" &&
        error !== null &&
        "reason" in error &&
        error.reason === "conflict"
      ) {
        return new CommunicationError({ reason: "message-conflict" });
      }
      return unavailable();
    }),
  );
}
