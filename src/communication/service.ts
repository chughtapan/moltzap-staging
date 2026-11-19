/**
 * @file One agent's communication service on a daemon that hosts every agent.
 *
 * The file opens with what the service accepts and returns, and the queries
 * that store each agent's ordered messages and which of them it may read. It
 * then reads an agent's inbox, its conversations and their ordered history. A
 * send orders the message in its conversation and saves it for every member in
 * one transaction.
 */

import { SqlClient } from "@effect/sql/SqlClient";
import {
  Cause,
  Context,
  Effect,
  Either,
  Encoding,
  Layer,
  Option,
  PubSub,
  Queue,
  Schema,
  type Scope,
} from "effect";
import { createHash } from "node:crypto";
import {
  afterCommit,
  executeQuery,
  type Generated,
  makeQueries,
  transaction,
  withAgentRows,
} from "../database.js";
import {
  canonicalIdentifier,
  compareAscii,
  decodeJson,
  encodeJson,
  exactStruct,
  isStrictlyAscending,
  type JsonRepresentationError,
  representationFailure,
} from "../encoding.js";
import { AgentCard, AgentId } from "../identity.js";
import {
  CommunicationError,
  Message,
  messageMembers,
  type MessageReference,
  type Page,
  PAGE_SIZE,
  type PaginatedRequest,
} from "./messages.js";

/** Selects a paged forward traversal or one bounded recent window of the conversation among `members`, given in any order. */
type HistoryQuery =
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
/**
 * How the recipient of a message holds it: `readable` in its inbox, with the
 * guidance its firewall gave as text, or withheld from it.
 */
export interface Received {
  readonly readable: boolean;
  readonly guidance?: string;
}

/**
 * Messages retain conversation order and count only their encoded value bytes.
 * Each says how the reading agent holds it; a message it wrote is not readable.
 */
export interface HistoryPage extends Page {
  readonly entries: ReadonlyArray<
    Received & {
      readonly message: Message;
      readonly messageBytes: number;
    }
  >;
  readonly earlierMessagesOmitted: boolean;
}

/** One agent's sends, its inbox and its history. */
export interface CommunicationService {
  /**
   * Returns once the message is ordered and saved for every member, held by
   * its recipient as `received` says. A message is sent once.
   */
  readonly send: (
    message: Message,
    received: Received,
  ) => Effect.Effect<void, CommunicationError>;
  /**
   * Subscribes to this agent's inbox. The effect it gives ends once a message
   * has become readable for the agent since the subscription.
   */
  readonly readable: Effect.Effect<Effect.Effect<void>, never, Scope.Scope>;
  /** Every conversation this agent has a message in, in no order, each as its members in agent-name order. */
  readonly listConversations: Effect.Effect<
    ReadonlyArray<readonly AgentCard[]>,
    CommunicationError
  >;
  /** Reads a conversation's messages in order. A page's `nextCursor` is the position of its last message. */
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
   * Published with an agent's key each time a message becomes readable for
   * it. Every agent of a daemon is given the same one.
   */
  readonly readable: PubSub.PubSub<string>;
}

const ConversationId = canonicalIdentifier("ConversationId", "cnv_", 32);
const ConversationMembers = exactStruct({
  kind: Schema.Literal("conversation_identity"),
  memberAgentIds: Schema.Tuple(AgentId, AgentId),
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
    Record<"sequence", Generated<number>> &
    Record<"read_number", number | null> &
    Record<"guidance", string | null>
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

/** The newest read number a message of the agent holds, or zero when it may read none. */
const newestReadNumber = Effect.fn("newestReadNumber")(function* (
  agentKey: string,
) {
  const db = yield* communicationQueries;
  return db
    .selectFrom("message_log")
    .where("agent_key", "=", agentKey)
    .select((eb) =>
      eb.fn.coalesce(eb.fn.max("read_number"), eb.val(0)).as("newest"),
    );
});

/**
 * Adds a message to one agent's history at its position. A message its agent
 * may read takes the agent's next read number. The advisory lock on the agent
 * is the last lock its transaction takes and is held until it ends, so
 * messages become readable in read-number order: a reader that has read to
 * one number has seen every message before it.
 */
const saveMessage = Effect.fn("saveMessage")(
  function* (
    agentKey: string,
    position: { readonly conversationId: string; readonly ordinal: number },
    message: Message,
    received: Received,
  ) {
    const db = yield* communicationQueries;
    const newest = yield* newestReadNumber(agentKey);
    if (received.readable) {
      const sql = yield* SqlClient;
      yield* sql`SELECT pg_advisory_xact_lock(hashtext(${agentKey}))`;
    }
    const encoded = yield* encodeJson(Message, message);
    yield* executeQuery(
      db.insertInto("message_log").values((eb) => ({
        agent_key: agentKey,
        conversation_id: position.conversationId,
        ordinal: position.ordinal,
        author_agent_id: message.author.agentId,
        msg_id: message.msgId,
        message: encoded,
        read_number: received.readable ? eb(newest, "+", 1) : null,
        guidance: received.guidance ?? null,
      })),
    );
  },
  (work, agentKey) => withAgentRows(agentKey, work),
);

/**
 * A bounded range of one agent's messages: those of a conversation in its
 * order, or with `afterReadNumber` those the agent may read after it in read
 * order. Each comes with its position in its conversation, the size of its
 * stored JSON text, its read number and its guidance.
 */
const readMessages = Effect.fn("readMessages")(
  function* (
    agentKey: string,
    range: {
      readonly conversationId?: string;
      readonly after?: number;
      readonly through?: number;
      readonly newestFirst?: boolean;
      readonly afterReadNumber?: number;
      readonly limit: number;
    },
  ) {
    const db = yield* communicationQueries;
    let query = db
      .selectFrom("message_log")
      .select(["ordinal", "message", "read_number", "guidance"])
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
    if (range.afterReadNumber !== undefined) {
      query = query.where("read_number", ">", range.afterReadNumber);
    }
    const ascending =
      range.afterReadNumber === undefined ? "ordinal" : "read_number";
    const rows = yield* executeQuery(
      range.newestFirst
        ? query.orderBy("ordinal", "desc")
        : query.orderBy(ascending),
    );
    return yield* Effect.forEach(
      rows,
      (row) =>
        decodeJson(Message, row.message).pipe(
          Effect.map((message) => ({
            ordinal: row.ordinal,
            message,
            bytes: Buffer.byteLength(row.message),
            readNumber: row.read_number,
            ...(row.guidance === null ? {} : { guidance: row.guidance }),
          })),
        ),
      { concurrency: 1 },
    );
  },
  (work, agentKey) => withAgentRows(agentKey, work),
);

/**
 * At most `limit` of the messages the agent stored under `agentKey` may read
 * after `cursor`, in read order, with the cursor to read on from and whether
 * more follow. Without a cursor the read starts from the newest and returns
 * none.
 */
export const readInbox = Effect.fn("readInbox")(function* (
  agentKey: string,
  { limit, cursor }: { readonly limit: number; readonly cursor?: string },
) {
  const [row] = yield* withAgentRows(
    agentKey,
    Effect.flatMap(newestReadNumber(agentKey), executeQuery),
  );
  const from = Number(cursor ?? row?.newest ?? 0);
  const rows = yield* readMessages(agentKey, {
    afterReadNumber: from,
    limit: limit + 1,
  });
  const page = rows.slice(0, limit);
  return {
    messages: page,
    nextCursor: String(page.at(-1)?.readNumber ?? from),
    hasMore: rows.length > limit,
  };
});

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
        ? String(last.ordinal)
        : undefined;
    if (window.newestFirst) {
      page.reverse();
    }
    return {
      entries: page.map((row) => ({
        message: row.message,
        messageBytes: row.bytes,
        readable: row.readNumber !== null,
        ...(row.guidance === undefined ? {} : { guidance: row.guidance }),
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
    return query.order === "newest"
      ? {
          conversationId,
          through:
            query.beforeMessage === undefined
              ? undefined
              : (yield* ordinalOf(
                  agentKey,
                  conversationId,
                  query.beforeMessage,
                )) - 1,
          newestFirst: true,
          limit: query.limit,
        }
      : {
          conversationId,
          after: Number(query.cursor ?? -1),
          newestFirst: false,
          limit: PAGE_SIZE,
        };
  });
}

/**
 * The conversation between the two distinct agents `members`, in either
 * order, `self` among them.
 */
function conversationOf(self: AgentCard, members: readonly AgentCard[]) {
  return Effect.gen(function* () {
    const ids = members.map((member) => member.agentId).sort(compareAgentIds);
    const [first, second] = ids;
    if (
      ids.length !== 2 ||
      first === undefined ||
      second === undefined ||
      !ids.includes(self.agentId)
    ) {
      return yield* refused("membership-invalid");
    }
    return yield* deriveConversationId([first, second]);
  });
}

/**
 * The identifier of the conversation among these members: a hash of their
 * agent identifiers, which must be given in ascending order with no repeats.
 */
function deriveConversationId(
  memberAgentIds: (typeof ConversationMembers.Type)["memberAgentIds"],
): Effect.Effect<typeof ConversationId.Type, JsonRepresentationError> {
  return isStrictlyAscending(memberAgentIds, compareAgentIds)
    ? encodeJson(ConversationMembers, {
        kind: "conversation_identity" as const,
        memberAgentIds,
      }).pipe(
        Effect.map(
          (members) =>
            `cnv_${createHash("sha256").update(members).digest("base64url")}`,
        ),
        Effect.flatMap(Schema.decodeUnknown(ConversationId)),
        Effect.mapError(representationFailure),
      )
    : Effect.fail(representationFailure());
}

/**
 * Compares validated agent IDs by decoded bytes, as membership lists require.
 */
function compareAgentIds(left: string, right: string): number {
  const leftBytes = decodeCanonicalBase64Url(left.slice(4));
  const rightBytes = decodeCanonicalBase64Url(right.slice(4));
  if (leftBytes === undefined || rightBytes === undefined) {
    return 0;
  }
  const length = Math.min(leftBytes.byteLength, rightBytes.byteLength);
  for (let index = 0; index < length; index += 1) {
    const difference = (leftBytes[index] ?? 0) - (rightBytes[index] ?? 0);
    if (difference !== 0) {
      return difference;
    }
  }
  return leftBytes.byteLength - rightBytes.byteLength;
}

/**
 * Returns bytes for canonical base64url, or undefined for another spelling.
 */
function decodeCanonicalBase64Url(value: string): Uint8Array | undefined {
  return Either.match(Encoding.decodeBase64Url(value), {
    onLeft: () => undefined,
    onRight: (bytes) =>
      Encoding.encodeBase64Url(bytes) === value ? bytes : undefined,
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

/** One hosted agent's database client and settings. */
interface HostedAgent {
  readonly sql: SqlClient;
  readonly settings: CommunicationSettings;
}
const agentRow = Schema.Struct({
  agentKey: Schema.String,
  ...AgentCard.fields,
});

/** One agent's sends, its inbox subscription and its history on this daemon. */
const makeCommunication = Effect.fn("makeCommunication")(function* (
  settings: CommunicationSettings,
) {
  const agent = { sql: yield* SqlClient, settings };
  return {
    send: (message, received) =>
      withSqlAndServiceErrors(agent, sendMessage(agent, message, received)),
    readable: PubSub.subscribe(settings.readable).pipe(
      Effect.map((keys) =>
        Queue.take(keys).pipe(
          Effect.repeat({ until: (key) => key === settings.agentKey }),
          Effect.asVoid,
        ),
      ),
    ),
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
 * Orders one message and saves it for its author and its recipient, or
 * changes nothing. Every write of a conversation first takes the row of the
 * conversation's next position, so two sends cannot deadlock. A waiting read
 * of the recipient wakes once a message it may read is committed.
 */
function sendMessage(
  { settings }: HostedAgent,
  message: Message,
  received: Received,
) {
  return transaction(
    Effect.gen(function* () {
      if (!Schema.equivalence(AgentCard)(message.author, settings.card)) {
        return yield* refused("membership-invalid");
      }
      const conversationId = yield* conversationOf(settings.card, [
        message.author,
        ...message.recipients,
      ]);
      const recipient = yield* recipientKey(message.recipients[0]);
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
      const position = { conversationId, ordinal: next.ordinal };
      yield* saveMessage(settings.agentKey, position, message, {
        readable: false,
      });
      yield* saveMessage(recipient, position, message, received);
      if (received.readable) {
        yield* afterCommit(PubSub.publish(settings.readable, recipient));
      }
    }),
  );
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
