/**
 * @file The server one daemon is: each owner's agent, the messages a server
 * agent and its clients send each other, every agent's inbox and history,
 * and the decisions of the server agent's firewall, with the tables they
 * live in; and the MCP tools and inbox event through which an agent's host
 * reaches them. A message is stored once, for both of its agents. A query
 * that fails ends its caller as a defect.
 */

import { SqlClient } from "@effect/sql";
import * as PgClient from "@effect/sql-pg/PgClient";
import {
  type AuthInfo,
  createMcpHandler,
  McpServer,
  type StandardSchemaWithJSON,
} from "@modelcontextprotocol/server";
import { EventsExtension } from "@modelcontextprotocol/server/ext/events";
import { nodeWebhookSender } from "@modelcontextprotocol/server/ext/events/node";
import {
  Config,
  Context,
  Data,
  Duration,
  Effect,
  JSONSchema,
  Layer,
  Order,
  PubSub,
  Queue,
  type Redacted,
  Runtime,
  Schema,
} from "effect";
import { AsyncLocalStorage } from "node:async_hooks";
import { randomBytes } from "node:crypto";
import packageJson from "../package.json" with { type: "json" };
import {
  type CheckRecord,
  checkRecordSchema,
  type Firewall,
  type FirewallRefusalReason,
  LocalGuidance,
} from "./firewall.js";
import {
  AGENT_NAME_RULE,
  AgentCard,
  AgentId,
  AgentName,
  type CommunicationError,
  Message,
  MsgId,
  PrincipalId,
  wellFormedString,
} from "./messages.js";

/** The version of this package, as the daemon reports it to MCP clients and in telemetry. */
export const packageVersion: string = packageJson.version;

/** Authentication supplies both fields of an agent's owner. */
export interface Owner {
  readonly issuer: string;
  readonly subject: string;
}

/** The scheme of an address naming one agent. */
const AGENT_ADDRESS_PREFIX = "agent:";

const isAgentName = Schema.is(AgentName);

/** The address of one agent. */
const AgentAddress = Schema.String.pipe(
  Schema.filter(
    (value) =>
      value.startsWith(AGENT_ADDRESS_PREFIX) &&
      isAgentName(value.slice(AGENT_ADDRESS_PREFIX.length)),
    { description: `agent:<name>; a name is ${AGENT_NAME_RULE}` },
  ),
  Schema.brand("AgentAddress"),
  Schema.annotations({ identifier: "AgentAddress" }),
);
type AgentAddress = typeof AgentAddress.Type;

/** How the agents of the daemon address the agent named `agentName`. */
const addressOf = (agentName: AgentName): AgentAddress =>
  AgentAddress.make(`${AGENT_ADDRESS_PREFIX}${agentName}`);

/** One message as the host reads it: its conversation's address, the message and any guidance. */
const InboxEntry = Schema.Struct({
  address: AgentAddress,
  message: Message,
  guidance: Schema.optionalWith(LocalGuidance, { exact: true }),
});

/**
 * One record of a conversation's history: a message the agent sent, or the
 * inbox entry of a message it received.
 */
const HistoryRecord = Schema.Union(
  Schema.Struct({
    kind: Schema.Literal("sent"),
    message: Schema.typeSchema(Message),
  }),
  Schema.Struct({
    kind: Schema.Literal("received"),
    address: AgentAddress,
    message: Schema.typeSchema(Message),
    guidance: Schema.optionalWith(LocalGuidance, { exact: true }),
  }),
);
type InboxEntry = typeof InboxEntry.Type;
type HistoryRecord = typeof HistoryRecord.Type;

/** What every agent's operations share on one daemon. */
export interface Daemon {
  /** The agent whose firewall checks every message. Every other agent is its client. */
  readonly serverAgent: AgentCard;
  /** The server agent's firewall. */
  readonly firewall: Firewall;
  /** The connection every operation queries through. */
  readonly sql: SqlClient.SqlClient;
  /** The norm documents the server agent holds, which every agent then reads with `read_norms`. */
  readonly norms?: ReadonlyArray<{
    readonly name: string;
    readonly description: string;
    readonly document: string;
  }>;
  /** The lock of each conversation a message was sent in, by its client's agent id. */
  readonly conversationLocks: Map<string, Effect.Semaphore>;
  /** Published with an agent's id each time a message becomes readable for it. */
  readonly readable: PubSub.PubSub<string>;
}

/**
 * The environment names no database URL.
 * Exported so the daemon's inferred startup failure can be named.
 * @public
 */
export class DatabaseUrlMissingError extends Data.TaggedError(
  "DatabaseUrlMissingError",
) {
  override get message(): string {
    return "Set SHARNESS_DATABASE_URL to the database the daemon uses";
  }
}

/** The URL of the daemon's database. */
export const databaseUrl = Config.redacted("SHARNESS_DATABASE_URL").pipe(
  Effect.catchTag("ConfigError", () => new DatabaseUrlMissingError()),
  Effect.withSpan("databaseUrl"),
);

/**
 * The daemon's tables could not be created. Messages name the operator's
 * action without disclosing SQL.
 * Exported so the daemon's inferred startup failure can be named.
 * @public
 */
export class DatabasePreparationError extends Data.TaggedError(
  "DatabasePreparationError",
)<{
  readonly reason: "schema" | "preparation";
}> {
  override get message(): string {
    return this.reason === "schema"
      ? "The database holds tables that are not this daemon's; give the daemon an empty database"
      : "The daemon could not create its tables; check that SHARNESS_DATABASE_URL names a reachable database its login may create tables in";
  }
}

/**
 * The owner's agent has another name than the one asked for, or the name
 * belongs to another owner's agent. Carries no account claims.
 * Exported so dependent declarations can name the inferred Effect failure.
 * @public
 */
export class OwnershipError extends Data.TaggedError("OwnershipError") {}

/**
 * Creates the tables in an empty database, in one transaction, and leaves a
 * database that already holds them as it is, so a login that cannot create
 * tables serves a database another login prepared. A database that holds
 * other tables is refused. `agent_cards` holds each owner and the card of the
 * agent created for them. `message_log` holds each message once, in the
 * order it was stored; one its recipient may read holds a `read_number`,
 * which orders that agent's inbox, and the guidance the firewall gave with
 * it; `checked_at` is when the server agent's firewall checked it.
 * `firewall_checks` holds the record of each check of that firewall.
 */
const prepareTables = Effect.fn("prepareDatabase")(
  function* (sql: SqlClient.SqlClient) {
    const tables = yield* sql<{
      name: string;
    }>`SELECT tablename AS name FROM pg_tables WHERE schemaname=current_schema()`;
    if (tables.some((table) => table.name === "agent_cards")) {
      return;
    }
    if (tables.length > 0) {
      return yield* new DatabasePreparationError({ reason: "schema" });
    }
    yield* sql.withTransaction(
      Effect.all(
        [
          sql`CREATE TABLE agent_cards (
            agent_id TEXT PRIMARY KEY,
            issuer TEXT NOT NULL,
            subject TEXT NOT NULL,
            agent_name TEXT NOT NULL UNIQUE,
            principal_id TEXT NOT NULL,
            UNIQUE(issuer, subject)
          )`,
          sql`CREATE TABLE message_log (
            sequence INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            author_id TEXT NOT NULL,
            recipient_id TEXT NOT NULL,
            msg_id TEXT NOT NULL,
            message TEXT NOT NULL,
            read_number INTEGER,
            guidance TEXT,
            checked_at BIGINT,
            UNIQUE(author_id, msg_id),
            UNIQUE(recipient_id, read_number)
          )`,
          sql`CREATE INDEX message_log_conversation ON message_log(author_id, recipient_id, sequence)`,
          sql`CREATE TABLE firewall_checks (
            sequence INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            decided_at BIGINT NOT NULL,
            record TEXT NOT NULL
          )`,
          sql`CREATE INDEX firewall_checks_time ON firewall_checks(decided_at, sequence)`,
        ],
        { concurrency: 1, discard: true },
      ),
    );
  },
  Effect.catchTag(
    "SqlError",
    () => new DatabasePreparationError({ reason: "preparation" }),
  ),
);

const agentCards = Schema.Array(AgentCard);

/** The agents whose row holds each of `where`'s values in its column. */
const agentsWith = (
  sql: SqlClient.SqlClient,
  where: Readonly<Record<string, string>>,
) =>
  sql`SELECT agent_id AS "agentId", agent_name AS "agentName",
      principal_id AS "principalId"
    FROM agent_cards WHERE ${sql.and(
      Object.entries(where).map(
        ([column, value]) => sql`${sql(column)} = ${value}`,
      ),
    )}`.pipe(Effect.flatMap(Schema.decodeUnknown(agentCards)), Effect.orDie);

/**
 * The owner's agent, created on first use with a new identifier and
 * principal. Concurrent first calls for one owner return the same agent. With
 * `agentName`, the agent is created under that name and an existing agent
 * must already have it.
 */
const getOrCreateOwnedAgent = Effect.fn("getOrCreateOwnedAgent")(function* (
  { sql }: Pick<Daemon, "sql">,
  owner: Owner,
  agentName?: AgentName,
) {
  yield* sql`INSERT INTO agent_cards ${sql.insert({
    ...owner,
    agent_id: AgentId.mint(),
    agent_name: agentName ?? `agent-${randomBytes(12).toString("hex")}`,
    principal_id: PrincipalId.mint(),
  })} ON CONFLICT DO NOTHING`.pipe(Effect.orDie);
  const [saved] = yield* agentsWith(sql, { ...owner });
  if (
    saved === undefined ||
    (agentName !== undefined && saved.agentName !== agentName)
  ) {
    return yield* new OwnershipError();
  }
  return saved;
});

/**
 * Opens the daemon on the database at `url` until the scope closes:
 * connects, prepares the tables, and finds or creates the server agent, whose
 * card `server.firewall` builds its firewall from. A URL that reaches no
 * PostgreSQL server fails the connection.
 */
export const openDaemon = Effect.fn("openDaemon")(function* (
  url: Redacted.Redacted,
  server: {
    readonly owner: Owner;
    readonly name: AgentName;
    readonly firewall: (me: AgentCard) => Firewall;
    readonly norms?: Daemon["norms"];
  },
) {
  const sql = Context.get(
    yield* Layer.build(PgClient.layer({ url })),
    SqlClient.SqlClient,
  );
  yield* prepareTables(sql);
  const serverAgent = yield* getOrCreateOwnedAgent(
    { sql },
    server.owner,
    server.name,
  );
  const opened: Daemon = {
    serverAgent,
    firewall: server.firewall(serverAgent),
    sql,
    ...(server.norms === undefined ? {} : { norms: server.norms }),
    conversationLocks: new Map(),
    readable: yield* PubSub.unbounded<string>(),
  };
  return opened;
});

/** The agent named `name`, if there is one. */
const agentNamed = (daemon: Daemon, name: string) =>
  Effect.map(agentsWith(daemon.sql, { agent_name: name }), ([agent]) => agent);

/** The most inbox entries or history records one page holds. */
const PAGE_SIZE = 50;

/** A send that cannot be a message. The host may show `message` to its agent. */
class SendError extends Data.TaggedError("SendError")<{
  readonly reason: "content-invalid";
}> {
  override get message(): string {
    return "send failed: the message content is invalid";
  }
}

const refusalText: Readonly<Record<FirewallRefusalReason, string>> = {
  "rule-violation": "It breaks a rule of the agent's message policy.",
  "unresolved-evidence":
    "The policy check could not settle whether the message is allowed.",
  "check-unavailable": "The policy check could not complete.",
};

/**
 * A send the server agent's firewall refused. The sender can act on
 * registered feedback without seeing the check's evidence. `rule` and
 * `detail` are those of a structure check that refused or passed the message.
 * The message states each deciding rule's registered feedback as an error, or
 * the reason when there is none, then the check.
 */
class FirewallRefusal extends Data.TaggedError("FirewallRefusal")<{
  readonly reason: "firewall-refused";
  readonly checkId: string;
  readonly refusal: FirewallRefusalReason;
  readonly rule?: string;
  readonly feedback: readonly string[];
  readonly detail?: unknown;
}> {
  override get message(): string {
    const rule = this.rule === undefined ? "" : `, rule ${this.rule}`;
    return [
      ...(this.feedback.length === 0
        ? [`Error: ${refusalText[this.refusal]}`]
        : this.feedback.map((text) =>
            text.startsWith("Error:") ? text : `Error: ${text}`,
          )),
      `Check: ${this.checkId}${rule}; ${this.refusal}.`,
    ].join(" ");
  }
}

/** One stored message: its place among all messages, and the read number, guidance and check time it holds. */
const storedMessages = Schema.Array(
  Schema.Struct({
    sequence: Schema.Number,
    message: Schema.parseJson(Message),
    readNumber: Schema.NullOr(Schema.Number),
    guidance: Schema.NullOr(Schema.parseJson(LocalGuidance)),
    checkedAt: Schema.NullOr(Schema.NumberFromString),
  }),
);
type StoredMessage = (typeof storedMessages.Type)[number];

/** A stored message as the inbox entry its recipient reads, under the address of its author. */
const entryOf = ({ message, guidance }: StoredMessage): InboxEntry => ({
  address: addressOf(message.author.agentName),
  message,
  ...(guidance === null ? {} : { guidance }),
});

/** Holds the conversation of the server agent with `client` until the scope closes. */
function lockConversation(daemon: Daemon, client: AgentCard) {
  return Effect.sync(() => {
    const lock =
      daemon.conversationLocks.get(client.agentId) ??
      Effect.unsafeMakeSemaphore(1);
    daemon.conversationLocks.set(client.agentId, lock);
    return lock;
  }).pipe(
    Effect.flatMap((lock) =>
      Effect.acquireReleaseInterruptible(lock.take(1), () => lock.release(1)),
    ),
    Effect.asVoid,
  );
}

/**
 * The firewall's reply to a message it withheld, as an ordinary message from
 * the server agent to the withheld message's author that names it as the one
 * it answers. A reply that is not a valid message is not sent.
 */
function replyWith(
  server: AgentCard,
  withheld: Message,
  reply: { readonly text: string; readonly record: CheckRecord },
) {
  return Message.create({
    author: server,
    recipient: withheld.author,
    text: reply.text,
    replyToMsgId: withheld.msgId,
  }).pipe(
    Effect.map((message) => ({ message, record: reply.record })),
    Effect.orElseSucceed(() => undefined),
  );
}

const savedRecord = Schema.parseJson(checkRecordSchema);

/** Saves what the server agent's firewall concluded of one message. */
const storeDecision = (sql: SqlClient.SqlClient, record: CheckRecord) =>
  Effect.flatMap(
    Schema.encode(savedRecord)(record),
    (saved) =>
      sql`INSERT INTO firewall_checks ${sql.insert({
        decided_at: record.time,
        record: saved,
      })}`,
  ).pipe(Effect.orDie);

/**
 * Stores one message with the decision of its check, when it had one. A
 * message its recipient may read takes that agent's next read number. The
 * advisory lock on the recipient is held until the enclosing transaction
 * ends, and a transaction takes it for one agent only, so messages become
 * readable in read-number order: a reader that has read to one number has
 * seen every message before it.
 */
const storeMessage = Effect.fn("saveMessage")(function* (
  sql: SqlClient.SqlClient,
  message: Message,
  held: {
    readonly readable: boolean;
    readonly guidance?: LocalGuidance;
    readonly record?: CheckRecord;
  },
) {
  const recipient = message.recipient.agentId;
  const encoded = yield* Schema.encode(Schema.parseJson(Message))(message);
  const guidance =
    held.guidance === undefined
      ? null
      : yield* Schema.encode(Schema.parseJson(LocalGuidance))(held.guidance);
  yield* sql.withTransaction(
    Effect.gen(function* () {
      if (held.readable) {
        yield* sql`SELECT pg_advisory_xact_lock(hashtext(${recipient}))`;
      }
      yield* sql`INSERT INTO message_log
          (author_id, recipient_id, msg_id, message, guidance, checked_at,
            read_number)
        VALUES (${message.author.agentId}, ${recipient}, ${message.msgId},
          ${encoded}, ${guidance}, ${held.record?.time ?? null}, ${
            held.readable
              ? sql`(SELECT COALESCE(MAX(read_number), 0) + 1 FROM message_log
                  WHERE recipient_id = ${recipient})`
              : sql`NULL`
          })`;
      if (held.record !== undefined) {
        yield* storeDecision(sql, held.record);
      }
    }),
  );
}, Effect.orDie);

/**
 * The messages two agents sent each other, oldest first: every one, or with
 * `page` at most `limit` of those stored after the message at `after`.
 */
const conversationRows = Effect.fn("readConversation")(function* (
  sql: SqlClient.SqlClient,
  one: AgentCard,
  other: AgentCard,
  page?: { readonly after: number; readonly limit: number },
) {
  const rows = yield* sql`SELECT sequence, message, guidance,
      read_number AS "readNumber", checked_at AS "checkedAt" FROM message_log
    WHERE ((author_id = ${one.agentId} AND recipient_id = ${other.agentId})
      OR (author_id = ${other.agentId} AND recipient_id = ${one.agentId}))
      AND sequence > ${page?.after ?? 0}
    ORDER BY sequence LIMIT ${page?.limit ?? null}`;
  return yield* Schema.decodeUnknown(storedMessages)(rows);
}, Effect.orDie);

/**
 * Sends one message between the server agent and a client, whichever of the
 * two is `author`, under their conversation's lock: a check reads the
 * conversation's earlier messages, so its messages are checked and stored one
 * at a time, while other conversations do not wait. The server agent's
 * firewall checks the message as one its agent sends or is sent. A message
 * the server agent may not send, or one whose check could not run, fails the
 * send and is not stored. Otherwise one transaction stores the message with
 * the decision and, for a message withheld from the server agent, the
 * firewall's reply to its sender with the decision that allows it. No
 * transaction is open while a check runs. A waiting read wakes once a message
 * it may read is stored. Each call creates a new message, so retrying an
 * uncertain send can duplicate it.
 */
const send = Effect.fn("send")(
  function* (
    daemon: Daemon,
    sent: {
      readonly author: AgentCard;
      readonly recipient: AgentCard;
      readonly text: string;
      readonly replyToMsgId?: MsgId;
    },
  ) {
    const { author, recipient } = sent;
    const { serverAgent, firewall, sql } = daemon;
    const outgoing = author.agentId === serverAgent.agentId;
    const message = yield* Message.create(sent).pipe(
      Effect.catchTag(
        "CommunicationError",
        ({ reason }: CommunicationError) => new SendError({ reason }),
      ),
    );
    yield* lockConversation(daemon, outgoing ? recipient : author);
    const earlier = (yield* conversationRows(sql, author, recipient)).map(
      (stored) => ({
        message: stored.message,
        allowed: stored.readNumber !== null,
        ...(stored.checkedAt === null ? {} : { checkedAt: stored.checkedAt }),
      }),
    );
    const result = outgoing
      ? yield* firewall.checkOutgoing(message, earlier)
      : yield* firewall.checkIncoming(message, earlier);
    if (
      result.kind === "refused" &&
      (outgoing || result.reason === "check-unavailable")
    ) {
      if (result.record !== undefined) {
        yield* storeDecision(sql, result.record);
      }
      return yield* new FirewallRefusal({
        reason: "firewall-refused",
        checkId: result.checkId,
        refusal: result.reason,
        ...(result.rule === undefined ? {} : { rule: result.rule }),
        feedback: result.feedback,
        ...(result.detail === undefined ? {} : { detail: result.detail }),
      });
    }
    const reply =
      result.kind === "refused" && result.reply !== undefined
        ? yield* replyWith(serverAgent, message, result.reply)
        : undefined;
    yield* sql
      .withTransaction(
        Effect.gen(function* () {
          yield* storeMessage(sql, message, {
            readable: result.kind === "allowed",
            ...(result.kind === "allowed" && result.guidance !== undefined
              ? { guidance: result.guidance }
              : {}),
            ...(result.record === undefined ? {} : { record: result.record }),
          });
          if (reply !== undefined) {
            yield* storeMessage(sql, reply.message, {
              readable: true,
              record: reply.record,
            });
          }
        }),
      )
      .pipe(Effect.orDie);
    if (result.kind === "allowed") {
      yield* PubSub.publish(daemon.readable, recipient.agentId);
    } else if (reply !== undefined) {
      yield* PubSub.publish(daemon.readable, author.agentId);
    }
    return message.msgId;
  },
  Effect.scoped,
  (effect, ...[, { author }]) =>
    Effect.tapError(effect, (error) =>
      Effect.logWarning("Send failed").pipe(
        Effect.annotateLogs({
          agentId: author.agentId,
          error: error._tag,
          reason: error.message,
        }),
      ),
    ),
);

/**
 * At most `limit` of the inbox entries of the agent after `cursor`, in read
 * order, with the cursor to read on from and whether more entries follow.
 * Without a cursor the read starts from the newest entry and returns none.
 */
const readInbox = Effect.fn("readInbox")(function* (
  { sql }: Daemon,
  agentId: string,
  { limit, cursor }: { readonly limit: number; readonly cursor?: string },
) {
  const [newest] = yield* sql<{
    readNumber: number;
  }>`SELECT COALESCE(MAX(read_number), 0) AS "readNumber" FROM message_log
    WHERE recipient_id = ${agentId}`;
  const from = Number(cursor ?? newest?.readNumber ?? 0);
  const rows = yield* sql`SELECT sequence, message, guidance,
      read_number AS "readNumber", checked_at AS "checkedAt" FROM message_log
    WHERE recipient_id = ${agentId} AND read_number > ${from}
    ORDER BY read_number LIMIT ${limit + 1}`.pipe(
    Effect.flatMap(Schema.decodeUnknown(storedMessages)),
  );
  const page = rows.slice(0, limit);
  return {
    entries: page.map(entryOf),
    nextCursor: String(page.at(-1)?.readNumber ?? from),
    hasMore: rows.length > limit,
  };
}, Effect.orDie);

/** One inbox read: the cursor to read after and, in seconds, how long to wait for an entry when none follows it. */
interface ReceiveRequest {
  readonly cursor?: string;
  readonly wait?: number;
}

/**
 * A page of an agent's inbox entries after `cursor`, in the order they were
 * allowed, with the cursor that reads on from the last of them. Without a
 * cursor the read starts from now. With `wait` seconds and no entry to
 * return, it reads once more when an entry becomes readable or the wait
 * ends; a waiting read subscribes before it reads, so an entry that becomes
 * readable after the read is not missed.
 */
const receive = Effect.fn("receive")(function* (
  daemon: Daemon,
  { agentId }: AgentCard,
  { cursor, wait }: ReceiveRequest,
) {
  const read = (from: typeof cursor) =>
    readInbox(daemon, agentId, { limit: PAGE_SIZE, cursor: from }).pipe(
      Effect.map(({ entries, nextCursor }) => ({ entries, nextCursor })),
    );
  if (wait === undefined) {
    return yield* read(cursor);
  }
  const readable = yield* PubSub.subscribe(daemon.readable);
  const page = yield* read(cursor);
  if (page.entries.length > 0) {
    return page;
  }
  yield* Queue.take(readable).pipe(
    Effect.repeat({ until: (id) => id === agentId }),
    Effect.timeoutOption(Duration.seconds(wait)),
  );
  return yield* read(page.nextCursor);
}, Effect.scoped);

/**
 * One page of the messages of `self`'s conversation with `other` as history
 * records, oldest first, after the cursor an earlier page returned: each one
 * `self` sent, and each one it received and may read. A page's `nextCursor`
 * is the place of its last message. Rule edits leave stored inbox entries as
 * they are.
 */
const history = Effect.fn("historyRecords")(function* (
  { sql }: Daemon,
  self: AgentCard,
  other: AgentCard,
  cursor?: string,
) {
  const stored = yield* conversationRows(sql, self, other, {
    after: Number(cursor ?? 0),
    limit: PAGE_SIZE + 1,
  });
  const page = stored.slice(0, PAGE_SIZE);
  const last = page.at(-1);
  return {
    records: page.flatMap((row): HistoryRecord[] => {
      if (row.message.author.agentId === self.agentId) {
        return [{ kind: "sent", message: row.message }];
      }
      return row.readNumber === null
        ? []
        : [{ kind: "received", ...entryOf(row) }];
    }),
    ...(stored.length > PAGE_SIZE && last !== undefined
      ? { nextCursor: String(last.sequence) }
      : {}),
  };
});

/** The address of every conversation of `self`, in order. */
const conversationsOf = Effect.fn("conversationsOf")(function* (
  { sql }: Daemon,
  self: AgentCard,
) {
  const rows = yield* sql`SELECT DISTINCT peer.agent_name AS "agentName"
    FROM message_log JOIN agent_cards peer ON peer.agent_id =
      CASE WHEN author_id = ${self.agentId} THEN recipient_id ELSE author_id END
    WHERE author_id = ${self.agentId} OR recipient_id = ${self.agentId}`;
  const peers = yield* Schema.decodeUnknown(
    Schema.Array(Schema.Struct({ agentName: AgentName })),
  )(rows);
  return {
    addresses: peers
      .map((peer) => addressOf(peer.agentName))
      .sort(Order.string),
  };
}, Effect.orDie);

/** The record of each of the latest hundred checks of the server agent's firewall at or after `since`, newest first. */
const readDecisions = Effect.fn("readDecisions")(function* (
  { sql }: Daemon,
  since: number,
) {
  const rows = yield* sql`SELECT record FROM firewall_checks
    WHERE decided_at >= ${since}
    ORDER BY decided_at DESC, sequence DESC LIMIT 100`;
  const saved = yield* Schema.decodeUnknown(
    Schema.Array(Schema.Struct({ record: savedRecord })),
  )(rows);
  return saved.map(({ record }) => record);
}, Effect.orDie);

/**
 * An address a send or a history read cannot use: it names no agent of this
 * daemon (`unknown-agent`) or the calling agent itself (`invalid-address`).
 */
class AddressError extends Data.TaggedError("AddressError")<{
  readonly reason: "unknown-agent" | "invalid-address";
  readonly address: string;
}> {
  override get message(): string {
    return this.reason === "unknown-agent"
      ? `This daemon has no agent named ${this.address}. Check the name with its owner.`
      : `The address ${this.address} names you, not another agent.`;
  }
}

/**
 * A send that ended unexpectedly: it may already have happened, so a new send
 * can duplicate it.
 */
class SendOutcomeUnknown extends Data.TaggedError("SendError")<{
  readonly reason: "outcome-unknown";
}> {
  override get message(): string {
    return "send failed: the send did not complete; the message may or may not have been sent";
  }
}

/** A place in an ordered read, as decimal text. A read returns what follows it. */
const Cursor = Schema.String.pipe(Schema.pattern(/^\d{1,15}$/u)).annotations({
  description: "the nextCursor of an earlier result",
});

/** Omit cursor to begin and pass the returned nextCursor unchanged. */
const PaginatedRequest = Schema.Struct({
  cursor: Schema.optional(Cursor),
});

/** Absence of nextCursor marks the end of the listing. */
const Page = Schema.Struct({
  nextCursor: Schema.optional(Cursor),
});

const noArguments = Schema.Record({
  key: Schema.String,
  value: Schema.Never,
}).annotations({
  jsonSchema: { type: "object", properties: {}, additionalProperties: false },
});

/** What `send` takes: the recipient's address, the text and the message it answers. */
const SendRequest = Schema.Struct({
  to: Schema.propertySignature(AgentAddress).annotations({
    description: "The recipient: agent:<name>.",
  }),
  text: Schema.propertySignature(wellFormedString).annotations({
    description: "The message text.",
  }),
  replyToMsgId: Schema.optional(MsgId).annotations({
    description:
      "The ID of the message this replies to. Omit when starting a new norm interaction.",
  }),
}).annotations({ identifier: "SendRequest" });
/** @useDeclaredType */
type SendRequest = typeof SendRequest.Type;

/**
 * What a send returns: the message id and the conversation's canonical
 * address. Accepted work continues independently of the caller's lifetime.
 */
const SendResult = Schema.Struct({
  id: MsgId,
  address: AgentAddress,
}).annotations({ identifier: "SendResult" });

/** A page of one conversation's records. Absence of nextCursor marks its end. */
const historyRecordsPage = Schema.Struct({
  records: Schema.Array(HistoryRecord).pipe(Schema.maxItems(PAGE_SIZE)),
  ...Page.fields,
});

/**
 * The longest a receive_messages call may wait. It stays under the time after
 * which hosts and proxies give up on a tool call.
 */
const MAXIMUM_WAIT_SECONDS = 50;

const sendOutcomeUnknown = new SendOutcomeUnknown({
  reason: "outcome-unknown",
});

/** How a message states one act of a norm, as every reader of the norms needs it. */
const ACT_SYNTAX =
  "A message is one act and nothing else: (act :ontology <norm> :field value ...). Quote text as a JSON string, write a multi-select as a list such as (option1 option3), and send an act without fields as (act :ontology <norm>). The opening act omits replyToMsgId in send_message; every later act supplies the id of the message it answers.";

/**
 * The runtime of the fiber that carries the HTTP request the MCP SDK is
 * answering. The SDK builds the request's server and calls its tools from
 * promises, which this storage follows.
 */
const requestRuntime = new AsyncLocalStorage<Runtime.Runtime<never>>();

/** How often the SDK reads the inbox of an agent whose host holds a push stream or a webhook subscription. */
const INBOX_CHECK_MILLISECONDS = 1000;

/** The longest a webhook subscription lives without a refresh. */
const WEBHOOK_TTL_MILLISECONDS = 30 * 60_000;

/** The name of the event whose occurrences are an agent's inbox entries, in read order. */
const INBOX_EVENT = "org.social-harness/inbox.pending";

/**
 * The MCP handler of a daemon. For an admitted request, whose `authInfo`
 * names its owner in `extra.issuer` and `extra.subject`, it gives the web
 * handler that answers as the owner's agent: the SDK parses the request,
 * negotiates the protocol revision, builds an MCP server with the tools of
 * the agent's role and the inbox event, dispatches the method and frames the
 * response. No MCP server outlives a request, and an aborted request ends a
 * call in flight.
 */
export const makeMcpHandler = Effect.fn("makeMcpHandler")(function* (
  daemon: Daemon,
) {
  const events = yield* makeInboxEvents(daemon);
  const mcp = createMcpHandler(({ authInfo }) =>
    inRequest(
      Effect.map(admittedAgent(daemon, authInfo), (agent) =>
        requestServer(daemon, agent, events),
      ),
    ),
  );
  const answerIn =
    (authInfo: AuthInfo, runtime: Runtime.Runtime<never>) =>
    (request: Request) =>
      requestRuntime.run(runtime, () => mcp.fetch(request, { authInfo }));
  return (authInfo: AuthInfo) =>
    Effect.map(Effect.runtime(), (runtime) => answerIn(authInfo, runtime));
});

/** The agent of the owner an admitted request's token names. */
function admittedAgent(daemon: Daemon, authInfo?: AuthInfo) {
  return getOrCreateOwnedAgent(daemon, {
    issuer: String(authInfo?.extra?.issuer),
    subject: String(authInfo?.extra?.subject),
  }).pipe(Effect.orDie);
}

/** The MCP server for one request, with the inbox event and the tools of `agent`'s role. */
function requestServer(
  daemon: Daemon,
  agent: AgentCard,
  events: EventsExtension,
) {
  const { norms } = daemon;
  const mcp = new McpServer(
    { name: "sharnessd", version: packageVersion },
    { extensions: [events] },
  );
  const isServerAgent = agent.agentId === daemon.serverAgent.agentId;
  register(
    mcp,
    "status",
    {
      description:
        "Read this agent's identifier and name. Other agents address this agent as agent:<agentName>.",
      input: noArguments,
      output: Schema.Struct({ agentId: AgentId, agentName: AgentName }),
      readOnly: true,
    },
    () =>
      Effect.succeed({ agentId: agent.agentId, agentName: agent.agentName }),
  );
  register(
    mcp,
    "receive_messages",
    {
      description:
        "Receive inbox entries in order, each with its conversation address, its message and any guidance. Keep the nextCursor of every result and pass it as cursor on the next call to receive the entries after it. Without cursor the call starts from now: it returns no earlier entries. Pass wait to wait up to that many seconds when no entry follows the cursor; the call returns as soon as one becomes readable, or with no entries when the wait ends.",
      input: Schema.Struct({
        ...PaginatedRequest.fields,
        wait: Schema.optional(
          Schema.Number.pipe(
            Schema.positive(),
            Schema.lessThanOrEqualTo(MAXIMUM_WAIT_SECONDS),
          ).annotations({
            description: `a number of seconds, at most ${String(MAXIMUM_WAIT_SECONDS)}, to wait for an entry when none follows the cursor. Call receive_messages again with nextCursor to keep waiting. Without wait the call returns at once.`,
          }),
        ),
      }),
      output: Schema.Struct({
        entries: Schema.Array(InboxEntry).pipe(Schema.maxItems(PAGE_SIZE)),
        nextCursor: Cursor,
      }),
      readOnly: true,
    },
    (request) => receive(daemon, agent, request),
  );
  if (isServerAgent) {
    registerServerAgentTools(mcp, daemon, agent);
  } else {
    registerClientTools(mcp, daemon, agent);
  }
  if (norms !== undefined) {
    register(
      mcp,
      "read_norms",
      {
        description: `${
          isServerAgent
            ? "List the installed norms and their complete documents: the roles, when each act may be sent and the fields it takes."
            : "List the norms the server agent holds, each with its complete document: the roles, when each act may be sent and the fields it takes. The server agent checks your messages against them."
        } ${ACT_SYNTAX}`,
        input: noArguments,
        output: Schema.Struct({
          norms: Schema.Array(
            Schema.Struct({
              name: Schema.String,
              description: Schema.String,
              document: Schema.String,
            }),
          ),
        }),
        readOnly: true,
      },
      () =>
        Effect.succeed({
          norms: [...norms],
        }),
    );
  }
  return mcp;
}

/** The server agent's tools: it addresses any client and reads its firewall's decisions. */
function registerServerAgentTools(
  mcp: McpServer,
  daemon: Daemon,
  agent: AgentCard,
) {
  registerConversationTools(
    mcp,
    { daemon, agent },
    {
      description:
        "Send a message. Returns id, the message id, and address, the canonical conversation address. A refusal names the argument or rule to correct. An outcome-unknown failure must not be retried as a new send. Use receive_messages to read what others send.",
      input: SendRequest,
      peer: ({ to }) => addressedAgent(daemon, agent, to),
    },
    {
      description:
        "Read one conversation's records: the messages you sent and the inbox entries of the messages you received. A page holds the oldest records after cursor; pass nextCursor as cursor, with the same address, until it is absent.",
      input: Schema.Struct({
        address: AgentAddress,
        ...PaginatedRequest.fields,
      }),
      peer: ({ address }) => addressedAgent(daemon, agent, address),
    },
  );
  register(
    mcp,
    "list_conversations",
    {
      description:
        "List the address of every conversation with a message. Use read_conversation to read one address.",
      input: noArguments,
      output: Schema.Struct({ addresses: Schema.Array(AgentAddress) }),
      readOnly: true,
    },
    () => conversationsOf(daemon, agent),
  );
  register(
    mcp,
    "read_firewall_decisions",
    {
      description:
        "Read completed firewall decision summaries since inclusive Unix milliseconds. Each gives the decision applied (allowed, refused or withheld), the reason and the deciding rules' registered advice, without message content or private model evidence.",
      input: Schema.Struct({ since: Schema.optional(Schema.NonNegativeInt) }),
      output: Schema.Struct({ decisions: Schema.Array(checkRecordSchema) }),
      readOnly: true,
    },
    ({ since }) =>
      Effect.map(readDecisions(daemon, since ?? 0), (decisions) => ({
        decisions,
      })),
  );
}

/** The tools of a client, which name no recipient and no conversation: both are the server agent. */
function registerClientTools(mcp: McpServer, daemon: Daemon, agent: AgentCard) {
  const peer = () => Effect.succeed(daemon.serverAgent);
  registerConversationTools(
    mcp,
    { daemon, agent },
    {
      description:
        "Send a message to the server agent. Returns the message id. An outcome-unknown failure must not be retried as a new send.",
      input: Schema.Struct({
        text: Schema.propertySignature(wellFormedString).annotations({
          description: "The message text.",
        }),
        replyToMsgId: Schema.optional(MsgId),
      }),
      peer,
    },
    {
      description:
        "Read the messages you sent and received in your conversation with the server agent. Follow nextCursor until absent.",
      input: Schema.Struct({ ...PaginatedRequest.fields }),
      peer,
    },
  );
}

/** One of the two conversation tools as a role has it: what it says, what it takes and the peer its input names. */
interface PeerTool<Input, Encoded> {
  readonly description: string;
  readonly input: Schema.Schema<Input, Encoded>;
  readonly peer: (input: Input) => Effect.Effect<AgentCard, AddressError>;
}

/**
 * Registers `send_message` and `read_conversation` for `agent`. The server
 * agent names its peer by address; a client's peer is the server agent.
 */
function registerConversationTools<
  Sent extends Omit<SendRequest, "to">,
  SentEncoded,
  Read extends { readonly cursor?: string },
  ReadEncoded,
>(
  mcp: McpServer,
  context: {
    readonly daemon: Daemon;
    readonly agent: AgentCard;
  },
  sending: PeerTool<Sent, SentEncoded>,
  reading: PeerTool<Read, ReadEncoded>,
) {
  const { daemon, agent } = context;
  register(
    mcp,
    "send_message",
    {
      description: sending.description,
      input: sending.input,
      output: SendResult,
      unexpected: sendOutcomeUnknown,
    },
    (input) =>
      Effect.gen(function* () {
        const recipient = yield* sending.peer(input);
        const id = yield* send(daemon, {
          text: input.text,
          replyToMsgId: input.replyToMsgId,
          author: agent,
          recipient,
        });
        return { id, address: addressOf(recipient.agentName) };
      }),
  );
  register(
    mcp,
    "read_conversation",
    {
      description: reading.description,
      input: reading.input,
      output: historyRecordsPage,
      readOnly: true,
    },
    (input) =>
      Effect.flatMap(reading.peer(input), (other) =>
        history(daemon, agent, other, input.cursor),
      ),
  );
}

/** The other agent `address` names for `self`. */
function addressedAgent(
  daemon: Daemon,
  self: AgentCard,
  address: AgentAddress,
) {
  return Effect.gen(function* () {
    const agent = yield* agentNamed(
      daemon,
      address.slice(AGENT_ADDRESS_PREFIX.length),
    );
    if (agent === undefined) {
      return yield* new AddressError({ reason: "unknown-agent", address });
    }
    if (agent.agentId === self.agentId) {
      return yield* new AddressError({ reason: "invalid-address", address });
    }
    return agent;
  });
}

/**
 * The daemon's Events extension, which every request's server is built with.
 * Its one event type is the inbox of the agent a request is admitted as: an
 * occurrence is an inbox entry, and a subscription's cursor is the read
 * number of the last entry it was sent. The SDK delivers by poll, push and
 * webhook, and keeps every subscription in memory.
 */
function makeInboxEvents(daemon: Daemon) {
  return Effect.gen(function* () {
    const run = Runtime.runPromise(yield* Effect.runtime());
    const events = new EventsExtension({
      getPrincipal: ({ http }) =>
        inRequest(
          Effect.map(
            admittedAgent(daemon, http?.authInfo),
            ({ agentId }) => agentId,
          ),
        ),
      checkIntervalMs: INBOX_CHECK_MILLISECONDS,
      webhookTtlMs: WEBHOOK_TTL_MILLISECONDS,
      webhookSender: nodeWebhookSender(),
    });
    events.registerEvent(INBOX_EVENT, {
      description:
        "An inbox entry: its conversation address, its message and any guidance.",
      payloadSchema: standard(InboxEntry),
      check: (...[{ principal, maxEvents, signal }, , cursor]) =>
        run(inboxOccurrences(daemon, principal, { cursor, maxEvents }), {
          signal,
        }),
    });
    return events;
  }).pipe(Effect.withSpan("makeInboxEvents"));
}

/**
 * One check of the inbox event: at most `maxEvents` of the entries after
 * `cursor` in the inbox of the agent `agentId` names, as occurrences.
 */
function inboxOccurrences(
  daemon: Daemon,
  agentId: string | null,
  after: { readonly cursor: string | null; readonly maxEvents?: number },
) {
  return Effect.gen(function* () {
    const page = yield* readInbox(daemon, yield* Effect.fromNullable(agentId), {
      limit: Math.min(after.maxEvents ?? PAGE_SIZE, PAGE_SIZE),
      ...(after.cursor === null
        ? {}
        : { cursor: yield* Schema.decode(Cursor)(after.cursor) }),
    });
    return {
      events: (yield* Schema.encode(Schema.Array(InboxEntry))(
        page.entries,
      )).map((data) => ({
        eventId: `${data.message.author.agentId}/${data.message.msgId}`,
        data,
      })),
      cursor: page.nextCursor,
      hasMore: page.hasMore,
    };
  });
}

/**
 * Registers a tool whose calls `execute` answers. A call runs in the runtime
 * of the HTTP request's fiber and is interrupted when the request is aborted.
 * Its value is the result's structured content. A typed failure is a result
 * marked as an error. A call that ends any other way is logged and answered
 * as `unexpected`, or with a fixed text that says nothing of what went wrong.
 */
function register<A, I, B extends object, J extends object, E extends Failure>(
  mcp: McpServer,
  name: string,
  tool: {
    readonly description: string;
    readonly input: Schema.Schema<A, I>;
    readonly output: Schema.Schema<B, J>;
    readonly readOnly?: true;
    readonly unexpected?: Failure;
  },
  execute: (input: A) => Effect.Effect<B, E>,
) {
  const outputSchema: StandardSchemaWithJSON<object, object> = standard(
    tool.output,
  );
  mcp.registerTool(
    name,
    {
      description: tool.description,
      inputSchema: standard(tool.input),
      outputSchema,
      annotations: tool.readOnly && {
        readOnlyHint: true,
        openWorldHint: false,
      },
    },
    (input, ctx) =>
      inRequest(
        execute(input).pipe(
          Effect.flatMap((value) =>
            Effect.orDie(Schema.encode(tool.output)(value)),
          ),
          Effect.map((structuredContent) => ({ structuredContent })),
          Effect.catchAll((failure) => Effect.succeed(failed(failure))),
          Effect.catchAllDefect((defect) =>
            Effect.logError("A tool call ended unexpectedly", defect).pipe(
              Effect.as(
                tool.unexpected === undefined
                  ? {
                      isError: true as const,
                      content: [
                        {
                          type: "text" as const,
                          text: "The daemon could not complete this call. Try again; if it keeps failing, check the daemon's log.",
                        },
                      ],
                    }
                  : failed(tool.unexpected),
              ),
            ),
          ),
        ),
        ctx.mcpReq.signal,
      ),
  );
}

/** A typed failure of a tool: what a model reads of it, and its own fields, which a host reads. */
interface Failure {
  readonly message: string;
  readonly reason: string;
}

/** The result of a call that failed with `failure`: its message as the text, and its fields without its tag. */
function failed(failure: Failure) {
  return {
    isError: true as const,
    content: [{ type: "text" as const, text: failure.message }],
    structuredContent: Object.fromEntries(
      Object.entries(failure).filter(([field]) => field !== "_tag"),
    ),
  };
}

/**
 * `schema` as the Standard Schema the MCP SDK validates a value with and
 * lists as JSON Schema. Effect's own Standard Schema carries no JSON Schema,
 * which the SDK requires. A value with a field the schema does not name is
 * invalid.
 */
function standard<A, I>(
  schema: Schema.Schema<A, I>,
): StandardSchemaWithJSON<I, A> {
  const definitions: Record<string, JSONSchema.JsonSchema7> = {};
  const root = JSONSchema.fromAST(schema.ast, {
    definitions,
    target: "jsonSchema2020-12",
    topLevelReferenceStrategy: "skip",
  });
  const json = () => ({
    ...root,
    ...(Object.keys(definitions).length === 0 ? {} : { $defs: definitions }),
  });
  return {
    "~standard": {
      ...Schema.standardSchemaV1(schema, { onExcessProperty: "error" })[
        "~standard"
      ],
      jsonSchema: { input: json, output: json },
    },
  };
}

/**
 * Runs `work` for the MCP SDK in the runtime of the HTTP request's fiber, so
 * its spans keep their parent, and interrupts it when `signal` aborts.
 */
function inRequest<A>(work: Effect.Effect<A>, signal?: AbortSignal) {
  return Effect.runPromise(
    Effect.fromNullable(requestRuntime.getStore()).pipe(
      Effect.flatMap((runtime) => Effect.provide(work, runtime)),
      Effect.orDie,
    ),
    { signal },
  );
}
