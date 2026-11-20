/**
 * @file The agents of one daemon. Each owner has one agent, saved as one row
 * with its card. An agent sends, reads its inbox and reads its history
 * through plain functions over the stored messages; the server agent's
 * firewall checks every message of its conversations.
 */

import { SqlClient } from "@effect/sql";
import { Data, Duration, Effect, PubSub, Queue, Schema } from "effect";
import { randomBytes, randomUUID } from "node:crypto";
import {
  type CommunicationError,
  Message,
  PAGE_SIZE,
  type PaginatedRequest,
} from "../communication/index.js";
import {
  type HeldMessage,
  listConversations,
  readHistory,
  readInbox,
  readLatest,
  type Received,
  sendMessage,
} from "../communication/service.js";
import { transaction } from "../database.js";
import {
  compareAscii,
  decodeJson,
  encodeJson,
  MAXIMUM_RUNTIME_ITEM_BYTES,
} from "../encoding.js";
import { type FirewallRefusal, LocalGuidance } from "../firewall/index.js";
import { Firewall, type IncomingDecision } from "../firewall/service.js";
import { AgentCard, type AgentName } from "../identity.js";
import {
  addressedAgent,
  addressOf,
  type AgentAddress,
  type Conversations,
  HistoryError,
  type HistoryRecord,
  type HistoryRecordsPage,
  type InboxEntry,
  type InboxPage,
  SendError,
  type SendRequest,
  type SendResult,
} from "./values.js";

/** Authentication supplies both fields; email and host-provided agent selectors are excluded. */
export interface Owner {
  readonly issuer: string;
  readonly subject: string;
}

/**
 * The owner's agent has another name than the one asked for, or the name
 * belongs to another owner's agent. Carries no account claims.
 * Exported so dependent declarations can name the inferred Effect failure.
 * @public
 */
export class OwnershipError extends Data.TaggedError("OwnershipError") {}

/** One agent of the daemon: the key its rows are stored under and its card. */
export interface HostedAgent {
  readonly agentKey: string;
  readonly card: AgentCard;
}

const hostedAgentRows = Schema.Array(
  Schema.Struct({ agentKey: Schema.String, ...AgentCard.fields }),
);

/** The agents whose row holds `value` in `column`. */
const agentsWith = Effect.fn("agentsWith")(function* (
  where: Readonly<Record<string, string>>,
) {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql`SELECT agent_key AS "agentKey", agent_id AS "agentId",
      agent_name AS "agentName", principal_id AS "principalId"
    FROM agent_cards WHERE ${sql.and(
      Object.entries(where).map(
        ([column, value]) => sql`${sql(column)} = ${value}`,
      ),
    )}`;
  return (yield* Schema.decodeUnknown(hostedAgentRows)(rows)).map(
    ({ agentKey, ...card }): HostedAgent => ({ agentKey, card }),
  );
});

const minted = (prefix: string) =>
  `${prefix}${randomBytes(16).toString("base64url")}`;

/**
 * The owner's agent, created on first use with a new identifier and
 * principal. Concurrent first calls for one owner return the same agent. With
 * `agentName`, the agent is created under that name and an existing agent
 * must already have it.
 */
export const getOrCreateOwnedAgent = Effect.fn("getOrCreateOwnedAgent")(
  function* (owner: Owner, agentName?: AgentName) {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO agent_cards ${sql.insert({
      agent_key: randomUUID(),
      issuer: owner.issuer,
      subject: owner.subject,
      agent_id: minted("agt_"),
      agent_name: agentName ?? `agent-${randomBytes(12).toString("hex")}`,
      principal_id: minted("prn_"),
    })} ON CONFLICT DO NOTHING`;
    const [saved] = yield* agentsWith({ ...owner });
    if (
      saved === undefined ||
      (agentName !== undefined && saved.card.agentName !== agentName)
    ) {
      return yield* new OwnershipError();
    }
    return saved;
  },
);

/** Keep address unchanged when supplying the next cursor. */
interface ReadHistoryRequest extends PaginatedRequest {
  readonly address: AgentAddress;
}
/** One inbox read: the cursor to read after and, in seconds, how long to wait for an entry when none follows it. */
interface ReceiveRequest {
  readonly cursor?: string;
  readonly wait?: number;
}
/** Sending, inbox and history operations for one agent. */
export interface SocialHarness {
  /**
   * Has the server agent's firewall check the message and stores it. The
   * result carries its message id and canonical conversation; a message the
   * firewall withholds from the server agent is stored and returns the same.
   * Each call creates a new message. Retrying an uncertain send can duplicate
   * it.
   */
  readonly send: (
    request: SendRequest,
  ) => Effect.Effect<SendResult, SendError | FirewallRefusal>;
  /**
   * A page of the inbox entries after `cursor`, in the order they were
   * allowed, with the cursor that reads on from the last of them. Without a
   * cursor the read starts from now. With `wait` and no entry to return, it
   * reads once more when an entry becomes readable or `wait` seconds pass.
   */
  readonly receive: (request: ReceiveRequest) => Effect.Effect<InboxPage>;
  /** The address of every conversation of the agent, in order. */
  readonly listConversations: () => Effect.Effect<Conversations, HistoryError>;
  /**
   * A page of a conversation's records, oldest first, after the cursor an
   * earlier page returned. Rule edits leave stored inbox entries as they are.
   */
  readonly readHistory: (
    request: ReadHistoryRequest,
  ) => Effect.Effect<HistoryRecordsPage, HistoryError>;
}

/** What the daemon's agents are served from. */
export interface HarnessSettings {
  /** The agent whose firewall checks every message. Every other agent is its client. */
  readonly serverAgent: HostedAgent;
  readonly maximumHistoryRecords?: number;
  /**
   * What to tell the sender of a message the firewall withheld for breaking a
   * rule, when no rule supplied a reply of its own. Without it such a sender
   * is told nothing.
   */
  readonly withheldReply?: string;
}

/** What every agent's operations share on one daemon. */
interface Daemon {
  readonly sql: SqlClient.SqlClient;
  readonly settings: HarnessSettings;
  /** The server agent's firewall. */
  readonly firewall: typeof Firewall.Service;
  readonly historyLimit: number;
  /** The turn of each conversation a message was sent in, by its client's agent id. */
  readonly turns: Map<string, Effect.Semaphore>;
  /** Published with an agent's key each time a message becomes readable for it. */
  readonly readable: PubSub.PubSub<string>;
}

/**
 * Gives each agent of the daemon its operations, over the server agent's
 * firewall and the database of the caller.
 */
export const makeHarness = Effect.fn("makeHarness")(function* (
  settings: HarnessSettings,
) {
  const daemon: Daemon = {
    sql: yield* SqlClient.SqlClient,
    settings,
    firewall: yield* Firewall,
    historyLimit: settings.maximumHistoryRecords ?? 256,
    turns: new Map(),
    readable: yield* PubSub.unbounded<string>(),
  };
  const stored = <A, E>(work: Effect.Effect<A, E, SqlClient.SqlClient>) =>
    Effect.provideService(work, SqlClient.SqlClient, daemon.sql);
  return (self: HostedAgent): SocialHarness => ({
    send: (request) => stored(send(daemon, self, request)),
    receive: (request) => stored(receive(daemon, self, request)),
    listConversations: () => stored(conversationsOf(self)),
    readHistory: (request) => stored(historyRecords(self, request)),
  });
});

/**
 * At most `limit` of the inbox entries of the agent stored under `agentKey`
 * after `cursor`, in read order, with the cursor to read on from and whether
 * more entries follow. Without a cursor the read starts from the newest entry
 * and returns none.
 */
export const readInboxEntries = Effect.fn("readInboxEntries")(function* (
  agentKey: string,
  read: Parameters<typeof readInbox>[1],
) {
  const { messages, nextCursor, hasMore } = yield* readInbox(agentKey, read);
  const entries = yield* Effect.forEach(messages, entryOf, { concurrency: 1 });
  return { entries, nextCursor, hasMore };
});

/** The inbox entry of a message its recipient may read, with the guidance stored for it. */
function entryOf(held: HeldMessage) {
  const { message } = held;
  const address = addressOf([message.author], message.recipients[0].agentId);
  return held.guidance === undefined
    ? Effect.succeed<InboxEntry>({ address, message })
    : Effect.map(
        decodeJson(LocalGuidance, held.guidance),
        (guidance): InboxEntry => ({ address, message, guidance }),
      );
}

/**
 * One read of the inbox. A waiting read subscribes before it reads, so an
 * entry that becomes readable after the read is not missed.
 */
function receive(
  daemon: Daemon,
  { agentKey }: HostedAgent,
  { cursor, wait }: ReceiveRequest,
) {
  const read = (from: typeof cursor) =>
    readInboxEntries(agentKey, { limit: PAGE_SIZE, cursor: from }).pipe(
      Effect.map(({ entries, nextCursor }) => ({ entries, nextCursor })),
      Effect.orDie,
    );
  if (wait === undefined) {
    return read(cursor);
  }
  return Effect.gen(function* () {
    const readable = yield* PubSub.subscribe(daemon.readable);
    const page = yield* read(cursor);
    if (page.entries.length > 0) {
      return page;
    }
    yield* Queue.take(readable).pipe(
      Effect.repeat({ until: (key) => key === agentKey }),
      Effect.timeoutOption(Duration.seconds(wait)),
    );
    return yield* read(page.nextCursor);
  }).pipe(Effect.scoped);
}

/**
 * Holds the conversation of the server agent with `client` until the scope
 * closes. A check reads the conversation's earlier messages, so the messages
 * of one conversation are checked and stored one at a time. Other
 * conversations do not wait.
 */
function takeTurn(daemon: Daemon, client: HostedAgent) {
  return Effect.sync(() => {
    const turn =
      daemon.turns.get(client.card.agentId) ?? Effect.unsafeMakeSemaphore(1);
    daemon.turns.set(client.card.agentId, turn);
    return turn;
  }).pipe(
    Effect.flatMap((turn) =>
      Effect.acquireReleaseInterruptible(turn.take(1), () => turn.release(1)),
    ),
    Effect.asVoid,
  );
}

/**
 * Sends one message in its conversation's turn: the server agent's firewall
 * checks it as a message its agent sends or is sent, and one transaction
 * stores it with what was decided. No transaction is open while a check runs.
 * A waiting read wakes once a message it may read is stored.
 */
function send(daemon: Daemon, self: HostedAgent, input: SendRequest) {
  return Effect.suspend(() => {
    const request = structuredClone(input);
    return Effect.gen(function* () {
      const name = yield* addressedAgent(request.to, self.card.agentName);
      const [recipient] = yield* agentsWith({ agent_name: name });
      if (recipient === undefined) {
        return yield* new SendError({
          reason: "unknown-agent",
          detail: `This daemon has no agent named agent:${name}. Check the name with its owner.`,
        });
      }
      const outgoing = yield* Message.create({
        author: self.card,
        recipients: [recipient.card],
        content: [{ type: "text", text: request.text }],
        replyToMsgId: request.replyToMsgId,
      });
      const route = {
        authorKey: self.agentKey,
        recipientKey: recipient.agentKey,
      };
      if (self.agentKey === daemon.settings.serverAgent.agentKey) {
        yield* takeTurn(daemon, recipient);
        yield* sendAsServerAgent(daemon, route, outgoing);
      } else {
        yield* takeTurn(daemon, self);
        yield* sendAsClient(daemon, route, outgoing);
      }
      return { id: outgoing.msgId, address: request.to };
    }).pipe(
      Effect.scoped,
      Effect.catchAll(sendFailure),
      Effect.tapError((error) =>
        Effect.logWarning("Send failed").pipe(
          Effect.annotateLogs({
            agentKey: self.agentKey,
            error: error._tag,
            reason: error.message,
          }),
        ),
      ),
    );
  });
}

type Route = Parameters<typeof sendMessage>[0];

/** Stores a message the server agent's firewall allows it to send, readable for its client. */
function sendAsServerAgent(daemon: Daemon, route: Route, outgoing: Message) {
  return Effect.gen(function* () {
    const decision = yield* daemon.firewall.checkOutgoing(
      outgoing,
      (protocolHistory) => evidenceFor(daemon, outgoing, protocolHistory),
    );
    yield* decision.save;
    if (decision.kind === "refused") {
      return yield* decision.refusal;
    }
    yield* sendMessage(route, outgoing, { readable: true });
    yield* PubSub.publish(daemon.readable, route.recipientKey);
  });
}

/**
 * Stores a message a client sends with what the server agent's firewall
 * decides of it, and sends the reply to a withheld one, in one transaction.
 * The evidence is read at most once and serves both the check and the check
 * of the reply. A check that could not run fails the send.
 */
function sendAsClient(daemon: Daemon, route: Route, incoming: Message) {
  return Effect.gen(function* () {
    const evidence = yield* Effect.cachedFunction((protocolHistory: boolean) =>
      evidenceFor(daemon, incoming, protocolHistory),
    );
    const decision = yield* daemon.firewall.checkIncoming(incoming, evidence);
    if (
      decision.kind === "withheld" &&
      decision.reason === "check-unavailable"
    ) {
      return yield* new SendError({
        reason: "network-unavailable",
        detail:
          "the recipient's message check could not run; send the message again later",
      });
    }
    const reply = yield* checkedReply(daemon, incoming, decision, evidence);
    const received: Received = {
      readable: decision.kind === "allowed",
      ...(decision.kind === "allowed" && decision.guidance !== undefined
        ? { guidance: yield* encodeJson(LocalGuidance, decision.guidance) }
        : {}),
    };
    yield* transaction(
      Effect.gen(function* () {
        yield* sendMessage(route, incoming, received);
        yield* decision.save;
        yield* reply.save;
        if (reply.message !== undefined) {
          yield* sendMessage(
            { authorKey: route.recipientKey, recipientKey: route.authorKey },
            reply.message,
            { readable: true },
          );
        }
      }),
    );
    if (received.readable) {
      yield* PubSub.publish(daemon.readable, route.recipientKey);
    } else if (reply.message !== undefined) {
      yield* PubSub.publish(daemon.readable, route.authorKey);
    }
  });
}

/**
 * The reply to a withheld message once the firewall has checked it as a
 * message its agent sends, with the withheld message as one the reply may
 * name: the reply when it is allowed, and the saving of that check either way.
 */
function checkedReply(
  daemon: Daemon,
  withheld: Message,
  decision: IncomingDecision,
  evidence: (protocolHistory: boolean) => ReturnType<typeof evidenceFor>,
) {
  return Effect.gen(function* () {
    const proposed = yield* replyTo(daemon, withheld, decision);
    if (proposed === undefined) {
      return { save: Effect.void };
    }
    const checked = yield* daemon.firewall.checkOutgoing(
      proposed,
      (protocolHistory) =>
        Effect.map(evidence(protocolHistory), (read) => ({
          ...read,
          references: [withheld],
        })),
    );
    return {
      save: checked.save,
      ...(checked.kind === "allowed" ? { message: proposed } : {}),
    };
  });
}

/**
 * The reply proposed for a message the firewall withheld: an ordinary message
 * from the server agent to the withheld message's author that names it as the
 * one it answers. A rule's own reply is used when it gave one; otherwise
 * `withheldReply` answers a message that broke a rule. The reply is checked
 * as any message the server agent sends, and is sent only if allowed. A reply
 * that is not a valid message is not sent.
 */
function replyTo(
  daemon: Daemon,
  withheld: Message,
  decision: IncomingDecision,
) {
  if (decision.kind !== "withheld") {
    return Effect.succeed(undefined);
  }
  const text =
    decision.reply ??
    (decision.reason === "rule-violation"
      ? daemon.settings.withheldReply
      : undefined);
  if (text === undefined) {
    return Effect.succeed(undefined);
  }
  return Message.create({
    author: daemon.settings.serverAgent.card,
    recipients: [withheld.author],
    content: [{ type: "text", text }],
    replyToMsgId: withheld.msgId,
  }).pipe(Effect.orElseSucceed(() => undefined));
}

/**
 * The evidence the check of `next`, the next message of its conversation,
 * reads from the server agent's history: the bounded window of the latest
 * messages, and with `withProtocolHistory` every message of the conversation.
 */
function evidenceFor(
  daemon: Daemon,
  next: Message,
  withProtocolHistory: boolean,
) {
  return Effect.gen(function* () {
    const { agentKey } = daemon.settings.serverAgent;
    const members = [next.author, ...next.recipients];
    const all = withProtocolHistory
      ? (yield* readLatest(agentKey, { members })).entries
      : undefined;
    const page =
      all === undefined
        ? yield* readLatest(agentKey, { members, limit: daemon.historyLimit })
        : {
            entries: all.slice(-daemon.historyLimit),
            earlierMessagesOmitted: all.length > daemon.historyLimit,
          };
    let bytes = 0;
    const recent: HeldMessage[] = [];
    for (const earlier of [...page.entries].reverse()) {
      if (bytes + earlier.messageBytes > MAXIMUM_RUNTIME_ITEM_BYTES) {
        break;
      }
      recent.push(earlier);
      bytes += earlier.messageBytes;
    }
    recent.reverse();
    return {
      history: recent.map((earlier) => ({
        message: earlier.message,
        visibleToAgent: earlier.readable,
      })),
      earlierHistoryOmitted:
        page.earlierMessagesOmitted || recent.length < page.entries.length,
      gaps:
        recent.length < page.entries.length
          ? ["The preceding history exceeds the evidence byte limit."]
          : [],
      ...(all === undefined
        ? {}
        : {
            protocolHistory: {
              messages: all.map(({ message }) => message),
              complete: true,
            },
          }),
    };
  });
}

/** The address of every conversation of `self`, in order. */
function conversationsOf(self: HostedAgent) {
  return listConversations(self.agentKey).pipe(
    Effect.map((conversations) => ({
      addresses: conversations
        .map((members) => addressOf(members, self.card.agentId))
        .sort(compareAscii),
    })),
    Effect.catchAll(historyFailure),
  );
}

/**
 * One page of a conversation's messages as history records: each one `self`
 * sent, and each one it received and may read.
 */
function historyRecords(self: HostedAgent, request: ReadHistoryRequest) {
  return Effect.gen(function* () {
    const name = yield* addressedAgent(request.address, self.card.agentName);
    const [other] = yield* agentsWith({ agent_name: name });
    if (other === undefined) {
      return yield* new HistoryError({ reason: "unknown-agent" });
    }
    const page = yield* readHistory(self.agentKey, {
      members: [self.card, other.card],
      ...(request.cursor === undefined ? {} : { cursor: request.cursor }),
    });
    const records: HistoryRecord[] = [];
    for (const held of page.entries) {
      if (held.message.author.agentId === self.card.agentId) {
        records.push({ kind: "sent", message: held.message });
      } else if (held.readable) {
        records.push({ kind: "received", ...(yield* entryOf(held)) });
      }
    }
    return {
      records,
      ...(page.nextCursor === undefined ? {} : { nextCursor: page.nextCursor }),
    };
  }).pipe(Effect.catchAll(historyFailure));
}

/**
 * Fails with what the sender can correct. Any other failure is one of storage
 * and ends the send as a defect, which the tool reports as an unknown outcome.
 */
function sendFailure(
  error:
    | SendError
    | FirewallRefusal
    | CommunicationError
    | {
        readonly _tag:
          | "SqlError"
          | "ParseError"
          | "JsonRepresentationError"
          | "FirewallPolicyError";
      },
) {
  if (error._tag === "SendError" || error._tag === "FirewallRefusal") {
    return Effect.fail(error);
  }
  return error._tag === "CommunicationError"
    ? Effect.fail(new SendError({ reason: error.reason }))
    : Effect.die(error);
}

/**
 * Fails with what the reader can correct: a wrong address or an unknown
 * agent. Any other failure is one of storage and ends the read as a defect.
 */
function historyFailure(
  error:
    | HistoryError
    | SendError
    | {
        readonly _tag: "SqlError" | "ParseError" | "JsonRepresentationError";
      },
) {
  if (error._tag === "HistoryError") {
    return Effect.fail(error);
  }
  return error._tag === "SendError"
    ? Effect.fail(new HistoryError({ reason: "invalid-address" }))
    : Effect.die(error);
}
