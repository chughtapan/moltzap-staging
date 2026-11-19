/** @file Coordinates identity, message policy and communication: an agent's sends, the check of what it is sent, its inbox and its history. */

import { SqlClient } from "@effect/sql";
import { Context, Duration, Effect, Layer, type Scope } from "effect";
import {
  type CommunicationError,
  Message,
  PAGE_SIZE,
  type PaginatedRequest,
} from "../communication/index.js";
import {
  Communication,
  type HistoryPage,
  readInbox,
  type Received,
} from "../communication/service.js";
import { transaction } from "../database.js";
import {
  compareAscii,
  decodeJson,
  encodeJson,
  MAXIMUM_RUNTIME_ITEM_BYTES,
} from "../encoding.js";
import {
  type FirewallPolicyError,
  type FirewallRefusal,
  LocalGuidance,
} from "../firewall/index.js";
import { Firewall, type IncomingDecision } from "../firewall/service.js";
import { type AgentCard, Identity, type IdentityError } from "../identity.js";
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

/** Keep address unchanged when supplying the next cursor. */
interface ReadHistoryRequest extends PaginatedRequest {
  readonly address: AgentAddress;
}
/** One inbox read: the cursor to read after and, in seconds, how long to wait for an entry when none follows it. */
interface ReceiveRequest {
  readonly cursor?: string;
  readonly wait?: number;
}
/**
 * What an agent's firewall decided of a message a peer sends it, before the
 * message is stored: how the agent holds it, and `save`, which saves the
 * decision and sends the reply to a withheld message. `save` runs after the
 * message is stored, in the same transaction.
 */
interface Arrival {
  readonly received: Received;
  readonly save: Effect.Effect<void, FirewallPolicyError | CommunicationError>;
}
/** Sending, inbox and history operations for one agent. */
interface SocialHarnessService {
  /**
   * Checks policy, the recipient's included, and stores the message. The
   * result carries its message id and canonical conversation; a message the
   * recipient's firewall withholds is stored and returns the same. Each call
   * creates a new message. Retrying an uncertain send can duplicate it.
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
  /**
   * Decides a message a peer sends this agent, holding their conversation's
   * turn until the scope closes. Fails, with nothing saved, when the check
   * could not run.
   */
  readonly checkIncoming: (
    message: Message,
  ) => Effect.Effect<Arrival, SendError | FirewallRefusal, Scope.Scope>;
}

/**
 * Hosts use this service for identity, messaging and local policy. A stored
 * inbox entry survives policy edits and restart.
 */
export class SocialHarness extends Context.Tag("SocialHarness/Harness")<
  SocialHarness,
  SocialHarnessService
>() {
  /** One agent's harness over its identity, firewall and communication. */
  static layer(settings: HarnessSettings) {
    return Layer.effect(SocialHarness, makeHarness(settings));
  }
}

/** Supplies one agent's key, its limits and who checks what it sends. */
export interface HarnessSettings {
  readonly agentKey: string;
  readonly maximumHistoryRecords?: number;
  /**
   * What to tell the sender of a message the firewall withheld for breaking a
   * rule, when no rule supplied a reply of its own. Without it such a sender
   * is told nothing.
   */
  readonly withheldReply?: string;
  /**
   * For a client, the harness of the server agent, whose firewall checks what
   * the client sends. The server agent has none: a client checks nothing.
   */
  readonly checkedBy?: Effect.Effect<SocialHarnessService>;
}

/** What one agent's operations share: its card, the services it acts through and its settings. */
interface Agent {
  readonly card: AgentCard;
  readonly identity: Context.Tag.Service<typeof Identity>;
  readonly firewall: Context.Tag.Service<typeof Firewall>;
  readonly communication: Context.Tag.Service<typeof Communication>;
  readonly sql: SqlClient.SqlClient;
  readonly settings: HarnessSettings;
  readonly historyLimit: number;
  /** The turn of each conversation a message was sent in, by the other agent's id. */
  readonly turns: Map<string, Effect.Semaphore>;
}

function makeHarness(settings: HarnessSettings) {
  return Effect.gen(function* () {
    const identity = yield* Identity;
    const agent: Agent = {
      card: identity.me,
      identity,
      firewall: yield* Firewall,
      communication: yield* Communication,
      sql: yield* SqlClient.SqlClient,
      settings,
      historyLimit: settings.maximumHistoryRecords ?? 256,
      turns: new Map(),
    };
    return SocialHarness.of({
      send: (request) => send(agent, request),
      receive: (request) => receive(agent, request),
      listConversations: () => listConversations(agent),
      readHistory: (request) => readHistory(agent, request),
      checkIncoming: (message) =>
        takeTurn(agent, message.author.agentId).pipe(
          Effect.zipRight(checkIncoming(agent, message)),
          Effect.catchAll(sendFailure),
        ),
    });
  }).pipe(Effect.withSpan("SocialHarness.layer"));
}

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
function entryOf(stored: {
  readonly message: Message;
  readonly guidance?: string;
}) {
  const { message } = stored;
  const address = addressOf([message.author], message.recipients[0].agentId);
  return stored.guidance === undefined
    ? Effect.succeed<InboxEntry>({ address, message })
    : Effect.map(
        decodeJson(LocalGuidance, stored.guidance),
        (guidance): InboxEntry => ({ address, message, guidance }),
      );
}

/**
 * One read of the inbox. A waiting read subscribes before it reads, so an
 * entry that becomes readable after the read is not missed.
 */
function receive(agent: Agent, { cursor, wait }: ReceiveRequest) {
  const read = (from: typeof cursor) =>
    readInboxEntries(agent.settings.agentKey, {
      limit: PAGE_SIZE,
      cursor: from,
    }).pipe(
      Effect.map(({ entries, nextCursor }) => ({ entries, nextCursor })),
      Effect.provideService(SqlClient.SqlClient, agent.sql),
      Effect.orDie,
    );
  if (wait === undefined) {
    return read(cursor);
  }
  return Effect.gen(function* () {
    const readable = yield* agent.communication.readable;
    const page = yield* read(cursor);
    if (page.entries.length > 0) {
      return page;
    }
    yield* readable.pipe(Effect.timeoutOption(Duration.seconds(wait)));
    return yield* read(page.nextCursor);
  }).pipe(Effect.scoped);
}

/**
 * Holds this agent's conversation with `peer` until the scope closes. A check
 * reads the conversation's earlier messages, so the messages of one
 * conversation are checked and stored one at a time. Other conversations do
 * not wait.
 */
function takeTurn(agent: Agent, peer: string) {
  return Effect.sync(() => {
    const turn = agent.turns.get(peer) ?? Effect.unsafeMakeSemaphore(1);
    agent.turns.set(peer, turn);
    return turn;
  }).pipe(
    Effect.flatMap((turn) =>
      Effect.acquireReleaseInterruptible(turn.take(1), () => turn.release(1)),
    ),
    Effect.asVoid,
  );
}

/**
 * Sends one message in its conversation's turn, which the server agent
 * holds: this agent's firewall checks it, then the server agent's when a
 * client sends, and one transaction stores it with what was decided. No
 * transaction is open while a check runs.
 */
function send(agent: Agent, input: SendRequest) {
  return Effect.suspend(() => {
    const request = structuredClone(input);
    return Effect.gen(function* () {
      const outgoing = yield* outgoingFor(
        agent,
        request,
        yield* addressedAgent(request.to, agent.card.agentName),
      );
      const { checkedBy } = agent.settings;
      if (checkedBy === undefined) {
        yield* takeTurn(agent, outgoing.recipients[0].agentId);
      }
      const decision = yield* agent.firewall.checkOutgoing(
        outgoing,
        (protocolHistory) =>
          evidenceFor(
            agent,
            [outgoing.author, ...outgoing.recipients],
            protocolHistory,
          ),
      );
      yield* decision.save;
      if (decision.kind === "refused") {
        return yield* decision.refusal;
      }
      const arrival =
        checkedBy === undefined
          ? { received: { readable: true }, save: Effect.void }
          : yield* Effect.flatMap(checkedBy, (recipient) =>
              recipient.checkIncoming(outgoing),
            );
      yield* transaction(
        agent.communication
          .send(outgoing, arrival.received)
          .pipe(Effect.zipRight(arrival.save)),
      );
      return { id: outgoing.msgId, address: request.to };
    }).pipe(
      Effect.scoped,
      Effect.provideService(SqlClient.SqlClient, agent.sql),
      Effect.catchAll(sendFailure),
      Effect.tapError((error) =>
        Effect.logWarning("Send failed").pipe(
          Effect.annotateLogs({
            agentKey: agent.settings.agentKey,
            error: error._tag,
            reason: error.message,
          }),
        ),
      ),
    );
  });
}

/** The message a send request amounts to, addressed to the agent `name`. */
function outgoingFor(
  agent: Agent,
  request: SendRequest,
  name: AgentCard["agentName"],
) {
  return Effect.gen(function* () {
    const [recipient] = yield* agent.identity.resolve([name]).pipe(
      Effect.catchTag("IdentityError", (error) =>
        error.reason === "unknown-agent"
          ? Effect.fail(
              new SendError({
                reason: "unknown-agent",
                ...(error.agentName === undefined
                  ? {}
                  : {
                      detail: `This daemon has no agent named agent:${error.agentName}. Check the name with its owner.`,
                    }),
              }),
            )
          : Effect.die(error),
      ),
    );
    if (recipient === undefined) {
      return yield* new SendError({ reason: "unknown-agent" });
    }
    return yield* Message.create({
      author: agent.card,
      recipients: [recipient],
      content: [{ type: "text", text: request.text }],
      replyToMsgId: request.replyToMsgId,
    });
  });
}

function listConversations(agent: Agent) {
  return agent.communication.listConversations.pipe(
    Effect.map((conversations) => ({
      addresses: conversations
        .map((members) => addressOf(members, agent.card.agentId))
        .sort(compareAscii),
    })),
    Effect.catchAll(historyFailure),
  );
}

function readHistory(agent: Agent, request: ReadHistoryRequest) {
  const { card } = agent;
  return Effect.gen(function* () {
    const other = yield* addressedAgent(request.address, card.agentName);
    return yield* messageRecords(
      agent,
      [card, ...(yield* agent.identity.resolve([other]))],
      request.cursor,
    );
  }).pipe(Effect.catchAll(historyFailure));
}

/**
 * One page of a conversation's messages as history records: each one this
 * agent sent, and each one it received and may read. The cursor is the one
 * communication returned for this conversation.
 */
function messageRecords(
  agent: Agent,
  members: readonly AgentCard[],
  cursor?: string,
) {
  return Effect.gen(function* () {
    const page = yield* agent.communication.readHistory({
      members,
      ...(cursor === undefined ? {} : { cursor }),
    });
    const records: HistoryRecord[] = [];
    for (const entry of page.entries) {
      if (entry.message.author.agentId === agent.card.agentId) {
        records.push({ kind: "sent", message: entry.message });
      } else if (entry.readable) {
        records.push({ kind: "received", ...(yield* entryOf(entry)) });
      }
    }
    return {
      records,
      ...(page.nextCursor === undefined ? {} : { nextCursor: page.nextCursor }),
    };
  });
}

/**
 * Decides a message a peer sends this agent, from the conversation as it
 * stands, and proposes the reply to a withheld one. The evidence is read at
 * most once and serves both the check and the check of the reply. A check
 * that could not run fails the send.
 */
function checkIncoming(agent: Agent, message: Message) {
  return Effect.gen(function* () {
    const evidence = yield* Effect.cachedFunction((protocolHistory: boolean) =>
      evidenceFor(
        agent,
        [message.author, ...message.recipients],
        protocolHistory,
      ),
    );
    const decision = yield* agent.firewall.checkIncoming(message, evidence);
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
    const reply = yield* checkedReply(agent, message, decision, evidence);
    const guidance =
      decision.kind === "allowed" && decision.guidance !== undefined
        ? yield* encodeJson(LocalGuidance, decision.guidance)
        : undefined;
    const arrival: Arrival = {
      received: {
        readable: decision.kind === "allowed",
        ...(guidance === undefined ? {} : { guidance }),
      },
      save: decision.save.pipe(
        Effect.zipRight(reply.save),
        Effect.zipRight(
          reply.message === undefined
            ? Effect.void
            : agent.communication.send(reply.message, { readable: true }),
        ),
      ),
    };
    return arrival;
  });
}

/**
 * The reply to a withheld message once this agent's own firewall has checked
 * it as a message it sends, with the withheld message as one the reply may
 * name: the reply when it is allowed, and the saving of that check either way.
 */
function checkedReply(
  agent: Agent,
  withheld: Message,
  decision: IncomingDecision,
  evidence: (protocolHistory: boolean) => ReturnType<typeof evidenceFor>,
) {
  return Effect.gen(function* () {
    const proposed = yield* replyTo(agent, withheld, decision);
    if (proposed === undefined) {
      return { save: Effect.void };
    }
    const checked = yield* agent.firewall.checkOutgoing(
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
 * from this agent to the withheld message's author that names it as the one
 * it answers. A rule's own reply is used when it gave one; otherwise
 * `withheldReply` answers a message that broke a rule. The reply is checked
 * as any message this agent sends, and is sent only if allowed. A reply that
 * is not a valid message is not sent.
 */
function replyTo(agent: Agent, withheld: Message, decision: IncomingDecision) {
  if (decision.kind !== "withheld") {
    return Effect.succeed(undefined);
  }
  const text =
    decision.reply ??
    (decision.reason === "rule-violation"
      ? agent.settings.withheldReply
      : undefined);
  if (text === undefined) {
    return Effect.succeed(undefined);
  }
  return Message.create({
    author: agent.card,
    recipients: [withheld.author],
    content: [{ type: "text", text }],
    replyToMsgId: withheld.msgId,
  }).pipe(Effect.orElseSucceed(() => undefined));
}

/**
 * The evidence a check of the next message among `members` reads: the bounded
 * window of the latest history, and with `withProtocolHistory` every message
 * of the conversation.
 */
function evidenceFor(
  agent: Agent,
  members: readonly AgentCard[],
  withProtocolHistory: boolean,
) {
  return Effect.gen(function* () {
    const protocolHistory = withProtocolHistory
      ? yield* readProtocolHistory(agent, members)
      : undefined;
    const page =
      protocolHistory === undefined
        ? yield* readHistoryBefore(agent, members)
        : {
            entries: protocolHistory.entries.slice(-agent.historyLimit),
            earlierMessagesOmitted:
              !protocolHistory.complete ||
              protocolHistory.entries.length > agent.historyLimit,
          };
    let bytes = 0;
    const recent: Array<(typeof page.entries)[number]> = [];
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
      ...(protocolHistory === undefined
        ? {}
        : {
            protocolHistory: {
              messages: protocolHistory.entries.map(({ message }) => message),
              complete: protocolHistory.complete,
            },
          }),
    };
  });
}

/**
 * Every message of a conversation, in order, read a page at a time from the
 * newest. `complete` is false when a page that reports
 * earlier messages gives none to continue from.
 */
function readProtocolHistory(agent: Agent, members: readonly AgentCard[]) {
  return Effect.gen(function* () {
    const pages: Array<HistoryPage["entries"]> = [];
    let boundary: Message | undefined;
    while (true) {
      const page = yield* readHistoryBefore(agent, members, boundary);
      pages.push(page.entries);
      const oldest = page.entries[0]?.message;
      const advanced =
        oldest !== undefined &&
        !(
          oldest.author.agentId === boundary?.author.agentId &&
          oldest.msgId === boundary.msgId
        );
      if (!page.earlierMessagesOmitted || !advanced) {
        return {
          entries: [...pages].reverse().flat(),
          complete: !page.earlierMessagesOmitted,
        };
      }
      boundary = oldest;
    }
  });
}

/** One page of a conversation's messages, in order: the latest before `before`, or the latest of all. */
function readHistoryBefore(
  agent: Agent,
  members: readonly AgentCard[],
  before?: Message,
) {
  return agent.communication.readHistory({
    members,
    order: "newest",
    limit: agent.historyLimit,
    ...(before === undefined
      ? {}
      : {
          beforeMessage: {
            authorAgentId: before.author.agentId,
            msgId: before.msgId,
          },
        }),
  });
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
  return error._tag !== "CommunicationError" ||
    error.reason === "persistence-failed"
    ? Effect.die(error)
    : Effect.fail(new SendError({ reason: error.reason }));
}

/**
 * Fails with what the reader can correct: a wrong address or an unknown
 * agent. Any other failure is one of storage and ends the read as a defect.
 */
function historyFailure(
  error:
    | HistoryError
    | SendError
    | CommunicationError
    | IdentityError
    | {
        readonly _tag: "SqlError" | "ParseError" | "JsonRepresentationError";
      },
) {
  if (error._tag === "HistoryError") {
    return Effect.fail(error);
  }
  if (error._tag === "SendError") {
    return Effect.fail(new HistoryError({ reason: "invalid-address" }));
  }
  return error._tag === "IdentityError" && error.reason === "unknown-agent"
    ? Effect.fail(new HistoryError({ reason: "unknown-agent" }))
    : Effect.die(error);
}
