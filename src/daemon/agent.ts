/**
 * @file The agents of one daemon. Each owner has one agent, saved as one row
 * with its card. An agent sends, reads its inbox and reads its history
 * through plain functions over the stored messages; the server agent's
 * firewall checks every message of its conversations.
 */

import { SqlClient } from "@effect/sql";
import { Data, Duration, Effect, PubSub, Queue, Schema } from "effect";
import { randomBytes, randomUUID } from "node:crypto";
import type { Firewall, IncomingDecision } from "../firewall/service.js";
import {
  type CommunicationError,
  Message,
  MsgId,
  PAGE_SIZE,
  type PaginatedRequest,
  wellFormedString,
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
  exactStruct,
  MAXIMUM_RUNTIME_ITEM_BYTES,
} from "../encoding.js";
import { type FirewallRefusal, LocalGuidance } from "../firewall/index.js";
import { AGENT_NAME_RULE, AgentCard, AgentName } from "../identity.js";

/** Authentication supplies both fields; email and host-provided agent selectors are excluded. */
interface Owner {
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

/** What every agent's operations share on one daemon. */
export interface Daemon {
  /** The agent whose firewall checks every message. Every other agent is its client. */
  readonly serverAgent: HostedAgent;
  /** The server agent's firewall. */
  readonly firewall: typeof Firewall.Service;
  /** Whether the server agent holds norms, which every agent then reads with `read_norms`. */
  readonly hasNorms: boolean;
  /** The most messages of a conversation a check reads as its recent history. */
  readonly historyLimit: number;
  /**
   * What to tell the sender of a message the firewall withheld for breaking a
   * rule, when no rule supplied a reply of its own. Without it such a sender
   * is told nothing.
   */
  readonly withheldReply?: string;
  /** The turn of each conversation a message was sent in, by its client's agent id. */
  readonly turns: Map<string, Effect.Semaphore>;
  /** Published with an agent's key each time a message becomes readable for it. */
  readonly readable: PubSub.PubSub<string>;
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

/** The scheme of an address naming one agent. */
const AGENT_ADDRESS_PREFIX = "agent:";

const isAgentName = Schema.is(AgentName);

/** The agent name an `agent:` address names, or undefined when the value is not an agent address. */
const parseAgentAddress = (value: string): AgentName | undefined => {
  if (!value.startsWith(AGENT_ADDRESS_PREFIX)) {
    return undefined;
  }
  const name = value.slice(AGENT_ADDRESS_PREFIX.length);
  return isAgentName(name) ? name : undefined;
};

/** The address of one agent. */
export const AgentAddress = Schema.String.pipe(
  Schema.filter((value) => parseAgentAddress(value) !== undefined, {
    description: `agent:<name>; a name is ${AGENT_NAME_RULE}`,
  }),
  Schema.brand("AgentAddress"),
  Schema.annotations({ identifier: "AgentAddress" }),
);
/**
 * A validated agent address.
 * @useDeclaredType
 */
export type AgentAddress = typeof AgentAddress.Type;

/** One message as the host reads it. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- The inbox owns this grammar shared by persistence and MCP schema composition.
export const InboxEntry = Schema.Struct({
  address: AgentAddress,
  message: Message,
  guidance: Schema.optionalWith(LocalGuidance, { exact: true }),
});
/** @useDeclaredType */
export type InboxEntry = typeof InboxEntry.Type;

/** What `send` takes: the recipient's address, the text and the message it answers. */
export const SendRequest = exactStruct({
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
export type SendRequest = typeof SendRequest.Type;

/**
 * What a send returns: the message id and the conversation's canonical
 * address. Accepted work continues independently of the caller's lifetime.
 */
export const SendResult = Schema.Struct({
  id: MsgId,
  address: AgentAddress,
}).annotations({ identifier: "SendResult" });

/** What a send and a history read both say of an address they cannot use. */
const addressFailureText = {
  "unknown-agent": "an agent the address names is not a known agent",
} as const;

/**
 * What the agent is told for each reason a send fails with. A send with an
 * unknown outcome may already have happened, so a new send can duplicate it.
 */
const sendFailureText = {
  ...addressFailureText,
  "membership-invalid": "the address names only you",
  "content-invalid": "the message content is invalid",
  "network-unavailable": "Social Harness is unavailable (network unavailable)",
  "outcome-unknown":
    "the send did not complete; the message may or may not have been sent",
} as const;

/**
 * Check `reason` before retrying. A send with `outcome-unknown` may already
 * have happened, so a new send can duplicate it. The host may show `message`
 * to its agent.
 */
export class SendError extends Data.TaggedError("SendError")<{
  readonly reason: keyof typeof sendFailureText;
  readonly detail?: string;
}> {
  override get message(): string {
    return `send failed: ${this.detail ?? sendFailureText[this.reason]}`;
  }
}

const historyFailureText = {
  ...addressFailureText,
  "invalid-address": "the address names you, not another agent",
} as const;

/** History failure never exposes private protocol or persistence diagnostics. */
class HistoryError extends Schema.TaggedError<HistoryError>()("HistoryError", {
  reason: Schema.Literal("invalid-address", "unknown-agent"),
}) {
  override get message(): string {
    return `reading failed: ${historyFailureText[this.reason]}`;
  }
}

/**
 * One record of a conversation's history: a message this agent sent, or the
 * stored inbox entry of a message it received.
 */
type HistoryRecord =
  | { readonly kind: "sent"; readonly message: Message }
  | ({ readonly kind: "received" } & InboxEntry);

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

/** The address of every conversation of `self`, in order. */
export const conversationsOf = Effect.fn("conversationsOf")(
  (self: HostedAgent) =>
    listConversations(self.agentKey).pipe(
      Effect.map((conversations) => ({
        addresses: conversations
          .map((members) => addressOf(members, self.card.agentId))
          .sort(compareAscii),
      })),
      Effect.catchAll(historyFailure),
    ),
);

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
 * A page of an agent's inbox entries after `cursor`, in the order they were
 * allowed, with the cursor that reads on from the last of them. Without a cursor the read starts from now. With `wait` and no entry to
 * return, it reads once more when an entry becomes readable or `wait` seconds
 * pass; a waiting read subscribes before it reads, so an entry that becomes
 * readable after the read is not missed.
 */
export const receive = Effect.fn("receive")(function* (
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
    return yield* read(cursor);
  }
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
}, Effect.scoped);

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
 * A waiting read wakes once a message it may read is stored. The result
 * carries the message id and canonical conversation; a message the firewall
 * withholds from the server agent is stored and returns the same. Each call
 * creates a new message, so retrying an uncertain send can duplicate it.
 */
export const send = Effect.fn("send")(
  function* (daemon: Daemon, self: HostedAgent, input: SendRequest) {
    const request = structuredClone(input);
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
    if (self.agentKey === daemon.serverAgent.agentKey) {
      yield* takeTurn(daemon, recipient);
      yield* sendAsServerAgent(daemon, route, outgoing);
    } else {
      yield* takeTurn(daemon, self);
      yield* sendAsClient(daemon, route, outgoing);
    }
    return { id: outgoing.msgId, address: request.to };
  },
  Effect.scoped,
  Effect.catchAll(sendFailure),
  (effect, ...[, self]) =>
    Effect.tapError(effect, (error) =>
      Effect.logWarning("Send failed").pipe(
        Effect.annotateLogs({
          agentKey: self.agentKey,
          error: error._tag,
          reason: error.message,
        }),
      ),
    ),
);

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
    (decision.reason === "rule-violation" ? daemon.withheldReply : undefined);
  if (text === undefined) {
    return Effect.succeed(undefined);
  }
  return Message.create({
    author: daemon.serverAgent.card,
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
    const { agentKey } = daemon.serverAgent;
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

/**
 * One page of a conversation's messages as history records, oldest first,
 * after the cursor an earlier page returned: each one `self` sent, and each
 * one it received and may read. Rule edits leave stored inbox entries as they
 * are.
 */
export const historyRecords = Effect.fn("historyRecords")(function* (
  self: HostedAgent,
  request: ReadHistoryRequest,
) {
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
}, Effect.catchAll(historyFailure));

/** The agent `to` names. An address that names the local agent is refused. */
function addressedAgent(
  to: AgentAddress,
  localAgentName: AgentName,
): Effect.Effect<AgentName, SendError> {
  const name = AgentName.make(to.slice(AGENT_ADDRESS_PREFIX.length));
  return name === localAgentName
    ? Effect.fail(new SendError({ reason: "membership-invalid" }))
    : Effect.succeed(name);
}

/** The address, as the local agent states it, of its conversation with the other of `members`. */
function addressOf(
  members: readonly AgentCard[],
  localAgentId: AgentCard["agentId"],
): AgentAddress {
  const other = members.find((member) => member.agentId !== localAgentId);
  return AgentAddress.make(`${AGENT_ADDRESS_PREFIX}${other?.agentName}`);
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
