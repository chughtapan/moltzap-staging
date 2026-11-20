/**
 * @file The agents of one daemon. Each owner has one agent, saved as one row
 * with its card. An agent sends, reads its inbox and reads its history
 * through plain functions over the stored messages; the server agent's
 * firewall checks every message of its conversations.
 */

import { Data, Duration, Effect, Order, PubSub, Queue, Schema } from "effect";
import { randomBytes } from "node:crypto";
import type { InstalledNorms } from "../norms/index.js";
import {
  type CheckRecord,
  type Firewall,
  type FirewallRefusalReason,
  LocalGuidance,
} from "../firewall/firewall.js";
import {
  AGENT_NAME_RULE,
  type AgentCard,
  AgentId,
  AgentName,
  type CommunicationError,
  Message,
  MsgId,
  PrincipalId,
  wellFormedString,
} from "../messages.js";
import {
  agentNamed,
  agentOwnedBy,
  conversationPeers,
  type Owner,
  readConversation,
  readInbox,
  saveAgent,
  saveDecision,
  saveMessage,
  type StoredMessage,
  transaction,
  withDecisions,
} from "./store.js";

/**
 * The owner's agent has another name than the one asked for, or the name
 * belongs to another owner's agent. Carries no account claims.
 * Exported so dependent declarations can name the inferred Effect failure.
 * @public
 */
export class OwnershipError extends Data.TaggedError("OwnershipError") {}

/** What every agent's operations share on one daemon. */
export interface Daemon {
  /** The agent whose firewall checks every message. Every other agent is its client. */
  readonly serverAgent: AgentCard;
  /** The server agent's firewall. */
  readonly firewall: Firewall;
  /** The norms the server agent holds, which every agent then reads with `read_norms`. */
  readonly norms?: InstalledNorms;
  /** The turn of each conversation a message was sent in, by its client's agent id. */
  readonly turns: Map<string, Effect.Semaphore>;
  /** Published with an agent's id each time a message becomes readable for it. */
  readonly readable: PubSub.PubSub<string>;
}

/**
 * The owner's agent, created on first use with a new identifier and
 * principal. Concurrent first calls for one owner return the same agent. With
 * `agentName`, the agent is created under that name and an existing agent
 * must already have it.
 */
export const getOrCreateOwnedAgent = Effect.fn("getOrCreateOwnedAgent")(
  function* (owner: Owner, agentName?: AgentName) {
    yield* saveAgent(owner, {
      agentId: AgentId.mint(),
      agentName:
        agentName ?? AgentName.make(`agent-${randomBytes(12).toString("hex")}`),
      principalId: PrincipalId.mint(),
    });
    const saved = yield* agentOwnedBy(owner);
    if (
      saved === undefined ||
      (agentName !== undefined && saved.agentName !== agentName)
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

/** The most inbox entries, history records or conversation addresses one page holds. */
export const PAGE_SIZE = 50;

/** How the agents of its daemon address the agent named `agentName`. */
export const addressOfAgent = (agentName: AgentName): AgentAddress =>
  AgentAddress.make(`${AGENT_ADDRESS_PREFIX}${agentName}`);

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
export const SendRequest = Schema.Struct({
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
export class FirewallRefusal extends Data.TaggedError("FirewallRefusal")<{
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

/** History failure never exposes private protocol or persistence diagnostics. */
export class HistoryError extends Schema.TaggedError<HistoryError>()(
  "HistoryError",
  {
    reason: Schema.Literal("invalid-address", "unknown-agent"),
  },
) {
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

/** One inbox read: the cursor to read after and, in seconds, how long to wait for an entry when none follows it. */
interface ReceiveRequest {
  readonly cursor?: string;
  readonly wait?: number;
}

/**
 * At most `limit` of the inbox entries of the agent after `cursor`, in read
 * order, with the cursor to read on from and whether more entries follow.
 * Without a cursor the read starts from the newest entry and returns none.
 */
export const readInboxEntries = Effect.fn("readInboxEntries")(function* (
  agentId: string,
  read: Parameters<typeof readInbox>[1],
) {
  const { messages, nextCursor, hasMore } = yield* readInbox(agentId, read);
  const entries = yield* Effect.forEach(messages, entryOf, { concurrency: 1 });
  return { entries, nextCursor, hasMore };
});

/** The address of every conversation of `self`, in order. */
export const conversationsOf = Effect.fn("conversationsOf")((self: AgentCard) =>
  Effect.map(conversationPeers(self.agentId), (peers) => ({
    addresses: peers.map(addressOfAgent).sort(Order.string),
  })),
);

/** The guidance of a message as the store holds it: JSON text. */
const storedGuidance = Schema.parseJson(LocalGuidance);

/** The inbox entry of a message its recipient may read, with the guidance stored for it. */
function entryOf({ message, guidance }: StoredMessage) {
  const address = addressOfAgent(message.author.agentName);
  return guidance === null
    ? Effect.succeed<InboxEntry>({ address, message })
    : Effect.map(
        Effect.orDie(Schema.decode(storedGuidance)(guidance)),
        (decoded): InboxEntry => ({ address, message, guidance: decoded }),
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
  { agentId }: AgentCard,
  { cursor, wait }: ReceiveRequest,
) {
  const read = (from: typeof cursor) =>
    readInboxEntries(agentId, { limit: PAGE_SIZE, cursor: from }).pipe(
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
 * Holds the conversation of the server agent with `client` until the scope
 * closes. A check reads the conversation's earlier messages, so the messages
 * of one conversation are checked and stored one at a time. Other
 * conversations do not wait.
 */
function takeTurn(daemon: Daemon, client: AgentCard) {
  return Effect.sync(() => {
    const turn =
      daemon.turns.get(client.agentId) ?? Effect.unsafeMakeSemaphore(1);
    daemon.turns.set(client.agentId, turn);
    return turn;
  }).pipe(
    Effect.flatMap((turn) =>
      Effect.acquireReleaseInterruptible(turn.take(1), () => turn.release(1)),
    ),
    Effect.asVoid,
  );
}

/** The agent an address names, or none when no agent has the name. */
export const addressedAgent = (address: AgentAddress) =>
  agentNamed(address.slice(AGENT_ADDRESS_PREFIX.length));

/**
 * Sends one message between the server agent and a client, whichever of the
 * two is `author`, in their conversation's turn. The server agent's firewall
 * checks it as a message its agent sends or is sent. A message the server
 * agent may not send, or one whose check could not run, fails the send and is
 * not stored. Otherwise one transaction stores the message with the decision
 * and, for a message withheld from the server agent, the firewall's reply to
 * its sender with the decision that allows it. No transaction is open while a
 * check runs. A waiting read wakes once a message it may read is stored. Each
 * call creates a new message, so retrying an uncertain send can duplicate it.
 */
export const send = Effect.fn("send")(
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
    const server = daemon.serverAgent;
    const outgoing = author.agentId === server.agentId;
    const message = yield* Message.create({
      author,
      recipient,
      text: sent.text,
      replyToMsgId: sent.replyToMsgId,
    }).pipe(
      Effect.catchTag(
        "CommunicationError",
        ({ reason }: CommunicationError) => new SendError({ reason }),
      ),
    );
    yield* takeTurn(daemon, outgoing ? recipient : author);
    const earlier = yield* Effect.flatMap(
      readConversation([author.agentId, recipient.agentId]),
      (stored) => withDecisions(stored.map((row) => row.message)),
    );
    const result = outgoing
      ? yield* daemon.firewall.checkOutgoing(message, earlier)
      : yield* daemon.firewall.checkIncoming(message, earlier);
    if (
      result.kind === "refused" &&
      (outgoing || result.reason === "check-unavailable")
    ) {
      if (result.record !== undefined) {
        yield* saveDecision(message, result.record);
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
        ? yield* replyWith(server, message, result.reply)
        : undefined;
    const guidance =
      result.kind === "allowed" && result.guidance !== undefined
        ? yield* Effect.orDie(Schema.encode(storedGuidance)(result.guidance))
        : undefined;
    yield* transaction(
      Effect.gen(function* () {
        yield* saveMessage(message, {
          readable: result.kind === "allowed",
          ...(guidance === undefined ? {} : { guidance }),
        });
        if (result.record !== undefined) {
          yield* saveDecision(message, result.record);
        }
        if (reply !== undefined) {
          yield* saveMessage(reply.message, { readable: true });
          yield* saveDecision(reply.message, reply.record);
        }
      }),
    ).pipe(Effect.orDie);
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

/**
 * One page of the messages of `self`'s conversation with `other` as history
 * records, oldest first, after the cursor an earlier page returned: each one
 * `self` sent, and each one it received and may read. A page's `nextCursor`
 * is the place of its last message. Rule edits leave stored inbox entries as
 * they are.
 */
export const historyRecords = Effect.fn("historyRecords")(function* (
  self: AgentCard,
  other: AgentCard,
  cursor?: string,
) {
  const stored = yield* readConversation([self.agentId, other.agentId], {
    after: Number(cursor ?? 0),
    limit: PAGE_SIZE + 1,
  });
  const page = stored.slice(0, PAGE_SIZE);
  const records: HistoryRecord[] = [];
  for (const row of page) {
    if (row.message.author.agentId === self.agentId) {
      records.push({ kind: "sent", message: row.message });
    } else if (row.readNumber !== null) {
      records.push({ kind: "received", ...(yield* entryOf(row)) });
    }
  }
  const last = page.at(-1);
  return {
    records,
    ...(stored.length > PAGE_SIZE && last !== undefined
      ? { nextCursor: String(last.sequence) }
      : {}),
  };
});
