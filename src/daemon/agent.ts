/**
 * @file The agents of one daemon. Each owner has one agent, saved as one row
 * with its card. An agent sends, reads its inbox and reads its history
 * through plain functions over the stored messages; the server agent's
 * firewall checks every message of its conversations.
 */

import { SqlClient } from "@effect/sql";
import { Data, Duration, Effect, PubSub, Queue, Schema } from "effect";
import { randomBytes, randomUUID } from "node:crypto";
import type { InstalledNorms } from "../norms/index.js";
import {
  type CommunicationError,
  Message,
  MsgId,
  PAGE_SIZE,
  wellFormedString,
} from "../communication/index.js";
import {
  type HeldMessage,
  listConversations,
  readHistory,
  readInbox,
  readLatest,
  sendMessage,
} from "../communication/service.js";
import { transaction } from "../database.js";
import {
  compareAscii,
  decodeJson,
  encodeJson,
  exactStruct,
} from "../encoding.js";
import {
  type CheckRecord,
  type Firewall,
  type FirewallRefusalReason,
  LocalGuidance,
} from "../firewall/firewall.js";
import { AGENT_NAME_RULE, AgentCard, AgentName } from "../identity.js";
import { saveDecision, withDecisions } from "./decisions.js";

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
  readonly firewall: Firewall;
  /** The norms the server agent holds, which every agent then reads with `read_norms`. */
  readonly norms?: InstalledNorms;
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
      Effect.orDie,
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

/** The agent an address names, or none when no agent has the name. */
export const addressedAgent = Effect.fn("addressedAgent")(function* (
  address: AgentAddress,
) {
  const [agent] = yield* agentsWith({
    agent_name: address.slice(AGENT_ADDRESS_PREFIX.length),
  });
  return agent;
}, Effect.orDie);

/** How the agents of its daemon address `agent`. */
export const addressOfAgent = (agent: HostedAgent): AgentAddress =>
  AgentAddress.make(`${AGENT_ADDRESS_PREFIX}${agent.card.agentName}`);

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
      readonly author: HostedAgent;
      readonly recipient: HostedAgent;
      readonly text: string;
      readonly replyToMsgId?: MsgId;
    },
  ) {
    const { author, recipient } = sent;
    const server = daemon.serverAgent;
    const outgoing = author.agentKey === server.agentKey;
    const message = yield* Message.create({
      author: author.card,
      recipients: [recipient.card],
      content: [{ type: "text", text: sent.text }],
      replyToMsgId: sent.replyToMsgId,
    }).pipe(
      Effect.catchTag(
        "CommunicationError",
        ({ reason }: CommunicationError) => new SendError({ reason }),
      ),
    );
    yield* takeTurn(daemon, outgoing ? recipient : author);
    const earlier = yield* earlierMessages(server, message);
    const result = outgoing
      ? yield* daemon.firewall.checkOutgoing(message, earlier)
      : yield* daemon.firewall.checkIncoming(message, earlier);
    if (
      result.kind === "refused" &&
      (outgoing || result.reason === "check-unavailable")
    ) {
      if (result.record !== undefined) {
        yield* saveDecision(server.agentKey, message, result.record);
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
    const route = {
      authorKey: author.agentKey,
      recipientKey: recipient.agentKey,
    };
    yield* transaction(
      Effect.gen(function* () {
        yield* sendMessage(route, message, {
          readable: result.kind === "allowed",
          ...(result.kind === "allowed" && result.guidance !== undefined
            ? { guidance: yield* encodeJson(LocalGuidance, result.guidance) }
            : {}),
        });
        if (result.record !== undefined) {
          yield* saveDecision(server.agentKey, message, result.record);
        }
        if (reply !== undefined) {
          yield* sendMessage(
            { authorKey: route.recipientKey, recipientKey: route.authorKey },
            reply.message,
            { readable: true },
          );
          yield* saveDecision(server.agentKey, reply.message, reply.record);
        }
      }),
    ).pipe(Effect.orDie);
    if (result.kind === "allowed") {
      yield* PubSub.publish(daemon.readable, recipient.agentKey);
    } else if (reply !== undefined) {
      yield* PubSub.publish(daemon.readable, author.agentKey);
    }
    return message.msgId;
  },
  Effect.scoped,
  (effect, ...[, { author }]) =>
    Effect.tapError(effect, (error) =>
      Effect.logWarning("Send failed").pipe(
        Effect.annotateLogs({
          agentKey: author.agentKey,
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
  server: HostedAgent,
  withheld: Message,
  reply: { readonly text: string; readonly record: CheckRecord },
) {
  return Message.create({
    author: server.card,
    recipients: [withheld.author],
    content: [{ type: "text", text: reply.text }],
    replyToMsgId: withheld.msgId,
  }).pipe(
    Effect.map((message) => ({ message, record: reply.record })),
    Effect.orElseSucceed(() => undefined),
  );
}

/**
 * Every message before `next` in its conversation, in order, each with what
 * the server agent's firewall decided of it.
 */
function earlierMessages(server: HostedAgent, next: Message) {
  return readLatest(server.agentKey, {
    members: [next.author, ...next.recipients],
  }).pipe(
    Effect.orDie,
    Effect.flatMap(({ entries }) =>
      withDecisions(
        server.agentKey,
        entries.map(({ message }) => message),
      ),
    ),
  );
}

/**
 * One page of the messages of `self`'s conversation with `other` as history
 * records, oldest first, after the cursor an earlier page returned: each one
 * `self` sent, and each one it received and may read. Rule edits leave stored
 * inbox entries as they are.
 */
export const historyRecords = Effect.fn("historyRecords")(function* (
  self: HostedAgent,
  other: HostedAgent,
  cursor?: string,
) {
  const page = yield* readHistory(self.agentKey, {
    members: [self.card, other.card],
    ...(cursor === undefined ? {} : { cursor }),
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
}, Effect.orDie);

/** The address, as the local agent states it, of its conversation with the other of `members`. */
function addressOf(
  members: readonly AgentCard[],
  localAgentId: AgentCard["agentId"],
): AgentAddress {
  const other = members.find((member) => member.agentId !== localAgentId);
  return AgentAddress.make(`${AGENT_ADDRESS_PREFIX}${other?.agentName}`);
}
