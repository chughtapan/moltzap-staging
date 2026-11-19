/** @file Coordinates identity, message policy, communication and the agent's inbox. */

import {
  Cause,
  Context,
  Duration,
  Effect,
  Layer,
  Predicate,
  PubSub,
  Queue,
  Schedule,
  Schema,
  Scope,
} from "effect";
import { createHash } from "node:crypto";
import type { FirewallRefusal } from "../firewall/index.js";
import {
  type CommunicationError,
  Message,
  messageWithId,
  MsgId,
  PAGE_SIZE,
  type PaginatedRequest,
} from "../communication/index.js";
import { Communication, type HistoryPage } from "../communication/service.js";
import { afterCommit, transaction } from "../database.js";
import { compareAscii, MAXIMUM_RUNTIME_ITEM_BYTES } from "../encoding.js";
import {
  defaultConcurrentChecks,
  Firewall,
  type IncomingDecision,
} from "../firewall/service.js";
import { type AgentCard, Identity, type IdentityError } from "../identity.js";
import {
  addressedAgent,
  addressOf,
  type AgentAddress,
  HistoryError,
  type HistoryRecord,
  SendError,
  type SendRequest,
  type SendResult,
  type ToolRequest,
  type ToolResult,
} from "../mcp/tools.js";
import { type Inbox, makeInbox, type Unchecked } from "./inbox.js";

/** Keep address unchanged when supplying the next cursor. */
interface ReadHistoryRequest extends PaginatedRequest {
  readonly address: AgentAddress;
}
/** Sending, inbox and history operations for one agent. */
interface SocialHarnessService {
  /**
   * Checks policy and saves the permitted message for delivery. The result
   * carries its message id and canonical conversation, without confirming
   * peer receipt. Each call creates a new message. Retrying an uncertain send
   * can duplicate it.
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
  readonly receive: (
    request: ToolRequest<"receive_messages">["arguments"],
  ) => Effect.Effect<ToolResult<"receive_messages">>;
  /** The address of every conversation of the agent, in order. */
  readonly listConversations: () => Effect.Effect<
    ToolResult<"list_conversations">,
    HistoryError
  >;
  /**
   * A page of a conversation's records, oldest first, after the cursor an
   * earlier page returned. Rule edits leave stored inbox entries as they are.
   */
  readonly readHistory: (
    request: ReadHistoryRequest,
  ) => Effect.Effect<ToolResult<"read_conversation">, HistoryError>;
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
    return Layer.scoped(SocialHarness, makeHarness(settings));
  }
}

/** Supplies inbox processing limits for one agent. */
export interface HarnessSettings {
  readonly agentKey: string;
  readonly maximumHistoryRecords?: number;
  readonly maximumConcurrentChecks?: number;
  /**
   * What to tell the sender of a message the firewall withheld for breaking a
   * rule, when no rule supplied a reply of its own. Without it such a sender
   * is told nothing.
   */
  readonly withheldReply?: string;
}

/** What one agent's operations share: its card, the services it acts through and its settings. */
interface Agent {
  readonly card: AgentCard;
  readonly identity: Context.Tag.Service<typeof Identity>;
  readonly firewall: Context.Tag.Service<typeof Firewall>;
  readonly communication: Context.Tag.Service<typeof Communication>;
  readonly inbox: Inbox;
  readonly settings: HarnessSettings;
  readonly historyLimit: number;
  /** Published to each time an entry of this agent becomes readable, for the reads that wait. */
  readonly entryReadable: PubSub.PubSub<undefined>;
}

function makeHarness(settings: HarnessSettings) {
  return Effect.gen(function* () {
    const identity = yield* Identity;
    const firewall = yield* Firewall;
    const communication = yield* Communication;
    const scope = yield* Scope.Scope;
    const inbox = yield* makeInbox(settings.agentKey);
    /** Offered once an unchecked entry is committed, so the check that it starts can read the entry. */
    const wake = yield* Queue.sliding<undefined>(1);
    const capacity =
      settings.maximumConcurrentChecks ?? defaultConcurrentChecks;
    const agent: Agent = {
      entryReadable: yield* PubSub.sliding<undefined>(1),
      card: identity.me,
      identity,
      firewall,
      communication,
      inbox,
      settings,
      historyLimit: settings.maximumHistoryRecords ?? 256,
    };
    const fork = <E, R>(work: Effect.Effect<unknown, E, R>) =>
      Effect.forkIn(Effect.interruptible(work), scope);
    yield* fork(
      communication
        .consume((message) =>
          inbox
            .storeUnchecked(
              addressOf(
                [message.author, ...message.recipients],
                agent.card.agentId,
              ),
              message,
            )
            .pipe(Effect.zipRight(afterCommit(Queue.offer(wake, undefined)))),
        )
        .pipe(restarting(settings.agentKey, "consumer")),
    );
    const addressesBeingChecked = new Set<string>();
    const startChecks = Effect.gen(function* () {
      for (const unchecked of yield* inbox.unchecked(capacity)) {
        if (addressesBeingChecked.size >= capacity) {
          break;
        }
        if (!addressesBeingChecked.has(unchecked.address)) {
          addressesBeingChecked.add(unchecked.address);
          yield* fork(
            checkIncoming(agent, unchecked).pipe(
              Effect.catchAllCause((cause) => retryLater(agent, cause)),
              Effect.ensuring(
                Effect.sync(() =>
                  addressesBeingChecked.delete(unchecked.address),
                ).pipe(Effect.zipRight(Queue.offer(wake, undefined))),
              ),
            ),
          );
        }
      }
    });
    yield* fork(
      startChecks.pipe(
        Effect.zipRight(Queue.take(wake)),
        Effect.forever,
        restarting(settings.agentKey, "check loop"),
      ),
    );
    return SocialHarness.of({
      send: (request) => send(agent, request),
      receive: (request) => receive(agent, request),
      listConversations: () => listConversations(agent),
      readHistory: (request) => readHistory(agent, request),
    });
  }).pipe(Effect.withSpan("SocialHarness.layer"));
}

/**
 * One read of the inbox. A waiting read subscribes before it reads, so an
 * entry that becomes readable after the read is not missed.
 */
function receive(
  agent: Agent,
  { cursor, wait }: ToolRequest<"receive_messages">["arguments"],
) {
  const read = (from: typeof cursor) =>
    agent.inbox.listInbox(PAGE_SIZE, from).pipe(
      Effect.map(({ entries, nextCursor }) => ({ entries, nextCursor })),
      Effect.orDie,
    );
  if (wait === undefined) {
    return read(cursor);
  }
  return Effect.gen(function* () {
    const readable = yield* PubSub.subscribe(agent.entryReadable);
    const page = yield* read(cursor);
    if (page.entries.length > 0) {
      return page;
    }
    yield* Queue.take(readable).pipe(
      Effect.timeoutOption(Duration.seconds(wait)),
    );
    return yield* read(page.nextCursor);
  }).pipe(Effect.scoped);
}

function send(agent: Agent, input: SendRequest) {
  return Effect.suspend(() => {
    const request = structuredClone(input);
    return Effect.gen(function* () {
      const outgoing = yield* outgoingFor(
        agent,
        request,
        yield* addressedAgent(request.to, agent.card.agentName),
      );
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
      yield* agent.communication.send(outgoing);
      return { id: outgoing.msgId, address: request.to };
    }).pipe(
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
 * agent sent, and each one it received that its inbox stored an entry for.
 * The cursor is the one communication returned for this conversation.
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
    for (const { message } of page.entries) {
      if (message.author.agentId === agent.card.agentId) {
        records.push({ kind: "sent", message });
      } else {
        const entry = yield* agent.inbox.storedEntry({
          authorAgentId: message.author.agentId,
          msgId: message.msgId,
        });
        if (entry !== undefined) {
          records.push({ kind: "received", ...entry });
        }
      }
    }
    return {
      records,
      ...(page.nextCursor === undefined ? {} : { nextCursor: page.nextCursor }),
    };
  });
}

/**
 * Checks one unchecked message and records the decision, with the reply to a
 * withheld message, in one transaction. The evidence is read at most once and
 * serves both the check and the check of the reply.
 */
function checkIncoming(agent: Agent, unchecked: Unchecked) {
  const { communication, inbox } = agent;
  return Effect.gen(function* () {
    const { message } = unchecked;
    const evidence = yield* Effect.cachedFunction((protocolHistory: boolean) =>
      evidenceFor(
        agent,
        [message.author, ...message.recipients],
        protocolHistory,
        message,
      ),
    );
    const decision = yield* agent.firewall.checkIncoming(message, evidence);
    const reply = yield* checkedReply(agent, message, decision, evidence);
    yield* transaction(
      decision.save.pipe(
        Effect.zipRight(reply.save),
        Effect.zipRight(inbox.recordDecision(unchecked, decision)),
        Effect.tap((readable) =>
          readable
            ? afterCommit(PubSub.publish(agent.entryReadable, undefined))
            : Effect.void,
        ),
        Effect.tap(() =>
          reply.message === undefined
            ? Effect.void
            : communication.send(reply.message).pipe(
                Effect.catchIf(
                  (error) => error.reason === "message-conflict",
                  () => Effect.void,
                ),
              ),
        ),
      ),
    );
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
 * from this agent to the same conversation that names the withheld message as
 * the one it answers. A rule's own reply is used when it gave one; otherwise
 * `withheldReply` answers a message that broke a rule. Its id derives from
 * the withheld message, so checking that message again sends at most one
 * reply. The reply is checked as any message this agent sends, and is sent
 * only if allowed. A reply that is not a valid message is not sent, and the
 * message stays withheld.
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
  return Schema.decodeUnknown(MsgId)(
    `msg_${createHash("sha256")
      .update(`reply\n${withheld.author.agentId}\n${withheld.msgId}`)
      .digest("base64url")}`,
  ).pipe(
    Effect.flatMap((msgId) =>
      messageWithId(
        agent.card,
        [withheld.author, ...withheld.recipients].filter(
          (member) => member.agentId !== agent.card.agentId,
        ),
        { msgId, replyToMsgId: withheld.msgId },
        [{ type: "text", text }],
      ),
    ),
    Effect.orElseSucceed(() => undefined),
  );
}

/**
 * The evidence a check of a message among `members` reads: the bounded window
 * of history before `before`, or the latest when it is not given, and with
 * `withProtocolHistory` every message before it.
 */
function evidenceFor(
  agent: Agent,
  members: readonly AgentCard[],
  withProtocolHistory: boolean,
  before?: Message,
) {
  return Effect.gen(function* () {
    const protocolHistory = withProtocolHistory
      ? yield* readProtocolHistory(agent, members, before)
      : undefined;
    const page =
      protocolHistory === undefined
        ? yield* readHistoryBefore(agent, members, before)
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
    const history = yield* agent.inbox.visibleMessages(
      recent.map((earlier) => earlier.message),
    );
    return {
      history,
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
 * Every message of a conversation before `before`, in order, read a page at
 * a time from the newest. `complete` is false when a page that reports
 * earlier messages gives none to continue from.
 */
function readProtocolHistory(
  agent: Agent,
  members: readonly AgentCard[],
  before?: Message,
) {
  return Effect.gen(function* () {
    const pages: Array<HistoryPage["entries"]> = [];
    let boundary = before;
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

/**
 * Keeps a background loop of one agent running. A failure is logged and the
 * loop starts again after a pause that doubles from one second up to thirty.
 */
function restarting(agentKey: string, loop: string) {
  return <E, R>(work: Effect.Effect<never, E, R>) =>
    work.pipe(
      Effect.tapError((error) =>
        Effect.logError(`The ${loop} failed and will start again`).pipe(
          Effect.annotateLogs({
            agentKey,
            failure: Predicate.hasProperty(error, "_tag")
              ? String(error._tag)
              : "unknown",
          }),
        ),
      ),
      Effect.retry(
        Schedule.union(
          Schedule.exponential("1 second"),
          Schedule.spaced("30 seconds"),
        ),
      ),
    );
}

/**
 * Logs a check that failed and holds its conversation for a second. The
 * message stays unchecked, so the checks started next take it again.
 */
function retryLater(agent: Agent, cause: Cause.Cause<unknown>) {
  return Cause.isInterruptedOnly(cause)
    ? Effect.void
    : Effect.logError("A check failed and will be tried again").pipe(
        Effect.annotateLogs({ agentKey: agent.settings.agentKey, cause }),
        Effect.zipRight(Effect.sleep("1 second")),
      );
}
