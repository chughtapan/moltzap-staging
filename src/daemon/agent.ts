/** @file Coordinates identity, message policy, communication and the agent's inbox. */

import { SqlClient } from "@effect/sql";
import {
  Context,
  Effect,
  Layer,
  Predicate,
  Schedule,
  Schema,
  Scope,
} from "effect";
import type { FirewallRefusal } from "../firewall/index.js";
import {
  type CommunicationError,
  Message,
  PAGE_SIZE,
  type PaginatedRequest,
} from "../communication/index.js";
import { Communication, type HistoryPage } from "../communication/service.js";
import { transaction } from "../database.js";
import { compareAscii, MAXIMUM_RUNTIME_ITEM_BYTES } from "../encoding.js";
import { Firewall } from "../firewall/service.js";
import { type AgentCard, Identity, type IdentityError } from "../identity.js";
import {
  addressedAgent,
  addressOf,
  AgentAddress,
  HistoryError,
  type HistoryRecord,
  SendError,
  type SendRequest,
  type SendResult,
  type ToolResult,
} from "../mcp/tools.js";
import {
  type CursorError,
  decodeCursor,
  encodeCursor,
  type Inbox,
  makeInbox,
} from "./inbox.js";

const conversationsCursor = Schema.Struct({
  operation: Schema.Literal("conversations"),
  address: AgentAddress,
});

/** Keep address unchanged when supplying the next cursor. */
interface ReadHistoryRequest extends PaginatedRequest {
  readonly address: AgentAddress;
}
/** Sending, inbox and history operations for one agent. */
interface SocialHarnessService {
  /**
   * Checks the message against this agent's policy and its recipient's, and
   * stores it when both allow it. The result carries its message id and
   * canonical conversation, without confirming that the recipient read it.
   * Each call creates a new message. Retrying an uncertain send can
   * duplicate it.
   */
  readonly send: (
    request: SendRequest,
  ) => Effect.Effect<SendResult, SendError | FirewallRefusal>;
  /**
   * Checks a message another agent is sending to this agent. When it is
   * allowed, `store` and this agent's inbox entry for the message commit in
   * one transaction with the check; a refusal stores only the check. One
   * sender's messages are admitted one at a time, so each check reads the
   * conversation the one before it left.
   */
  readonly admit: Admission;
  /**
   * A page of the inbox entries after `cursor`, in the order they were
   * stored, with the cursor that reads on from the last of them. Without a
   * cursor the read starts from now and returns no entry.
   */
  readonly receive: (request: {
    readonly cursor?: string;
  }) => Effect.Effect<ToolResult<"receive_messages">>;
  /**
   * Lists the addresses of the agent's conversations.
   * Omitting the cursor starts a live traversal. Follow `nextCursor` until
   * absent. New conversations before the cursor require a fresh traversal.
   */
  readonly listConversations: (
    request?: PaginatedRequest,
  ) => Effect.Effect<ToolResult<"list_conversations">, HistoryError>;
  /**
   * Pages include only history available at the first read. Rule edits leave
   * stored inbox entries as they are. Start a new traversal after reconnecting
   * or restarting.
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

/** A recipient's check of a message, run around the `store` that saves it. */
export type Admission = (
  message: Message,
  store: Effect.Effect<void, CommunicationError>,
) => Effect.Effect<void, SendError | FirewallRefusal>;

/** One agent's key and limits, and the check its recipient makes of what it sends. */
export interface HarnessSettings {
  readonly agentKey: string;
  readonly maximumHistoryRecords?: number;
  /** The recipient's admission of this agent's messages. Without one the recipient checks nothing. */
  readonly admission?: Admission;
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
}

function makeHarness(settings: HarnessSettings) {
  return Effect.gen(function* () {
    const identity = yield* Identity;
    const firewall = yield* Firewall;
    const communication = yield* Communication;
    const scope = yield* Scope.Scope;
    const sql = yield* SqlClient.SqlClient;
    const inbox = yield* makeInbox(settings.agentKey);
    const agent: Agent = {
      card: identity.me,
      identity,
      firewall,
      communication,
      inbox,
      settings,
      historyLimit: settings.maximumHistoryRecords ?? 256,
    };
    yield* Effect.forkIn(
      Effect.interruptible(
        communication
          .consume((message) =>
            inbox.store(
              addressOf(
                [message.author, ...message.recipients],
                agent.card.agentId,
              ),
              message,
            ),
          )
          .pipe(restarting(settings.agentKey, "consumer")),
      ),
      scope,
    );
    const turns = new Map<string, Effect.Semaphore>();
    return SocialHarness.of({
      send: (request) => send(agent, request),
      admit: (message, store) => {
        const sender = message.author.agentId;
        const turn = turns.get(sender) ?? Effect.unsafeMakeSemaphore(1);
        turns.set(sender, turn);
        return turn
          .withPermits(1)(admit(agent, message, store))
          .pipe(Effect.provideService(SqlClient.SqlClient, sql));
      },
      receive: ({ cursor }) =>
        inbox.listInbox(PAGE_SIZE, cursor).pipe(
          Effect.map(({ entries, nextCursor }) => ({ entries, nextCursor })),
          Effect.orDie,
        ),
      listConversations: (request = {}) => listConversations(agent, request),
      readHistory: (request) => readHistory(agent, request),
    });
  }).pipe(Effect.withSpan("SocialHarness.layer"));
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
      const store = agent.communication.send(outgoing);
      yield* agent.settings.admission?.(outgoing, store) ?? store;
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

function listConversations(agent: Agent, request: PaginatedRequest) {
  return Effect.gen(function* () {
    const after =
      request.cursor === undefined
        ? ""
        : (yield* decodeCursor(conversationsCursor, request.cursor)).address;
    const addresses = (yield* agent.communication.listConversations)
      .map((members) => addressOf(members, agent.card.agentId))
      .filter((address) => address > after)
      .sort(compareAscii);
    const selected = addresses.slice(0, PAGE_SIZE);
    const last = selected.at(-1);
    return {
      addresses: selected,
      ...(last !== undefined && addresses.length > PAGE_SIZE
        ? {
            nextCursor: yield* encodeCursor(conversationsCursor, {
              operation: "conversations",
              address: last,
            }),
          }
        : {}),
    };
  }).pipe(Effect.catchAll(historyFailure));
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

function admit(
  agent: Agent,
  message: Message,
  store: Effect.Effect<void, CommunicationError>,
) {
  const members = [message.author, ...message.recipients];
  return Effect.gen(function* () {
    const decision = yield* agent.firewall.checkIncoming(
      message,
      (protocolHistory) => evidenceFor(agent, members, protocolHistory),
    );
    if (decision.kind === "refused") {
      yield* decision.save;
      return yield* decision.refusal;
    }
    yield* transaction(
      decision.save.pipe(
        Effect.zipRight(store),
        Effect.zipRight(
          agent.inbox.store(
            addressOf(members, agent.card.agentId),
            message,
            decision.guidance,
          ),
        ),
      ),
    );
  }).pipe(Effect.catchAll(sendFailure));
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
      history: recent.map(({ message }) => ({ message, visibleToAgent: true })),
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
    error.reason === "persistence-failed" ||
    error.reason === "invalid-cursor"
    ? Effect.die(error)
    : Effect.fail(new SendError({ reason: error.reason }));
}

/**
 * Fails with what the reader can correct: a wrong address, an unknown agent
 * or a cursor this read did not return. Any other failure is one of storage
 * and ends the read as a defect.
 */
function historyFailure(
  error:
    | HistoryError
    | SendError
    | CursorError
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
  if (
    error._tag === "CursorError" ||
    (error._tag === "CommunicationError" && error.reason === "invalid-cursor")
  ) {
    return Effect.fail(new HistoryError({ reason: "invalid-cursor" }));
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
