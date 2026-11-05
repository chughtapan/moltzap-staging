/** @file Coordinates identity, message policy, communication and the agent's saved inbox. */

import { SqlClient } from "@effect/sql";
import {
  absurd,
  Clock,
  Context,
  Deferred,
  type Duration,
  Effect,
  Fiber,
  FiberSet,
  Layer,
  Option,
  PubSub,
  Queue,
  Schema,
  Scope,
  Stream,
} from "effect";
import { createHash } from "node:crypto";
import type { FirewallEvidence } from "../firewall/chain.js";
import { SendError } from "../communication/errors.js";
import {
  AgentAddress,
  GroupAddress,
  type MessageAddressInput,
  messageFromParticipants,
  MsgId,
} from "../communication/messages.js";
import {
  Answer,
  Communication,
  CommunicationError,
  type Conversation,
  type Incoming,
  Message,
  type Outgoing,
  type PaginatedRequest,
  type PaginatedResult,
  Request,
  RequestError,
} from "../communication/service.js";
import { MAXIMUM_RUNTIME_ITEM_BYTES } from "../encoding/canonical-json.js";
import { compareAscii } from "../encoding/identifier.js";
import {
  Firewall,
  type FirewallRefusal,
  type IncomingDecision,
  type ResultEvidence,
} from "../firewall/service.js";
import {
  type AgentCard,
  agentCardOf,
  type VerifiedAgentIdentityCard,
} from "../identity/agent-cards.js";
import {
  Identity,
  IdentityError,
  type RegistrationError,
  type RegistrationRequest,
} from "../identity/service.js";
import {
  catalogCursor,
  DeliveryAcknowledgeError,
  type DeliveryToken,
  historyCursor,
  InboxError,
  type InboxEvent,
  type InboxPage,
  makeInbox,
  type MessageAddress,
  type PendingItem,
} from "./inbox.js";
import {
  canonicalMessageAddress,
  type SendRequest,
  type SendResult,
} from "./message-format.js";

/** Host input is resolved before a communication value is constructed. */
export type { SendRequest } from "./message-format.js";
/** Start a live conversation-list traversal by omitting cursor. */
export interface ListConversationsRequest extends PaginatedRequest {}
/** Conversation addresses are canonical and follow unsigned-ASCII order. */
export interface ListConversationsResult extends PaginatedResult {
  readonly addresses: readonly MessageAddress[];
}
/** Keep address unchanged when supplying the next cursor. */
export interface ReadHistoryRequest extends PaginatedRequest {
  readonly address: MessageAddressInput;
}
/** History preserves sent messages and the saved view of received content. */
export type HistoryRecord =
  | { readonly kind: "sent"; readonly message: Message }
  | ({
      readonly kind: "received";
      readonly message: {
        readonly authorAgentId: Message["author"]["agentId"];
        readonly msgId: Message["msgId"];
      };
    } & InboxEvent)
  | ({
      readonly kind: "result";
      readonly deliveryToken: DeliveryToken;
    } & InboxEvent);
/** A missing nextCursor ends this fixed-boundary history traversal. */
export interface ReadHistoryResult extends PaginatedResult {
  readonly records: readonly HistoryRecord[];
}
/** History failure never exposes private protocol or persistence diagnostics. */
export class HistoryError extends Schema.TaggedError<HistoryError>()(
  "HistoryError",
  {
    reason: Schema.Literal(
      "not-registered",
      "invalid-address",
      "unknown-agent",
      "invalid-cursor",
      "history-gap",
      "persistence-failed",
    ),
  },
) {}

type CommunicationService = Context.Tag.Service<typeof Communication>;
type Inbox = Effect.Effect.Success<ReturnType<typeof makeInbox>>;
/** Sending, saved inbox and history operations for one agent. */
interface SocialHarnessService {
  /**
   * Saves registration and starts communication without waiting for recovery.
   * Cancellation preserves successful registration.
   * Retry with the same request, including `operationId`.
   */
  readonly register: (
    request: RegistrationRequest,
  ) => Effect.Effect<VerifiedAgentIdentityCard, RegistrationError>;
  /**
   * Checks policy and saves permitted work for delivery. The receipt identifies
   * accepted work and its canonical conversation, without confirming peer receipt.
   * Each call creates fresh work. Retrying an uncertain send can duplicate it.
   */
  readonly send: (
    request: SendRequest,
  ) => Effect.Effect<SendResult, SendError | RequestError | FirewallRefusal>;
  /**
   * Reads pending content without acknowledging it. With `wait`, a read that
   * finds nothing waits up to that long, and returns as soon as an item is
   * pending or empty when the wait ends.
   */
  readonly listInbox: (
    request?: PaginatedRequest & { readonly wait?: Duration.Duration },
  ) => Effect.Effect<InboxPage, InboxError>;
  /** Saved content remains readable after acknowledgment and restart. */
  readonly getInboxEvent: (
    token: DeliveryToken,
  ) => Effect.Effect<InboxEvent, InboxError>;
  /** Repeating an acknowledgment is harmless and does not answer a request. */
  readonly acknowledgeDelivery: (
    token: DeliveryToken,
  ) => Effect.Effect<void, DeliveryAcknowledgeError>;
  /**
   * Lists live canonical addresses, direct conversations before groups.
   * Follow `nextCursor` until absent. New conversations before the cursor require
   * a fresh traversal.
   */
  readonly listConversations: (
    request?: ListConversationsRequest,
  ) => Effect.Effect<ListConversationsResult, HistoryError>;
  /**
   * Pages include only history available at the first read. Rule edits preserve
   * saved views. Start a new traversal after reconnecting or restarting.
   */
  readonly readHistory: (
    request: ReadHistoryRequest,
  ) => Effect.Effect<ReadHistoryResult, HistoryError>;
  /**
   * Each notification carries no data and may cover several arrivals. Read the
   * inbox initially, after each notification and after reconnecting because
   * notifications can be missed.
   */
  readonly inboxChanged: Stream.Stream<undefined>;
}

/**
 * Hosts use this service for identity, messaging and local policy. It saves
 * permitted incoming content before notification. Saved content survives
 * acknowledgment, policy edits and restart.
 */
export class SocialHarness extends Context.Tag("SocialHarness/Harness")<
  SocialHarness,
  SocialHarnessService
>() {
  /** Shares one database and starts communication after registration is retained. */
  static layer<R>(settings: HarnessSettings<R>, workers: FiberSet.FiberSet) {
    return Layer.scoped(SocialHarness, makeHarness(settings, workers));
  }
}

/** Supplies communication and inbox processing limits for one agent. */
export interface HarnessSettings<R> {
  readonly agentKey: string;
  /** Builds communication after registration or saved identity loading. */
  readonly communication: (
    identityCard: VerifiedAgentIdentityCard,
  ) => Layer.Layer<Communication, CommunicationError, R>;
  readonly maximumHistoryRecords?: number;
  readonly maximumConcurrentChecks?: number;
  readonly hideGroups?: boolean;
  readonly hideCollectives?: boolean;
}

function makeHarness<R>(
  settings: HarnessSettings<R>,
  workers: FiberSet.FiberSet,
) {
  return Effect.gen(function* () {
    const identity = yield* Identity;
    const firewall = yield* Firewall;
    const scope = yield* Scope.Scope;
    const environment = yield* Effect.context<R>();
    const inbox = yield* makeInbox(settings.agentKey);
    const ready = yield* Deferred.make<
      CommunicationService,
      CommunicationError
    >();
    const wake = yield* Queue.sliding<undefined>(1);
    const changes = yield* PubSub.sliding<undefined>(1);
    yield* Effect.addFinalizer(() => PubSub.shutdown(changes));
    const capacity = settings.maximumConcurrentChecks ?? 4;
    const historyLimit = settings.maximumHistoryRecords ?? 256;
    let activated = false;
    const activate = Effect.fn(function* (card: VerifiedAgentIdentityCard) {
      if (activated) {
        return;
      }
      activated = true;
      const runtime = Layer.build(settings.communication(card)).pipe(
        Effect.provide(environment),
        Effect.provideService(Scope.Scope, scope),
        Effect.tapErrorCause((cause) => Deferred.failCause(ready, cause)),
        Effect.flatMap((context) =>
          Effect.gen(function* () {
            const communication = Context.get(context, Communication);
            yield* Deferred.succeed(ready, communication);
            return yield* communication.consume((incoming) =>
              inbox
                .queueIncoming(
                  addressOf(incomingParticipants(incoming), card.agentId),
                  incoming,
                )
                .pipe(
                  Effect.zipRight(Queue.offer(wake, undefined)),
                  Effect.asVoid,
                ),
            );
          }),
        ),
      );
      yield* supervise(workers, scope, runtime);
    });
    const self = Effect.gen(function* () {
      const state = yield* identity.me();
      return state.kind === "registered" ? state.identityCard : undefined;
    });
    const active = Effect.gen(function* () {
      const card = yield* self;
      if (card === undefined) {
        return yield* new SendError({ reason: "not-registered" });
      }
      yield* activate(card);
      const communication = yield* Deferred.await(ready).pipe(
        Effect.mapError(sendFailure),
      );
      return { card, communication };
    });
    const register = (request: RegistrationRequest) =>
      identity
        .register(request)
        .pipe(
          Effect.tap(activate),
          Effect.forkIn(scope),
          Effect.flatMap(Fiber.join),
        );
    const saved = yield* self;
    if (saved !== undefined) {
      yield* activate(saved);
    }
    const pending = new Set<string>();
    const checkPending = Effect.gen(function* () {
      const { card, communication } = yield* active;
      for (const entry of yield* inbox.pending(capacity)) {
        if (pending.size >= capacity) {
          break;
        }
        if (!pending.has(entry.address)) {
          pending.add(entry.address);
          const check = checkIncoming(
            inbox,
            firewall,
            communication,
            entry,
            card,
            historyLimit,
          ).pipe(
            Effect.zipRight(PubSub.publish(changes, undefined)),
            Effect.ensuring(
              Effect.sync(() => pending.delete(entry.address)).pipe(
                Effect.zipRight(Queue.offer(wake, undefined)),
              ),
            ),
          );
          yield* supervise(workers, scope, check);
        }
      }
    });
    yield* supervise(
      workers,
      scope,
      Queue.take(wake).pipe(Effect.zipRight(checkPending), Effect.forever),
    );
    if (saved !== undefined) {
      yield* Queue.offer(wake, undefined);
    }
    function requireRegistered<E>(failure: E) {
      return self.pipe(
        Effect.flatMap((card) =>
          card === undefined ? Effect.fail(failure) : Effect.succeed(card),
        ),
      );
    }
    return SocialHarness.of({
      register: (request) =>
        register(request).pipe(Effect.tap(() => Queue.offer(wake, undefined))),
      send: (input) =>
        Effect.suspend(() => {
          const request = structuredClone(input);
          return Effect.gen(function* () {
            if (settings.hideGroups && request.to.startsWith("group:")) {
              return yield* new SendError({ reason: "content-invalid" });
            }
            if (
              settings.hideCollectives &&
              ("collectiveResponse" in request ||
                request.collective?.op === "gather" ||
                request.collective?.op === "all_gather")
            ) {
              return yield* new SendError({ reason: "content-invalid" });
            }
            const { card, communication } = yield* active;
            const author = agentCardOf(card);
            const destination = yield* canonicalMessageAddress(
              request.to,
              author.agentName,
            );
            let value: Outgoing;
            if ("collectiveResponse" in request) {
              if (
                !(yield* inbox.isRequestVisible(
                  request.collectiveResponse.operationId,
                ))
              ) {
                return yield* new RequestError({
                  failure: { kind: "request-none" },
                });
              }
              const original = yield* communication
                .getRequest(request.collectiveResponse.operationId)
                .pipe(
                  Effect.catchTag("CommunicationError", (error) =>
                    Effect.fail(sendFailure(error)),
                  ),
                );
              const expected = [original.author, ...original.recipients]
                .filter((member) => member.agentId !== author.agentId)
                .map((member) => member.agentName)
                .sort(compareAscii);
              const selectedNames =
                destination.kind === "direct"
                  ? [destination.remoteName]
                  : destination.memberNames.filter(
                      (name) => name !== author.agentName,
                    );
              if (
                expected.join(",") !==
                [...selectedNames].sort(compareAscii).join(",")
              ) {
                return yield* new SendError({ reason: "invalid-address" });
              }
              value = yield* Answer.create({
                author,
                request: original,
                response: request.collectiveResponse,
              }).pipe(
                Effect.catchTag("CommunicationError", (error) =>
                  Effect.fail(sendFailure(error)),
                ),
              );
            } else {
              const recipients = yield* identity
                .resolve(
                  destination.kind === "direct"
                    ? [destination.remoteName]
                    : destination.memberNames.filter(
                        (name) => name !== author.agentName,
                      ),
                )
                .pipe(
                  Effect.catchTag("IdentityError", (error) =>
                    Effect.fail(
                      new SendError({
                        reason:
                          error.reason === "unknown-agent"
                            ? "unknown-agent"
                            : "network-unavailable",
                        ...(error.reason === "unknown-agent" &&
                        error.agentName !== undefined
                          ? {
                              detail: `No registered agent named agent:${error.agentName}.`,
                            }
                          : {}),
                      }),
                    ),
                  ),
                );
              const [first, ...rest] = recipients;
              if (first === undefined) {
                return yield* new SendError({ reason: "membership-invalid" });
              }
              value =
                request.collective?.op === "gather" ||
                request.collective?.op === "all_gather"
                  ? yield* Request.create({
                      author,
                      recipients: [first, ...rest],
                      question: request.text,
                      ...request.collective,
                    }).pipe(
                      Effect.catchTag("CommunicationError", (error) =>
                        Effect.fail(sendFailure(error)),
                      ),
                    )
                  : yield* Message.create({
                      author,
                      recipients: [first, ...rest],
                      content: [{ type: "text", text: request.text }],
                    }).pipe(Effect.mapError(sendFailure));
            }
            const context = yield* evidenceFor(
              inbox,
              communication,
              [value.author, ...value.recipients],
              historyLimit,
            );
            const decision = yield* firewall
              .checkOutgoing(value, context)
              .pipe(
                Effect.catchTag("CheckInputError", () =>
                  Effect.fail(new SendError({ reason: "content-invalid" })),
                ),
              );
            yield* decision.save.pipe(
              Effect.catchTag("FirewallPolicyError", () =>
                Effect.fail(new SendError({ reason: "persistence-failed" })),
              ),
            );
            if (decision.kind === "refused") {
              return yield* decision.refusal;
            }
            yield* communication
              .send(value)
              .pipe(
                Effect.catchTag("CommunicationError", (error) =>
                  Effect.fail(sendFailure(error)),
                ),
              );
            return {
              id: "kind" in value ? value.operationId : value.msgId,
              address: destination.address,
            };
          }).pipe(
            Effect.catchTag("InboxError", () =>
              Effect.fail(new SendError({ reason: "persistence-failed" })),
            ),
            Effect.tapError((error) =>
              Effect.logWarning("Send failed").pipe(
                Effect.annotateLogs({
                  agentKey: settings.agentKey,
                  error: error._tag,
                  reason: error.message,
                }),
              ),
            ),
          );
        }),
      listInbox: ({ wait, ...page } = {}) =>
        requireRegistered(new InboxError({ reason: "not-registered" })).pipe(
          Effect.zipRight(
            wait === undefined
              ? inbox.listInbox(page)
              : waitForInbox(inbox, changes, page, wait),
          ),
        ),
      getInboxEvent: (token) =>
        requireRegistered(new InboxError({ reason: "not-registered" })).pipe(
          Effect.zipRight(inbox.getInboxEvent(token)),
        ),
      acknowledgeDelivery: (token) =>
        requireRegistered(
          new DeliveryAcknowledgeError({ reason: "not-registered" }),
        ).pipe(Effect.zipRight(inbox.acknowledgeDelivery(token))),
      listConversations: (request = {}) =>
        Effect.gen(function* () {
          const { card, communication } = yield* active;
          const position =
            request.cursor === undefined
              ? undefined
              : yield* inbox.decodeCursor(catalogCursor, request.cursor);
          const after = position?.address ?? "";
          const addresses = new Set(yield* inbox.resultAddresses(after));
          let cursor = position?.communication;
          let resume = cursor;
          let more = true;
          while (more) {
            const pageStart = cursor;
            const page = yield* communication.listConversations(
              cursor === undefined ? {} : { cursor },
            );
            const observed = page.conversations.map((members) =>
              addressOf(members, card.agentId),
            );
            for (const address of observed) {
              if (address > after) {
                addresses.add(address);
              }
            }
            cursor = page.nextCursor;
            resume = pageStart;
            more = cursor !== undefined;
            const boundary = [...addresses].sort(compareAscii)[50];
            const lastObserved = observed[observed.length - 1];
            if (
              boundary !== undefined &&
              ((lastObserved !== undefined && lastObserved >= boundary) ||
                !more)
            ) {
              break;
            }
          }
          const ordered = [...addresses].sort(compareAscii);
          const selected = ordered.slice(0, 50);
          const last = selected[selected.length - 1];
          return {
            addresses: selected,
            ...(last !== undefined && (ordered.length > 50 || more)
              ? {
                  nextCursor: yield* inbox.encodeCursor(catalogCursor, {
                    operation: "conversations",
                    address: last,
                    ...(resume === undefined ? {} : { communication: resume }),
                  }),
                }
              : {}),
          };
        }).pipe(Effect.mapError(historyFailure)),
      readHistory: (request) =>
        Effect.gen(function* () {
          const { card, communication } = yield* active;
          const destination = yield* canonicalMessageAddress(
            request.address,
            card.agentName,
          );
          if (destination.address !== request.address) {
            return yield* new HistoryError({ reason: "invalid-address" });
          }
          const initial: typeof historyCursor.Type = {
            operation: "history",
            phase: "messages",
            address: destination.address,
            after: 0,
            through: yield* inbox.resultHead(request.address),
          };
          const position =
            request.cursor === undefined
              ? initial
              : yield* inbox.decodeCursor(historyCursor, request.cursor);
          if (position.address !== request.address) {
            return yield* new HistoryError({ reason: "invalid-cursor" });
          }
          const names =
            destination.kind === "direct"
              ? [destination.remoteName]
              : destination.memberNames.filter(
                  (name) => name !== card.agentName,
                );
          const records: HistoryRecord[] = [];
          const members =
            position.phase === "messages"
              ? yield* communication.getConversation([card.agentName, ...names])
              : undefined;
          if (position.phase === "messages" && members === undefined) {
            if (position.communication !== undefined) {
              return yield* new HistoryError({ reason: "history-gap" });
            }
            if (position.through === 0) {
              yield* identity.resolve(names);
              return yield* new HistoryError({ reason: "invalid-address" });
            }
          }
          if (position.phase === "messages" && members !== undefined) {
            const page = yield* communication.readHistory({
              members,
              ...(position.communication === undefined
                ? {}
                : { cursor: position.communication }),
            });
            for (const { message } of page.entries) {
              if (message.author.agentId === card.agentId) {
                records.push({ kind: "sent", message });
              } else {
                const view = yield* inbox.historyView({
                  authorAgentId: message.author.agentId,
                  msgId: message.msgId,
                });
                if (view !== undefined) {
                  records.push({
                    kind: "received",
                    message: {
                      authorAgentId: message.author.agentId,
                      msgId: message.msgId,
                    },
                    ...view,
                  });
                }
              }
            }
            if (page.nextCursor !== undefined) {
              return {
                records,
                nextCursor: yield* inbox.encodeCursor(historyCursor, {
                  ...position,
                  operation: "history",
                  communication: page.nextCursor,
                }),
              };
            }
          }
          const available = 50 - records.length;
          if (available === 0 && position.through > 0) {
            return {
              records,
              nextCursor: yield* inbox.encodeCursor(historyCursor, {
                operation: "history",
                phase: "results",
                address: destination.address,
                after: position.after,
                through: position.through,
              }),
            };
          }
          const resultPage = yield* inbox.resultPage(
            request.address,
            position.after,
            position.through,
            available,
          );
          const selected = resultPage.slice(0, available);
          for (const result of selected) {
            records.push({
              kind: "result",
              deliveryToken: result.token,
              ...result.event,
            });
          }
          const last = selected[selected.length - 1];
          return {
            records,
            ...(resultPage.length > selected.length && last !== undefined
              ? {
                  nextCursor: yield* inbox.encodeCursor(historyCursor, {
                    operation: "history",
                    phase: "results",
                    address: destination.address,
                    after: last.sequence,
                    through: position.through,
                  }),
                }
              : {}),
          };
        }).pipe(Effect.mapError(historyFailure)),
      inboxChanged: Stream.fromPubSub(changes),
    });
  }).pipe(Effect.withSpan("SocialHarness.layer"));
}

function checkIncoming(
  inbox: Inbox,
  firewall: Context.Tag.Service<typeof Firewall>,
  communication: CommunicationService,
  entry: PendingItem,
  card: VerifiedAgentIdentityCard,
  limit: number,
) {
  return Effect.gen(function* () {
    const value = entry.incoming;
    if (
      value.kind === "request" &&
      (yield* Clock.currentTimeMillis) >= value.message.operation.deadlineAt
    ) {
      return yield* inbox.retire(entry.token);
    }
    let evidence: FirewallEvidence | ResultEvidence | undefined;
    if (value.kind === "message" || value.kind === "request") {
      evidence = yield* evidenceFor(
        inbox,
        communication,
        [value.message.author, ...value.message.recipients],
        limit,
        value.message,
      );
    } else if (value.kind === "result") {
      const question =
        value.sources.request?.author.agentId === card.agentId
          ? undefined
          : value.sources.request;
      const answers = yield* Effect.forEach(
        value.sources.answers.filter(
          (message) => message.author.agentId !== card.agentId,
        ),
        (message) =>
          evidenceFor(
            inbox,
            communication,
            [message.author, ...message.recipients],
            limit,
            message,
          ).pipe(
            Effect.map((evidence) => ({
              message: {
                authorAgentId: message.author.agentId,
                msgId: message.msgId,
              },
              evidence,
            })),
          ),
        { concurrency: 1 },
      );
      evidence = {
        answers,
        ...(question === undefined
          ? {}
          : {
              question: yield* evidenceFor(
                inbox,
                communication,
                [question.author, ...question.recipients],
                limit,
                question,
              ),
            }),
      };
    }
    const decision = yield* firewall.checkIncoming(value, evidence);
    const reply = yield* replyTo(value, decision, agentCardOf(card));
    const expired =
      value.kind === "request" &&
      (yield* Clock.currentTimeMillis) >= value.message.operation.deadlineAt;
    const sql = yield* SqlClient.SqlClient;
    yield* sql.withTransaction(
      decision.save.pipe(
        Effect.zipRight(inbox.publish(entry, decision, expired)),
        Effect.tap(() =>
          reply === undefined
            ? Effect.void
            : communication.send(reply).pipe(
                Effect.catchIf(
                  (error) =>
                    error._tag === "CommunicationError" &&
                    error.reason === "message-conflict",
                  () => Effect.void,
                ),
              ),
        ),
      ),
    );
  });
}

/**
 * The firewall's reply to a message it withheld, when a rule asked for one: an
 * ordinary message from this agent to the same conversation. Its id derives
 * from the withheld message, so checking that message again sends at most one
 * reply. A reply that is not a valid message is not sent, and the message
 * stays withheld.
 */
function replyTo(value: Incoming, decision: IncomingDecision, self: AgentCard) {
  if (
    decision.kind !== "withheld" ||
    decision.reply === undefined ||
    !("message" in value)
  ) {
    return Effect.succeed(undefined);
  }
  const withheld = value.message;
  const text = decision.reply;
  return Schema.decodeUnknown(MsgId)(
    `msg_${createHash("sha256")
      .update(`reply\n${withheld.author.agentId}\n${withheld.msgId}`)
      .digest("base64url")}`,
  ).pipe(
    Effect.flatMap((msgId) =>
      messageFromParticipants(
        self,
        [withheld.author, ...withheld.recipients].filter(
          (member) => member.agentId !== self.agentId,
        ),
        { msgId, content: [{ type: "text", text }] },
      ),
    ),
    Effect.orElseSucceed(() => undefined),
  );
}

/**
 * Reads the inbox and, while it holds nothing, rereads after each change until
 * `wait` ends. Subscribing before the first read keeps an item that arrives
 * between that read and the wait from being missed.
 */
function waitForInbox(
  inbox: Inbox,
  changes: PubSub.PubSub<undefined>,
  request: PaginatedRequest,
  wait: Duration.Duration,
) {
  return Effect.gen(function* () {
    const arrivals = yield* PubSub.subscribe(changes);
    const read = inbox.listInbox(request);
    const first = yield* read;
    if (first.items.length > 0) {
      return first;
    }
    const arrived = yield* Queue.take(arrivals).pipe(
      Effect.zipRight(read),
      Effect.repeat({ until: (page) => page.items.length > 0 }),
      Effect.timeoutOption(wait),
    );
    return Option.isSome(arrived) ? arrived.value : yield* read;
  }).pipe(Effect.scoped);
}

function evidenceFor(
  inbox: Inbox,
  communication: CommunicationService,
  members: readonly AgentCard[],
  limit: number,
  before?: Message,
): Effect.Effect<FirewallEvidence, InboxError> {
  return Effect.gen(function* () {
    const selected = yield* conversation(members).pipe(
      Effect.catchTag("HistoryError", () =>
        Effect.fail(new InboxError({ reason: "persistence-failed" })),
      ),
    );
    const page = yield* communication
      .readHistory({
        members: selected,
        order: "newest",
        limit,
        ...(before === undefined
          ? {}
          : {
              beforeMessage: {
                authorAgentId: before.author.agentId,
                msgId: before.msgId,
              },
            }),
      })
      .pipe(
        Effect.catchTag("CommunicationError", (error) =>
          error.reason === "history-gap" || error.reason === "unknown-message"
            ? Effect.succeed(undefined)
            : Effect.fail(new InboxError({ reason: "persistence-failed" })),
        ),
      );
    if (page === undefined) {
      return {
        history: [],
        earlierHistoryOmitted: false,
        pendingOutgoing: [],
        gaps: ["The preceding conversation history is unavailable."],
      };
    }
    let bytes = 0;
    const recent: Array<(typeof page.entries)[number]> = [];
    for (const entry of [...page.entries].reverse()) {
      if (bytes + entry.messageBytes > MAXIMUM_RUNTIME_ITEM_BYTES) {
        break;
      }
      recent.push(entry);
      bytes += entry.messageBytes;
    }
    recent.reverse();
    const history = yield* inbox.visibleMessages(
      recent.map((entry) => entry.message),
    );
    return {
      history,
      earlierHistoryOmitted:
        page.earlierMessagesOmitted || recent.length < page.entries.length,
      pendingOutgoing: page.pendingOutgoing,
      gaps:
        recent.length < page.entries.length
          ? ["The preceding history exceeds the evidence byte limit."]
          : [],
    };
  });
}

function conversation(
  members: readonly AgentCard[],
): Effect.Effect<Conversation, HistoryError> {
  const ordered = [...members].sort((left, right) =>
    compareAscii(left.agentName, right.agentName),
  );
  const [first, second, ...rest] = ordered;
  return first === undefined || second === undefined
    ? Effect.fail(new HistoryError({ reason: "invalid-address" }))
    : Effect.succeed<Conversation>([first, second, ...rest]);
}

function incomingParticipants(incoming: Incoming): readonly AgentCard[] {
  switch (incoming.kind) {
    case "message":
    case "request":
      return [incoming.message.author, ...incoming.message.recipients];
    case "result":
      return [
        incoming.result.request.author,
        ...incoming.result.request.recipients,
      ];
    case "request-failed":
      return [incoming.request.author, ...incoming.request.recipients];
    default:
      return absurd(incoming);
  }
}

function addressOf(
  members: readonly AgentCard[],
  localAgentId: AgentCard["agentId"],
): MessageAddress {
  const others = members.filter((member) => member.agentId !== localAgentId);
  return others.length === 1
    ? AgentAddress.make(`agent:${others[0]?.agentName}`)
    : GroupAddress.make(
        `group:${members
          .map((member) => member.agentName)
          .sort(compareAscii)
          .join(",")}`,
      );
}

function sendFailure(error: CommunicationError): SendError {
  switch (error.reason) {
    case "membership-invalid":
    case "content-invalid":
    case "version-mismatch":
    case "certification-unavailable":
    case "persistence-failed":
    case "network-unavailable":
    case "outcome-unknown":
    case "message-conflict":
      return new SendError({ reason: error.reason });
    case "invalid-cursor":
    case "history-gap":
    case "unknown-message":
    case "outbox-too-old":
    case "agent-not-admitted":
      return new SendError({ reason: "persistence-failed" });
    default:
      return absurd(error.reason);
  }
}

function historyFailure(error: unknown): HistoryError {
  if (error instanceof HistoryError) {
    return error;
  }
  if (error instanceof InboxError) {
    return new HistoryError({
      reason:
        error.reason === "invalid-cursor"
          ? "invalid-cursor"
          : "persistence-failed",
    });
  }
  if (error instanceof IdentityError) {
    return new HistoryError({
      reason:
        error.reason === "unknown-agent"
          ? "unknown-agent"
          : "persistence-failed",
    });
  }
  if (error instanceof SendError && error.reason === "not-registered") {
    return new HistoryError({ reason: "not-registered" });
  }
  if (error instanceof CommunicationError) {
    if (error.reason === "invalid-cursor" || error.reason === "history-gap") {
      return new HistoryError({ reason: error.reason });
    }
    return new HistoryError({ reason: "persistence-failed" });
  }
  return new HistoryError({ reason: "invalid-address" });
}

function supervise<E, R>(
  workers: FiberSet.FiberSet,
  scope: Scope.Scope,
  work: Effect.Effect<unknown, E, R>,
) {
  return Effect.forkIn(Effect.interruptible(work), scope).pipe(
    Effect.flatMap((fiber) => FiberSet.add(workers, fiber)),
  );
}
