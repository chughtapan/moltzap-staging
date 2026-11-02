/** @file Coordinates identity, message policy, communication and the agent's saved inbox. */

import { SqlClient } from "@effect/sql";
import {
  absurd,
  Clock,
  Context,
  DateTime,
  Deferred,
  Effect,
  Fiber,
  FiberSet,
  Layer,
  PubSub,
  Queue,
  Schema,
  Scope,
  Stream,
} from "effect";
import type { SendRequest } from "../host-protocol/send-request.js";
import type { SendResult } from "../host-protocol/send-result.js";
import { SendError } from "../communication/conversations/errors.js";
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
} from "../communication/index.js";
import {
  AgentAddress,
  GroupAddress,
  type MessageAddressInput,
} from "../communication/records/values.js";
import { compareAscii } from "../encoding/identifier.js";
import { MAXIMUM_RUNTIME_ITEM_BYTES } from "../encoding/limits.js";
import {
  Firewall,
  type FirewallEvidence,
  type FirewallRefusal,
  type ResultEvidence,
} from "../firewall/service.js";
import { canonicalMessageAddress } from "../host-protocol/address.js";
import {
  type AgentCard,
  agentCardOf,
  type VerifiedAgentIdentityCard,
} from "../identity/agent.js";
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
import { type HistoryExportPort, noHistoryExport } from "./message-log.js";

/** Host input is resolved before a communication value is constructed. */
export type { SendRequest } from "../host-protocol/send-request.js";
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
   * Saves registration and starts communication in the background. Returns the
   * saved identity without waiting for delivery recovery. Canceling the call
   * does not undo a successful registration. Reuse the registration id on retry.
   */
  readonly register: (
    request: RegistrationRequest,
  ) => Effect.Effect<VerifiedAgentIdentityCard, RegistrationError>;
  /**
   * Checks the supplied content and accepts permitted work for delivery.
   * The receipt identifies the accepted message or request and its canonical
   * conversation address. Completion does not mean that a peer has read it.
   * Each invocation creates fresh work. Retrying an uncertain send can duplicate it.
   */
  readonly send: (
    request: SendRequest,
  ) => Effect.Effect<SendResult, SendError | RequestError | FirewallRefusal>;
  /** Reads pending content without acknowledging it. */
  readonly listInbox: (
    request?: PaginatedRequest,
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
   * Lists canonical addresses, with direct conversations before groups.
   * Omit cursor to start and continue with nextCursor until it is absent.
   * Listing is live. A new conversation before the cursor appears on a fresh traversal.
   */
  readonly listConversations: (
    request?: ListConversationsRequest,
  ) => Effect.Effect<ListConversationsResult, HistoryError>;
  /**
   * Reads sent messages and saved received content for a canonical address.
   * Pages keep the first read's history boundary. Rule changes do not reassess
   * saved content. Start a fresh traversal after reconnecting or restarting.
   */
  readonly readHistory: (
    request: ReadHistoryRequest,
  ) => Effect.Effect<ReadHistoryResult, HistoryError>;
  /**
   * Signals that the saved inbox changed. Each signal carries no content and
   * may cover several arrivals. Read the inbox initially, after each signal
   * and after reconnecting to recover notifications that were missed.
   */
  readonly inboxChanged: Stream.Stream<undefined>;
}

/**
 * Connects an agent host to its identity, communication and message policy.
 * Sending checks the immutable message before submitting it. Receiving saves the
 * permitted content before notifying the host, so reads survive acknowledgment
 * and restart.
 *
 * ```mermaid
 * flowchart LR
 *   H[Host] &lt;-->|MCP| S[SocialHarness]
 *   S -->|Resolve names| I[L1 Identity]
 *   S -->|Check outgoing content| F[L3 Firewall]
 *   S -->|Send approved message| C[L2 Communication]
 *   C &lt;-->|Messages| P[Peers]
 *   C -->|Incoming message| S
 *   S -->|Check incoming content| F
 * ```
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

/** Construction captures capacities and network signing authority once. */
export interface HarnessSettings<R> {
  readonly communication: (
    identityCard: VerifiedAgentIdentityCard,
  ) => Layer.Layer<Communication, CommunicationError, R>;
  readonly maximumHistoryRecords?: number;
  readonly historyExport?: HistoryExportPort;
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
    const inbox = yield* makeInbox();
    const ready = yield* Deferred.make<
      CommunicationService,
      CommunicationError
    >();
    const wake = yield* Queue.sliding<undefined>(1);
    const changes = yield* PubSub.sliding<undefined>(1);
    yield* Effect.addFinalizer(() => PubSub.shutdown(changes));
    const capacity = settings.maximumConcurrentChecks ?? 4;
    const historyLimit = settings.maximumHistoryRecords ?? 256;
    const historyExport = settings.historyExport ?? noHistoryExport;
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
            historyExport,
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
              card.agentId,
              historyLimit,
            );
            const decision = yield* firewall
              .checkOutgoing(value, context)
              .pipe(
                Effect.catchTag("CheckInputError", () =>
                  Effect.fail(new SendError({ reason: "content-invalid" })),
                ),
              );
            yield* firewall
              .saveDecisions(decision.decisions)
              .pipe(
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
            const operationId =
              "kind" in value
                ? value.operationId
                : value.operation?.operationId;
            yield* historyExport.record({
              kind: "outbound",
              input: request,
              outcome: {
                kind: "sent",
                msgIds: "kind" in value ? [] : [value.msgId],
                ...(operationId === undefined ? {} : { operationId }),
              },
              at: yield* DateTime.now,
            });
            return {
              id: "kind" in value ? value.operationId : value.msgId,
              address: destination.address,
              ...(decision.guidance === undefined
                ? {}
                : { guidance: decision.guidance }),
            };
          }).pipe(
            Effect.catchTag("InboxError", () =>
              Effect.fail(new SendError({ reason: "persistence-failed" })),
            ),
            Effect.tapError((error) =>
              DateTime.now.pipe(
                Effect.flatMap((at) =>
                  historyExport.record({
                    kind: "outbound",
                    input: request,
                    outcome: { kind: "failed", error: error.message },
                    at,
                  }),
                ),
              ),
            ),
          );
        }),
      listInbox: (request) =>
        requireRegistered(new InboxError({ reason: "not-registered" })).pipe(
          Effect.zipRight(inbox.listInbox(request)),
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
  historyExport: HistoryExportPort,
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
        card.agentId,
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
            card.agentId,
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
                card.agentId,
                limit,
                question,
              ),
            }),
      };
    }
    const decision = yield* firewall.checkIncoming(value, evidence);
    const expired =
      value.kind === "request" &&
      (yield* Clock.currentTimeMillis) >= value.message.operation.deadlineAt;
    const sql = yield* SqlClient.SqlClient;
    const saved = yield* sql.withTransaction(
      firewall
        .saveDecisions(decision.decisions)
        .pipe(Effect.zipRight(inbox.publish(entry, decision, expired))),
    );
    if (saved !== undefined) {
      yield* historyExport.record({
        kind: "inbound",
        item: saved.item,
        at: yield* DateTime.now,
      });
    }
  });
}

function evidenceFor(
  inbox: Inbox,
  communication: CommunicationService,
  members: readonly AgentCard[],
  localAgentId: AgentCard["agentId"],
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
      history: history.map((entry) => ({
        ...entry,
        visibleToAgent:
          entry.visibleToAgent || entry.message.author.agentId === localAgentId,
      })),
      earlierHistoryOmitted:
        page.earlierMessagesOmitted || recent.length < page.entries.length,
      gaps: [
        ...(page.pendingOutgoing
          ? ["An earlier outgoing message is not yet complete."]
          : []),
        ...(recent.length < page.entries.length
          ? ["The preceding history exceeds the evidence byte limit."]
          : []),
      ],
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

/** The main service owns retained content and the types callers use to read it. */
export {
  DeliveryAcknowledgeError,
  DeliveryToken,
  InboxEntry,
  InboxError,
  InboxEvent,
  InboxPage,
} from "./inbox.js";
