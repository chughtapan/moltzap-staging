/** @file Coordinates identity, message policy, communication and the agent's inbox. */

import {
  absurd,
  Clock,
  Context,
  type Duration,
  Effect,
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
import type { FirewallEvidence, FirewallRefusal } from "../firewall/index.js";
import {
  AgentAddress,
  CollectiveRequest,
  CollectiveResponse,
  CommunicationError,
  GroupAddress,
  Message,
  type MessageAddressInput,
  messageWithId,
  MsgId,
  type PaginatedRequest,
  type PaginatedResult,
  RequestError,
  SendError,
} from "../communication/index.js";
import {
  Communication,
  type Conversation,
  type Incoming,
  type Outgoing,
  restarting,
} from "../communication/service.js";
import { transaction } from "../database/index.js";
import { compareAscii, MAXIMUM_RUNTIME_ITEM_BYTES } from "../encoding.js";
import {
  Firewall,
  type IncomingDecision,
  type ResultEvidence,
} from "../firewall/service.js";
import { type AgentCard, Identity, IdentityError } from "../identity.js";
import {
  conversationsCursor,
  historyCursor,
  makeInbox,
  type Unchecked,
} from "./inbox.js";
import {
  canonicalMessageAddress,
  type DeliveryAcknowledgeError,
  type DeliveryToken,
  HistoryError,
  type HistoryRecord,
  InboxError,
  type InboxPage,
  type MessageAddress,
  PAGE_SIZE,
  type SendRequest,
  type SendResult,
} from "./message-format.js";

/** Start a live conversation-list traversal by omitting cursor. */
interface ListConversationsRequest extends PaginatedRequest {}
/** Conversation addresses are canonical and follow unsigned-ASCII order. */
interface ConversationsPage extends PaginatedResult {
  readonly addresses: readonly MessageAddress[];
}
/** Keep address unchanged when supplying the next cursor. */
interface ReadHistoryRequest extends PaginatedRequest {
  readonly address: MessageAddressInput;
}
/** A missing nextCursor ends this fixed-boundary history traversal. */
export interface HistoryPage extends PaginatedResult {
  readonly records: readonly HistoryRecord[];
}
type CommunicationService = Context.Tag.Service<typeof Communication>;
type Inbox = Effect.Effect.Success<ReturnType<typeof makeInbox>>;
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
  ) => Effect.Effect<SendResult, SendError | RequestError | FirewallRefusal>;
  /**
   * Reads pending inbox entries without marking them delivered. With `wait`,
   * a read that finds nothing waits up to that long, and returns as soon as
   * an entry is pending or empty when the wait ends.
   */
  readonly listInbox: (
    request?: PaginatedRequest & { readonly wait?: Duration.Duration },
  ) => Effect.Effect<InboxPage, InboxError>;
  /**
   * Marks a pending inbox entry delivered. Repeating it is harmless, and it
   * does not respond to a collective request.
   */
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
  ) => Effect.Effect<ConversationsPage, HistoryError>;
  /**
   * Pages include only history available at the first read. Rule edits leave
   * stored inbox entries as they are. Start a new traversal after reconnecting
   * or restarting.
   */
  readonly readHistory: (
    request: ReadHistoryRequest,
  ) => Effect.Effect<HistoryPage, HistoryError>;
  /**
   * Each notification carries no data and may cover several arrivals. Read the
   * inbox initially, after each notification and after reconnecting because
   * notifications can be missed.
   */
  readonly inboxChanged: Stream.Stream<undefined>;
}

/**
 * Hosts use this service for identity, messaging and local policy. It stores
 * an inbox entry before notifying of it. A stored entry survives delivery,
 * policy edits and restart.
 */
export class SocialHarness extends Context.Tag("SocialHarness/Harness")<
  SocialHarness,
  SocialHarnessService
>() {
  /** One agent's harness over its identity, firewall and communication. */
  static layer(settings: HarnessSettings, workers: FiberSet.FiberSet) {
    return Layer.scoped(SocialHarness, makeHarness(settings, workers));
  }
}

/** Supplies inbox processing limits for one agent. */
export interface HarnessSettings {
  readonly agentKey: string;
  readonly maximumHistoryRecords?: number;
  readonly maximumConcurrentChecks?: number;
  readonly hideGroups?: boolean;
  /**
   * What to tell the sender of a message the firewall withheld for breaking a
   * rule, when no rule supplied a reply of its own. Without it such a sender
   * is told nothing.
   */
  readonly withheldReply?: string;
}

function makeHarness(settings: HarnessSettings, workers: FiberSet.FiberSet) {
  return Effect.gen(function* () {
    const identity = yield* Identity;
    const firewall = yield* Firewall;
    const communication = yield* Communication;
    const scope = yield* Scope.Scope;
    const inbox = yield* makeInbox(settings.agentKey);
    const wake = yield* Queue.sliding<undefined>(1);
    const changes = yield* PubSub.sliding<undefined>(1);
    yield* Effect.addFinalizer(() => PubSub.shutdown(changes));
    const capacity = settings.maximumConcurrentChecks ?? 4;
    const historyLimit = settings.maximumHistoryRecords ?? 256;
    const self = identity.me;
    yield* supervise(
      workers,
      scope,
      communication
        .consume((incoming) =>
          inbox
            .storeUnchecked(
              addressOf(incomingMembers(incoming), self.agentId),
              incoming,
            )
            .pipe(Effect.zipRight(Queue.offer(wake, undefined)), Effect.asVoid),
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
          const check = checkIncoming(
            inbox,
            firewall,
            communication,
            unchecked,
            self,
            historyLimit,
            Option.fromNullable(settings.withheldReply),
          ).pipe(
            Effect.zipRight(PubSub.publish(changes, undefined)),
            Effect.ensuring(
              Effect.sync(() =>
                addressesBeingChecked.delete(unchecked.address),
              ).pipe(Effect.zipRight(Queue.offer(wake, undefined))),
            ),
          );
          yield* supervise(workers, scope, check);
        }
      }
    });
    yield* supervise(
      workers,
      scope,
      Queue.take(wake).pipe(Effect.zipRight(startChecks), Effect.forever),
    );
    yield* Queue.offer(wake, undefined);
    return SocialHarness.of({
      send: (input) =>
        Effect.suspend(() => {
          const request = structuredClone(input);
          return Effect.gen(function* () {
            if (settings.hideGroups && request.to.startsWith("group:")) {
              return yield* new SendError({
                reason: "invalid-address",
                detail:
                  "this daemon does not offer groups; address one agent as agent:<name>",
              });
            }
            const destination = yield* canonicalMessageAddress(
              request.to,
              self.agentName,
            );
            let outgoing: Outgoing;
            if ("collectiveResponse" in request) {
              if (
                !(yield* inbox.isCollectiveRequestVisible(
                  request.collectiveResponse.requestId,
                ))
              ) {
                return yield* new RequestError({
                  failure: { kind: "request-none" },
                });
              }
              const collectiveRequest = yield* communication
                .getCollectiveRequest(request.collectiveResponse.requestId)
                .pipe(
                  Effect.catchTag("CommunicationError", (error) =>
                    Effect.fail(sendFailure(error)),
                  ),
                );
              const expected = [
                collectiveRequest.author,
                ...collectiveRequest.recipients,
              ]
                .filter((member) => member.agentId !== self.agentId)
                .map((member) => member.agentName)
                .sort(compareAscii);
              const selectedNames =
                destination.kind === "direct"
                  ? [destination.remoteName]
                  : destination.memberNames.filter(
                      (name) => name !== self.agentName,
                    );
              if (
                expected.join(",") !==
                [...selectedNames].sort(compareAscii).join(",")
              ) {
                return yield* new SendError({ reason: "invalid-address" });
              }
              outgoing = yield* CollectiveResponse.create({
                author: self,
                request: collectiveRequest,
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
                        (name) => name !== self.agentName,
                      ),
                )
                .pipe(
                  Effect.catchTag("IdentityError", (error) =>
                    Effect.fail(
                      new SendError({
                        reason:
                          error.reason === "unknown-agent"
                            ? "unknown-agent"
                            : "persistence-failed",
                        ...(error.reason === "unknown-agent" &&
                        error.agentName !== undefined
                          ? {
                              detail: `This daemon has no agent named agent:${error.agentName}. Check the name with its owner.`,
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
              outgoing =
                "collectiveRequest" in request
                  ? yield* CollectiveRequest.create({
                      author: self,
                      recipients: [first, ...rest],
                      question: request.text,
                      ...request.collectiveRequest,
                    }).pipe(
                      Effect.catchTag("CommunicationError", (error) =>
                        Effect.fail(sendFailure(error)),
                      ),
                    )
                  : yield* Message.create({
                      author: self,
                      recipients: [first, ...rest],
                      content: [{ type: "text", text: request.text }],
                    }).pipe(Effect.mapError(sendFailure));
            }
            const evidence = yield* evidenceFor(
              inbox,
              communication,
              [outgoing.author, ...outgoing.recipients],
              historyLimit,
            );
            const decision = yield* firewall
              .checkOutgoing(outgoing, evidence)
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
              .send(outgoing)
              .pipe(
                Effect.catchTag("CommunicationError", (error) =>
                  Effect.fail(sendFailure(error)),
                ),
              );
            return { id: outgoing.msgId, address: destination.address };
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
        wait === undefined
          ? inbox.listInbox(page)
          : waitForInbox(inbox, changes, page, wait),
      acknowledgeDelivery: inbox.acknowledgeDelivery,
      listConversations: (request = {}) =>
        Effect.gen(function* () {
          const position =
            request.cursor === undefined
              ? undefined
              : yield* inbox.decodeCursor(conversationsCursor, request.cursor);
          const afterAddress = position?.address ?? "";
          const addresses = new Set(
            yield* inbox.collectiveResultAddresses(afterAddress),
          );
          let nextPageCursor = position?.communicationCursor;
          let lastPageCursor = nextPageCursor;
          let morePages = true;
          while (morePages) {
            lastPageCursor = nextPageCursor;
            const page = yield* communication.listConversations(
              lastPageCursor === undefined ? {} : { cursor: lastPageCursor },
            );
            const observed = page.conversations.map((members) =>
              addressOf(members, self.agentId),
            );
            for (const address of observed) {
              if (address > afterAddress) {
                addresses.add(address);
              }
            }
            nextPageCursor = page.nextCursor;
            morePages = nextPageCursor !== undefined;
            const firstAddressPastPage = [...addresses].sort(compareAscii)[
              PAGE_SIZE
            ];
            const lastObserved = observed[observed.length - 1];
            if (
              firstAddressPastPage !== undefined &&
              ((lastObserved !== undefined &&
                lastObserved >= firstAddressPastPage) ||
                !morePages)
            ) {
              break;
            }
          }
          const ordered = [...addresses].sort(compareAscii);
          const selected = ordered.slice(0, PAGE_SIZE);
          const last = selected[selected.length - 1];
          return {
            addresses: selected,
            ...(last !== undefined && (ordered.length > PAGE_SIZE || morePages)
              ? {
                  nextCursor: yield* inbox.encodeCursor(conversationsCursor, {
                    operation: "conversations",
                    address: last,
                    ...(lastPageCursor === undefined
                      ? {}
                      : { communicationCursor: lastPageCursor }),
                  }),
                }
              : {}),
          };
        }).pipe(Effect.mapError(historyFailure)),
      readHistory: (request) =>
        Effect.gen(function* () {
          const destination = yield* canonicalMessageAddress(
            request.address,
            self.agentName,
          );
          if (destination.address !== request.address) {
            return yield* new HistoryError({ reason: "invalid-address" });
          }
          const initial: typeof historyCursor.Type = {
            operation: "history",
            phase: "messages",
            address: destination.address,
            after: 0,
            through: yield* inbox.collectiveResultHead(request.address),
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
                  (name) => name !== self.agentName,
                );
          const records: HistoryRecord[] = [];
          const members =
            position.phase === "messages"
              ? yield* communication.getConversation([self.agentName, ...names])
              : undefined;
          if (position.phase === "messages" && members === undefined) {
            if (position.communicationCursor !== undefined) {
              return yield* new HistoryError({ reason: "invalid-cursor" });
            }
            if (position.through === 0) {
              yield* identity.resolve(names);
              return yield* new HistoryError({ reason: "invalid-address" });
            }
          }
          if (position.phase === "messages" && members !== undefined) {
            const page = yield* communication.readHistory({
              members,
              ...(position.communicationCursor === undefined
                ? {}
                : { cursor: position.communicationCursor }),
            });
            for (const { message } of page.entries) {
              if (message.author.agentId === self.agentId) {
                records.push({ kind: "sent", message });
              } else {
                const entry = yield* inbox.storedEntry({
                  authorAgentId: message.author.agentId,
                  msgId: message.msgId,
                });
                if (entry !== undefined) {
                  records.push({
                    kind: "received",
                    message: {
                      authorAgentId: message.author.agentId,
                      msgId: message.msgId,
                    },
                    ...entry,
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
                  communicationCursor: page.nextCursor,
                }),
              };
            }
          }
          const available = PAGE_SIZE - records.length;
          if (available === 0 && position.through > 0) {
            return {
              records,
              nextCursor: yield* inbox.encodeCursor(historyCursor, {
                operation: "history",
                phase: "collectiveResults",
                address: destination.address,
                after: position.after,
                through: position.through,
              }),
            };
          }
          const collectiveResults = yield* inbox.collectiveResultPage(
            request.address,
            position.after,
            position.through,
            available,
          );
          const selected = collectiveResults.slice(0, available);
          for (const collectiveResult of selected) {
            records.push({
              kind: "collectiveResult",
              deliveryToken: collectiveResult.token,
              ...collectiveResult.entry,
            });
          }
          const last = selected[selected.length - 1];
          return {
            records,
            ...(collectiveResults.length > selected.length && last !== undefined
              ? {
                  nextCursor: yield* inbox.encodeCursor(historyCursor, {
                    operation: "history",
                    phase: "collectiveResults",
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
  unchecked: Unchecked,
  self: AgentCard,
  historyLimit: number,
  withheldReply: Option.Option<string>,
) {
  return Effect.gen(function* () {
    const incoming = unchecked.incoming;
    if (
      incoming.kind === "collectiveRequest" &&
      (yield* Clock.currentTimeMillis) >=
        incoming.message.collectiveRequest.deadlineAt
    ) {
      return yield* inbox.withholdExpired(unchecked.token);
    }
    let evidence: FirewallEvidence | ResultEvidence | undefined;
    if (incoming.kind === "message" || incoming.kind === "collectiveRequest") {
      evidence = yield* evidenceFor(
        inbox,
        communication,
        [incoming.message.author, ...incoming.message.recipients],
        historyLimit,
        incoming.message,
      );
    } else if (incoming.kind === "collectiveResult") {
      const collectiveRequest =
        incoming.sources.request?.author.agentId === self.agentId
          ? undefined
          : incoming.sources.request;
      const responses = yield* Effect.forEach(
        incoming.sources.responses.filter(
          (message) => message.author.agentId !== self.agentId,
        ),
        (message) =>
          evidenceFor(
            inbox,
            communication,
            [message.author, ...message.recipients],
            historyLimit,
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
        answers: responses,
        ...(collectiveRequest === undefined
          ? {}
          : {
              question: yield* evidenceFor(
                inbox,
                communication,
                [collectiveRequest.author, ...collectiveRequest.recipients],
                historyLimit,
                collectiveRequest,
              ),
            }),
      };
    }
    const decision = yield* firewall.checkIncoming(incoming, evidence);
    const reply = yield* replyTo(incoming, decision, self, withheldReply);
    const expired =
      incoming.kind === "collectiveRequest" &&
      (yield* Clock.currentTimeMillis) >=
        incoming.message.collectiveRequest.deadlineAt;
    yield* transaction(
      decision.save.pipe(
        Effect.zipRight(inbox.recordDecision(unchecked, decision, { expired })),
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
 * The reply to a message the firewall withheld: an ordinary message from this
 * agent to the same conversation. A rule's own reply is used when it gave one;
 * otherwise `withheldReply` answers a message that broke a rule. Its id derives
 * from the withheld message, so checking that message again sends at most one
 * reply. A reply that is not a valid message is not sent, and the message
 * stays withheld.
 */
function replyTo(
  incoming: Incoming,
  decision: IncomingDecision,
  self: AgentCard,
  withheldReply: Option.Option<string>,
) {
  if (decision.kind !== "withheld" || !("message" in incoming)) {
    return Effect.succeed(undefined);
  }
  const text =
    decision.reply ??
    (decision.reason === "rule-violation"
      ? Option.getOrUndefined(withheldReply)
      : undefined);
  if (text === undefined) {
    return Effect.succeed(undefined);
  }
  const withheld = incoming.message;
  return Schema.decodeUnknown(MsgId)(
    `msg_${createHash("sha256")
      .update(`reply\n${withheld.author.agentId}\n${withheld.msgId}`)
      .digest("base64url")}`,
  ).pipe(
    Effect.flatMap((msgId) =>
      messageWithId(
        self,
        [withheld.author, ...withheld.recipients].filter(
          (member) => member.agentId !== self.agentId,
        ),
        msgId,
        [{ type: "text", text }],
      ),
    ),
    Effect.orElseSucceed(() => undefined),
  );
}

/**
 * Reads the inbox and, while nothing is pending, rereads after each change
 * until `wait` ends. Subscribing before the first read keeps an entry that
 * becomes pending between that read and the wait from being missed.
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
    if (first.entries.length > 0) {
      return first;
    }
    const arrived = yield* Queue.take(arrivals).pipe(
      Effect.zipRight(read),
      Effect.repeat({ until: (page) => page.entries.length > 0 }),
      Effect.timeoutOption(wait),
    );
    return Option.isSome(arrived) ? arrived.value : yield* read;
  }).pipe(Effect.scoped);
}

function evidenceFor(
  inbox: Inbox,
  communication: CommunicationService,
  members: readonly AgentCard[],
  historyLimit: number,
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
        limit: historyLimit,
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
        Effect.catchTag(
          "CommunicationError",
          () => new InboxError({ reason: "persistence-failed" }),
        ),
      );
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
    const history = yield* inbox.visibleMessages(
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

function incomingMembers(incoming: Incoming): readonly AgentCard[] {
  switch (incoming.kind) {
    case "message":
    case "collectiveRequest":
      return [incoming.message.author, ...incoming.message.recipients];
    case "collectiveResult":
      return [
        incoming.result.request.author,
        ...incoming.result.request.recipients,
      ];
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
    case "persistence-failed":
    case "message-conflict":
      return new SendError({ reason: error.reason });
    case "invalid-cursor":
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
  if (error instanceof CommunicationError) {
    return new HistoryError({
      reason:
        error.reason === "invalid-cursor"
          ? "invalid-cursor"
          : "persistence-failed",
    });
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
