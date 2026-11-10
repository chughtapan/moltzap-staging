/** @file Accepts durable messages and questions, and supplies certified history and incoming work. */

import { SqlClient } from "@effect/sql/SqlClient";
import {
  Context,
  Deferred,
  Duration,
  Effect,
  FiberSet,
  Layer,
  Option,
  type ParseResult,
  Queue,
  Schedule,
  Schema,
  Scope,
} from "effect";
import type {
  CommunicationService,
  CommunicationSettings,
  Incoming,
  Outgoing,
} from "./contract.js";
import { withAgentTransaction } from "../database/transactions.js";
import {
  type ClientRepresentationError,
  encodeCanonical,
  sameBytes,
} from "../encoding/canonical-json.js";
import {
  type AgentPublicKey as agentAgentPublicKey,
  agentCardOf,
  AgentIdentityCard,
  type AgentName,
  type VerifiedAgentIdentityCard,
} from "../identity/agent-cards.js";
import { Registry } from "../identity/registry.js";
import { makeClientServer } from "./client-server/delivery.js";
import { makeRequests } from "./collectives.js";
import {
  type EndpointEngine,
  type EngineInitializationError,
  type EngineOutboundError,
  makeEndpointEngine,
} from "./conversations/conversation.js";
import {
  readStoredHistory,
  verifyStoredMembership,
} from "./conversations/history.js";
import {
  checkPublishedRecords,
  readMemberships,
  readRecentConversation,
  readUnpreparedMessages,
} from "./conversations/store.js";
import {
  CommunicationError,
  isTerminalSendFailure,
  SendError,
} from "./errors.js";
import { messageHistory } from "./history.js";
import {
  compareAgentIds,
  Message,
  messageInterpretation,
  messageMembers,
  type MessageReference,
  type RequestError,
} from "./messages.js";
import { bindCommunicationMode } from "./mode.js";
import {
  type DecodedOuterBody,
  decodeOuterBody,
  deriveConversationId,
  type OuterBodyReader,
  RecordHash,
} from "./records.js";
import { Router } from "./router/client.js";
import {
  makeRouterWorker,
  type RouterWorker,
  type RouterWorkerInput,
  RouterWorkerPayloadInvalidError,
} from "./router/connection.js";
import {
  type EndpointStoreError,
  failMessage,
  initializeCommunication,
  readAcceptedMessage,
  acceptMessage as storedAcceptMessage,
  type StoredMessage,
} from "./store.js";

/** Semantic handoffs are shared by every delivery mode. */
export {
  type Conversation,
  type ConversationQuery,
  type HistoryPage,
  type HistoryQuery,
  Incoming,
  Outgoing,
} from "./contract.js";

/** Owns durable message delivery, request completion and verified history. */
export class Communication extends Context.Tag("SocialHarness/Communication")<
  Communication,
  CommunicationService
>() {
  /** Configuration is checked before the daemon serves or restores any agent. */
  static configureTransport(
    transport:
      | { readonly kind: "p2p" }
      | { readonly kind: "client-server"; readonly serverAgentName: AgentName },
  ) {
    return bindCommunicationMode(
      transport.kind,
      transport.kind === "client-server" ? transport.serverAgentName : "",
    );
  }

  /**
   * Runs delivery and request workers until the layer's scope closes. Reopen
   * the same SQL store with the same identity and key to resume accepted work.
   */
  static layer(settings: CommunicationSettings) {
    return Layer.scoped(Communication, makeCommunicationLayer(settings));
  }

  /** Direct client/server delivery shares message storage with peer communication. */
  static clientServerLayer(
    settings: CommunicationSettings & { readonly serverAgentName: AgentName },
  ) {
    return Layer.scoped(Communication, makeClientServer(settings));
  }
}

interface Environment {
  readonly settings: CommunicationSettings;
  readonly sql: SqlClient;
  readonly registry: Context.Tag.Service<typeof Registry>;
  readonly router: Context.Tag.Service<typeof Router>;
  readonly failure: Deferred.Deferred<never, CommunicationError>;
  readonly scope: Scope.Scope;
  readonly ready: Deferred.Deferred<EndpointEngine>;
  readonly wake: Queue.Queue<undefined>;
  readonly consumer: Effect.Semaphore;
  readonly faults: Deferred.Deferred<never, EndpointStoreError>;
}

type Requests = Effect.Effect.Success<ReturnType<typeof makeRequests>>;

type WorkerFailure =
  | EngineInitializationError
  | EngineOutboundError
  | Effect.Effect.Error<RouterWorker["run"]>
  | EndpointStoreError
  | ClientRepresentationError
  | ParseResult.ParseError
  | CommunicationError;

const workerFailureReasons = {
  CommunicationError: "persistence-failed",
  EndpointStoreError: "persistence-failed",
  ClientRepresentationError: "persistence-failed",
  ParseError: "persistence-failed",
  RouterWorkerAuthenticationError: "network-unavailable",
  RouterWorkerPersistenceError: "persistence-failed",
  RouterWorkerTransportError: "network-unavailable",
  RouterWorkerProtocolError: "persistence-failed",
  RouterWorkerRecoveryError: "network-unavailable",
  RouterWorkerDiscontinuityError: "network-unavailable",
  RouterWorkerUnavailableError: "network-unavailable",
} as const;

const initializationFailureReasons = {
  identity: "persistence-failed",
  persistence: "persistence-failed",
  representation: "persistence-failed",
  "outbox-too-old": "outbox-too-old",
} as const satisfies Readonly<
  Record<EngineInitializationError["reason"], CommunicationError["reason"]>
>;

/** Caller failures distinguish refusal from retained or uncertain progress. */
export { CommunicationError } from "./errors.js";
/** Messages and requests retain the same semantic values at every service handoff. */
export {
  AcceptResponse,
  Answer,
  AnswerOutcome,
  CollectiveId,
  DeadlineSeconds,
  DeclineResponse,
  decodeCollectiveFailure,
  Message,
  MessageReference,
  PaginatedRequest,
  PaginatedResult,
  ReceivedRequest,
  Request,
  RequestedSchema,
  RequestError,
  RequestOperation,
  ResultSources,
} from "./messages.js";

/** Construction captures one registered identity and owns all worker resources. */
function makeCommunicationLayer(settings: CommunicationSettings) {
  const agentKey = settings.agentKey;

  return Effect.gen(function* () {
    yield* bindCommunicationMode("p2p");
    yield* initializeCommunication(agentKey, settings.identityCard).pipe(
      Effect.catchTag(
        "EndpointStoreError",
        () => new CommunicationError({ reason: "persistence-failed" }),
      ),
    );
    const workers = yield* FiberSet.make<never, CommunicationError>();
    const environment: Environment = {
      settings,
      sql: yield* SqlClient,
      registry: yield* Registry,
      router: yield* Router,
      scope: yield* Scope.Scope,
      ready: yield* Deferred.make<EndpointEngine>(),
      wake: yield* Queue.sliding<undefined>(1),
      faults: yield* Deferred.make<never, EndpointStoreError>(),
      failure: yield* Deferred.make<never, CommunicationError>(),
      consumer: yield* Effect.makeSemaphore(1),
    };
    yield* acquireCommunication(environment, settings.identityCard, workers);
    const provideSql = Effect.provideService(SqlClient, environment.sql);
    const requests = yield* makeRequests(agentKey, {
      self: agentCardOf(settings.identityCard),
      acceptMessage: (message) =>
        acceptMessage(environment, message).pipe(provideSql),
    });
    yield* supervise(workers, Deferred.await(environment.faults), environment);
    yield* supervise(
      workers,
      prepareAcceptedMessages(environment),
      environment,
    );
    const capture = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
      effect.pipe(
        provideSql,
        Effect.raceFirst(Deferred.await(environment.failure)),
      );
    const history = messageHistory({
      agentKey,
      check: checkPublishedRecords(agentKey).pipe(mapHistoryErrors),
      local: settings.identityCard,
      maximumHistoryRecords: settings.maximumHistoryRecords ?? 1024,
      verify: (rows) =>
        verifyPublishedMessages(
          agentKey,
          settings.registrySignerPublicKey,
          rows,
        ),
    });
    return {
      send: (value) => capture(acceptOutgoing(environment, requests, value)),
      getRequest: (operationId) => capture(requests.getRequest(operationId)),
      waitForMessage: (reference) =>
        capture(waitForMessage(environment, reference)),
      consume: (handle) =>
        environment.consumer.withPermits(1)(
          capture(consumeIncoming(environment, requests, handle)).pipe(
            Effect.raceFirst(requests.failed),
          ),
        ),
      getConversation: (names) => capture(history.getConversation(names)),
      listConversations: (query = {}) =>
        capture(history.listConversations(query)),
      readHistory: (query) => capture(history.readHistory(query)),
    } satisfies Context.Tag.Service<typeof Communication>;
  }).pipe(
    Effect.mapError(workerFailure),
    Effect.withSpan("Communication.layer"),
  );
}

function acceptOutgoing(
  environment: Environment,
  requests: Requests,
  value: Outgoing,
) {
  if ("kind" in value) {
    return requests.accept(value);
  }
  return Effect.gen(function* () {
    const decoded = yield* messageInterpretation(value).pipe(
      Effect.catchTag("CollectivePartInvalidError", () =>
        Effect.succeed(Option.none()),
      ),
    );
    return yield* Option.isSome(decoded) && decoded.value.kind === "response"
      ? requests.answer({ ...value, operation: decoded.value })
      : acceptMessage(environment, value);
  });
}

function consumeIncoming<E, R>(
  environment: Environment,
  requests: Requests,
  handle: (incoming: Incoming) => Effect.Effect<void, E, R>,
): Effect.Effect<never, CommunicationError | RequestError | E, R | SqlClient> {
  return Effect.gen(function* () {
    const engine = yield* Deferred.await(environment.ready);
    while (true) {
      const pending = yield* engine.readPendingMessages().pipe(
        Effect.catchTags({
          EndpointStoreError: () =>
            new CommunicationError({ reason: "persistence-failed" }),
          ClientRepresentationError: () =>
            new CommunicationError({ reason: "persistence-failed" }),
          ParseError: () =>
            new CommunicationError({ reason: "persistence-failed" }),
          RouterWorkerPersistenceError: () =>
            new CommunicationError({ reason: "persistence-failed" }),
        }),
      );
      for (const delivery of pending) {
        const classified = yield* requests.receive(delivery.message);
        if (Option.isSome(classified)) {
          const item = classified.value;
          yield* handle(item);
        }
        yield* engine
          .acknowledgeMessage(delivery.recordHash)
          .pipe(
            Effect.catchTag(
              "EndpointStoreError",
              () => new CommunicationError({ reason: "persistence-failed" }),
            ),
          );
      }
      while (
        yield* requests
          .deliverNextResult((delivery) =>
            delivery.item.kind === "result"
              ? handle({ ...delivery.item, sources: delivery.sources })
              : handle(delivery.item),
          )
          .pipe(
            Effect.catchTag(
              "EndpointStoreError",
              () => new CommunicationError({ reason: "persistence-failed" }),
            ),
          )
      ) {
        continue;
      }
      yield* Queue.take(environment.wake).pipe(
        Effect.timeoutOption("100 millis"),
      );
    }
  });
}

function acceptMessage(environment: Environment, message: Message) {
  const agentKey = environment.settings.agentKey;

  return Effect.gen(function* () {
    const local = agentCardOf(environment.settings.identityCard);
    if (
      message.author.agentId !== local.agentId ||
      message.author.agentName !== local.agentName ||
      message.author.principalId !== local.principalId
    ) {
      return yield* new CommunicationError({ reason: "membership-invalid" });
    }
    const ids = messageMembers(message)
      .map((member) => member.agentId)
      .sort(compareAgentIds);
    const [first, second, ...rest] = ids;
    if (first === undefined || second === undefined) {
      return yield* new CommunicationError({ reason: "membership-invalid" });
    }
    const conversationId = yield* deriveConversationId([
      first,
      second,
      ...rest,
    ]);
    yield* storedAcceptMessage(agentKey, {
      authorAgentId: message.author.agentId,
      msgId: message.msgId,
      conversationId,
      canonicalMessage: yield* encodeCanonical(Message, message),
    });
  }).pipe(
    Effect.catchTag(
      "ClientRepresentationError",
      () => new CommunicationError({ reason: "content-invalid" }),
    ),
    Effect.catchTag(
      "EndpointStoreError",
      (error) =>
        new CommunicationError({
          reason:
            error.reason === "conflict"
              ? "message-conflict"
              : "persistence-failed",
        }),
    ),
  );
}

function waitForMessage(environment: Environment, reference: MessageReference) {
  const agentKey = environment.settings.agentKey;

  return Effect.gen(function* () {
    const card = environment.settings.identityCard;
    if (reference.authorAgentId !== card.agentId) {
      return yield* new CommunicationError({ reason: "unknown-message" });
    }
    while (true) {
      const retained = yield* readAcceptedMessage(agentKey, reference).pipe(
        Effect.catchTag(
          "EndpointStoreError",
          () => new CommunicationError({ reason: "persistence-failed" }),
        ),
      );
      if (retained === undefined) {
        return yield* new CommunicationError({ reason: "unknown-message" });
      }
      if (retained.failure !== undefined) {
        return yield* sendFailure(new SendError({ reason: retained.failure }));
      }
      if (retained.completed) {
        yield* Deferred.await(environment.ready);
        return retained.message;
      }
      yield* Effect.sleep(Duration.millis(100));
    }
  });
}

function prepareAcceptedMessages(environment: Environment) {
  const agentKey = environment.settings.agentKey;

  return Effect.gen(function* () {
    const engine = yield* Deferred.await(environment.ready);
    for (const accepted of yield* readUnpreparedMessages(agentKey)) {
      const { message } = accepted;
      yield* engine.prepareMessage(accepted).pipe(
        Effect.catchTag("SendError", (error) =>
          isTerminalSendFailure(error.reason)
            ? failMessage(
                agentKey,
                {
                  authorAgentId: message.author.agentId,
                  msgId: message.msgId,
                },
                error.reason,
              )
            : Effect.void,
        ),
      );
    }
  }).pipe(
    Effect.provideService(SqlClient, environment.sql),
    Effect.repeat(Schedule.spaced(Duration.millis(200))),
    Effect.zipRight(Effect.never),
  );
}

function sendFailure(error: SendError) {
  switch (error.reason) {
    case "delivery-pending":
      return new CommunicationError({ reason: "certification-unavailable" });
    case "invalid-address":
    case "unknown-agent":
      return new CommunicationError({ reason: "membership-invalid" });
    case "content-invalid":
    case "membership-invalid":
    case "version-mismatch":
    case "certification-unavailable":
    case "persistence-failed":
    case "network-unavailable":
    case "outcome-unknown":
    case "message-conflict":
      return new CommunicationError({ reason: error.reason });
    default:
      return error.reason satisfies never;
  }
}

function acquireCommunication(
  environment: Environment,
  card: VerifiedAgentIdentityCard,
  workers: FiberSet.FiberSet<never, CommunicationError>,
) {
  return Effect.gen(function* () {
    const engineReady = yield* Deferred.make<EndpointEngine>();
    const pending = Queue.offer(environment.wake, undefined).pipe(
      Effect.asVoid,
    );
    const worker = yield* makeRouterWorker({
      agentKey: environment.settings.agentKey,
      callerAgentId: card.agentId,
      signingAuthority: environment.settings.signingAuthority,
      pinnedSenderCards: yield* pinnedSenderCards(environment, card),
      callbacks: workerCallbacks(
        {
          identityCard: card,
          signingAuthority: environment.settings.signingAuthority,
        },
        Deferred.await(engineReady),
        pending,
      ),
    }).pipe(
      Effect.provideService(SqlClient, environment.sql),
      Effect.provideService(Registry, environment.registry),
      Effect.provideService(Router, environment.router),
    );
    const engine = yield* makeEndpointEngine({
      agentKey: environment.settings.agentKey,
      localAgentCard: card,
      signingAuthority: environment.settings.signingAuthority,
      registrySignerPublicKey: environment.settings.registrySignerPublicKey,
      registry: environment.registry,
      routerWorker: worker,
      reportStorageFault: (error) =>
        Deferred.fail(environment.faults, error).pipe(Effect.asVoid),
    }).pipe(
      Effect.provideService(SqlClient, environment.sql),
      Scope.extend(environment.scope),
    );
    yield* Deferred.succeed(engineReady, engine);
    yield* Deferred.succeed(environment.ready, engine);
    yield* supervise(workers, worker.run, environment);
    yield* supervise(workers, engine.runOutbound, environment);
    yield* pending;
  }).pipe(Scope.extend(environment.scope));
}

function workerCallbacks(
  reader: OuterBodyReader,
  engine: Effect.Effect<EndpointEngine>,
  wake: Effect.Effect<void>,
): RouterWorkerInput<DecodedOuterBody>["callbacks"] {
  return {
    decodePayload: (message) =>
      decodeOuterBody({ ...reader, message }).pipe(
        Effect.catchTag(
          "ClientRepresentationError",
          () => new RouterWorkerPayloadInvalidError(),
        ),
      ),
    acceptPayload: (ingress) =>
      engine.pipe(
        Effect.flatMap((active) => active.acceptRouterIngress(ingress)),
        Effect.tap(() => wake),
      ),
    abandonVolatileFolds: (reason) =>
      engine.pipe(
        Effect.flatMap((active) => active.abandonVolatileFolds(reason)),
      ),
    recoverCertifiedHistory: (recovery) =>
      engine.pipe(
        Effect.flatMap((active) => active.recoverCertifiedHistory(recovery)),
        Effect.tap(() => wake),
      ),
    reattached: () =>
      engine.pipe(Effect.flatMap((active) => active.rearmCatchUp)),
  };
}

function pinnedSenderCards(
  environment: Environment,
  local: VerifiedAgentIdentityCard,
) {
  const agentKey = environment.settings.agentKey;

  return Effect.gen(function* () {
    const memberships = yield* readMemberships(agentKey).pipe(
      Effect.provideService(SqlClient, environment.sql),
    );
    const cards = new Map([[local.agentId, local]]);
    for (const stored of memberships) {
      const membership = yield* verifyStoredMembership(
        stored,
        environment.settings.registrySignerPublicKey,
      );
      yield* Effect.forEach(
        membership.members,
        (card) => retainPinnedCard(cards, card),
        { concurrency: 1, discard: true },
      );
    }
    return [...cards.values()];
  });
}

function retainPinnedCard(
  cards: Map<VerifiedAgentIdentityCard["agentId"], VerifiedAgentIdentityCard>,
  card: VerifiedAgentIdentityCard,
) {
  return Effect.gen(function* () {
    const existing = cards.get(card.agentId);
    if (existing !== undefined) {
      const bytes = yield* Effect.all([
        encodeCanonical(AgentIdentityCard, existing),
        encodeCanonical(AgentIdentityCard, card),
      ]);
      if (!sameBytes(bytes[0], bytes[1])) {
        return yield* new CommunicationError({ reason: "membership-invalid" });
      }
    }
    cards.set(card.agentId, card);
  });
}

function supervise(
  workers: FiberSet.FiberSet<never, CommunicationError>,
  worker: Effect.Effect<never, WorkerFailure>,
  environment: Environment,
) {
  return Effect.forkScoped(
    Effect.interruptible(
      worker.pipe(
        Effect.mapError(workerFailure),
        Effect.tapError((error) => Deferred.fail(environment.failure, error)),
      ),
    ),
  ).pipe(
    Effect.flatMap((fiber) => FiberSet.add(workers, fiber)),
    Effect.asVoid,
  );
}

function workerFailure(error: WorkerFailure): CommunicationError {
  if (error._tag === "EngineInitializationError") {
    return new CommunicationError({
      reason: initializationFailureReasons[error.reason],
    });
  }
  if (error._tag === "EngineOutboundError") {
    return new CommunicationError({
      reason:
        error.reason === "network"
          ? "network-unavailable"
          : "persistence-failed",
    });
  }
  if (error._tag === "RouterWorkerRejectedError") {
    return new CommunicationError({
      reason:
        error.reason === "authentication"
          ? "agent-not-admitted"
          : "network-unavailable",
    });
  }
  return new CommunicationError({ reason: workerFailureReasons[error._tag] });
}

function boundaryRecord(
  agentKey: string,
  conversationId: string,
  reference: MessageReference,
) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient;
    const [row] = yield* withAgentTransaction(
      agentKey,
      sql<Readonly<Record<string, unknown>>>`
      SELECT staged.record_hash FROM staged_records AS staged JOIN certified_records AS certified
        ON certified.agent_key=staged.agent_key AND certified.conversation_id=staged.conversation_id AND certified.record_hash=staged.record_hash
      WHERE staged.agent_key=${agentKey} AND staged.conversation_id=${conversationId} AND staged.author_agent_id=${reference.authorAgentId} AND staged.msg_id=${reference.msgId}`,
    );
    if (row === undefined) {
      return yield* historyGap();
    }
    return yield* Schema.decodeUnknown(RecordHash)(row.record_hash);
  });
}

function mapHistoryErrors<A, E, R>(effect: Effect.Effect<A, E, R>) {
  return effect.pipe(
    Effect.catchAll((error) => {
      if (error instanceof CommunicationError) {
        return Effect.fail(error);
      }
      if (
        typeof error === "object" &&
        error !== null &&
        "_tag" in error &&
        error._tag === "EndpointStoreError"
      ) {
        const reason = "reason" in error ? error.reason : undefined;
        if (reason === "invalid-continuation") {
          return Effect.fail(invalidCursor());
        }
        if (reason === "persistence" || reason === "closed") {
          return Effect.fail(
            new CommunicationError({ reason: "persistence-failed" }),
          );
        }
        return Effect.fail(historyGap());
      }
      if (
        typeof error === "object" &&
        error !== null &&
        "_tag" in error &&
        error._tag === "SqlError"
      ) {
        return Effect.fail(
          new CommunicationError({ reason: "persistence-failed" }),
        );
      }
      return Effect.fail(historyGap());
    }),
  );
}

function invalidCursor() {
  return new CommunicationError({ reason: "invalid-cursor" });
}

function historyGap() {
  return new CommunicationError({ reason: "history-gap" });
}

/** A valid certificate must certify the exact shared message and its retained position. */
function verifyPublishedMessages(
  agentKey: string,
  signer: agentAgentPublicKey,
  rows: readonly StoredMessage[],
) {
  return Effect.forEach(
    rows,
    (row) =>
      Effect.gen(function* () {
        const hash = yield* boundaryRecord(agentKey, row.conversationId, {
          authorAgentId: row.message.author.agentId,
          msgId: row.message.msgId,
        });
        const sql = yield* SqlClient;
        const [position] = yield* withAgentTransaction(
          agentKey,
          sql<{
            ordinal: number;
          }>`SELECT ordinal FROM certified_records WHERE agent_key=${agentKey} AND conversation_id=${row.conversationId} AND record_hash=${hash}`,
        );
        const snapshot = yield* readRecentConversation(agentKey, {
          conversationId: row.conversationId,
          maximumRecords: 1,
          candidateRecordHash: hash,
        });
        const verified = yield* readStoredHistory(
          signer,
          snapshot,
          snapshot.records,
        );
        const record = verified.find((item) => item.recordHash === hash);
        if (
          record === undefined ||
          position?.ordinal !== row.ordinal ||
          !sameBytes(
            yield* encodeCanonical(Message, record.message),
            yield* encodeCanonical(Message, row.message),
          )
        ) {
          return yield* historyGap();
        }
      }),
    { concurrency: 1, discard: true },
  ).pipe(mapHistoryErrors);
}
