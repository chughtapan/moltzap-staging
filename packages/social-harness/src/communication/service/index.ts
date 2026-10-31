/** @file Registration activates one scoped communication engine and its supervised Router workers. */

import { SqlClient } from "@effect/sql";
import {
  type Context,
  Deferred,
  Duration,
  Effect,
  FiberSet,
  Layer,
  Option,
  type ParseResult,
  Queue,
  Ref,
  Schedule,
  Schema,
  Scope,
  Stream,
} from "effect";
import type {
  AgentSigningAuthority,
  Ed25519PublicKey,
} from "../../identity/keys.js";
import {
  type ClientRepresentationError,
  encodeCanonical,
  sameBytes,
} from "../../encoding/canonical-json.js";
import {
  AgentCard,
  agentViewOf,
  type VerifiedAgentCard,
} from "../../identity/cards.js";
import { LocalIdentity } from "../../identity/local.js";
import { Registry } from "../../identity/registry/index.js";
import {
  isTerminalSendFailure,
  SendError,
  sendFailureReasons,
} from "../conversations/errors.js";
import {
  type EndpointEngine,
  type EngineInitializationError,
  type EngineOutboundError,
  makeEndpointEngine,
  verifyStoredMembership,
} from "../conversations/index.js";
import { Communication, CommunicationError } from "../index.js";
import {
  type Message,
  messageMembers,
  type MessageReference,
  Message as MessageSchema,
} from "../message.js";
import {
  type DecodedOuterBody,
  decodeOuterBody,
  deriveConversationId,
  type OuterBodyReader,
} from "../records/index.js";
import { compareAgentIds } from "../records/values.js";
import {
  makeRouterWorker,
  Router,
  type RouterWorker,
  type RouterWorkerInput,
  RouterWorkerPayloadInvalidError,
} from "../router/index.js";
import * as Store from "../store/index.js";
import { listConversations, readHistory } from "./history.js";

/** Signing and Registry authority belong to the one configured local agent. */
export interface CommunicationSettings {
  readonly signingAuthority: AgentSigningAuthority;
  readonly registrySignerPublicKey: Ed25519PublicKey;
}

interface Environment {
  readonly settings: CommunicationSettings;
  readonly sql: SqlClient.SqlClient;
  readonly registry: Context.Tag.Service<typeof Registry>;
  readonly router: Context.Tag.Service<typeof Router>;
  readonly identity: Context.Tag.Service<typeof LocalIdentity>;
  readonly scope: Scope.Scope;
  readonly ready: Deferred.Deferred<EndpointEngine>;
  readonly wake: Queue.Queue<undefined>;
  readonly consumer: Effect.Semaphore;
  readonly faults: Deferred.Deferred<never, Store.EndpointStoreError>;
}

/** Services are available before registration; their workers share the process's failure supervision. */
export function communicationLive(
  settings: CommunicationSettings,
  workers: FiberSet.FiberSet<never>,
) {
  return Layer.scoped(
    Communication,
    Effect.gen(function* () {
      const environment: Environment = {
        settings,
        sql: yield* SqlClient.SqlClient,
        registry: yield* Registry,
        router: yield* Router,
        identity: yield* LocalIdentity,
        scope: yield* Scope.Scope,
        ready: yield* Deferred.make<EndpointEngine>(),
        wake: yield* Queue.sliding<undefined>(1),
        faults: yield* Deferred.make<never, Store.EndpointStoreError>(),
        consumer: yield* Effect.makeSemaphore(1),
      };
      yield* supervise(workers, Deferred.await(environment.faults));
      yield* supervise(
        workers,
        activateAfterRegistration(environment, workers).pipe(
          Effect.zipRight(Effect.never),
        ),
      );
      yield* supervise(workers, prepareAcceptedMessages(environment));
      return makeCommunication(environment);
    }).pipe(Effect.withSpan("communicationLive")),
  );
}

function makeCommunication(
  environment: Environment,
): Context.Tag.Service<typeof Communication> {
  const provideSql = Effect.provideService(
    SqlClient.SqlClient,
    environment.sql,
  );
  return {
    send: (message) => acceptMessage(environment, message).pipe(provideSql),
    waitForMessage: (message) =>
      waitForMessage(environment, message).pipe(provideSql),
    consume: (handle) => consumeIncoming(environment, handle),
    listConversations: (query) =>
      registeredCard(environment).pipe(
        Effect.flatMap((card) =>
          listConversations(
            query,
            card,
            environment.settings.registrySignerPublicKey,
          ),
        ),
        provideSql,
      ),
    readHistory: (query) =>
      registeredCard(environment).pipe(
        Effect.zipRight(
          readHistory(query, environment.settings.registrySignerPublicKey),
        ),
        provideSql,
      ),
  };
}

function consumeIncoming<E, R>(
  environment: Environment,
  handle: (message: Message) => Effect.Effect<void, E, R>,
) {
  return environment.consumer.withPermits(1)(
    Deferred.await(environment.ready).pipe(
      Effect.flatMap((engine) =>
        Queue.offer(environment.wake, undefined).pipe(
          Effect.zipRight(consumeMessages(engine, environment.wake, handle)),
        ),
      ),
    ),
  );
}

function registeredCard(environment: Environment) {
  return Ref.get(environment.identity).pipe(
    Effect.flatMap((card) =>
      Option.isSome(card)
        ? Effect.succeed(card.value)
        : new CommunicationError({ reason: "not-registered" }),
    ),
  );
}

function acceptMessage(environment: Environment, message: Message) {
  return Effect.gen(function* () {
    const local = agentViewOf(yield* registeredCard(environment));
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
    yield* Store.acceptMessage({
      authorAgentId: message.author.agentId,
      msgId: message.msgId,
      conversationId,
      canonicalMessage: yield* encodeCanonical(MessageSchema, message),
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
  return Effect.gen(function* () {
    const card = yield* registeredCard(environment);
    if (reference.authorAgentId !== card.agentId) {
      return yield* new CommunicationError({ reason: "unknown-message" });
    }
    while (true) {
      const retained = yield* Store.readAcceptedMessage(reference).pipe(
        Effect.catchTag(
          "EndpointStoreError",
          () => new CommunicationError({ reason: "persistence-failed" }),
        ),
      );
      if (retained === undefined) {
        return yield* new CommunicationError({ reason: "unknown-message" });
      }
      if (retained.failure !== undefined) {
        const reason = yield* Schema.decodeUnknown(
          Schema.Literal(...sendFailureReasons),
        )(retained.failure).pipe(
          Effect.catchTag(
            "ParseError",
            () => new CommunicationError({ reason: "persistence-failed" }),
          ),
        );
        return yield* sendFailure(new SendError({ reason }));
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
  return Effect.gen(function* () {
    const engine = yield* Deferred.await(environment.ready);
    for (const accepted of yield* Store.readUnpreparedMessages()) {
      const { message } = accepted;
      yield* engine.prepareMessage(accepted).pipe(
        Effect.catchTag("SendError", (error) =>
          isTerminalSendFailure(error.reason)
            ? Store.failMessage(
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
    Effect.provideService(SqlClient.SqlClient, environment.sql),
    Effect.repeat(Schedule.spaced(Duration.millis(200))),
    Effect.zipRight(Effect.never),
  );
}

function sendFailure(error: SendError) {
  return new CommunicationError({
    reason:
      error.reason === "delivery-pending"
        ? "certification-unavailable"
        : error.reason,
  });
}

function activateAfterRegistration(
  environment: Environment,
  workers: FiberSet.FiberSet<never>,
) {
  return environment.identity.changes.pipe(
    Stream.filterMap((card) => card),
    Stream.take(1),
    Stream.runHead,
    Effect.flatMap((card) =>
      Option.isSome(card)
        ? acquireCommunication(environment, card.value, workers)
        : Effect.interrupt,
    ),
  );
}

function acquireCommunication(
  environment: Environment,
  card: VerifiedAgentCard,
  workers: FiberSet.FiberSet<never>,
) {
  return Effect.gen(function* () {
    const engineReady = yield* Deferred.make<EndpointEngine>();
    const pending = Queue.offer(environment.wake, undefined).pipe(
      Effect.asVoid,
    );
    const worker = yield* makeRouterWorker({
      callerAgentId: card.agentId,
      callerAgentCard: card,
      signingAuthority: environment.settings.signingAuthority,
      pinnedSenderCards: yield* pinnedSenderCards(environment, card),
      callbacks: workerCallbacks(
        {
          agentCard: card,
          signingAuthority: environment.settings.signingAuthority,
        },
        Deferred.await(engineReady),
        pending,
      ),
    }).pipe(
      Effect.provideService(SqlClient.SqlClient, environment.sql),
      Effect.provideService(Registry, environment.registry),
      Effect.provideService(Router, environment.router),
    );
    const engine = yield* makeEndpointEngine({
      localAgentCard: card,
      signingAuthority: environment.settings.signingAuthority,
      registrySignerPublicKey: environment.settings.registrySignerPublicKey,
      registry: environment.registry,
      routerWorker: worker,
      reportStorageFault: (error) =>
        Deferred.fail(environment.faults, error).pipe(Effect.asVoid),
    }).pipe(
      Effect.provideService(SqlClient.SqlClient, environment.sql),
      Scope.extend(environment.scope),
    );
    yield* Deferred.succeed(engineReady, engine);
    yield* Deferred.succeed(environment.ready, engine);
    yield* supervise(workers, worker.run);
    yield* supervise(workers, engine.runOutbound);
    yield* pending;
  }).pipe(Scope.extend(environment.scope));
}

function workerCallbacks(
  reader: OuterBodyReader,
  engine: Effect.Effect<EndpointEngine>,
  wake: Effect.Effect<void>,
): RouterWorkerInput<DecodedOuterBody>["callbacks"] {
  return {
    pinSenderCard: () => engine.pipe(Effect.asVoid),
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

function pinnedSenderCards(environment: Environment, local: VerifiedAgentCard) {
  return Effect.gen(function* () {
    const memberships = yield* Store.readMemberships().pipe(
      Effect.provideService(SqlClient.SqlClient, environment.sql),
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
  cards: Map<VerifiedAgentCard["agentId"], VerifiedAgentCard>,
  card: VerifiedAgentCard,
) {
  return Effect.gen(function* () {
    const existing = cards.get(card.agentId);
    if (existing !== undefined) {
      const bytes = yield* Effect.all([
        encodeCanonical(AgentCard, existing),
        encodeCanonical(AgentCard, card),
      ]);
      if (!sameBytes(bytes[0], bytes[1])) {
        return yield* new CommunicationError({ reason: "membership-invalid" });
      }
    }
    cards.set(card.agentId, card);
  });
}

type WorkerFailure =
  | EngineInitializationError
  | EngineOutboundError
  | Effect.Effect.Error<RouterWorker["run"]>
  | Store.EndpointStoreError
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

function supervise(
  workers: FiberSet.FiberSet<never>,
  worker: Effect.Effect<never, WorkerFailure>,
) {
  return Effect.forkScoped(
    Effect.interruptible(worker.pipe(Effect.mapError(workerFailure))),
  ).pipe(
    Effect.flatMap((fiber) => FiberSet.add(workers, fiber)),
    Effect.asVoid,
  );
}

const initializationFailureReasons = {
  identity: "persistence-failed",
  persistence: "persistence-failed",
  representation: "persistence-failed",
  "outbox-too-old": "outbox-too-old",
} as const satisfies Readonly<
  Record<EngineInitializationError["reason"], CommunicationError["reason"]>
>;

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

/** Success means the handler durably handled the message before its communication handoff retires. */
function consumeMessages<E, R>(
  engine: EndpointEngine,
  wake: Queue.Queue<undefined>,
  handle: (message: Message) => Effect.Effect<void, E, R>,
): Effect.Effect<never, CommunicationError | E, R> {
  return Queue.take(wake).pipe(
    Effect.zipRight(
      Effect.suspend(() => engine.readPendingMessages()).pipe(
        Effect.catchTag(
          "ListenError",
          () => new CommunicationError({ reason: "persistence-failed" }),
        ),
        Effect.flatMap((pending) =>
          Effect.forEach(
            pending,
            (delivery) =>
              handle(delivery.message).pipe(
                Effect.zipRight(
                  engine.acknowledgeMessage(delivery.recordHash).pipe(
                    Effect.catchTag(
                      "DeliveryAcknowledgeError",
                      () =>
                        new CommunicationError({
                          reason: "persistence-failed",
                        }),
                    ),
                  ),
                ),
              ),
            { concurrency: 1, discard: true },
          ),
        ),
      ),
    ),
    Effect.forever,
    Effect.withSpan("Communication.consume"),
  );
}
