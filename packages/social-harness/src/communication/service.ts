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
import { randomBytes } from "node:crypto";
import type {
  AgentPublicKey,
  AgentSigningAuthority,
} from "../identity/keys.js";
import { withAgentTransaction } from "../database/transactions.js";
import {
  type ClientRepresentationError,
  decodeCanonical,
  encodeCanonical,
  exactStruct,
  sameBytes,
} from "../encoding/canonical-json.js";
import {
  compareAscii,
  decodeCanonicalBase64Url,
} from "../encoding/identifier.js";
import {
  type AgentPublicKey as agentAgentPublicKey,
  AgentCard,
  agentCardOf,
  AgentIdentityCard,
  type AgentName,
  type VerifiedAgentIdentityCard,
} from "../identity/agent-cards.js";
import { Registry } from "../identity/registry.js";
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
  CommunicationError,
  isTerminalSendFailure,
  SendError,
} from "./errors.js";
import {
  type CollectiveId,
  compareAgentIds,
  conversationNameKey,
  conversationSortKey,
  maximumMembers,
  Message,
  messageInterpretation,
  messageMembers,
  type MessageReference,
  type PaginatedRequest,
  type PaginatedResult,
  ReceivedRequest,
  Request,
  type RequestError,
  RequestResult,
  ResultSources,
} from "./messages.js";
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
  readStoredConversation as readConversation,
  readMemberships,
  readPendingMessages,
  readRecentConversation,
  readUnpreparedMessages,
  acceptMessage as storedAcceptMessage,
} from "./store.js";

/** All conversation members, in ascending agent-name order. */
export type Conversation = readonly [
  Message["author"],
  Message["author"],
  ...Array<Message["author"]>,
];
/** Omit cursor to begin a live participant traversal. */
export interface ConversationQuery extends PaginatedRequest {}
/** A missing nextCursor ends the live traversal. */
interface ConversationPage extends PaginatedResult {
  readonly conversations: readonly Conversation[];
}
/** Selects a paged forward traversal or one bounded recent window. */
export type HistoryQuery =
  | (PaginatedRequest & {
      readonly members: Conversation;
      readonly order?: "oldest";
      readonly afterMessage?: MessageReference;
    })
  | {
      readonly members: Conversation;
      readonly order: "newest";
      readonly limit: number;
      readonly beforeMessage?: MessageReference;
    };
/** Messages retain conversation order and count only their encoded value bytes. */
export interface HistoryPage extends PaginatedResult {
  readonly entries: ReadonlyArray<{
    readonly message: Message;
    readonly messageBytes: number;
  }>;
  readonly earlierMessagesOmitted: boolean;
  /**
   * This agent's accepted messages that the Router has not ordered yet, in the
   * order they were accepted. They are not in `entries`.
   */
  readonly pendingOutgoing: readonly Message[];
}
/* eslint-disable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Named Effect schemas and their decoded values share one public contract. */
/** Encodes the fixed semantic work that Communication accepts. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- The owning handoff grammar lets Main persist Message values through their encoder.
export const Outgoing = Schema.Union(Message, Request);
/** Final outgoing work is fixed before policy assessment. */
export type Outgoing = typeof Outgoing.Type;
/** Incoming work retains encoded Message values at storage boundaries. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- The owning handoff grammar lets Main persist Message values through their encoder.
export const Incoming = Schema.Union(
  exactStruct({ kind: Schema.Literal("message"), message: Message }),
  exactStruct({ kind: Schema.Literal("request"), message: ReceivedRequest }),
  exactStruct({
    kind: Schema.Literal("result"),
    result: RequestResult,
    sources: ResultSources,
  }),
  exactStruct({
    kind: Schema.Literal("request-failed"),
    request: Request,
    error: Schema.String,
  }),
);
/** Incoming work awaits local policy before the host receives it. */
export type Incoming = typeof Incoming.Type;

interface CommunicationService {
  /**
   * Returns after durable acceptance. Certification continues in the layer's
   * scope after caller cancellation. Retry the same `Message` or `Request`,
   * retaining its `msgId` or `operationId`.
   */
  readonly send: (
    value: Outgoing,
  ) => Effect.Effect<void, CommunicationError | RequestError>;
  /** Checks current answer eligibility without reserving the request. */
  readonly getRequest: (
    operationId: CollectiveId,
  ) => Effect.Effect<ReceivedRequest, CommunicationError | RequestError>;
  /**
   * Waits for a locally authored message to certify, or reports its retained
   * terminal failure. Unknown references fail with `unknown-message`.
   * Cancellation stops this wait and leaves accepted work running.
   */
  readonly waitForMessage: (
    reference: MessageReference,
  ) => Effect.Effect<Message, CommunicationError>;
  /**
   * Calls the handler for incoming work and retires the item after success.
   * Failed or interrupted handling leaves it pending. Failed retirement can
   * replay a successfully handled item, so handlers must tolerate repeats.
   */
  readonly consume: <E, R>(
    handle: (incoming: Incoming) => Effect.Effect<void, E, R>,
  ) => Effect.Effect<never, CommunicationError | RequestError | E, R>;
  /** Finds a retained certified conversation by all member names, including self. */
  readonly getConversation: (
    names: readonly AgentName[],
  ) => Effect.Effect<Conversation | undefined, CommunicationError>;
  /** Lists retained conversations. Cursors expire when this service instance ends. */
  readonly listConversations: (
    query?: ConversationQuery,
  ) => Effect.Effect<ConversationPage, CommunicationError>;
  /** Reads verified application messages. Continuation cursors require this service instance. */
  readonly readHistory: (
    query: HistoryQuery,
  ) => Effect.Effect<HistoryPage, CommunicationError>;
}

/* eslint-enable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Restore naming after schema/type pairs. */

/** Owns durable message delivery, request completion and verified history. */
export class Communication extends Context.Tag("SocialHarness/Communication")<
  Communication,
  CommunicationService
>() {
  /**
   * Runs delivery and request workers until the layer's scope closes. Reopen
   * the same SQL store with the same identity and key to resume accepted work.
   */
  static layer(settings: CommunicationSettings) {
    return Layer.scoped(Communication, makeCommunicationLayer(settings));
  }
}

/** Signing and Registry authority belong to the one configured local agent. */
interface CommunicationSettings {
  readonly agentKey: string;
  readonly identityCard: VerifiedAgentIdentityCard;
  readonly maximumHistoryRecords?: number;
  readonly signingAuthority: AgentSigningAuthority;
  readonly registrySignerPublicKey: AgentPublicKey;
}

interface Environment {
  readonly settings: CommunicationSettings;
  readonly sql: SqlClient;
  readonly registry: Context.Tag.Service<typeof Registry>;
  readonly router: Context.Tag.Service<typeof Router>;
  readonly session: string;
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

// safer-arch-ignore no-module-cycle: History imports only service contract types; the service supplies its implementation.

const conversationCursor = Schema.Struct({
  operation: Schema.Literal("conversations"),
  session: Schema.String,
  after: Schema.String,
});

const historyCursor = Schema.Struct({
  operation: Schema.Literal("history"),
  session: Schema.String,
  conversation: Schema.String,
  after: Schema.String,
  position: Schema.String,
});

const membershipRow = Schema.Struct({
  conversation_id: Schema.String,
  membership_hash: Schema.String,
  canonical_membership: Schema.Uint8ArrayFromSelf,
  sort_key: Schema.String,
});

const listConversations = Effect.fn("Communication.listConversations")(
  function* (
    agentKey: string,
    query: ConversationQuery,
    reader: {
      readonly local: VerifiedAgentIdentityCard;
      readonly signer: agentAgentPublicKey;
      readonly session: string;
    },
  ) {
    const { local, signer, session } = reader;
    let after = "";
    if (query.cursor !== undefined) {
      const cursor = yield* decodeCursor(conversationCursor, query.cursor);
      if (cursor.session !== session) {
        return yield* invalidCursor();
      }
      after = cursor.after;
    }
    const sql = yield* SqlClient;
    const rows = yield* withAgentTransaction(
      agentKey,
      sql<Readonly<Record<string, unknown>>>`
      SELECT membership.conversation_id,membership.membership_hash,membership.canonical_membership,membership.sort_key
      FROM memberships AS membership JOIN conversation_state AS state
        ON state.agent_key=membership.agent_key AND state.conversation_id=membership.conversation_id
      WHERE membership.agent_key=${agentKey} AND state.head_record_hash IS NOT NULL AND membership.sort_key > ${after}
      ORDER BY membership.sort_key LIMIT 51`,
    );
    const selected = rows.slice(0, 50);
    const conversations = yield* Effect.forEach(
      selected,
      (row) => conversationEntry(row, local, signer),
      { concurrency: 1 },
    );
    const last = selected.at(-1);
    const nextCursor =
      rows.length > 50 && last !== undefined
        ? yield* encodeCursor(conversationCursor)({
            operation: "conversations",
            session,
            after: yield* Schema.decodeUnknown(Schema.String)(last.sort_key),
          })
        : undefined;
    return {
      conversations,
      ...(nextCursor === undefined ? {} : { nextCursor }),
    } satisfies ConversationPage;
  },
  mapHistoryErrors,
);

const getConversation = Effect.fn("Communication.getConversation")(function* (
  agentKey: string,
  names: readonly AgentName[],
  local: VerifiedAgentIdentityCard,
  signer: agentAgentPublicKey,
) {
  if (
    names.length < 2 ||
    names.length > maximumMembers ||
    new Set(names).size !== names.length ||
    !names.includes(local.agentName)
  ) {
    return yield* new CommunicationError({ reason: "membership-invalid" });
  }
  const sql = yield* SqlClient;
  const [row] = yield* withAgentTransaction(
    agentKey,
    sql<Readonly<Record<string, unknown>>>`
    SELECT membership.conversation_id,membership.membership_hash,membership.canonical_membership,membership.sort_key
    FROM memberships AS membership JOIN conversation_state AS state
      ON state.agent_key=membership.agent_key AND state.conversation_id=membership.conversation_id
    WHERE membership.agent_key=${agentKey} AND state.head_record_hash IS NOT NULL AND membership.sort_key=${conversationNameKey(names, local.agentName)}`,
  );
  return row === undefined
    ? undefined
    : yield* conversationEntry(row, local, signer);
}, mapHistoryErrors);

/** History returns only application messages, preserving one fixed private-log boundary. */
const readHistory = Effect.fn("Communication.readHistory")(function* (
  agentKey: string,
  query: HistoryQuery,
  reader: {
    readonly local: VerifiedAgentIdentityCard;
    readonly signer: agentAgentPublicKey;
    readonly session: string;
    readonly maximumHistoryRecords: number;
  },
) {
  const { local, signer, session, maximumHistoryRecords } = reader;
  const members = query.members;
  const conversationId = yield* historyConversationId(
    query,
    local,
    maximumHistoryRecords,
  );
  const resume =
    query.order !== "newest" && query.cursor !== undefined
      ? yield* decodeCursor(historyCursor, query.cursor)
      : undefined;
  const after =
    query.order !== "newest" && query.afterMessage !== undefined
      ? `${query.afterMessage.authorAgentId}:${query.afterMessage.msgId}`
      : "";
  if (
    resume !== undefined &&
    (resume.session !== session ||
      resume.conversation !== conversationId ||
      resume.after !== after)
  ) {
    return yield* invalidCursor();
  }
  const [stored] = yield* readMemberships(agentKey, conversationId);
  if (stored === undefined) {
    if (query.order === "newest" && query.beforeMessage === undefined) {
      return {
        entries: [],
        earlierMessagesOmitted: false,
        pendingOutgoing: yield* readPendingMessages(agentKey, conversationId),
      };
    }
    return yield* historyGap();
  }
  const verified = yield* verifyStoredMembership(stored, signer);
  const byName = (left: AgentCard, right: AgentCard) =>
    compareAscii(left.agentName, right.agentName);
  if (
    !Schema.equivalence(Schema.Array(AgentCard))(
      [...members].sort(byName),
      verified.members.map(agentCardOf).sort(byName),
    )
  ) {
    return yield* new CommunicationError({ reason: "membership-invalid" });
  }
  const page =
    query.order === "newest"
      ? yield* readNewest(agentKey, query, conversationId, signer)
      : yield* readOldest(agentKey, query, {
          conversationId,
          signer,
          session,
          initialPosition: resume?.position,
        });
  return {
    ...page,
    pendingOutgoing: yield* readPendingMessages(agentKey, conversationId),
  } satisfies HistoryPage;
}, mapHistoryErrors);

/** Caller failures distinguish refusal from retained or uncertain progress. */
export { CommunicationError } from "./errors.js";
/** Messages and requests retain the same semantic values at every service handoff. */
export {
  AcceptResponse,
  Answer,
  AnswerContent,
  type AnswerInput,
  AnswerOutcome,
  CollectiveId,
  DeadlineSeconds,
  DeclineResponse,
  decodeCollectiveFailure,
  Message,
  MessageOperation,
  MessageReference,
  PaginatedRequest,
  PaginatedResult,
  ReceivedRequest,
  Request,
  RequestedSchema,
  RequestError,
  RequestOperation,
  RequestResult,
  ResultSources,
} from "./messages.js";

/** Construction captures one registered identity and owns all worker resources. */
function makeCommunicationLayer(settings: CommunicationSettings) {
  const agentKey = settings.agentKey;

  return Effect.gen(function* () {
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
      session: randomBytes(16).toString("base64url"),
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
      getConversation: (names) =>
        capture(
          getConversation(
            agentKey,
            names,
            settings.identityCard,
            settings.registrySignerPublicKey,
          ),
        ),
      listConversations: (query = {}) =>
        capture(
          listConversations(agentKey, query, {
            local: settings.identityCard,
            signer: settings.registrySignerPublicKey,
            session: environment.session,
          }),
        ),
      readHistory: (query) =>
        capture(
          readHistory(agentKey, query, {
            local: settings.identityCard,
            signer: settings.registrySignerPublicKey,
            session: environment.session,
            maximumHistoryRecords: settings.maximumHistoryRecords ?? 1024,
          }),
        ),
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
    case "not-registered":
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

function conversationEntry(
  row: Readonly<Record<string, unknown>>,
  local: VerifiedAgentIdentityCard,
  signer: agentAgentPublicKey,
): Effect.Effect<Conversation, CommunicationError> {
  return Effect.gen(function* () {
    const stored = yield* Schema.decodeUnknown(membershipRow)(row);
    const membership = yield* verifyStoredMembership(
      {
        conversationId: stored.conversation_id,
        membershipHash: stored.membership_hash,
        canonicalMembership: stored.canonical_membership,
      },
      signer,
    );
    const members = membership.members
      .map(agentCardOf)
      .sort((left, right) => compareAscii(left.agentName, right.agentName));
    if (
      !members.some((member) => member.agentId === local.agentId) ||
      conversationSortKey(members, local.agentId) !== stored.sort_key
    ) {
      return yield* historyGap();
    }
    const [first, second, ...rest] = members;
    if (first === undefined || second === undefined) {
      return yield* historyGap();
    }
    return [first, second, ...rest] satisfies Conversation;
  }).pipe(mapHistoryErrors);
}

/** Typed participants still need a coherent local selection and a bounded window. */
function historyConversationId(
  query: HistoryQuery,
  local: VerifiedAgentIdentityCard,
  maximumHistoryRecords: number,
) {
  return Effect.gen(function* () {
    const members = query.members;
    if (
      members.length < 2 ||
      members.length > maximumMembers ||
      new Set(members.map((member) => member.agentId)).size !==
        members.length ||
      new Set(members.map((member) => member.agentName)).size !== members.length
    ) {
      return yield* new CommunicationError({ reason: "membership-invalid" });
    }
    if (
      query.order === "newest" &&
      (!Number.isSafeInteger(query.limit) ||
        query.limit < 1 ||
        query.limit > maximumHistoryRecords)
    ) {
      return yield* new CommunicationError({ reason: "content-invalid" });
    }
    if (!members.some((member) => member.agentId === local.agentId)) {
      return yield* new CommunicationError({ reason: "membership-invalid" });
    }
    const ids = members.map((member) => member.agentId).sort(compareAgentIds);
    const [first, second, ...rest] = ids;
    if (first === undefined || second === undefined) {
      return yield* historyGap();
    }
    return yield* deriveConversationId([first, second, ...rest]);
  });
}

function readOldest(
  agentKey: string,
  query: Extract<HistoryQuery, { readonly order?: "oldest" }>,
  reader: {
    readonly conversationId: string;
    readonly signer: agentAgentPublicKey;
    readonly session: string;
    readonly initialPosition?: string;
  },
) {
  const { conversationId, signer, session, initialPosition } = reader;
  return Effect.gen(function* () {
    const after =
      query.afterMessage === undefined
        ? ""
        : `${query.afterMessage.authorAgentId}:${query.afterMessage.msgId}`;
    let position = initialPosition;
    const entries: Array<{
      readonly message: Message;
      readonly messageBytes: number;
    }> = [];
    let firstPage = true;
    while (firstPage || position !== undefined) {
      firstPage = false;
      const request =
        position === undefined
          ? {
              conversationId,
              ...(query.afterMessage === undefined
                ? {}
                : {
                    afterRecordHash: yield* boundaryRecord(
                      agentKey,
                      conversationId,
                      query.afterMessage,
                    ),
                  }),
              maximumRecords: 50 - entries.length,
            }
          : { continuation: position, maximumRecords: 50 - entries.length };
      const page = yield* readConversation(agentKey, request);
      const records = yield* readStoredHistory(signer, page, page.records);
      entries.push(...(yield* applicationEntries(records)));
      position = page.continuation ?? undefined;
      if (entries.length === 50 || position === undefined) {
        break;
      }
    }
    if (
      position !== undefined &&
      !(yield* hasLaterApplicationMessage(agentKey, position, signer))
    ) {
      position = undefined;
    }
    const nextCursor =
      position === undefined
        ? undefined
        : yield* encodeCursor(historyCursor)({
            operation: "history",
            session,
            conversation: conversationId,
            after,
            position,
          });
    return {
      entries,
      ...(nextCursor === undefined ? {} : { nextCursor }),
      earlierMessagesOmitted: query.afterMessage !== undefined,
    };
  });
}

/** A continuation is present only when an application message remains in its fixed window. */
function hasLaterApplicationMessage(
  agentKey: string,
  initialPosition: string,
  signer: agentAgentPublicKey,
) {
  return Effect.gen(function* () {
    let position: string | undefined = initialPosition;
    while (position !== undefined) {
      const page: Effect.Effect.Success<ReturnType<typeof readConversation>> =
        yield* readConversation(agentKey, { continuation: position });
      const records = yield* readStoredHistory(signer, page, page.records);
      for (const record of records) {
        if (yield* isApplicationMessage(record.message)) {
          return true;
        }
      }
      position = page.continuation ?? undefined;
    }
    return false;
  });
}

function readNewest(
  agentKey: string,
  query: Extract<HistoryQuery, { readonly order: "newest" }>,
  conversationId: string,
  signer: agentAgentPublicKey,
) {
  return Effect.gen(function* () {
    let before =
      query.beforeMessage === undefined
        ? undefined
        : yield* boundaryRecord(agentKey, conversationId, query.beforeMessage);
    const entries: Array<{
      readonly message: Message;
      readonly messageBytes: number;
    }> = [];
    while (entries.length <= query.limit) {
      const snapshot = yield* readRecentConversation(agentKey, {
        conversationId,
        maximumRecords: Math.min(50, query.limit + 1 - entries.length),
        ...(before === undefined ? {} : { candidateRecordHash: before }),
      });
      const records = yield* readStoredHistory(
        signer,
        snapshot,
        snapshot.records,
      );
      const boundary = before;
      const candidates =
        boundary === undefined
          ? records
          : records.filter((record) => record.recordHash !== boundary);
      if (candidates.length === 0) {
        break;
      }
      entries.push(...(yield* applicationEntries(candidates)));
      before = candidates.at(-1)?.recordHash;
    }
    return {
      entries: entries.slice(0, query.limit).reverse(),
      earlierMessagesOmitted: entries.length > query.limit,
    };
  });
}

function applicationEntries(
  records: ReadonlyArray<{ readonly message: Message }>,
) {
  return Effect.forEach(
    records,
    (record) =>
      Effect.gen(function* () {
        if (!(yield* isApplicationMessage(record.message))) {
          return [];
        }
        return [
          {
            message: record.message,
            messageBytes: (yield* encodeCanonical(Message, record.message))
              .byteLength,
          },
        ];
      }),
    { concurrency: 1 },
  ).pipe(Effect.map((entries) => entries.flat()));
}

/** Reserved close and malformed traffic stays in the private log. */
function isApplicationMessage(message: Message) {
  return messageInterpretation(message).pipe(
    Effect.match({
      onFailure: () => false,
      onSuccess: (value) =>
        Option.isNone(value) || value.value.kind !== "close",
    }),
  );
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

function decodeCursor<A, I>(schema: Schema.Schema<A, I>, cursor: string) {
  return Effect.fromNullable(decodeCanonicalBase64Url(cursor)).pipe(
    Effect.mapError(invalidCursor),
    Effect.flatMap((bytes) => decodeCanonical(schema, bytes)),
    Effect.mapError(invalidCursor),
  );
}

function encodeCursor<A, I>(schema: Schema.Schema<A, I>) {
  return (cursor: A) =>
    encodeCanonical(schema, cursor).pipe(
      Effect.map((bytes) => Buffer.from(bytes).toString("base64url")),
    );
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
