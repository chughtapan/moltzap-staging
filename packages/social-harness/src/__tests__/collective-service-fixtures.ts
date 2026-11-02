/** @file Collective service scenarios use real SQL and a Message-only communication boundary. */

import { SqlClient } from "@effect/sql/SqlClient";
import {
  Clock,
  type Context,
  Deferred,
  Effect,
  Exit,
  Queue,
  Schema,
  Scope,
} from "effect";
import type { AgentCard } from "../identity/agent.js";
import {
  type Communication,
  CommunicationError,
} from "../communication/index.js";
import {
  type Message,
  messageFromParticipants,
} from "../communication/message.js";
import {
  type MessageReference,
  MsgId,
} from "../communication/records/values.js";
import {
  type CollectiveValue,
  encodeCollectiveContent,
} from "../communication/requests/answers.js";
import {
  Request,
  type ResultDelivery,
} from "../communication/requests/index.js";
import { makeRequests } from "../communication/requests/service.js";
import { digest, testAgentView } from "./agent-card-fixtures.js";
import { collectiveOperationId } from "./collective-id-fixtures.js";
import {
  openTestDatabase,
  type TestDatabase,
} from "./communication-test-database.js";

interface FixtureInput {
  readonly database?: TestDatabase;
  readonly agent?: AgentCard;
  readonly waitForMessage?: (
    reference: Parameters<
      Context.Tag.Service<typeof Communication>["waitForMessage"]
    >[0],
  ) => Effect.Effect<void, CommunicationError>;
  readonly send?: (message: Message) => Effect.Effect<void, CommunicationError>;
}

/** Fixed identities let tests distinguish participant selection from address parsing. */
export const alice = testAgentView("agent:alice");

/** Bob's message identities remain independent of Alice's. */
export const bob = testAgentView("agent:bob");

/** A third participant exercises group responses and forged selections. */
export const carol = testAgentView("agent:carol");

/** One required field makes invalid and declined responses distinguishable. */
export const form = {
  type: "object",
  properties: { slot: { type: "string", enum: ["mon", "tue"] } },
  required: ["slot"],
} as const;

/** A pending certification lets the same accepted own answer survive service scope closure. */
export function pendingBob() {
  return Effect.gen(function* () {
    const completion = yield* Deferred.make<undefined>();
    const fixture = yield* openCollectives({
      agent: bob,
      waitForMessage: () => Deferred.await(completion),
    });
    return { ...fixture, completion };
  }).pipe(Effect.withSpan("pendingBob"));
}

/** Separate service scopes share one database to exercise restart and pending handoffs. */
export function openCollectives(input: FixtureInput = {}) {
  return Effect.gen(function* () {
    const database = input.database ?? (yield* openTestDatabase());
    const agent = input.agent ?? alice;
    const sent = yield* Queue.unbounded<Message>();
    const results = yield* Queue.unbounded<ResultDelivery>();
    yield* database.run(database.store.initializeCommunication(agent));
    const scope = yield* Scope.make();
    yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
    const transport = createTransport(input, sent, database);
    const requests = yield* makeRequests({ self: agent, ...transport }).pipe(
      Effect.provideService(SqlClient, database.sql),
      Scope.extend(scope),
    );
    const consumeResults = <E, R>(
      handle: (result: ResultDelivery) => Effect.Effect<void, E, R>,
    ) =>
      requests
        .deliverNextResult(handle)
        .pipe(
          Effect.zipRight(Effect.sleep("10 millis")),
          Effect.forever,
          Effect.raceFirst(requests.failed),
          Effect.provideService(SqlClient, database.sql),
        );
    const service = {
      accept: (request: Request) => database.run(requests.accept(request)),
      answer: (message: Message) => database.run(requests.answer(message)),
      getRequest: (operationId: Request["operationId"]) =>
        database.run(requests.getRequest(operationId)),
      receive: (message: Message) => database.run(requests.receive(message)),
      consumeResults,
    };
    return {
      database,
      service,
      sent,
      results,
      close: Scope.close(scope, Exit.void),
      collect: () =>
        service
          .consumeResults((result) =>
            Queue.offer(results, result).pipe(Effect.asVoid),
          )
          .pipe(Effect.forkIn(scope)),
    };
  }).pipe(Effect.withSpan("openCollectives"));
}

/** Alice's original request can be captured before the service accepts it. */
export function aliceRequest(
  deadlineAt: number,
  options: {
    readonly op?: "gather" | "all_gather";
    readonly recipients?: readonly AgentCard[];
    readonly operationId?: Request["operationId"];
  } = {},
) {
  return wireMessage(
    alice,
    options.recipients ?? [bob],
    {
      kind: "operation",
      op: options.op ?? "gather",
      operationId: options.operationId ?? collectiveOperationId(alice.agentId),
      deadlineAt,
      requestedSchema: form,
    },
    { byte: 1, text: "Which day works?" },
  );
}

/** A requester close selects complete author-scoped message identities. */
export function aliceClose(
  operationId: Request["operationId"],
  included: ReadonlyArray<{
    readonly authorAgentId: AgentCard["agentId"];
    readonly msgId: Message["msgId"];
  }>,
  closedBy: "settled" | "deadline" = "deadline",
) {
  return Clock.currentTimeMillis.pipe(
    Effect.flatMap((closedAt) =>
      wireMessage(
        alice,
        [bob, carol],
        { kind: "close", operationId, closedAt, closedBy, included },
        { byte: 2 },
      ),
    ),
  );
}

/** Declines retain their message identity even though they carry no answer content. */
export function declineMessage(
  author: AgentCard,
  recipients: readonly AgentCard[],
  operationId: Request["operationId"],
  byte: number,
) {
  return wireMessage(
    author,
    recipients,
    { kind: "response", operationId, action: "decline" },
    { byte },
  );
}

/** Original wire messages are constructed once with a caller-selected author-scoped id. */
export function wireMessage(
  author: AgentCard,
  recipients: readonly AgentCard[],
  value: CollectiveValue,
  options: { readonly byte: number; readonly text?: string },
) {
  return Effect.gen(function* () {
    const content = yield* encodeCollectiveContent(value, options.text);
    return yield* messageFromParticipants(author, recipients, {
      msgId: Schema.decodeUnknownSync(MsgId)(digest("msg_", options.byte)),
      content,
    });
  }).pipe(Effect.orDie, Effect.withSpan("wireMessage"));
}

/** The question deadline belongs to the caller's captured command. */
export function question(
  deadlineAt: number,
  recipients: readonly [AgentCard, ...AgentCard[]] = [bob],
  op: "gather" | "all_gather" = "gather",
) {
  return Schema.decodeUnknownSync(Request)({
    kind: "request",
    author: alice,
    recipients,
    op,
    operationId: collectiveOperationId(alice.agentId),
    question: "Which day works?",
    requestedSchema: form,
    deadlineAt,
  });
}

const retainedMessages = new WeakMap<TestDatabase, Map<string, Message>>();

function createTransport(
  input: FixtureInput,
  sent: Queue.Queue<Message>,
  database: TestDatabase,
) {
  const messages = retainedMessages.get(database) ?? new Map<string, Message>();
  retainedMessages.set(database, messages);
  return {
    acceptMessage: (message: Message) =>
      Effect.gen(function* () {
        yield* input.send?.(message) ?? Effect.void;
        messages.set(`${message.author.agentId}:${message.msgId}`, message);
        yield* Queue.offer(sent, message);
      }),
    waitForMessage: (reference: MessageReference) =>
      Effect.gen(function* () {
        yield* input.waitForMessage?.(reference) ?? Effect.void;
        const message = messages.get(
          `${reference.authorAgentId}:${reference.msgId}`,
        );
        if (message === undefined) {
          return yield* new CommunicationError({ reason: "unknown-message" });
        }
        return message;
      }),
  };
}
