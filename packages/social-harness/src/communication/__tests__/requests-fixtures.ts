/** @file Collective service scenarios use real SQL and a Message-only communication boundary. */

import { SqlClient } from "@effect/sql/SqlClient";
import { Clock, Effect, Exit, Queue, Schema, Scope } from "effect";
import type { AgentCard } from "../../identity/agent-cards.js";
import { digest, testAgentCard } from "../../__tests__/agents.js";
import { encodeCanonical } from "../../encoding/canonical-json.js";
import { makeRequests } from "../collectives.js";
import {
  type CollectiveValue,
  compareAgentIds,
  encodeCollectiveContent,
  Message,
  messageFromParticipants,
  MsgId,
  Request,
  type ResultDelivery,
} from "../messages.js";
import { deriveConversationId } from "../records.js";
import { CommunicationError } from "../service.js";
import { acceptMessage } from "../store.js";
import { collectiveOperationId } from "./request-ids.js";
import { openTestDatabase, type TestDatabase } from "./store-fixtures.js";

interface FixtureInput {
  readonly database?: TestDatabase;
  readonly agent?: AgentCard;
  readonly pendingCompletion?: boolean;
  readonly send?: (message: Message) => Effect.Effect<void, CommunicationError>;
}

/** Fixed identities let tests distinguish participant selection from address parsing. */
export const alice = testAgentCard("agent:alice");

/** Bob's message identities remain independent of Alice's. */
export const bob = testAgentCard("agent:bob");

/** A third participant exercises group responses and forged selections. */
export const carol = testAgentCard("agent:carol");

/** One required field makes invalid and declined responses distinguishable. */
export const form = {
  type: "object",
  properties: { slot: { type: "string", enum: ["mon", "tue"] } },
  required: ["slot"],
} as const;

/** A pending own answer survives service scope closure without a second submission. */
export function pendingBob() {
  return openCollectives({ agent: bob, pendingCompletion: true });
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
    const complete = database
      .run(
        database.sql.unsafe(
          "UPDATE message_intents SET completed_record_hash=msg_id WHERE completed_record_hash IS NULL AND failure_reason IS NULL",
        ),
      )
      .pipe(Effect.asVoid);
    if (!input.pendingCompletion) {
      yield* complete;
    }
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
      complete,
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

/** The transport double records accepted payloads and projects certification; protocol suites exercise the real certificates. */
function createTransport(
  input: FixtureInput,
  sent: Queue.Queue<Message>,
  database: TestDatabase,
) {
  return {
    acceptMessage: (message: Message) =>
      Effect.gen(function* () {
        yield* input.send?.(message) ?? Effect.void;
        const members: [
          AgentCard["agentId"],
          AgentCard["agentId"],
          ...Array<AgentCard["agentId"]>,
        ] = [
          message.author.agentId,
          message.recipients[0].agentId,
          ...message.recipients.slice(1).map((member) => member.agentId),
        ];
        const conversationId = yield* deriveConversationId(
          members.sort(compareAgentIds),
        );
        yield* acceptMessage({
          authorAgentId: message.author.agentId,
          msgId: message.msgId,
          conversationId,
          canonicalMessage: yield* encodeCanonical(Message, message),
        });
        if (!input.pendingCompletion) {
          const sql = yield* SqlClient;
          yield* sql.unsafe(
            "UPDATE message_intents SET completed_record_hash=msg_id WHERE author_agent_id=? AND msg_id=?",
            [message.author.agentId, message.msgId],
          );
        }
        yield* Queue.offer(sent, message);
      }).pipe(
        Effect.catchTags({
          EndpointStoreError: () =>
            new CommunicationError({ reason: "persistence-failed" }),
          ClientRepresentationError: () =>
            new CommunicationError({ reason: "persistence-failed" }),
          SqlError: () =>
            new CommunicationError({ reason: "persistence-failed" }),
        }),
        database.run,
      ),
  };
}
