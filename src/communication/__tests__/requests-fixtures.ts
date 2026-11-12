/** @file Question handling over real SQL, with sends recorded in place of delivery. */

import { SqlClient } from "@effect/sql/SqlClient";
import { Clock, Effect, Exit, Queue, Schema, Scope } from "effect";
import type { AgentCard } from "../../identity/agent-cards.js";
import type { CommunicationError } from "../service.js";
import { digest, testAgentCard } from "../../__tests__/agents.js";
import { makeRequests } from "../collectives.js";
import {
  type CollectiveValue,
  encodeCollectiveContent,
  type Message,
  messageFromParticipants,
  MsgId,
  Request,
  type ResultDelivery,
} from "../messages.js";
import { collectiveOperationId } from "./request-ids.js";
import { openTestDatabase, type TestDatabase } from "./store-fixtures.js";

interface FixtureInput {
  readonly database?: TestDatabase;
  readonly agent?: AgentCard;
  /** Runs in place of delivery for each message the service sends. */
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

/** A question service for one agent. Reopening on the same database stands for a restart. */
export const openCollectives = Effect.fn("openCollectives")(function* (
  input: FixtureInput = {},
) {
  const database = input.database ?? (yield* openTestDatabase());
  const agent = input.agent ?? alice;
  const sent = yield* Queue.unbounded<Message>();
  const results = yield* Queue.unbounded<ResultDelivery>();
  const scope = yield* Scope.make();
  yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
  const requests = yield* makeRequests(database.agentKey, {
    self: agent,
    send: (message) =>
      (input.send?.(message) ?? Effect.void).pipe(
        Effect.zipRight(Queue.offer(sent, message)),
        Effect.asVoid,
      ),
    completed: Effect.void,
  }).pipe(Effect.provideService(SqlClient, database.sql), Scope.extend(scope));
  const consumeResults = <E, R>(
    handle: (result: ResultDelivery) => Effect.Effect<void, E, R>,
  ) =>
    requests
      .deliverNextResult(handle)
      .pipe(
        Effect.zipRight(Effect.sleep("10 millis")),
        Effect.forever,
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
});

/** Original wire messages are constructed once with a caller-selected author-scoped id. */
export const wireMessage = Effect.fn("wireMessage")(function* (
  author: AgentCard,
  recipients: readonly AgentCard[],
  value: CollectiveValue,
  options: { readonly byte: number; readonly text?: string },
) {
  const content = yield* encodeCollectiveContent(value, options.text);
  return yield* messageFromParticipants(author, recipients, {
    msgId: Schema.decodeUnknownSync(MsgId)(digest("msg_", options.byte)),
    content,
  });
}, Effect.orDie);

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
