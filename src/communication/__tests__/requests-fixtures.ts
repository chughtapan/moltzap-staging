/** @file Collective handling over real SQL, with sends recorded in place of delivery. */

import { SqlClient } from "@effect/sql/SqlClient";
import { Clock, Effect, Exit, Queue, Schema, Scope } from "effect";
import type { AgentCard } from "../../identity.js";
import type { CommunicationError } from "../errors.js";
import { digest, testAgentCard } from "../../__tests__/agents.js";
import { makeCollectives } from "../collectives.js";
import {
  type CollectivePart,
  CollectiveRequest,
  type CollectiveResult,
  type CollectiveResultSources,
  encodeCollectiveContent,
  type Message,
  messageWithId,
  MsgId,
} from "../messages.js";
import { openTestDatabase, type TestDatabase } from "./store-fixtures.js";

/** A collective result as the consumer is handed it. */
export interface CompletedCollective {
  readonly result: CollectiveResult;
  readonly sources: CollectiveResultSources;
}

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

/** One agent's collectives. Reopening on the same database stands for a restart. */
export const openCollectives = Effect.fn("openCollectives")(function* (
  input: FixtureInput = {},
) {
  const database = input.database ?? (yield* openTestDatabase());
  const agent = input.agent ?? alice;
  const sent = yield* Queue.unbounded<Message>();
  const results = yield* Queue.unbounded<CompletedCollective>();
  const scope = yield* Scope.make();
  yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
  const collectives = yield* makeCollectives(database.agentKey, {
    card: agent,
    send: (message) =>
      (input.send?.(message) ?? Effect.void).pipe(
        Effect.zipRight(Queue.offer(sent, message)),
        Effect.asVoid,
      ),
    completed: Effect.void,
  }).pipe(Effect.provideService(SqlClient, database.sql), Scope.extend(scope));
  const consumeResults = <E, R>(
    handle: (completed: CompletedCollective) => Effect.Effect<void, E, R>,
  ) =>
    collectives
      .deliverNextResult(handle)
      .pipe(
        Effect.zipRight(Effect.sleep("10 millis")),
        Effect.forever,
        Effect.provideService(SqlClient, database.sql),
      );
  const service = {
    sendRequest: (request: CollectiveRequest) =>
      database.run(collectives.sendRequest(request)),
    sendResponse: (message: Message) =>
      message.collectiveResponse === undefined
        ? Effect.dieMessage("The message is not a collective response")
        : database.run(
            collectives.sendResponse(message, message.collectiveResponse),
          ),
    getRequest: (requestId: MsgId) =>
      database.run(collectives.getRequest(requestId)),
    receive: (message: Message) => database.run(collectives.receive(message)),
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

/** A message carrying a collective part, with a caller-selected message id. */
export const wireMessage = Effect.fn("wireMessage")(function* (
  author: AgentCard,
  recipients: readonly AgentCard[],
  part: CollectivePart,
  options: { readonly byte: number; readonly text?: string },
) {
  const content = yield* encodeCollectiveContent(part, options.text);
  return yield* messageWithId(
    author,
    recipients,
    testMsgId(options.byte),
    content,
  );
}, Effect.orDie);

/** A collective request from Alice as a recipient receives it. Its message id is `testMsgId(1)`. */
export function aliceRequest(
  deadlineAt: number,
  options: {
    readonly op?: "gather" | "all_gather";
    readonly recipients?: readonly AgentCard[];
  } = {},
) {
  return wireMessage(
    alice,
    options.recipients ?? [bob],
    {
      kind: "request",
      op: options.op ?? "gather",
      deadlineAt,
      requestedSchema: form,
    },
    { byte: 1, text: "Which day works?" },
  );
}

/** A requester close selects complete author-scoped message identities. */
export function aliceClose(
  requestId: MsgId,
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
        { kind: "close", requestId, included, closedAt, closedBy },
        { byte: 2 },
      ),
    ),
  );
}

/** A declining response to the request, or to the private copy of it, that `requestId` names. */
export function declineMessage(
  author: AgentCard,
  recipients: readonly AgentCard[],
  requestId: MsgId,
  byte: number,
) {
  return wireMessage(
    author,
    recipients,
    { kind: "response", requestId, action: "decline" },
    { byte },
  );
}

/** A collective request Alice sends, with the message id `testMsgId(byte)`. */
export function aliceAsks(
  deadlineAt: number,
  recipients: readonly [AgentCard, ...AgentCard[]] = [bob],
  op: "gather" | "all_gather" = "gather",
  byte = 0,
) {
  return Schema.decodeUnknownSync(CollectiveRequest)({
    kind: "collectiveRequest",
    msgId: testMsgId(byte),
    author: alice,
    recipients,
    op,
    question: "Which day works?",
    requestedSchema: form,
    deadlineAt,
  });
}

/** The message id whose random bytes all equal `byte`. */
export function testMsgId(byte: number): MsgId {
  return Schema.decodeUnknownSync(MsgId)(digest("msg_", byte));
}
