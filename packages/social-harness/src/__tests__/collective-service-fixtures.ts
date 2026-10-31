/** @file Collective service scenarios use real SQL and a Message-only communication boundary. */

import { SqlClient } from "@effect/sql";
import {
  Clock,
  Context,
  Deferred,
  Effect,
  Exit,
  Layer,
  Queue,
  Schema,
  Scope,
} from "effect";

import type { AgentView } from "../identity/cards.js";

import {
  Collectives,
  Question,
  type ResultDelivery,
} from "../communication/collectives/index.js";
import {
  type CollectiveValue,
  encodeCollectiveContent,
} from "../communication/collectives/wire.js";
import { Communication, CommunicationError } from "../communication/index.js";
import {
  type Message,
  messageFromParticipants,
} from "../communication/message.js";
import { MsgId } from "../communication/records/index.js";
import { Identity } from "../identity/local.js";
import { digest, testAgentView } from "./agent-card-fixtures.js";
import { collectiveOperationId } from "./collective-id-fixtures.js";
import {
  openTestDatabase,
  type TestDatabase,
} from "./communication-test-database.js";

interface FixtureInput {
  readonly database?: TestDatabase;
  readonly agent?: AgentView;
  readonly waitForMessage?: (
    reference: Parameters<
      Context.Tag.Service<typeof Communication>["waitForMessage"]
    >[0],
  ) => Effect.Effect<void, CommunicationError>;
  readonly send?: Context.Tag.Service<typeof Communication>["send"];
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
    const communication = createCommunication(input, sent, database);
    const identity = registeredIdentity(agent);
    const scope = yield* Scope.make();
    yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
    const services = yield* Layer.buildWithScope(
      Collectives.layer.pipe(
        Layer.provide(Layer.succeed(Identity, identity)),
        Layer.provide(Layer.succeed(Communication, communication)),
        Layer.provide(Layer.succeed(SqlClient.SqlClient, database.sql)),
      ),
      scope,
    );
    const service = Context.get(services, Collectives);
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
    readonly recipients?: readonly AgentView[];
    readonly operationId?: Question["operationId"];
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
  operationId: Question["operationId"],
  included: ReadonlyArray<{
    readonly authorAgentId: AgentView["agentId"];
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
  author: AgentView,
  recipients: readonly AgentView[],
  operationId: Question["operationId"],
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
  author: AgentView,
  recipients: readonly AgentView[],
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
  recipients: readonly [AgentView, ...AgentView[]] = [bob],
  op: "gather" | "all_gather" = "gather",
) {
  return Schema.decodeUnknownSync(Question)({
    author: alice,
    recipients,
    op,
    operationId: collectiveOperationId(alice.agentId),
    text: "Which day works?",
    requestedSchema: form,
    deadlineAt,
  });
}

const retainedMessages = new WeakMap<TestDatabase, Map<string, Message>>();

function createCommunication(
  input: FixtureInput,
  sent: Queue.Queue<Message>,
  database: TestDatabase,
) {
  const messages = retainedMessages.get(database) ?? new Map<string, Message>();
  retainedMessages.set(database, messages);
  return Communication.of({
    send: (message) =>
      Effect.gen(function* () {
        yield* input.send?.(message) ?? Effect.void;
        messages.set(`${message.author.agentId}:${message.msgId}`, message);
        yield* Queue.offer(sent, message);
      }),
    waitForMessage: (reference) =>
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
    consume: () => Effect.never,
    listConversations: () =>
      Effect.succeed({ conversations: [], hasMore: false }),
    readHistory: () => Effect.succeed({ messages: [], continuation: null }),
  });
}

function registeredIdentity(agent: AgentView) {
  return Identity.of({
    me: () => Effect.succeed({ kind: "registered", agent }),
    register: () => Effect.dieMessage("unexpected registration"),
    resolve: () => Effect.dieMessage("unexpected resolution"),
  });
}
