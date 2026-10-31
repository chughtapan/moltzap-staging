/** @file The MCP boundary preserves host delivery identity, replay and send outcomes. */

import { scopedLive as it } from "@effect/vitest";
import {
  Chunk,
  Context,
  Deferred,
  Effect,
  Queue,
  Schema,
  Stream,
} from "effect";
import { describe, expect } from "vitest";
import type { Message } from "../../communication/message.js";
import { untilLive } from "../../__tests__/advance-clock.js";
import { collectiveOperationId } from "../../__tests__/collective-id-fixtures.js";
import {
  authoredMessage,
  conversationIdentityFixture,
} from "../../__tests__/conversation-fixtures.js";
import { makeHostMessagingFixture } from "../../__tests__/host-messaging-fixtures.js";
import {
  loopbackMcpEndpoint,
  makeToolCallRequest,
  mcpTestCredential,
} from "../../__tests__/mcp-http-fixtures.js";
import { CollectiveId } from "../../communication/collectives/contracts.js";
import { encodeCollectiveContent } from "../../communication/collectives/wire.js";
import { Communication } from "../../communication/index.js";
import { AgentAddress } from "../../communication/records/index.js";
import { DeliveryToken } from "../../communication/store/index.js";
import { acquireHarnessEndpoint } from "../../host-client/index.js";
import { decodeHarnessReadInboxResult } from "../../host-protocol/operations.js";
import { McpInbox } from "../messaging/index.js";
import { makeHarnessMcpHttpHandler } from "./index.js";

const unreachable = () =>
  Effect.dieMessage("Unrelated delivery contract operation");
interface Handoff {
  readonly message: Message;
  readonly handled: Deferred.Deferred<undefined>;
}

const fixture = Effect.gen(function* () {
  const subject = yield* conversationIdentityFixture;
  const incoming = yield* Queue.unbounded<Handoff>();
  const sent = yield* Queue.unbounded<Message>();
  const communication = communicationEdge(incoming, sent);
  const host = yield* makeHostMessagingFixture(subject, { communication });
  const handler = yield* deliveryHandler(host);
  yield* Effect.addFinalizer(() =>
    Effect.tryPromise(() => handler.close()).pipe(Effect.orDie),
  );
  const pending = yield* authoredMessage(
    { local: subject.remote, remote: subject.local },
    [{ type: "text", text: "incoming message" }],
  );
  const deliver = deliveryWriter(incoming, pending);
  const call = callTool(handler);
  const inbox = call("read_inbox").pipe(
    Effect.flatMap((result) =>
      decodeHarnessReadInboxResult(result.structuredContent),
    ),
  );
  return {
    subject,
    pending,
    sent,
    deliver,
    call,
    inbox,
    url: yield* loopbackMcpEndpoint(handler),
  };
});

function communicationEdge(
  incoming: Queue.Queue<Handoff>,
  sent: Queue.Queue<Message>,
) {
  return Communication.of({
    send: (message) =>
      Queue.offer(sent, message).pipe(
        Effect.as({
          kind: "completed",
          message: {
            authorAgentId: message.author.agentId,
            msgId: message.msgId,
          },
        } as const),
      ),
    waitForMessage: () =>
      Effect.dieMessage("Delivery fixture does not wait for protocol messages"),
    consume: (handle) =>
      Queue.take(incoming).pipe(
        Effect.flatMap((entry) =>
          handle(entry.message).pipe(
            Effect.zipRight(Deferred.succeed(entry.handled, undefined)),
          ),
        ),
        Effect.forever,
      ),
    listConversations: () =>
      Effect.succeed({ conversations: [], hasMore: false }),
    readHistory: () => Effect.succeed({ messages: [], continuation: null }),
  });
}

function deliveryHandler(
  host: Effect.Effect.Success<ReturnType<typeof makeHostMessagingFixture>>,
) {
  const events = Context.get(host.context, McpInbox);
  const messaging = host.messaging;
  return makeHarnessMcpHttpHandler({
    credential: mcpTestCredential,
    implementation: { name: "delivery-contract", version: "1" },
    eventStore: events,
    operations: {
      send: messaging.send,
      readInbox: messaging.listInbox,
      readEvent: ({ eventId }) =>
        Schema.decodeUnknown(DeliveryToken)(eventId).pipe(
          Effect.catchTag("ParseError", () =>
            Effect.fail({ reason: "invalid-event" }),
          ),
          Effect.flatMap(messaging.getInboxItem),
        ),
      acknowledgeDelivery: messaging.acknowledgeDelivery,
      readInboxSummary: events.readInboxSummary,
      readStatus: unreachable,
      register: unreachable,
      searchConversations: messaging.listConversations,
      readConversation: messaging.readHistory,
    },
  }).pipe(Effect.provide(host.context));
}

function deliveryWriter(incoming: Queue.Queue<Handoff>, pending: Message) {
  return (messages: readonly Message[] = [pending]) =>
    Effect.forEach(
      messages,
      (message) =>
        Effect.gen(function* () {
          const handled = yield* Deferred.make<undefined>();
          yield* Queue.offer(incoming, { message, handled });
          yield* Deferred.await(handled);
        }),
      { concurrency: 1, discard: true },
    );
}

function callTool(handler: Parameters<typeof loopbackMcpEndpoint>[0]) {
  return (name: string, args: Readonly<Record<string, unknown>> = {}) =>
    Effect.tryPromise(() =>
      handler.fetch(
        makeToolCallRequest({ id: "delivery", name, toolArguments: args }),
      ),
    ).pipe(
      Effect.flatMap((response) => Effect.tryPromise(() => response.json())),
      Effect.flatMap(
        Schema.decodeUnknown(
          Schema.Struct({
            result: Schema.Struct({ structuredContent: Schema.Unknown }),
          }),
        ),
      ),
      Effect.map((response) => response.result),
    );
}

const sendsWithoutInventingAdvice = () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    const result = yield* test.call("send_message", {
      input: {
        to: `agent:${test.subject.remote.card.agentName}`,
        text: "Hello",
      },
    });
    const message = yield* Queue.take(test.sent);
    expect(result.structuredContent).toEqual({
      kind: "multicast",
      msgId: message.msgId,
      address: `agent:${test.subject.remote.card.agentName}`,
    });
  });

const consumesAnswers = () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    const operationId = yield* Schema.decodeUnknown(CollectiveId)(
      collectiveOperationId(test.subject.local.card.agentId, 2),
    );
    const content = yield* encodeCollectiveContent({
      kind: "response",
      operationId,
      action: "decline",
    });
    yield* test.deliver([{ ...test.pending, content }]);
    expect((yield* test.inbox).items).toEqual([]);
    const ordinary = yield* authoredMessage(
      { local: test.subject.remote, remote: test.subject.local },
      [{ type: "text", text: "ordinary message" }],
    );
    yield* test.deliver([ordinary]);
    yield* untilLive(
      test.inbox.pipe(
        Effect.map((page) => page.items.length === 1),
        Effect.orDie,
      ),
    );
    expect((yield* test.inbox).items.map((entry) => entry.item)).toEqual([
      { kind: "multicast", message: ordinary },
    ]);
  });

const replaysUntilAcknowledged = () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    yield* test.deliver();
    yield* untilLive(
      test.inbox.pipe(
        Effect.map((page) => page.items.length === 1),
        Effect.orDie,
      ),
    );
    const page = yield* test.inbox;
    expect(yield* test.inbox).toEqual(page);
    const first = page.items[0];
    if (first === undefined) {
      return yield* Effect.dieMessage("Expected the admitted message");
    }
    yield* test.call("acknowledge_delivery", {
      deliveryToken: first.deliveryToken,
    });
    expect((yield* test.inbox).items).toEqual([]);
  });

const returnsACollectiveRefusal = () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    const endpoint = yield* acquireHarnessEndpoint(test.url, {
      credential: mcpTestCredential,
      receiveMode: "push",
    });
    const operationId = yield* Schema.decodeUnknown(CollectiveId)(
      collectiveOperationId(test.subject.local.card.agentId, 2),
    );
    expect(
      yield* endpoint
        .send({
          to: AgentAddress.make(`agent:${test.subject.remote.card.agentName}`),
          collectiveResponse: { operationId, action: "decline" },
        })
        .pipe(Effect.flip),
    ).toMatchObject({
      _tag: "CollectiveError",
      failure: { kind: "request-none" },
    });
    expect((yield* test.inbox).items).toEqual([]);
  });

const retainsEventReference = () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    yield* test.deliver();
    yield* untilLive(
      test.inbox.pipe(
        Effect.map((page) => page.items.length === 1),
        Effect.orDie,
      ),
    );
    const endpoint = yield* acquireHarnessEndpoint(test.url, {
      credential: mcpTestCredential,
      receiveMode: "push",
    });
    const first = yield* endpoint.messages.pipe(
      Stream.take(1),
      Stream.runCollect,
      Effect.map(Chunk.unsafeHead),
    );
    yield* endpoint.messages.pipe(
      Stream.take(1),
      Stream.runForEach((delivery) =>
        Effect.gen(function* () {
          expect(delivery.eventId).toBe(first.eventId);
          yield* delivery.acknowledge;
          expect(
            (yield* test.call("read_event", { eventId: delivery.eventId }))
              .structuredContent,
          ).toEqual({ item: first.item });
        }),
      ),
    );
    expect((yield* test.inbox).items).toEqual([]);
  });

const retainsFailedHandoff = () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    const later = yield* authoredMessage(
      { local: test.subject.remote, remote: test.subject.local },
      [{ type: "text", text: "later message" }],
    );
    yield* test.deliver([test.pending, later]);
    yield* untilLive(
      test.inbox.pipe(
        Effect.map((page) => page.items.length === 2),
        Effect.orDie,
      ),
    );
    const endpoint = yield* acquireHarnessEndpoint(test.url, {
      credential: mcpTestCredential,
      receiveMode: "push",
    });
    const failure = new Error("host refused handoff");
    const outcome = yield* endpoint.messages.pipe(
      Stream.mapEffect((entry) => Effect.fail({ failure, item: entry.item })),
      Stream.runDrain,
      Effect.flip,
    );
    expect(outcome).toEqual({
      failure,
      item: { kind: "multicast", message: test.pending },
    });
    const replay = yield* endpoint.messages.pipe(
      Stream.take(2),
      Stream.map((entry) => entry.item),
      Stream.runCollect,
    );
    expect(Chunk.toReadonlyArray(replay)).toEqual([
      { kind: "multicast", message: test.pending },
      { kind: "multicast", message: later },
    ]);
  });

// @agent-code-guard/regression-only: MCP and native delivery preserve the same durable item identity through refusal and acknowledgment.
describe("MCP delivery contract", () => {
  it(
    "returns a successful send without inventing advice",
    sendsWithoutInventingAdvice,
  );
  it(
    "consumes protocol answers without exposing them in the inbox",
    consumesAnswers,
  );
  it(
    "replays the same item until its token is acknowledged",
    replaysUntilAcknowledged,
  );
  it(
    "returns a collective refusal without creating an inbox outcome",
    returnsACollectiveRefusal,
  );
  it(
    "retains an event reference across redelivery and acknowledgment",
    retainsEventReference,
  );
  it("replays a refused item before later deliveries", retainsFailedHandoff);
});
