/** @file Collective requests keep their responses and results across handoff and restart. */

import { scopedLive as it } from "@effect/vitest";
import {
  Clock,
  Data,
  Deferred,
  Effect,
  FastCheck as fc,
  Fiber,
  Option,
  Queue,
  Ref,
  Schema,
  TestClock,
  TestContext,
} from "effect";
import { expect } from "vitest";
import {
  alice,
  aliceAsks,
  aliceClose,
  aliceRequest,
  bob,
  carol,
  type CompletedCollective,
  declineMessage,
  openCollectives,
  testMsgId,
  wireMessage,
} from "./__tests__/requests-fixtures.js";
import { restarting } from "./collectives.js";
import { CommunicationError } from "./errors.js";
import { collectivePartOf, Message, RequestError } from "./messages.js";
import { agentTransaction } from "./store.js";

class HandlerUnavailable extends Data.TaggedError("HandlerUnavailable") {}

it("sends each gather recipient a private copy with its own message id, and counts a response that names any copy", () =>
  Effect.gen(function* () {
    const fixture = yield* openCollectives();
    const captured = aliceAsks((yield* Clock.currentTimeMillis) + 5000, [
      bob,
      carol,
    ]);
    yield* fixture.service.sendRequest(captured);
    const messages = [
      yield* Queue.take(fixture.sent),
      yield* Queue.take(fixture.sent),
    ];
    const [toBob, toCarol] = messages;
    expect(messages.map((message) => message.recipients)).toEqual([
      [bob],
      [carol],
    ]);
    expect(toBob?.msgId).toBe(captured.msgId);
    expect(toCarol?.msgId).not.toBe(captured.msgId);
    yield* fixture.service.sendRequest(captured);
    yield* fixture.close;
    const restarted = yield* openCollectives({ database: fixture.database });
    yield* restarted.collect();
    const response = yield* declineMessage(bob, [alice], captured.msgId, 3);
    yield* restarted.service.receive(response);
    const other = yield* declineMessage(
      carol,
      [alice],
      toCarol?.msgId ?? captured.msgId,
      4,
    );
    yield* restarted.service.receive(other);
    const completed = yield* Queue.take(restarted.results);
    expect(completed.result.request.msgId).toBe(captured.msgId);
    expect(completed.sources.responses).toEqual([response, other]);
  }));

it("counts the first response once and retains its original source", () =>
  Effect.gen(function* () {
    const fixture = yield* openCollectives();
    const captured = aliceAsks((yield* Clock.currentTimeMillis) + 5000);
    yield* fixture.collect();
    yield* fixture.service.sendRequest(captured);
    yield* Queue.take(fixture.sent);
    const first = yield* wireMessage(
      bob,
      [alice],
      {
        kind: "response",
        requestId: captured.msgId,
        action: "accept",
        content: { slot: "mon" },
      },
      { byte: 11 },
    );
    const later = yield* wireMessage(
      bob,
      [alice],
      {
        kind: "response",
        requestId: captured.msgId,
        action: "accept",
        content: { slot: "tue" },
      },
      { byte: 12 },
    );
    yield* fixture.service.receive(first);
    yield* fixture.service.receive(later);
    const result = yield* Queue.take(fixture.results);
    expect(result.sources.responses).toEqual([first]);
    expect(result.result).toMatchObject({
      request: { msgId: captured.msgId },
      outcomes: [{ recipient: bob, outcome: { content: { slot: "mon" } } }],
    });
  }));

it("records invalid responses as counted sources while leaving silent recipients with no outcome but no-answer", () =>
  Effect.gen(function* () {
    const fixture = yield* openCollectives();
    const captured = aliceAsks((yield* Clock.currentTimeMillis) + 300, [
      bob,
      carol,
    ]);
    yield* fixture.collect();
    yield* fixture.service.sendRequest(captured);
    yield* Queue.take(fixture.sent);
    yield* Queue.take(fixture.sent);
    const invalid = yield* wireMessage(
      bob,
      [alice],
      {
        kind: "response",
        requestId: captured.msgId,
        action: "accept",
        content: { slot: "wed" },
      },
      { byte: 13 },
    );
    yield* fixture.service.receive(invalid);
    const result = yield* Queue.take(fixture.results);
    expect(result.sources.responses).toEqual([invalid]);
    expect(result.result).toMatchObject({
      request: { msgId: captured.msgId },
      outcomes: [
        { recipient: bob, outcome: { kind: "invalid" } },
        { recipient: carol, outcome: { kind: "no-answer" } },
      ],
    });
  }));

it("does not count a response received when the deadline has come", () =>
  Effect.gen(function* () {
    const fixture = yield* openCollectives();
    const captured = aliceAsks((yield* Clock.currentTimeMillis) + 200);
    yield* fixture.collect();
    yield* fixture.service.sendRequest(captured);
    yield* Queue.take(fixture.sent);
    const late = yield* declineMessage(bob, [alice], captured.msgId, 14);
    yield* TestClock.setTime(captured.deadlineAt).pipe(
      Effect.zipRight(fixture.service.receive(late)),
      Effect.provide(TestContext.TestContext),
    );
    const result = yield* Queue.take(fixture.results);
    expect(result.sources.responses).toEqual([]);
    expect(result.result).toMatchObject({
      outcomes: [{ recipient: bob, outcome: { kind: "no-answer" } }],
    });
  }));

it("publishes the all_gather close with both parts of every selected message identity", () =>
  Effect.gen(function* () {
    const fixture = yield* openCollectives();
    const captured = aliceAsks(
      (yield* Clock.currentTimeMillis) + 5000,
      [bob, carol],
      "all_gather",
    );
    const consuming = yield* fixture.collect();
    yield* fixture.service.sendRequest(captured);
    yield* Queue.take(fixture.sent);
    const one = yield* declineMessage(bob, [alice, carol], captured.msgId, 15);
    const two = yield* declineMessage(carol, [alice, bob], captured.msgId, 15);
    yield* fixture.service.receive(one);
    yield* fixture.service.receive(two);
    const close = yield* Queue.take(fixture.sent);
    const part = yield* collectivePartOf(close);
    expect(part).toEqual(
      Option.some(
        expect.objectContaining({
          requestId: captured.msgId,
          included: [
            { authorAgentId: bob.agentId, msgId: one.msgId },
            { authorAgentId: carol.agentId, msgId: two.msgId },
          ],
        }),
      ),
    );
    const result = yield* Queue.take(fixture.results).pipe(
      Effect.raceFirst(Fiber.join(consuming)),
    );
    expect(result.sources.responses).toEqual([one, two]);
  }));

it("delivers a closed all_gather result after a restart without a response that came later", () =>
  Effect.gen(function* () {
    const fixture = yield* openCollectives();
    const captured = aliceAsks(
      (yield* Clock.currentTimeMillis) + 250,
      [bob, carol],
      "all_gather",
    );
    yield* fixture.service.sendRequest(captured);
    yield* Queue.take(fixture.sent);
    const selected = yield* declineMessage(
      bob,
      [alice, carol],
      captured.msgId,
      43,
    );
    yield* fixture.service.receive(selected);
    const close = yield* Queue.take(fixture.sent);
    expect(yield* collectivePartOf(close)).toEqual(
      Option.some(
        expect.objectContaining({
          included: [{ authorAgentId: bob.agentId, msgId: selected.msgId }],
        }),
      ),
    );
    const late = yield* declineMessage(carol, [alice, bob], captured.msgId, 44);
    yield* fixture.service.receive(late);
    yield* fixture.close;
    const restarted = yield* openCollectives({ database: fixture.database });
    yield* restarted.collect();
    const result = yield* Queue.take(restarted.results);
    expect(result.sources.responses).toEqual([selected]);
    expect(result.result).toMatchObject({
      outcomes: [
        { recipient: bob, outcome: { kind: "declined" } },
        { recipient: carol, outcome: { kind: "no-answer" } },
      ],
    });
  }));

it("takes a new response after the send of an earlier one failed", () =>
  Effect.gen(function* () {
    const fixture = yield* openCollectives({
      agent: bob,
      send: () =>
        Effect.fail(new CommunicationError({ reason: "persistence-failed" })),
    });
    const request = yield* aliceRequest(
      (yield* Clock.currentTimeMillis) + 5000,
    );
    const requestId = request.msgId;
    yield* fixture.service.receive(request);
    const refused = yield* declineMessage(bob, [alice], requestId, 45);
    expect(
      yield* fixture.service.sendResponse(refused).pipe(Effect.flip),
    ).toMatchObject({ reason: "persistence-failed" });
    yield* fixture.close;
    const restarted = yield* openCollectives({
      database: fixture.database,
      agent: bob,
    });
    const accepted = yield* declineMessage(bob, [alice], requestId, 46);
    yield* restarted.service.sendResponse(accepted);
    expect(yield* Queue.take(restarted.sent)).toEqual(accepted);
  }));

it("accepts its own response again after a restart and refuses a different one", () =>
  Effect.gen(function* () {
    const fixture = yield* openCollectives({ agent: bob });
    const request = yield* aliceRequest(
      (yield* Clock.currentTimeMillis) + 5000,
      { op: "gather", recipients: [bob] },
    );
    const requestId = request.msgId;
    yield* fixture.service.receive(request);
    const message = yield* wireMessage(
      bob,
      [alice],
      {
        kind: "response",
        requestId,
        action: "accept",
        content: { slot: "mon" },
      },
      { byte: 17 },
    );
    yield* fixture.service.sendResponse(message);
    expect(yield* Queue.take(fixture.sent)).toEqual(message);
    yield* fixture.close;
    const restarted = yield* openCollectives({
      database: fixture.database,
      agent: bob,
    });
    yield* restarted.service.sendResponse(message);
    expect(yield* Queue.take(restarted.sent)).toEqual(message);
    expect(
      yield* restarted.service.getRequest(requestId).pipe(Effect.flip),
    ).toMatchObject({
      failure: { kind: "request-answered" },
    });
    const replacement = yield* declineMessage(bob, [alice], requestId, 18);
    expect(
      yield* restarted.service.sendResponse(replacement).pipe(Effect.flip),
    ).toBeInstanceOf(RequestError);
  }));

it("hands over a result again when its handler failed before the result was marked consumed", () =>
  Effect.gen(function* () {
    const fixture = yield* openCollectives();
    const captured = aliceAsks((yield* Clock.currentTimeMillis) + 200);
    const attempted = yield* Deferred.make<CompletedCollective>();
    const consuming = yield* fixture.service
      .consumeResults((result) =>
        Deferred.succeed(attempted, result).pipe(
          Effect.zipRight(Effect.fail(new HandlerUnavailable())),
        ),
      )
      .pipe(Effect.forkScoped);
    yield* fixture.service.sendRequest(captured);
    const first = yield* Deferred.await(attempted);
    yield* Fiber.await(consuming);
    yield* fixture.close;
    const restarted = yield* openCollectives({ database: fixture.database });
    yield* restarted.collect();
    expect(yield* Queue.take(restarted.results)).toEqual(first);
  }));

it("passes messages with no collective part unchanged and consumes malformed collective content", () =>
  Effect.gen(function* () {
    const fixture = yield* openCollectives();
    const valid = yield* declineMessage(bob, [alice], testMsgId(1), 19);
    const ordinary = Schema.decodeUnknownSync(Message)({
      author: valid.author,
      recipients: valid.recipients,
      msgId: valid.msgId,
      content: [
        { type: "text", text: "Hello" },
        { type: "data", value: { topic: "calendar" } },
      ],
    });
    expect(yield* fixture.service.receive(ordinary)).toEqual(
      Option.some({ kind: "message", message: ordinary }),
    );
    const duplicate = Schema.decodeUnknownSync(Message)({
      author: valid.author,
      recipients: valid.recipients,
      msgId: valid.msgId,
      content: [...valid.content, ...valid.content],
    });
    expect(yield* fixture.service.receive(duplicate)).toEqual(Option.none());
  }));

it("ignores an unknown close permanently rather than applying it to a later request", () =>
  Effect.gen(function* () {
    const fixture = yield* openCollectives({ agent: bob });
    const request = yield* aliceRequest(
      (yield* Clock.currentTimeMillis) + 5000,
      { op: "all_gather", recipients: [bob, carol] },
    );
    const requestId = request.msgId;
    const close = yield* aliceClose(requestId, [], "deadline");
    yield* fixture.service.receive(close);
    yield* fixture.service.receive(request);
    yield* fixture.close;
    const restarted = yield* openCollectives({
      database: fixture.database,
      agent: bob,
    });
    const response = yield* declineMessage(bob, [alice, carol], requestId, 22);
    yield* restarted.service.sendResponse(response);
    expect(yield* Queue.take(restarted.sent)).toEqual(response);
  }));

it("presents the result of a close that counts its own response", () =>
  Effect.gen(function* () {
    const fixture = yield* openCollectives({ agent: bob });
    const request = yield* aliceRequest(
      (yield* Clock.currentTimeMillis) + 5000,
      { op: "all_gather", recipients: [bob, carol] },
    );
    const requestId = request.msgId;
    yield* fixture.service.receive(request);
    yield* fixture.collect();
    const own = yield* wireMessage(
      bob,
      [alice, carol],
      {
        kind: "response",
        requestId,
        action: "accept",
        content: { slot: "mon" },
      },
      { byte: 26 },
    );
    yield* fixture.service.sendResponse(own);
    yield* Queue.take(fixture.sent);
    const peer = yield* declineMessage(carol, [alice, bob], requestId, 27);
    yield* fixture.service.receive(peer);
    const close = yield* aliceClose(
      requestId,
      [
        { authorAgentId: bob.agentId, msgId: own.msgId },
        { authorAgentId: carol.agentId, msgId: peer.msgId },
      ],
      "settled",
    );
    yield* fixture.service.receive(close);
    const result = yield* Queue.take(fixture.results);
    expect(result.sources.request).toEqual(request);
    expect(result.sources.responses).toEqual([own, peer]);
  }));

it("ends a malformed close with repeated authors without letting the recipient respond afterwards", () =>
  Effect.gen(function* () {
    const fixture = yield* openCollectives({ agent: bob });
    const request = yield* aliceRequest(
      (yield* Clock.currentTimeMillis) + 5000,
      { op: "all_gather", recipients: [bob, carol] },
    );
    const requestId = request.msgId;
    yield* fixture.service.receive(request);
    const peer = yield* declineMessage(carol, [alice, bob], requestId, 30);
    yield* fixture.service.receive(peer);
    const reference = { authorAgentId: peer.author.agentId, msgId: peer.msgId };
    const close = yield* aliceClose(
      requestId,
      [reference, reference],
      "deadline",
    );
    yield* fixture.service.receive(close);
    yield* fixture.close;
    const reopened = yield* openCollectives({
      database: fixture.database,
      agent: bob,
    });
    const own = yield* declineMessage(bob, [alice, carol], requestId, 32);
    expect(
      yield* reopened.service.sendResponse(own).pipe(Effect.flip),
    ).toBeInstanceOf(RequestError);
  }));

it("refuses a response to a request whose deadline has passed", () =>
  Effect.gen(function* () {
    const fixture = yield* openCollectives({ agent: bob });
    const deadlineAt = (yield* Clock.currentTimeMillis) + 100;
    const request = yield* aliceRequest(deadlineAt, {
      op: "gather",
      recipients: [bob],
    });
    const requestId = request.msgId;
    yield* fixture.service.receive(request);
    const response = yield* declineMessage(bob, [alice], requestId, 34);
    yield* Effect.sleep(150);
    expect(
      yield* fixture.service.sendResponse(response).pipe(Effect.flip),
    ).toBeInstanceOf(RequestError);
  }));

it("publishes independent results while incoming handlers share SQL transactions with workers", () =>
  Effect.gen(function* () {
    const fixture = yield* openCollectives();
    yield* fixture.collect();
    const deadline = (yield* Clock.currentTimeMillis) + 5000;
    const requests = [37, 38].map((byte) =>
      aliceAsks(deadline, [bob], "gather", byte),
    );
    yield* Effect.forEach(
      requests,
      (captured) => fixture.service.sendRequest(captured),
      { concurrency: 2 },
    );
    yield* Queue.take(fixture.sent);
    yield* Queue.take(fixture.sent);
    const responses = yield* Effect.forEach(
      requests,
      (captured, index) =>
        declineMessage(bob, [alice], captured.msgId, 39 + index),
      { concurrency: 1 },
    );
    yield* Effect.forEach(
      responses,
      (message) =>
        fixture.database.run(
          agentTransaction(
            fixture.database.agentKey,
            fixture.service.receive(message),
          ),
        ),
      { concurrency: 2 },
    );
    const results = [
      yield* Queue.take(fixture.results),
      yield* Queue.take(fixture.results),
    ];
    expect(
      new Map(
        results.map((completed) => [
          completed.result.request.msgId,
          completed.sources.responses,
        ]),
      ),
    ).toEqual(
      new Map(
        responses.map((response, index) => [
          requests[index]?.msgId,
          [response],
        ]),
      ),
    );
  }));

it("starts a failed loop again after one second, then after two more", () =>
  Effect.gen(function* () {
    const attempts = yield* Ref.make(0);
    yield* Ref.updateAndGet(attempts, (count) => count + 1).pipe(
      Effect.flatMap((count) =>
        count < 3 ? Effect.fail(new HandlerUnavailable()) : Effect.never,
      ),
      restarting("agent", "loop"),
      Effect.fork,
    );
    yield* TestClock.adjust(0);
    for (const pause of [1000, 2000]) {
      const before = yield* Ref.get(attempts);
      yield* TestClock.adjust(pause - 1);
      expect(yield* Ref.get(attempts)).toBe(before);
      yield* TestClock.adjust(1);
      expect(yield* Ref.get(attempts)).toBe(before + 1);
    }
  }).pipe(Effect.provide(TestContext.TestContext)));

it.prop(
  "preserves the text of every message with no collective part",
  [fc.string()],
  ([text]) =>
    Effect.gen(function* () {
      const fixture = yield* openCollectives();
      const header = yield* declineMessage(bob, [alice], testMsgId(1), 41);
      const message = Schema.decodeUnknownSync(Message)({
        author: header.author,
        recipients: header.recipients,
        msgId: header.msgId,
        content: [{ type: "text", text }],
      });
      expect(yield* fixture.service.receive(message)).toEqual(
        Option.some({ kind: "message", message }),
      );
    }),
  { fastCheck: { numRuns: 24 } },
);
