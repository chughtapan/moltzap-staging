/** @file Request handling retains answer claims and completion across handoff and restart. */

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
  Schema,
} from "effect";
import { expect } from "vitest";
import { collectiveOperationId } from "./__tests__/request-ids.js";
import {
  alice,
  aliceClose,
  aliceRequest,
  bob,
  carol,
  declineMessage,
  form,
  openCollectives,
  pendingBob,
  question,
  wireMessage,
} from "./__tests__/requests-fixtures.js";
import {
  Message,
  readCollectiveValue,
  RequestError,
  type ResultDelivery,
} from "./messages.js";
import { CommunicationError } from "./service.js";
import { transaction } from "./store.js";

class HandlerUnavailable extends Data.TaggedError("HandlerUnavailable") {}

it("saves separate gather requests without exposing the other recipients", () =>
  Effect.gen(function* () {
    const fixture = yield* openCollectives();
    const captured = question((yield* Clock.currentTimeMillis) + 5000, [
      bob,
      carol,
    ]);
    yield* fixture.service.accept(captured);
    const messages = [
      yield* Queue.take(fixture.sent),
      yield* Queue.take(fixture.sent),
    ];
    expect(messages.map((message) => message.recipients)).toEqual([
      [bob],
      [carol],
    ]);
    yield* fixture.service.accept(captured);
    yield* fixture.close;
    const restarted = yield* openCollectives({ database: fixture.database });
    yield* restarted.collect();
    const response = yield* declineMessage(
      bob,
      [alice],
      captured.operationId,
      3,
    );
    yield* restarted.service.receive(response);
    const other = yield* declineMessage(
      carol,
      [alice],
      captured.operationId,
      4,
    );
    yield* restarted.service.receive(other);
    expect((yield* Queue.take(restarted.results)).sources.answers).toEqual([
      response,
      other,
    ]);
  }));

it("counts the first answer once and retains its original source", () =>
  Effect.gen(function* () {
    const fixture = yield* openCollectives();
    const captured = question((yield* Clock.currentTimeMillis) + 5000);
    yield* fixture.collect();
    yield* fixture.service.accept(captured);
    yield* Queue.take(fixture.sent);
    const first = yield* wireMessage(
      bob,
      [alice],
      {
        kind: "response",
        operationId: captured.operationId,
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
        operationId: captured.operationId,
        action: "accept",
        content: { slot: "tue" },
      },
      { byte: 12 },
    );
    yield* fixture.service.receive(first);
    yield* fixture.service.receive(later);
    const result = yield* Queue.take(fixture.results);
    expect(result.sources.answers).toEqual([first]);
    expect(
      result.item.kind === "result" ? result.item.result : result.item,
    ).toMatchObject({
      request: { operationId: captured.operationId },
      outcomes: [{ member: bob, outcome: { content: { slot: "mon" } } }],
    });
  }));

it("records invalid answers as selected sources while leaving silent members unanswered", () =>
  Effect.gen(function* () {
    const fixture = yield* openCollectives();
    const captured = question((yield* Clock.currentTimeMillis) + 300, [
      bob,
      carol,
    ]);
    yield* fixture.collect();
    yield* fixture.service.accept(captured);
    yield* Queue.take(fixture.sent);
    yield* Queue.take(fixture.sent);
    const invalid = yield* wireMessage(
      bob,
      [alice],
      {
        kind: "response",
        operationId: captured.operationId,
        action: "accept",
        content: { slot: "wed" },
      },
      { byte: 13 },
    );
    yield* fixture.service.receive(invalid);
    const result = yield* Queue.take(fixture.results);
    expect(result.sources.answers).toEqual([invalid]);
    expect(
      result.item.kind === "result" ? result.item.result : result.item,
    ).toMatchObject({
      request: { operationId: captured.operationId },
      outcomes: [
        { member: bob, outcome: { kind: "invalid" } },
        { member: carol, outcome: { kind: "no-answer" } },
      ],
    });
  }));

it("does not count an answer first received after the collector deadline", () =>
  Effect.gen(function* () {
    const fixture = yield* openCollectives();
    const captured = question((yield* Clock.currentTimeMillis) + 200);
    yield* fixture.collect();
    yield* fixture.service.accept(captured);
    yield* Queue.take(fixture.sent);
    const result = yield* Queue.take(fixture.results);
    const late = yield* declineMessage(bob, [alice], captured.operationId, 14);
    yield* fixture.service.receive(late);
    expect(result.sources.answers).toEqual([]);
    expect(
      result.item.kind === "result" ? result.item.result : result.item,
    ).toMatchObject({
      outcomes: [{ member: bob, outcome: { kind: "no-answer" } }],
    });
  }));

it("publishes the all_gather close with both parts of every selected message identity", () =>
  Effect.gen(function* () {
    const fixture = yield* openCollectives();
    const captured = question(
      (yield* Clock.currentTimeMillis) + 5000,
      [bob, carol],
      "all_gather",
    );
    const consuming = yield* fixture.collect();
    yield* fixture.service.accept(captured);
    yield* Queue.take(fixture.sent);
    const one = yield* declineMessage(
      bob,
      [alice, carol],
      captured.operationId,
      15,
    );
    const two = yield* declineMessage(
      carol,
      [alice, bob],
      captured.operationId,
      15,
    );
    yield* fixture.service.receive(one);
    yield* fixture.service.receive(two);
    const close = yield* Queue.take(fixture.sent);
    const value = yield* readCollectiveValue(close.content);
    expect(value).toEqual(
      Option.some(
        expect.objectContaining({
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
    expect(result.sources.answers).toEqual([one, two]);
  }));

it("recovers a frozen result while its accepted close is still certifying", () =>
  Effect.gen(function* () {
    const fixture = yield* openCollectives({ pendingCompletion: true });
    const captured = question(
      (yield* Clock.currentTimeMillis) + 250,
      [bob, carol],
      "all_gather",
    );
    yield* fixture.service.accept(captured);
    yield* Queue.take(fixture.sent);
    const selected = yield* declineMessage(
      bob,
      [alice, carol],
      captured.operationId,
      43,
    );
    yield* fixture.service.receive(selected);
    const close = yield* Queue.take(fixture.sent);
    expect(yield* readCollectiveValue(close.content)).toEqual(
      Option.some(
        expect.objectContaining({
          included: [{ authorAgentId: bob.agentId, msgId: selected.msgId }],
        }),
      ),
    );
    const late = yield* declineMessage(
      carol,
      [alice, bob],
      captured.operationId,
      44,
    );
    yield* fixture.service.receive(late);
    yield* fixture.close;
    const restarted = yield* openCollectives({ database: fixture.database });
    yield* restarted.collect();
    const result = yield* Queue.take(restarted.results);
    expect(result.sources.answers).toEqual([selected]);
    expect(
      result.item.kind === "result" ? result.item.result : result.item,
    ).toMatchObject({
      outcomes: [
        { member: bob, outcome: { kind: "declined" } },
        { member: carol, outcome: { kind: "no-answer" } },
      ],
    });
  }));

it("rolls back an answer claim when Communication refuses its acceptance", () =>
  Effect.gen(function* () {
    const fixture = yield* openCollectives({
      agent: bob,
      send: () =>
        Effect.fail(new CommunicationError({ reason: "persistence-failed" })),
    });
    const operationId = collectiveOperationId(alice.agentId);
    yield* fixture.service.receive(
      yield* aliceRequest((yield* Clock.currentTimeMillis) + 5000, {
        operationId,
      }),
    );
    const refused = yield* declineMessage(bob, [alice], operationId, 45);
    expect(
      yield* fixture.service.answer(refused).pipe(Effect.flip),
    ).toMatchObject({ reason: "persistence-failed" });
    yield* fixture.close;
    const restarted = yield* openCollectives({
      database: fixture.database,
      agent: bob,
    });
    const accepted = yield* declineMessage(bob, [alice], operationId, 46);
    yield* restarted.service.answer(accepted);
    expect(yield* Queue.take(restarted.sent)).toEqual(accepted);
  }));

it("retains a received question and pending own answer without minting another message on restart", () =>
  Effect.gen(function* () {
    const fixture = yield* pendingBob();
    const operationId = collectiveOperationId(alice.agentId);
    const request = yield* aliceRequest(
      (yield* Clock.currentTimeMillis) + 5000,
      { op: "gather", recipients: [bob], operationId: operationId },
    );
    yield* fixture.service.receive(request);
    const message = yield* wireMessage(
      bob,
      [alice],
      {
        kind: "response",
        operationId,
        action: "accept",
        content: { slot: "mon" },
      },
      { byte: 17 },
    );
    yield* fixture.service.answer(message);
    expect(yield* Queue.take(fixture.sent)).toEqual(message);
    yield* fixture.close;
    const restarted = yield* openCollectives({
      database: fixture.database,
      agent: bob,
    });
    yield* restarted.service.answer(message);
    expect(yield* Queue.take(restarted.sent)).toEqual(message);
    expect(
      yield* restarted.service.getRequest(operationId).pipe(Effect.flip),
    ).toMatchObject({
      failure: { kind: "request-answered" },
    });
    const replacement = yield* declineMessage(bob, [alice], operationId, 18);
    expect(
      yield* restarted.service.answer(replacement).pipe(Effect.flip),
    ).toBeInstanceOf(RequestError);
  }));

it("replays a result when its handler failed before durable retirement", () =>
  Effect.gen(function* () {
    const fixture = yield* openCollectives();
    const captured = question((yield* Clock.currentTimeMillis) + 200);
    const attempted = yield* Deferred.make<ResultDelivery>();
    const consuming = yield* fixture.service
      .consumeResults((result) =>
        Deferred.succeed(attempted, result).pipe(
          Effect.zipRight(Effect.fail(new HandlerUnavailable())),
        ),
      )
      .pipe(Effect.forkScoped);
    yield* fixture.service.accept(captured);
    const first = yield* Deferred.await(attempted);
    yield* Fiber.await(consuming);
    yield* fixture.close;
    const restarted = yield* openCollectives({ database: fixture.database });
    yield* restarted.collect();
    expect(yield* Queue.take(restarted.results)).toEqual(first);
  }));

it("passes ordinary messages unchanged and consumes malformed collective content", () =>
  Effect.gen(function* () {
    const fixture = yield* openCollectives();
    const operationId = collectiveOperationId(alice.agentId);
    const valid = yield* declineMessage(bob, [alice], operationId, 19);
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
    const operationId = collectiveOperationId(alice.agentId);
    const close = yield* aliceClose(operationId, [], "deadline");
    yield* fixture.service.receive(close);
    const request = yield* aliceRequest(
      (yield* Clock.currentTimeMillis) + 5000,
      { op: "all_gather", recipients: [bob, carol], operationId: operationId },
    );
    yield* fixture.service.receive(request);
    yield* fixture.close;
    const restarted = yield* openCollectives({
      database: fixture.database,
      agent: bob,
    });
    const answer = yield* declineMessage(bob, [alice, carol], operationId, 22);
    yield* restarted.service.answer(answer);
    expect(yield* Queue.take(restarted.sent)).toEqual(answer);
  }));

it("waits for its own accepted answer before presenting a close that selects it", () =>
  Effect.gen(function* () {
    const fixture = yield* pendingBob();
    const { complete } = fixture;
    const operationId = collectiveOperationId(alice.agentId);
    const request = yield* aliceRequest(
      (yield* Clock.currentTimeMillis) + 5000,
      { op: "all_gather", recipients: [bob, carol], operationId: operationId },
    );
    yield* fixture.service.receive(request);
    yield* fixture.collect();
    const own = yield* wireMessage(
      bob,
      [alice, carol],
      {
        kind: "response",
        operationId,
        action: "accept",
        content: { slot: "mon" },
      },
      { byte: 26 },
    );
    yield* fixture.service.answer(own);
    yield* Queue.take(fixture.sent);
    const peer = yield* declineMessage(carol, [alice, bob], operationId, 27);
    yield* fixture.service.receive(peer);
    const close = yield* aliceClose(
      operationId,
      [
        { authorAgentId: bob.agentId, msgId: own.msgId },
        { authorAgentId: carol.agentId, msgId: peer.msgId },
      ],
      "settled",
    );
    yield* fixture.service.receive(close);
    yield* complete;
    const result = yield* Queue.take(fixture.results);
    expect(result.sources.request).toEqual(request);
    expect(result.sources.answers).toEqual([own, peer]);
  }));

it("ends a malformed close with repeated authors without reopening its answer slot", () =>
  Effect.gen(function* () {
    const fixture = yield* openCollectives({ agent: bob });
    const operationId = collectiveOperationId(alice.agentId);
    const request = yield* aliceRequest(
      (yield* Clock.currentTimeMillis) + 5000,
      { op: "all_gather", recipients: [bob, carol], operationId: operationId },
    );
    yield* fixture.service.receive(request);
    const peer = yield* declineMessage(carol, [alice, bob], operationId, 30);
    yield* fixture.service.receive(peer);
    const reference = { authorAgentId: peer.author.agentId, msgId: peer.msgId };
    const close = yield* aliceClose(
      operationId,
      [reference, reference],
      "deadline",
    );
    yield* fixture.service.receive(close);
    yield* fixture.close;
    const reopened = yield* openCollectives({
      database: fixture.database,
      agent: bob,
    });
    const own = yield* declineMessage(bob, [alice, carol], operationId, 32);
    expect(
      yield* reopened.service.answer(own).pipe(Effect.flip),
    ).toBeInstanceOf(RequestError);
  }));

it("refuses a new answer when the captured request expires before custody", () =>
  Effect.gen(function* () {
    const fixture = yield* openCollectives({ agent: bob });
    const operationId = collectiveOperationId(alice.agentId);
    const deadlineAt = (yield* Clock.currentTimeMillis) + 100;
    const request = yield* aliceRequest(deadlineAt, {
      op: "gather",
      recipients: [bob],
      operationId: operationId,
    });
    yield* fixture.service.receive(request);
    const answer = yield* declineMessage(bob, [alice], operationId, 34);
    yield* Effect.sleep(150);
    expect(
      yield* fixture.service.answer(answer).pipe(Effect.flip),
    ).toBeInstanceOf(RequestError);
  }));

it("does not admit a request under another author's operation namespace", () =>
  Effect.gen(function* () {
    const fixture = yield* openCollectives();
    const operationId = collectiveOperationId(bob.agentId);
    const forged = yield* wireMessage(
      carol,
      [alice],
      {
        kind: "operation",
        op: "gather",
        operationId,
        deadlineAt: (yield* Clock.currentTimeMillis) + 5000,
        requestedSchema: form,
      },
      { byte: 35, text: "Forged request" },
    );
    expect(yield* fixture.service.receive(forged)).toEqual(Option.none());
    const honest = yield* wireMessage(
      bob,
      [alice],
      {
        kind: "operation",
        op: "gather",
        operationId,
        deadlineAt: (yield* Clock.currentTimeMillis) + 5000,
        requestedSchema: form,
      },
      { byte: 36, text: "Actual request" },
    );
    yield* fixture.service.receive(honest);
    expect(yield* fixture.service.getRequest(operationId)).toMatchObject({
      author: bob,
      msgId: honest.msgId,
    });
  }));

it("publishes independent results while incoming handlers share SQL transactions with workers", () =>
  Effect.gen(function* () {
    const fixture = yield* openCollectives();
    yield* fixture.collect();
    const deadline = (yield* Clock.currentTimeMillis) + 5000;
    const questions = [37, 38].map((byte) => ({
      ...question(deadline),
      operationId: collectiveOperationId(alice.agentId, byte),
    }));
    yield* Effect.forEach(
      questions,
      (captured) => fixture.service.accept(captured),
      { concurrency: 2 },
    );
    yield* Queue.take(fixture.sent);
    yield* Queue.take(fixture.sent);
    const responses = yield* Effect.forEach(
      questions,
      (captured, index) =>
        declineMessage(bob, [alice], captured.operationId, 39 + index),
      { concurrency: 1 },
    );
    yield* Effect.forEach(
      responses,
      (message) =>
        fixture.database.run(transaction(fixture.service.receive(message))),
      { concurrency: 2 },
    );
    const results = [
      yield* Queue.take(fixture.results),
      yield* Queue.take(fixture.results),
    ];
    expect(
      new Map(
        results.map((result) => [result.operationId, result.sources.answers]),
      ),
    ).toEqual(
      new Map(
        responses.map((response, index) => [
          questions[index]?.operationId,
          [response],
        ]),
      ),
    );
  }));

it.prop(
  "preserves ordinary text for every message passed through the collective boundary",
  [fc.string()],
  ([text]) =>
    Effect.gen(function* () {
      const fixture = yield* openCollectives();
      const header = yield* declineMessage(
        bob,
        [alice],
        collectiveOperationId(alice.agentId),
        41,
      );
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
