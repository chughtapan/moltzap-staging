/** @file Messages between a server agent and its clients on one daemon. */
import { scopedLive as it } from "@effect/vitest";
import { Effect, FastCheck as fc, Queue, Record, Schema } from "effect";
import { expect } from "vitest";
import { testAgentCard } from "../__tests__/agents.js";
import { conversationFixture } from "./__tests__/conversation-fixtures.js";
import { Message } from "./messages.js";

/** Alice as a server agent and Bob as its client. */
const fixture = Effect.gen(function* () {
  const f = yield* conversationFixture;
  const members = [f.cards.alice, f.cards.bob];
  const message = (text: string) =>
    Message.create({
      author: f.cards.bob,
      recipients: [f.cards.alice],
      content: [{ type: "text", text }],
    });
  return { ...f, server: f.alice, client: f.bob, members, message };
});

it("retains a client/server exchange and hands each message only to its recipient", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const request = yield* f.message("hello");
    yield* f.client.send(request);
    const inbox = yield* f.received(f.server);
    expect(yield* Queue.take(inbox)).toEqual(request);
    const reply = yield* Message.create({
      author: request.recipients[0],
      recipients: [request.author],
      content: [{ type: "text", text: "reply" }],
    });
    yield* f.server.send(reply);
    expect(yield* f.client.listConversations).toEqual([f.members]);
    const reopened = yield* f.open(f.agents.bob);
    expect(yield* f.history(reopened, f.members)).toEqual([request, reply]);
  }));

it("rolls back both members' views and serializes concurrent sends", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const cancelled = yield* f.message("rolled back");
    yield* f.sql
      .withTransaction(
        f.client.send(cancelled).pipe(Effect.zipRight(Effect.fail("cancel"))),
      )
      .pipe(Effect.either);
    for (const agent of [f.server, f.client]) {
      expect(yield* f.history(agent, f.members)).toEqual([]);
    }
    const first = yield* f.message("client");
    const second = yield* Message.create({
      author: first.recipients[0],
      recipients: [first.author],
      content: [{ type: "text", text: "server" }],
    });
    yield* Effect.all([f.client.send(first), f.server.send(second)], {
      concurrency: 2,
    });
    const server = yield* f.history(f.server, f.members);
    expect(server).toEqual(yield* f.history(f.client, f.members));
    expect(server).toHaveLength(2);
  }));

it("hands a message to the next consumer after one fails", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const message = yield* f.message("durable before delivery");
    yield* f.client.send(message);
    const consumerFailure = new Error("handler failed");
    expect(
      yield* f.server
        .consume(() => Effect.fail(consumerFailure))
        .pipe(Effect.flip),
    ).toBe(consumerFailure);
    const reopened = yield* f.open(f.agents.alice);
    expect(
      yield* reopened
        .consume((received) => Effect.fail(received))
        .pipe(Effect.flip),
    ).toEqual(message);
  }));

it("appends a message once to the history of the server agent and the client it names, and of no one else, exactly when its author sends it to an agent on the daemon", () =>
  Effect.gen(function* () {
    const f = yield* conversationFixture;
    const people = { ...f.cards, dave: testAgentCard("dave") };
    const names = Record.keys(people);
    /** A send through `caller`'s service of a message it wrote, or one `author` wrote when that names an agent. Alice, the server agent, writes it or receives it. */
    const sends = fc
      .record({
        caller: fc.constantFrom(...Record.keys(f.hosted)),
        author: fc.oneof(
          { weight: 3, arbitrary: fc.constant(undefined) },
          { weight: 1, arbitrary: fc.constantFrom(...names) },
        ),
        to: fc.constantFrom(...names),
      })
      .filter(
        ({ caller, author = caller, to }) =>
          to !== author && (to === "alice" || author === "alice"),
      );
    const property = fc.asyncProperty(
      sends,
      ({ caller, author = caller, to }) =>
        Effect.gen(function* () {
          const message = yield* Message.create({
            author: people[author],
            recipients: [people[to]],
            content: [{ type: "text", text: "hello" }],
          });
          const members = [message.author, ...message.recipients];
          const admitted = author === caller && to in f.hosted;
          const before = yield* f.histories(members);
          const refusal = yield* f.hosted[caller].send(message).pipe(
            Effect.as("none"),
            Effect.catchTag("CommunicationError", (refused) =>
              Effect.succeed(refused.reason),
            ),
          );
          expect(refusal).toBe(admitted ? "none" : "membership-invalid");
          expect(yield* f.histories(members)).toEqual(
            Record.map(before, (earlier) =>
              admitted ? [...earlier, message] : earlier,
            ),
          );
          const outsiders = Record.keys(f.hosted).filter(
            (name) => name !== author && name !== to,
          );
          for (const outsider of outsiders) {
            expect(
              yield* f.history(f.hosted[outsider], members).pipe(Effect.flip),
            ).toMatchObject({ reason: "membership-invalid" });
          }
        }).pipe(Effect.runPromise),
    );
    yield* Effect.tryPromise(() => fc.assert(property));
  }));

it("keeps one copy of a message its author sends again, and refuses a different message under the same identifier", () =>
  Effect.gen(function* () {
    const f = yield* conversationFixture;
    const names = Record.keys(f.hosted);
    type Name = (typeof names)[number];
    const write = (author: Name, recipient: Name, text: string) =>
      Message.create({
        author: f.cards[author],
        recipients: [f.cards[recipient]],
        content: [{ type: "text", text }],
      });
    /** An agent and two choices of whom it writes to: a client for Alice, the server agent, and Alice for a client. */
    const sends = fc.constantFrom(...names).chain((author) => {
      const others = fc.constantFrom(
        ...names.filter((name) => (name === "alice") !== (author === "alice")),
      );
      return fc.record({
        author: fc.constant(author),
        to: others,
        elsewhere: others,
      });
    });
    const property = fc.asyncProperty(sends, ({ author, to, elsewhere }) =>
      Effect.gen(function* () {
        const message = yield* write(author, to, "original");
        const different = yield* Schema.decodeUnknown(Message)({
          ...(yield* write(author, elsewhere, "changed")),
          msgId: message.msgId,
        });
        const members = [message.author, ...message.recipients];
        const before = yield* f.histories(members);
        yield* f.hosted[author].send(message);
        yield* f.hosted[author].send(message);
        expect(
          yield* f.hosted[author].send(different).pipe(Effect.flip),
        ).toMatchObject({ reason: "message-conflict" });
        expect(yield* f.histories(members)).toEqual(
          Record.map(before, (earlier) => [...earlier, message]),
        );
        const reused = yield* f.histories([
          different.author,
          ...different.recipients,
        ]);
        for (const history of Record.values(reused)) {
          expect(history).not.toContainEqual(different);
        }
      }).pipe(Effect.runPromise),
    );
    /** Every run lengthens the histories the next one reads. */
    const numRuns = 30;
    yield* Effect.tryPromise(() => fc.assert(property, { numRuns }));
  }));
