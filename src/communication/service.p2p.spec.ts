/** @file Group messages, and messages between any agents, on one daemon. */

import { scopedLive as it } from "@effect/vitest";
import {
  Array as Arr,
  Chunk,
  Effect,
  FastCheck as fc,
  Queue,
  Record,
  Schema,
} from "effect";
import { expect } from "vitest";
import type { Conversation } from "./service.js";
import { testAgentCard } from "../__tests__/agents.js";
import { conversationFixture } from "./__tests__/conversation-fixtures.js";
import { Message } from "./messages.js";

it("carries a group message to every member in one order, between any agents", () =>
  Effect.gen(function* () {
    const f = yield* conversationFixture;
    const bobInbox = yield* f.received(f.bob);
    const carolInbox = yield* f.received(f.carol);
    const first = yield* Message.create({
      author: f.cards.alice,
      recipients: [f.cards.bob, f.cards.carol],
      content: [{ type: "text", text: "to the group" }],
    });
    const second = yield* Message.create({
      author: f.cards.bob,
      recipients: [f.cards.alice, f.cards.carol],
      content: [{ type: "text", text: "from another member" }],
    });
    yield* f.alice.send(first);
    yield* f.bob.send(second);
    expect(yield* Queue.take(bobInbox)).toEqual(first);
    expect(yield* Queue.takeN(carolInbox, 2)).toEqual(
      Chunk.make(first, second),
    );
    const direct = yield* Message.create({
      author: f.cards.bob,
      recipients: [f.cards.carol],
      content: [{ type: "text", text: "between two members" }],
    });
    yield* f.bob.send(direct);
    expect(yield* Queue.take(carolInbox)).toEqual(direct);
  }));

it("appends a message once to the history of each member of its conversation, and of no one else, exactly when its author sends it to agents on the daemon", () =>
  Effect.gen(function* () {
    const f = yield* conversationFixture;
    const people = { ...f.cards, dave: testAgentCard("dave") };
    const names = Record.keys(people);
    /** A send through `caller`'s service of a message it wrote, or one `author` wrote when that names an agent. */
    const sends = fc
      .record({
        caller: fc.constantFrom(...Record.keys(f.hosted)),
        author: fc.oneof(
          { weight: 3, arbitrary: fc.constant(undefined) },
          { weight: 1, arbitrary: fc.constantFrom(...names) },
        ),
        to: fc.subarray(names).filter(Arr.isNonEmptyArray),
      })
      .filter(({ caller, author = caller, to }) => !to.includes(author));
    const property = fc.asyncProperty(
      sends,
      ({ caller, author = caller, to }) =>
        Effect.gen(function* () {
          const message = yield* Message.create({
            author: people[author],
            recipients: Arr.map(to, (name) => people[name]),
            content: [{ type: "text", text: "hello" }],
          });
          const members: Conversation = [message.author, ...message.recipients];
          const admitted =
            author === caller && to.every((name) => name in f.hosted);
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
            (name) => name !== author && !to.includes(name),
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
    const write = (
      author: Name,
      recipients: Arr.NonEmptyReadonlyArray<Name>,
      text: string,
    ) =>
      Message.create({
        author: f.cards[author],
        recipients: Arr.map(recipients, (name) => f.cards[name]),
        content: [{ type: "text", text }],
      });
    /** An agent and two choices of the other agents to write to. */
    const sends = fc.constantFrom(...names).chain((author) => {
      const others = fc
        .subarray(names.filter((name) => name !== author))
        .filter(Arr.isNonEmptyArray);
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
        const members: Conversation = [message.author, ...message.recipients];
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
    /** Sender and recipients combine in 27 ways, and every run lengthens the histories the next one reads. */
    const numRuns = 30;
    yield* Effect.tryPromise(() => fc.assert(property, { numRuns }));
  }));
