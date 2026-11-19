/** @file Messages between a server agent and its clients on one daemon. */
import { scopedLive as it } from "@effect/vitest";
import { Array as Arr, Effect, FastCheck as fc, Record } from "effect";
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

const readable = { readable: true };

it("retains a client/server exchange and puts each message only in its recipient's inbox", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const request = yield* f.message("hello");
    yield* f.client.send(request, readable);
    expect((yield* f.inbox(f.agents.alice, "0")).ids).toEqual([request.msgId]);
    expect((yield* f.inbox(f.agents.bob, "0")).ids).toEqual([]);
    const reply = yield* Message.create({
      author: request.recipients[0],
      recipients: [request.author],
      content: [{ type: "text", text: "reply" }],
    });
    yield* f.server.send(reply, readable);
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
        f.client
          .send(cancelled, readable)
          .pipe(Effect.zipRight(Effect.fail("cancel"))),
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
    yield* Effect.all(
      [f.client.send(first, readable), f.server.send(second, readable)],
      { concurrency: 2 },
    );
    const server = yield* f.history(f.server, f.members);
    expect(server).toEqual(yield* f.history(f.client, f.members));
    expect(server).toHaveLength(2);
  }));

const steps = fc.array(
  fc.oneof(
    fc.record({
      step: fc.constant("send" as const),
      author: fc.constantFrom("bob" as const, "carol" as const),
      readable: fc.boolean(),
    }),
    fc.record({
      step: fc.constant("read" as const),
      /** Which earlier read's cursor the read presents, counted back from the latest; none when absent. */
      cursor: fc.option(fc.nat({ max: 5 }), { nil: undefined }),
    }),
  ),
  { maxLength: 40 },
);

it("returns, for any cursor a read gave, every message that became readable since that read in the order it did, and none for a read without a cursor", () =>
  Effect.gen(function* () {
    const f = yield* conversationFixture;
    /** The ids of the messages alice may read, in the order they were stored. */
    const expected: string[] = [];
    /** Each cursor a read returned, with the count of messages readable when it did. */
    const cursors: Array<{ cursor: string; through: number }> = [];
    const holds = fc.asyncProperty(steps, (sequence) =>
      Effect.gen(function* () {
        for (const step of sequence) {
          if (step.step === "send") {
            const message = yield* Message.create({
              author: f.cards[step.author],
              recipients: [f.cards.alice],
              content: [{ type: "text", text: "Can we meet tomorrow?" }],
            });
            yield* f.hosted[step.author].send(message, step);
            if (step.readable) {
              expected.push(message.msgId);
            }
          } else {
            const presented =
              step.cursor === undefined
                ? undefined
                : cursors.at(-1 - step.cursor);
            const read = yield* f.inbox(f.agents.alice, presented?.cursor);
            expect(read.ids).toEqual(
              expected.slice(presented?.through ?? expected.length),
            );
            cursors.push({ cursor: read.nextCursor, through: expected.length });
          }
        }
      }).pipe(Effect.runPromise),
    );
    yield* Effect.tryPromise(() => fc.assert(holds, { numRuns: 30 }));
  }));

it("gives messages stored at the same time distinct places in their recipient's read order", () =>
  Effect.gen(function* () {
    const f = yield* conversationFixture;
    const sent = yield* Effect.forEach(
      Arr.makeBy(12, (index) => (index % 2 === 0 ? "bob" : "carol")),
      (author) =>
        Effect.gen(function* () {
          const message = yield* Message.create({
            author: f.cards[author],
            recipients: [f.cards.alice],
            content: [{ type: "text", text: "hello" }],
          });
          yield* f.hosted[author].send(message, readable);
          return message.msgId;
        }),
      { concurrency: 12 },
    );
    const read = yield* f.inbox(f.agents.alice, "0");
    expect(new Set(read.ids)).toEqual(new Set(sent));
    expect(read.nextCursor).toBe("12");
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
          const refusal = yield* f.hosted[caller].send(message, readable).pipe(
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
