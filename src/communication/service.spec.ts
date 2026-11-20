/** @file Messages between a server agent and its clients on one daemon, as each holds them. */
import { scopedLive as it } from "@effect/vitest";
import { Array as Arr, Effect, FastCheck as fc } from "effect";
import { expect } from "vitest";
import type { AgentCard } from "../identity.js";
import { createTestAgent } from "../__tests__/agents.js";
import { openTestDatabase } from "../__tests__/test.js";
import { Message } from "./messages.js";
import {
  listConversations,
  readHistory,
  readInbox,
  sendMessage,
} from "./service.js";

type Name = "alice" | "bob" | "carol";

/** Alice, the server agent, and her clients Bob and Carol in one database, with what a test does as them. */
const fixture = Effect.gen(function* () {
  const { sql, run } = yield* openTestDatabase();
  const agents = {
    alice: yield* run(createTestAgent("alice")),
    bob: yield* run(createTestAgent("bob")),
    carol: yield* run(createTestAgent("carol")),
  };
  return {
    sql,
    run,
    agents,
    /** Sends a new message from one agent to another, held by its recipient as `received` says. */
    send: (from: Name, to: Name, received: { readonly readable: boolean }) =>
      Effect.gen(function* () {
        const message = yield* Message.create({
          author: agents[from].card,
          recipients: [agents[to].card],
          content: [{ type: "text", text: "hello" }],
        });
        yield* sendMessage(
          {
            authorKey: agents[from].agentKey,
            recipientKey: agents[to].agentKey,
          },
          message,
          received,
        );
        return message;
      }).pipe(run),
    /** The ids of the messages an agent may read after `cursor`, in read order, with the cursor to read on from. */
    inbox: (name: Name, cursor?: string) =>
      readInbox(agents[name].agentKey, { limit: 100, cursor }).pipe(
        Effect.map(({ messages, nextCursor }) => ({
          ids: messages.map(({ message }) => message.msgId),
          nextCursor,
        })),
        run,
      ),
    /** The first page of a conversation as one agent holds it. */
    history: (name: Name, members: readonly AgentCard[]) =>
      readHistory(agents[name].agentKey, { members }).pipe(
        Effect.map((page) => page.entries.map((entry) => entry.message)),
        run,
      ),
  };
});

const readable = { readable: true };

it("retains a client/server exchange for its two members only and puts each message in its recipient's inbox", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const members = [f.agents.alice.card, f.agents.bob.card];
    const request = yield* f.send("bob", "alice", readable);
    expect((yield* f.inbox("alice", "0")).ids).toEqual([request.msgId]);
    expect((yield* f.inbox("bob", "0")).ids).toEqual([]);
    const reply = yield* f.send("alice", "bob", readable);
    expect(yield* f.run(listConversations(f.agents.bob.agentKey))).toEqual([
      members,
    ]);
    for (const member of ["alice", "bob"] as const) {
      expect(yield* f.history(member, members)).toEqual([request, reply]);
    }
    expect(yield* f.history("carol", members)).toEqual([]);
  }));

it("rolls back both members' views and gives concurrent sends one order", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const members = [f.agents.alice.card, f.agents.bob.card];
    yield* f.sql
      .withTransaction(
        f
          .send("bob", "alice", readable)
          .pipe(Effect.zipRight(Effect.fail("cancel"))),
      )
      .pipe(Effect.either);
    for (const member of ["alice", "bob"] as const) {
      expect(yield* f.history(member, members)).toEqual([]);
    }
    yield* Effect.all(
      [f.send("bob", "alice", readable), f.send("alice", "bob", readable)],
      { concurrency: 2 },
    );
    const server = yield* f.history("alice", members);
    expect(server).toEqual(yield* f.history("bob", members));
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
    const f = yield* fixture;
    /** The ids of the messages alice may read, in the order they were stored. */
    const expected: string[] = [];
    /** Each cursor a read returned, with the count of messages readable when it did. */
    const cursors: Array<{ cursor: string; through: number }> = [];
    const holds = fc.asyncProperty(steps, (sequence) =>
      Effect.gen(function* () {
        for (const step of sequence) {
          if (step.step === "send") {
            const message = yield* f.send(step.author, "alice", step);
            if (step.readable) {
              expected.push(message.msgId);
            }
          } else {
            const presented =
              step.cursor === undefined
                ? undefined
                : cursors.at(-1 - step.cursor);
            const read = yield* f.inbox("alice", presented?.cursor);
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
    const f = yield* fixture;
    const sent = yield* Effect.forEach(
      Arr.makeBy(12, (index) => (index % 2 === 0 ? "bob" : "carol")),
      (author) => f.send(author, "alice", readable),
      { concurrency: 12 },
    );
    const read = yield* f.inbox("alice", "0");
    expect(new Set(read.ids)).toEqual(new Set(sent.map(({ msgId }) => msgId)));
    expect(read.nextCursor).toBe("12");
  }));
