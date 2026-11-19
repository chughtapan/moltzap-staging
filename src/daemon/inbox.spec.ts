/** @file An agent's inbox hands over the entries after a cursor in the order they were stored, and stays scoped to its agent. */
import { scopedLive as it } from "@effect/vitest";
import { Array as Arr, Effect, FastCheck as fc, Schema } from "effect";
import { expect } from "vitest";
import { testAgentCard } from "../__tests__/agents.js";
import { Message } from "../communication/index.js";
import { compareAscii } from "../encoding.js";
import { AgentAddress } from "../mcp/tools.js";
import { openTestDatabase } from "../test.js";
import { type Inbox, makeInbox } from "./inbox.js";

/** A new message from `peer` to bob. */
function messageFrom(peer: string) {
  return Message.create({
    author: testAgentCard(peer),
    recipients: [testAgentCard("agent:bob")],
    content: [{ type: "text", text: "Can we meet tomorrow?" }],
  });
}

/** Stores `message` as an entry of the inbox. */
function store(inbox: Inbox, message: Message) {
  return Effect.flatMap(
    Schema.decodeUnknown(AgentAddress)(`agent:${message.author.agentName}`),
    (address) => inbox.store(address, message),
  );
}

/** Stores a new message from `peer` and returns its id. */
function arrive(inbox: Inbox, peer: string) {
  return Effect.gen(function* () {
    const message = yield* messageFrom(peer);
    yield* store(inbox, message);
    return message.msgId;
  });
}

const peers = fc.constantFrom("alice", "carol", "dave");
const steps = fc.array(
  fc.oneof(
    fc.record({ step: fc.constant("arrive" as const), peer: peers }),
    fc.record({
      step: fc.constant("read" as const),
      /** Which earlier read's cursor the read presents, counted back from the latest; none when absent. */
      cursor: fc.option(fc.nat({ max: 5 }), { nil: undefined }),
    }),
  ),
  { maxLength: 40 },
);
type Step =
  typeof steps extends fc.Arbitrary<ReadonlyArray<infer One>> ? One : never;

/** What an inbox should hold after the steps taken on it. */
interface Expected {
  /** The ids of the stored messages, in the order they were stored. */
  readonly readable: string[];
  /** Each cursor a read returned, with the count of entries readable when it did. */
  readonly cursors: Array<{ cursor: string; through: number }>;
}

function take(inbox: Inbox, expected: Expected, step: Step) {
  return Effect.gen(function* () {
    if (step.step === "arrive") {
      expected.readable.push(yield* arrive(inbox, step.peer));
    } else {
      const presented =
        step.cursor === undefined
          ? undefined
          : expected.cursors.at(-1 - step.cursor);
      const { entries, nextCursor } = yield* inbox.listInbox(
        100,
        presented?.cursor,
      );
      expect(entries.map((entry) => entry.message.msgId)).toEqual(
        expected.readable.slice(presented?.through ?? expected.readable.length),
      );
      expected.cursors.push({
        cursor: nextCursor,
        through: expected.readable.length,
      });
    }
  });
}

it("returns, for any cursor a read gave, every entry stored since that read in the order the entries were stored, and none for a read without a cursor", () =>
  Effect.gen(function* () {
    const database = yield* openTestDatabase();
    let agents = 0;
    const holds = (sequence: readonly Step[]) =>
      Effect.gen(function* () {
        agents += 1;
        const inbox = yield* makeInbox(`agent-${String(agents)}`);
        const expected: Expected = { readable: [], cursors: [] };
        yield* Effect.forEach(sequence, (step) => take(inbox, expected, step), {
          concurrency: 1,
          discard: true,
        });
      }).pipe(database.run, Effect.runPromise);
    yield* Effect.tryPromise(() => fc.assert(fc.asyncProperty(steps, holds)));
  }));

it("gives entries stored at the same time distinct places in the read order, and a message stored again no second place", () =>
  Effect.gen(function* () {
    const inbox = yield* makeInbox("bob");
    const messages = yield* Effect.forEach(
      Arr.makeBy(12, (index) => `peer-${String(index)}`),
      messageFrom,
      { concurrency: 1 },
    );
    yield* Effect.forEach(
      [...messages, ...messages],
      (message) => store(inbox, message),
      { concurrency: messages.length },
    );
    const read = yield* inbox.listInbox(100, "0");
    expect(
      read.entries.map((entry) => entry.message.msgId).sort(compareAscii),
    ).toEqual(messages.map((message) => message.msgId).sort(compareAscii));
    expect(read.nextCursor).toBe(String(messages.length));
  }).pipe((test) =>
    Effect.flatMap(openTestDatabase(), (database) => database.run(test)),
  ));

it("keeps one agent's entries out of another agent's inbox", () =>
  Effect.gen(function* () {
    const first = yield* makeInbox("first");
    const second = yield* makeInbox("second");
    yield* arrive(first, "alice");
    expect((yield* second.listInbox(1, "0")).entries).toEqual([]);
    expect((yield* first.listInbox(2, "0")).entries).toHaveLength(1);
  }).pipe((test) =>
    Effect.flatMap(openTestDatabase(), (database) => database.run(test)),
  ));
