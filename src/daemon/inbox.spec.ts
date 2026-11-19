/** @file An agent's inbox hands over the allowed entries after a cursor in the order they were allowed, and stays scoped to its agent. */
import { scopedLive as it } from "@effect/vitest";
import { Array as Arr, Effect, FastCheck as fc, Schema } from "effect";
import { expect } from "vitest";
import { testAgentCard } from "../__tests__/agents.js";
import { Message } from "../communication/index.js";
import { compareAscii } from "../encoding.js";
import { openTestDatabase } from "../test.js";
import { type Inbox, makeInbox } from "./inbox.js";
import { AgentAddress } from "./values.js";

const allowed = { kind: "allowed", save: Effect.void } as const;
const withheld = {
  kind: "withheld",
  reason: "rule-violation",
  save: Effect.void,
} as const;

/** Stores a new unchecked message from `peer` in its conversation with bob and returns its id. */
function arrive(inbox: Inbox, peer: string) {
  return Effect.gen(function* () {
    const message = yield* Message.create({
      author: testAgentCard(peer),
      recipients: [testAgentCard("agent:bob")],
      content: [{ type: "text", text: "Can we meet tomorrow?" }],
    });
    yield* inbox.storeUnchecked(
      yield* Schema.decodeUnknown(AgentAddress)(`agent:${peer}`),
      message,
    );
    return message.msgId;
  });
}

/** Records `decision` for the oldest unchecked message from `peer`, and returns that message's id when there was one. */
function decide(
  inbox: Inbox,
  peer: string,
  decision: typeof allowed | typeof withheld,
) {
  return Effect.gen(function* () {
    const unchecked = (yield* inbox.unchecked(10)).find(
      (candidate) => candidate.address === `agent:${peer}`,
    );
    if (unchecked === undefined) {
      return undefined;
    }
    yield* inbox.recordDecision(unchecked, decision);
    return unchecked.message.msgId;
  });
}

const peers = fc.constantFrom("alice", "carol", "dave");
const steps = fc.array(
  fc.oneof(
    fc.record({ step: fc.constant("arrive" as const), peer: peers }),
    fc.record({
      step: fc.constant("decide" as const),
      peer: peers,
      decision: fc.constantFrom(allowed, withheld),
    }),
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
  /** The ids of the allowed messages, in the order they were allowed. */
  readonly readable: string[];
  /** Each cursor a read returned, with the count of entries readable when it did. */
  readonly cursors: Array<{ cursor: string; through: number }>;
}

function take(inbox: Inbox, expected: Expected, step: Step) {
  return Effect.gen(function* () {
    if (step.step === "arrive") {
      yield* arrive(inbox, step.peer);
    } else if (step.step === "decide") {
      const decided = yield* decide(inbox, step.peer, step.decision);
      if (decided !== undefined && step.decision === allowed) {
        expected.readable.push(decided);
      }
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

it("returns, for any cursor a read gave, every entry allowed since that read in the order the entries were allowed, and none for a read without a cursor", () =>
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

it("gives entries allowed at the same time distinct places in the read order, and a message whose decision is recorded again no second place", () =>
  Effect.gen(function* () {
    const inbox = yield* makeInbox("bob");
    const names = Arr.makeBy(12, (index) => `peer-${String(index)}`);
    const sent = yield* Effect.forEach(names, (peer) => arrive(inbox, peer), {
      concurrency: 1,
    });
    const unchecked = yield* inbox.unchecked(names.length);
    yield* Effect.forEach(
      [...unchecked, ...unchecked],
      (message) => inbox.recordDecision(message, allowed),
      { concurrency: names.length },
    );
    const read = yield* inbox.listInbox(100, "0");
    expect(
      read.entries.map((entry) => entry.message.msgId).sort(compareAscii),
    ).toEqual([...sent].sort(compareAscii));
    expect(read.nextCursor).toBe(String(names.length));
  }).pipe((test) =>
    Effect.flatMap(openTestDatabase(), (database) => database.run(test)),
  ));

it("stores the same message for each agent, and one agent's decision leaves the other's inbox as it was", () =>
  Effect.gen(function* () {
    const first = yield* makeInbox("first");
    const second = yield* makeInbox("second");
    yield* arrive(first, "alice");
    yield* arrive(second, "alice");
    yield* decide(first, "alice", allowed);
    expect((yield* second.listInbox(1, "0")).entries).toEqual([]);
    yield* decide(second, "alice", allowed);
    expect((yield* second.listInbox(2, "0")).entries).toHaveLength(1);
    expect((yield* first.listInbox(2, "0")).entries).toHaveLength(1);
  }).pipe((test) =>
    Effect.flatMap(openTestDatabase(), (database) => database.run(test)),
  ));
