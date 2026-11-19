/** @file An agent's inbox hands over allowed entries in the order they became readable, from the position its host committed, and stays scoped to its agent. */
import { scopedLive as it } from "@effect/vitest";
import { Array as Arr, Effect, FastCheck as fc, Schema } from "effect";
import { expect } from "vitest";
import { testAgentCard } from "../__tests__/agents.js";
import { Message } from "../communication/index.js";
import { compareAscii } from "../encoding.js";
import { MessageAddress } from "../mcp/tools.js";
import { openTestDatabase } from "../test.js";
import { encodeCursor, type Inbox, makeInbox } from "./inbox.js";

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
      yield* Schema.decodeUnknown(MessageAddress)(`agent:${peer}`),
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
      /** Which earlier page's cursor the read presents, counted back from the latest; none when absent. */
      cursor: fc.option(fc.nat({ max: 3 }), { nil: undefined }),
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
  /** Each cursor a read returned, with the count of readable entries it commits. */
  readonly cursors: Array<{ cursor: string; through: number }>;
  committed: number;
}

/** Presents the cursor `back` pages before the latest and expects it committed, or refused when the position is already past it. */
function commit(inbox: Inbox, expected: Expected, back: number) {
  return Effect.gen(function* () {
    const presented = expected.cursors.at(-1 - back);
    if (presented === undefined) {
      return;
    }
    const outcome = yield* inbox
      .commitPosition(presented.cursor)
      .pipe(Effect.either);
    if (presented.through < expected.committed) {
      expect(outcome).toMatchObject({ left: { reason: "invalid-cursor" } });
    } else {
      expect(outcome._tag).toBe("Right");
      expected.committed = presented.through;
    }
  });
}

/** Reads a page and expects every readable entry after the committed position, in order, with a cursor exactly when there are any. */
function read(inbox: Inbox, expected: Expected) {
  return Effect.gen(function* () {
    const page = yield* inbox.listInbox();
    expect(page.entries.map((entry) => entry.message.msgId)).toEqual(
      expected.readable.slice(expected.committed),
    );
    expect(page.nextCursor !== undefined).toBe(page.entries.length > 0);
    if (page.nextCursor !== undefined) {
      expected.cursors.push({
        cursor: page.nextCursor,
        through: expected.readable.length,
      });
    }
  });
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
      if (step.cursor !== undefined) {
        yield* commit(inbox, expected, step.cursor);
      }
      yield* read(inbox, expected);
    }
  });
}

it("returns every allowed entry, in the order the entries were allowed, on each read until a cursor at or past it is committed, never after, and refuses a cursor behind the committed position", () =>
  Effect.gen(function* () {
    const database = yield* openTestDatabase();
    let agents = 0;
    const holds = (sequence: readonly Step[]) =>
      Effect.gen(function* () {
        agents += 1;
        const inbox = yield* makeInbox(`agent-${String(agents)}`);
        const expected: Expected = { readable: [], cursors: [], committed: 0 };
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
    const first = yield* inbox.listInbox();
    expect(
      first.entries.map((entry) => entry.message.msgId).sort(compareAscii),
    ).toEqual([...sent].sort(compareAscii));
    expect(yield* inbox.position).toEqual({
      newest: names.length,
      committed: 0,
    });
  }).pipe((test) =>
    Effect.flatMap(openTestDatabase(), (database) => database.run(test)),
  ));

it("refuses a cursor that is malformed or ahead of the newest entry, and leaves the committed position where it was", () =>
  Effect.gen(function* () {
    const inbox = yield* makeInbox("bob");
    yield* arrive(inbox, "alice");
    yield* decide(inbox, "alice", allowed);
    const ahead = yield* encodeCursor(
      Schema.Struct({ operation: Schema.String, after: Schema.Number }),
      { operation: "inbox", after: 2 },
    );
    for (const cursor of ["invalid", ahead]) {
      expect(
        yield* inbox.commitPosition(cursor).pipe(Effect.flip),
      ).toMatchObject({ reason: "invalid-cursor" });
    }
    expect((yield* inbox.listInbox()).entries).toHaveLength(1);
  }).pipe((test) =>
    Effect.flatMap(openTestDatabase(), (database) => database.run(test)),
  ));

it("stores the same message for each agent, and one agent's decision and committed position leave the other's as they were", () =>
  Effect.gen(function* () {
    const first = yield* makeInbox("first");
    const second = yield* makeInbox("second");
    const address = yield* Schema.decodeUnknown(MessageAddress)("agent:alice");
    const message = yield* Message.create({
      author: testAgentCard("agent:alice"),
      recipients: [testAgentCard("agent:bob")],
      content: [{ type: "text", text: "Can we meet tomorrow?" }],
    });
    yield* first.storeUnchecked(address, message);
    yield* second.storeUnchecked(address, message);
    yield* decide(first, "alice", allowed);
    expect(
      (yield* first.visibleMessages([message]))[0]?.visibleToAgent,
    ).toBeTruthy();
    expect(
      (yield* second.visibleMessages([message]))[0]?.visibleToAgent,
    ).toBeFalsy();
    expect((yield* second.listInbox()).entries).toEqual([]);
    yield* decide(second, "alice", allowed);
    const { nextCursor } = yield* first.listInbox();
    if (nextCursor === undefined) {
      return yield* Effect.dieMessage("Expected a cursor with the entry");
    }
    yield* first.commitPosition(nextCursor);
    expect((yield* first.listInbox()).entries).toEqual([]);
    const reopened = yield* makeInbox("second");
    expect((yield* reopened.listInbox()).entries).toHaveLength(1);
  }).pipe((test) =>
    Effect.flatMap(openTestDatabase(), (database) => database.run(test)),
  ));
