/**
 * @file The store prepares a database without touching what it cannot adopt,
 * holds each message once for both of its agents, and gives a recipient its
 * readable messages in the order they became readable.
 */
import { Migrator, SqlClient } from "@effect/sql";
import { scopedLive as it } from "@effect/vitest";
import { Array as Arr, Effect, FastCheck as fc } from "effect";
import { expect } from "vitest";
import { createTestAgent } from "../__tests__/agents.js";
import { openTestDatabase } from "../__tests__/test.js";
import { Message } from "../messages.js";
import {
  conversationPeers,
  prepareDatabase,
  readConversation,
  readInbox,
  saveMessage,
} from "./store.js";

/** A database nothing has prepared, and the daemon's preparation of it. */
const unprepared = Effect.gen(function* () {
  const { sql, run } = yield* openTestDatabase({ prepared: false });
  const tables = sql<{
    name: string;
  }>`SELECT tablename AS name FROM pg_tables WHERE schemaname=current_schema()`;
  return { sql, tables, prepare: run(prepareDatabase) };
});

it("prepares a fresh database and leaves a prepared one as it is", () =>
  Effect.gen(function* () {
    const test = yield* unprepared;
    expect(yield* test.tables).toEqual([]);
    yield* test.prepare;
    yield* test.sql`INSERT INTO agent_cards (issuer, subject, agent_id, agent_name, principal_id) VALUES ('i', 's', 'a', 'n', 'p')`;
    yield* test.prepare;
    expect(yield* test.sql`SELECT * FROM agent_cards`).toHaveLength(1);
  }));

it("prepares a fresh database whose migration ledger already exists but is empty", () =>
  Effect.gen(function* () {
    const test = yield* unprepared;
    yield* Migrator.make({})({ loader: Effect.succeed([]) }).pipe(
      Effect.provideService(SqlClient.SqlClient, test.sql),
    );
    yield* test.prepare;
    expect(yield* test.sql`SELECT * FROM agent_cards`).toHaveLength(0);
  }));

it("rejects a database that holds other tables without changing their records", () =>
  Effect.gen(function* () {
    const test = yield* unprepared;
    yield* test.sql`CREATE TABLE retained (value TEXT NOT NULL)`;
    yield* test.sql`INSERT INTO retained VALUES ('prior installation')`;
    expect(yield* test.prepare.pipe(Effect.flip)).toMatchObject({
      reason: "schema",
    });
    expect(yield* test.sql`SELECT * FROM retained`).toEqual([
      { value: "prior installation" },
    ]);
    expect(yield* test.tables).toEqual([{ name: "retained" }]);
  }));

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
    /** Stores a new message from one agent to another, held by its recipient as `received` says. */
    send: (from: Name, to: Name, received: { readonly readable: boolean }) =>
      Effect.gen(function* () {
        const message = yield* Message.create({
          author: agents[from],
          recipient: agents[to],
          text: "hello",
        });
        yield* saveMessage(message, received);
        return message;
      }).pipe(run),
    /** The ids of the messages an agent may read after `cursor`, in read order, with the cursor to read on from. */
    inbox: (name: Name, cursor?: string) =>
      readInbox(agents[name].agentId, { limit: 100, cursor }).pipe(
        Effect.map(({ messages, nextCursor }) => ({
          ids: messages.map(({ message }) => message.msgId),
          nextCursor,
        })),
        run,
      ),
    /** The messages two agents sent each other, oldest first. */
    conversation: (one: Name, other: Name) =>
      readConversation([agents[one].agentId, agents[other].agentId]).pipe(
        Effect.map((stored) => stored.map(({ message }) => message)),
        run,
      ),
  };
});

const readable = { readable: true };

it("holds a client/server exchange once for its two agents only and puts each message in its recipient's inbox", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const request = yield* f.send("bob", "alice", readable);
    expect((yield* f.inbox("alice", "0")).ids).toEqual([request.msgId]);
    expect((yield* f.inbox("bob", "0")).ids).toEqual([]);
    const reply = yield* f.send("alice", "bob", readable);
    expect(yield* f.run(conversationPeers(f.agents.bob.agentId))).toEqual([
      "alice",
    ]);
    expect(yield* f.conversation("alice", "bob")).toEqual([request, reply]);
    expect(yield* f.conversation("bob", "alice")).toEqual([request, reply]);
    expect(yield* f.conversation("alice", "carol")).toEqual([]);
  }));

it("stores nothing of a message whose transaction fails, and both of two messages stored at the same time", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    yield* f.sql
      .withTransaction(
        f
          .send("bob", "alice", readable)
          .pipe(Effect.zipRight(Effect.fail("cancel"))),
      )
      .pipe(Effect.either);
    expect(yield* f.conversation("alice", "bob")).toEqual([]);
    yield* Effect.all(
      [f.send("bob", "alice", readable), f.send("alice", "bob", readable)],
      { concurrency: 2 },
    );
    expect(yield* f.conversation("alice", "bob")).toHaveLength(2);
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
