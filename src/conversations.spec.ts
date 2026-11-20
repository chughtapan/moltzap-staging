/**
 * @file Conversations prepare a database without touching what they cannot
 * adopt, give each owner one agent, hold each message once for both of its
 * agents, and give a recipient its readable messages in the order they
 * became readable.
 */
import { scopedLive as it } from "@effect/vitest";
import { Array as Arr, Effect, FastCheck as fc } from "effect";
import { expect } from "vitest";
import type { Firewall } from "./firewall.js";
import { openTestDatabase } from "./__tests__/test.js";
import {
  agentOf,
  history,
  openConversations,
  peersOf,
  readInbox,
  send,
} from "./conversations.js";
import { AgentName } from "./messages.js";

const issuer = "urn:social-harness:test";

/** The decision of a firewall that withholds exactly the messages whose text is `withheld`. */
const byText: Firewall["checkIncoming"] = (message) =>
  Effect.succeed(
    message.text === "withheld"
      ? {
          kind: "refused",
          checkId: "chk_withheld",
          reason: "rule-violation",
          feedback: [],
        }
      : { kind: "allowed" },
  );

/** Conversations opened on `database`, with Alice as their server agent. */
const openOn = (
  database: Effect.Effect.Success<ReturnType<typeof openTestDatabase>>,
) =>
  openConversations(database.url, {
    owner: { issuer, subject: "alice" },
    name: AgentName.make("alice"),
    firewall: () => ({ checkIncoming: byText, checkOutgoing: byText }),
  });

/** Alice, the server agent, and her clients Bob and Carol, with what a test does as them. */
const fixture = Effect.gen(function* () {
  const conversations = yield* openOn(yield* openTestDatabase());
  const join = (name: string) =>
    agentOf(conversations, { issuer, subject: name }, AgentName.make(name));
  const agents = {
    alice: conversations.serverAgent,
    bob: yield* join("bob"),
    carol: yield* join("carol"),
  };
  type Name = keyof typeof agents;
  return {
    conversations,
    agents,
    /** Sends `text` from one agent to another and gives the message's id. */
    send: (from: Name, to: Name, text = "hello") =>
      send(conversations, {
        author: agents[from],
        recipient: agents[to],
        text,
      }),
    /** The ids of the messages an agent may read after `cursor`, in read order, with the cursor to read on from. */
    inbox: (name: Name, cursor?: string) =>
      readInbox(conversations, agents[name].agentId, {
        limit: 100,
        cursor,
      }).pipe(
        Effect.map(({ entries, nextCursor }) => ({
          ids: entries.map(({ message }) => message.msgId),
          nextCursor,
        })),
      ),
    /** The ids of the first page of one agent's history with another. */
    history: (self: Name, other: Name) =>
      Effect.map(history(conversations, agents[self], agents[other]), (page) =>
        page.records.map((record) => record.message.msgId),
      ),
  };
});

it("prepares an empty database and leaves a prepared one as it is", () =>
  Effect.gen(function* () {
    const database = yield* openTestDatabase();
    const first = yield* openOn(database);
    const again = yield* openOn(database);
    expect(again.serverAgent).toEqual(first.serverAgent);
    expect(yield* database.sql`SELECT * FROM agent_cards`).toHaveLength(1);
  }));

it("rejects a database that holds other tables without changing their records", () =>
  Effect.gen(function* () {
    const database = yield* openTestDatabase();
    yield* database.sql`CREATE TABLE retained (value TEXT NOT NULL)`;
    yield* database.sql`INSERT INTO retained VALUES ('prior installation')`;
    expect(yield* openOn(database).pipe(Effect.flip)).toMatchObject({
      reason: "schema",
    });
    expect(yield* database.sql`SELECT * FROM retained`).toEqual([
      { value: "prior installation" },
    ]);
    expect(
      yield* database.sql`SELECT tablename AS name FROM pg_tables WHERE schemaname=current_schema()`,
    ).toEqual([{ name: "retained" }]);
  }));

const accounts = fc.array(
  fc.record({
    issuer: fc.constantFrom("https://issuer.example", "https://other.example"),
    subject: fc.constantFrom("owner", "another", "third"),
  }),
  { minLength: 1, maxLength: 8 },
);

it.prop(
  "binds each issuer and subject pair to one agent of its own, however many connections arrive at once or again",
  [accounts],
  ([owners]) =>
    Effect.gen(function* () {
      const conversations = yield* openOn(yield* openTestDatabase());
      const account = (owner: (typeof owners)[number]) =>
        `${owner.issuer} ${owner.subject}`;
      const distinct = new Set(owners.map(account)).size;
      const agents = yield* Effect.all(
        owners.map((each) => agentOf(conversations, each)),
        { concurrency: owners.length },
      );
      const again = yield* Effect.forEach(
        owners,
        (each) => agentOf(conversations, each),
        { concurrency: 1 },
      );
      expect(again).toEqual(agents);
      expect(
        new Set(
          owners.map(
            (each, index) => `${account(each)} ${agents[index]?.agentId ?? ""}`,
          ),
        ).size,
      ).toBe(distinct);
      for (const identifiers of [
        agents.map((agent): string => agent.agentId),
        agents.map((agent): string => agent.principalId),
      ]) {
        expect(new Set(identifiers).size).toBe(distinct);
      }
    }),
  { fastCheck: { numRuns: 25 } },
);

it("holds a client/server exchange once for its two agents only and puts each message in its recipient's inbox", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const request = yield* f.send("bob", "alice");
    expect((yield* f.inbox("alice", "0")).ids).toEqual([request]);
    expect((yield* f.inbox("bob", "0")).ids).toEqual([]);
    const reply = yield* f.send("alice", "bob");
    expect(yield* peersOf(f.conversations, f.agents.bob)).toEqual(["alice"]);
    expect(yield* f.history("alice", "bob")).toEqual([request, reply]);
    expect(yield* f.history("bob", "alice")).toEqual([request, reply]);
    expect(yield* f.history("alice", "carol")).toEqual([]);
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
            const id = yield* f.send(
              step.author,
              "alice",
              step.readable ? "hello" : "withheld",
            );
            if (step.readable) {
              expected.push(id);
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
      (author) => f.send(author, "alice"),
      { concurrency: 12 },
    );
    const read = yield* f.inbox("alice", "0");
    expect(new Set(read.ids)).toEqual(new Set(sent));
    expect(read.nextCursor).toBe("12");
  }));
