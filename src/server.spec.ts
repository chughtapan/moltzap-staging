/**
 * @file What agents observe of the server through their tools: the tools of
 * their role, each call's result or failure, sends and their checks, the
 * inbox, history and conversations; and what opening it does to its
 * database.
 */

import { scopedLive as it } from "@effect/vitest";
import {
  Array as Arr,
  Deferred,
  Effect,
  Either,
  FastCheck as fc,
  Fiber,
  Order,
  Struct,
} from "effect";
import { expect } from "vitest";
import type { DecisionProvider, Firewall } from "./firewall.js";
import {
  callTool,
  decisionState,
  inboxOf,
  inProcessServer,
  send,
  type SignedIn,
} from "./__tests__/daemon.js";
import { answersTo, type TestPolicy } from "./__tests__/firewall-fixtures.js";
import { openTestDatabase } from "./__tests__/test.js";
import { AgentName } from "./messages.js";
import { compileNorm } from "./norms/compiler.js";
import { serverFactory } from "./server.js";

it("gives the server agent its tools, and a status result that names the agent", () =>
  Effect.gen(function* () {
    const server = yield* inProcessServer();
    const alice = yield* server.signInAs("alice");
    const { tools } = yield* Effect.tryPromise(() => alice.mcp.listTools());
    expect(new Set(tools.map((tool) => tool.name))).toEqual(
      new Set([
        "list_conversations",
        "read_conversation",
        "read_firewall_decisions",
        "receive_messages",
        "send_message",
        "status",
      ]),
    );
    const reply = yield* Effect.tryPromise(() =>
      alice.mcp.callTool({ name: "status", arguments: {} }),
    );
    expect(reply.structuredContent).toEqual({ agentName: "alice" });
    expect(reply.content).toEqual([
      { type: "text", text: JSON.stringify(reply.structuredContent) },
    ]);
  }));

it("gives a client tools that name no recipient and no conversation", () =>
  Effect.gen(function* () {
    const server = yield* inProcessServer();
    const bob = yield* server.signInAs("bob");
    const { tools } = yield* Effect.tryPromise(() => bob.mcp.listTools());
    expect(new Set(tools.map((tool) => tool.name))).toEqual(
      new Set([
        "read_conversation",
        "receive_messages",
        "send_message",
        "status",
      ]),
    );
    expect(
      Object.keys(
        tools.find((tool) => tool.name === "send_message")?.inputSchema
          .properties ?? {},
      ),
    ).toEqual(["text", "replyToMsgId"]);
    const { id } = yield* send(bob, { text: "hello" });
    expect((yield* callTool(bob, "read_conversation")).records).toEqual([
      {
        message: {
          author: bob.name,
          recipient: "alice",
          msgId: id,
          text: "hello",
        },
      },
    ]);
  }));

it("refuses a call with a wrong or an unknown argument without running the tool", () =>
  Effect.gen(function* () {
    const server = yield* inProcessServer();
    const alice = yield* server.signInAs("alice");
    for (const wrong of [{ text: 1 }, { extra_field: "x" }]) {
      const failure = yield* callTool(alice, "send_message", {
        to: "bob",
        text: "hello",
        ...wrong,
      }).pipe(Effect.flip);
      expect(failure.message).toContain(Object.keys(wrong)[0]);
    }
  }));

it("answers a failed call with the failure's message and its fields", () =>
  Effect.gen(function* () {
    const refusals = [
      {
        reason: "firewall-refused" as const,
        checkId: "chk_1",
        refusal: "rule-violation" as const,
        rule: "shape",
        feedback: ["Send a greeting first."],
      },
      {
        reason: "firewall-refused" as const,
        checkId: "chk_2",
        refusal: "unresolved-evidence" as const,
        feedback: [],
      },
    ];
    const pending = [...refusals];
    const server = yield* inProcessServer({
      firewall: {
        checkOutgoing: () =>
          Effect.map(Effect.fromNullable(pending.shift()), (next) => ({
            kind: "refused" as const,
            ...Struct.omit(next, "reason", "refusal"),
            reason: next.refusal,
          })).pipe(Effect.orDie),
      },
    });
    const alice = yield* server.signInAs("alice");
    const bob = yield* server.signInAs("bob");
    yield* send(bob, { text: "hi" });
    const said: Readonly<Record<string, string>> = {
      chk_1:
        "Error: Send a greeting first. Check: chk_1, rule shape; rule-violation.",
      chk_2:
        "Error: The policy check could not settle whether the message is allowed. Check: chk_2; unresolved-evidence.",
    };
    for (const fields of refusals) {
      expect(
        yield* send(alice, { to: bob.name, text: "hello" }).pipe(Effect.flip),
      ).toEqual({ ...fields, message: said[fields.checkId] });
    }
    for (const name of ["nobody", "alice"]) {
      const refusal = {
        reason: "unknown-agent",
        name,
        message: `No client named ${name} has written to this agent. Use list_conversations to see the clients that have.`,
      };
      expect(
        yield* Effect.flip(send(alice, { to: name, text: "hello" })),
      ).toEqual(refusal);
      expect(
        yield* Effect.flip(
          callTool(alice, "read_conversation", { with: name }),
        ),
      ).toEqual(refusal);
    }
  }));

it("answers a call that dies without the defect's text", () =>
  Effect.gen(function* () {
    const server = yield* inProcessServer({
      firewall: {
        checkOutgoing: () =>
          Effect.dieMessage("PRIVATE relation does not exist"),
      },
    });
    const alice = yield* server.signInAs("alice");
    const bob = yield* server.signInAs("bob");
    yield* send(bob, { text: "hi" });
    const died = yield* send(alice, { to: bob.name, text: "hello" }).pipe(
      Effect.flip,
    );
    expect(Object.keys(died)).toEqual(["message"]);
    expect(died.message).not.toContain("PRIVATE");
    yield* server.sql`DROP TABLE firewall_checks`;
    const failure = yield* callTool(alice, "read_firewall_decisions").pipe(
      Effect.flip,
    );
    expect(Object.keys(failure)).toEqual(["message"]);
    expect(failure.message).not.toContain("firewall_checks");
  }));

it("interrupts only the aborted call, whose conversation the next send then uses", () =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<undefined>();
    const interrupted = yield* Deferred.make<undefined>();
    let first = true;
    const server = yield* inProcessServer({
      firewall: {
        checkOutgoing: () =>
          Effect.suspend(() => {
            if (!first) {
              return Effect.succeed({ kind: "allowed" as const });
            }
            first = false;
            return Deferred.succeed(entered, undefined).pipe(
              Effect.zipRight(Effect.never),
              Effect.onInterrupt(() =>
                Deferred.succeed(interrupted, undefined),
              ),
            );
          }),
      },
    });
    const alice = yield* server.signInAs("alice");
    const bob = yield* server.signInAs("bob");
    yield* send(bob, { text: "hi" });
    const hello = { to: bob.name, text: "hello" };

    const waiting = yield* Effect.fork(send(alice, hello));
    yield* Deferred.await(entered);
    yield* Fiber.interrupt(waiting);
    yield* Deferred.await(interrupted);

    expect(yield* send(alice, hello)).toHaveProperty("id");
  }));

/** A server opened on `database`, with Alice as its server agent behind a firewall that allows everything. */
const openOn = (
  database: Effect.Effect.Success<ReturnType<typeof openTestDatabase>>,
) =>
  serverFactory(database.url, {
    issuer: "urn:social-harness:test",
    subject: "alice",
    name: AgentName.make("alice"),
    firewall: {
      checkIncoming: () => Effect.succeed({ kind: "allowed" }),
      checkOutgoing: () => Effect.succeed({ kind: "allowed" }),
    },
  });

/** The one rule of every policy here. */
const privacy = {
  id: "privacy",
  applies: "both" as const,
  text: "Protect the owner's private information.",
  feedback: "Keep private information private.",
};

/** A message policy of the rule `privacy`, judged by `provider`. With `action`, the rule replies to or silently withholds what breaks it. */
const policy = (
  provider: DecisionProvider,
  action?: "deny" | "reply",
): TestPolicy => ({
  provider,
  source: {
    policy: {
      rules: [{ ...privacy, ...(action === undefined ? {} : { action }) }],
      settings: { minimumProbability: 0.8, timeoutMilliseconds: 5_000 },
    },
  },
});

/** A provider that finds every message compliant. */
const compliant: DecisionProvider = {
  decide: (request) => Effect.succeed(answersTo(request)),
};

/** A provider that finds every message a client sends in violation, and no message of the server agent. */
const incomingViolation: DecisionProvider = {
  decide: (request) =>
    Effect.succeed(
      answersTo(
        request,
        {},
        decisionState(request.state).direction === "incoming"
          ? "violation"
          : "compliant",
      ),
    ),
};

/** A server with its server agent Alice and her client Bob signed in. */
const aliceAndBob = (options?: Parameters<typeof inProcessServer>[0]) =>
  Effect.gen(function* () {
    const server = yield* inProcessServer(options);
    const alice = yield* server.signInAs("alice");
    const bob = yield* server.signInAs("bob");
    return { server, alice, bob };
  });

/** Whether `agent` sent or received each message on the first page of its history with `client`, or with the server agent. */
const historyOf = (agent: SignedIn, client?: string) =>
  Effect.map(
    callTool(agent, "read_conversation", client ? { with: client } : {}),
    ({ records }) =>
      records.map((record) =>
        record.message.author === agent.name ? "sent" : "received",
      ),
  );

it("stores each send as a new message to the agent it names, with the reply reference it gives", () =>
  Effect.gen(function* () {
    const { alice, bob } = yield* aliceAndBob();

    yield* send(bob, { text: "Hello" });
    const first = yield* send(alice, { to: bob.name, text: "Hello" });
    const second = yield* send(alice, {
      to: bob.name,
      text: "Hello",
      replyToMsgId: first.id,
    });

    const [stored, reply] = yield* inboxOf(bob);
    expect(stored).toMatchObject({ msgId: first.id, recipient: bob.name });
    expect(stored).not.toHaveProperty("replyToMsgId");
    expect(reply).toMatchObject({ msgId: second.id, replyToMsgId: first.id });
  }));

it("checks the messages of one conversation in order while another conversation progresses", () =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<undefined>();
    const release = yield* Deferred.make<undefined>();
    const assessed: string[] = [];
    const { server, alice, bob } = yield* aliceAndBob({
      policy: policy({
        decide: (request) =>
          Effect.gen(function* () {
            const { text } = decisionState(request.state).candidate;
            assessed.push(text);
            if (text === "First") {
              yield* Deferred.succeed(entered, undefined);
              yield* Deferred.await(release);
            }
            return answersTo(request);
          }),
      }),
    });
    const carol = yield* server.signInAs("carol");
    const authors = Effect.map(inboxOf(alice), (messages) =>
      messages.map((message) => message.author),
    );

    const first = yield* Effect.forkScoped(send(bob, { text: "First" }));
    yield* Deferred.await(entered);
    const second = yield* Effect.forkScoped(send(bob, { text: "Second" }));
    yield* send(carol, { text: "Independent" });

    expect(yield* authors).toEqual([carol.name]);
    expect(assessed).toEqual(["First", "Independent"]);
    yield* Deferred.succeed(release, undefined);
    yield* Fiber.joinAll([first, second]);
    expect(yield* authors).toEqual([carol.name, bob.name, bob.name]);
    expect(assessed).toEqual(["First", "Independent", "Second"]);
  }));

it("reads the inbox a page of fifty at a time after a cursor, the same page again for the same cursor, and nothing earlier without one", () =>
  Effect.gen(function* () {
    const { alice, bob } = yield* aliceAndBob();
    const receive = (cursor?: string) =>
      callTool(alice, "receive_messages", cursor ? { cursor } : {});
    const start = yield* receive();
    expect(start.entries).toEqual([]);
    const sent = yield* Effect.forEach(
      Array.from({ length: 57 }),
      () => Effect.map(send(bob, { text: "Hello" }), ({ id }) => id),
      { concurrency: 1 },
    );

    const first = yield* receive(start.nextCursor);
    const rest = yield* receive(first.nextCursor);

    expect(first.entries.map((entry) => entry.message.msgId)).toEqual(
      sent.slice(0, 50),
    );
    expect(rest.entries.map((entry) => entry.message.msgId)).toEqual(
      sent.slice(50),
    );
    expect(yield* receive(first.nextCursor)).toEqual(rest);
    expect(yield* receive()).toEqual({
      entries: [],
      nextCursor: rest.nextCursor,
    });
  }));

it("stores a withheld message for its sender, whose send succeeds, and keeps it from the recipient's inbox and history", () =>
  Effect.gen(function* () {
    const { alice, bob } = yield* aliceAndBob({
      policy: policy(incomingViolation),
    });

    yield* send(bob, { text: "Private peer content" });

    expect(
      (yield* callTool(alice, "read_firewall_decisions")).decisions,
    ).toMatchObject([{ reason: "rule-violation", decision: "withheld" }]);
    expect(yield* inboxOf(alice)).toEqual([]);
    expect(yield* historyOf(alice, bob.name)).toEqual([]);
    expect(yield* historyOf(bob)).toEqual(["sent"]);
    expect(yield* inboxOf(bob)).toEqual([]);
  }));

it("fails a client's send and stores nothing when the check of its message could not run", () =>
  Effect.gen(function* () {
    const { alice, bob } = yield* aliceAndBob({
      policy: policy({ decide: () => Effect.succeed({ answers: {} }) }),
    });

    expect(yield* send(bob, { text: "Hello" }).pipe(Effect.flip)).toMatchObject(
      { reason: "firewall-refused", refusal: "check-unavailable" },
    );

    expect(
      (yield* callTool(alice, "read_firewall_decisions")).decisions,
    ).toEqual([]);
    expect(yield* historyOf(bob)).toEqual([]);
  }));

it.each([
  { from: "the notice", action: undefined, text: "Not accepted." },
  { from: "a reply rule", action: "reply" as const, text: privacy.feedback },
])(
  "sends the firewall's reply to a withheld message, from $from, as an ordinary message that answers it",
  ({ action, text }) =>
    Effect.gen(function* () {
      const { alice, bob } = yield* aliceAndBob({
        notice: "Not accepted.",
        policy: policy(incomingViolation, action),
      });

      const withheld = yield* send(bob, { text: "Private peer content" });

      expect(yield* inboxOf(bob)).toMatchObject([
        {
          author: alice.name,
          recipient: bob.name,
          replyToMsgId: withheld.id,
          text,
        },
      ]);
      expect(yield* inboxOf(alice)).toEqual([]);
      expect(yield* historyOf(bob)).toEqual(["sent", "received"]);
    }),
);

/** `open` starts an instance and `close`, from the other agent, ends it. */
const exchange = Either.getOrThrow(
  compileNorm(`(norm exchange :initiator Sender :participant Receiver)
(content open :by Sender :starts true :initiates opened)
(content close :by Receiver :when opened :ends true)
`),
);

it("answers what an agent that holds norms withholds, plain text and an act out of turn alike, and the conversation goes on", () =>
  Effect.gen(function* () {
    const { alice, bob } = yield* aliceAndBob({
      notice: "Not accepted.",
      norms: [exchange],
      policy: policy(compliant),
    });

    const plain = yield* send(bob, { text: "Hello, can you help?" });
    const outOfTurn = yield* send(bob, { text: "(close :ontology exchange)" });

    const replies = yield* inboxOf(bob);
    expect(replies).toMatchObject(
      [plain, outOfTurn].map((withheld) => ({ replyToMsgId: withheld.id })),
    );
    expect(
      replies.map(({ text }) => text.startsWith("Not accepted. ")),
    ).toEqual([true, true]);
    yield* send(bob, { text: "(open :ontology exchange)" });
    expect(yield* inboxOf(alice)).toHaveLength(1);
  }));

it("checks each act of a norm against the whole conversation, though the provider's history window holds one message", () =>
  Effect.gen(function* () {
    const { alice, bob } = yield* aliceAndBob({
      historyLimit: 1,
      norms: [exchange],
      policy: policy(compliant),
    });

    for (let instance = 0; instance < 3; instance += 1) {
      const opened = yield* send(bob, { text: "(open :ontology exchange)" });
      yield* send(alice, {
        to: bob.name,
        text: "(close :ontology exchange)",
        replyToMsgId: opened.id,
      });
    }

    expect(yield* inboxOf(alice)).toHaveLength(3);
  }));

it("continues a conversation's history from the cursor a page returned, with what was sent since", () =>
  Effect.gen(function* () {
    const { alice, bob } = yield* aliceAndBob();
    const say = (text: string) => send(bob, { text });
    for (let index = 0; index < 51; index += 1) {
      yield* say(`Message ${index}`);
    }

    const first = yield* callTool(alice, "read_conversation", {
      with: bob.name,
    });
    yield* say("Later message");
    const rest = yield* callTool(alice, "read_conversation", {
      with: bob.name,
      cursor: first.nextCursor,
    });

    expect(first.records).toHaveLength(50);
    expect(rest.records.map((record) => record.message.text)).toEqual([
      "Message 50",
      "Later message",
    ]);
    expect(rest.nextCursor).toBeUndefined();
  }));

it("lists every conversation once, in name order", () =>
  Effect.gen(function* () {
    const server = yield* inProcessServer();
    const alice = yield* server.signInAs("alice");
    const clients: string[] = [];
    for (const owner of ["one", "two", "three", "two"]) {
      const client = yield* server.signInAs(owner);
      yield* send(client, { text: "Hello" });
      clients.push(client.name);
    }

    expect(yield* callTool(alice, "list_conversations")).toEqual({
      clients: [...new Set(clients)].sort(Order.string),
    });
  }));

it("prepares an empty database and leaves a prepared one as it is", () =>
  Effect.gen(function* () {
    const database = yield* openTestDatabase();
    yield* openOn(database);
    yield* database.sql`INSERT INTO firewall_checks (decided_at, record) VALUES (1, 'kept')`;
    yield* openOn(database);
    expect(yield* database.sql`SELECT record FROM firewall_checks`).toEqual([
      { record: "kept" },
    ]);
    expect(yield* database.sql`SELECT * FROM messages`).toEqual([]);
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
  { minLength: 1, maxLength: 6 },
);

it.prop(
  "gives each issuer and subject pair one name of its own, however many sign in at once or again",
  [accounts],
  ([owners]) =>
    Effect.gen(function* () {
      const server = yield* inProcessServer();
      const nameOf = (owner: (typeof owners)[number]) =>
        Effect.map(server.signInAs(owner), (signedIn) => signedIn.name);
      const names = yield* Effect.forEach(owners, nameOf, {
        concurrency: owners.length,
      });
      expect(yield* Effect.forEach(owners, nameOf, { concurrency: 1 })).toEqual(
        names,
      );
      expect(
        new Set(
          owners.map(
            (owner, index) =>
              `${owner.issuer} ${owner.subject} ${names[index] ?? ""}`,
          ),
        ).size,
      ).toBe(new Set(names).size);
    }),
  { fastCheck: { numRuns: 10 } },
);

/** A server whose firewall withholds exactly the messages whose text is `withheld`, with Alice and two clients signed in. */
const withholdingByText = Effect.gen(function* () {
  const decide: Firewall["checkIncoming"] = (message) =>
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
  const server = yield* inProcessServer({
    firewall: { checkIncoming: decide },
  });
  const alice = yield* server.signInAs("alice");
  const clients = {
    bob: yield* server.signInAs("bob"),
    carol: yield* server.signInAs("carol"),
  };
  /** The ids of the messages Alice may read after `cursor`, in read order, with the cursor to read on from. */
  const inbox = (cursor?: string) =>
    Effect.map(
      callTool(alice, "receive_messages", cursor ? { cursor } : {}),
      ({ entries, nextCursor }) => ({
        ids: entries.map(({ message }) => message.msgId),
        nextCursor,
      }),
    );
  return { clients, inbox };
});

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
    const { clients, inbox } = yield* withholdingByText;
    /** The ids of the messages alice may read, in the order they were stored. */
    const expected: string[] = [];
    /** Each cursor a read returned, with the count of messages readable when it did. */
    const cursors: Array<{ cursor: string; through: number }> = [];
    const holds = fc.asyncProperty(steps, (sequence) =>
      Effect.gen(function* () {
        for (const step of sequence) {
          if (step.step === "send") {
            const { id } = yield* send(clients[step.author], {
              text: step.readable ? "hello" : "withheld",
            });
            if (step.readable) {
              expected.push(id);
            }
          } else {
            const presented =
              step.cursor === undefined
                ? undefined
                : cursors.at(-1 - step.cursor);
            const read = yield* inbox(presented?.cursor);
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
    const { clients, inbox } = yield* withholdingByText;
    const sent = yield* Effect.forEach(
      Arr.makeBy(12, (index) => (index % 2 === 0 ? "bob" : "carol")),
      (author) =>
        Effect.map(send(clients[author], { text: "hello" }), ({ id }) => id),
      { concurrency: 12 },
    );
    const read = yield* inbox("0");
    expect(new Set(read.ids)).toEqual(new Set(sent));
    expect(read.nextCursor).toBe("12");
  }));
