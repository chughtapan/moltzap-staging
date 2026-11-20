/**
 * @file What agents observe of one daemon through their tools: the tools of
 * their role, each call's result or failure, sends and their checks, the
 * inbox, history and conversations; and what opening a daemon does to its
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
  inProcessDaemon,
  send,
} from "./__tests__/daemon.js";
import { answersTo, type TestPolicy } from "./__tests__/firewall-fixtures.js";
import { openTestDatabase } from "./__tests__/test.js";
import { AgentName } from "./messages.js";
import { compileNorm } from "./norms/compiler.js";
import { openDaemon } from "./server.js";

it("gives the server agent its tools, and a status result that names the agent", () =>
  Effect.gen(function* () {
    const daemon = yield* inProcessDaemon();
    const alice = yield* daemon.signInAs("alice");
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
    expect(reply.structuredContent).toEqual({
      agentId: alice.agent.agentId,
      agentName: "alice",
    });
    expect(reply.content).toEqual([
      { type: "text", text: JSON.stringify(reply.structuredContent) },
    ]);
  }));

it("gives a client tools that name no recipient and no conversation", () =>
  Effect.gen(function* () {
    const daemon = yield* inProcessDaemon();
    const bob = yield* daemon.signInAs("bob");
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
    expect(yield* send(bob, { text: "hello" })).toMatchObject({
      address: "agent:alice",
    });
    expect((yield* callTool(bob, "read_conversation")).records).toMatchObject([
      {
        kind: "sent",
        message: { recipient: { agentName: "alice" }, text: "hello" },
      },
    ]);
  }));

it("refuses a call with a wrong or an unknown argument without running the tool", () =>
  Effect.gen(function* () {
    const daemon = yield* inProcessDaemon();
    const alice = yield* daemon.signInAs("alice");
    for (const wrong of [{ text: 1 }, { extra_field: "x" }]) {
      const failure = yield* callTool(alice, "send_message", {
        to: "agent:bob",
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
    const daemon = yield* inProcessDaemon({
      firewall: {
        checkOutgoing: () =>
          Effect.map(Effect.fromNullable(pending.shift()), (next) => ({
            kind: "refused" as const,
            ...Struct.omit(next, "reason", "refusal"),
            reason: next.refusal,
          })).pipe(Effect.orDie),
      },
    });
    const alice = yield* daemon.signInAs("alice");
    const bob = yield* daemon.signInAs("bob");
    const said: Readonly<Record<string, string>> = {
      chk_1:
        "Error: Send a greeting first. Check: chk_1, rule shape; rule-violation.",
      chk_2:
        "Error: The policy check could not settle whether the message is allowed. Check: chk_2; unresolved-evidence.",
    };
    for (const fields of refusals) {
      expect(
        yield* send(alice, { to: bob.address, text: "hello" }).pipe(
          Effect.flip,
        ),
      ).toEqual({ ...fields, message: said[fields.checkId] });
    }
    for (const [name, input] of [
      ["send_message", { to: "agent:nobody", text: "hello" }],
      ["send_message", { to: "agent:alice", text: "hello" }],
      ["read_conversation", { address: "agent:nobody" }],
      ["read_conversation", { address: "agent:alice" }],
    ] as const) {
      const address = "to" in input ? input.to : input.address;
      const unknown = address === "agent:nobody";
      expect(yield* callTool(alice, name, input).pipe(Effect.flip)).toEqual({
        reason: unknown ? "unknown-agent" : "invalid-address",
        address,
        message: unknown
          ? "This daemon has no agent named agent:nobody. Check the name with its owner."
          : "The address agent:alice names you, not another agent.",
      });
    }
  }));

it("answers a call that dies without the defect's text, and a send that dies as one whose outcome is unknown", () =>
  Effect.gen(function* () {
    const daemon = yield* inProcessDaemon({
      firewall: {
        checkOutgoing: () =>
          Effect.dieMessage("PRIVATE relation does not exist"),
      },
    });
    const alice = yield* daemon.signInAs("alice");
    const bob = yield* daemon.signInAs("bob");
    expect(
      yield* send(alice, { to: bob.address, text: "hello" }).pipe(Effect.flip),
    ).toEqual({
      reason: "outcome-unknown",
      message:
        "send failed: the send did not complete; the message may or may not have been sent",
    });
    yield* daemon.sql`DROP TABLE firewall_checks`;
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
    const daemon = yield* inProcessDaemon({
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
    const alice = yield* daemon.signInAs("alice");
    const bob = yield* daemon.signInAs("bob");
    const hello = { to: bob.address, text: "hello" };

    const waiting = yield* Effect.fork(send(alice, hello));
    yield* Deferred.await(entered);
    yield* Fiber.interrupt(waiting);
    yield* Deferred.await(interrupted);

    expect(yield* send(alice, hello)).toMatchObject({ address: bob.address });
  }));

/** A daemon opened on `database`, with Alice as its server agent behind a firewall that allows everything. */
const openOn = (
  database: Effect.Effect.Success<ReturnType<typeof openTestDatabase>>,
) =>
  openDaemon(database.url, {
    owner: { issuer: "urn:social-harness:test", subject: "alice" },
    name: AgentName.make("alice"),
    firewall: () => ({
      checkIncoming: () => Effect.succeed({ kind: "allowed" }),
      checkOutgoing: () => Effect.succeed({ kind: "allowed" }),
    }),
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

/** A daemon with its server agent Alice and her client Bob signed in. */
const aliceAndBob = (options?: Parameters<typeof inProcessDaemon>[0]) =>
  Effect.gen(function* () {
    const daemon = yield* inProcessDaemon(options);
    const alice = yield* daemon.signInAs("alice");
    const bob = yield* daemon.signInAs("bob");
    return { daemon, alice, bob };
  });

/** The kind of each record on the first page of the agent's history with the agent at `address`, or with the server agent. */
const historyOf = (agent: Parameters<typeof callTool>[0], address?: string) =>
  Effect.map(
    callTool(agent, "read_conversation", address ? { address } : {}),
    ({ records }) => records.map((record) => record.kind),
  );

it("stores each send as a new message to the agent it names, with the reply reference it gives", () =>
  Effect.gen(function* () {
    const { alice, bob } = yield* aliceAndBob();

    const first = yield* send(alice, { to: bob.address, text: "Hello" });
    const second = yield* send(alice, {
      to: bob.address,
      text: "Hello",
      replyToMsgId: first.id,
    });

    expect(first.address).toBe(bob.address);
    const [stored, reply] = yield* inboxOf(bob);
    expect(stored).toMatchObject({ msgId: first.id, recipient: bob.agent });
    expect(stored).not.toHaveProperty("replyToMsgId");
    expect(reply).toMatchObject({ msgId: second.id, replyToMsgId: first.id });
  }));

it("checks the messages of one conversation in order while another conversation progresses", () =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<undefined>();
    const release = yield* Deferred.make<undefined>();
    const assessed: string[] = [];
    const { daemon, alice, bob } = yield* aliceAndBob({
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
    const carol = yield* daemon.signInAs("carol");
    const addresses = Effect.map(
      callTool(alice, "receive_messages", { cursor: "0" }),
      (page) => page.entries.map((entry) => entry.address),
    );

    const first = yield* Effect.forkScoped(send(bob, { text: "First" }));
    yield* Deferred.await(entered);
    const second = yield* Effect.forkScoped(send(bob, { text: "Second" }));
    yield* send(carol, { text: "Independent" });

    expect(yield* addresses).toEqual([carol.address]);
    expect(assessed).toEqual(["First", "Independent"]);
    yield* Deferred.succeed(release, undefined);
    yield* Fiber.joinAll([first, second]);
    expect(yield* addresses).toEqual([carol.address, bob.address, bob.address]);
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
    expect(yield* historyOf(alice, bob.address)).toEqual([]);
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
          author: alice.agent,
          recipient: bob.agent,
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
      const opened = yield* send(alice, {
        to: bob.address,
        text: "(open :ontology exchange)",
      });
      yield* send(bob, {
        text: "(close :ontology exchange)",
        replyToMsgId: opened.id,
      });
    }

    expect(yield* inboxOf(alice)).toHaveLength(3);
  }));

it("continues a conversation's history from the cursor a page returned, with what was sent since", () =>
  Effect.gen(function* () {
    const { alice, bob } = yield* aliceAndBob();
    const say = (text: string) => send(alice, { to: bob.address, text });
    for (let index = 0; index < 51; index += 1) {
      yield* say(`Message ${index}`);
    }

    const first = yield* callTool(alice, "read_conversation", {
      address: bob.address,
    });
    yield* say("Later message");
    const rest = yield* callTool(alice, "read_conversation", {
      address: bob.address,
      cursor: first.nextCursor,
    });

    expect(first.records).toHaveLength(50);
    expect(rest.records.map((record) => record.message.text)).toEqual([
      "Message 50",
      "Later message",
    ]);
    expect(rest.nextCursor).toBeUndefined();
  }));

it("lists every conversation once, in address order", () =>
  Effect.gen(function* () {
    const daemon = yield* inProcessDaemon();
    const alice = yield* daemon.signInAs("alice");
    const addresses: string[] = [];
    for (const owner of ["one", "two", "three", "two"]) {
      const client = yield* daemon.signInAs(owner);
      yield* send(client, { text: "Hello" });
      addresses.push(client.address);
    }

    expect(yield* callTool(alice, "list_conversations")).toEqual({
      addresses: [...new Set(addresses)].sort(Order.string),
    });
  }));

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
  { minLength: 1, maxLength: 6 },
);

it.prop(
  "binds each issuer and subject pair to one agent of its own, however many sign in at once or again",
  [accounts],
  ([owners]) =>
    Effect.gen(function* () {
      const daemon = yield* inProcessDaemon();
      const distinct = new Set(
        owners.map((owner) => `${owner.issuer} ${owner.subject}`),
      ).size;
      const agentOf = (owner: (typeof owners)[number]) =>
        Effect.map(daemon.signInAs(owner), (signedIn) => signedIn.agent);
      const agents = yield* Effect.forEach(owners, agentOf, {
        concurrency: owners.length,
      });
      const again = yield* Effect.forEach(owners, agentOf, { concurrency: 1 });
      expect(again).toEqual(agents);
      expect(
        new Set(
          owners.map(
            (owner, index) =>
              `${owner.issuer} ${owner.subject} ${agents[index]?.agentId ?? ""}`,
          ),
        ).size,
      ).toBe(distinct);
      expect(new Set(agents.map((agent) => agent.agentId)).size).toBe(distinct);
    }),
  { fastCheck: { numRuns: 10 } },
);

/** A daemon whose firewall withholds exactly the messages whose text is `withheld`, with Alice and two clients signed in. */
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
  const daemon = yield* inProcessDaemon({
    firewall: { checkIncoming: decide },
  });
  const alice = yield* daemon.signInAs("alice");
  const clients = {
    bob: yield* daemon.signInAs("bob"),
    carol: yield* daemon.signInAs("carol"),
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
