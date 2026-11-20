/** @file What agents observe of one daemon through their tools: sends and their checks, the inbox, history and conversations; and each owner's one agent. */

import { scopedLive as it } from "@effect/vitest";
import { Deferred, Effect, Either, FastCheck as fc, Fiber } from "effect";
import { expect } from "vitest";
import type { DecisionProvider } from "../firewall.js";
import {
  callTool,
  decisionState,
  inboxOf,
  inProcessDaemon,
  send,
} from "../__tests__/daemon.js";
import { answersTo, type TestPolicy } from "../__tests__/firewall-fixtures.js";
import { openTestDatabase } from "../__tests__/test.js";
import { compileNorm } from "../norms/compiler.js";
import { getOrCreateOwnedAgent } from "./agent.js";

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
    for (const name of ["peer-2", "peer-10", "peer-1", "peer-10"]) {
      yield* send(yield* daemon.signInAs(name), { text: "Hello" });
    }

    expect(yield* callTool(alice, "list_conversations")).toEqual({
      addresses: ["agent:peer-1", "agent:peer-10", "agent:peer-2"],
    });
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
      const { run, sql } = yield* openTestDatabase();
      const account = ({ issuer, subject }: (typeof owners)[number]) =>
        `${issuer} ${subject}`;
      const distinct = new Set(owners.map(account)).size;
      const agents = yield* Effect.all(
        owners.map((each) => getOrCreateOwnedAgent(each)),
        { concurrency: owners.length },
      ).pipe(run);
      const again = yield* Effect.forEach(
        owners,
        (each) => getOrCreateOwnedAgent(each),
        { concurrency: 1 },
      ).pipe(run);
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
      expect(yield* sql`SELECT * FROM agent_cards`).toHaveLength(distinct);
    }),
  { fastCheck: { numRuns: 25 } },
);
