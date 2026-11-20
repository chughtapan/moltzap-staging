/** @file Each owner's one agent, and its sends, the checks of what it sends and is sent, its inbox and its history. */

import { scopedLive as it } from "@effect/vitest";
import {
  Deferred,
  Effect,
  Exit,
  FastCheck as fc,
  Fiber,
  Order,
  Schema,
} from "effect";
import { expect } from "vitest";
import type { DecisionProvider } from "../firewall.js";
import { digest } from "../__tests__/agents.js";
import { answersTo } from "../__tests__/firewall-fixtures.js";
import {
  composedPolicy,
  decisionState,
  makeHarnessFixture,
} from "../__tests__/service-fixtures.js";
import { openTestDatabase } from "../__tests__/test.js";
import { AgentName, MsgId } from "../messages.js";
import { form, noFields, normOf } from "../norms/__tests__/norms-fixtures.js";
import { AgentAddress, getOrCreateOwnedAgent } from "./agent.js";

const bobAddress = AgentAddress.make("agent:bob");
const carolAddress = AgentAddress.make("agent:carol");

type Fixture = Effect.Effect.Success<typeof makeHarnessFixture>;

/** A decision service that finds every message in violation of its rule. */
const violation: DecisionProvider = {
  decide: (request) => Effect.succeed(answersTo(request, {}, "violation")),
};

/** A decision service that finds every message a peer sent in violation, and every message this agent sends compliant. */
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

/** Gives alice a conversation with a new peer `name` and returns its address. */
function hold(subject: Fixture, name: string) {
  return Effect.gen(function* () {
    yield* subject.sendFrom({ text: "Hello" }, yield* subject.addPeer(name));
    return `agent:${name}`;
  });
}

/** Checks `rule` against generated values and fails with the smallest value that breaks it. */
function forAll<Value>(
  values: fc.Arbitrary<Value>,
  rule: (value: Value) => Effect.Effect<void, unknown>,
) {
  return Effect.tryPromise(() =>
    fc.assert(
      fc.asyncProperty(values, (value) => Effect.runPromise(rule(value))),
    ),
  );
}

/** A client an address names. */
const named = fc.constantFrom("bob", "carol");

it("sends each call as new work to the agent it names, with the reply reference it gives", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const { harness } = yield* subject.open();
    const receipts: string[] = [];
    yield* forAll(
      fc.tuple(
        named,
        fc.option(fc.integer({ min: 0, max: 255 }), { nil: undefined }),
      ),
      Effect.fn(function* ([name, byte]) {
        const replyToMsgId =
          byte === undefined
            ? undefined
            : Schema.decodeUnknownSync(MsgId)(digest("msg_", byte));
        const to = AgentAddress.make(`agent:${name}`);
        const request = { to, text: "Hello", replyToMsgId };
        const dispatched = (yield* subject.sent).length;
        const receipt = yield* harness.send(request);
        expect(receipts).not.toContain(receipt.id);
        receipts.push(receipt.id);
        const [stored] = (yield* subject.sent).slice(dispatched);
        expect(stored).toMatchObject({
          msgId: receipt.id,
          recipient: { agentName: name },
        });
        expect(stored?.replyToMsgId).toBe(replyToMsgId);
      }),
    );
  }));

it("checks the messages of one conversation in order while another conversation progresses", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const entered = yield* Deferred.make<undefined>();
    const release = yield* Deferred.make<undefined>();
    const assessed: string[] = [];
    const runtime = yield* subject.open({
      policy: composedPolicy({
        decide: (request) =>
          Effect.gen(function* () {
            const text = decisionState(request.state).candidate.text;
            assessed.push(text);
            if (text === "First") {
              yield* Deferred.succeed(entered, undefined);
              yield* Deferred.await(release);
            }
            return answersTo(request);
          }),
      }),
    });
    const first = yield* Effect.forkScoped(subject.sendFrom({ text: "First" }));
    yield* Deferred.await(entered);
    const second = yield* Effect.forkScoped(
      subject.sendFrom({ text: "Second" }),
    );
    yield* subject.sendFrom({ text: "Independent" }, subject.carol);
    expect(yield* runtime.inbox).toMatchObject([{ address: carolAddress }]);
    expect(assessed).toEqual(["First", "Independent"]);
    yield* Deferred.succeed(release, undefined);
    yield* Fiber.joinAll([first, second]);
    expect((yield* runtime.inbox).map((entry) => entry.address)).toEqual([
      "agent:carol",
      "agent:bob",
      "agent:bob",
    ]);
    expect(assessed).toEqual(["First", "Independent", "Second"]);
  }));

it("reads the entries after a cursor in the order they were allowed, a page at a time, and none before the cursor a read without one returns", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open();
    const start = yield* runtime.harness.receive({});
    expect(start.entries).toEqual([]);
    for (let index = 0; index < 57; index++) {
      yield* hold(subject, `peer-${index}`);
    }
    const all = yield* runtime.inbox;
    const first = yield* runtime.harness.receive({ cursor: start.nextCursor });
    const rest = yield* runtime.harness.receive({ cursor: first.nextCursor });
    expect(first.entries).toEqual(all.slice(0, 50));
    expect(rest.entries).toEqual(all.slice(50));
    expect(
      yield* runtime.harness.receive({ cursor: first.nextCursor }),
    ).toEqual(rest);
    expect(yield* runtime.harness.receive({})).toEqual({
      entries: [],
      nextCursor: rest.nextCursor,
    });
  }));

it("stores a withheld message for its sender, whose send succeeds, and keeps it from the recipient's inbox and history", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open({
      policy: composedPolicy(violation),
    });
    yield* subject.sendFrom({ text: "Private peer content" });
    expect(yield* runtime.decisions).toMatchObject([
      { reason: "rule-violation", decision: "withheld" },
    ]);
    expect(yield* runtime.inbox).toEqual([]);
    expect(
      (yield* runtime.harness.readHistory({ address: bobAddress })).records,
    ).toEqual([]);
    expect(yield* subject.historyOf()).toEqual(["sent"]);
    expect(yield* subject.sent).toEqual([]);
  }));

it("fails a client's send and stores nothing when the check of its message could not run", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open({
      policy: composedPolicy({
        decide: () => Effect.succeed({ answers: {} }),
      }),
    });
    expect(
      yield* subject.sendFrom({ text: "Hello" }).pipe(Effect.flip),
    ).toMatchObject({ _tag: "FirewallRefusal", refusal: "check-unavailable" });
    expect(yield* runtime.decisions).toEqual([]);
    expect(yield* subject.historyOf()).toEqual([]);
  }));

it("answers a message that broke a rule with the configured reply when no rule gave one", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const withheldReply = "Your message was not accepted.";
    const runtime = yield* subject.open({
      notice: withheldReply,
      policy: composedPolicy(incomingViolation),
    });
    yield* subject.sendFrom({ text: "Private peer content" });
    expect(yield* subject.sent).toMatchObject([{ text: withheldReply }]);
    expect(yield* runtime.inbox).toEqual([]);
    expect(yield* subject.historyOf()).toEqual(["sent", "received"]);
  }));

it("answers a message its rule withholds with that rule's feedback, as an ordinary message that names the withheld one", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open({
      policy: composedPolicy(incomingViolation, "reply"),
    });
    const withheld = yield* subject.sendFrom({ text: "Private peer content" });
    expect(yield* subject.sent).toMatchObject([
      {
        author: subject.self,
        recipient: subject.bob,
        replyToMsgId: withheld.id,
        text: "Keep private information private.",
      },
    ]);
    expect(yield* runtime.inbox).toEqual([]);
  }));

/** `open` starts an instance and `close`, from the other agent, ends it. */
const exchange = normOf("exchange", [
  {
    ...form,
    act: "open",
    by: ["Sender"],
    fields: noFields,
    starts: true,
    initiates: ["opened"],
  },
  {
    ...form,
    act: "close",
    by: ["Receiver"],
    fields: noFields,
    when: ["opened"],
    ends: true,
  },
]);

it("answers what an agent that holds norms withholds, plain text and an act out of turn alike, and the conversation goes on", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const withheldReply = "Your message was not accepted.";
    const runtime = yield* subject.open({
      notice: withheldReply,
      norms: [exchange],
      policy: composedPolicy({
        decide: (request) => Effect.succeed(answersTo(request)),
      }),
    });
    const plain = yield* subject.sendFrom({ text: "Hello, can you help?" });
    const outOfTurn = yield* subject.sendFrom({
      text: "(close :ontology exchange)",
    });
    const decisions = yield* runtime.decisions;
    expect(
      decisions
        .map(({ direction, decision }) => `${direction} ${decision}`)
        .sort(Order.string),
    ).toEqual([
      "incoming withheld",
      "incoming withheld",
      "outgoing allowed",
      "outgoing allowed",
    ]);
    const replies = yield* subject.sent;
    expect(replies).toMatchObject(
      [plain, outOfTurn].map((withheld) => ({
        recipient: subject.bob,
        replyToMsgId: withheld.id,
      })),
    );
    expect(
      replies.map(({ text }) => text.startsWith(`${withheldReply} `)),
    ).toEqual([true, true]);
    yield* subject.sendFrom({ text: "(open :ontology exchange)" });
    expect(yield* runtime.inbox).toHaveLength(1);
  }));

it("answers an act its text rule withholds with that rule's feedback when it holds norms", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    yield* subject.open({
      norms: [exchange],
      policy: composedPolicy(incomingViolation, "reply"),
    });
    const act = yield* subject.sendFrom({ text: "(open :ontology exchange)" });
    expect(yield* subject.sent).toMatchObject([
      {
        replyToMsgId: act.id,
        text: "Keep private information private.",
      },
    ]);
  }));

it.prop(
  "checks each act of a contract against the whole conversation, however short the history window",
  [fc.integer({ min: 1, max: 3 }), fc.integer({ min: 1, max: 4 })],
  ([window, instances]) =>
    Effect.gen(function* () {
      const subject = yield* makeHarnessFixture;
      const runtime = yield* subject.open({
        historyLimit: window,
        norms: [exchange],
        policy: composedPolicy({
          decide: (request) => Effect.succeed(answersTo(request)),
        }),
      });
      for (let instance = 1; instance <= instances; instance += 1) {
        const opened = yield* runtime.harness.send({
          to: bobAddress,
          text: "(open :ontology exchange)",
        });
        yield* subject.sendFrom({
          text: "(close :ontology exchange)",
          replyToMsgId: opened.id,
        });
      }
      expect(yield* runtime.inbox).toHaveLength(instances);
    }),
  { fastCheck: { numRuns: 6 } },
);

it("continues a conversation's history from the cursor a page returned, with what was sent since", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open();
    for (let index = 0; index < 51; index++) {
      yield* runtime.harness.send({
        to: bobAddress,
        text: `Message ${index}`,
      });
    }
    const first = yield* runtime.harness.readHistory({ address: bobAddress });
    expect(first.records).toHaveLength(50);
    yield* runtime.harness.send({ to: bobAddress, text: "Later message" });
    const rest = yield* runtime.harness.readHistory({
      address: bobAddress,
      cursor: first.nextCursor,
    });
    expect(rest.records.map((record) => record.message.text)).toEqual([
      "Message 50",
      "Later message",
    ]);
    expect(rest.nextCursor).toBeUndefined();
  }));

it("lists every conversation once, in address order", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open();
    const held = yield* Effect.forEach(
      ["peer-2", "peer-10", "peer-1", "peer-10"],
      (name) => hold(subject, name),
      { concurrency: 1 },
    );
    expect(yield* runtime.harness.listConversations()).toEqual({
      addresses: [...new Set(held)].sort(Order.string),
    });
  }));

it("cancels an unaccepted draft and releases its conversation's turn", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const entered = yield* Deferred.make<undefined>();
    let first = true;
    const policy = composedPolicy({
      decide: (request) =>
        Effect.suspend(() => {
          if (first) {
            first = false;
            return Deferred.succeed(entered, undefined).pipe(
              Effect.zipRight(Effect.never),
            );
          }
          return Effect.succeed(answersTo(request));
        }),
    });
    const runtime = yield* subject.open({ policy });
    const cancelled = yield* runtime.harness
      .send({ to: bobAddress, text: "Cancelled draft" })
      .pipe(Effect.forkScoped);
    yield* Deferred.await(entered);
    yield* Fiber.interrupt(cancelled);
    yield* runtime.harness.send({ to: bobAddress, text: "Next draft" });
    expect(yield* subject.sent).toMatchObject([{ text: "Next draft" }]);
  }));

it("reads a saved conversation's history with each agent", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open();
    yield* runtime.harness.send({ to: bobAddress, text: "Hello" });
    yield* subject.sendFrom({ text: "Hello from carol" }, subject.carol);
    const saved: Record<string, unknown> = {
      bob: ["sent"],
      carol: ["received"],
    };
    yield* forAll(
      named,
      Effect.fn(function* (name) {
        const read = yield* runtime.harness
          .readHistory({ address: AgentAddress.make(`agent:${name}`) })
          .pipe(Effect.map(({ records }) => records.map(({ kind }) => kind)));
        expect(read).toEqual(saved[name]);
      }),
    );
  }));

const owner = { issuer: "https://issuer.example", subject: "owner" };

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

it("refuses to rename a saved agent", () =>
  Effect.gen(function* () {
    const { run, sql } = yield* openTestDatabase();
    yield* run(getOrCreateOwnedAgent(owner));
    const before = yield* sql`SELECT * FROM agent_cards`;
    const changed = yield* getOrCreateOwnedAgent(
      owner,
      AgentName.make("another"),
    ).pipe(run, Effect.exit);
    expect(Exit.isFailure(changed)).toBe(true);
    expect(yield* sql`SELECT * FROM agent_cards`).toEqual(before);
  }));
