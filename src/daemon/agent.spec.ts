/** @file SocialHarness sends, the checks of what an agent sends and is sent, the inbox and history. */

import { scopedLive as it } from "@effect/vitest";
import {
  Cause,
  Deferred,
  Effect,
  Exit,
  FastCheck as fc,
  Fiber,
  Schema,
} from "effect";
import { expect } from "vitest";
import { digest } from "../__tests__/agents.js";
import { MsgId } from "../communication/index.js";
import { compareAscii } from "../encoding.js";
import {
  contractOf,
  form,
  noFields,
} from "../firewall/__tests__/firewall-fixtures.js";
import {
  type DecisionProvider,
  type DecisionRequest,
  FirewallPolicyError,
} from "../firewall/index.js";
import {
  composedPolicy,
  decisionResponse,
  decisionState,
  makeHarnessFixture,
} from "./__tests__/service-fixtures.js";
import { AgentAddress } from "./values.js";

const bobAddress = AgentAddress.make("agent:bob");
const carolAddress = AgentAddress.make("agent:carol");

type Fixture = Effect.Effect.Success<typeof makeHarnessFixture>;

/** A decision service that finds every message in violation of its rule. */
const violation: DecisionProvider = {
  decide: (request) => Effect.succeed(decisionResponse(request, "violation")),
};

/** A decision service that finds every message a peer sent in violation, and every message this agent sends compliant. */
const incomingViolation: DecisionProvider = {
  decide: (request) =>
    Effect.succeed(
      decisionResponse(
        request,
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

/** An agent an address names: bob and carol, whom the fixture knows, and nobody, whom it does not. */
const named = fc.constantFrom("bob", "carol", "nobody");

it("sends each call as new work to the agent its address names, with the reply reference it gives, and refuses an address that names an unknown agent", () =>
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
        const dispatched = subject.submitted.length;
        if (name === "nobody") {
          expect(yield* harness.send(request).pipe(Effect.flip)).toMatchObject({
            reason: "unknown-agent",
          });
          expect(subject.submitted).toHaveLength(dispatched);
        } else {
          const receipt = yield* harness.send(request);
          expect(receipt.address).toBe(to);
          expect(receipts).not.toContain(receipt.id);
          receipts.push(receipt.id);
          expect(subject.submitted.slice(dispatched)).toMatchObject([
            { msgId: receipt.id, recipients: [{ agentName: name }] },
          ]);
          const submitted = subject.submitted.at(-1);
          expect(
            submitted !== undefined && "replyToMsgId" in submitted
              ? submitted.replyToMsgId
              : undefined,
          ).toBe(replyToMsgId);
        }
      }),
    );
  }));

it("ends a send that could not be stored as a defect without sending it again", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open();
    yield* subject.failSends();
    const sent = yield* runtime.harness
      .send({ to: bobAddress, text: "Not stored" })
      .pipe(Effect.exit);
    expect(Exit.isFailure(sent) && Cause.isDie(sent.cause)).toBe(true);
    expect(subject.submitted).toHaveLength(1);
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
            return decisionResponse(request);
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
    expect((yield* runtime.firewall.readDecisions({})).decisions).toMatchObject(
      [{ reason: "rule-violation", decision: "withheld" }],
    );
    expect(yield* runtime.inbox).toEqual([]);
    expect(
      (yield* runtime.harness.readHistory({ address: bobAddress })).records,
    ).toEqual([]);
    expect(yield* subject.historyOf()).toEqual(["sent"]);
    expect(subject.submitted).toEqual([]);
  }));

it("fails a client's send and stores nothing when the check of its message could not run", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open({
      policy: composedPolicy({ decide: () => Effect.dieMessage("down") }),
    });
    expect(
      yield* subject.sendFrom({ text: "Hello" }).pipe(Effect.flip),
    ).toMatchObject({ _tag: "SendError", reason: "network-unavailable" });
    expect((yield* runtime.firewall.readDecisions({})).decisions).toEqual([]);
    expect(yield* subject.historyOf()).toEqual([]);
  }));

it("answers a message that broke a rule with the configured reply when no rule gave one", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const withheldReply = "Your message was not accepted.";
    const runtime = yield* subject.open({
      withheldReply,
      policy: composedPolicy(incomingViolation),
    });
    yield* subject.sendFrom({ text: "Private peer content" });
    expect(subject.submitted).toMatchObject([
      { content: [{ type: "text", text: withheldReply }] },
    ]);
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
    expect(subject.submitted).toMatchObject([
      {
        author: subject.self,
        recipients: [subject.bob],
        replyToMsgId: withheld.id,
        content: [{ type: "text", text: "Keep private information private." }],
      },
    ]);
    expect(yield* runtime.inbox).toEqual([]);
  }));

it("sends no reply that its own message policy refuses, and saves that refusal", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open({
      policy: composedPolicy(violation, "reply"),
    });
    yield* subject.sendFrom({ text: "Private peer content" });
    const { decisions } = yield* runtime.firewall.readDecisions({});
    expect(
      decisions.map(({ decision }) => decision).sort(compareAscii),
    ).toEqual(["refused", "withheld"]);
    expect(subject.submitted).toEqual([]);
  }));

it("stores nothing of a message whose decision cannot be saved, fails its send and keeps serving", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    let fails = true;
    const runtime = yield* subject.open({
      policy: composedPolicy({
        decide: (request) => Effect.succeed(decisionResponse(request)),
      }),
      decorateFirewall: (firewall) => ({
        ...firewall,
        checkIncoming: (incoming, evidence) =>
          Effect.map(firewall.checkIncoming(incoming, evidence), (decision) =>
            fails
              ? {
                  ...decision,
                  save: new FirewallPolicyError({
                    reason: "persistence-failed",
                  }),
                }
              : decision,
          ),
      }),
    });
    const sent = yield* subject
      .sendFrom({ text: "Publish atomically" })
      .pipe(Effect.exit);
    expect(Exit.isFailure(sent) && Cause.isDie(sent.cause)).toBe(true);
    expect((yield* runtime.firewall.readDecisions({})).decisions).toEqual([]);
    expect(yield* subject.historyOf()).toEqual([]);
    fails = false;
    yield* subject.sendFrom({ text: "Publish atomically" });
    expect(yield* runtime.inbox).toHaveLength(1);
    expect((yield* runtime.firewall.readDecisions({})).decisions).toHaveLength(
      1,
    );
  }));

/** `open` starts an instance and `close`, from the other agent, ends it. */
const exchange = contractOf("exchange", [
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
      withheldReply,
      contracts: [exchange],
      policy: composedPolicy({
        decide: (request) => Effect.succeed(decisionResponse(request)),
      }),
    });
    const plain = yield* subject.message("Hello, can you help?");
    yield* subject.deliver(plain);
    const outOfTurn = yield* subject.message("(close :ontology exchange)");
    yield* subject.deliver(outOfTurn);
    const { decisions } = yield* waitForDecisions(runtime, 4);
    expect(
      decisions
        .map(({ direction, decision }) => `${direction} ${decision}`)
        .sort(compareAscii),
    ).toEqual([
      "incoming withheld",
      "incoming withheld",
      "outgoing allowed",
      "outgoing allowed",
    ]);
    expect(subject.submitted).toMatchObject(
      [plain, outOfTurn].map((withheld) => ({
        recipients: [subject.bob],
        replyToMsgId: withheld.msgId,
        content: [{ type: "text", text: withheldReply }],
      })),
    );
    yield* subject.deliver(yield* subject.message("(open :ontology exchange)"));
    expect((yield* waitForInbox(runtime)).entries).toHaveLength(1);
  }));

it("answers an act its text rule withholds with that rule's feedback when it holds norms", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open({
      contracts: [exchange],
      policy: composedPolicy(incomingViolation, "reply"),
    });
    const act = yield* subject.message("(open :ontology exchange)");
    yield* subject.deliver(act);
    yield* waitForDecisions(runtime, 2);
    expect(subject.submitted).toMatchObject([
      {
        replyToMsgId: act.msgId,
        content: [{ type: "text", text: "Keep private information private." }],
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
        maximumHistoryRecords: window,
        contracts: [exchange],
        policy: composedPolicy({
          decide: (request) =>
            Effect.succeed(decisionResponse(request, "compliant")),
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

it("assesses the latest bounded preceding history without the candidate", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const unchecked = yield* subject.open();
    for (const text of ["Oldest", "Older", "Recent", "Newest"]) {
      yield* unchecked.harness.send({ to: bobAddress, text });
    }
    const seen: DecisionRequest[] = [];
    yield* subject.open({
      maximumHistoryRecords: 2,
      policy: composedPolicy({
        decide: (request) =>
          Effect.sync(() => {
            seen.push(request);
            return decisionResponse(request);
          }),
      }),
    });
    yield* subject.sendFrom({ text: "Candidate" });
    const [assessment] = seen;
    if (assessment === undefined) {
      return yield* Effect.dieMessage("Expected assessment");
    }
    expect(decisionState(assessment.state)).toMatchObject({
      candidate: { text: "Candidate" },
      history: [{ text: "Recent" }, { text: "Newest" }],
      earlierHistoryOmitted: true,
    });
  }));

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
    expect(
      rest.records.map((record) => record.message.content[0]),
    ).toMatchObject([{ text: "Message 50" }, { text: "Later message" }]);
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
      addresses: [...new Set(held)].sort(compareAscii),
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
          return Effect.succeed(decisionResponse(request));
        }),
    });
    const runtime = yield* subject.open({ policy });
    const cancelled = yield* runtime.harness
      .send({ to: bobAddress, text: "Cancelled draft" })
      .pipe(Effect.forkScoped);
    yield* Deferred.await(entered);
    yield* Fiber.interrupt(cancelled);
    yield* runtime.harness.send({ to: bobAddress, text: "Next draft" });
    expect(subject.submitted).toMatchObject([
      { content: [{ text: "Next draft" }] },
    ]);
  }));

it("retains a draft's input while its caller changes the original object", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const entered = yield* Deferred.make<undefined>();
    const release = yield* Deferred.make<undefined>();
    const runtime = yield* subject.open({
      policy: composedPolicy({
        decide: (request) =>
          Deferred.succeed(entered, undefined).pipe(
            Effect.zipRight(Deferred.await(release)),
            Effect.as(decisionResponse(request)),
          ),
      }),
    });
    const input = { to: bobAddress, text: "Captured draft" };
    const sending = yield* runtime.harness.send(input).pipe(Effect.forkScoped);
    yield* Deferred.await(entered);
    input.text = "Later mutation";
    yield* Deferred.succeed(release, undefined);
    yield* Fiber.join(sending);
    expect(subject.submitted).toMatchObject([
      { content: [{ text: "Captured draft" }] },
    ]);
  }));

it("reads a saved conversation's history at its agent's address, and reports an unknown agent", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open();
    yield* runtime.harness.send({ to: bobAddress, text: "Hello" });
    yield* subject.sendFrom({ text: "Hello from carol" }, subject.carol);
    const saved: Record<string, unknown> = {
      bob: ["sent"],
      carol: ["received"],
      nobody: "unknown-agent",
    };
    yield* forAll(
      named,
      Effect.fn(function* (name) {
        const read = yield* runtime.harness
          .readHistory({ address: AgentAddress.make(`agent:${name}`) })
          .pipe(
            Effect.match({
              onFailure: ({ reason }) => reason,
              onSuccess: ({ records }) => records.map((record) => record.kind),
            }),
          );
        expect(read).toEqual(saved[name]);
      }),
    );
  }));
