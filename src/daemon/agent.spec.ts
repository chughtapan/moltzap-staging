/** @file SocialHarness sends, policy checks, the inbox and history, including across restarts. */

import { scopedLive as it } from "@effect/vitest";
import {
  Array as Arr,
  Cause,
  Deferred,
  Effect,
  Exit,
  FastCheck as fc,
  Fiber,
  Order,
  Schema,
  Scope,
} from "effect";
import { expect } from "vitest";
import type { AgentCard } from "../identity.js";
import { digest } from "../__tests__/agents.js";
import { Message, MsgId } from "../communication/index.js";
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
import { AgentAddress, type HistoryRecord } from "../mcp/tools.js";
import {
  composedPolicy,
  decisionResponse,
  decisionState,
  makeHarnessFixture,
  waitForInbox,
} from "./__tests__/service-fixtures.js";

const bobAddress = AgentAddress.make("agent:bob");
const carolAddress = AgentAddress.make("agent:carol");

type Fixture = Effect.Effect.Success<typeof makeHarnessFixture>;
type Runtime = Effect.Effect.Success<ReturnType<Fixture["open"]>>;

/** A decision service that finds every message in violation of its rule. */
const violation: DecisionProvider = {
  decide: (request) => Effect.succeed(decisionResponse(request, "violation")),
};

function ordinaryMessage(subject: Fixture, text: string) {
  return Message.create({
    author: subject.self,
    recipients: [subject.bob],
    content: [{ type: "text", text }],
  });
}

/** Delivers a text message to alice from `author`, bob unless given. */
function receive(subject: Fixture, text: string, author?: AgentCard) {
  const sender = author ?? subject.bob;
  return Effect.flatMap(subject.message(text, sender), subject.deliver);
}

/** Has alice admit a text message `author` sends her, bob unless given, storing it in the fixture's history. */
function admit(
  subject: Fixture,
  runtime: Runtime,
  text: string,
  author?: AgentCard,
) {
  return Effect.flatMap(
    subject.message(text, author ?? subject.bob),
    (message) => runtime.harness.admit(message, subject.retain(message)),
  );
}

/** Gives alice a conversation with a new peer `name` and returns its address. */
function hold(subject: Fixture, name: string) {
  return Effect.gen(function* () {
    const peer = yield* subject.addPeer(name);
    yield* subject.addConversation([subject.self, peer]);
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

it("admits one sender's messages one at a time while another sender's proceed", () =>
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
    const first = yield* Effect.fork(admit(subject, runtime, "First"));
    yield* Deferred.await(entered);
    const second = yield* Effect.fork(admit(subject, runtime, "Second"));
    yield* admit(subject, runtime, "Independent", subject.carol);
    expect((yield* waitForInbox(runtime)).entries).toMatchObject([
      { address: carolAddress },
    ]);
    expect(assessed).toEqual(["First", "Independent"]);
    yield* Deferred.succeed(release, undefined);
    yield* Fiber.join(Fiber.zip(first, second));
    expect(
      (yield* waitForInbox(runtime, 3)).entries.map((entry) => entry.address),
    ).toEqual(["agent:carol", "agent:bob", "agent:bob"]);
    expect(assessed).toEqual(["First", "Independent", "Second"]);
  }));

it("keeps an admitted message's inbox entry across a restart and gives a message delivered again no second entry", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const scope = yield* Scope.make();
    const admitting = yield* subject
      .open({
        policy: composedPolicy({
          decide: (request) =>
            Effect.succeed(decisionResponse(request, "compliant", 0.6)),
        }),
      })
      .pipe(Scope.extend(scope));
    const message = yield* subject.message("Can we meet Tuesday?");
    yield* admitting.harness.admit(message, subject.retain(message));
    const [delivery] = (yield* waitForInbox(admitting)).entries;
    if (delivery === undefined) {
      return yield* Effect.dieMessage("Expected an inbox entry");
    }
    expect(delivery.guidance).toBeDefined();
    yield* Scope.close(scope, Exit.void);
    const reopened = yield* subject.open();
    expect(
      (yield* reopened.harness.readHistory({ address: bobAddress })).records,
    ).toMatchObject([{ kind: "received", ...delivery }]);
    yield* subject.deliver(message);
    expect((yield* waitForInbox(reopened)).entries).toHaveLength(1);
  }));

it("reads the entries after a cursor in the order they were allowed, a page at a time, and none before the cursor a read without one returns", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open();
    const start = yield* runtime.harness.receive({});
    expect(start.entries).toEqual([]);
    for (let index = 0; index < 57; index++) {
      yield* receive(subject, "Hello", yield* subject.addPeer(`peer-${index}`));
    }
    const all = (yield* waitForInbox(runtime, 57)).entries;
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

it("refuses a message that breaks a rule to its sender and stores only the check", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open({
      policy: composedPolicy(violation),
    });
    const refusal = yield* admit(subject, runtime, "Private peer content").pipe(
      Effect.flip,
    );
    expect(refusal).toMatchObject({
      reason: "firewall-refused",
      refusal: "rule-violation",
      feedback: ["Keep private information private."],
    });
    expect(refusal.message).toMatch(
      /^Error: Keep private information private\. Check: \S+; rule-violation\.$/u,
    );
    expect((yield* runtime.firewall.readDecisions({})).decisions).toMatchObject(
      [
        {
          direction: "incoming",
          reason: "rule-violation",
          decision: "refused",
        },
      ],
    );
    expect((yield* runtime.inbox.listInbox(1, "0")).entries).toEqual([]);
    expect(
      (yield* runtime.harness.readHistory({ address: bobAddress })).records,
    ).toEqual([]);
  }));

it("stores nothing of a message whose decision cannot be saved, and admits the next", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    let failing = true;
    const failure = Effect.suspend(() =>
      failing
        ? new FirewallPolicyError({ reason: "persistence-failed" })
        : Effect.void,
    );
    const runtime = yield* subject.open({
      policy: composedPolicy({
        decide: (request) => Effect.succeed(decisionResponse(request)),
      }),
      decorateFirewall: (firewall) => ({
        ...firewall,
        checkIncoming: (incoming, evidence) =>
          firewall.checkIncoming(incoming, evidence).pipe(
            Effect.map((decision) => ({
              ...decision,
              save: Effect.zipRight(decision.save, failure),
            })),
          ),
      }),
    });
    const failed = yield* admit(subject, runtime, "Publish atomically").pipe(
      Effect.exit,
    );
    expect(Exit.isFailure(failed) && Cause.isDie(failed.cause)).toBe(true);
    expect((yield* runtime.firewall.readDecisions({})).decisions).toEqual([]);
    expect((yield* runtime.inbox.listInbox(1, "0")).entries).toEqual([]);
    expect(
      (yield* runtime.harness.readHistory({ address: bobAddress })).records,
    ).toEqual([]);
    failing = false;
    yield* admit(subject, runtime, "Publish atomically");
    expect((yield* runtime.inbox.listInbox(1, "0")).entries).toHaveLength(1);
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
        const close = yield* Message.create({
          author: subject.bob,
          recipients: [subject.self],
          content: [{ type: "text", text: "(close :ontology exchange)" }],
          replyToMsgId: opened.id,
        });
        yield* runtime.harness.admit(close, subject.retain(close));
      }
      expect((yield* waitForInbox(runtime)).entries).toHaveLength(instances);
    }),
  { fastCheck: { numRuns: 6 } },
);

it("assesses the latest bounded preceding history without the candidate", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    for (const text of ["Oldest", "Older", "Recent", "Newest"]) {
      yield* subject.retain(yield* ordinaryMessage(subject, text));
    }
    const seen: DecisionRequest[] = [];
    const runtime = yield* subject.open({
      maximumHistoryRecords: 2,
      policy: composedPolicy({
        decide: (request) =>
          Effect.sync(() => {
            seen.push(request);
            return decisionResponse(request);
          }),
      }),
    });
    yield* admit(subject, runtime, "Candidate");
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

it("continues history through the boundary its first page fixed", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open();
    for (let index = 0; index < 51; index++) {
      yield* subject.retain(
        yield* ordinaryMessage(subject, `Message ${index}`),
      );
    }
    const first = yield* runtime.harness.readHistory({
      address: bobAddress,
    });
    expect(first.records).toHaveLength(50);
    if (first.nextCursor === undefined) {
      return yield* Effect.dieMessage("Expected history cursor");
    }
    yield* subject.retain(yield* ordinaryMessage(subject, "Later message"));
    const remaining = yield* runtime.harness.readHistory({
      address: bobAddress,
      cursor: first.nextCursor,
    });
    expect(remaining.records.map((record) => record.kind)).toEqual(["sent"]);
    expect(remaining.nextCursor).toBeUndefined();
    expect(
      (yield* runtime.harness
        .readHistory({ address: carolAddress, cursor: first.nextCursor })
        .pipe(Effect.flip)).reason,
    ).toBe("invalid-cursor");
    const records: HistoryRecord[] = [];
    let page = yield* runtime.harness.readHistory({ address: bobAddress });
    records.push(...page.records);
    while (page.nextCursor !== undefined) {
      page = yield* runtime.harness.readHistory({
        address: bobAddress,
        cursor: page.nextCursor,
      });
      records.push(...page.records);
    }
    expect(records.filter((record) => record.kind === "sent")).toHaveLength(52);
  }));

it("lists every conversation once in address order in pages of 50, with one that starts mid-way only when it sorts after the pages already read", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open();
    const peer = fc.nat({ max: 199 }).map((index) => `peer-${index}`);
    const held = new Set<string>();
    for (let index = 0; index < 200; index += 7) {
      held.add(yield* hold(subject, `peer-${index}`));
    }
    yield* forAll(
      fc.tuple(fc.array(peer, { maxLength: 8 }), peer),
      Effect.fn(function* ([arrivals, late]) {
        for (const name of arrivals) {
          held.add(yield* hold(subject, name));
        }
        const before = Arr.sort(held, Order.string);
        const read = before.slice(0, 50);
        let page = yield* runtime.harness.listConversations();
        const pages = [page.addresses];
        held.add(yield* hold(subject, late));
        while (page.nextCursor !== undefined) {
          page = yield* runtime.harness.listConversations({
            cursor: page.nextCursor,
          });
          pages.push(page.addresses);
        }
        const unread =
          before.length > 50
            ? Arr.sort(held, Order.string).filter((address) =>
                read.every((earlier) => earlier < address),
              )
            : [];
        expect(pages).toEqual([read, ...Arr.chunksOf(unread, 50)]);
      }),
    );
  }));

it("cancels an unaccepted draft and releases assessment capacity", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    yield* subject.addConversation([subject.self, subject.bob]);
    const entered = yield* Deferred.make<undefined>();
    let first = true;
    const policy = composedPolicy(
      {
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
      },
      1,
    );
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
    yield* subject.addConversation([subject.self, subject.bob]);
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
    yield* subject.retain(yield* ordinaryMessage(subject, "Hello"));
    yield* receive(subject, "Hello from carol", subject.carol);
    yield* waitForInbox(runtime);
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
