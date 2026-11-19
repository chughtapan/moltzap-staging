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
  Schedule,
  Schema,
  Scope,
} from "effect";
import { expect } from "vitest";
import type { AgentCard } from "../identity.js";
import type { InboxEntry } from "../mcp/tools.js";
import type { HistoryPage } from "./agent.js";
import { digest } from "../__tests__/agents.js";
import { Message, MessageAddressInput, MsgId } from "../communication/index.js";
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
  waitForInbox,
} from "./__tests__/service-fixtures.js";

const bobAddress = MessageAddressInput.make("agent:bob");
const carolAddress = MessageAddressInput.make("agent:carol");

type Fixture = Effect.Effect.Success<typeof makeHarnessFixture>;
type Runtime = Effect.Effect.Success<ReturnType<Fixture["open"]>>;

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

function waitForDecisions(runtime: Runtime, count = 1) {
  return runtime.firewall.readDecisions({}).pipe(
    Effect.repeat({
      until: (page) =>
        new Set(page.decisions.map((decision) => decision.checkId)).size >=
        count,
      schedule: Schedule.spaced("5 millis"),
    }),
    Effect.zipRight(runtime.firewall.readDecisions({})),
    Effect.timeout("10 seconds"),
  );
}

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

/** Waits for a pending inbox entry, then receives the pending ones. */
function acknowledgeNext(runtime: Runtime) {
  return Effect.zipRight(waitForInbox(runtime), runtime.harness.receive({}));
}

/** Waits until the inbox holds `count` pending entries across all its pages. */
function waitForPending(runtime: Runtime, count: number) {
  return readInbox(runtime).pipe(
    Effect.repeat({
      until: (entries) => entries.length === count,
      schedule: Schedule.spaced("5 millis"),
    }),
    Effect.timeout("10 seconds"),
  );
}

function readInbox(runtime: Runtime) {
  return Effect.gen(function* () {
    const entries: InboxEntry[] = [];
    let page = yield* runtime.inbox.listInbox();
    entries.push(...page.entries);
    while (page.nextCursor !== undefined) {
      page = yield* runtime.inbox.listInbox({ cursor: page.nextCursor });
      entries.push(...page.entries);
    }
    return entries;
  });
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

/**
 * One or more peers an address names, in name order: bob and carol, whom the
 * fixture knows, and nobody, whom it does not. `canonical` is the one address
 * of alice's conversation with them. `variant` lists the same agents as a
 * group, with or without alice and in either order. `address` is one of the two.
 */
const membership = fc
  .tuple(
    fc.subarray(["bob", "carol", "nobody"], { minLength: 1 }),
    fc.boolean(),
    fc.boolean(),
    fc.boolean(),
  )
  .map(([others, namesItself, reversed, asVariant]) => {
    const members = ["alice", ...others];
    const listed = namesItself ? members : others;
    const canonical = MessageAddressInput.make(
      others.length === 1
        ? `agent:${others.join("")}`
        : `group:${members.join(",")}`,
    );
    const variant = MessageAddressInput.make(
      `group:${(reversed ? Arr.reverse(listed) : listed).join(",")}`,
    );
    return {
      others,
      canonical,
      variant,
      address: asVariant ? variant : canonical,
    };
  });

it("sends each call as new work to exactly the agents its address names, with the reply reference it gives, and refuses an address that names an unknown agent", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const { harness } = yield* subject.open();
    const receipts: string[] = [];
    yield* forAll(
      fc.tuple(
        membership,
        fc.option(fc.integer({ min: 0, max: 255 }), { nil: undefined }),
      ),
      Effect.fn(function* ([{ others, canonical, address }, byte]) {
        const replyToMsgId =
          byte === undefined
            ? undefined
            : Schema.decodeUnknownSync(MsgId)(digest("msg_", byte));
        const request = { to: address, text: "Hello", replyToMsgId };
        const dispatched = subject.submitted.length;
        if (others.includes("nobody")) {
          expect(yield* harness.send(request).pipe(Effect.flip)).toMatchObject({
            reason: "unknown-agent",
          });
          expect(subject.submitted).toHaveLength(dispatched);
        } else {
          const receipt = yield* harness.send(request);
          expect(receipt.address).toBe(canonical);
          expect(receipts).not.toContain(receipt.id);
          receipts.push(receipt.id);
          expect(subject.submitted.slice(dispatched)).toMatchObject([
            {
              msgId: receipt.id,
              recipients: others.map((agentName) => ({ agentName })),
            },
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

it("refuses every group address before dispatch on a daemon without groups, and still sends to one agent", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const { harness } = yield* subject.open({ hideGroups: true });
    yield* forAll(
      membership,
      Effect.fn(function* ({ variant }) {
        expect(
          yield* harness.send({ to: variant, text: "Hello" }).pipe(Effect.flip),
        ).toMatchObject({ reason: "invalid-address" });
      }),
    );
    expect(subject.submitted).toEqual([]);
    yield* harness.send({ to: bobAddress, text: "Hello" });
    expect(subject.submitted).toHaveLength(1);
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

it("checks each conversation in order while another conversation progresses", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const entered = yield* Deferred.make<undefined>();
    const release = yield* Deferred.make<undefined>();
    const assessed: string[] = [];
    const runtime = yield* subject.open({
      maximumConcurrentChecks: 2,
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
    yield* receive(subject, "First");
    yield* Deferred.await(entered);
    yield* receive(subject, "Second");
    yield* receive(subject, "Independent", subject.carol);
    expect((yield* waitForInbox(runtime)).entries).toMatchObject([
      { address: carolAddress },
    ]);
    expect(assessed).toEqual(["First", "Independent"]);
    yield* Deferred.succeed(release, undefined);
    expect(
      (yield* waitForInbox(runtime, 3)).entries.map((entry) => entry.address),
    ).toEqual(["agent:bob", "agent:bob", "agent:carol"]);
    expect(assessed).toEqual(["First", "Independent", "Second"]);
  }));

it("recovers an interrupted check and keeps its inbox entry after edits, delivery and restart", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const scope = yield* Scope.make();
    const entered = yield* Deferred.make<undefined>();
    yield* subject
      .open({
        policy: composedPolicy({
          decide: () =>
            Deferred.succeed(entered, undefined).pipe(
              Effect.zipRight(Effect.never),
            ),
        }),
      })
      .pipe(Scope.extend(scope));
    const message = yield* subject.message("Can we meet Tuesday?");
    yield* subject.deliver(message);
    yield* Deferred.await(entered);
    yield* Scope.close(scope, Exit.void);
    const resumedScope = yield* Scope.make();
    const resumed = yield* subject
      .open({
        policy: composedPolicy({
          decide: (request) =>
            Effect.succeed(decisionResponse(request, "compliant", 0.6)),
        }),
      })
      .pipe(Scope.extend(resumedScope));
    const [delivery] = (yield* waitForInbox(resumed)).entries;
    if (delivery === undefined) {
      return yield* Effect.dieMessage("Expected recovered delivery");
    }
    const stored = {
      address: delivery.address,
      item: delivery.item,
      guidance: delivery.guidance,
    };
    expect(stored.guidance).toBeDefined();
    yield* resumed.firewall.updatePolicy({ remove: ["privacy"] });
    yield* resumed.inbox.markDelivered([delivery.deliveryToken]);
    yield* resumed.inbox.markDelivered([delivery.deliveryToken]);
    expect((yield* resumed.inbox.listInbox()).entries).toEqual([]);
    expect(
      (yield* resumed.harness.readHistory({ address: bobAddress })).records,
    ).toMatchObject([{ kind: "received", ...stored }]);
    yield* Scope.close(resumedScope, Exit.void);
    const reopened = yield* subject.open();
    expect(
      (yield* reopened.harness.readHistory({ address: bobAddress })).records,
    ).toMatchObject([{ kind: "received", ...stored }]);
    yield* subject.deliver(message);
    expect((yield* reopened.inbox.listInbox()).entries).toEqual([]);
  }));

it("keeps an inbox page boundary while arrivals and deliveries change what is pending", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open();
    for (let index = 0; index < 57; index++) {
      yield* receive(subject, "Hello", yield* subject.addPeer(`peer-${index}`));
    }
    yield* waitForPending(runtime, 57);
    const first = yield* runtime.inbox.listInbox();
    expect(first.entries).toHaveLength(50);
    if (first.nextCursor === undefined) {
      return yield* Effect.dieMessage("Expected inbox cursor");
    }
    const preview = yield* runtime.inbox.listInbox({
      cursor: first.nextCursor,
    });
    const delivered = preview.entries[0];
    if (delivered === undefined) {
      return yield* Effect.dieMessage("Expected remaining delivery");
    }
    yield* runtime.inbox.markDelivered([delivered.deliveryToken]);
    yield* receive(subject, "Hello", yield* subject.addPeer("later"));
    yield* waitForPending(runtime, 57);
    const remainder = yield* runtime.inbox.listInbox({
      cursor: first.nextCursor,
    });
    expect(remainder.entries.map((entry) => entry.deliveryToken)).toEqual(
      preview.entries.slice(1).map((entry) => entry.deliveryToken),
    );
    expect(remainder.nextCursor).toBeUndefined();
    expect(
      (yield* runtime.harness.receive({ cursor: "invalid" }).pipe(Effect.flip))
        .reason,
    ).toBe("invalid-cursor");
  }));

it("withholds peer content from both the inbox and history", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open({
      policy: composedPolicy(violation),
    });
    yield* receive(subject, "Private peer content");
    expect((yield* waitForDecisions(runtime)).decisions).toMatchObject([
      { reason: "rule-violation", decision: "withheld" },
    ]);
    expect((yield* runtime.inbox.listInbox()).entries).toEqual([]);
    expect(
      (yield* runtime.harness.readHistory({ address: bobAddress })).records,
    ).toEqual([]);
    expect(subject.submitted).toEqual([]);
  }));

it("answers a message that broke a rule with the configured reply when no rule gave one", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const withheldReply = "Your message was not accepted.";
    const runtime = yield* subject.open({
      withheldReply,
      policy: composedPolicy(incomingViolation),
    });
    yield* receive(subject, "Private peer content");
    yield* waitForDecisions(runtime, 2);
    expect(subject.submitted).toMatchObject([
      { content: [{ type: "text", text: withheldReply }] },
    ]);
    expect((yield* runtime.inbox.listInbox()).entries).toEqual([]);
  }));

it("answers a message its rule withholds with that rule's feedback, as an ordinary message that names the withheld one", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open({
      policy: composedPolicy(incomingViolation, "reply"),
    });

    const withheld = yield* subject.message(
      "Private peer content",
      subject.bob,
    );
    yield* subject.deliver(withheld);
    yield* waitForDecisions(runtime, 2);

    expect(subject.submitted).toMatchObject([
      {
        author: subject.self,
        recipients: [subject.bob],
        replyToMsgId: withheld.msgId,
        content: [{ type: "text", text: "Keep private information private." }],
      },
    ]);
    expect((yield* runtime.inbox.listInbox()).entries).toEqual([]);
  }));

it("sends no reply that its own message policy refuses, and saves that refusal", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open({
      policy: composedPolicy(violation, "reply"),
    });

    yield* receive(subject, "Private peer content");
    const { decisions } = yield* waitForDecisions(runtime, 2);

    expect(
      decisions.map(({ decision }) => decision).sort(compareAscii),
    ).toEqual(["refused", "withheld"]);
    expect(subject.submitted).toEqual([]);
  }));

it("returns a waiting read at once when an entry is already pending", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open();
    yield* receive(subject, "Already here");
    yield* waitForInbox(runtime);

    const page = yield* runtime.harness
      .receive({ wait: 300 })
      .pipe(Effect.timeout("2 seconds"));

    expect(page.entries).toHaveLength(1);
  }));

it("ends a waiting read as soon as an entry arrives", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open();
    const reading = yield* runtime.harness
      .receive({ wait: 300 })
      .pipe(Effect.fork);

    yield* receive(subject, "Arrived while waiting");
    const page = yield* Fiber.join(reading).pipe(Effect.timeout("10 seconds"));

    expect(page.entries).toMatchObject([
      {
        item: {
          message: {
            content: [{ type: "text", text: "Arrived while waiting" }],
          },
        },
      },
    ]);
  }));

it("returns an empty page when the wait ends with nothing pending", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open();

    const page = yield* runtime.harness.receive({ wait: 0.05 });

    expect(page.entries).toEqual([]);
  }));

it("rolls back a check whose decision cannot be saved, then checks the message again and keeps serving", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const failed = yield* Deferred.make<undefined>();
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
              save: Effect.zipRight(
                decision.save,
                Effect.whenEffect(
                  new FirewallPolicyError({ reason: "persistence-failed" }),
                  Deferred.succeed(failed, undefined),
                ),
              ),
            })),
          ),
      }),
    });
    yield* receive(subject, "Publish atomically");
    yield* Deferred.await(failed);
    expect((yield* runtime.firewall.readDecisions({})).decisions).toEqual([]);
    expect((yield* runtime.harness.receive({})).entries).toEqual([]);
    expect((yield* waitForInbox(runtime)).entries).toHaveLength(1);
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
        yield* subject.deliver(
          yield* Message.create({
            author: subject.bob,
            recipients: [subject.self],
            content: [{ type: "text", text: "(close :ontology exchange)" }],
            replyToMsgId: opened.id,
          }),
        );
        yield* waitForInbox(runtime, instance);
      }
      expect((yield* readInbox(runtime)).length).toBe(instances);
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
    yield* receive(subject, "Candidate");
    yield* waitForInbox(runtime);
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
        .receive({ cursor: first.nextCursor })
        .pipe(Effect.flip)).reason,
    ).toBe("invalid-cursor");
    expect(
      (yield* runtime.harness
        .readHistory({ address: carolAddress, cursor: first.nextCursor })
        .pipe(Effect.flip)).reason,
    ).toBe("invalid-cursor");
    const records: Array<HistoryPage["records"][number]> = [];
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
    const runtime = yield* subject.open({
      policy: { ...policy, maximumConcurrentChecks: 1 },
    });
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

it("reads a saved conversation's history at its canonical address, reports an unknown agent at a canonical address that names one, and refuses every other spelling as invalid", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open();
    yield* subject.retain(yield* ordinaryMessage(subject, "Hello"));
    yield* subject.retain(
      yield* Message.create({
        author: subject.self,
        recipients: [subject.bob, subject.carol],
        content: [{ type: "text", text: "Hello both" }],
      }),
    );
    yield* receive(subject, "Hello from carol", subject.carol);
    yield* acknowledgeNext(runtime);
    const saved: Record<string, readonly string[]> = {
      "agent:bob": ["sent"],
      "agent:carol": ["received"],
      "group:alice,bob,carol": ["sent"],
    };
    const read = (address: MessageAddressInput) =>
      runtime.harness.readHistory({ address }).pipe(
        Effect.match({
          onFailure: ({ reason }) => reason,
          onSuccess: ({ records }) => records.map((record) => record.kind),
        }),
      );
    yield* forAll(
      membership,
      Effect.fn(function* ({ others, canonical, variant }) {
        expect(yield* read(canonical)).toEqual(
          others.includes("nobody") ? "unknown-agent" : saved[canonical],
        );
        if (variant !== canonical) {
          expect(yield* read(variant)).toBe("invalid-address");
        }
      }),
    );
  }));
