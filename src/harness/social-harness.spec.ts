/** @file SocialHarness sends, policy checks, the saved inbox and history, including across restarts. */

import { scopedLive as it } from "@effect/vitest";
import {
  Array as Arr,
  Deferred,
  Duration,
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
import type { InboxEntry } from "../index.js";
import type { ReadHistoryResult } from "./social-harness.js";
import {
  Answer,
  encodeCollectiveContent,
  Message,
  MessageAddressInput,
  ReceivedRequest,
  Request,
} from "../communication/index.js";
import { compareAscii } from "../encoding.js";
import {
  type DecisionProvider,
  type DecisionRequest,
  FirewallPolicyError,
} from "../firewall/index.js";
import {
  decisionResponse,
  decisionState,
  firewallOptions,
  makeHarnessFixture,
  waitForInbox,
} from "./__tests__/service-fixtures.js";

/* eslint-disable agent-code-guard/no-hardcoded-assertion-literals -- Closed public failure reasons are part of the service contract. */

const bobAddress = MessageAddressInput.make("agent:bob");
const carolAddress = MessageAddressInput.make("agent:carol");

type Fixture = Effect.Effect.Success<typeof makeHarnessFixture>;
type Runtime = Effect.Effect.Success<ReturnType<Fixture["open"]>>;

/** A decision service that finds every message in violation of its rule. */
const violation: DecisionProvider = {
  decide: (request) => Effect.succeed(decisionResponse(request, "violation")),
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
    Effect.raceFirst(runtime.failed),
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
  return Effect.flatMap(subject.message(text, sender), (message) =>
    subject.deliver({ kind: "message", message }),
  );
}

/** Waits for a delivery, then acknowledges every pending one. */
function acknowledgeNext(runtime: Runtime) {
  return Effect.flatMap(waitForInbox(runtime), ({ items }) =>
    Effect.forEach(
      items,
      (entry) => runtime.harness.acknowledgeDelivery(entry.deliveryToken),
      { concurrency: 1 },
    ),
  );
}

/** Waits until the inbox holds `count` pending items across all its pages. */
function waitForPending(runtime: Runtime, count: number) {
  return readInbox(runtime).pipe(
    Effect.repeat({
      until: (items) => items.length === count,
      schedule: Schedule.spaced("5 millis"),
    }),
    Effect.raceFirst(runtime.failed),
    Effect.timeout("10 seconds"),
  );
}

function readInbox(runtime: Runtime) {
  return Effect.gen(function* () {
    const items: InboxEntry[] = [];
    let page = yield* runtime.harness.listInbox();
    items.push(...page.items);
    while (page.nextCursor !== undefined) {
      page = yield* runtime.harness.listInbox({ cursor: page.nextCursor });
      items.push(...page.items);
    }
    return items;
  });
}

/** The result of a question this agent asked and nobody answered. */
function unanswered(subject: Fixture, recipient?: AgentCard) {
  const member = recipient ?? subject.bob;
  return question(subject, subject.self, [member]).pipe(
    Effect.map(
      ({ request }) =>
        ({
          kind: "result",
          result: {
            request,
            closedAt: request.deadlineAt,
            closedBy: "deadline",
            outcomes: [{ member, outcome: { kind: "no-answer" } }],
          },
          sources: { answers: [] },
        }) as const,
    ),
  );
}

function question(
  subject: Fixture,
  author?: AgentCard,
  recipients?: readonly [AgentCard, ...AgentCard[]],
  op: "gather" | "all_gather" = "gather",
) {
  return Effect.gen(function* () {
    const participants: Pick<Message, "author" | "recipients"> = {
      author: author ?? subject.bob,
      recipients: recipients ?? [subject.self],
    };
    const request = yield* Request.create({
      ...participants,
      op,
      question: "Which day works?",
      requestedSchema: {
        type: "object",
        properties: { day: { type: "string" } },
      },
      deadline: 3600,
    });
    const message = yield* Message.create({
      ...participants,
      content: yield* encodeCollectiveContent(
        {
          kind: "operation",
          op,
          operationId: request.operationId,
          requestedSchema: request.requestedSchema,
          deadlineAt: request.deadlineAt,
        },
        request.question,
      ),
    });
    return {
      request,
      message: yield* Schema.validate(ReceivedRequest)(message),
    };
  });
}

/**
 * Gives alice a conversation with a new peer `name` and returns its address.
 * A `live` conversation is one communication lists; a `result` conversation
 * holds only a saved result.
 */
function hold(subject: Fixture, name: string, holding: "live" | "result") {
  return Effect.gen(function* () {
    const peer = yield* subject.addPeer(name);
    yield* holding === "live"
      ? subject.addConversation([subject.self, peer])
      : subject.deliver(yield* unanswered(subject, peer));
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

it("sends each call as new work to exactly the agents its address names, and refuses an address that names an unknown agent", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const { harness } = yield* subject.open();
    const receipts: string[] = [];
    yield* forAll(
      membership,
      Effect.fn(function* ({ others, canonical, address }) {
        const request = { to: address, text: "Hello" };
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

it("reports a failed send once without sending it again", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open();
    yield* subject.failSends();
    expect(
      yield* runtime.harness
        .send({ to: bobAddress, text: "Not stored" })
        .pipe(Effect.flip),
    ).toMatchObject({ reason: "persistence-failed" });
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
      policy: firewallOptions({
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
    expect((yield* waitForInbox(runtime)).items).toMatchObject([
      { address: carolAddress },
    ]);
    expect(assessed).toEqual(["First", "Independent"]);
    yield* Deferred.succeed(release, undefined);
    expect(
      (yield* waitForInbox(runtime, 3)).items.map((item) => item.address),
    ).toEqual(["agent:bob", "agent:bob", "agent:carol"]);
    expect(assessed).toEqual(["First", "Independent", "Second"]);
  }));

it("recovers an interrupted check and retains its published view after edits, acknowledgment and restart", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const scope = yield* Scope.make();
    const entered = yield* Deferred.make<undefined>();
    yield* subject
      .open({
        policy: firewallOptions({
          decide: () =>
            Deferred.succeed(entered, undefined).pipe(
              Effect.zipRight(Effect.never),
            ),
        }),
      })
      .pipe(Scope.extend(scope));
    const message = yield* subject.message("Can we meet Tuesday?");
    yield* subject.deliver({ kind: "message", message });
    yield* Deferred.await(entered);
    yield* Scope.close(scope, Exit.void);
    const resumedScope = yield* Scope.make();
    const resumed = yield* subject
      .open({
        policy: firewallOptions({
          decide: (request) =>
            Effect.succeed(decisionResponse(request, "compliant", 0.6)),
        }),
      })
      .pipe(Scope.extend(resumedScope));
    const [delivery] = (yield* waitForInbox(resumed)).items;
    if (delivery === undefined) {
      return yield* Effect.dieMessage("Expected recovered delivery");
    }
    const saved = {
      address: delivery.address,
      item: delivery.item,
      guidance: delivery.guidance,
    };
    expect(saved.guidance).toBeDefined();
    yield* resumed.firewall.updatePolicy({ remove: ["privacy"] });
    yield* resumed.harness.acknowledgeDelivery(delivery.deliveryToken);
    yield* resumed.harness.acknowledgeDelivery(delivery.deliveryToken);
    expect((yield* resumed.harness.listInbox()).items).toEqual([]);
    expect(
      (yield* resumed.harness.readHistory({ address: bobAddress })).records,
    ).toMatchObject([{ kind: "received", ...saved }]);
    yield* Scope.close(resumedScope, Exit.void);
    const reopened = yield* subject.open();
    expect(
      (yield* reopened.harness.readHistory({ address: bobAddress })).records,
    ).toMatchObject([{ kind: "received", ...saved }]);
    yield* subject.deliver({ kind: "message", message });
    expect((yield* reopened.harness.listInbox()).items).toEqual([]);
  }));

it("keeps an inbox page boundary while arrivals and acknowledgments change pending work", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open();
    for (let index = 0; index < 57; index++) {
      yield* subject.deliver(yield* unanswered(subject));
    }
    yield* waitForPending(runtime, 57);
    const first = yield* runtime.harness.listInbox();
    expect(first.items).toHaveLength(50);
    if (first.nextCursor === undefined) {
      return yield* Effect.dieMessage("Expected inbox cursor");
    }
    const preview = yield* runtime.harness.listInbox({
      cursor: first.nextCursor,
    });
    const acknowledged = preview.items[0];
    if (acknowledged === undefined) {
      return yield* Effect.dieMessage("Expected remaining delivery");
    }
    yield* runtime.harness.acknowledgeDelivery(acknowledged.deliveryToken);
    const later = yield* unanswered(subject);
    yield* subject.deliver(later);
    yield* waitForPending(runtime, 57);
    const remainder = yield* runtime.harness.listInbox({
      cursor: first.nextCursor,
    });
    expect(remainder.items.map((item) => item.deliveryToken)).toEqual(
      preview.items.slice(1).map((item) => item.deliveryToken),
    );
    expect(remainder.nextCursor).toBeUndefined();
    expect(
      (yield* runtime.harness
        .listInbox({ cursor: "invalid" })
        .pipe(Effect.flip)).reason,
    ).toBe("invalid-cursor");
  }));

it("withholds peer content from both the inbox and history", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open({
      policy: firewallOptions(violation),
    });
    yield* receive(subject, "Private peer content");
    expect((yield* waitForDecisions(runtime)).decisions).toMatchObject([
      { reason: "rule-violation", disposition: "withheld" },
    ]);
    expect((yield* runtime.harness.listInbox()).items).toEqual([]);
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
      policy: firewallOptions(violation),
    });
    yield* receive(subject, "Private peer content");
    yield* waitForDecisions(runtime);
    expect(subject.submitted).toMatchObject([
      { content: [{ type: "text", text: withheldReply }] },
    ]);
    expect((yield* runtime.harness.listInbox()).items).toEqual([]);
  }));

it("answers a message its rule withholds with that rule's feedback, as an ordinary message", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open({
      policy: firewallOptions(violation, "reply"),
    });

    yield* receive(subject, "Private peer content");
    yield* waitForDecisions(runtime);

    expect(subject.submitted).toMatchObject([
      {
        author: subject.self,
        recipients: [subject.bob],
        content: [{ type: "text", text: "Keep private information private." }],
      },
    ]);
    expect((yield* runtime.harness.listInbox()).items).toEqual([]);
  }));

it("returns a waiting read at once when an item is already pending", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open();
    yield* receive(subject, "Already here");
    yield* waitForInbox(runtime);

    const page = yield* runtime.harness
      .listInbox({ wait: Duration.minutes(5) })
      .pipe(Effect.timeout("2 seconds"));

    expect(page.items).toHaveLength(1);
  }));

it("ends a waiting read as soon as an item arrives", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open();
    const reading = yield* runtime.harness
      .listInbox({ wait: Duration.minutes(5) })
      .pipe(Effect.fork);

    yield* receive(subject, "Arrived while waiting");
    const page = yield* Fiber.join(reading).pipe(Effect.timeout("10 seconds"));

    expect(page.items).toMatchObject([
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

    const page = yield* runtime.harness.listInbox({
      wait: Duration.millis(50),
    });

    expect(page.items).toEqual([]);
  }));

it("rolls back policy decisions when publication fails and recovers both on restart", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const scope = yield* Scope.make();
    const policy = firewallOptions({
      decide: (request) => Effect.succeed(decisionResponse(request)),
    });
    const runtime = yield* subject
      .open({
        policy,
        decorateFirewall: (firewall) => ({
          ...firewall,
          checkIncoming: (value, evidence) =>
            firewall.checkIncoming(value, evidence).pipe(
              Effect.map((decision) => ({
                ...decision,
                save: decision.save.pipe(
                  Effect.zipRight(
                    Effect.fail(
                      new FirewallPolicyError({ reason: "persistence-failed" }),
                    ),
                  ),
                ),
              })),
            ),
        }),
      })
      .pipe(Scope.extend(scope));
    yield* receive(subject, "Publish atomically");
    expect((yield* runtime.failed.pipe(Effect.either))._tag).toBe("Left");
    expect((yield* runtime.firewall.readDecisions({})).decisions).toEqual([]);
    expect((yield* runtime.harness.listInbox()).items).toEqual([]);
    yield* Scope.close(scope, Exit.void);
    const reopened = yield* subject.open({ policy });
    expect((yield* waitForInbox(reopened)).items).toHaveLength(1);
    expect((yield* reopened.firewall.readDecisions({})).decisions).toHaveLength(
      1,
    );
  }));

it("assesses the latest bounded preceding history without the candidate", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    for (const text of ["Oldest", "Older", "Recent", "Newest"]) {
      yield* subject.retain(yield* ordinaryMessage(subject, text));
    }
    const seen: DecisionRequest[] = [];
    const runtime = yield* subject.open({
      maximumHistoryRecords: 2,
      policy: firewallOptions({
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

it("continues history through a fixed message and local-result boundary", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open();
    for (let index = 0; index < 51; index++) {
      yield* subject.retain(
        yield* ordinaryMessage(subject, `Message ${index}`),
      );
    }
    yield* subject.deliver(yield* unanswered(subject));
    yield* waitForInbox(runtime);
    const first = yield* runtime.harness.readHistory({
      address: bobAddress,
    });
    expect(first.records).toHaveLength(50);
    if (first.nextCursor === undefined) {
      return yield* Effect.dieMessage("Expected history cursor");
    }
    yield* subject.retain(yield* ordinaryMessage(subject, "Later message"));
    yield* subject.deliver(yield* unanswered(subject));
    yield* waitForInbox(runtime, 2);
    const remaining = yield* runtime.harness.readHistory({
      address: bobAddress,
      cursor: first.nextCursor,
    });
    expect(remaining.records.map((record) => record.kind)).toEqual([
      "sent",
      "result",
    ]);
    expect(remaining.nextCursor).toBeUndefined();
    expect(
      (yield* runtime.harness
        .listInbox({ cursor: first.nextCursor })
        .pipe(Effect.flip)).reason,
    ).toBe("invalid-cursor");
    expect(
      (yield* runtime.harness
        .readHistory({ address: carolAddress, cursor: first.nextCursor })
        .pipe(Effect.flip)).reason,
    ).toBe("invalid-cursor");
    const records: Array<ReadHistoryResult["records"][number]> = [];
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
    expect(records.filter((record) => record.kind === "result")).toHaveLength(
      2,
    );
  }));

it("lists every conversation once in address order in pages of 50, with one that starts mid-way only when it sorts after the pages already read", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open();
    const peer = fc.nat({ max: 199 }).map((index) => `peer-${index}`);
    const held = new Set<string>();
    for (let index = 0; index < 200; index += 7) {
      held.add(yield* hold(subject, `peer-${index}`, "result"));
    }
    yield* waitForInbox(runtime, held.size);
    yield* forAll(
      fc.tuple(fc.array(peer, { maxLength: 8 }), peer),
      Effect.fn(function* ([arrivals, late]) {
        for (const name of arrivals) {
          held.add(yield* hold(subject, name, "live"));
        }
        const before = Arr.sort(held, Order.string);
        const read = before.slice(0, 50);
        let page = yield* runtime.harness.listConversations();
        const pages = [page.addresses];
        held.add(yield* hold(subject, late, "live"));
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

it("uses one captured policy for peer-controlled result text and trusts the local answer", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const entered = yield* Deferred.make<undefined>();
    const release = yield* Deferred.make<undefined>();
    const assessed: string[] = [];
    let completing = false;
    const options = firewallOptions({
      decide: (request) =>
        Effect.gen(function* () {
          if (completing) {
            assessed.push(decisionState(request.state).candidate.sender);
            yield* Deferred.succeed(entered, undefined);
            yield* Deferred.await(release);
          }
          return decisionResponse(
            request,
            completing ? "violation" : "compliant",
          );
        }),
    });
    const runtime = yield* subject.open({
      policy: { ...options, maximumConcurrentChecks: 1 },
    });
    const original = yield* question(
      subject,
      subject.bob,
      [subject.self, subject.carol],
      "all_gather",
    );
    yield* subject.deliver({ kind: "request", message: original.message });
    yield* acknowledgeNext(runtime);
    completing = true;
    const own = yield* Answer.create({
      author: subject.self,
      request: original.message,
      response: { action: "accept", content: { day: "Tuesday" } },
    });
    const remote = yield* Answer.create({
      author: subject.carol,
      request: original.message,
      response: { action: "accept", content: { day: "Wednesday" } },
    });
    const before = yield* runtime.firewall.readPolicy();
    yield* subject.deliver({
      kind: "result",
      result: {
        request: original.request,
        closedAt: Date.now(),
        closedBy: "settled",
        outcomes: [
          {
            member: subject.self,
            outcome: { kind: "answered", content: { day: "Tuesday" } },
          },
          {
            member: subject.carol,
            outcome: { kind: "answered", content: { day: "Wednesday" } },
          },
        ],
      },
      sources: { request: original.message, answers: [own, remote] },
    });
    yield* Deferred.await(entered);
    yield* runtime.firewall.updatePolicy({ remove: ["privacy"] });
    yield* Deferred.succeed(release, undefined);
    const [entry] = (yield* waitForInbox(runtime)).items;
    expect(entry?.item).toMatchObject({
      kind: "collectiveResult",
      question: { kind: "withheld", reason: "rule-violation" },
      outcomes: [
        { outcome: { kind: "answered", content: { day: "Tuesday" } } },
        { outcome: { kind: "withheld", reason: "rule-violation" } },
      ],
    });
    const ordered = assessed.slice().sort(compareAscii);
    expect(ordered).toEqual(
      [subject.bob.agentId, subject.carol.agentId].sort(compareAscii),
    );
    expect(
      new Set(
        (yield* runtime.firewall.readDecisions({})).decisions.map(
          (decision) => decision.activeRevision,
        ),
      ),
    ).toEqual(new Set([before.activeRevision]));
  }));

it("can answer a presented request after acknowledgment and restart", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const scope = yield* Scope.make();
    const runtime = yield* subject.open().pipe(Scope.extend(scope));
    const original = yield* question(subject);
    yield* subject.deliver({ kind: "request", message: original.message });
    yield* acknowledgeNext(runtime);
    expect(subject.submitted).toEqual([]);
    yield* Scope.close(scope, Exit.void);
    const reopened = yield* subject.open();
    yield* reopened.harness.send({
      to: bobAddress,
      collectiveResponse: {
        operationId: original.request.operationId,
        action: "accept",
        content: { day: "Tuesday" },
      },
    });
    expect(subject.submitted).toMatchObject([
      {
        operation: {
          kind: "response",
          operationId: original.request.operationId,
        },
      },
    ]);
  }));

it("cannot answer a request withheld from the agent", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;

    const runtime = yield* subject.open({
      policy: firewallOptions(violation),
    });
    const original = yield* question(subject);
    yield* subject.deliver({ kind: "request", message: original.message });
    yield* waitForDecisions(runtime);
    expect(
      yield* runtime.harness
        .send({
          to: bobAddress,
          collectiveResponse: {
            operationId: original.request.operationId,
            action: "decline",
          },
        })
        .pipe(Effect.flip),
    ).toMatchObject({ failure: { kind: "request-none" } });
    expect(subject.submitted).toEqual([]);
  }));

it("cancels an unaccepted draft and releases assessment capacity", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    yield* subject.addConversation([subject.self, subject.bob]);
    const entered = yield* Deferred.make<undefined>();
    let first = true;
    const policy = firewallOptions({
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
      policy: firewallOptions({
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
    yield* subject.deliver(yield* unanswered(subject, subject.carol));
    yield* acknowledgeNext(runtime);
    const saved: Record<string, readonly string[]> = {
      "agent:bob": ["sent"],
      "agent:carol": ["result"],
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

it("uses the public invalid-answer reason without exposing the original answer", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const seen: DecisionRequest[] = [];
    const runtime = yield* subject.open({
      policy: firewallOptions({
        decide: (request) =>
          Effect.sync(() => {
            seen.push(request);
            return decisionResponse(request, "compliant", 0.6);
          }),
      }),
    });
    const original = yield* question(subject, subject.self, [subject.bob]);
    yield* subject.retain(original.message);
    const privateAnswer = "private invalid answer sentinel";
    const invalid = yield* Message.create({
      author: subject.bob,
      recipients: [subject.self],
      content: yield* encodeCollectiveContent({
        kind: "response",
        operationId: original.request.operationId,
        action: "accept",
        content: { day: [privateAnswer] },
      }),
    });
    yield* subject.deliver({
      kind: "result",
      result: {
        request: original.request,
        closedAt: Date.now(),
        closedBy: "settled",
        outcomes: [
          {
            member: subject.bob,
            outcome: { kind: "invalid", reason: "Expected a day" },
          },
        ],
      },
      sources: { request: original.message, answers: [invalid] },
    });
    const [entry] = (yield* waitForInbox(runtime)).items;
    expect(entry?.item).toMatchObject({
      kind: "collectiveResult",
      outcomes: [{ outcome: { kind: "invalid", reason: "Expected a day" } }],
    });
    expect(
      seen.some((request) =>
        decisionState(request.state).candidate.text.includes("Expected a day"),
      ),
    ).toBe(true);
    expect(JSON.stringify(seen)).not.toContain(privateAnswer);
    expect(JSON.stringify(entry)).not.toContain(privateAnswer);
    expect(
      JSON.stringify(
        yield* runtime.harness.readHistory({ address: bobAddress }),
      ),
    ).not.toContain(privateAnswer);
  }));

/* eslint-enable agent-code-guard/no-hardcoded-assertion-literals -- Restore defaults after service contract assertions. */
