/** @file Main-service behavior across registration, policy assessment, delivery and history recovery. */

import { scopedLive as it } from "@effect/vitest";
import {
  Deferred,
  Duration,
  Effect,
  Exit,
  Fiber,
  Logger,
  Schedule,
  Schema,
  Scope,
} from "effect";
import { expect } from "vitest";
import type { DecisionRequest } from "../firewall/check.js";
import type { AgentCard } from "../identity/agent-cards.js";
import type { InboxEntry } from "../index.js";
import type { ReadHistoryResult, SendRequest } from "./social-harness.js";
import {
  encodeCollectiveContent,
  MessageAddressInput,
} from "../communication/messages.js";
import {
  Answer,
  Message,
  ReceivedRequest,
  Request,
} from "../communication/service.js";
import { compareAscii } from "../encoding/identifier.js";
import { FirewallPolicyError } from "../firewall/rules.js";
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

function localFailure(subject: Fixture, recipient?: AgentCard) {
  return Request.create({
    author: subject.self,
    recipients: [recipient ?? subject.bob],
    op: "gather",
    question: "Which day works?",
    requestedSchema: { type: "object", properties: {} },
    deadline: 3600,
  }).pipe(
    Effect.map(
      (request) =>
        ({
          kind: "request-failed",
          request,
          error: "The request could not complete.",
        }) as const,
    ),
  );
}

// @agent-code-guard/regression-only: These timelines cover cancellation, restart, ordering and atomic publication through real SQLite.
it("requires saved registration before serving agent operations", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open({ registerOnStart: false });
    expect(yield* runtime.harness.listInbox().pipe(Effect.flip)).toMatchObject({
      reason: "not-registered",
    });
    expect(
      yield* runtime.harness
        .send({ to: bobAddress, text: "Hello" })
        .pipe(Effect.flip),
    ).toMatchObject({ reason: "not-registered" });
    expect(
      yield* runtime.harness
        .readHistory({ address: bobAddress })
        .pipe(Effect.flip),
    ).toMatchObject({ reason: "not-registered" });
    expect(
      yield* runtime.harness.listConversations().pipe(Effect.flip),
    ).toMatchObject({ reason: "not-registered" });
  }));

it("finishes registration and activates communication when the caller cancels", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const entered = yield* Deferred.make<undefined>();
    const release = yield* Deferred.make<undefined>();
    const scope = yield* Scope.make();
    const runtime = yield* subject
      .open({
        registerOnStart: false,
        beforeRegistration: Deferred.succeed(entered, undefined).pipe(
          Effect.zipRight(Deferred.await(release)),
        ),
      })
      .pipe(Scope.extend(scope));
    const caller = yield* runtime.harness
      .register(subject.request)
      .pipe(Effect.forkScoped);
    yield* Deferred.await(entered);
    expect(Exit.isInterrupted(yield* Fiber.interrupt(caller))).toBe(true);
    yield* Deferred.succeed(release, undefined);
    yield* Deferred.await(subject.activated).pipe(Effect.timeout("10 seconds"));
    expect(yield* runtime.identity.me()).toMatchObject({
      kind: "registered",
    });
    yield* runtime.harness.send({
      to: bobAddress,
      text: "Registration completed",
    });
    yield* Scope.close(scope, Exit.void);
    const reopened = yield* subject.open({
      beforeRegistration: Effect.dieMessage(
        "Saved registration must not call Registry again",
      ),
    });
    expect(yield* reopened.identity.me()).toMatchObject({
      kind: "registered",
    });
    expect((yield* reopened.harness.listInbox()).items).toEqual([]);
  }));

it("serves saved identity and inbox while communication starts", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const entered = yield* Deferred.make<undefined>();
    const release = yield* Deferred.make<undefined>();
    const runtime = yield* subject.open({
      registerOnStart: false,
      startCommunication: Deferred.succeed(entered, undefined).pipe(
        Effect.zipRight(Deferred.await(release)),
      ),
    });
    yield* runtime.harness
      .register(subject.request)
      .pipe(Effect.timeout("5 seconds"));
    yield* Deferred.await(entered);
    expect(yield* runtime.identity.me()).toMatchObject({ kind: "registered" });
    expect((yield* runtime.harness.listInbox()).items).toEqual([]);
    yield* Deferred.succeed(release, undefined);
    yield* runtime.harness.send({
      to: bobAddress,
      text: "Communication started",
    });
  }));

it("gives each host invocation a fresh message identity", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const { harness } = yield* subject.open();
    const input = { to: bobAddress, text: "Send this intentionally twice" };
    const first = yield* harness.send(input);
    const second = yield* harness.send(input);
    expect(first.id).not.toBe(second.id);
    expect(subject.submitted).toHaveLength(2);
    expect(
      subject.submitted.map((message) =>
        "kind" in message ? undefined : message.msgId,
      ),
    ).toEqual([first.id, second.id]);
  }));

it("reports an uncertain send without repeating accepted work", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open();
    yield* subject.uncertainSend();
    expect(
      yield* runtime.harness
        .send({
          to: bobAddress,
          text: "Accepted before the connection failed",
        })
        .pipe(Effect.flip),
    ).toMatchObject({ reason: "outcome-unknown" });
    expect(subject.submitted).toHaveLength(1);
    expect(
      (yield* runtime.harness.readHistory({ address: bobAddress })).records,
    ).toMatchObject([
      {
        kind: "sent",
        message: {
          content: [{ text: "Accepted before the connection failed" }],
        },
      },
    ]);
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
            if (request.stage === "compliance") {
              assessed.push(text);
              if (text === "First") {
                yield* Deferred.succeed(entered, undefined);
                yield* Deferred.await(release);
              }
            }
            return decisionResponse(request);
          }),
      }),
    });
    yield* subject.deliver({
      kind: "message",
      message: yield* subject.message("First"),
    });
    yield* Deferred.await(entered);
    yield* subject.deliver({
      kind: "message",
      message: yield* subject.message("Second"),
    });
    yield* subject.deliver({
      kind: "message",
      message: yield* subject.message("Independent", subject.carol),
    });
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
            Effect.succeed(decisionResponse(request, "compliant", "required")),
        }),
      })
      .pipe(Scope.extend(resumedScope));
    const [delivery] = (yield* waitForInbox(resumed)).items;
    if (delivery === undefined) {
      return yield* Effect.dieMessage("Expected recovered delivery");
    }
    const saved = yield* resumed.harness.getInboxEvent(delivery.deliveryToken);
    expect(saved.guidance).toBeDefined();
    yield* resumed.firewall.updatePolicy({ remove: ["privacy"] });
    yield* resumed.harness.acknowledgeDelivery(delivery.deliveryToken);
    yield* resumed.harness.acknowledgeDelivery(delivery.deliveryToken);
    expect((yield* resumed.harness.listInbox()).items).toEqual([]);
    expect(
      yield* resumed.harness.getInboxEvent(delivery.deliveryToken),
    ).toEqual(saved);
    yield* subject.disconnectRegistry();
    expect(
      (yield* resumed.harness.readHistory({ address: bobAddress })).records,
    ).toMatchObject([{ kind: "received", ...saved }]);
    yield* Scope.close(resumedScope, Exit.void);
    const reopened = yield* subject.open();
    expect(
      yield* reopened.harness.getInboxEvent(delivery.deliveryToken),
    ).toEqual(saved);
    yield* subject.deliver({ kind: "message", message });
    expect((yield* reopened.harness.listInbox()).items).toEqual([]);
  }));

it("reads result-only history without contacting Registry", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open();
    yield* subject.deliver(yield* localFailure(subject));
    const [entry] = (yield* waitForInbox(runtime)).items;
    if (entry === undefined) {
      return yield* Effect.dieMessage("Expected result delivery");
    }
    yield* runtime.harness.acknowledgeDelivery(entry.deliveryToken);
    yield* subject.disconnectRegistry();
    expect(
      (yield* runtime.harness.readHistory({ address: bobAddress })).records,
    ).toMatchObject([{ kind: "result", item: { kind: "operationFailed" } }]);
  }));

it("keeps an inbox page boundary while arrivals and acknowledgments change pending work", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open();
    for (let index = 0; index < 57; index++) {
      yield* subject.deliver(yield* localFailure(subject));
    }
    yield* readInbox(runtime).pipe(
      Effect.repeat({
        until: (items) => items.length === 57,
        schedule: Schedule.spaced("5 millis"),
      }),
      Effect.raceFirst(runtime.failed),
      Effect.timeout("10 seconds"),
    );
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
    const later = yield* localFailure(subject);
    yield* subject.deliver(later);
    yield* readInbox(runtime).pipe(
      Effect.repeat({
        until: (items) => items.length === 57,
        schedule: Schedule.spaced("5 millis"),
      }),
      Effect.raceFirst(runtime.failed),
      Effect.timeout("10 seconds"),
    );
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
      policy: firewallOptions({
        decide: (request) =>
          Effect.succeed(decisionResponse(request, "violation")),
      }),
    });
    yield* subject.deliver({
      kind: "message",
      message: yield* subject.message("Private peer content"),
    });
    expect((yield* waitForDecisions(runtime)).decisions).toMatchObject([
      { reason: "rule-violation", disposition: "withheld" },
    ]);
    expect((yield* runtime.harness.listInbox()).items).toEqual([]);
    expect(
      (yield* runtime.harness.readHistory({ address: bobAddress })).records,
    ).toEqual([]);
  }));

it("answers a message its rule withholds with that rule's feedback, as an ordinary message", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open({
      policy: firewallOptions(
        {
          decide: (request) =>
            Effect.succeed(decisionResponse(request, "violation")),
        },
        "reply",
      ),
    });

    yield* subject.deliver({
      kind: "message",
      message: yield* subject.message("Private peer content"),
    });
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
    yield* subject.deliver({
      kind: "message",
      message: yield* subject.message("Already here"),
    });
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

    yield* subject.deliver({
      kind: "message",
      message: yield* subject.message("Arrived while waiting"),
    });
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
    yield* subject.deliver({
      kind: "message",
      message: yield* subject.message("Publish atomically"),
    });
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
    yield* subject.deliver({
      kind: "message",
      message: yield* subject.message("Candidate"),
    });
    yield* waitForInbox(runtime);
    const assessment = seen.find((request) => request.stage === "compliance");
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
    yield* subject.deliver(yield* localFailure(subject));
    yield* waitForInbox(runtime);
    const first = yield* runtime.harness.readHistory({
      address: bobAddress,
    });
    expect(first.records).toHaveLength(50);
    if (first.nextCursor === undefined) {
      return yield* Effect.dieMessage("Expected history cursor");
    }
    yield* subject.retain(yield* ordinaryMessage(subject, "Later message"));
    yield* subject.deliver(yield* localFailure(subject));
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

it("merges live conversation and result catalogs into bounded ordered pages", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open();
    const expected: string[] = [];
    for (let index = 0; index < 120; index++) {
      const peer = yield* subject.addPeer(
        `peer-${String(index).padStart(3, "0")}`,
      );
      expected.push(`agent:${peer.agentName}`);
      if (index % 3 === 0) {
        yield* subject.deliver(yield* localFailure(subject, peer));
      } else {
        yield* subject.addConversation([subject.self, peer]);
      }
    }
    yield* waitForInbox(runtime, 40);
    const first = yield* runtime.harness.listConversations();
    expect(first.addresses).toEqual(expected.slice(0, 50));
    if (first.nextCursor === undefined) {
      return yield* Effect.dieMessage("Expected catalog cursor");
    }
    const later = yield* subject.addPeer("peer-080-new");
    yield* subject.addConversation([subject.self, later]);
    const seen = [...first.addresses];
    let page = yield* runtime.harness.listConversations({
      cursor: first.nextCursor,
    });
    seen.push(...page.addresses);
    while (page.nextCursor !== undefined) {
      expect(page.addresses.length).toBeLessThanOrEqual(50);
      page = yield* runtime.harness.listConversations({
        cursor: page.nextCursor,
      });
      seen.push(...page.addresses);
    }
    expect(seen).toEqual(
      [...expected, `agent:${later.agentName}`].sort(compareAscii),
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
          if (completing && request.stage === "compliance") {
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
    const [shown] = (yield* waitForInbox(runtime)).items;
    if (shown === undefined) {
      return yield* Effect.dieMessage("Expected original request");
    }
    const originalView = yield* runtime.harness.getInboxEvent(
      shown.deliveryToken,
    );
    yield* runtime.harness.acknowledgeDelivery(shown.deliveryToken);
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
    expect(yield* runtime.harness.getInboxEvent(shown.deliveryToken)).toEqual(
      originalView,
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
    const [entry] = (yield* waitForInbox(runtime)).items;
    if (entry === undefined) {
      return yield* Effect.dieMessage("Expected request delivery");
    }
    yield* runtime.harness.acknowledgeDelivery(entry.deliveryToken);
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
      policy: firewallOptions({
        decide: (request) =>
          Effect.succeed(decisionResponse(request, "violation")),
      }),
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

const hiddenSends: readonly SendRequest[] = [
  {
    to: MessageAddressInput.make("group:bob,carol,http-signer"),
    text: "Hello",
  },
  {
    to: bobAddress,
    text: "Which day?",
    collective: {
      op: "gather",
      deadline: 60,
      requestedSchema: { type: "object", properties: {} },
    },
  },
];
it.each(hiddenSends)(
  "refuses a hidden capability before dispatch: $to",
  (request) =>
    Effect.gen(function* () {
      const subject = yield* makeHarnessFixture;
      const logs: Array<{
        message: unknown;
        annotations: Record<string, unknown>;
      }> = [];
      const logger = Logger.make(({ message, annotations }) => {
        logs.push({ message, annotations: Object.fromEntries(annotations) });
      });
      const runtime = yield* subject.open({
        hideGroups: true,
        hideCollectives: true,
      });
      expect(
        yield* runtime.harness
          .send(request)
          .pipe(Effect.flip, Effect.provide(Logger.add(logger))),
      ).toMatchObject({ reason: "content-invalid" });
      expect(subject.submitted).toEqual([]);
      expect(logs).toHaveLength(1);
      expect(logs[0]?.message).toEqual(["Send failed"]);
      expect(typeof logs[0]?.annotations.agentKey).toBe("string");
      expect(logs[0]?.annotations.error).toBe("SendError");
      expect(logs[0]?.annotations.reason).toContain("content is invalid");
    }),
);

it("requires canonical history addresses and identifies unknown agents", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const { harness } = yield* subject.open();
    expect(
      yield* harness
        .readHistory({ address: MessageAddressInput.make("agent:missing") })
        .pipe(Effect.flip),
    ).toMatchObject({ reason: "unknown-agent" });
    expect(
      yield* harness
        .readHistory({
          address: MessageAddressInput.make("group:carol,bob,http-signer"),
        })
        .pipe(Effect.flip),
    ).toMatchObject({ reason: "invalid-address" });
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
            return decisionResponse(request, "compliant", "required");
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

it("sends the first message under a configured policy without inventing a history gap", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open({
      policy: firewallOptions({
        decide: (request) => Effect.succeed(decisionResponse(request)),
      }),
    });
    const receipt = yield* runtime.harness.send({
      to: bobAddress,
      text: "Our first message",
    });
    expect(receipt.id).toBeDefined();
    expect(subject.submitted).toHaveLength(1);
  }));

/* eslint-enable agent-code-guard/no-hardcoded-assertion-literals -- Restore defaults after service contract assertions. */
