/**
 * @file Gather and all_gather run end to end through real daemons that
 * install firewalls, beside an open member whose daemon installs none.
 *
 * Every other daemon runs `firewall-daemon.ts`, whose policy withholds an
 * incoming text carrying `[in]`, refuses an outgoing text carrying `[out]`,
 * asks for a response to an incoming text carrying `[guide]`, and leaves its
 * judgment of a text carrying `[unsure]` unsettled. Each endpoint
 * checks with its own firewall: a requester checks the question it sends and
 * the answers it receives, a member checks the question it is asked.
 */

import { FirewallRefusal } from "@moltzap/social-harness";
import { Effect, Queue } from "effect";
import { expect, it } from "vitest";
import {
  acquireParticipants,
  groupAddress,
  groupRound,
  measuredDeadlineSeconds,
  nextItem,
  type Participant,
  send,
} from "../agent-fixture.js";
import { processTrace } from "../scenario-fixture.js";
import { anyNumber, anyString, arrayContaining } from "../test-matchers.js";

const requireAll = (participants: readonly Participant[], count: 3 | 4) =>
  participants.length === count
    ? Effect.succeed(participants)
    : Effect.dieMessage(`expected ${count} participants`);

const formSchema = {
  type: "object",
  properties: {
    slot: { type: "string", enum: ["mon", "tue"] },
    note: { type: "string" },
  },
  required: ["slot"],
} as const;

const collecting = (
  requester: Participant,
  op: "gather" | "all_gather",
  members: readonly Participant[],
  options: { readonly question: string; readonly deadline: number },
) =>
  send(requester, {
    to: groupAddress([requester, ...members]),
    text: options.question,
    collective: { op, deadline: options.deadline, requestedSchema: formSchema },
  });

interface GatherScenario {
  readonly requester: Participant;
  readonly open: Participant;
  readonly strict: Participant;
  readonly members: readonly Participant[];
}

interface AllGatherScenario {
  readonly requester: Participant;
  readonly open: Participant;
  readonly strictA: Participant;
  readonly strictB: Participant;
  readonly members: readonly Participant[];
  readonly group: string;
  readonly deadline: number;
}

const acquireGatherScenario = Effect.gen(function* () {
  const [requester, open, strict] = yield* acquireParticipants(
    ["fw-gather-requester", "fw-gather-open", "fw-gather-strict"],
    ["fw-gather-requester", "fw-gather-strict"],
  ).pipe(Effect.flatMap((all) => requireAll(all, 3)));
  if (requester === undefined || open === undefined || strict === undefined) {
    return yield* Effect.dieMessage("expected three participants");
  }
  const members = [open, strict];
  return { requester, open, strict, members };
});

const acquireAllGatherScenario = Effect.gen(function* () {
  const all = yield* acquireParticipants(
    [
      "fw-all-gather-requester",
      "fw-all-gather-open",
      "fw-all-gather-strict-a",
      "fw-all-gather-strict-b",
    ],
    [
      "fw-all-gather-requester",
      "fw-all-gather-strict-a",
      "fw-all-gather-strict-b",
    ],
  ).pipe(Effect.flatMap((participants) => requireAll(participants, 4)));
  const [requester, open, strictA, strictB] = all;
  if (
    requester === undefined ||
    open === undefined ||
    strictA === undefined ||
    strictB === undefined
  ) {
    return yield* Effect.dieMessage("expected four participants");
  }
  const members = [open, strictA, strictB];
  /** Canonical conversation address shared by every participant. */
  const group = groupAddress([open, requester, strictA, strictB]);
  const deadline = yield* measuredDeadlineSeconds(
    groupRound(requester, members),
  );
  return { requester, open, strictA, strictB, members, group, deadline };
});

/**
 * Each test starts PGlite, the Registry, the Router and three or four
 * daemons under `tsx`, and one step of each waits out a deadline of several
 * measured message rounds. The limit is the only wall-clock bound these traces
 * set themselves apart from that measured deadline.
 */
const FIREWALL_TEST_TIMEOUT_MS = 300_000;

/** The deadline of an operation every asked member answers, so no deadline races the answers. */
const ANSWERED_DEADLINE_SECONDS = FIREWALL_TEST_TIMEOUT_MS / 1000;

const accept = (
  member: Participant,
  to: string,
  operationId: string,
  content: object,
) =>
  send(member, {
    to,
    collectiveResponse: { action: "accept", operationId, content },
  });

/**
 * A requester and a member with firewalls, and one open member. A refused
 * question sends nothing; a question the strict member's firewall withholds
 * never reaches it, and the requester records it as no-answer at the
 * deadline; an answer the requester's firewall withholds is marked withheld
 * in its result; and a strict requester sees every answer it admits with the
 * advice each check gave.
 */
const gatherBehavior = Effect.gen(function* () {
  const scenario = yield* acquireGatherScenario;
  yield* expectOutgoingGatherRefused(scenario.requester, scenario.members);
  yield* expectIncomingGatherWithheld(scenario);
  yield* expectGatherAnswerWithheld(scenario);
  yield* expectGuidedGatherAdvice(scenario);
  yield* expectUnsettledGatherAdvice(scenario);
}).pipe(Effect.scoped);

/**
 * A requester and two members with firewalls, and one open member. A
 * question the strict members' firewalls withhold never reaches them, yet
 * each still applies the close and receives the result with the question
 * withheld. An answer the firewalled endpoints withhold is marked withheld in
 * their results and nowhere else. Every endpoint's result has the same
 * members and the same outcome for every answer it shows.
 */
const allGatherBehavior = Effect.gen(function* () {
  const scenario = yield* acquireAllGatherScenario;
  yield* expectAllGatherQuestionWithheld(scenario);
  const operationId = yield* answerAllGatherWithPrivateContent(scenario);
  yield* expectAllGatherAnswerVisibility(scenario, operationId);
}).pipe(Effect.scoped);

it(
  "checks a gather's question at each endpoint and its answers at the requester",
  processTrace(gatherBehavior),
  FIREWALL_TEST_TIMEOUT_MS,
);

it(
  "gives every all_gather member the result while each firewall withholds what it holds",
  processTrace(allGatherBehavior),
  FIREWALL_TEST_TIMEOUT_MS,
);

/** A refused question sends nothing; the next incoming request must belong to the subsequent operation. */
function expectOutgoingGatherRefused(
  requester: Participant,
  members: readonly Participant[],
) {
  return Effect.gen(function* () {
    const refused = yield* collecting(requester, "gather", members, {
      question: "[out] Which day works?",
      deadline: 60,
    }).pipe(Effect.flip);
    expect(refused).toBeInstanceOf(FirewallRefusal);
    expect(refused).toMatchObject({ refusal: "rule-violation" });
  });
}

/** The strict member never receives the withheld request, so it has no response context and times out as no-answer. */
function expectIncomingGatherWithheld({
  requester,
  open,
  strict,
  members,
}: GatherScenario) {
  return Effect.gen(function* () {
    const deadline = yield* measuredDeadlineSeconds(
      groupRound(requester, members),
    );
    const withheld = yield* collecting(requester, "gather", members, {
      question: "[in] Which day works?",
      deadline: deadline,
    });
    expect(yield* nextItem(open)).toMatchObject({
      kind: "collectiveRequest",
      message: { operation: { operationId: withheld.id } },
    });
    yield* accept(open, requester.address, withheld.id, {
      slot: "mon",
    });
    expect(
      yield* accept(strict, requester.address, withheld.id, {
        slot: "tue",
      }).pipe(Effect.flip),
    ).toMatchObject({ failure: { kind: "request-none" } });
    const result = yield* nextItem(requester);
    expect(result, JSON.stringify(result)).toMatchObject({
      kind: "collectiveResult",
      operationId: withheld.id,
      outcomes: [
        { member: open.agent, outcome: { kind: "answered" } },
        { member: strict.agent, outcome: { kind: "no-answer" } },
      ],
    });
  });
}

function expectGatherAnswerWithheld({
  requester,
  open,
  strict,
  members,
}: GatherScenario) {
  return Effect.gen(function* () {
    const answered = yield* collecting(requester, "gather", members, {
      question: "Which day works for the review?",
      deadline: ANSWERED_DEADLINE_SECONDS,
    });
    expect(yield* nextItem(strict)).toMatchObject({
      kind: "collectiveRequest",
      message: { operation: { operationId: answered.id } },
    });
    expect(yield* nextItem(open)).toMatchObject({
      message: { operation: { operationId: answered.id } },
    });
    yield* accept(open, requester.address, answered.id, {
      slot: "mon",
      note: "[in] my calendar is private",
    });
    yield* accept(strict, requester.address, answered.id, {
      slot: "tue",
    });
    expect(yield* nextItem(requester)).toMatchObject({
      kind: "collectiveResult",
      operationId: answered.id,
      outcomes: [
        {
          member: open.agent,
          outcome: { kind: "withheld", reason: "rule-violation" },
        },
        {
          member: strict.agent,
          outcome: { kind: "answered", content: { slot: "tue" } },
        },
      ],
    });
  });
}

/** A strict requester admits an answer that asks for a reply, with the advice its check gave. */
function expectGuidedGatherAdvice({ requester, open, strict }: GatherScenario) {
  return Effect.gen(function* () {
    const advised = yield* collecting(strict, "gather", [open, requester], {
      question: "Which day works for the retro?",
      deadline: ANSWERED_DEADLINE_SECONDS,
    });
    expect(yield* nextItem(open)).toMatchObject({
      message: { operation: { operationId: advised.id } },
    });
    expect(yield* nextItem(requester)).toMatchObject({
      message: { operation: { operationId: advised.id } },
    });
    yield* accept(open, strict.address, advised.id, { slot: "tue" });
    yield* accept(requester, strict.address, advised.id, {
      slot: "mon",
      note: "[guide] reply to me",
    });
    const result = yield* Queue.take(strict.inbox);

    expect(result.item).toMatchObject({
      kind: "collectiveResult",
      operationId: advised.id,
      outcomes: [
        { member: open.agent, outcome: { kind: "answered" } },
        {
          member: requester.agent,
          outcome: {
            kind: "answered",
            content: { note: "[guide] reply to me" },
          },
        },
      ],
    });
    const guidance = result.guidance;
    if (guidance === undefined || !("answers" in guidance)) {
      return yield* Effect.dieMessage(
        "expected guidance for collective answers",
      );
    }
    const advice = guidance.answers.find(
      ({ member }) => member.agentId === requester.agent.agentId,
    );
    expect(advice).toMatchObject({
      member: requester.agent,
      instructions: [{ instruction: "Respond to the marked message." }],
    });
  });
}

const uncertainAdvice = {
  ruleId: "marked",
  uncertain: { choice: "compliant", probability: 0.4 },
  instruction: "Respond to the marked message.",
};

/** An unsettled judgment supplies advice without blocking the question or its answer. */
function expectUnsettledGatherAdvice({
  requester,
  open,
  strict,
  members,
}: GatherScenario) {
  return Effect.gen(function* () {
    const unsettled = yield* collecting(requester, "gather", members, {
      question: "[unsure] Which day works for the demo?",
      deadline: ANSWERED_DEADLINE_SECONDS,
    });
    expect(unsettled.guidance?.instructions).toEqual([uncertainAdvice]);
    expect(yield* nextItem(open)).toMatchObject({
      message: { operation: { operationId: unsettled.id } },
    });
    expect(yield* nextItem(strict)).toMatchObject({
      message: { operation: { operationId: unsettled.id } },
    });
    yield* accept(open, requester.address, unsettled.id, {
      slot: "mon",
    });
    const unsure = yield* accept(strict, requester.address, unsettled.id, {
      slot: "tue",
      note: "[unsure] maybe",
    });
    expect(unsure.guidance?.instructions).toEqual([uncertainAdvice]);
    const unsureResult = yield* Queue.take(requester.inbox);

    expect(unsureResult.item).toMatchObject({
      kind: "collectiveResult",
      operationId: unsettled.id,
      outcomes: [
        { member: open.agent, outcome: { kind: "answered" } },
        {
          member: strict.agent,
          outcome: { kind: "answered", content: { note: "[unsure] maybe" } },
        },
      ],
    });
    const guidance = unsureResult.guidance;
    if (guidance === undefined || !("answers" in guidance)) {
      return yield* Effect.dieMessage(
        "expected guidance for collective answers",
      );
    }
    const advice = guidance.answers.find(
      ({ member }) => member.agentId === strict.agent.agentId,
    );
    expect(advice).toMatchObject({
      member: strict.agent,
      instructions: arrayContaining([uncertainAdvice]),
    });
  });
}

/** Withheld questions never reach enforcing members, yet those members still receive the close with the question hidden. */
function expectAllGatherQuestionWithheld(scenario: AllGatherScenario) {
  const { requester, open, members, group, deadline } = scenario;
  return Effect.gen(function* () {
    const withheld = yield* collecting(requester, "all_gather", members, {
      question: "[in] Which day works?",
      deadline: deadline,
    });
    expect(yield* nextItem(open)).toMatchObject({
      kind: "collectiveRequest",
      message: { operation: { operationId: withheld.id } },
    });
    yield* accept(open, group, withheld.id, { slot: "mon" });
    expect(
      yield* accept(scenario.strictA, group, withheld.id, {
        slot: "tue",
      }).pipe(Effect.flip),
    ).toMatchObject({ failure: { kind: "request-none" } });
    const outcomes = [
      {
        member: open.agent,
        outcome: { kind: "answered", content: { slot: "mon" } },
      },
      { member: scenario.strictA.agent, outcome: { kind: "no-answer" } },
      { member: scenario.strictB.agent, outcome: { kind: "no-answer" } },
    ];
    const shown = {
      kind: "collectiveResult",
      op: "all_gather",
      operationId: withheld.id,
      closedAt: anyNumber,
      closedBy: "deadline",
      question: "[in] Which day works?",
      outcomes,
    };
    const hidden = {
      ...shown,
      question: {
        kind: "withheld",
        checkId: anyString,
        reason: "rule-violation",
      },
    };
    const result = yield* Queue.take(requester.inbox);
    expect(result.address).toBe(group);
    expect(result.item).toEqual(shown);
    expect(yield* nextItem(open)).toEqual(shown);
    expect(yield* nextItem(scenario.strictA)).toEqual(hidden);
    expect(yield* nextItem(scenario.strictB)).toEqual(hidden);
  });
}

function answerAllGatherWithPrivateContent({
  requester,
  open,
  strictA,
  strictB,
  members,
  group,
}: AllGatherScenario) {
  return Effect.gen(function* () {
    const answered = yield* collecting(requester, "all_gather", members, {
      question: "Which day works for the review?",
      deadline: ANSWERED_DEADLINE_SECONDS,
    });
    for (const member of members) {
      expect(yield* nextItem(member)).toMatchObject({
        kind: "collectiveRequest",
        message: { operation: { operationId: answered.id } },
      });
    }
    yield* accept(open, group, answered.id, {
      slot: "mon",
      note: "[in] private",
    });
    yield* accept(strictA, group, answered.id, { slot: "tue" });
    yield* send(strictB, {
      to: group,
      collectiveResponse: {
        action: "decline",
        operationId: answered.id,
      },
    });
    return answered.id;
  });
}

function expectAllGatherAnswerVisibility(
  { requester, open, strictA, strictB, group }: AllGatherScenario,
  operationId: string,
) {
  return Effect.gen(function* () {
    const openAnswer = {
      member: open.agent,
      outcome: {
        kind: "answered",
        content: { slot: "mon", note: "[in] private" },
      },
    };
    const withheldAnswer = {
      member: open.agent,
      outcome: {
        kind: "withheld",
        checkId: anyString,
        reason: "rule-violation",
      },
    };
    const others = [
      {
        member: strictA.agent,
        outcome: { kind: "answered", content: { slot: "tue" } },
      },
      { member: strictB.agent, outcome: { kind: "declined" } },
    ];
    const result = {
      kind: "collectiveResult",
      op: "all_gather",
      operationId,
      closedAt: anyNumber,
      closedBy: "settled",
      question: "Which day works for the review?",
    };
    const delivery = yield* Queue.take(requester.inbox);
    expect(delivery.address).toBe(group);
    expect(delivery.item).toEqual({
      ...result,
      outcomes: [withheldAnswer, ...others],
    });
    expect(yield* nextItem(open)).toEqual({
      ...result,
      outcomes: [openAnswer, ...others],
    });
    expect(yield* nextItem(strictA)).toEqual({
      ...result,
      outcomes: [withheldAnswer, ...others],
    });
    expect(yield* nextItem(strictB)).toEqual({
      ...result,
      outcomes: [withheldAnswer, ...others],
    });
  });
}
