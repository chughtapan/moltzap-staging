/**
 * @file Gather and all_gather run end to end through real daemons that
 * install firewalls, in guidance and enforcement modes side by side.
 *
 * Every daemon runs `firewall-daemon.ts`, whose policy withholds an incoming
 * text carrying `[in]`, refuses an outgoing text carrying `[out]`, asks for
 * a response to an incoming text carrying `[guide]`, and leaves its
 * judgment of a text carrying `[unsure]` unsettled. Each endpoint
 * checks with its own firewall: a requester checks the question it sends and
 * the answers it receives, a member checks the question it is asked.
 */

import { FirewallSendError } from "@moltzap/social-harness";
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
} from "../participants.js";
import { processTrace } from "../process-trace.js";
import { anyNumber } from "../test-matchers.js";

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
  }).pipe(
    Effect.flatMap((result) =>
      result.kind === "collective"
        ? Effect.succeed(result)
        : Effect.dieMessage("expected a collective operation"),
    ),
  );

interface GatherScenario {
  readonly requester: Participant;
  readonly guided: Participant;
  readonly strict: Participant;
  readonly members: readonly Participant[];
}

interface AllGatherScenario {
  readonly requester: Participant;
  readonly guided: Participant;
  readonly strictA: Participant;
  readonly strictB: Participant;
  readonly members: readonly Participant[];
  readonly group: string;
  readonly deadline: number;
}

const acquireGatherScenario = Effect.gen(function* () {
  const [requester, guided, strict] = yield* acquireParticipants(
    ["fw-gather-requester", "fw-gather-guided", "fw-gather-strict"],
    {
      "fw-gather-requester": "enforcement",
      "fw-gather-guided": "guidance",
      "fw-gather-strict": "enforcement",
    },
  ).pipe(Effect.flatMap((all) => requireAll(all, 3)));
  if (requester === undefined || guided === undefined || strict === undefined) {
    return yield* Effect.dieMessage("expected three participants");
  }
  const members = [guided, strict];
  return { requester, guided, strict, members };
});

const acquireAllGatherScenario = Effect.gen(function* () {
  const all = yield* acquireParticipants(
    [
      "fw-all-gather-requester",
      "fw-all-gather-guided",
      "fw-all-gather-strict-a",
      "fw-all-gather-strict-b",
    ],
    {
      "fw-all-gather-requester": "enforcement",
      "fw-all-gather-guided": "guidance",
      "fw-all-gather-strict-a": "enforcement",
      "fw-all-gather-strict-b": "enforcement",
    },
  ).pipe(Effect.flatMap((participants) => requireAll(participants, 4)));
  const [requester, guided, strictA, strictB] = all;
  if (
    requester === undefined ||
    guided === undefined ||
    strictA === undefined ||
    strictB === undefined
  ) {
    return yield* Effect.dieMessage("expected four participants");
  }
  const members = [guided, strictA, strictB];
  /** The group's canonical spelling, its names in ASCII order, as results carry it. */
  const group = groupAddress([guided, requester, strictA, strictB]);
  const deadline = yield* measuredDeadlineSeconds(
    groupRound(requester, members),
  );
  return { requester, guided, strictA, strictB, members, group, deadline };
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
 * A requester and a member in enforcement mode, and one member in guidance
 * mode. A refused question sends nothing; a question the strict member's
 * firewall withholds never reaches it, and the requester records it as
 * no-answer at the deadline; an answer the requester's firewall withholds is
 * marked withheld in its result; and a guidance-mode requester sees every
 * answer with the advice each check gave.
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
 * A requester and two members in enforcement mode, and one in guidance
 * mode. A question the strict members' firewalls withhold never reaches
 * them, yet each still applies the close and receives the result with the
 * question withheld. An answer the enforcing endpoints withhold is marked
 * withheld in their results and nowhere else. Every endpoint's result has
 * the same members and the same outcome for every answer it shows.
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
    expect(refused).toBeInstanceOf(FirewallSendError);
    expect(refused).toMatchObject({ refusal: "rule-violation" });
  });
}

/** The strict member never receives the withheld request, so it has no response context and times out as no-answer. */
function expectIncomingGatherWithheld({
  requester,
  guided,
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
    expect(yield* nextItem(guided)).toMatchObject({
      kind: "collectiveRequest",
      message: { content: { operationId: withheld.operationId } },
    });
    yield* accept(guided, requester.address, withheld.operationId, {
      slot: "mon",
    });
    expect(
      yield* accept(strict, requester.address, withheld.operationId, {
        slot: "tue",
      }).pipe(Effect.flip),
    ).toMatchObject({ failure: { kind: "request-none" } });
    expect(yield* nextItem(requester)).toMatchObject({
      kind: "collectiveResult",
      operationId: withheld.operationId,
      outcomes: [
        { member: guided.agent, outcome: { kind: "answered" } },
        { member: strict.agent, outcome: { kind: "no-answer" } },
      ],
    });
  });
}

function expectGatherAnswerWithheld({
  requester,
  guided,
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
      message: { content: { operationId: answered.operationId } },
    });
    expect(yield* nextItem(guided)).toMatchObject({
      message: { content: { operationId: answered.operationId } },
    });
    yield* accept(guided, requester.address, answered.operationId, {
      slot: "mon",
      note: "[in] my calendar is private",
    });
    yield* accept(strict, requester.address, answered.operationId, {
      slot: "tue",
    });
    expect(yield* nextItem(requester)).toMatchObject({
      kind: "collectiveResult",
      operationId: answered.operationId,
      outcomes: [
        {
          member: guided.agent,
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

function expectGuidedGatherAdvice({
  requester,
  guided,
  strict,
}: GatherScenario) {
  return Effect.gen(function* () {
    const advised = yield* collecting(guided, "gather", [requester, strict], {
      question: "Which day works for the retro?",
      deadline: ANSWERED_DEADLINE_SECONDS,
    });
    expect(yield* nextItem(requester)).toMatchObject({
      message: { content: { operationId: advised.operationId } },
    });
    expect(yield* nextItem(strict)).toMatchObject({
      message: { content: { operationId: advised.operationId } },
    });
    yield* accept(requester, guided.address, advised.operationId, {
      slot: "mon",
      note: "[guide] reply to me",
    });
    yield* accept(strict, guided.address, advised.operationId, { slot: "tue" });
    const result = yield* Queue.take(guided.inbox);
    yield* result.acknowledge;
    expect(result.item).toMatchObject({
      kind: "collectiveResult",
      operationId: advised.operationId,
      outcomes: [
        {
          member: requester.agent,
          outcome: {
            kind: "answered",
            content: { note: "[guide] reply to me" },
          },
        },
        { member: strict.agent, outcome: { kind: "answered" } },
      ],
    });
    expect(result.guidance).toMatchObject({
      answers: [
        {
          member: requester.agent,
          instructions: [{ instruction: "Respond to the marked message." }],
        },
      ],
    });
  });
}

const uncertainAdvice = {
  middlewareId: "integration-markers",
  ruleId: "marked",
  uncertain: { choice: "none", probability: 0.33 },
  instruction: "Respond to the marked message.",
};

/** An unsettled judgment supplies advice without blocking the question or its answer. */
function expectUnsettledGatherAdvice({
  requester,
  guided,
  strict,
  members,
}: GatherScenario) {
  return Effect.gen(function* () {
    const unsettled = yield* collecting(requester, "gather", members, {
      question: "[unsure] Which day works for the demo?",
      deadline: ANSWERED_DEADLINE_SECONDS,
    });
    expect(unsettled.guidance?.instructions).toEqual([uncertainAdvice]);
    expect(yield* nextItem(guided)).toMatchObject({
      message: { content: { operationId: unsettled.operationId } },
    });
    expect(yield* nextItem(strict)).toMatchObject({
      message: { content: { operationId: unsettled.operationId } },
    });
    yield* accept(guided, requester.address, unsettled.operationId, {
      slot: "mon",
    });
    const unsure = yield* accept(
      strict,
      requester.address,
      unsettled.operationId,
      {
        slot: "tue",
        note: "[unsure] maybe",
      },
    );
    expect(unsure.guidance?.instructions).toEqual([uncertainAdvice]);
    const unsureResult = yield* Queue.take(requester.inbox);
    yield* unsureResult.acknowledge;
    expect(unsureResult.item).toMatchObject({
      kind: "collectiveResult",
      operationId: unsettled.operationId,
      outcomes: [
        { member: guided.agent, outcome: { kind: "answered" } },
        {
          member: strict.agent,
          outcome: { kind: "answered", content: { note: "[unsure] maybe" } },
        },
      ],
    });
    expect(unsureResult.guidance).toMatchObject({
      answers: [{ member: strict.agent, instructions: [uncertainAdvice] }],
    });
  });
}

/** Withheld questions never reach enforcing members, yet those members still receive the close with the question hidden. */
function expectAllGatherQuestionWithheld(scenario: AllGatherScenario) {
  const { requester, guided, members, group, deadline } = scenario;
  return Effect.gen(function* () {
    const withheld = yield* collecting(requester, "all_gather", members, {
      question: "[in] Which day works?",
      deadline: deadline,
    });
    expect(yield* nextItem(guided)).toMatchObject({
      kind: "collectiveRequest",
      message: { content: { operationId: withheld.operationId } },
    });
    yield* accept(guided, group, withheld.operationId, { slot: "mon" });
    expect(
      yield* accept(scenario.strictA, group, withheld.operationId, {
        slot: "tue",
      }).pipe(Effect.flip),
    ).toMatchObject({ failure: { kind: "request-none" } });
    const outcomes = [
      {
        member: guided.agent,
        outcome: { kind: "answered", content: { slot: "mon" } },
      },
      { member: scenario.strictA.agent, outcome: { kind: "no-answer" } },
      { member: scenario.strictB.agent, outcome: { kind: "no-answer" } },
    ];
    const shown = {
      kind: "collectiveResult",
      op: "all_gather",
      operationId: withheld.operationId,
      closedAt: anyNumber,
      closedBy: "deadline",
      to: group,
      question: "[in] Which day works?",
      outcomes,
    };
    const hidden = {
      ...shown,
      question: { kind: "withheld", reason: "rule-violation" },
    };
    expect(yield* nextItem(requester)).toEqual(shown);
    expect(yield* nextItem(guided)).toEqual(shown);
    expect(yield* nextItem(scenario.strictA)).toMatchObject(hidden);
    expect(yield* nextItem(scenario.strictB)).toMatchObject(hidden);
  });
}

function answerAllGatherWithPrivateContent({
  requester,
  guided,
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
        message: { content: { operationId: answered.operationId } },
      });
    }
    yield* accept(guided, group, answered.operationId, {
      slot: "mon",
      note: "[in] private",
    });
    yield* accept(strictA, group, answered.operationId, { slot: "tue" });
    yield* send(strictB, {
      to: group,
      collectiveResponse: {
        action: "decline",
        operationId: answered.operationId,
      },
    });
    return answered.operationId;
  });
}

function expectAllGatherAnswerVisibility(
  { requester, guided, strictA, strictB, group }: AllGatherScenario,
  operationId: string,
) {
  return Effect.gen(function* () {
    const guidedAnswer = {
      member: guided.agent,
      outcome: {
        kind: "answered",
        content: { slot: "mon", note: "[in] private" },
      },
    };
    const withheldAnswer = {
      member: guided.agent,
      outcome: { kind: "withheld", reason: "rule-violation" },
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
      to: group,
      question: "Which day works for the review?",
    };
    expect(yield* nextItem(requester)).toMatchObject({
      ...result,
      outcomes: [withheldAnswer, ...others],
    });
    expect(yield* nextItem(guided)).toEqual({
      ...result,
      outcomes: [guidedAnswer, ...others],
    });
    expect(yield* nextItem(strictA)).toMatchObject({
      ...result,
      outcomes: [withheldAnswer, ...others],
    });
    expect(yield* nextItem(strictB)).toMatchObject({
      ...result,
      outcomes: [withheldAnswer, ...others],
    });
  });
}
