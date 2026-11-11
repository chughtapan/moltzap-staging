/** @file A requester and three members on one daemon run all_gather operations. */

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
import {
  anyNumber,
  anyString,
  arrayContaining,
  objectContaining,
} from "../test-matchers.js";
import { inNameOrder } from "./name-order.js";

/**
 * The requester and the three members every all_gather trace signs in, and
 * the address of their group: every member in name order.
 */
const acquireFour = Effect.gen(function* () {
  const [requester, first, second, third] = inNameOrder(
    yield* acquireParticipants(["alice", "bob", "carol", "dave"]),
  );
  if (requester === undefined || first === undefined) {
    return yield* Effect.dieMessage("expected four participants");
  }
  if (second === undefined || third === undefined) {
    return yield* Effect.dieMessage("expected four participants");
  }
  const group = groupAddress([requester, first, second, third]);
  return { requester, first, second, third, group };
});

const question = "Which day works for the review?";

const slotSchema = {
  type: "object",
  properties: { slot: { type: "string", enum: ["mon", "tue"] } },
  required: ["slot"],
} as const;

const allGather = (requester: Participant, to: string, deadline: number) =>
  send(requester, {
    to,
    text: question,
    collective: { op: "all_gather", deadline, requestedSchema: slotSchema },
  });

/**
 * Each trace starts one daemon and signs four owners in. The silent-member
 * trace also waits for a deadline measured from real message delivery.
 */
const ALL_GATHER_TEST_TIMEOUT_MS = 300_000;

/**
 * The deadline of an all_gather every member answers: the test's own limit,
 * so only the answers close it and no deadline races them.
 */
const ANSWERED_DEADLINE_SECONDS = ALL_GATHER_TEST_TIMEOUT_MS / 1000;

const answer = (
  member: Participant,
  group: string,
  operationId: string,
  slot: string,
) =>
  send(member, {
    to: group,
    collectiveResponse: { action: "accept", operationId, content: { slot } },
  });

interface AllGatherParticipants {
  readonly requester: Participant;
  readonly first: Participant;
  readonly second: Participant;
  readonly third: Participant;
  readonly group: string;
}

function startAllGatherAndExpectRequests({
  requester,
  first,
  second,
  third,
  group,
}: AllGatherParticipants) {
  return Effect.gen(function* () {
    const started = yield* allGather(
      requester,
      group,
      ANSWERED_DEADLINE_SECONDS,
    );
    const request = {
      kind: "collectiveRequest",
      message: {
        msgId: anyString,
        author: requester.agent,
        recipients: [first.agent, second.agent, third.agent],
        content: arrayContaining([{ type: "text", text: question }]),
        operation: {
          op: "all_gather",
          operationId: started.id,
          requestedSchema: slotSchema,
          deadlineAt: anyNumber,
        },
      },
    };
    expect(yield* nextItem(first)).toMatchObject(request);
    expect(yield* nextItem(second)).toMatchObject(request);
    expect(yield* nextItem(third)).toMatchObject(request);
    return started;
  });
}

const allAnsweredBehavior = Effect.gen(function* () {
  const participants = yield* acquireFour;
  const started = yield* startAllGatherAndExpectRequests(participants);
  yield* answerAllGatherAndExpectSharedResult(participants, started.id);
}).pipe(Effect.scoped);

/**
 * Two members answer and the third stays silent, so the all_gather closes at
 * its measured deadline. The members take the request together and the two
 * answer together.
 */
const silentMemberBehavior = Effect.gen(function* () {
  const { requester, first, second, third, group } = yield* acquireFour;
  const deadline = yield* measuredDeadlineSeconds(
    groupRound(requester, [first, second, third]),
  );

  const started = yield* allGather(requester, group, deadline);
  yield* Effect.all([nextItem(first), nextItem(second), nextItem(third)], {
    concurrency: 3,
  });
  yield* Effect.all(
    [
      answer(first, group, started.id, "mon"),
      answer(second, group, started.id, "mon"),
    ],
    { concurrency: 2 },
  );

  const requesterResult = yield* nextItem(requester);
  expect(requesterResult).toMatchObject({
    kind: "collectiveResult",
    op: "all_gather",
    operationId: started.id,
    outcomes: [
      { member: first.agent, outcome: { kind: "answered" } },
      { member: second.agent, outcome: { kind: "answered" } },
      { member: third.agent, outcome: { kind: "no-answer" } },
    ],
  });
  expect(yield* nextItem(first)).toEqual(requesterResult);
  expect(yield* nextItem(second)).toEqual(requesterResult);
  expect(yield* nextItem(third)).toEqual(requesterResult);
}).pipe(Effect.scoped);

it(
  "gives the requester and every member the same all_gather result",
  processTrace(allAnsweredBehavior),
  ALL_GATHER_TEST_TIMEOUT_MS,
);

it(
  "closes an all_gather at the deadline without the silent member",
  processTrace(silentMemberBehavior),
  ALL_GATHER_TEST_TIMEOUT_MS,
);

function answerAllGatherAndExpectSharedResult(
  { requester, first, second, third, group }: AllGatherParticipants,
  operationId: string,
) {
  return Effect.gen(function* () {
    yield* answer(first, group, operationId, "tue");
    yield* answer(second, group, operationId, "mon");
    yield* send(third, {
      to: group,
      collectiveResponse: { action: "decline", operationId },
    });
    const delivery = yield* Queue.take(requester.inbox);
    expect(delivery.address).toBe(group);
    const requesterResult = delivery.item;
    expect(requesterResult).toEqual({
      kind: "collectiveResult",
      op: "all_gather",
      operationId,
      closedAt: anyNumber,
      closedBy: "settled",
      question,
      outcomes: [
        {
          member: objectContaining(first.agent),
          outcome: { kind: "answered", content: { slot: "tue" } },
        },
        {
          member: objectContaining(second.agent),
          outcome: { kind: "answered", content: { slot: "mon" } },
        },
        {
          member: objectContaining(third.agent),
          outcome: { kind: "declined" },
        },
      ],
    });
    expect(yield* nextItem(first)).toEqual(requesterResult);
    expect(yield* nextItem(second)).toEqual(requesterResult);
    expect(yield* nextItem(third)).toEqual(requesterResult);
  });
}
