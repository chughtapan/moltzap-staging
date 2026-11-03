/** @file A requester and three members run all_gather operations through four real daemons. */

import { SendError } from "@moltzap/social-harness";
import { Effect, Queue } from "effect";
import { expect, it } from "vitest";
import {
  acquireParticipants,
  groupRound,
  measuredDeadlineSeconds,
  nextItem,
  type Participant,
  send,
} from "../agent-fixture.js";
import { processTrace } from "../scenario-fixture.js";
import { anyNumber, anyString, arrayContaining } from "../test-matchers.js";

/** The requester and the three members every all_gather trace starts. */
const acquireFour = Effect.gen(function* () {
  const [requester, first, second, third] = yield* acquireParticipants([
    "all-gather-requester",
    "all-gather-member-a",
    "all-gather-member-b",
    "all-gather-member-c",
  ]);
  if (requester === undefined || first === undefined) {
    return yield* Effect.dieMessage("expected four participants");
  }
  if (second === undefined || third === undefined) {
    return yield* Effect.dieMessage("expected four participants");
  }
  return { requester, first, second, third };
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

/** Canonical conversation address shared by every participant. */
const group =
  "group:all-gather-member-a,all-gather-member-b,all-gather-member-c,all-gather-requester";

/**
 * Each trace starts four daemons and their services. The silent-member
 * trace also waits for a deadline measured from real message delivery.
 */
const ALL_GATHER_TEST_TIMEOUT_MS = 300_000;

/**
 * The deadline of an all_gather every member answers: the test's own limit,
 * so only the answers close it and no deadline races them.
 */
const ANSWERED_DEADLINE_SECONDS = ALL_GATHER_TEST_TIMEOUT_MS / 1000;

const answer = (member: Participant, operationId: string, slot: string) =>
  send(member, {
    to: group,
    collectiveResponse: { action: "accept", operationId, content: { slot } },
  });

interface AllGatherParticipants {
  readonly requester: Participant;
  readonly first: Participant;
  readonly second: Participant;
  readonly third: Participant;
}

/** A refused operation sends nothing before the valid all_gather requests arrive. */
function expectUnknownAllGatherMemberRefused(requester: Participant) {
  return Effect.gen(function* () {
    const failure = yield* allGather(
      requester,
      "group:all-gather-member-a,all-gather-nobody,all-gather-requester",
      60,
    ).pipe(Effect.flip);
    expect(failure).toBeInstanceOf(SendError);
    expect(failure).toMatchObject({ reason: "unknown-agent" });
    expect(failure).toHaveProperty(
      "detail",
      expect.stringContaining("agent:all-gather-nobody"),
    );
  });
}

function startAllGatherAndExpectRequests({
  requester,
  first,
  second,
  third,
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
  yield* expectUnknownAllGatherMemberRefused(participants.requester);
  const started = yield* startAllGatherAndExpectRequests(participants);
  yield* answerAllGatherAndExpectSharedResult(participants, started.id);
}).pipe(Effect.scoped);

/**
 * Two members answer and the third stays silent, so the all_gather closes at
 * its measured deadline. The members take the request together and the two
 * answer together; both answers extend the one group head, so they certify
 * one after the other within the deadline's rounds.
 */
const silentMemberBehavior = Effect.gen(function* () {
  const { requester, first, second, third } = yield* acquireFour;
  const deadline = yield* measuredDeadlineSeconds(
    groupRound(requester, [first, second, third]),
  );

  const started = yield* allGather(requester, group, deadline);
  yield* Effect.all([nextItem(first), nextItem(second), nextItem(third)], {
    concurrency: 3,
  });
  yield* Effect.all(
    [answer(first, started.id, "mon"), answer(second, started.id, "mon")],
    {
      concurrency: 2,
    },
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
  { requester, first, second, third }: AllGatherParticipants,
  operationId: string,
) {
  return Effect.gen(function* () {
    yield* answer(first, operationId, "tue");
    yield* answer(second, operationId, "mon");
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
          member: first.agent,
          outcome: { kind: "answered", content: { slot: "tue" } },
        },
        {
          member: second.agent,
          outcome: { kind: "answered", content: { slot: "mon" } },
        },
        { member: third.agent, outcome: { kind: "declined" } },
      ],
    });
    expect(yield* nextItem(first)).toEqual(requesterResult);
    expect(yield* nextItem(second)).toEqual(requesterResult);
    expect(yield* nextItem(third)).toEqual(requesterResult);
  });
}
