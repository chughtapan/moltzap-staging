/** @file A requester and three recipients on one daemon run all_gathers. */

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
  arrayContaining,
  objectContaining,
} from "../test-matchers.js";
import { inNameOrder } from "./name-order.js";

/**
 * The requester and the three recipients every all_gather trace signs in, and
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
    collectiveRequest: {
      op: "all_gather",
      deadline,
      requestedSchema: slotSchema,
    },
  });

/**
 * Each trace starts one daemon and signs four owners in. The silent-recipient
 * trace also waits for a deadline measured from real message delivery.
 */
const ALL_GATHER_TEST_TIMEOUT_MS = 300_000;

/**
 * The deadline of an all_gather every recipient responds to: the test's own
 * limit, so only the responses complete it and no deadline races them.
 */
const ANSWERED_DEADLINE_SECONDS = ALL_GATHER_TEST_TIMEOUT_MS / 1000;

/** An accepting response to the group request whose message id is `requestId`. */
const accept = (
  recipient: Participant,
  group: string,
  requestId: string,
  slot: string,
) =>
  send(recipient, {
    to: group,
    collectiveResponse: { action: "accept", requestId, content: { slot } },
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
        msgId: started.id,
        author: requester.agent,
        recipients: [first.agent, second.agent, third.agent],
        content: arrayContaining([{ type: "text", text: question }]),
        collectiveRequest: {
          op: "all_gather",
          question,
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
  yield* respondToAllGatherAndExpectSharedResult(participants, started.id);
}).pipe(Effect.scoped);

/**
 * Two recipients respond and the third stays silent, so the all_gather
 * completes at its measured deadline. The recipients take the request
 * together and the two respond together.
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
      accept(first, group, started.id, "mon"),
      accept(second, group, started.id, "mon"),
    ],
    { concurrency: 2 },
  );

  const requesterResult = yield* nextItem(requester);
  expect(requesterResult).toMatchObject({
    kind: "collectiveResult",
    op: "all_gather",
    requestId: started.id,
    outcomes: [
      { recipient: first.agent, outcome: { kind: "answered" } },
      { recipient: second.agent, outcome: { kind: "answered" } },
      { recipient: third.agent, outcome: { kind: "no-answer" } },
    ],
  });
  expect(yield* nextItem(first)).toEqual(requesterResult);
  expect(yield* nextItem(second)).toEqual(requesterResult);
  expect(yield* nextItem(third)).toEqual(requesterResult);
}).pipe(Effect.scoped);

it(
  "gives the requester and every recipient the same all_gather result",
  processTrace(allAnsweredBehavior),
  ALL_GATHER_TEST_TIMEOUT_MS,
);

it(
  "completes an all_gather at the deadline without the silent recipient",
  processTrace(silentMemberBehavior),
  ALL_GATHER_TEST_TIMEOUT_MS,
);

function respondToAllGatherAndExpectSharedResult(
  { requester, first, second, third, group }: AllGatherParticipants,
  requestId: string,
) {
  return Effect.gen(function* () {
    yield* accept(first, group, requestId, "tue");
    yield* accept(second, group, requestId, "mon");
    yield* send(third, {
      to: group,
      collectiveResponse: { action: "decline", requestId },
    });
    const delivery = yield* Queue.take(requester.inbox);
    expect(delivery.address).toBe(group);
    const requesterResult = delivery.item;
    expect(requesterResult).toEqual({
      kind: "collectiveResult",
      op: "all_gather",
      requestId,
      closedAt: anyNumber,
      closedBy: "settled",
      question,
      outcomes: [
        {
          recipient: objectContaining(first.agent),
          outcome: { kind: "answered", content: { slot: "tue" } },
        },
        {
          recipient: objectContaining(second.agent),
          outcome: { kind: "answered", content: { slot: "mon" } },
        },
        {
          recipient: objectContaining(third.agent),
          outcome: { kind: "declined" },
        },
      ],
    });
    expect(yield* nextItem(first)).toEqual(requesterResult);
    expect(yield* nextItem(second)).toEqual(requesterResult);
    expect(yield* nextItem(third)).toEqual(requesterResult);
  });
}
