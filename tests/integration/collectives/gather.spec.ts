/** @file A requester and two members on one daemon run gather operations. */

import { RequestError, SendError } from "@social-harness/social-harness";
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
import { acquirePostgres } from "../daemon/postgres-fixture.js";
import { processTrace } from "../scenario-fixture.js";
import {
  anyNumber,
  anyString,
  arrayContaining,
  objectContaining,
} from "../test-matchers.js";
import { inNameOrder } from "./name-order.js";

const requireThree = (participants: readonly Participant[]) => {
  const [requester, first, second] = inNameOrder(participants);
  return requester === undefined || first === undefined || second === undefined
    ? Effect.dieMessage("expected three participants")
    : Effect.succeed({ requester, first, second });
};

/** The requester and the two members every gather trace signs in. */
const acquireThree = acquireParticipants(["alice", "bob", "carol"]).pipe(
  Effect.flatMap(requireThree),
);

const question = "Which day works for the review?";

/** A name the daemon gives no agent. */
const nobody = "gather-nobody";

const slotSchema = {
  type: "object",
  properties: { slot: { type: "string", enum: ["mon", "tue"] } },
  required: ["slot"],
} as const;

const gather = (
  requester: Participant,
  members: readonly Participant[],
  deadline: number,
) =>
  send(requester, {
    to: groupAddress([requester, ...members]),
    text: question,
    collective: { op: "gather", deadline, requestedSchema: slotSchema },
  });

/**
 * Each trace starts one daemon and signs three owners in. The silent-member
 * trace also waits for a deadline measured from real message delivery.
 */
const GATHER_TEST_TIMEOUT_MS = 240_000;

/**
 * The deadline of a gather every member answers: the test's own limit, so
 * only the answers close it and no deadline races them.
 */
const ANSWERED_DEADLINE_SECONDS = GATHER_TEST_TIMEOUT_MS / 1000;

interface GatherParticipants {
  readonly requester: Participant;
  readonly first: Participant;
  readonly second: Participant;
}

/** A refused gather sends nothing, so the following request is each member's first item. */
function expectUnknownGatherMemberRefused({
  requester,
  first,
}: GatherParticipants) {
  return Effect.gen(function* () {
    const failure = yield* send(requester, {
      to: `group:${first.agent.agentName},${nobody},${requester.agent.agentName}`,
      text: question,
      collective: { op: "gather", deadline: 60, requestedSchema: slotSchema },
    }).pipe(Effect.flip);
    expect(failure).toBeInstanceOf(SendError);
    expect(failure).toMatchObject({ reason: "unknown-agent" });
    expect(failure).toHaveProperty(
      "detail",
      expect.stringContaining(`agent:${nobody}`),
    );
  });
}

function startGatherAndExpectRequests({
  requester,
  first,
  second,
}: GatherParticipants) {
  return Effect.gen(function* () {
    const started = yield* gather(
      requester,
      [first, second],
      ANSWERED_DEADLINE_SECONDS,
    );
    const firstRequest = yield* nextItem(first);
    const secondRequest = yield* nextItem(second);
    expect(firstRequest).toMatchObject({
      kind: "collectiveRequest",
      message: {
        msgId: anyString,
        author: requester.agent,
        recipients: [first.agent],
        content: arrayContaining([{ type: "text", text: question }]),
        operation: {
          op: "gather",
          operationId: started.id,
          requestedSchema: slotSchema,
          deadlineAt: anyNumber,
        },
      },
    });
    expect(secondRequest).toMatchObject({
      message: { operation: { operationId: started.id } },
    });
    return started;
  });
}

const allAnsweredBehavior = Effect.gen(function* () {
  const participants = yield* acquireThree;
  yield* expectUnknownGatherMemberRefused(participants);
  const started = yield* startGatherAndExpectRequests(participants);
  yield* expectInvalidAnswerRefused(
    participants.first,
    participants.requester,
    started.id,
  );
  yield* answerGatherAndExpectResult(participants, started.id, started.address);
}).pipe(Effect.scoped);

/**
 * The deadline is measured on a message to the group of every participant. The
 * gather's requests go to direct conversations, which that message leaves
 * untouched.
 */
const silentMemberBehavior = Effect.gen(function* () {
  const { requester, first, second } = yield* acquireThree;
  const deadline = yield* measuredDeadlineSeconds(
    groupRound(requester, [first, second]),
  );

  const started = yield* gather(requester, [first, second], deadline);
  yield* Effect.all([nextItem(first), nextItem(second)], { concurrency: 2 });
  yield* send(first, {
    to: requester.address,
    collectiveResponse: {
      action: "accept",
      operationId: started.id,
      content: { slot: "mon" },
    },
  });

  expect(yield* nextItem(requester)).toMatchObject({
    kind: "collectiveResult",
    op: "gather",
    operationId: started.id,
    outcomes: [
      {
        member: first.agent,
        outcome: { kind: "answered", content: { slot: "mon" } },
      },
      { member: second.agent, outcome: { kind: "no-answer" } },
    ],
  });
  const late = yield* send(second, {
    to: requester.address,
    collectiveResponse: { action: "decline", operationId: started.id },
  }).pipe(Effect.flip);
  expect(late).toMatchObject({ failure: { kind: "request-expired" } });
}).pipe(Effect.scoped);

it(
  "gathers every member's answer",
  processTrace(allAnsweredBehavior),
  GATHER_TEST_TIMEOUT_MS,
);

it(
  "reports a silent member as no-answer at the deadline",
  processTrace(silentMemberBehavior),
  GATHER_TEST_TIMEOUT_MS,
);

/** How many times each agent sends the other a question and a message at once. */
const CROSSING_ROUNDS = 8;

/**
 * Two agents on a PostgreSQL daemon each send the other a question and a
 * message at the same moment, repeatedly. Every send completes and every item
 * arrives, so neither agent waited on the other.
 */
const crossingSendsBehavior = Effect.gen(function* () {
  const [alice, bob] = yield* acquireParticipants(["alice", "bob"], {
    database: yield* acquirePostgres,
  });
  if (alice === undefined || bob === undefined) {
    return yield* Effect.dieMessage("expected two participants");
  }
  const ask = (from: Participant, to: Participant) =>
    send(from, {
      to: to.address,
      text: question,
      collective: {
        op: "gather",
        deadline: ANSWERED_DEADLINE_SECONDS,
        requestedSchema: slotSchema,
      },
    });
  const tell = (from: Participant, to: Participant) =>
    send(from, { to: to.address, text: "a message beside the question" });
  for (let round = 0; round < CROSSING_ROUNDS; round += 1) {
    yield* Effect.all(
      [ask(alice, bob), ask(bob, alice), tell(alice, bob), tell(bob, alice)],
      { concurrency: "unbounded" },
    );
  }
  for (const participant of [alice, bob]) {
    const kinds = yield* Effect.forEach(
      Array.from({ length: 2 * CROSSING_ROUNDS }),
      () => nextItem(participant).pipe(Effect.map((item) => item.kind)),
      { concurrency: 1 },
    );
    expect(kinds.filter((kind) => kind === "collectiveRequest")).toHaveLength(
      CROSSING_ROUNDS,
    );
    expect(kinds.filter((kind) => kind === "multicast")).toHaveLength(
      CROSSING_ROUNDS,
    );
  }
}).pipe(Effect.scoped);

it(
  "completes questions and messages two agents send each other at once on PostgreSQL",
  processTrace(crossingSendsBehavior),
  GATHER_TEST_TIMEOUT_MS,
);

function expectInvalidAnswerRefused(
  first: Participant,
  requester: Participant,
  operationId: string,
) {
  return Effect.gen(function* () {
    const invalid = yield* send(first, {
      to: requester.address,
      collectiveResponse: {
        action: "accept",
        operationId,
        content: { slot: "sun" },
      },
    }).pipe(Effect.flip);
    expect(invalid).toBeInstanceOf(RequestError);
    expect(invalid).toMatchObject({
      operationId,
      failure: {
        kind: "answer-invalid",
        fields: [{ field: "slot", reason: "invalid" }],
      },
    });
  });
}

function answerGatherAndExpectResult(
  { requester, first, second }: GatherParticipants,
  operationId: string,
  address: string,
) {
  return Effect.gen(function* () {
    yield* send(first, {
      to: requester.address,
      collectiveResponse: {
        action: "accept",
        operationId,
        content: { slot: "tue" },
      },
    });
    yield* send(second, {
      to: requester.address,
      collectiveResponse: { action: "decline", operationId },
    });
    const result = yield* Queue.take(requester.inbox);
    expect(result.address).toBe(address);
    expect(result.item).toEqual({
      kind: "collectiveResult",
      op: "gather",
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
          outcome: { kind: "declined" },
        },
      ],
    });
  });
}
