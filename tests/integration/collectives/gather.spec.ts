/** @file A requester and two recipients on one daemon run gathers. */

import { RequestError, SendError } from "@social-harness/social-harness";
import { Effect, Queue } from "effect";
import { expect, it } from "vitest";
import {
  acquireParticipants,
  groupAddress,
  groupRound,
  measuredDeadlineSeconds,
  nextItem,
  nextRequestId,
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

/** The requester and the two recipients every gather trace signs in. */
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
    collectiveRequest: { op: "gather", deadline, requestedSchema: slotSchema },
  });

/**
 * Each trace starts one daemon and signs three owners in. The silent-recipient
 * trace also waits for a deadline measured from real message delivery.
 */
const GATHER_TEST_TIMEOUT_MS = 240_000;

/**
 * The deadline of a gather every recipient responds to: the test's own limit,
 * so only the responses complete it and no deadline races them.
 */
const ANSWERED_DEADLINE_SECONDS = GATHER_TEST_TIMEOUT_MS / 1000;

interface GatherParticipants {
  readonly requester: Participant;
  readonly first: Participant;
  readonly second: Participant;
}

/** A refused gather sends nothing, so the following request is each recipient's first item. */
function expectUnknownGatherMemberRefused({
  requester,
  first,
}: GatherParticipants) {
  return Effect.gen(function* () {
    const failure = yield* send(requester, {
      to: `group:${first.agent.agentName},${nobody},${requester.agent.agentName}`,
      text: question,
      collectiveRequest: {
        op: "gather",
        deadline: 60,
        requestedSchema: slotSchema,
      },
    }).pipe(Effect.flip);
    expect(failure).toBeInstanceOf(SendError);
    expect(failure).toMatchObject({ reason: "unknown-agent" });
    expect(failure).toHaveProperty(
      "detail",
      expect.stringContaining(`agent:${nobody}`),
    );
  });
}

/**
 * Each recipient receives a private copy of the request with its own message
 * id. The copy sent to the first recipient in agent-name order has the id the
 * send returned.
 */
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
        msgId: started.id,
        author: requester.agent,
        recipients: [first.agent],
        content: arrayContaining([{ type: "text", text: question }]),
        collectiveRequest: {
          op: "gather",
          question,
          requestedSchema: slotSchema,
          deadlineAt: anyNumber,
        },
      },
    });
    expect(secondRequest).toMatchObject({
      kind: "collectiveRequest",
      message: {
        msgId: anyString,
        recipients: [second.agent],
        collectiveRequest: { op: "gather", question },
      },
    });
    if (secondRequest.kind !== "collectiveRequest") {
      return yield* Effect.dieMessage("expected a collective request");
    }
    expect(secondRequest.message.msgId).not.toBe(started.id);
    return { started, secondRequestId: secondRequest.message.msgId };
  });
}

const allAnsweredBehavior = Effect.gen(function* () {
  const participants = yield* acquireThree;
  yield* expectUnknownGatherMemberRefused(participants);
  const { started, secondRequestId } =
    yield* startGatherAndExpectRequests(participants);
  yield* expectInvalidResponseRefused(
    participants.first,
    participants.requester,
    started.id,
  );
  yield* respondToGatherAndExpectResult(participants, started, secondRequestId);
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
  const [firstRequestId, secondRequestId] = yield* Effect.all(
    [nextRequestId(first), nextRequestId(second)],
    { concurrency: 2 },
  );
  yield* send(first, {
    to: requester.address,
    collectiveResponse: {
      action: "accept",
      requestId: firstRequestId,
      content: { slot: "mon" },
    },
  });

  expect(yield* nextItem(requester)).toMatchObject({
    kind: "collectiveResult",
    op: "gather",
    requestId: started.id,
    outcomes: [
      {
        recipient: first.agent,
        outcome: { kind: "answered", content: { slot: "mon" } },
      },
      { recipient: second.agent, outcome: { kind: "no-answer" } },
    ],
  });
  const late = yield* send(second, {
    to: requester.address,
    collectiveResponse: { action: "decline", requestId: secondRequestId },
  }).pipe(Effect.flip);
  expect(late).toMatchObject({ failure: { kind: "request-expired" } });
}).pipe(Effect.scoped);

it(
  "gathers every recipient's response",
  processTrace(allAnsweredBehavior),
  GATHER_TEST_TIMEOUT_MS,
);

it(
  "reports a silent recipient as no-answer at the deadline",
  processTrace(silentMemberBehavior),
  GATHER_TEST_TIMEOUT_MS,
);

/** How many times each agent sends the other a collective request and a message at once. */
const CROSSING_ROUNDS = 8;

/**
 * Two agents on a PostgreSQL daemon each send the other a collective request
 * and a message at the same moment, repeatedly. Every send completes and every item
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
      collectiveRequest: {
        op: "gather",
        deadline: ANSWERED_DEADLINE_SECONDS,
        requestedSchema: slotSchema,
      },
    });
  const tell = (from: Participant, to: Participant) =>
    send(from, { to: to.address, text: "a message beside the request" });
  for (let round = 0; round < CROSSING_ROUNDS; round += 1) {
    yield* Effect.all(
      [ask(alice, bob), ask(bob, alice), tell(alice, bob), tell(bob, alice)],
      { concurrency: 4 },
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
    expect(kinds.filter((kind) => kind === "message")).toHaveLength(
      CROSSING_ROUNDS,
    );
  }
}).pipe(Effect.scoped);

it(
  "completes collective requests and messages two agents send each other at once on PostgreSQL",
  processTrace(crossingSendsBehavior),
  GATHER_TEST_TIMEOUT_MS,
);

function expectInvalidResponseRefused(
  first: Participant,
  requester: Participant,
  requestId: string,
) {
  return Effect.gen(function* () {
    const invalid = yield* send(first, {
      to: requester.address,
      collectiveResponse: {
        action: "accept",
        requestId,
        content: { slot: "sun" },
      },
    }).pipe(Effect.flip);
    expect(invalid).toBeInstanceOf(RequestError);
    expect(invalid).toMatchObject({
      requestId,
      failure: {
        kind: "answer-invalid",
        fields: [{ field: "slot", reason: "invalid" }],
      },
    });
  });
}

/**
 * Each recipient responds naming the message id of its own copy. The result
 * carries the id the requester's send returned.
 */
function respondToGatherAndExpectResult(
  { requester, first, second }: GatherParticipants,
  started: { readonly id: string; readonly address: string },
  secondRequestId: string,
) {
  return Effect.gen(function* () {
    yield* send(first, {
      to: requester.address,
      collectiveResponse: {
        action: "accept",
        requestId: started.id,
        content: { slot: "tue" },
      },
    });
    yield* send(second, {
      to: requester.address,
      collectiveResponse: { action: "decline", requestId: secondRequestId },
    });
    const result = yield* Queue.take(requester.inbox);
    expect(result.address).toBe(started.address);
    expect(result.item).toEqual({
      kind: "collectiveResult",
      op: "gather",
      requestId: started.id,
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
          outcome: { kind: "declined" },
        },
      ],
    });
  });
}
