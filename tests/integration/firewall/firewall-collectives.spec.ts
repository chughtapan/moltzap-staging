/**
 * @file Gather and all_gather run end to end between agents of one daemon
 * whose firewalls hold a rule, beside an open recipient whose firewall holds
 * none.
 *
 * The daemon runs `firewall-daemon.ts`, and each of its agents starts with the
 * marked rule among the rules it can edit; the open recipient removes it. Under
 * that rule a firewall withholds an incoming text carrying `[in]`, refuses an
 * outgoing text carrying `[out]`, and is unsure about a text carrying
 * `[unsure]`: it refuses such a text outgoing and admits it incoming with a
 * reminder. Each agent checks with its own firewall: a requester checks the
 * collective request it sends and the responses it receives, a recipient
 * checks the request it receives.
 */

import { callTool, FirewallRefusal } from "@social-harness/social-harness";
import { Effect, Queue } from "effect";
import { expect, it } from "vitest";
import {
  groupAddress,
  groupRound,
  joinParticipants,
  measuredDeadlineSeconds,
  nextItem,
  nextRequestId,
  type Participant,
  send,
} from "../agent-fixture.js";
import { inNameOrder } from "../collectives/name-order.js";
import { sharedDaemon } from "../daemon/daemon-fixture.js";
import { processTrace } from "../scenario-fixture.js";
import { anyNumber, anyString, objectContaining } from "../test-matchers.js";
import {
  markedPolicy,
  markedRule,
  writeMessagePolicy,
} from "./policy-fixture.js";

/**
 * One agent per owner on a firewall daemon, in name order. Each starts with
 * the marked rule among the rules it can edit.
 */
const acquireStrict = (owners: readonly string[]) =>
  Effect.gen(function* () {
    const daemon = yield* sharedDaemon({ firewall: true });
    yield* daemon.configure(
      yield* writeMessagePolicy(
        daemon.directory,
        markedPolicy({ initialRules: [markedRule] }),
      ),
    );
    yield* daemon.start();
    return inNameOrder(yield* joinParticipants(daemon, owners));
  });

/** Removes the marked rule from the agent's firewall, which then holds no rule. */
const withoutRule = (member: Participant) =>
  callTool(member.connection.client, {
    name: "update_policy",
    arguments: { remove: [markedRule.id] },
  }).pipe(Effect.as(member));

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
    collectiveRequest: {
      op,
      deadline: options.deadline,
      requestedSchema: formSchema,
    },
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

/**
 * The open recipient is first in name order, so it is first among the
 * recipients of the requester's gathers and of the strict agent's. Its
 * private copy of a gather's request therefore has the id the send returned.
 */
const acquireGatherScenario = Effect.gen(function* () {
  const [open, requester, strict] = yield* acquireStrict([
    "alice",
    "bob",
    "carol",
  ]);
  if (open === undefined || requester === undefined || strict === undefined) {
    return yield* Effect.dieMessage("expected three participants");
  }
  const members = [yield* withoutRule(open), strict];
  return { requester, open, strict, members };
});

/** The requester is last in name order, and its recipients come before it in theirs. */
const acquireAllGatherScenario = Effect.gen(function* () {
  const [open, strictA, strictB, requester] = yield* acquireStrict([
    "alice",
    "bob",
    "carol",
    "dave",
  ]);
  if (open === undefined || strictA === undefined) {
    return yield* Effect.dieMessage("expected four participants");
  }
  if (strictB === undefined || requester === undefined) {
    return yield* Effect.dieMessage("expected four participants");
  }
  const members = [yield* withoutRule(open), strictA, strictB];
  /** The address every member's daemon reports: the members in name order. */
  const group = groupAddress([...members, requester]);
  const deadline = yield* measuredDeadlineSeconds(
    groupRound(requester, members),
  );
  return { requester, open, strictA, strictB, members, group, deadline };
});

/**
 * Each test starts one daemon under `tsx`, and one step of each waits out a
 * deadline of several measured message rounds. The limit is the only
 * wall-clock bound these traces set themselves apart from that measured
 * deadline.
 */
const FIREWALL_TEST_TIMEOUT_MS = 300_000;

/** The deadline of a collective request every recipient responds to, so no deadline races the responses. */
const ANSWERED_DEADLINE_SECONDS = FIREWALL_TEST_TIMEOUT_MS / 1000;

/** An accepting response to the collective request whose message id is `requestId`. */
const accept = (
  recipient: Participant,
  to: string,
  requestId: string,
  content: object,
) =>
  send(recipient, {
    to,
    collectiveResponse: { action: "accept", requestId, content },
  });

/**
 * A requester and a recipient whose firewalls hold the rule, and one open
 * recipient. A refused collective request sends nothing; a request the strict
 * recipient's firewall withholds never reaches it, and the requester records
 * it as no-answer at the deadline; a response the requester's firewall
 * withholds is marked withheld in its result; and a strict requester sees
 * every response it admits with the advice each check gave.
 */
const gatherBehavior = Effect.gen(function* () {
  const scenario = yield* acquireGatherScenario;
  yield* expectOutgoingGatherRefused(scenario.requester, scenario.members);
  yield* expectIncomingGatherWithheld(scenario);
  yield* expectGatherResponseWithheld(scenario);
  yield* expectUnsureResponseReminder(scenario);
  yield* expectUnsureRequestRefused(scenario.requester, scenario.members);
}).pipe(Effect.scoped);

/**
 * A requester and two recipients whose firewalls hold the rule, and one open
 * recipient. A request the strict recipients' firewalls withhold never
 * reaches them, yet each still applies the close and receives the result with
 * the question withheld. A response those firewalls withhold is marked
 * withheld in their agents' results and nowhere else. Every agent's result
 * has the same members and the same outcome for every response it shows.
 */
const allGatherBehavior = Effect.gen(function* () {
  const scenario = yield* acquireAllGatherScenario;
  yield* expectAllGatherRequestWithheld(scenario);
  const requestId = yield* respondToAllGatherWithPrivateContent(scenario);
  yield* expectAllGatherResponseVisibility(scenario, requestId);
}).pipe(Effect.scoped);

it(
  "checks a gather's request at each agent and its responses at the requester",
  processTrace(gatherBehavior),
  FIREWALL_TEST_TIMEOUT_MS,
);

it(
  "gives every all_gather recipient the result while each firewall withholds what it holds",
  processTrace(allGatherBehavior),
  FIREWALL_TEST_TIMEOUT_MS,
);

/** A refused collective request sends nothing; the next incoming request must be the one sent after it. */
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

/**
 * The strict recipient never receives its withheld copy of the request, so it
 * has no request to respond to and its outcome is no-answer at the deadline.
 * Its copy's message id is read from the requester's history of the
 * conversation, where the copy is the last message the requester sent.
 */
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
      message: { msgId: withheld.id },
    });
    yield* accept(open, requester.address, withheld.id, {
      slot: "mon",
    });
    const strictCopy = (yield* callTool(requester.connection.client, {
      name: "read_conversation",
      arguments: { address: strict.address },
    })).records.findLast((record) => record.kind === "sent");
    if (strictCopy?.kind !== "sent") {
      return yield* Effect.dieMessage(
        "The requester's history holds no copy sent to the strict recipient",
      );
    }
    expect(strictCopy.message.msgId).not.toBe(withheld.id);
    expect(
      yield* accept(strict, requester.address, strictCopy.message.msgId, {
        slot: "tue",
      }).pipe(Effect.flip),
    ).toMatchObject({ failure: { kind: "request-none" } });
    const result = yield* nextItem(requester);
    expect(result, JSON.stringify(result)).toMatchObject({
      kind: "collectiveResult",
      requestId: withheld.id,
      outcomes: [
        { recipient: open.agent, outcome: { kind: "answered" } },
        { recipient: strict.agent, outcome: { kind: "no-answer" } },
      ],
    });
  });
}

function expectGatherResponseWithheld({
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
    const strictRequestId = yield* nextRequestId(strict);
    expect(yield* nextItem(open)).toMatchObject({
      kind: "collectiveRequest",
      message: { msgId: answered.id },
    });
    yield* accept(open, requester.address, answered.id, {
      slot: "mon",
      note: "[in] my calendar is private",
    });
    yield* accept(strict, requester.address, strictRequestId, {
      slot: "tue",
    });
    expect(yield* nextItem(requester)).toMatchObject({
      kind: "collectiveResult",
      requestId: answered.id,
      outcomes: [
        {
          recipient: open.agent,
          outcome: { kind: "withheld", reason: "rule-violation" },
        },
        {
          recipient: strict.agent,
          outcome: { kind: "answered", content: { slot: "tue" } },
        },
      ],
    });
  });
}

const uncertainAdvice = {
  ruleId: "marked",
  uncertain: { choice: "compliant", probability: 0.6 },
  instruction: "The marked message may break the rule.",
};

/** A strict requester admits a response its firewall is unsure about, with a reminder naming the rule. */
function expectUnsureResponseReminder({
  requester,
  open,
  strict,
}: GatherScenario) {
  return Effect.gen(function* () {
    const asked = yield* collecting(strict, "gather", [open, requester], {
      question: "Which day works for the retro?",
      deadline: ANSWERED_DEADLINE_SECONDS,
    });
    expect(yield* nextItem(open)).toMatchObject({
      kind: "collectiveRequest",
      message: { msgId: asked.id },
    });
    const requesterRequestId = yield* nextRequestId(requester);
    yield* accept(open, strict.address, asked.id, {
      slot: "tue",
      note: "[unsure] maybe",
    });
    yield* accept(requester, strict.address, requesterRequestId, {
      slot: "mon",
    });
    const result = yield* Queue.take(strict.inbox);

    expect(result.item).toMatchObject({
      kind: "collectiveResult",
      requestId: asked.id,
      outcomes: [
        {
          recipient: open.agent,
          outcome: { kind: "answered", content: { note: "[unsure] maybe" } },
        },
        { recipient: requester.agent, outcome: { kind: "answered" } },
      ],
    });
    const guidance = result.guidance;
    if (guidance === undefined || !("answers" in guidance)) {
      return yield* Effect.dieMessage(
        "expected a reminder for collective responses",
      );
    }
    expect(
      guidance.answers.find(
        ({ recipient }) => recipient.agentId === open.agent.agentId,
      ),
    ).toMatchObject({ recipient: open.agent, instructions: [uncertainAdvice] });
  });
}

/** A collective request the requester's firewall is unsure about is refused, so the agent can revise it. */
function expectUnsureRequestRefused(
  requester: Participant,
  members: readonly Participant[],
) {
  return Effect.gen(function* () {
    const refused = yield* collecting(requester, "gather", members, {
      question: "[unsure] Which day works for the demo?",
      deadline: ANSWERED_DEADLINE_SECONDS,
    }).pipe(Effect.flip);
    expect(refused).toBeInstanceOf(FirewallRefusal);
    expect(refused).toMatchObject({
      refusal: "unresolved-evidence",
      feedback: [{ text: "Remove the marked text." }],
    });
  });
}

/** A withheld request never reaches the strict recipients, yet they still receive the result, with the question hidden. */
function expectAllGatherRequestWithheld(scenario: AllGatherScenario) {
  const { requester, open, members, group, deadline } = scenario;
  return Effect.gen(function* () {
    const withheld = yield* collecting(requester, "all_gather", members, {
      question: "[in] Which day works?",
      deadline: deadline,
    });
    expect(yield* nextItem(open)).toMatchObject({
      kind: "collectiveRequest",
      message: { msgId: withheld.id },
    });
    yield* accept(open, group, withheld.id, { slot: "mon" });
    expect(
      yield* accept(scenario.strictA, group, withheld.id, {
        slot: "tue",
      }).pipe(Effect.flip),
    ).toMatchObject({ failure: { kind: "request-none" } });
    const outcomes = [
      {
        recipient: objectContaining(open.agent),
        outcome: { kind: "answered", content: { slot: "mon" } },
      },
      {
        recipient: objectContaining(scenario.strictA.agent),
        outcome: { kind: "no-answer" },
      },
      {
        recipient: objectContaining(scenario.strictB.agent),
        outcome: { kind: "no-answer" },
      },
    ];
    const shown = {
      kind: "collectiveResult",
      op: "all_gather",
      requestId: withheld.id,
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

function respondToAllGatherWithPrivateContent({
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
        message: { msgId: answered.id },
      });
    }
    yield* accept(open, group, answered.id, {
      slot: "mon",
      note: "[in] private",
    });
    yield* accept(strictA, group, answered.id, { slot: "tue" });
    yield* send(strictB, {
      to: group,
      collectiveResponse: { action: "decline", requestId: answered.id },
    });
    return answered.id;
  });
}

function expectAllGatherResponseVisibility(
  { requester, open, strictA, strictB, group }: AllGatherScenario,
  requestId: string,
) {
  return Effect.gen(function* () {
    const openOutcome = {
      recipient: objectContaining(open.agent),
      outcome: {
        kind: "answered",
        content: { slot: "mon", note: "[in] private" },
      },
    };
    const withheldOutcome = {
      recipient: objectContaining(open.agent),
      outcome: {
        kind: "withheld",
        checkId: anyString,
        reason: "rule-violation",
      },
    };
    const others = [
      {
        recipient: objectContaining(strictA.agent),
        outcome: { kind: "answered", content: { slot: "tue" } },
      },
      {
        recipient: objectContaining(strictB.agent),
        outcome: { kind: "declined" },
      },
    ];
    const result = {
      kind: "collectiveResult",
      op: "all_gather",
      requestId,
      closedAt: anyNumber,
      closedBy: "settled",
      question: "Which day works for the review?",
    };
    const delivery = yield* Queue.take(requester.inbox);
    expect(delivery.address).toBe(group);
    expect(delivery.item).toEqual({
      ...result,
      outcomes: [withheldOutcome, ...others],
    });
    expect(yield* nextItem(open)).toEqual({
      ...result,
      outcomes: [openOutcome, ...others],
    });
    expect(yield* nextItem(strictA)).toEqual({
      ...result,
      outcomes: [withheldOutcome, ...others],
    });
    expect(yield* nextItem(strictB)).toEqual({
      ...result,
      outcomes: [withheldOutcome, ...others],
    });
  });
}
