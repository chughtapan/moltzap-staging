/**
 * @file Norms between agents of one daemon. Members agree on a norm before
 * its acts, and each agent's firewall refuses its own agent's act out of turn.
 * An agent's acts sent back to back are judged in the order sent. The daemon
 * runs its own composition with the marked decision provider, so the norm's
 * English rule is judged on every request: `[out]` in a topic breaks it for
 * the sender's firewall and `[in]` for the receiver's.
 */

import { callTool, type PermittedItem } from "@social-harness/social-harness";
import { Effect } from "effect";
import { expect, it } from "vitest";
import {
  joinParticipants,
  nextItem,
  type Participant,
  send,
} from "../agent-fixture.js";
import { sharedDaemon } from "../daemon/daemon-fixture.js";
import { processTrace } from "../scenario-fixture.js";
import { arrayContaining, objectContaining } from "../test-matchers.js";
import { writeNorms } from "./policy-fixture.js";

/** Each test starts one daemon under `tsx`. */
const NORMS_TEST_TIMEOUT_MS = 120_000;

/** The text of a delivered message, or nothing for any other item. */
function textOf(item: PermittedItem) {
  return item.kind === "multicast"
    ? item.message.content
        .map((part) => (part.type === "text" ? part.text : ""))
        .join("")
    : "";
}

const proposal = "(propose :ontology norm-agreement :norm office-hours)";
const acceptance = "(accept-proposal :ontology norm-agreement)";
const request = '(request :ontology office-hours :topic "Project scope")';
const offer =
  '(propose :ontology office-hours :option1 "Tue 14:00" :option2 "Thu 10:00")';

/**
 * A student and a professor on one firewall daemon, each holding
 * `office-hours` and `chat`.
 */
const officeHours = Effect.gen(function* () {
  const daemon = yield* sharedDaemon({ firewall: true });
  yield* daemon.configure(
    yield* writeNorms(daemon.directory, [
      "examples/chat.lisp",
      "examples/office-hours.lisp",
    ]),
  );
  yield* daemon.start();
  const [student, professor] = yield* joinParticipants(daemon, [
    "student",
    "professor",
  ]);
  if (student === undefined || professor === undefined) {
    return yield* Effect.dieMessage("expected two participants");
  }
  return { student, professor };
});

/** The student proposes office-hours and the professor accepts. */
function agree(student: Participant, professor: Participant) {
  return Effect.gen(function* () {
    yield* send(student, { to: professor.address, text: proposal });
    yield* nextItem(professor);
    yield* send(professor, { to: student.address, text: acceptance });
    yield* nextItem(student);
  });
}

const agreedNorm = Effect.gen(function* () {
  const { student, professor } = yield* officeHours;
  expect(
    yield* callTool(professor.connection.client, {
      name: "read_norms",
      arguments: {},
    }),
  ).toEqual({
    norms: arrayContaining([objectContaining({ name: "office-hours" })]),
  });
  yield* send(student, { to: professor.address, text: proposal });
  expect(yield* nextItem(professor)).toMatchObject({
    message: { content: [{ type: "text", text: proposal }] },
  });
  yield* send(professor, { to: student.address, text: acceptance });
  yield* nextItem(student);
  const outOfTurn = yield* send(student, {
    to: professor.address,
    text: "(query-if :ontology office-hours)",
  }).pipe(Effect.flip);
  yield* send(student, { to: professor.address, text: request });

  expect(outOfTurn).toMatchObject({
    refusal: "rule-violation",
    rule: "refuse-nonconforming",
    detail: {
      reason: "out-of-turn",
      error: "acts you may send now: (request :ontology office-hours)",
    },
  });
  expect(yield* nextItem(professor)).toMatchObject({
    message: { content: [{ type: "text", text: request }] },
  });
}).pipe(Effect.scoped);

/** The student requests and then cancels, which only the request allows. */
const backToBack = Effect.gen(function* () {
  const { student, professor } = yield* officeHours;
  const cancel = "(cancel :ontology office-hours)";
  yield* agree(student, professor);

  yield* send(student, { to: professor.address, text: request });
  yield* send(student, { to: professor.address, text: cancel });

  expect(yield* nextItem(professor)).toMatchObject({
    message: { content: [{ type: "text", text: request }] },
  });
  expect(yield* nextItem(professor)).toMatchObject({
    message: { content: [{ type: "text", text: cancel }] },
  });
}).pipe(Effect.scoped);

/**
 * A chat line and a request sent back to back, then the offer, the slots the
 * student can make, the hold, the check-back and the confirmation.
 */
const booking = Effect.gen(function* () {
  const { student, professor } = yield* officeHours;
  const slots = "(accept-proposal :ontology office-hours :available (1 2))";
  const hold = "(agree :ontology office-hours :recheck-after 1)";
  const checkBack = "(query-if :ontology office-hours)";
  const confirmation = '(confirm :ontology office-hours :slot "Tue 14:00")';
  yield* agree(student, professor);

  yield* send(student, { to: professor.address, text: "Hello again." });
  yield* send(student, { to: professor.address, text: request });
  const chatLine = yield* nextItem(professor);
  const received = yield* nextItem(professor);
  yield* send(professor, { to: student.address, text: offer });
  const offered = yield* nextItem(student);
  yield* send(student, { to: professor.address, text: slots });
  const named = yield* nextItem(professor);
  yield* send(professor, { to: student.address, text: hold });
  const held = yield* nextItem(student);
  yield* send(student, { to: professor.address, text: checkBack });
  const checked = yield* nextItem(professor);
  yield* send(professor, { to: student.address, text: confirmation });
  const confirmed = yield* nextItem(student);

  expect(
    [chatLine, received, offered, named, held, checked, confirmed].map(textOf),
  ).toEqual([
    "Hello again.",
    request,
    offer,
    slots,
    hold,
    checkBack,
    confirmation,
  ]);
}).pipe(Effect.scoped);

/** The student's own firewall refuses a request whose topic breaks the rule. */
const ownBreach = Effect.gen(function* () {
  const { student, professor } = yield* officeHours;
  yield* agree(student, professor);

  const refusal = yield* send(student, {
    to: professor.address,
    text: '(request :ontology office-hours :topic "Project scope [out]")',
  }).pipe(Effect.flip);

  expect(refusal).toMatchObject({
    refusal: "rule-violation",
    feedback: arrayContaining([
      objectContaining({
        text: expect.stringContaining("office-hours norm's rule"),
      }),
    ]),
  });
}).pipe(Effect.scoped);

/** The professor's firewall withholds a breaching request, and the corrected one starts the task. */
const peerBreach = Effect.gen(function* () {
  const { student, professor } = yield* officeHours;
  yield* agree(student, professor);

  yield* send(student, {
    to: professor.address,
    text: '(request :ontology office-hours :topic "Project scope [in]")',
  });
  const answer = yield* nextItem(student);
  yield* send(student, { to: professor.address, text: request });
  const received = yield* nextItem(professor);

  expect(textOf(answer)).toMatch(
    /^\(not-understood :ontology feedback .*:reason "rule-violation" :error "office-hours-rule-1: /u,
  );
  expect(textOf(received)).toBe(request);
}).pipe(Effect.scoped);

/** A professor who stops holding office-hours fetches it from the student, then the task starts. */
const discovery = Effect.gen(function* () {
  const { student, professor } = yield* officeHours;
  const query = "(query-ref :ontology norm-discovery :norm office-hours)";
  yield* callTool(professor.connection.client, {
    name: "update_policy",
    arguments: { remove: [{ norm: "office-hours" }] },
  });
  yield* send(student, { to: professor.address, text: proposal });
  yield* nextItem(professor);
  const unknown = yield* send(professor, {
    to: student.address,
    text: acceptance,
  }).pipe(Effect.flip);

  yield* send(professor, { to: student.address, text: query });
  const fetched = yield* nextItem(professor);
  yield* send(professor, { to: student.address, text: acceptance });
  yield* nextItem(student);
  yield* send(student, { to: professor.address, text: request });
  const received = yield* nextItem(professor);

  expect(unknown).toMatchObject({ detail: { reason: "norm-unknown" } });
  expect(textOf(fetched)).toMatch(/^\(inform :ontology norm-discovery /u);
  expect(textOf(received)).toBe(request);
}).pipe(Effect.scoped);

it(
  "lets members follow a norm they agreed to and refuses an act out of turn",
  processTrace(agreedNorm),
  NORMS_TEST_TIMEOUT_MS,
);

it(
  "delivers an agent's two acts sent back to back, in the order sent",
  processTrace(backToBack),
  NORMS_TEST_TIMEOUT_MS,
);

it(
  "books a slot through the office-hours turns after a chat line and a request sent back to back",
  processTrace(booking),
  NORMS_TEST_TIMEOUT_MS,
);

it(
  "refuses the student's own request whose topic breaks the norm's rule",
  processTrace(ownBreach),
  NORMS_TEST_TIMEOUT_MS,
);

it(
  "withholds a peer's request that breaks the norm's rule and admits the corrected request",
  processTrace(peerBreach),
  NORMS_TEST_TIMEOUT_MS,
);

it(
  "lets an agent fetch a norm through discovery before accepting it",
  processTrace(discovery),
  NORMS_TEST_TIMEOUT_MS,
);
