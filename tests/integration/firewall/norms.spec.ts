/**
 * @file Norms through real daemons. In a direct conversation a daemon answers
 * a stranger's message it cannot read or that does not fit a norm, such as
 * plain text where it holds no `chat` norm, an act with a field its form lacks
 * or an answer to no open proposal, with `not-understood`, which the stranger
 * receives as an ordinary message. Members agree on a norm before its acts,
 * and each daemon refuses its own agent's act out of turn. An agent's acts sent
 * back to back are judged in the order sent. The office-hours daemons run the
 * daemon's own composition with the marked decision provider, so the norm's
 * English rule is judged on every request: `[out]` in a topic breaks it for
 * the sender's daemon and `[in]` for the receiver's.
 */

import { FileSystem } from "@effect/platform";
import { Effect } from "effect";
import { basename, join } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, it } from "vitest";
import { callTool } from "../../../plugins/shared/mcp-client.js";
import { nextItem, send, startParticipants } from "../agent-fixture.js";
import {
  acquireProcessInfrastructure,
  makeDaemonProcessFixture,
} from "../daemon/daemon-fixture.js";
import { processTrace } from "../scenario-fixture.js";
import { arrayContaining, objectContaining } from "../test-matchers.js";

const shipped = fileURLToPath(
  new URL("../../../packages/social-harness/config/norms/", import.meta.url),
);

/** Each test starts PGlite, the Registry, the Router and two daemons. */
const NORMS_TEST_TIMEOUT_MS = 120_000;

/** A norms directory holding the shipped `documents` and rule file. */
function normsDirectory(
  root: string,
  name: string,
  documents: readonly string[],
) {
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = join(root, `${name}-norms`);
    yield* fs.makeDirectory(directory, { recursive: true });
    yield* Effect.forEach(
      [...documents, "rules.json"],
      (document) =>
        fs.copyFile(
          join(shipped, document),
          join(directory, basename(document)),
        ),
      { concurrency: 1 },
    );
    return directory;
  });
}

const strangerText = Effect.gen(function* () {
  const infrastructure = yield* acquireProcessInfrastructure;
  const strict = yield* normsDirectory(
    infrastructure.temporaryDirectory,
    "norms-bob",
    [],
  );
  const [alice, bob] = yield* startParticipants([
    yield* makeDaemonProcessFixture(infrastructure, "norms-alice"),
    yield* makeDaemonProcessFixture(infrastructure, "norms-bob", false, {
      settings: { normsDirectory: strict },
    }),
  ]);
  if (alice === undefined || bob === undefined) {
    return yield* Effect.dieMessage("expected two participants");
  }

  yield* send(alice, { to: bob.address, text: "Can we meet today?" });
  const reply = yield* nextItem(alice);
  yield* send(alice, {
    to: bob.address,
    text: "(propose :ontology norm-agreement :norm office-hours :when today)",
  });
  const fieldReply = yield* nextItem(alice);
  yield* send(alice, {
    to: bob.address,
    text: "(accept-proposal :ontology norm-agreement)",
  });
  const turnReply = yield* nextItem(alice);

  expect(reply).toMatchObject({
    kind: "multicast",
    message: { author: { agentName: bob.agent.agentName } },
  });
  expect(textOf(reply)).toMatch(
    /^\(not-understood :ontology feedback .*:reason "plain-text"/u,
  );
  expect(textOf(fieldReply)).toMatch(
    /:reason "invalid-content" :error "field \\"when\\"/u,
  );
  expect(textOf(turnReply)).toMatch(/:reason "unknown-instance"/u);
}).pipe(Effect.scoped);

/** The text of a delivered message, or nothing for any other item. */
function textOf(item: Effect.Effect.Success<ReturnType<typeof nextItem>>) {
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

const officeHoursDocuments = ["chat.lisp", "examples/office-hours.lisp"];

/**
 * A student and a professor whose firewall daemons hold `office-hours` and
 * `chat`, unless `professorDocuments` gives the professor other norms.
 */
function officeHours(professorDocuments = officeHoursDocuments) {
  return Effect.gen(function* () {
    const infrastructure = yield* acquireProcessInfrastructure;
    const fixtures = yield* Effect.forEach(
      [
        { name: "norms-student", documents: officeHoursDocuments },
        { name: "norms-professor", documents: professorDocuments },
      ],
      ({ name, documents }) =>
        normsDirectory(infrastructure.temporaryDirectory, name, documents).pipe(
          Effect.flatMap((directory) =>
            makeDaemonProcessFixture(infrastructure, name, true, {
              settings: { normsDirectory: directory },
            }),
          ),
        ),
      { concurrency: 1 },
    );
    const [student, professor] = yield* startParticipants(fixtures);
    if (student === undefined || professor === undefined) {
      return yield* Effect.dieMessage("expected two participants");
    }
    return { student, professor };
  });
}

/** The student proposes office-hours and the professor accepts. */
function agree(
  student: Effect.Effect.Success<ReturnType<typeof officeHours>>["student"],
  professor: Effect.Effect.Success<ReturnType<typeof officeHours>>["professor"],
) {
  return Effect.gen(function* () {
    yield* send(student, { to: professor.address, text: proposal });
    yield* nextItem(professor);
    yield* send(professor, { to: student.address, text: acceptance });
    yield* nextItem(student);
  });
}

const agreedNorm = Effect.gen(function* () {
  const { student, professor } = yield* officeHours();
  expect(
    yield* callTool(professor.endpoint.client, {
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
    detail: { reason: "out-of-turn", allowed: ["request"] },
  });
  expect(yield* nextItem(professor)).toMatchObject({
    message: { content: [{ type: "text", text: request }] },
  });
}).pipe(Effect.scoped);

/** The student requests and then cancels, which only the request allows. */
const backToBack = Effect.gen(function* () {
  const { student, professor } = yield* officeHours();
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

/** A chat line and a request sent back to back, then the offer, the pick and the confirmation. */
const booking = Effect.gen(function* () {
  const { student, professor } = yield* officeHours();
  const pick = "(accept-proposal :ontology office-hours :option 1)";
  const confirmation = '(confirm :ontology office-hours :slot "Tue 14:00")';
  yield* agree(student, professor);

  yield* send(student, { to: professor.address, text: "Hello again." });
  yield* send(student, { to: professor.address, text: request });
  const chatLine = yield* nextItem(professor);
  const received = yield* nextItem(professor);
  yield* send(professor, { to: student.address, text: offer });
  const offered = yield* nextItem(student);
  yield* send(student, { to: professor.address, text: pick });
  const picked = yield* nextItem(professor);
  yield* send(professor, { to: student.address, text: confirmation });
  const confirmed = yield* nextItem(student);

  expect([chatLine, received, offered, picked, confirmed].map(textOf)).toEqual([
    "Hello again.",
    request,
    offer,
    pick,
    confirmation,
  ]);
}).pipe(Effect.scoped);

/** After the student declines the only offer, the professor's failure ends the task. */
const escalation = Effect.gen(function* () {
  const { student, professor } = yield* officeHours();
  const decline = "(reject-proposal :ontology office-hours)";
  const failure =
    '(failure :ontology office-hours :reason "No offered slot works.")';
  yield* agree(student, professor);
  yield* send(student, { to: professor.address, text: request });
  yield* nextItem(professor);
  yield* send(professor, { to: student.address, text: offer });
  yield* nextItem(student);
  yield* send(student, { to: professor.address, text: decline });
  yield* nextItem(professor);

  yield* send(professor, { to: student.address, text: failure });
  const ended = yield* nextItem(student);
  const afterEnd = yield* send(student, {
    to: professor.address,
    text: request,
  }).pipe(Effect.flip);

  expect(textOf(ended)).toBe(failure);
  expect(afterEnd).toMatchObject({
    refusal: "rule-violation",
    rule: "refuse-nonconforming",
    detail: { reason: "no-agreement" },
  });
}).pipe(Effect.scoped);

/** The student's own daemon refuses a request whose topic breaks the rule. */
const ownBreach = Effect.gen(function* () {
  const { student, professor } = yield* officeHours();
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

/** The professor's daemon withholds a breaching request, and the corrected one starts the task. */
const peerBreach = Effect.gen(function* () {
  const { student, professor } = yield* officeHours();
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

/** A professor whose daemon lacks office-hours fetches it from the student, then the task starts. */
const discovery = Effect.gen(function* () {
  const { student, professor } = yield* officeHours(["chat.lisp"]);
  const query = "(query-ref :ontology norm-discovery :norm office-hours)";
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
  "answers a stranger's unreadable or out-of-turn messages with not-understood",
  processTrace(strangerText),
  NORMS_TEST_TIMEOUT_MS,
);

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
  "ends the task when the professor escalates after a declined offer",
  processTrace(escalation),
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
  "lets a daemon fetch a norm through discovery before accepting it",
  processTrace(discovery),
  NORMS_TEST_TIMEOUT_MS,
);
