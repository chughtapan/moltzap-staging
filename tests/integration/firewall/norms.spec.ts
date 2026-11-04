/**
 * @file Norms through real daemons. A daemon that holds no `chat` norm
 * answers a stranger's plain text with `not-understood`, which the stranger
 * receives as an ordinary message. Members agree on a norm before its acts,
 * and each daemon refuses its own agent's act out of turn. An agent's acts
 * that the Router has not ordered yet are judged in the order it sent them.
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

const builtIn = ["norm-agreement.lisp", "norm-discovery.lisp", "feedback.lisp"];

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
    builtIn,
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
  const text =
    reply.kind === "multicast"
      ? reply.message.content.map((part) =>
          part.type === "text" ? part.text : "",
        )
      : [];

  expect(reply).toMatchObject({
    kind: "multicast",
    message: { author: { agentName: bob.agent.agentName } },
  });
  expect(text.join("")).toMatch(
    /^\(not-understood :ontology feedback .*:reason "plain-text"/u,
  );
}).pipe(Effect.scoped);

const proposal =
  "(propose :ontology norm-agreement :conversation-id a1 :norm office-hours)";
const acceptance =
  "(accept-proposal :ontology norm-agreement :conversation-id a1)";
const request =
  '(request :ontology office-hours :conversation-id o1 :topic "Project scope" :slot "Tue 2pm")';

/** A student and a professor whose daemons hold `office-hours` and `chat`. */
const officeHours = Effect.gen(function* () {
  const infrastructure = yield* acquireProcessInfrastructure;
  const documents = [...builtIn, "chat.lisp", "examples/office-hours.lisp"];
  const fixtures = yield* Effect.forEach(
    ["norms-student", "norms-professor"],
    (name) =>
      normsDirectory(infrastructure.temporaryDirectory, name, documents).pipe(
        Effect.flatMap((directory) =>
          makeDaemonProcessFixture(infrastructure, name, false, {
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
  return { infrastructure, student, professor };
});

const agreedNorm = Effect.gen(function* () {
  const { student, professor } = yield* officeHours;
  expect(
    yield* callTool(professor.endpoint.client, {
      name: "read_norms",
      arguments: {},
    }),
  ).toEqual({
    norms: arrayContaining([
      objectContaining({ name: "office-hours", protocol: "fipa-request" }),
    ]),
  });
  yield* send(student, { to: professor.address, text: proposal });
  expect(yield* nextItem(professor)).toMatchObject({
    message: { content: [{ type: "text", text: proposal }] },
  });
  yield* send(professor, { to: student.address, text: acceptance });
  yield* nextItem(student);
  const outOfTurn = yield* send(student, {
    to: professor.address,
    text: "(agree :ontology office-hours :conversation-id o1)",
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

const actsBeforeOrdering = Effect.gen(function* () {
  const { infrastructure, student, professor } = yield* officeHours;
  const agree = "(agree :ontology office-hours :conversation-id o1)";
  const inform =
    '(inform :ontology office-hours :conversation-id o1 :slot "Tue 2pm")';
  yield* send(student, { to: professor.address, text: proposal });
  yield* nextItem(professor);
  yield* send(professor, { to: student.address, text: acceptance });
  yield* nextItem(student);
  yield* send(student, { to: professor.address, text: request });
  yield* nextItem(professor);

  yield* Effect.acquireUseRelease(
    infrastructure.holdRouter,
    () =>
      send(professor, { to: student.address, text: agree }).pipe(
        Effect.zipRight(send(professor, { to: student.address, text: inform })),
      ),
    (release) => Effect.sync(release),
  );

  expect(yield* nextItem(student)).toMatchObject({
    message: { content: [{ type: "text", text: agree }] },
  });
  expect(yield* nextItem(student)).toMatchObject({
    message: { content: [{ type: "text", text: inform }] },
  });
}).pipe(Effect.scoped);

it(
  "answers a stranger's plain text with not-understood where chat is not held",
  processTrace(strangerText),
  NORMS_TEST_TIMEOUT_MS,
);

it(
  "lets members follow a norm they agreed to and refuses an act out of turn",
  processTrace(agreedNorm),
  NORMS_TEST_TIMEOUT_MS,
);

it(
  "judges an agent's second act after its first while the Router has not ordered either",
  processTrace(actsBeforeOrdering),
  NORMS_TEST_TIMEOUT_MS,
);
