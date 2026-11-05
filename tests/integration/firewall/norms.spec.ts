/**
 * @file Norms through real daemons. In a direct conversation a daemon answers
 * a stranger's message it cannot read or that does not fit a norm, such as
 * plain text where it holds no `chat` norm, an act with a field its form lacks
 * or an answer to no open proposal, with `not-understood`, which the stranger
 * receives as an ordinary message. Members agree on a norm before its acts,
 * and each daemon refuses its own agent's act out of turn. An agent's acts sent
 * back to back are judged in the order sent.
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

/**
 * A norms directory holding the shipped `documents` and rule file. The copies
 * leave out the documents' English `(rule ...)` forms: only a decision
 * provider can check those, and these daemons run without one.
 */
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
        fs
          .readFileString(join(shipped, document))
          .pipe(
            Effect.flatMap((text) =>
              fs.writeFileString(
                join(directory, basename(document)),
                text.replaceAll(/^\(rule .*\)$/gmu, ""),
              ),
            ),
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
  return { student, professor };
});

const agreedNorm = Effect.gen(function* () {
  const { student, professor } = yield* officeHours;
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
  const { student, professor } = yield* officeHours;
  const cancel = "(cancel :ontology office-hours)";
  yield* send(student, { to: professor.address, text: proposal });
  yield* nextItem(professor);
  yield* send(professor, { to: student.address, text: acceptance });
  yield* nextItem(student);

  yield* send(student, { to: professor.address, text: request });
  yield* send(student, { to: professor.address, text: cancel });

  expect(yield* nextItem(professor)).toMatchObject({
    message: { content: [{ type: "text", text: request }] },
  });
  expect(yield* nextItem(professor)).toMatchObject({
    message: { content: [{ type: "text", text: cancel }] },
  });
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
