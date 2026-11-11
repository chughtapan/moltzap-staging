/** @file An agent's edits to the rules of its own firewall, through a running daemon and its MCP tools. */

import type {
  DecisionProvider,
  DecisionRequest,
} from "@social-harness/social-harness/service";
import { callTool, toolDefinitions } from "@social-harness/social-harness";
import { Deferred, Effect, Fiber, Schedule, Schema } from "effect";
import { describe, expect, it } from "vitest";
import { send } from "../agent-fixture.js";
import {
  type Daemon,
  sharedDaemon,
  type SignedIn,
} from "../daemon/daemon-fixture.js";
import { processTrace } from "../scenario-fixture.js";
import { arrayContaining, objectContaining } from "../test-matchers.js";
import {
  launchDaemon,
  markedPolicy,
  writeMessagePolicy,
} from "./policy-fixture.js";

const provider: DecisionProvider = {
  decide: (request) => Effect.succeed(answer(request)),
};

const read = (agent: SignedIn) =>
  callTool(agent.connection.client, { name: "read_policy", arguments: {} });

/** Sends an edit as written, including one the tool's input type does not admit. */
const update = (agent: SignedIn, edit: Readonly<Record<string, unknown>>) =>
  Effect.tryPromise(() =>
    agent.connection.client.callTool({
      name: "update_policy",
      arguments: { ...edit },
    }),
  ).pipe(
    Effect.filterOrFail((result) => result.isError !== true),
    Effect.flatMap((result) =>
      Schema.decodeUnknown(toolDefinitions.update_policy.output)(
        result.structuredContent,
      ),
    ),
  );

const secretText = "private candidate";

const sendSecret = (from: SignedIn, to: SignedIn) =>
  send(from, { to: to.address, text: secretText });

const blockedRule = {
  id: "private",
  applies: "outgoing",
  text: "Never disclose private text.",
} as const;

const fixedRule = {
  id: "owner",
  applies: "both",
  text: "Follow the owner.",
  guidance: "Ask the owner.",
} as const;

const swapDuringCheck = Effect.gen(function* () {
  const daemon = yield* sharedDaemon();
  const entered = yield* Deferred.make<undefined>();
  const release = yield* Deferred.make<undefined>();
  yield* start(daemon, markedPolicy({ fixedRules: [fixedRule] }), {
    decide: (request) =>
      Deferred.succeed(entered, undefined).pipe(
        Effect.zipRight(Deferred.await(release)),
        Effect.as(answer(request)),
      ),
  });
  const { alice, bob } = yield* signIn(daemon);
  const before = yield* read(alice);
  const inFlight = yield* sendSecret(alice, bob).pipe(Effect.forkScoped);
  yield* Deferred.await(entered);
  const changed = yield* update(alice, { add: [blockedRule] });
  expect(changed.activeRevision).not.toBe(before.activeRevision);
  yield* Deferred.succeed(release, undefined);
  yield* Fiber.join(inFlight);
  expect(yield* sendSecret(alice, bob).pipe(Effect.isFailure)).toBe(true);
});

const persistAndValidate = Effect.gen(function* () {
  const daemon = yield* sharedDaemon();
  const fixedPolicy = markedPolicy({ fixedRules: [fixedRule] });
  const runtime = yield* start(daemon, fixedPolicy);
  const { alice, bob } = yield* signIn(daemon);
  const changed = yield* update(alice, { add: [blockedRule] });
  expect(
    yield* update(alice, { remove: [fixedRule.id] }).pipe(Effect.isFailure),
  ).toBe(true);
  expect(
    yield* update(alice, { add: [{ ...blockedRule, id: fixedRule.id }] }).pipe(
      Effect.isFailure,
    ),
  ).toBe(true);
  expect(
    yield* update(alice, {
      add: [{ id: "invalid", applies: "sideways", text: "Follow the owner." }],
    }).pipe(Effect.isFailure),
  ).toBe(true);
  expect(yield* read(alice)).toMatchObject({
    activeRevision: changed.activeRevision,
    fixedRules: arrayContaining([fixedRule]),
    rules: [{ id: blockedRule.id }],
  });
  expect(yield* sendSecret(alice, bob).pipe(Effect.isFailure)).toBe(true);
  yield* Fiber.interrupt(runtime);
  yield* start(daemon, fixedPolicy);
  const fresh = (yield* signIn(daemon)).alice;
  expect(yield* read(fresh)).toMatchObject({
    activeRevision: changed.activeRevision,
    rules: [{ id: blockedRule.id }],
  });
  expect(yield* sendSecret(fresh, bob).pipe(Effect.isFailure)).toBe(true);
  yield* update(fresh, { remove: [blockedRule.id] });
  yield* sendSecret(fresh, bob);
});

const concurrentEdits = Effect.gen(function* () {
  const daemon = yield* sharedDaemon();
  const runtime = yield* start(daemon, markedPolicy({}));
  const { alice, bob } = yield* signIn(daemon);
  const additions = [
    fixedRule,
    { ...fixedRule, id: "second" },
    { ...fixedRule, id: "third" },
  ];
  yield* Effect.forEach(additions, (rule) => update(alice, { add: [rule] }), {
    concurrency: 4,
  });
  const before = yield* read(alice);
  expect(before.rules.map((rule) => rule.id)).toEqual(
    arrayContaining(additions.map((rule) => rule.id)),
  );
  yield* sendSecret(alice, bob);
  yield* Fiber.interrupt(runtime);
  yield* start(daemon, markedPolicy({}));
  expect(yield* read((yield* signIn(daemon)).alice)).toEqual(before);
});

const ownPolicyEdits = Effect.gen(function* () {
  const daemon = yield* sharedDaemon();
  yield* start(daemon, markedPolicy({}));
  const { alice, bob } = yield* signIn(daemon);
  const catalog = yield* Effect.tryPromise(() =>
    alice.connection.client.listTools(),
  );
  expect(catalog.tools).toEqual(
    arrayContaining([
      objectContaining({ name: "read_policy" }),
      objectContaining({ name: "update_policy" }),
    ]),
  );
  yield* update(alice, { add: [blockedRule] });
  const adopted = {
    id: "shared-review",
    applies: "incoming",
    text: "Reject peer scheduling requests without prior confirmation.",
    feedback: { text: "Confirm first." },
  };
  yield* update(alice, { add: [adopted] });
  expect((yield* read(alice)).rules).toEqual([blockedRule, adopted]);
  expect((yield* read(bob)).rules).toEqual([]);
  yield* update(alice, { remove: [blockedRule.id, adopted.id] });
  expect((yield* read(alice)).rules).toEqual([]);
});

describe("an agent's editable rules", () => {
  it(
    "applies edits to later sends while an in-flight check keeps its policy",
    processTrace(swapDuringCheck),
    60000,
  );
  it(
    "rejects invalid and fixed edits and restores active policy after restart",
    processTrace(persistAndValidate),
    60000,
  );
  it(
    "serializes concurrent additions without losing rules across restart",
    processTrace(concurrentEdits),
    60000,
  );
  it(
    "lets an agent read and change its own rules and no other agent's",
    processTrace(ownPolicyEdits),
    60000,
  );
});

function answer(request: DecisionRequest) {
  return {
    provider: "fixture",
    model: "deterministic",
    usage: {},
    raw: secretText,
    answers: Object.fromEntries(
      Object.entries(request.questions).map(([id, question]) => {
        const selected =
          question.ruleId === blockedRule.id ? "violation" : "compliant";
        return [
          id,
          {
            choice: selected,
            probabilities: Object.fromEntries(
              Object.keys(question.criteria).map((choice) => [
                choice,
                choice === selected ? 1 : 0,
              ]),
            ),
          },
        ];
      }),
    ),
  };
}

/**
 * Writes `document` as the daemon's message policy and runs the daemon in
 * this process, judged by `decisions`, until its fiber is interrupted.
 */
function start(
  daemon: Daemon,
  document: unknown,
  decisions: DecisionProvider = provider,
) {
  return writeMessagePolicy(daemon.directory, document).pipe(
    Effect.flatMap(daemon.configure),
    Effect.zipRight(launchDaemon(daemon.environment, decisions)),
  );
}

/**
 * Signs in the agent whose rules a trace edits and the agent it sends to. The
 * first sign-in repeats until the daemon started in this process serves.
 */
function signIn(daemon: Daemon) {
  return Effect.all({
    alice: daemon
      .signInAs("alice")
      .pipe(
        Effect.retry(
          Schedule.intersect(
            Schedule.spaced("25 millis"),
            Schedule.recurs(200),
          ),
        ),
      ),
    bob: daemon.signInAs("bob"),
  });
}
