/**
 * @file The norms each agent of one P2P daemon holds. Each agent starts with
 * the contracts of the norms directory and changes its own with
 * `update_policy`, and its firewall applies what it holds: an agent that
 * holds a norm refuses a direct text that is not an act of it, while an agent
 * that stopped holding it sends any text.
 */

import { Effect } from "effect";
import { expect, it } from "vitest";
import { send } from "../agent-fixture.js";
import { P2P } from "../agent-fixture.p2p.js";
import { sharedDaemon } from "../daemon/daemon-fixture.js";
import { processTrace } from "../scenario-fixture.js";
import { callTool as call } from "../tool-call.js";
import {
  exchangeDocument,
  exchangeName,
  writeNorms,
} from "./policy-fixture.js";

const policyTool = "update_policy";

const heldNorms = Effect.gen(function* () {
  const daemon = yield* sharedDaemon({ firewall: true, settings: P2P });
  yield* daemon.configure(
    yield* writeNorms(daemon.directory, [exchangeDocument]),
  );
  yield* daemon.start();
  const alice = yield* daemon.signInAs("alice");
  const bob = yield* daemon.signInAs("bob");

  yield* call(alice.connection.mcp, {
    name: policyTool,
    arguments: { remove: [{ norm: exchangeName }] },
  });
  const [alicePolicy, bobPolicy] = yield* Effect.all([
    call(alice.connection.mcp, { name: "read_policy", arguments: {} }),
    call(bob.connection.mcp, { name: "read_policy", arguments: {} }),
  ]);
  const [aliceNorms, bobNorms] = yield* Effect.all([
    call(alice.connection.mcp, { name: "read_norms", arguments: {} }),
    call(bob.connection.mcp, { name: "read_norms", arguments: {} }),
  ]);
  const refusal = yield* send(bob, {
    to: alice.address,
    text: "An ordinary message",
  }).pipe(Effect.flip);
  yield* send(alice, { to: bob.address, text: "An ordinary message" });
  yield* call(alice.connection.mcp, {
    name: policyTool,
    arguments: { add: [{ norm: exchangeDocument }] },
  });
  const restored = yield* call(alice.connection.mcp, {
    name: "read_norms",
    arguments: {},
  });

  expect(alicePolicy.norms).toEqual([]);
  expect(alicePolicy.fixedRules.map((rule) => rule.id)).toEqual(["marked"]);
  expect(bobPolicy.norms).toEqual([exchangeName]);
  expect(bobPolicy.fixedRules.map((rule) => rule.id)).toEqual([
    "marked",
    `${exchangeName}-rule-1`,
  ]);
  expect(aliceNorms.norms).toEqual([]);
  expect(bobNorms.norms).toEqual([
    { name: exchangeName, description: "", document: exchangeDocument },
  ]);
  expect(refusal).toMatchObject({
    refusal: "rule-violation",
    detail: { stage: "structural" },
  });
  expect(restored).toEqual(bobNorms);
});

it(
  "lets two agents of one daemon hold different norms, each applied by its own firewall",
  processTrace(heldNorms),
  180_000,
);
