/**
 * @file Each member of a group checks the group's messages with its own
 * firewall: two agents whose firewalls hold a rule, beside an open member
 * whose firewall holds none.
 *
 * The marked provider judges text rules, and each agent starts with the
 * marked rule among the rules it can edit; the open member removes it. Under
 * that rule a firewall withholds an incoming text carrying `[in]`, refuses an
 * outgoing text carrying `[out]`, and is unsure about a text carrying
 * `[unsure]`: it refuses such a text outgoing and allows it incoming with
 * guidance.
 */

import { Effect, Queue } from "effect";
import { expect, it } from "vitest";
import {
  groupAddress,
  joinParticipants,
  nextItem,
  send,
} from "../agent-fixture.js";
import { sharedDaemon } from "../daemon/daemon-fixture.js";
import { processTrace } from "../scenario-fixture.js";
import { callTool } from "../tool-call.js";
import {
  markedPolicy,
  markedRule,
  writeMessagePolicy,
} from "./policy-fixture.js";

/** The test starts one daemon. */
const FIREWALL_TEST_TIMEOUT_MS = 120_000;

const uncertainAdvice = {
  ruleId: markedRule.id,
  uncertain: { choice: "compliant", probability: 0.6 },
  instruction: markedRule.guidance,
};

/**
 * A strict sender's refused message reaches nobody. A message the strict
 * recipient's firewall withholds reaches only the open member. A message a
 * firewall is unsure about is refused when its own agent sends it, and
 * reaches a strict recipient with the rule's guidance when the open member
 * sends it.
 */
const groupBehavior = Effect.gen(function* () {
  const daemon = yield* sharedDaemon({ firewall: true });
  yield* daemon.configure(
    yield* writeMessagePolicy(
      daemon.directory,
      markedPolicy({ initialRules: [markedRule] }),
    ),
  );
  yield* daemon.start();
  const [open, sender, strict] = yield* joinParticipants(daemon, [
    "alice",
    "bob",
    "carol",
  ]);
  if (open === undefined || sender === undefined || strict === undefined) {
    return yield* Effect.dieMessage("expected three participants");
  }
  yield* callTool(open.connection.mcp, {
    name: "update_policy",
    arguments: { remove: [markedRule.id] },
  });
  const to = groupAddress([open, sender, strict]);

  expect(
    yield* send(sender, { to, text: "[out] Which day works?" }).pipe(
      Effect.flip,
    ),
  ).toMatchObject({ reason: "firewall-refused", refusal: "rule-violation" });
  expect(
    yield* send(sender, { to, text: "[unsure] Which day works?" }).pipe(
      Effect.flip,
    ),
  ).toMatchObject({
    reason: "firewall-refused",
    refusal: "unresolved-evidence",
    feedback: [{ text: markedRule.feedback.text }],
  });

  const withheld = yield* send(sender, { to, text: "[in] Which day works?" });
  const allowed = yield* send(sender, { to, text: "Which day works?" });
  expect(yield* nextItem(open)).toMatchObject({
    message: { msgId: withheld.id },
  });
  expect(yield* nextItem(open)).toMatchObject({
    message: { msgId: allowed.id },
  });
  expect(yield* nextItem(strict)).toMatchObject({
    message: { msgId: allowed.id },
  });

  const unsure = yield* send(open, { to, text: "[unsure] maybe Monday" });
  const guided = yield* Queue.take(strict.inbox);
  expect(guided.item).toMatchObject({ message: { msgId: unsure.id } });
  expect(guided.guidance).toMatchObject({ instructions: [uncertainAdvice] });
}).pipe(Effect.scoped);

it(
  "checks a group message at each member's own firewall",
  processTrace(groupBehavior),
  FIREWALL_TEST_TIMEOUT_MS,
);
