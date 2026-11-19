/**
 * @file The server agent's firewall checks what it sends a client and what a
 * client sends it.
 *
 * The marked provider judges the server agent's one text rule. Under that
 * rule the firewall refuses an outgoing text carrying `[out]`, withholds an
 * incoming text carrying `[in]`, and is unsure about a text carrying
 * `[unsure]`: it refuses such a text outgoing and allows it incoming with
 * guidance.
 */

import { Effect, Queue } from "effect";
import { expect, it } from "vitest";
import { joinParticipants, nextItem, send } from "../agent-fixture.js";
import { SERVER_AGENT_OWNER, sharedDaemon } from "../daemon/daemon-fixture.js";
import { processTrace } from "../scenario-fixture.js";
import { markedRule } from "./policy-fixture.js";

/** The test starts one daemon. */
const FIREWALL_TEST_TIMEOUT_MS = 120_000;

/**
 * The server agent's refused message does not reach the client. A client's
 * message the firewall withholds does not reach the server agent, and the
 * client receives an answer in its place. A message the firewall is unsure
 * about is refused when the server agent sends it, and reaches the server
 * agent with the rule's guidance when the client sends it.
 */
const checkedExchange = Effect.gen(function* () {
  const daemon = yield* sharedDaemon({ firewall: true });
  yield* daemon.start();
  const [server, client] = yield* joinParticipants(daemon, [
    SERVER_AGENT_OWNER,
    "bob",
  ]);
  if (server === undefined || client === undefined) {
    return yield* Effect.dieMessage("expected two participants");
  }
  const to = client.address;

  expect(
    yield* send(server, { to, text: "[out] Which day works?" }).pipe(
      Effect.flip,
    ),
  ).toMatchObject({ reason: "firewall-refused", refusal: "rule-violation" });
  expect(
    yield* send(server, { to, text: "[unsure] Which day works?" }).pipe(
      Effect.flip,
    ),
  ).toMatchObject({
    reason: "firewall-refused",
    refusal: "unresolved-evidence",
    feedback: [{ text: markedRule.feedback.text }],
  });

  const withheld = yield* send(client, { text: "[in] Which day works?" });
  const allowed = yield* send(client, { text: "Which day works?" });
  expect(yield* nextItem(client)).toMatchObject({
    message: { author: server.agent, replyToMsgId: withheld.id },
  });
  expect(yield* nextItem(server)).toMatchObject({
    message: { msgId: allowed.id },
  });

  const unsure = yield* send(client, { text: "[unsure] maybe Monday" });
  const guided = yield* Queue.take(server.inbox);
  expect(guided.item).toMatchObject({ message: { msgId: unsure.id } });
  expect(guided.guidance).toMatchObject({
    instructions: [
      {
        ruleId: markedRule.id,
        uncertain: { choice: "compliant", probability: 0.6 },
        instruction: markedRule.guidance,
      },
    ],
  });
}).pipe(Effect.scoped);

it(
  "checks the server agent's messages and a client's at the server agent's firewall",
  processTrace(checkedExchange),
  FIREWALL_TEST_TIMEOUT_MS,
);
