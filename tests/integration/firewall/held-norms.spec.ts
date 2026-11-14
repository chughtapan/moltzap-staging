/**
 * @file The norms each agent of one daemon holds.
 *
 * In P2P mode two agents hold different norms.
 * Each agent starts with the norms directory's initial norms and changes its
 * own with `update_policy`, and its firewall applies the norms it holds: an
 * agent that stops holding `chat` answers plain text with `not-understood`
 * and refuses its own, while the other agent on the same daemon reads it.
 *
 * In client/server mode only the server agent holds norms. A client reads the
 * server agent's norms and sends without a check of its own. The server
 * agent's firewall answers a message it cannot read or that does not fit a
 * norm with `not-understood`, which the client receives as an ordinary
 * message: plain text where the server agent holds no `chat` norm, an act
 * with a field its form lacks, or an answer to no open proposal.
 */

import {
  callTool as call,
  MAXIMUM_WAIT_SECONDS,
  type PermittedItem,
} from "@social-harness/social-harness";
import { Effect } from "effect";
import { expect, it } from "vitest";
import { send } from "../agent-fixture.js";
import { sharedDaemon, type SignedIn } from "../daemon/daemon-fixture.js";
import { processTrace } from "../scenario-fixture.js";
import { writeNorms } from "./policy-fixture.js";

/** Each test starts one daemon under `tsx`. */
const HELD_NORMS_TEST_TIMEOUT_MS = 180_000;

/**
 * A running firewall daemon whose new agents hold `chat` and `office-hours`,
 * in P2P mode unless `settings` name another.
 */
function normsDaemon(settings: Readonly<Record<string, unknown>> = {}) {
  return Effect.gen(function* () {
    const daemon = yield* sharedDaemon({ firewall: true, settings });
    yield* daemon.configure(
      yield* writeNorms(daemon.directory, [
        "examples/chat.lisp",
        "examples/office-hours.lisp",
      ]),
    );
    yield* daemon.start();
    return daemon;
  });
}

/** Sends `text` as a client does, naming no recipient, and returns the server agent's answer. */
function answerTo(client: SignedIn, text: string) {
  return Effect.tryPromise(() =>
    client.connection.client.callTool({
      name: "send_message",
      arguments: { text },
    }),
  ).pipe(
    Effect.filterOrFail((sent) => sent.isError !== true),
    Effect.zipRight(firstDelivery(client)),
  );
}

/**
 * The first item in the agent's inbox. One call waits for it: the daemon
 * answers as soon as the item arrives.
 */
function firstDelivery(agent: SignedIn) {
  return call(agent.connection.client, {
    name: "receive_messages",
    arguments: { wait: MAXIMUM_WAIT_SECONDS },
  }).pipe(
    Effect.map(({ items }) => items[0]?.item),
    Effect.timeout("60 seconds"),
  );
}

/** The text of a delivered message, or nothing for any other item. */
function textOf(item?: PermittedItem) {
  return item?.kind === "message"
    ? item.message.content
        .map((part) => (part.type === "text" ? part.text : ""))
        .join("")
    : "";
}

const policyTool = "update_policy";
const droppedNorm = "chat";

/** The first English rule of the `office-hours` norm, as an agent that holds the norm reads it in its policy. */
const officeHoursRule = "office-hours-rule-1";

const heldNorms = Effect.gen(function* () {
  const daemon = yield* normsDaemon();
  const alice = yield* daemon.signInAs("alice");
  const bob = yield* daemon.signInAs("bob");

  yield* call(alice.connection.client, {
    name: policyTool,
    arguments: { remove: [{ norm: droppedNorm }] },
  });
  const [alicePolicy, bobPolicy] = yield* Effect.all([
    call(alice.connection.client, { name: "read_policy", arguments: {} }),
    call(bob.connection.client, { name: "read_policy", arguments: {} }),
  ]);
  const aliceNorms = yield* call(alice.connection.client, {
    name: "read_norms",
    arguments: {},
  });
  yield* send(bob, { to: alice.address, text: "Can we meet today?" });
  const answer = yield* firstDelivery(bob);
  const refusal = yield* send(alice, {
    to: bob.address,
    text: "Yes, today.",
  }).pipe(Effect.flip);

  expect(alicePolicy.norms).toEqual(["office-hours"]);
  expect(bobPolicy.norms).toEqual(["chat", "office-hours"]);
  expect(alicePolicy.fixedRules.map((rule) => rule.id)).toContain(
    officeHoursRule,
  );
  expect(aliceNorms.norms.map((norm) => norm.name)).toEqual([
    "feedback",
    "norm-agreement",
    "norm-discovery",
    "office-hours",
  ]);
  expect(textOf(answer)).toMatch(
    /^\(not-understood :ontology feedback .*:reason "plain-text"/u,
  );
  expect(refusal).toMatchObject({ refusal: "rule-violation" });
});

it(
  "lets two agents of one daemon hold different norms, each applied by its own firewall",
  processTrace(heldNorms),
  HELD_NORMS_TEST_TIMEOUT_MS,
);

const serverOwnsNorms = Effect.gen(function* () {
  const daemon = yield* normsDaemon({
    serverAgentName: "norms-server",
  });
  const server = yield* daemon.signIn(daemon.serverAgentBearer);
  const client = yield* daemon.signInAs("client");

  yield* call(server.connection.client, {
    name: policyTool,
    arguments: { remove: [{ norm: droppedNorm }] },
  });
  const serverNorms = yield* call(server.connection.client, {
    name: "read_norms",
    arguments: {},
  });
  const clientNorms = yield* Effect.tryPromise(() =>
    client.connection.client.callTool({ name: "read_norms", arguments: {} }),
  );
  const clientTools = yield* Effect.tryPromise(() =>
    client.connection.client.listTools(),
  );
  const plainText = yield* answerTo(client, "Can we meet today?");
  const extraField = yield* answerTo(
    client,
    "(propose :ontology norm-agreement :norm office-hours :when today)",
  );
  const noProposal = yield* answerTo(
    client,
    "(accept-proposal :ontology norm-agreement)",
  );

  expect(clientTools.tools.map((tool) => tool.name)).not.toContain(policyTool);
  expect(serverNorms.norms.map((norm) => norm.name)).not.toContain(droppedNorm);
  expect(clientNorms.structuredContent).toEqual(serverNorms);
  expect(plainText).toMatchObject({
    kind: "message",
    message: { author: server.agent },
  });
  expect(textOf(plainText)).toMatch(
    /^\(not-understood :ontology feedback .*:reason "plain-text"/u,
  );
  expect(textOf(extraField)).toMatch(
    /:reason "invalid-content" :error "field \\"when\\"/u,
  );
  expect(textOf(noProposal)).toMatch(/:reason "unknown-instance"/u);
});

it(
  "in client/server mode gives a client the server agent's norms and an answer to each message the server agent does not accept",
  processTrace(serverOwnsNorms),
  HELD_NORMS_TEST_TIMEOUT_MS,
);
