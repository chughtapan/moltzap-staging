/**
 * @file The norms the server agent holds, which a client reads.
 *
 * A client sends without a check of its own. Inside an instance, the server
 * agent answers an act it withholds with the norm's refusal act, which the
 * client receives as an ordinary message. A message it withholds outside an
 * instance gets no answer: the server agent's own firewall refuses any answer
 * that is not an act of the norm.
 */

import {
  MAXIMUM_WAIT_SECONDS,
  type PermittedItem,
} from "@social-harness/social-harness";
import { Effect } from "effect";
import { expect, it } from "vitest";
import { send } from "../agent-fixture.js";
import {
  SERVER_AGENT_OWNER,
  sharedDaemon,
  type SignedIn,
} from "../daemon/daemon-fixture.js";
import { processTrace } from "../scenario-fixture.js";
import { callTool as call } from "../tool-call.js";
import {
  exchangeDocument,
  exchangeName,
  writeNorms,
} from "./policy-fixture.js";

/** Each test starts one daemon. */
const HELD_NORMS_TEST_TIMEOUT_MS = 180_000;

const policyTool = "update_policy";
const ask = `(ask :ontology ${exchangeName} :text "A request")`;

/**
 * The first item in the agent's inbox. One call waits for it: the daemon
 * answers as soon as the item arrives.
 */
function firstDelivery(agent: SignedIn) {
  return call(agent.connection.mcp, {
    name: "receive_messages",
    arguments: { wait: MAXIMUM_WAIT_SECONDS },
  }).pipe(
    Effect.map(({ entries }) => entries[0]?.item),
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

const serverOwnsNorms = Effect.gen(function* () {
  const daemon = yield* sharedDaemon({ firewall: true });
  yield* daemon.configure(
    yield* writeNorms(daemon.directory, [exchangeDocument]),
  );
  yield* daemon.start();
  const server = yield* daemon.signInAs(SERVER_AGENT_OWNER);
  const client = yield* daemon.signInAs("client");

  const serverNorms = yield* call(server.connection.mcp, {
    name: "read_norms",
    arguments: {},
  });
  const clientNorms = yield* call(client.connection.mcp, {
    name: "read_norms",
    arguments: {},
  });
  const clientTools = yield* Effect.tryPromise(() =>
    client.connection.mcp.listTools(),
  );
  yield* send(client, { text: "Can we meet today?" });
  const opening = yield* send(client, { text: ask });
  const opened = yield* firstDelivery(server);
  const repeated = yield* send(client, { text: ask, replyToMsgId: opening.id });
  const answer = yield* firstDelivery(client);
  yield* call(server.connection.mcp, {
    name: policyTool,
    arguments: { remove: [{ norm: exchangeName }] },
  });
  const afterRemoval = yield* call(client.connection.mcp, {
    name: "read_norms",
    arguments: {},
  });

  expect(clientTools.tools.map((tool) => tool.name)).not.toContain(policyTool);
  expect(serverNorms.norms.map((norm) => norm.document)).toEqual([
    exchangeDocument,
  ]);
  expect(clientNorms).toEqual(serverNorms);
  expect(textOf(opened)).toBe(ask);
  expect(answer).toMatchObject({
    kind: "message",
    message: { author: server.agent, replyToMsgId: repeated.id },
  });
  expect(textOf(answer)).toMatch(
    new RegExp(`^\\(not-understood :ontology ${exchangeName} :reason "`, "u"),
  );
  expect(afterRemoval).toEqual({ norms: [] });
});

it(
  "gives a client the server agent's norms, no answer to plain text the server agent withholds, and inside an instance the norm's refusal act for an act it withholds",
  processTrace(serverOwnsNorms),
  HELD_NORMS_TEST_TIMEOUT_MS,
);
