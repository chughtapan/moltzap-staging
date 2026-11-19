/**
 * @file The norms each agent of one daemon holds.
 *
 * In P2P mode each agent starts with the contracts of the norms directory and
 * changes its own with `update_policy`, and its firewall applies what it
 * holds: an agent that holds a norm refuses a direct text that is not an act
 * of it, while an agent that stopped holding it sends any text.
 *
 * In client/server mode only the server agent holds norms. A client reads the
 * server agent's norms and sends without a check of its own. Inside an
 * instance, the server agent answers an act it withholds with the norm's
 * refusal act, which the client receives as an ordinary message. A message it
 * withholds outside an instance gets no answer: the server agent's own
 * firewall refuses any answer that is not an act of the norm.
 */

import {
  MAXIMUM_WAIT_SECONDS,
  type PermittedItem,
} from "@social-harness/social-harness";
import { Effect, Schema } from "effect";
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

/** Each test starts one daemon under `tsx`. */
const HELD_NORMS_TEST_TIMEOUT_MS = 180_000;

const policyTool = "update_policy";
const ask = `(ask :ontology ${exchangeName} :text "A request")`;

/**
 * A running firewall daemon whose new agents hold the exchange norm, in P2P
 * mode unless `settings` name another.
 */
function normsDaemon(settings: Readonly<Record<string, unknown>> = {}) {
  return Effect.gen(function* () {
    const daemon = yield* sharedDaemon({ firewall: true, settings });
    yield* daemon.configure(
      yield* writeNorms(daemon.directory, [exchangeDocument]),
    );
    yield* daemon.start();
    return daemon;
  });
}

/** Sends `text` as a client does, naming no recipient, and returns the message id. */
function sendAsClient(client: SignedIn, text: string, replyToMsgId?: string) {
  return Effect.tryPromise(() =>
    client.connection.mcp.callTool({
      name: "send_message",
      arguments: {
        text,
        ...(replyToMsgId === undefined ? {} : { replyToMsgId }),
      },
    }),
  ).pipe(
    Effect.filterOrFail((sent) => sent.isError !== true),
    Effect.map(
      (sent) =>
        Schema.decodeUnknownSync(Schema.Struct({ id: Schema.String }))(
          sent.structuredContent,
        ).id,
    ),
  );
}

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

const heldNorms = Effect.gen(function* () {
  const daemon = yield* normsDaemon();
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
  HELD_NORMS_TEST_TIMEOUT_MS,
);

const serverOwnsNorms = Effect.gen(function* () {
  const daemon = yield* normsDaemon({
    serverAgent: { name: "norms-server", subject: SERVER_AGENT_OWNER },
  });
  const server = yield* daemon.signInAs(SERVER_AGENT_OWNER);
  const client = yield* daemon.signInAs("client");

  const serverNorms = yield* call(server.connection.mcp, {
    name: "read_norms",
    arguments: {},
  });
  const clientNorms = yield* Effect.tryPromise(() =>
    client.connection.mcp.callTool({ name: "read_norms", arguments: {} }),
  );
  const clientTools = yield* Effect.tryPromise(() =>
    client.connection.mcp.listTools(),
  );
  yield* sendAsClient(client, "Can we meet today?");
  const opening = yield* sendAsClient(client, ask);
  const opened = yield* firstDelivery(server);
  const repeated = yield* sendAsClient(client, ask, opening);
  const answer = yield* firstDelivery(client);
  yield* call(server.connection.mcp, {
    name: policyTool,
    arguments: { remove: [{ norm: exchangeName }] },
  });
  const afterRemoval = yield* Effect.tryPromise(() =>
    client.connection.mcp.callTool({ name: "read_norms", arguments: {} }),
  );

  expect(clientTools.tools.map((tool) => tool.name)).not.toContain(policyTool);
  expect(serverNorms.norms.map((norm) => norm.document)).toEqual([
    exchangeDocument,
  ]);
  expect(clientNorms.structuredContent).toEqual(serverNorms);
  expect(textOf(opened)).toBe(ask);
  expect(answer).toMatchObject({
    kind: "message",
    message: { author: server.agent, replyToMsgId: repeated },
  });
  expect(textOf(answer)).toMatch(
    new RegExp(`^\\(not-understood :ontology ${exchangeName} :reason "`, "u"),
  );
  expect(afterRemoval.structuredContent).toEqual({ norms: [] });
});

it(
  "in client/server mode gives a client the server agent's norms, no answer to plain text the server agent withholds, and inside an instance the norm's refusal act for an act it withholds",
  processTrace(serverOwnsNorms),
  HELD_NORMS_TEST_TIMEOUT_MS,
);
