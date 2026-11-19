/**
 * @file A norm the server agent holds. It and a client start with an act of
 * it, with no agreement exchange, and the server agent's firewall checks its
 * own acts and the client's. The daemon runs its own composition with the
 * marked decision provider, so the norm's English rule is judged on every
 * act: `[out]` in a text breaks it when the server agent sends it and `[in]`
 * when the server agent receives it.
 */

import { Effect } from "effect";
import { expect, it } from "vitest";
import { joinParticipants, nextItem, send } from "../agent-fixture.js";
import { SERVER_AGENT_OWNER, sharedDaemon } from "../daemon/daemon-fixture.js";
import { processTrace } from "../scenario-fixture.js";
import { arrayContaining, stringMatching } from "../test-matchers.js";
import { callTool } from "../tool-call.js";
import {
  exchangeDocument,
  exchangeFeedback,
  exchangeName,
  writeNorms,
} from "./policy-fixture.js";

/** Each test starts one daemon. */
const NORMS_TEST_TIMEOUT_MS = 120_000;

const ask = `(ask :ontology ${exchangeName} :text "A request")`;
const answer = `(answer :ontology ${exchangeName} :text "A response")`;
const cancel = `(cancel :ontology ${exchangeName})`;

/** The server agent, which holds the exchange norm, and a client of one firewall daemon. */
const participants = Effect.gen(function* () {
  const daemon = yield* sharedDaemon({ firewall: true });
  yield* daemon.configure(
    yield* writeNorms(daemon.directory, [exchangeDocument]),
  );
  yield* daemon.start();
  const [server, client] = yield* joinParticipants(daemon, [
    SERVER_AGENT_OWNER,
    "client",
  ]);
  if (server === undefined || client === undefined) {
    return yield* Effect.dieMessage("expected two participants");
  }
  return { server, client };
});

const replyReference = Effect.gen(function* () {
  const { server, client } = yield* participants;
  const to = client.address;
  const opening = yield* send(client, { text: ask });
  expect(yield* nextItem(server)).toMatchObject({
    message: { msgId: opening.id, content: [{ type: "text", text: ask }] },
  });
  const unreferenced = yield* send(server, { to, text: answer }).pipe(
    Effect.flip,
  );
  yield* send(server, { to, text: answer, replyToMsgId: opening.id });
  const received = yield* nextItem(client);
  const history = yield* callTool(server.connection.mcp, {
    name: "read_conversation",
    arguments: { address: to },
  });

  expect(unreferenced).toMatchObject({
    refusal: "rule-violation",
    detail: { norm: exchangeName, stage: "protocol", field: "replyToMsgId" },
  });
  expect(received).toMatchObject({
    message: {
      replyToMsgId: opening.id,
      content: [{ type: "text", text: answer }],
    },
  });
  expect(history.records).toMatchObject([
    { kind: "received", item: { message: { msgId: opening.id } } },
    { kind: "sent", message: { replyToMsgId: opening.id } },
  ]);
}).pipe(Effect.scoped);

const backToBack = Effect.gen(function* () {
  const { server, client } = yield* participants;
  const opening = yield* send(client, { text: ask });
  const cancelled = yield* send(client, {
    text: cancel,
    replyToMsgId: opening.id,
  });

  expect(yield* nextItem(server)).toMatchObject({
    message: { msgId: opening.id },
  });
  expect(yield* nextItem(server)).toMatchObject({
    message: { msgId: cancelled.id, replyToMsgId: opening.id },
  });
}).pipe(Effect.scoped);

const ownBreach = Effect.gen(function* () {
  const { server, client } = yield* participants;
  const to = client.address;
  const refusal = yield* send(server, {
    to,
    text: `(ask :ontology ${exchangeName} :text "[out]")`,
  }).pipe(Effect.flip);
  yield* send(server, { to, text: ask });

  expect(refusal).toMatchObject({
    refusal: "rule-violation",
    feedback: arrayContaining([{ text: exchangeFeedback }]),
    detail: { norm: exchangeName, act: "ask", stage: "semantic" },
  });
  expect(yield* nextItem(client)).toMatchObject({
    message: { content: [{ type: "text", text: ask }] },
  });
}).pipe(Effect.scoped);

const clientBreach = Effect.gen(function* () {
  const { server, client } = yield* participants;
  const opening = yield* send(server, { to: client.address, text: ask });
  yield* nextItem(client);
  const withheld = yield* send(client, {
    text: `(answer :ontology ${exchangeName} :text "[in]")`,
    replyToMsgId: opening.id,
  });
  const refusalAct = yield* nextItem(client);
  yield* send(client, { text: answer, replyToMsgId: opening.id });

  expect(refusalAct).toMatchObject({
    message: {
      author: server.agent,
      replyToMsgId: withheld.id,
      content: [
        {
          type: "text",
          text: stringMatching(
            new RegExp(
              `^\\(not-understood :ontology ${exchangeName} :reason ".*${exchangeFeedback}"\\)$`,
              "u",
            ),
          ),
        },
      ],
    },
  });
  expect(yield* nextItem(server)).toMatchObject({
    message: { content: [{ type: "text", text: answer }] },
  });
}).pipe(Effect.scoped);

it(
  "starts with an act of a held norm, refuses a continuation that names no message, and carries the reply reference through the inbox and history",
  processTrace(replyReference),
  NORMS_TEST_TIMEOUT_MS,
);

it(
  "delivers a client's opening act and its cancellation sent back to back, in the order sent",
  processTrace(backToBack),
  NORMS_TEST_TIMEOUT_MS,
);

it(
  "refuses the server agent's own act that breaks the norm's English rule, and sends the corrected act",
  processTrace(ownBreach),
  NORMS_TEST_TIMEOUT_MS,
);

it(
  "withholds a client's act that breaks the norm's English rule, answers it with the norm's refusal act, and delivers the corrected act",
  processTrace(clientBreach),
  NORMS_TEST_TIMEOUT_MS,
);
