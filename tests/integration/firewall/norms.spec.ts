/**
 * @file A norm two agents of one daemon both hold. They start with an act of
 * it, with no agreement exchange, and each agent's firewall checks its own
 * agent's acts and its peer's. The daemon runs its own composition with the
 * marked decision provider, so the norm's English rule is judged on every
 * act: `[out]` in a text breaks it for the sender's firewall and `[in]` for
 * the receiver's.
 */

import { Effect } from "effect";
import { expect, it } from "vitest";
import { joinParticipants, nextItem, send } from "../agent-fixture.js";
import { sharedDaemon } from "../daemon/daemon-fixture.js";
import { processTrace } from "../scenario-fixture.js";
import { arrayContaining, stringMatching } from "../test-matchers.js";
import { callTool } from "../tool-call.js";
import {
  exchangeDocument,
  exchangeFeedback,
  exchangeName,
  writeNorms,
} from "./policy-fixture.js";

/** Each test starts one daemon under `tsx`. */
const NORMS_TEST_TIMEOUT_MS = 120_000;

const ask = `(ask :ontology ${exchangeName} :text "A request")`;
const answer = `(answer :ontology ${exchangeName} :text "A response")`;
const cancel = `(cancel :ontology ${exchangeName})`;

/** A sender and a receiver on one firewall daemon, each holding the exchange norm. */
const participants = Effect.gen(function* () {
  const daemon = yield* sharedDaemon({ firewall: true });
  yield* daemon.configure(
    yield* writeNorms(daemon.directory, [exchangeDocument]),
  );
  yield* daemon.start();
  const [sender, receiver] = yield* joinParticipants(daemon, [
    "sender",
    "receiver",
  ]);
  if (sender === undefined || receiver === undefined) {
    return yield* Effect.dieMessage("expected two participants");
  }
  return { sender, receiver };
});

const replyReference = Effect.gen(function* () {
  const { sender, receiver } = yield* participants;
  const opening = yield* send(sender, { to: receiver.address, text: ask });
  expect(yield* nextItem(receiver)).toMatchObject({
    message: { msgId: opening.id, content: [{ type: "text", text: ask }] },
  });
  const unreferenced = yield* send(receiver, {
    to: sender.address,
    text: answer,
  }).pipe(Effect.flip);
  yield* send(receiver, {
    to: sender.address,
    text: answer,
    replyToMsgId: opening.id,
  });
  const received = yield* nextItem(sender);
  const history = yield* callTool(receiver.connection.mcp, {
    name: "read_conversation",
    arguments: { address: sender.address },
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
  const { sender, receiver } = yield* participants;
  const opening = yield* send(sender, { to: receiver.address, text: ask });
  const cancelled = yield* send(sender, {
    to: receiver.address,
    text: cancel,
    replyToMsgId: opening.id,
  });

  expect(yield* nextItem(receiver)).toMatchObject({
    message: { msgId: opening.id },
  });
  expect(yield* nextItem(receiver)).toMatchObject({
    message: { msgId: cancelled.id, replyToMsgId: opening.id },
  });
}).pipe(Effect.scoped);

const ownBreach = Effect.gen(function* () {
  const { sender, receiver } = yield* participants;
  const refusal = yield* send(sender, {
    to: receiver.address,
    text: `(ask :ontology ${exchangeName} :text "[out]")`,
  }).pipe(Effect.flip);
  yield* send(sender, { to: receiver.address, text: ask });

  expect(refusal).toMatchObject({
    refusal: "rule-violation",
    feedback: arrayContaining([{ text: exchangeFeedback }]),
    detail: { norm: exchangeName, act: "ask", stage: "semantic" },
  });
  expect(yield* nextItem(receiver)).toMatchObject({
    message: { content: [{ type: "text", text: ask }] },
  });
}).pipe(Effect.scoped);

const peerBreach = Effect.gen(function* () {
  const { sender, receiver } = yield* participants;
  const opening = yield* send(sender, { to: receiver.address, text: ask });
  yield* nextItem(receiver);
  const withheld = yield* send(receiver, {
    to: sender.address,
    text: `(answer :ontology ${exchangeName} :text "[in]")`,
    replyToMsgId: opening.id,
  });
  const refusalAct = yield* nextItem(receiver);
  yield* send(receiver, {
    to: sender.address,
    text: answer,
    replyToMsgId: opening.id,
  });

  expect(refusalAct).toMatchObject({
    message: {
      author: sender.agent,
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
  expect(yield* nextItem(sender)).toMatchObject({
    message: { content: [{ type: "text", text: answer }] },
  });
}).pipe(Effect.scoped);

it(
  "starts with an act of a held norm, refuses a continuation that names no message, and carries the reply reference through the inbox and history",
  processTrace(replyReference),
  NORMS_TEST_TIMEOUT_MS,
);

it(
  "delivers an agent's opening act and its cancellation sent back to back, in the order sent",
  processTrace(backToBack),
  NORMS_TEST_TIMEOUT_MS,
);

it(
  "refuses the sender's own act that breaks the norm's English rule, and sends the corrected act",
  processTrace(ownBreach),
  NORMS_TEST_TIMEOUT_MS,
);

it(
  "withholds a peer's act that breaks the norm's English rule, answers it with the norm's refusal act, and delivers the corrected act",
  processTrace(peerBreach),
  NORMS_TEST_TIMEOUT_MS,
);
