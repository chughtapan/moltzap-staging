/** @file Pre-agreed contracts govern real MCP sends without an agreement exchange. */
import { callTool } from "@social-harness/social-harness";
import { Effect } from "effect";
import { expect, it } from "vitest";
import { joinParticipants, nextItem, send } from "../agent-fixture.js";
import { sharedDaemon } from "../daemon/daemon-fixture.js";
import { processTrace } from "../scenario-fixture.js";
import {
  exchangeDocument,
  exchangeName,
  writeNorms,
} from "./policy-fixture.js";

const timeoutMilliseconds = 120_000;
const ask = `(ask :ontology ${exchangeName} :text "A request")`;
const answer = `(answer :ontology ${exchangeName} :text "A response")`;
const cancel = `(cancel :ontology ${exchangeName})`;
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
    return yield* Effect.dieMessage("Missing participants");
  }
  return { sender, receiver };
});

it(
  "starts directly with a contract act and carries the reply's message ID through MCP and history",
  processTrace(
    Effect.gen(function* () {
      const { sender, receiver } = yield* participants;
      const opening = yield* send(sender, { to: receiver.address, text: ask });
      expect(yield* nextItem(receiver)).toMatchObject({
        message: { msgId: opening.id, content: [{ type: "text", text: ask }] },
      });
      yield* send(receiver, {
        to: sender.address,
        text: answer,
        replyToMsgId: opening.id,
      });
      expect(yield* nextItem(sender)).toMatchObject({
        message: {
          replyToMsgId: opening.id,
          content: [{ type: "text", text: answer }],
        },
      });
      const history = yield* callTool(receiver.connection.client, {
        name: "read_conversation",
        arguments: { address: sender.address },
      });
      expect(JSON.stringify(history)).toContain(opening.id);
    }),
  ),
  timeoutMilliseconds,
);

it(
  "refuses an invalid continuation without preventing the corrected reply",
  processTrace(
    Effect.gen(function* () {
      const { sender, receiver } = yield* participants;
      const opening = yield* send(sender, { to: receiver.address, text: ask });
      yield* nextItem(receiver);
      const failure = yield* send(receiver, {
        to: sender.address,
        text: answer,
      }).pipe(Effect.flip);
      expect(failure).toMatchObject({ refusal: "rule-violation" });
      yield* send(receiver, {
        to: sender.address,
        text: answer,
        replyToMsgId: opening.id,
      });
      expect(yield* nextItem(sender)).toMatchObject({
        message: { replyToMsgId: opening.id },
      });
    }),
  ),
  timeoutMilliseconds,
);

it(
  "orders back-to-back opening and cancellation messages",
  processTrace(
    Effect.gen(function* () {
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
    }),
  ),
  timeoutMilliseconds,
);

it(
  "assesses the contract's English rule on an opening send",
  processTrace(
    Effect.gen(function* () {
      const { sender, receiver } = yield* participants;
      const failure = yield* send(sender, {
        to: receiver.address,
        text: `(ask :ontology ${exchangeName} :text "[out]")`,
      }).pipe(Effect.flip);
      expect(failure).toMatchObject({ refusal: "rule-violation" });
      yield* send(sender, { to: receiver.address, text: ask });
      expect(yield* nextItem(receiver)).toMatchObject({
        message: { content: [{ type: "text", text: ask }] },
      });
    }),
  ),
  timeoutMilliseconds,
);
