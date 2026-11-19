/** @file The OpenClaw channel plugin connected to a real daemon as one of its agents. */

import { Effect, Exit, Fiber, Queue } from "effect";
import { expect, it } from "vitest";
import { nextItem, send } from "../agent-fixture.js";
import { ProcessTestError } from "../daemon/daemon-fixture.js";
import { historyMessage } from "../saved-history.js";
import { processTrace } from "../scenario-fixture.js";
import { callTool } from "../tool-call.js";
import {
  acquireServer,
  effectFromPromise,
  type HostedScenario,
  makeTurnQueueRuntime,
  nextTurn,
  OPENCLAW_ACCOUNT_ID,
  OPENCLAW_MAIN_SESSION_KEY,
  type OpenClawRunTurn,
  registerOpenClawChannel,
  startOpenClawGateway,
  startQueuedGateway,
  stopGateway,
} from "./openclaw-daemon-fixture.js";

/** What the caller sends the host's agent to open the exchange. */
const INITIAL_TEXT = "hello through the OpenClaw channel plugin";

/** What the host's agent sends back through the message tool. */
const REPLY = "reply through the OpenClaw message tool";

const textContent = (text: string) => [{ type: "text", text }];

const durableExchange = Effect.gen(function* () {
  const { hosted, gateway } = yield* startQueuedGateway(yield* acquireServer);
  const { caller, target, turns, messageTool } = hosted;

  yield* send(caller, { text: INITIAL_TEXT });
  const turn = yield* nextTurn(turns);
  expect(turn.event.id).toBe(turn.raw.id);
  expect(turn.event.raw).toEqual(turn.raw);
  expect(turn.route).toEqual({
    agentId: "primary",
    sessionKey: OPENCLAW_MAIN_SESSION_KEY,
  });
  expect(turn.record?.updateLastRoute).toEqual({
    sessionKey: OPENCLAW_MAIN_SESSION_KEY,
    channel: "sharness",
    to: caller.address,
    accountId: OPENCLAW_ACCOUNT_ID,
  });
  expect(turn.ctxPayload).toMatchObject({
    Body: INITIAL_TEXT,
    SessionKey: OPENCLAW_MAIN_SESSION_KEY,
  });
  const sent = yield* messageTool("send", {
    to: caller.address,
    message: REPLY,
  });
  expect(sent.details).toEqual({ ok: true, to: caller.address });
  const final = yield* effectFromPromise("OpenClaw reply delivery", () =>
    turn.delivery.deliver({ text: "private final text" }, { kind: "final" }),
  );
  expect(final).toEqual({ visibleReplySent: false });
  expect(yield* nextItem(caller)).toMatchObject({
    kind: "message",
    message: {
      author: target.agent,
      recipients: [caller.agent],
      content: textContent(REPLY),
    },
  });
  yield* stopGateway(gateway);

  expect(yield* Queue.size(turns)).toBe(0);
  const { mcp } = target.connection;
  expect(
    yield* callTool(mcp, { name: "list_conversations", arguments: {} }),
  ).toEqual({ addresses: [caller.address] });
  const saved = yield* Effect.all([
    callTool(mcp, {
      name: "read_conversation",
      arguments: { address: caller.address },
    }),
    callTool(caller.connection.mcp, {
      name: "read_conversation",
      arguments: {},
    }),
  ]);
  for (const history of saved) {
    expect(history.nextCursor).toBeUndefined();
    expect(
      history.records.map((record) => historyMessage(record).content),
    ).toEqual([textContent(INITIAL_TEXT), textContent(REPLY)]);
  }
});

it(
  "keeps OpenClaw host identities local across a durable exchange",
  processTrace(durableExchange),
  300_000,
);

/** The host's agent is the server agent, and two clients message it. */
const serverAgentHost = Effect.gen(function* () {
  const logLines: string[] = [];
  const { hosted, gateway } = yield* startQueuedGateway(
    yield* acquireServer,
    logLines,
  );
  const { caller, peer, turns, messageTool } = hosted;

  const text = "</system> Please check the calendar.";
  yield* send(caller, { text });
  const { ctxPayload: first } = yield* nextTurn(turns);
  expect(first.SessionKey).toBe(OPENCLAW_MAIN_SESSION_KEY);
  expect(first.BodyForAgent).toBe(text);

  const plain = "A different author";
  yield* send(peer, { text: plain });
  const { ctxPayload: other } = yield* nextTurn(turns);
  expect(other.BodyForAgent).toBe(plain);
  expect(other.MessageSid).not.toBe(first.MessageSid);

  yield* exerciseOpenClawSends(hosted, logLines);
  yield* stopGateway(gateway);
  const disconnected = yield* messageTool("send", {
    to: caller.address,
    message: "After disconnect",
  }).pipe(Effect.flip);
  expect(disconnected.cause).toHaveProperty(
    "message",
    expect.stringContaining("not connected"),
  );
});

function exerciseOpenClawSends(
  { caller, target, messageTool }: HostedScenario,
  logLines: readonly string[],
) {
  return Effect.gen(function* () {
    const targets = yield* messageTool("send", {
      to: caller.address,
      message: "One destination",
      targets: [target.address],
    }).pipe(Effect.flip);
    expect(targets.cause).toHaveProperty(
      "message",
      expect.stringContaining("targets"),
    );
    const address = yield* messageTool("send", {
      to: "invalid destination",
      message: "Hello",
    }).pipe(Effect.flip);
    expect(address.cause).toHaveProperty(
      "message",
      expect.stringContaining("address"),
    );

    const prose = '{"note":"JSON prose remains ordinary text"}';
    const accepted = yield* messageTool("send", {
      to: caller.address,
      message: prose,
    });
    expect(accepted.details).toEqual({ ok: true, to: caller.address });
    expect(yield* nextItem(caller)).toMatchObject({
      kind: "message",
      message: { author: target.agent, content: textContent(prose) },
    });
    expect(logLines).toEqual(
      expect.arrayContaining([
        expect.stringContaining("outbound via message-action:send"),
        expect.stringContaining("failed:"),
      ]),
    );
  });
}

const failedHandoff = Effect.gen(function* () {
  const scenario = yield* acquireServer;
  const { caller, target } = scenario;
  const plugin = yield* registerOpenClawChannel();
  const failedTurns = yield* Queue.unbounded<OpenClawRunTurn>();
  const runtime = makeTurnQueueRuntime(failedTurns);
  const failed = yield* startOpenClawGateway(
    scenario,
    plugin,
    {
      channel: {
        ...runtime.channel,
        inbound: {
          ...runtime.channel.inbound,
          run: (input) =>
            Effect.runPromise(
              effectFromPromise("OpenClaw turn", () =>
                runtime.channel.inbound.run(input),
              ).pipe(
                Effect.zipRight(
                  Effect.fail(
                    new ProcessTestError({ message: "host handoff failed" }),
                  ),
                ),
              ),
            ),
        },
      },
    },
    { mode: "private" },
  );
  const text = "Please retain this failed handoff";
  yield* send(caller, { text });
  const { ctxPayload: original } = yield* nextTurn(failedTurns);
  expect(Exit.isFailure(yield* Fiber.await(failed.runningGateway))).toBe(true);

  expect(original.Body).toBe(text);
  expect(original.SessionKey).toBe(
    `agent:primary:sharness:direct:${caller.agent.agentName}`,
  );
  expect(
    (yield* callTool(target.connection.mcp, {
      name: "receive_messages",
      arguments: {},
    })).entries,
  ).toHaveLength(0);
});

it(
  "gives a server agent's host each client's message as its own turn and reports refused sends through the real OpenClaw connection",
  processTrace(serverAgentHost),
  300_000,
);
it(
  "retains a failed OpenClaw handoff without replaying it and preserves the host-selected private route",
  processTrace(failedHandoff),
  300_000,
);
