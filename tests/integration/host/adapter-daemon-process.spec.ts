/** @file The OpenClaw channel plugin connected to a real daemon as one of its agents. */

import { Effect, Exit, Fiber, Queue } from "effect";
import { expect, it } from "vitest";
import {
  groupAddress,
  nextItem,
  type Participant,
  send,
} from "../agent-fixture.js";
import { ProcessTestError, type SignedIn } from "../daemon/daemon-fixture.js";
import { historyMessage } from "../saved-history.js";
import { processTrace } from "../scenario-fixture.js";
import { callTool } from "../tool-call.js";
import {
  acquirePeers,
  acquireServer,
  DELIVERY_TIMEOUT,
  effectFromPromise,
  makeTurnQueueRuntime,
  nextTurn,
  OPENCLAW_ACCOUNT_ID,
  OPENCLAW_MAIN_SESSION_KEY,
  openClawConfig,
  type OpenClawMessageActionContext,
  type OpenClawRunTurn,
  registerOpenClawChannel,
  type Scenario,
  startOpenClawGateway,
} from "./openclaw-daemon-fixture.js";

/** What the caller sends the host's agent to open the exchange. */
const INITIAL_TEXT = "hello through the OpenClaw channel plugin";

/** What the host's agent sends back through the message tool. */
const REPLY = "reply through the OpenClaw message tool";

/** What the caller sends its group with the host's agent and a peer. */
const GROUP_TEXT = "hello to the group";

/** What the host's agent sends the group back through the message tool. */
const GROUP_REPLY = "reply to the group through the OpenClaw message tool";

const textContent = (text: string) => [{ type: "text", text }];

/** A scenario whose host hands every turn to the test and sends through the message tool. */
interface HostedScenario extends Scenario {
  readonly turns: Queue.Queue<OpenClawRunTurn>;
  readonly messageTool: ReturnType<typeof makeMessageTool>;
}

type ChannelPlugin = Effect.Effect.Success<
  ReturnType<typeof registerOpenClawChannel>
>;

type Gateway = Effect.Effect.Success<ReturnType<typeof startOpenClawGateway>>;

function makeMessageTool(channelPlugin: ChannelPlugin) {
  const cfg = openClawConfig();
  return (
    action: OpenClawMessageActionContext["action"],
    params: OpenClawMessageActionContext["params"],
  ) =>
    effectFromPromise(`OpenClaw message tool ${action}`, () =>
      channelPlugin.actions.handleAction({
        channel: "sharness",
        action,
        cfg,
        accountId: OPENCLAW_ACCOUNT_ID,
        params,
      }),
    );
}

/** Connects the plugin as the scenario's target with a host that queues its turns. */
function startQueuedGateway(scenario: Scenario, logLines?: string[]) {
  return Effect.gen(function* () {
    const channelPlugin = yield* registerOpenClawChannel();
    const turns = yield* Queue.unbounded<OpenClawRunTurn>();
    const gateway = yield* startOpenClawGateway(
      scenario,
      channelPlugin,
      makeTurnQueueRuntime(turns),
      logLines === undefined ? {} : { logLines },
    );
    const hosted: HostedScenario = {
      ...scenario,
      turns,
      messageTool: makeMessageTool(channelPlugin),
    };
    return { hosted, gateway };
  });
}

function stopGateway({ abortController, runningGateway }: Gateway) {
  abortController.abort();
  return Fiber.join(runningGateway).pipe(
    Effect.timeoutFail({
      duration: DELIVERY_TIMEOUT,
      onTimeout: () =>
        new ProcessTestError({
          message: "timed out stopping OpenClaw gateway",
        }),
    }),
  );
}

/**
 * The contents of the reader's saved conversation with `peer`, which must be
 * the reader's only conversation and fit one page.
 */
function savedExchange(reader: SignedIn, peer: SignedIn) {
  return Effect.gen(function* () {
    const { mcp } = reader.connection;
    expect(
      yield* callTool(mcp, { name: "list_conversations", arguments: {} }),
    ).toEqual({ addresses: [peer.address] });
    const history = yield* callTool(mcp, {
      name: "read_conversation",
      arguments: { address: peer.address },
    });
    expect(history.nextCursor).toBeUndefined();
    return history.records.map((record) => historyMessage(record).content);
  });
}

/** A client's send in client/server mode, which names no recipient. */
function sendAsClient(client: Participant, text: string) {
  return Effect.tryPromise(() =>
    client.connection.mcp.callTool({
      name: "send_message",
      arguments: { text },
    }),
  ).pipe(Effect.filterOrFail((result) => result.isError !== true));
}

const durableExchange = Effect.gen(function* () {
  const { hosted, gateway } = yield* startQueuedGateway(yield* acquirePeers);
  const { caller, target, turns, messageTool } = hosted;

  yield* send(caller, { to: target.address, text: INITIAL_TEXT });
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
  const exchange = [textContent(INITIAL_TEXT), textContent(REPLY)];
  expect(yield* savedExchange(caller, target)).toEqual(exchange);
  expect(yield* savedExchange(target, caller)).toEqual(exchange);
});

const groupBothWays = Effect.gen(function* () {
  const { hosted, gateway } = yield* startQueuedGateway(yield* acquirePeers);
  const { caller, peer, target, turns, messageTool } = hosted;
  const { id, address } = yield* send(caller, {
    to: groupAddress([caller, peer, target]),
    text: GROUP_TEXT,
  });
  const turn = yield* nextTurn(turns);
  expect(turn.ctxPayload).toMatchObject({
    ChatType: "group",
    From: caller.address,
    BodyForAgent: GROUP_TEXT,
  });
  expect(turn.raw.id).toBe(id);
  yield* nextItem(peer);
  const replied = yield* messageTool("reply", {
    to: address,
    message: GROUP_REPLY,
  });
  expect(replied.details).toMatchObject({ ok: true });
  const reply = {
    kind: "message",
    message: { author: target.agent, content: textContent(GROUP_REPLY) },
  };
  expect(yield* nextItem(caller)).toMatchObject(reply);
  expect(yield* nextItem(peer)).toMatchObject(reply);
  yield* stopGateway(gateway);
});

it(
  "keeps OpenClaw host identities local across a durable exchange",
  processTrace(durableExchange),
  300_000,
);

it(
  "carries a group message in both directions through the OpenClaw message tool",
  processTrace(groupBothWays),
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
  yield* sendAsClient(caller, text);
  const { ctxPayload: first } = yield* nextTurn(turns);
  expect(first.SessionKey).toBe(OPENCLAW_MAIN_SESSION_KEY);
  expect(first.BodyForAgent).toBe(text);

  const plain = "A different author";
  yield* sendAsClient(peer, plain);
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
  const scenario = yield* acquirePeers;
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
  yield* send(caller, { to: target.address, text });
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
