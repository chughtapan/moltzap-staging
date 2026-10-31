/** @file Real-daemon acceptance for the runtime adapter boundaries. */

import {
  acquireHarnessEndpoint,
  type AgentView,
  type InboundDelivery,
} from "@moltzap/social-harness";
import { Deferred, Effect, Fiber, Queue } from "effect";
import { expect, it } from "vitest";

import {
  acquireGroupScenario,
  acquireScenario,
  assertDurableExchange,
  awaitSignal,
  DELIVERY_TIMEOUT,
  directAddress,
  effectFromPromise,
  groupAddress,
  makeObservedOpenClawAccountRuntime,
  makeTurnQueueRuntime,
  multicastContent,
  nextDelivery,
  nextItem,
  nextTurn,
  OPENCLAW_ACCOUNT_ID,
  OPENCLAW_INITIAL_TEXT,
  OPENCLAW_MAIN_SESSION_KEY,
  OPENCLAW_REPLY,
  openClawConfig,
  type OpenClawInboundContext,
  type OpenClawMessageActionContext,
  RecordedSessionStore,
  registerOpenClawChannel,
  requireRequest,
  startOpenClawGateway,
} from "./adapter-daemon-fixture.js";
import { ProcessTestError } from "../daemon/daemon-process-harness.js";
import { processTrace } from "../process-trace.js";

const SLOT_SCHEMA = {
  type: "object",
  properties: { slot: { type: "string", enum: ["mon", "tue"] } },
  required: ["slot"],
} as const;

const GATHER_QUESTION = "Which day works?";

/** The text a model sends to ask its question, or to answer one. */
const gatherText = (operation: "gather" | "all_gather") =>
  JSON.stringify({
    [operation]: GATHER_QUESTION,
    deadline: 60,
    requestedSchema: SLOT_SCHEMA,
  });

const acceptText = (slot: string, operationId: string) =>
  JSON.stringify({ action: "accept", operationId, content: { slot } });

interface AllGatherContext {
  readonly caller: Effect.Effect.Success<
    ReturnType<typeof acquireHarnessEndpoint>
  >;
  readonly peer: Effect.Effect.Success<
    ReturnType<typeof acquireHarnessEndpoint>
  >;
  readonly callerAddress: ReturnType<typeof directAddress>;
  readonly peerAddress: ReturnType<typeof directAddress>;
  readonly targetAddress: ReturnType<typeof directAddress>;
  readonly callerAgent: AgentView;
  readonly peerAgent: AgentView;
  readonly targetAgent: AgentView;
  readonly group: ReturnType<typeof groupAddress>;
  readonly turns: Queue.Queue<OpenClawInboundContext>;
  readonly messageTool: ReturnType<typeof makeMessageTool>;
  readonly allGather: {
    readonly op: "all_gather";
    readonly deadline: number;
    readonly requestedSchema: typeof SLOT_SCHEMA;
  };
}

function runOpenClawScenario() {
  return Effect.scoped(
    Effect.gen(function* () {
      const scenario = yield* acquireScenario("openclaw");
      const caller = yield* acquireHarnessEndpoint(scenario.caller.endpoint, {
        credential: scenario.caller.mcpCredential,
        receiveMode: "push",
      });
      const callerAddress = directAddress(scenario.caller.agentName);
      const targetAddress = directAddress(scenario.target.agentName);
      const {
        initialText,
        initial,
        reply,
        responseSent,
        sessions,
        abortController,
        runningGateway,
      } = yield* startReplyScenario(scenario, callerAddress);

      const callerDelivery = yield* Effect.forkScoped(
        nextDelivery(caller.messages),
      );
      yield* caller.send({ to: targetAddress, text: initialText });
      yield* Effect.raceFirst(
        awaitSignal(responseSent, "OpenClaw channel reply"),
        Fiber.join(runningGateway).pipe(
          Effect.zipRight(
            Effect.fail(
              new ProcessTestError({
                message: "OpenClaw account stopped before its channel reply",
              }),
            ),
          ),
        ),
      );
      const returned = yield* Fiber.join(callerDelivery);
      assertReply(returned, scenario.target.agent, scenario.caller.agent);

      yield* stopGateway(abortController, runningGateway);

      const session = sessions.snapshot(OPENCLAW_MAIN_SESSION_KEY);
      expect(sessions.keys()).toEqual([OPENCLAW_MAIN_SESSION_KEY]);
      expect(session.messageIds).toHaveLength(1);
      yield* assertDurableExchange(scenario, initial, [reply]);
    }),
  );
}

function runOpenClawGatherScenario() {
  return Effect.scoped(
    Effect.gen(function* () {
      const scenario = yield* acquireScenario("openclaw-gather");
      const caller = yield* acquireHarnessEndpoint(scenario.caller.endpoint, {
        credential: scenario.caller.mcpCredential,
        receiveMode: "push",
      });
      const callerAddress = directAddress(scenario.caller.agentName);
      const targetAddress = directAddress(scenario.target.agentName);
      const cfg = openClawConfig(scenario.target.stateDirectory);
      const channelPlugin = yield* registerOpenClawChannel();
      const { turns, abortController, runningGateway } =
        yield* startQueuedGateway(scenario, channelPlugin);
      const messageTool = makeMessageTool(channelPlugin, cfg);

      yield* sendGather({
        caller,
        callerAddress,
        targetAddress,
        targetAgent: scenario.target.agent,
        turns,
        messageTool,
      });

      yield* receiveGather({
        caller,
        callerAddress,
        targetAddress,
        targetAgent: scenario.target.agent,
        turns,
        messageTool,
      });

      yield* stopGateway(abortController, runningGateway);
    }),
  );
}

function receiveGather({
  caller,
  callerAddress,
  targetAddress,
  targetAgent,
  turns,
  messageTool,
}: Parameters<typeof sendGather>[0]) {
  return Effect.gen(function* () {
    const started = yield* caller.send({
      to: targetAddress,
      text: GATHER_QUESTION,
      collective: {
        op: "gather",
        deadline: 60,
        requestedSchema: SLOT_SCHEMA,
      },
    });
    const operationId = yield* Effect.fromNullable(started.operationId);
    const requestTurn = yield* nextTurn(turns);
    expect(requestTurn.From).toBe(callerAddress);
    const answered = yield* messageTool("reply", {
      to: callerAddress,
      message: acceptText("tue", operationId),
    });
    expect(answered.details).toMatchObject({ ok: true });
    expect(yield* nextItem(caller.messages)).toMatchObject({
      kind: "collectiveResult",
      outcomes: [
        {
          member: targetAgent,
          outcome: { kind: "answered", content: { slot: "tue" } },
        },
      ],
    });
  });
}

function runOpenClawAllGatherScenario() {
  return Effect.scoped(
    Effect.gen(function* () {
      const scenario = yield* acquireGroupScenario("openclaw-all-gather");
      const caller = yield* acquireHarnessEndpoint(scenario.caller.endpoint, {
        credential: scenario.caller.mcpCredential,
        receiveMode: "push",
      });
      const peer = yield* acquireHarnessEndpoint(scenario.peer.endpoint, {
        credential: scenario.peer.mcpCredential,
        receiveMode: "push",
      });
      const callerAddress = directAddress(scenario.caller.agentName);
      const peerAddress = directAddress(scenario.peer.agentName);
      const targetAddress = directAddress(scenario.target.agentName);
      const group = groupAddress([
        scenario.caller.agentName,
        scenario.peer.agentName,
        scenario.target.agentName,
      ]);
      const cfg = openClawConfig(scenario.target.stateDirectory);
      const channelPlugin = yield* registerOpenClawChannel();
      const { turns, abortController, runningGateway } =
        yield* startQueuedGateway(scenario, channelPlugin);
      const messageTool = makeMessageTool(channelPlugin, cfg);
      const allGather = {
        op: "all_gather",
        deadline: 60,
        requestedSchema: SLOT_SCHEMA,
      } as const;

      const context = {
        caller,
        peer,
        callerAddress,
        peerAddress,
        targetAddress,
        callerAgent: scenario.caller.agent,
        peerAgent: scenario.peer.agent,
        targetAgent: scenario.target.agent,
        group,
        turns,
        messageTool,
        allGather,
      };
      yield* receiveAllGather(context);
      yield* sendAllGather(context);

      yield* stopGateway(abortController, runningGateway);
    }),
  );
}

function stopGateway(
  abortController: AbortController,
  runningGateway: Fiber.RuntimeFiber<void, ProcessTestError>,
) {
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

function makeMessageTool(
  channelPlugin: Effect.Effect.Success<
    ReturnType<typeof registerOpenClawChannel>
  >,
  cfg: ReturnType<typeof openClawConfig>,
) {
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

function startReplyScenario(
  scenario: Effect.Effect.Success<ReturnType<typeof acquireScenario>>,
  callerAddress: string,
) {
  return Effect.gen(function* () {
    const initialText = OPENCLAW_INITIAL_TEXT;
    const initial = multicastContent(initialText);
    const reply = multicastContent(OPENCLAW_REPLY);
    const responseSent = yield* Deferred.make<undefined>();
    const sessions = new RecordedSessionStore();
    const cfg = openClawConfig(scenario.target.stateDirectory);
    const channelPlugin = yield* registerOpenClawChannel();
    const runtime = makeObservedOpenClawAccountRuntime({
      callerAddress,
      callerId: scenario.caller.agentName,
      responseSent,
      sessions,
      sendReply: () =>
        channelPlugin.actions.handleAction({
          channel: "sharness",
          action: "send",
          cfg,
          accountId: OPENCLAW_ACCOUNT_ID,
          params: { to: callerAddress, message: OPENCLAW_REPLY },
        }),
    });

    const { abortController, runningGateway } = yield* startOpenClawGateway(
      scenario.target,
      channelPlugin,
      runtime,
    );

    return {
      initialText,
      initial,
      reply,
      responseSent,
      sessions,
      abortController,
      runningGateway,
    };
  });
}

function receiveAllGather(context: AllGatherContext) {
  const { caller, peer, callerAddress, group, turns, messageTool, allGather } =
    context;
  return Effect.gen(function* () {
    yield* caller.send({
      to: group,
      text: GATHER_QUESTION,
      collective: allGather,
    });
    const requestTurn = yield* nextTurn(turns);
    expect(requestTurn).toMatchObject({
      ChatType: "group",
      From: callerAddress,
    });
    const peerRequest = yield* nextItem(peer.messages).pipe(
      Effect.flatMap(requireRequest),
    );
    yield* peer.send({
      to: group,
      collectiveResponse: {
        action: "decline",
        operationId: peerRequest.message.content.operationId,
      },
    });
    const answered = yield* messageTool("reply", {
      to: group,
      message: acceptText("tue", peerRequest.message.content.operationId),
    });
    expect(answered.details).toMatchObject({ ok: true });
    yield* assertReceivedAllGather(
      context,
      peerRequest.message.content.operationId,
    );
  });
}

function sendAllGather(context: AllGatherContext) {
  const { caller, peer, group, turns, messageTool } = context;
  return Effect.gen(function* () {
    yield* sendQuestion(messageTool, group, "all_gather");
    const callerRequest = yield* nextItem(caller.messages).pipe(
      Effect.flatMap(requireRequest),
    );
    const peerRequest = yield* nextItem(peer.messages).pipe(
      Effect.flatMap(requireRequest),
    );
    yield* caller.send({
      to: group,
      collectiveResponse: {
        action: "accept",
        operationId: callerRequest.message.content.operationId,
        content: { slot: "mon" },
      },
    });
    yield* peer.send({
      to: group,
      collectiveResponse: {
        action: "decline",
        operationId: peerRequest.message.content.operationId,
      },
    });
    expect(yield* nextTurn(turns)).toMatchObject({
      Body: `all_gather result for the question sent to ${group}: "${GATHER_QUESTION}"\n- ${context.callerAddress}: answered {"slot":"mon"}\n- ${context.peerAddress}: declined`,
      SenderName: "Social Harness",
    });
    const memberResult = yield* nextItem(caller.messages);
    expect(memberResult).toMatchObject({
      kind: "collectiveResult",
      operationId: callerRequest.message.content.operationId,
      to: group,
      outcomes: [
        {
          member: context.callerAgent,
          outcome: { kind: "answered", content: { slot: "mon" } },
        },
        { member: context.peerAgent, outcome: { kind: "declined" } },
      ],
    });
    expect(yield* nextItem(peer.messages)).toEqual(memberResult);
  });
}

function sendGather(
  context: Pick<
    AllGatherContext,
    | "caller"
    | "callerAddress"
    | "targetAddress"
    | "targetAgent"
    | "turns"
    | "messageTool"
  >,
) {
  const { caller, callerAddress, messageTool } = context;
  return Effect.gen(function* () {
    const started = yield* messageTool("send", {
      to: callerAddress,
      message: gatherText("gather"),
    });
    const request = yield* nextItem(caller.messages).pipe(
      Effect.flatMap(requireRequest),
    );
    expect(started.details).toEqual({
      ok: true,
      to: callerAddress,
    });
    expect(request).toMatchObject({
      kind: "collectiveRequest",
      message: {
        author: context.targetAgent,
        content: { question: GATHER_QUESTION, requestedSchema: SLOT_SCHEMA },
      },
    });
    yield* caller.send({
      to: context.targetAddress,
      collectiveResponse: {
        action: "accept",
        operationId: request.message.content.operationId,
        content: { slot: "mon" },
      },
    });
    const resultTurn = yield* nextTurn(context.turns);
    expect(resultTurn).toMatchObject({
      Body: `gather result for the question sent to ${callerAddress}: "${GATHER_QUESTION}"\n- ${callerAddress}: answered {"slot":"mon"}`,
      SenderName: "Social Harness",
    });
  });
}

function assertReply(
  returned: InboundDelivery,
  targetAgent: AgentView,
  callerAgent: AgentView,
): void {
  expect(returned.item).toMatchObject({
    kind: "multicast",
    message: {
      author: targetAgent,
      recipients: [callerAgent],
      content: [{ type: "text", text: OPENCLAW_REPLY }],
    },
  });
}

function startQueuedGateway(
  scenario: Effect.Effect.Success<ReturnType<typeof acquireScenario>>,
  channelPlugin: Effect.Effect.Success<
    ReturnType<typeof registerOpenClawChannel>
  >,
) {
  return Effect.gen(function* () {
    const turns = yield* Queue.unbounded<OpenClawInboundContext>();
    const gateway = yield* startOpenClawGateway(
      scenario.target,
      channelPlugin,
      makeTurnQueueRuntime(turns),
    );
    return { turns, ...gateway };
  });
}

function assertReceivedAllGather(context: AllGatherContext, requestId: string) {
  const {
    caller,
    peer,
    peerAddress,
    targetAddress,
    peerAgent,
    targetAgent,
    group,
    turns,
  } = context;
  return Effect.gen(function* () {
    const callerResult = yield* nextItem(caller.messages);
    expect(callerResult).toMatchObject({
      kind: "collectiveResult",
      operationId: requestId,
      to: group,
      outcomes: [
        { member: peerAgent, outcome: { kind: "declined" } },
        {
          member: targetAgent,
          outcome: { kind: "answered", content: { slot: "tue" } },
        },
      ],
    });
    expect(yield* nextItem(peer.messages)).toEqual(callerResult);
    expect(yield* nextTurn(turns)).toMatchObject({
      Body: `all_gather result for the question sent to ${group}: "${GATHER_QUESTION}"\n- ${peerAddress}: declined\n- ${targetAddress}: answered {"slot":"tue"}`,
      ChatType: "group",
      SenderName: "Social Harness",
    });
  });
}

function sendQuestion(
  messageTool: ReturnType<typeof makeMessageTool>,
  to: string,
  operation: "gather" | "all_gather",
) {
  return messageTool("send", { to, message: gatherText(operation) }).pipe(
    Effect.tap((started) =>
      Effect.sync(() => {
        expect(started.details).toEqual({ ok: true, to });
      }),
    ),
  );
}

it(
  "keeps OpenClaw host identities local across a durable exchange",
  processTrace(runOpenClawScenario()),
  300_000,
);

it(
  "runs a gather in both directions through the OpenClaw message tool",
  processTrace(runOpenClawGatherScenario()),
  300_000,
);

it(
  "runs an all_gather in both directions through the OpenClaw message tool",
  processTrace(runOpenClawAllGatherScenario()),
  300_000,
);
