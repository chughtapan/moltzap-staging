/**
 * @file One daemon whose agents message an OpenClaw host. The channel plugin
 * runs in this process with OpenClaw's own route resolver and context builder,
 * and a stand-in for the model queues each turn for the test.
 */

import type {
  ChannelPlugin,
  OpenClawConfig,
} from "openclaw/plugin-sdk/channel-core";
import openClawPlugin from "@social-harness/openclaw-channel";
import { Deferred, Duration, Effect, Fiber, Queue, type Scope } from "effect";
import {
  buildChannelInboundEventContext,
  type ChannelInboundTurnPlan,
} from "openclaw/plugin-sdk/channel-inbound";
import { resolveAgentRoute } from "openclaw/plugin-sdk/routing";
import { vi } from "vitest";
import {
  type Daemon,
  type DaemonOptions,
  joinParticipants,
  type Participant,
  ProcessTestError,
  SERVER_AGENT_OWNER,
  sharedDaemon,
  type SignedIn,
} from "./daemon-fixture.js";

/**
 * A running daemon, the agent the OpenClaw host connects as, and two agents
 * that message it.
 */
interface Scenario {
  readonly daemon: Daemon;
  readonly target: SignedIn;
  readonly caller: Participant;
  readonly peer: Participant;
}

/** How the host's account routes turns: all to the agent's main session, or each peer's to a session of its own. */
type Mode = "shared" | "private";

/** What the stand-in host reads of the turn the plugin hands OpenClaw's runner. */
interface RunnerInput {
  readonly adapter: { readonly resolveTurn: () => ChannelInboundTurnPlan };
}

/** The part of OpenClaw's plugin entry and registration API the channel uses. */
interface PluginEntry {
  register(api: {
    readonly runtime: object;
    registerChannel(registration: { readonly plugin: ChannelPlugin }): void;
  }): void;
}

const CHANNEL = "sharness";

/** Bounds waits for daemon delivery and host gateway shutdown. */
const DELIVERY_TIMEOUT = Duration.seconds(60);

/** Keeps the host account identity distinct from daemon agent identities. */
export const OPENCLAW_ACCOUNT_ID = "adapter-target";

/** The session OpenClaw gives the account's agent for every conversation in shared mode. */
export const OPENCLAW_MAIN_SESSION_KEY = resolveAgentRoute({
  cfg: openClawConfig("shared"),
  channel: CHANNEL,
  accountId: OPENCLAW_ACCOUNT_ID,
}).mainSessionKey;

const timedOut = (awaited: string) => ({
  duration: DELIVERY_TIMEOUT,
  onTimeout: () =>
    new ProcessTestError({ message: `timed out awaiting ${awaited}` }),
});

/**
 * A running daemon prepared with `options`, the agent `signInHost` signs in
 * for the OpenClaw host, and the agents of the owners `caller` and `peer`.
 */
export const acquireScenario = Effect.fn("acquireScenario")(function* (
  options: DaemonOptions,
  signInHost: (
    daemon: Daemon,
  ) => Effect.Effect<SignedIn, ProcessTestError, Scope.Scope>,
) {
  const daemon = yield* sharedDaemon(options);
  yield* daemon.start();
  const target = yield* signInHost(daemon);
  const [caller, peer] = yield* joinParticipants(daemon, ["caller", "peer"]);
  if (caller === undefined || peer === undefined) {
    return yield* Effect.dieMessage("expected two participants");
  }
  const scenario: Scenario = { daemon, target, caller, peer };
  return scenario;
});

/** A daemon whose server agent is the host's. The two others are clients. */
export const acquireServer = acquireScenario({}, (daemon) =>
  daemon.signInAs(SERVER_AGENT_OWNER),
);

/**
 * Registers the plugin as OpenClaw loads it and starts its account as the
 * scenario's target agent, which the plugin reads from this process's
 * environment. The stand-in model queues each turn in `turns` and completes
 * it at once, or fails it when `failTurns` is set. `messageTool` sends as the
 * model's message tool does.
 */
export const startQueuedGateway = Effect.fn("startQueuedGateway")(function* (
  scenario: Scenario,
  options: { readonly mode?: Mode; readonly failTurns?: boolean } = {},
) {
  const { mode = "shared", failTurns = false } = options;
  const cfg = openClawConfig(mode);
  const plugin = yield* registeredChannel();
  const startAccount = yield* Effect.fromNullable(plugin.gateway?.startAccount);
  const handleAction = yield* Effect.fromNullable(plugin.actions?.handleAction);
  const turns = yield* Queue.unbounded<ChannelInboundTurnPlan>();
  const connected = yield* Deferred.make<undefined>();
  const abortController = new AbortController();
  vi.stubEnv("SHARNESS_MCP_URL", scenario.daemon.endpoint.href);
  vi.stubEnv("SHARNESS_MCP_CREDENTIAL_FILE", scenario.target.credentialFile);
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      abortController.abort();
      vi.unstubAllEnvs();
    }),
  );
  let status = { accountId: OPENCLAW_ACCOUNT_ID };
  const runningGateway = yield* effectFromPromise(
    "OpenClaw gateway start",
    () =>
      startAccount({
        cfg,
        accountId: OPENCLAW_ACCOUNT_ID,
        account: { id: OPENCLAW_ACCOUNT_ID, mode },
        abortSignal: abortController.signal,
        runtime: { log: () => {}, error: () => {}, exit: () => {} },
        channelRuntime: {
          runtimeContexts: {
            register: () => ({ dispose: () => {} }),
            get: () => undefined,
            watch: () => () => {},
          },
          inbound: {
            buildContext: buildChannelInboundEventContext,
            run: (input: RunnerInput) =>
              Effect.runPromise(
                Queue.offer(turns, input.adapter.resolveTurn()).pipe(
                  Effect.zipRight(
                    failTurns
                      ? Effect.fail(
                          new ProcessTestError({ message: "host turn failed" }),
                        )
                      : Effect.succeed({}),
                  ),
                ),
              ),
          },
          routing: { resolveAgentRoute },
        },
        getStatus: () => status,
        setStatus: (next) => {
          status = next;
          if (next.connected === true) {
            Effect.runSync(Deferred.succeed(connected, undefined));
          }
        },
      }),
  ).pipe(Effect.forkScoped);
  yield* Deferred.await(connected).pipe(
    Effect.timeoutFail(timedOut("the OpenClaw gateway connection")),
  );
  return {
    ...scenario,
    turns,
    gateway: { abortController, runningGateway },
    messageTool: (
      action: "send" | "reply",
      params: Readonly<Record<string, unknown>>,
    ) =>
      effectFromPromise(`OpenClaw message tool ${action}`, () =>
        handleAction({
          channel: CHANNEL,
          action,
          cfg,
          accountId: OPENCLAW_ACCOUNT_ID,
          params: { ...params },
        }),
      ),
  };
});

/** A scenario whose host hands every turn to the test and sends through the message tool. */
export type HostedScenario = Effect.Effect.Success<
  ReturnType<typeof startQueuedGateway>
>;

/** Stops a started gateway and waits until it has stopped. */
export function stopGateway({ gateway }: HostedScenario) {
  gateway.abortController.abort();
  return Fiber.join(gateway.runningGateway).pipe(
    Effect.timeoutFail(timedOut("the OpenClaw gateway to stop")),
  );
}

/** The next turn the host ran. */
export function nextTurn({ turns }: HostedScenario) {
  return Queue.take(turns).pipe(
    Effect.timeoutFail(timedOut("an OpenClaw turn")),
  );
}

/** Maps external host callback failures into the process-fixture error channel. */
export function effectFromPromise<A>(
  operation: string,
  evaluate: () => PromiseLike<A>,
): Effect.Effect<A, ProcessTestError> {
  return Effect.tryPromise({
    try: evaluate,
    catch: (cause) =>
      new ProcessTestError({
        message: `${operation} failed: ${cause instanceof Error ? cause.message : String(cause)}`,
        cause,
      }),
  });
}

/** An OpenClaw configuration with one Social Harness account. */
function openClawConfig(mode: Mode): OpenClawConfig {
  return {
    channels: { sharness: { accounts: [{ id: OPENCLAW_ACCOUNT_ID, mode }] } },
    ...(mode === "private"
      ? { session: { dmScope: "per-account-channel-peer" } }
      : {}),
  };
}

/** The channel plugin the package's entry registers with OpenClaw. */
function registeredChannel() {
  const registered: ChannelPlugin[] = [];
  if (isPluginEntry(openClawPlugin)) {
    openClawPlugin.register({
      runtime: {},
      registerChannel: ({ plugin }) => {
        registered.push(plugin);
      },
    });
  }
  return Effect.fromNullable(registered[0]);
}

function isPluginEntry(value: object): value is PluginEntry {
  return "register" in value && typeof value.register === "function";
}
