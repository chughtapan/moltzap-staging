/**
 * @file One daemon whose agents message an OpenClaw host, and the OpenClaw
 * host contract the channel plugin runs against.
 */

import type { PermittedItem } from "@social-harness/social-harness";
import openClawPlugin from "@social-harness/openclaw-channel";
import { Deferred, Duration, Effect, Queue, type Scope } from "effect";
import { vi } from "vitest";
import { joinParticipants, type Participant } from "../agent-fixture.js";
import {
  type Daemon,
  type DaemonOptions,
  ProcessTestError,
  sharedDaemon,
  type SignedIn,
} from "../daemon/daemon-fixture.js";

/**
 * A running daemon, the agent the OpenClaw host connects as, and two agents
 * that message it.
 */
export interface Scenario {
  readonly daemon: Daemon;
  readonly target: SignedIn;
  readonly caller: Participant;
  readonly peer: Participant;
}

interface OpenClawConfig {
  readonly channels: {
    readonly sharness: {
      readonly accounts: readonly [
        {
          readonly id: string;
          readonly mode?: "shared" | "private";
        },
      ];
    };
  };
}

interface OpenClawBuildContextInput {
  readonly channel: string;
  readonly accountId: string;
  readonly provider: string;
  readonly surface: string;
  readonly messageId: string;
  readonly from: string;
  readonly sender: { readonly id: string; readonly name: string };
  readonly conversation: {
    readonly kind: "direct" | "group";
    readonly id: string;
    readonly label: string;
  };
  readonly route: {
    readonly routeSessionKey: string;
    readonly mainSessionKey: string;
  };
  readonly reply: { readonly to: string; readonly originatingTo: string };
  readonly message: {
    readonly body: string;
    readonly rawBody: string;
    readonly bodyForAgent: string;
    readonly commandBody: string;
  };
  // eslint-disable-next-line @typescript-eslint/naming-convention -- OpenClaw defines the GroupMembers host context key.
  readonly extra?: { readonly GroupMembers?: string };
}

/* eslint-disable @typescript-eslint/naming-convention -- OpenClaw defines the inbound host context keys. */
/** Models the host context keys consumed by the adapter acceptance scenarios. */
interface OpenClawInboundContext {
  readonly AccountId: string;
  readonly Body: string;
  readonly BodyForAgent: string;
  readonly BodyForCommands: string;
  readonly ChatId: string;
  readonly ChatType: "direct" | "group";
  readonly CommandAuthorized: boolean;
  readonly CommandBody: string;
  readonly From: string;
  readonly GroupMembers?: string;
  readonly GroupSubject?: string;
  readonly InboundEventKind: "message";
  readonly MessageSid: string;
  readonly OriginatingTo: string;
  readonly Provider: string;
  readonly RawBody: string;
  readonly SenderId: string;
  readonly SenderName: string;
  readonly SessionKey: string;
  readonly Surface: string;
  readonly To: string;
}
/* eslint-enable @typescript-eslint/naming-convention -- Restore repository naming outside the OpenClaw host context shapes. */

interface OpenClawInboundTurnInput {
  readonly cfg: OpenClawConfig;
  readonly channel: string;
  readonly accountId: string;
  readonly route: {
    readonly agentId: string;
    readonly sessionKey: string;
  };
  readonly ctxPayload: OpenClawInboundContext;
  readonly delivery: {
    readonly deliver: (
      payload: { readonly text: string },
      context: { readonly kind: "final" },
      // eslint-disable-next-line agent-code-guard/promise-type -- OpenClaw host callbacks require promises at this boundary.
    ) => Promise<{ readonly visibleReplySent: boolean }>;
  };
  readonly record?: {
    readonly updateLastRoute?: {
      readonly sessionKey: string;
      readonly channel: string;
      readonly to: string;
      readonly accountId: string;
    };
  };
}

/** The plugin's host turn, as far as this test reads it. */
interface OpenClawHostTurn {
  readonly id: string;
}

interface OpenClawInboundRunnerInput {
  readonly channel: string;
  readonly accountId: string;
  readonly raw: { readonly turn: OpenClawHostTurn };
  readonly adapter: {
    readonly ingest: () => {
      readonly id: string;
      readonly rawText: string;
      readonly textForAgent: string;
      readonly textForCommands: string;
      readonly raw: OpenClawHostTurn;
    };
    readonly resolveTurn: () => OpenClawInboundTurnInput;
  };
}

/**
 * One turn as the stand-in host ran it: the plugin's plan for the turn, the
 * event the plugin ingested for it and the raw turn the plugin handed over.
 */
export interface OpenClawRunTurn extends OpenClawInboundTurnInput {
  readonly event: ReturnType<OpenClawInboundRunnerInput["adapter"]["ingest"]>;
  readonly raw: OpenClawHostTurn;
}

interface OpenClawRouteInput {
  readonly accountId?: string | null;
  readonly peer?: { readonly kind: string; readonly id: string };
}

interface ObservedOpenClawAccountRuntime {
  readonly channel: {
    readonly runtimeContexts: object;
    readonly inbound: {
      readonly buildContext: (
        input: OpenClawBuildContextInput,
      ) => OpenClawInboundContext;
      // eslint-disable-next-line agent-code-guard/promise-type -- OpenClaw host callbacks require promises at this boundary.
      readonly run: (input: OpenClawInboundRunnerInput) => Promise<object>;
    };
    readonly routing: {
      readonly resolveAgentRoute: (input: OpenClawRouteInput) => {
        readonly agentId: string;
        readonly channel: string;
        readonly accountId: string;
        readonly sessionKey: string;
        readonly mainSessionKey: string;
        readonly lastRoutePolicy: "session";
        readonly matchedBy: "default";
      };
    };
  };
}

interface OpenClawGatewayStatus {
  readonly accountId: string;
  readonly connected?: boolean;
  readonly running?: boolean;
  readonly lastConnectedAt?: number;
}

interface OpenClawGatewayContext {
  readonly cfg: OpenClawConfig;
  readonly accountId: string;
  readonly account: {
    readonly id: string;
    readonly mode?: "shared" | "private";
  };
  readonly abortSignal: AbortSignal;
  readonly runtime: {
    readonly log: (message: string) => void;
    readonly error: (message: string) => void;
    readonly exit: (code: number) => void;
  };
  readonly channelRuntime: ObservedOpenClawAccountRuntime["channel"];
  readonly log?: {
    readonly info: (message: string) => void;
    readonly warn: (message: string) => void;
    readonly error: (message: string) => void;
  };
  readonly getStatus: () => OpenClawGatewayStatus;
  readonly setStatus: (status: OpenClawGatewayStatus) => void;
}

/** Carries the host message-tool action at the external plugin boundary. */
export interface OpenClawMessageActionContext {
  readonly channel: string;
  readonly action: "send" | "reply";
  readonly cfg: OpenClawConfig;
  readonly accountId: string;
  readonly params: {
    readonly to: string;
    readonly message: string;
    readonly targets?: readonly string[];
  };
}

interface OpenClawMessageActionResult {
  readonly details: unknown;
}

interface StableOpenClawChannelPlugin {
  readonly gateway: {
    readonly startAccount: (
      context: OpenClawGatewayContext,
      // eslint-disable-next-line agent-code-guard/promise-type -- OpenClaw host callbacks require promises at this boundary.
    ) => Promise<undefined>;
  };
  readonly actions: {
    readonly handleAction: (
      context: OpenClawMessageActionContext,
      // eslint-disable-next-line agent-code-guard/promise-type -- OpenClaw host callbacks require promises at this boundary.
    ) => Promise<OpenClawMessageActionResult>;
  };
}

interface StableOpenClawPluginApi {
  readonly runtime: object;
  registerChannel(registration: object): void;
}

interface StableOpenClawEntry {
  register(api: StableOpenClawPluginApi): void;
}

/** Bounds waits for daemon delivery and host gateway shutdown. */
export const DELIVERY_TIMEOUT = Duration.seconds(60);

/** Keeps the host account identity distinct from daemon agent identities. */
export const OPENCLAW_ACCOUNT_ID = "adapter-target";

/** Pins host session routing while daemon addresses stay on the wire. */
export const OPENCLAW_MAIN_SESSION_KEY = "agent:primary:main";

const acquireScenario = (
  options: DaemonOptions,
  signInHost: (
    daemon: Daemon,
  ) => Effect.Effect<SignedIn, ProcessTestError, Scope.Scope>,
): Effect.Effect<Scenario, ProcessTestError, Scope.Scope> =>
  Effect.gen(function* () {
    const daemon = yield* sharedDaemon(options);
    yield* daemon.start();
    const target = yield* signInHost(daemon);
    const [caller, peer] = yield* joinParticipants(daemon, ["caller", "peer"]);
    if (caller === undefined || peer === undefined) {
      return yield* Effect.dieMessage("expected two participants");
    }
    return { daemon, target, caller, peer };
  }).pipe(Effect.withSpan("acquireScenario"));

/** A P2P daemon, where the host's agent and the two others are peers. */
export const acquirePeers = acquireScenario({}, (daemon) =>
  daemon.signInAs("openclaw-host"),
);

/**
 * A client/server daemon whose server agent is the host's and runs the marked
 * message policy. The two others are clients, whose messages only the server
 * agent's firewall checks.
 */
export const acquireGuardedServer = acquireScenario(
  {
    firewall: true,
    settings: {
      mode: { kind: "client-server", serverAgentName: "openclaw-host" },
    },
  },
  (daemon) => daemon.signIn(daemon.serverAgentBearer),
);

/** Loads the real plugin and validates its host-facing registration hooks. */
export function registerOpenClawChannel(): Effect.Effect<
  StableOpenClawChannelPlugin,
  ProcessTestError
> {
  return Effect.try({
    try: () => {
      let registered: StableOpenClawChannelPlugin | null = null;
      const api: StableOpenClawPluginApi = {
        runtime: {},
        registerChannel(registration) {
          if (
            !("plugin" in registration) ||
            typeof registration.plugin !== "object" ||
            registration.plugin === null ||
            !isStableOpenClawChannelPlugin(registration.plugin)
          ) {
            throw new ProcessTestError({
              message: "OpenClaw registered an invalid channel plugin",
            });
          }
          registered = registration.plugin;
        },
      };
      if (!isStableOpenClawEntry(openClawPlugin)) {
        throw new ProcessTestError({
          message: "OpenClaw loader entry has no registration hook",
        });
      }
      openClawPlugin.register(api);
      if (registered === null) {
        throw new ProcessTestError({
          message: "OpenClaw did not register the Social Harness channel",
        });
      }
      return registered;
    },
    catch: (cause) =>
      new ProcessTestError({
        message: "OpenClaw channel registration failed",
        cause,
      }),
  });
}

/**
 * Starts the registered plugin's account as the scenario's target agent, with
 * OpenClaw's gateway context, and waits until it reports connected.
 */
export function startOpenClawGateway(
  scenario: Pick<Scenario, "daemon" | "target">,
  channelPlugin: StableOpenClawChannelPlugin,
  runtime: ObservedOpenClawAccountRuntime,
  options: {
    readonly mode?: "shared" | "private";
    readonly logLines?: string[];
  } = {},
) {
  return Effect.gen(function* () {
    const cfg = openClawConfig(options.mode);
    const connected = yield* Deferred.make<undefined>();
    yield* useDaemonEndpoint(scenario);

    const abortController = new AbortController();
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        abortController.abort();
      }),
    );
    let status: OpenClawGatewayStatus = {
      accountId: OPENCLAW_ACCOUNT_ID,
    };
    const gatewayContext: OpenClawGatewayContext = {
      cfg,
      accountId: OPENCLAW_ACCOUNT_ID,
      account: {
        id: OPENCLAW_ACCOUNT_ID,
        ...(options.mode === undefined ? {} : { mode: options.mode }),
      },
      abortSignal: abortController.signal,
      runtime: {
        log: () => {},
        error: () => {},
        exit: () => {},
      },
      channelRuntime: runtime.channel,
      ...(options.logLines === undefined
        ? {}
        : {
            log: {
              info: (message: string) =>
                options.logLines?.push(`info: ${message}`),
              warn: (message: string) =>
                options.logLines?.push(`warn: ${message}`),
              error: (message: string) =>
                options.logLines?.push(`error: ${message}`),
            },
          }),
      getStatus: () => status,
      setStatus: (next) => {
        status = next;
        if (next.connected === true) {
          Effect.runSync(Deferred.succeed(connected, undefined));
        }
      },
    };
    const runningGateway = yield* effectFromPromise(
      "OpenClaw gateway start",
      () => channelPlugin.gateway.startAccount(gatewayContext),
    ).pipe(Effect.forkScoped);
    yield* awaitSignal(connected, "OpenClaw gateway connection");
    return { abortController, runningGateway };
  }).pipe(Effect.withSpan("startOpenClawGateway"));
}

/** An OpenClaw configuration with one Social Harness account. */
export function openClawConfig(mode?: "shared" | "private"): OpenClawConfig {
  return {
    channels: {
      sharness: {
        accounts: [
          { id: OPENCLAW_ACCOUNT_ID, ...(mode === undefined ? {} : { mode }) },
        ],
      },
    },
  };
}

/**
 * An account runtime that hands every turn it runs to the test, standing in
 * for the model that reads it, and completes the turn at once.
 */
export function makeTurnQueueRuntime(
  turns: Queue.Queue<OpenClawRunTurn>,
): ObservedOpenClawAccountRuntime {
  return {
    channel: {
      runtimeContexts: {},
      inbound: {
        buildContext: buildOpenClawContext,
        run: (input) =>
          Effect.runPromise(
            Queue.offer(turns, {
              ...input.adapter.resolveTurn(),
              event: input.adapter.ingest(),
              raw: input.raw.turn,
            }).pipe(Effect.as({})),
          ),
      },
      routing: {
        resolveAgentRoute: (input) => ({
          agentId: "primary",
          channel: "sharness",
          accountId: input.accountId ?? OPENCLAW_ACCOUNT_ID,
          sessionKey:
            input.peer === undefined
              ? OPENCLAW_MAIN_SESSION_KEY
              : `agent:primary:sharness:${input.peer.kind}:${input.peer.id}`,
          mainSessionKey: OPENCLAW_MAIN_SESSION_KEY,
          lastRoutePolicy: "session",
          matchedBy: "default",
        }),
      },
    },
  };
}

/** Requires a host turn before the process-fixture deadline. */
export function nextTurn(turns: Queue.Queue<OpenClawRunTurn>) {
  return Queue.take(turns).pipe(
    Effect.timeoutFail({
      duration: DELIVERY_TIMEOUT,
      onTimeout: () =>
        new ProcessTestError({
          message: "timed out awaiting an OpenClaw turn",
        }),
    }),
  );
}

/** Fails the scenario when an expected collective request has another item kind. */
export function requireRequest(item: PermittedItem) {
  return item.kind === "collectiveRequest"
    ? Effect.succeed(item)
    : Effect.fail(
        new ProcessTestError({
          message: `expected a collective request, received ${item.kind}`,
        }),
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

/** Fails with the named host event when its bounded wait expires. */
function awaitSignal(
  signal: Deferred.Deferred<undefined>,
  description: string,
) {
  return Deferred.await(signal).pipe(
    Effect.timeoutFail({
      duration: DELIVERY_TIMEOUT,
      onTimeout: () =>
        new ProcessTestError({ message: `timed out awaiting ${description}` }),
    }),
  );
}

function isStableOpenClawEntry(value: unknown): value is StableOpenClawEntry {
  return (
    typeof value === "object" &&
    value !== null &&
    "register" in value &&
    typeof value.register === "function"
  );
}

function buildOpenClawContext(
  input: OpenClawBuildContextInput,
): OpenClawInboundContext {
  return {
    AccountId: input.accountId,
    Body: input.message.body,
    BodyForAgent: input.message.bodyForAgent,
    BodyForCommands: input.message.commandBody,
    ChatId: input.conversation.id,
    ChatType: input.conversation.kind,
    CommandAuthorized: false,
    CommandBody: input.message.commandBody,
    From: input.from,
    InboundEventKind: "message",
    MessageSid: input.messageId,
    OriginatingTo: input.reply.originatingTo,
    Provider: input.provider,
    RawBody: input.message.rawBody,
    SenderId: input.sender.id,
    SenderName: input.sender.name,
    SessionKey: input.route.routeSessionKey,
    Surface: input.surface,
    To: input.reply.to,
    ...(input.extra?.GroupMembers === undefined
      ? {}
      : {
          GroupMembers: input.extra.GroupMembers,
          GroupSubject: input.conversation.label,
        }),
  };
}

function isStableOpenClawChannelPlugin(
  value: object,
): value is StableOpenClawChannelPlugin {
  return hasGateway(value) && hasActions(value);
}

function hasGateway(value: object): boolean {
  if (
    !("gateway" in value) ||
    typeof value.gateway !== "object" ||
    value.gateway === null
  ) {
    return false;
  }
  return (
    "startAccount" in value.gateway &&
    typeof value.gateway.startAccount === "function"
  );
}

function hasActions(value: object): boolean {
  if (
    !("actions" in value) ||
    typeof value.actions !== "object" ||
    value.actions === null
  ) {
    return false;
  }
  return (
    "handleAction" in value.actions &&
    typeof value.actions.handleAction === "function"
  );
}

/**
 * Points the plugin, which reads this process's environment, at the daemon
 * with the target agent's credential until the scope closes.
 */
function useDaemonEndpoint({
  daemon,
  target,
}: Pick<Scenario, "daemon" | "target">) {
  return Effect.gen(function* () {
    vi.stubEnv("SHARNESS_MCP_URL", daemon.endpoint.href);
    vi.stubEnv("SHARNESS_MCP_CREDENTIAL_FILE", target.credentialFile);

    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        vi.unstubAllEnvs();
      }),
    );
  });
}
