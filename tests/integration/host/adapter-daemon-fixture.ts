/** @file Real-daemon fixtures and the OpenClaw host contract used by adapter acceptance. */

import openClawPlugin from "@moltzap/openclaw-channel";
import {
  AgentAddress,
  type AgentView,
  type Content,
  GroupAddress,
  type InboundDelivery,
  type InboundItem,
} from "@moltzap/social-harness";
import {
  Deferred,
  Duration,
  Effect,
  Option,
  Queue,
  Schema,
  type Scope,
  Stream,
} from "effect";
import { expect, vi } from "vitest";
import {
  acquireDaemonManagementClient,
  acquireDaemonProcess,
  acquireProcessInfrastructure,
  type DaemonProcessFixture,
  makeDaemonProcessFixture,
  makeRegistrationRequest,
  ProcessTestError,
} from "../daemon/daemon-process-harness.js";
import { registrationResultKind } from "../expected-contracts.js";
import { historyMessage } from "../saved-history.js";

interface RegisteredFixture extends DaemonProcessFixture {
  readonly agent: AgentView;
}

interface Scenario {
  readonly caller: RegisteredFixture;
  readonly target: RegisteredFixture;
}

interface GroupScenario extends Scenario {
  readonly peer: RegisteredFixture;
}

interface RecordedSessionSnapshot {
  readonly messageIds: readonly string[];
}

/** Records host-local message identity without exposing session keys to peers. */
export class RecordedSessionStore {
  private readonly messagesBySession = new Map<string, string[]>();

  acceptMessage(sessionKey: string, messageId: string): void {
    const messageIds = this.messagesBySession.get(sessionKey) ?? [];
    if (!messageIds.includes(messageId)) {
      messageIds.push(messageId);
    }
    this.messagesBySession.set(sessionKey, messageIds);
  }

  keys(): readonly string[] {
    return [...this.messagesBySession.keys()];
  }

  snapshot(sessionKey: string): RecordedSessionSnapshot {
    return {
      messageIds: [...(this.messagesBySession.get(sessionKey) ?? [])],
    };
  }
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
  readonly session: { readonly store: string };
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
export interface OpenClawInboundContext {
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

interface OpenClawRecordInput {
  readonly ctx: OpenClawInboundContext;
  readonly sessionKey: string;
  readonly storePath: string;
}

interface OpenClawDeliveryResult {
  readonly visibleReplySent: boolean;
}

interface OpenClawReplyDispatcherInput {
  readonly ctx: OpenClawInboundContext;
  readonly dispatcherOptions: {
    readonly deliver: (
      payload: { readonly text: string },
      context: { readonly kind: "final" },
      // eslint-disable-next-line agent-code-guard/promise-type -- OpenClaw host callbacks require promises at this boundary.
    ) => Promise<OpenClawDeliveryResult>;
  };
  readonly replyOptions?: object;
}

interface OpenClawReplyResult {
  readonly queuedFinal: false;
  readonly counts: {
    readonly tool: number;
    readonly block: number;
    readonly final: number;
  };
  readonly sourceReplyDeliveryMode: "message_tool_only";
}

interface OpenClawInboundTurnInput {
  readonly cfg: OpenClawConfig;
  readonly channel: string;
  readonly accountId: string;
  readonly route: {
    readonly agentId: string;
    readonly sessionKey: string;
  };
  readonly ctxPayload: OpenClawInboundContext;
  readonly delivery: OpenClawReplyDispatcherInput["dispatcherOptions"];
  readonly record?: {
    readonly updateLastRoute?: {
      readonly sessionKey: string;
      readonly channel: string;
      readonly to: string;
      readonly accountId: string;
    };
  };
  readonly replyOptions?: OpenClawReplyDispatcherInput["replyOptions"];
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

interface OpenClawReplyFixture {
  readonly callerAddress: string;
  readonly callerId: string;
  readonly responseSent: Deferred.Deferred<undefined>;
  readonly sessions: RecordedSessionStore;
  /** The plugin's `message` tool path, the only way a reply becomes a message. */
  // eslint-disable-next-line agent-code-guard/promise-type -- OpenClaw host callbacks require promises at this boundary.
  readonly sendReply: () => Promise<OpenClawMessageActionResult>;
}

type OpenClawRuntimeFixture = OpenClawReplyFixture;

/** Bounds waits for daemon delivery and host gateway shutdown. */
export const DELIVERY_TIMEOUT = Duration.seconds(60);

/** Keeps the host account identity distinct from daemon agent identities. */
export const OPENCLAW_ACCOUNT_ID = "adapter-target";

/** Pins host session routing while daemon addresses stay on the wire. */
export const OPENCLAW_MAIN_SESSION_KEY = "agent:primary:main";

/** Connects the host reply action to its durable daemon history assertions. */
export const OPENCLAW_REPLY = "reply from the real OpenClaw adapter";

/** The greeting shared by the caller and the host-side reply assertion. */
export const OPENCLAW_INITIAL_TEXT = "hello through the real OpenClaw adapter";

/** Ordinary messages retain their text through certification and delivery. */
export function multicastContent(text: string): Content {
  return [{ type: "text", text }];
}

/** Canonicalizes group membership before decoding the wire address. */
export function groupAddress(agentNames: readonly string[]) {
  return Schema.decodeUnknownSync(GroupAddress)(
    `group:${[...agentNames].sort((left, right) => left.localeCompare(right)).join(",")}`,
  );
}

/** Starts and registers two isolated daemon endpoints under the caller’s scope. */
export function acquireScenario(
  prefix: string,
  firewall?: "guidance" | "enforcement",
): Effect.Effect<Scenario, ProcessTestError, Scope.Scope> {
  return Effect.gen(function* () {
    const infrastructure = yield* acquireProcessInfrastructure;
    const [caller, target] = yield* Effect.all(
      [
        makeDaemonProcessFixture(infrastructure, `${prefix}-caller`),
        makeDaemonProcessFixture(infrastructure, `${prefix}-target`, firewall),
      ] as const,
      { concurrency: 2 },
    );
    yield* Effect.all(
      [acquireDaemonProcess(caller), acquireDaemonProcess(target)] as const,
      { concurrency: 2 },
    );
    const [registeredCaller, registeredTarget] = yield* Effect.all(
      [registerFixture(caller), registerFixture(target)] as const,
      { concurrency: 2 },
    );
    return { caller: registeredCaller, target: registeredTarget };
  }).pipe(Effect.withSpan("acquireScenario"));
}

/** Starts three registered daemons so all-gather exercises multiple remote members. */
export function acquireGroupScenario(
  prefix: string,
  firewall?: "guidance" | "enforcement",
): Effect.Effect<GroupScenario, ProcessTestError, Scope.Scope> {
  return Effect.gen(function* () {
    const infrastructure = yield* acquireProcessInfrastructure;
    const [caller, target, peer] = yield* Effect.all(
      [
        makeDaemonProcessFixture(infrastructure, `${prefix}-caller`),
        makeDaemonProcessFixture(infrastructure, `${prefix}-target`, firewall),
        makeDaemonProcessFixture(infrastructure, `${prefix}-peer`),
      ] as const,
      { concurrency: 3 },
    );
    yield* Effect.all(
      [
        acquireDaemonProcess(caller),
        acquireDaemonProcess(target),
        acquireDaemonProcess(peer),
      ] as const,
      { concurrency: 3 },
    );
    const [registeredCaller, registeredTarget, registeredPeer] =
      yield* Effect.all(
        [
          registerFixture(caller),
          registerFixture(target),
          registerFixture(peer),
        ] as const,
        { concurrency: 3 },
      );
    return {
      caller: registeredCaller,
      target: registeredTarget,
      peer: registeredPeer,
    };
  }).pipe(Effect.withSpan("acquireGroupScenario"));
}

/** Checks that both daemons retain the same unique certified messages after the exchange. */
export function assertDurableExchange(
  scenario: Scenario,
  initial: Content,
  replies: readonly Content[],
) {
  return Effect.all(
    [
      assertFixtureHistory(
        scenario.caller,
        directAddress(scenario.target.agentName),
        initial,
        replies,
      ),
      assertFixtureHistory(
        scenario.target,
        directAddress(scenario.caller.agentName),
        initial,
        replies,
      ),
    ] as const,
    { concurrency: 2, discard: true },
  );
}

/** Validates the wire address built from a registered fixture agent name. */
export function directAddress(agentName: string) {
  return Schema.decodeUnknownSync(AgentAddress)(`agent:${agentName}`);
}

/**
 * Creates the smallest account runtime needed by the process-boundary test.
 * The fixture records the public host values across a real daemon connection.
 */
export function makeObservedOpenClawAccountRuntime(
  fixture: OpenClawRuntimeFixture,
): ObservedOpenClawAccountRuntime {
  return {
    channel: {
      runtimeContexts: {},
      inbound: {
        buildContext: buildOpenClawContext,
        run: (input) => Effect.runPromise(runObservedTurn(input, fixture)),
      },
      routing: {
        resolveAgentRoute: (input) => {
          expect(input.peer).toEqual({
            kind: "direct",
            id: fixture.callerId,
          });
          return {
            agentId: "primary",
            channel: "sharness",
            accountId: input.accountId ?? OPENCLAW_ACCOUNT_ID,
            sessionKey: OPENCLAW_MAIN_SESSION_KEY,
            mainSessionKey: OPENCLAW_MAIN_SESSION_KEY,
            lastRoutePolicy: "session",
            matchedBy: "default",
          };
        },
      },
    },
  };
}

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
 * Start the registered plugin's account against one daemon, with OpenClaw's
 * gateway context, and wait until it reports connected.
 */
export function startOpenClawGateway(
  target: DaemonProcessFixture,
  channelPlugin: StableOpenClawChannelPlugin,
  runtime: ObservedOpenClawAccountRuntime,
  options: {
    readonly mode?: "shared" | "private";
    readonly logLines?: string[];
  } = {},
) {
  return Effect.gen(function* () {
    const cfg = openClawConfig(target.stateDirectory, options.mode);
    const connected = yield* Deferred.make<undefined>();
    yield* useDaemonEndpoint(target);

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

/** Fails with the named host event when its bounded wait expires. */
export function awaitSignal(
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

/** Binds the host session store to one isolated daemon state directory. */
export function openClawConfig(
  stateDirectory: string,
  mode?: "shared" | "private",
): OpenClawConfig {
  return {
    channels: {
      sharness: {
        accounts: [
          { id: OPENCLAW_ACCOUNT_ID, ...(mode === undefined ? {} : { mode }) },
        ],
      },
    },
    session: { store: stateDirectory },
  };
}

/**
 * An account runtime that hands every rendered turn to the test, standing in
 * for the model that reads it, and completes the turn at once.
 */
export function makeTurnQueueRuntime(
  turns: Queue.Queue<OpenClawInboundContext>,
): ObservedOpenClawAccountRuntime {
  return {
    channel: {
      runtimeContexts: {},
      inbound: {
        buildContext: buildOpenClawContext,
        run: (input) =>
          Effect.runPromise(
            Queue.offer(turns, input.adapter.resolveTurn().ctxPayload).pipe(
              Effect.as({}),
            ),
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

/** Requires a rendered host turn before the process-fixture deadline. */
export function nextTurn(turns: Queue.Queue<OpenClawInboundContext>) {
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

/** Acknowledges delivery after reading the next harness item. */
export function nextItem<E>(stream: Stream.Stream<InboundDelivery, E>) {
  return nextDelivery(stream).pipe(
    Effect.map((delivery): InboundItem => delivery.item),
  );
}

/** Accepts one delivery while its subscription is live, before the fixture deadline. */
export function nextDelivery<E>(stream: Stream.Stream<InboundDelivery, E>) {
  return Stream.runHead(
    stream.pipe(Stream.tap((delivery) => delivery.acknowledge)),
  ).pipe(
    Effect.timeoutFail({
      duration: DELIVERY_TIMEOUT,
      onTimeout: () =>
        new ProcessTestError({
          message: "timed out awaiting addressed delivery",
        }),
    }),
    Effect.flatMap(
      Option.match({
        onNone: () =>
          Effect.fail(
            new ProcessTestError({ message: "delivery stream ended" }),
          ),
        onSome: Effect.succeed,
      }),
    ),
  );
}

/** Fails the scenario when an expected collective request has another item kind. */
export function requireRequest(item: InboundItem) {
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

function isStableOpenClawEntry(value: unknown): value is StableOpenClawEntry {
  return (
    typeof value === "object" &&
    value !== null &&
    "register" in value &&
    typeof value.register === "function"
  );
}

function registerFixture(fixture: DaemonProcessFixture) {
  return Effect.scoped(
    Effect.gen(function* () {
      const management = yield* acquireDaemonManagementClient(fixture);
      expect(yield* management.status()).toEqual({ kind: "unregistered" });
      const registered = yield* management
        .register(makeRegistrationRequest(fixture))
        .pipe(
          Effect.filterOrFail(
            (result) => result.kind === registrationResultKind,
            (result) =>
              new ProcessTestError({
                message: `fixture registration returned ${result.kind}`,
              }),
          ),
        );
      return { ...fixture, agent: registered.agent };
    }),
  );
}

function assertFixtureHistory(
  fixture: DaemonProcessFixture,
  peerAddress: ReturnType<typeof directAddress>,
  initial: Content,
  replies: readonly Content[],
) {
  return Effect.scoped(
    Effect.gen(function* () {
      const management = yield* acquireDaemonManagementClient(fixture);
      expect(yield* management.searchConversations()).toEqual({
        kind: "page",
        addresses: [peerAddress],
        hasMore: false,
      });
      const history = yield* management.readConversation(peerAddress);
      expect(history.continuation).toBeNull();
      expect(history.records).toHaveLength(1 + replies.length);
      expect(
        history.records.map((record) => historyMessage(record).content),
      ).toEqual([initial, ...replies]);
    }),
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

function runObservedTurn(
  input: OpenClawInboundRunnerInput,
  fixture: OpenClawRuntimeFixture,
) {
  return Effect.gen(function* () {
    const ingested = input.adapter.ingest();
    expect(ingested.id).toBe(input.raw.turn.id);
    expect(ingested.raw).toEqual(input.raw.turn);
    const turn = input.adapter.resolveTurn();
    expect(turn.route).toEqual({
      agentId: "primary",
      sessionKey: OPENCLAW_MAIN_SESSION_KEY,
    });
    expect(turn.record?.updateLastRoute).toEqual({
      sessionKey: OPENCLAW_MAIN_SESSION_KEY,
      channel: "sharness",
      to: fixture.callerAddress,
      accountId: OPENCLAW_ACCOUNT_ID,
    });
    yield* recordInboundSession(
      {
        ctx: turn.ctxPayload,
        sessionKey: turn.route.sessionKey,
        storePath: turn.cfg.session.store,
      },
      fixture.sessions,
    );
    return yield* effectFromPromise("OpenClaw reply dispatch", () =>
      dispatchOpenClawReply(
        {
          ctx: turn.ctxPayload,
          dispatcherOptions: turn.delivery,
          ...(turn.replyOptions === undefined
            ? {}
            : { replyOptions: turn.replyOptions }),
        },
        fixture,
      ),
    );
  });
}

function recordInboundSession(
  input: OpenClawRecordInput,
  sessions: RecordedSessionStore,
) {
  return Effect.sync(() => {
    expect(input.sessionKey).toBe(OPENCLAW_MAIN_SESSION_KEY);
    sessions.acceptMessage(input.sessionKey, input.ctx.MessageSid);
  });
}

function dispatchOpenClawReply(
  input: OpenClawReplyDispatcherInput,
  fixture: OpenClawReplyFixture,
  // eslint-disable-next-line agent-code-guard/promise-type -- OpenClaw host callbacks require promises at this boundary.
): Promise<OpenClawReplyResult> {
  return Effect.runPromise(
    Effect.gen(function* () {
      expect(input.ctx.Body).toBe(OPENCLAW_INITIAL_TEXT);
      expect(input.ctx.SessionKey).toBe(OPENCLAW_MAIN_SESSION_KEY);
      const sent = yield* effectFromPromise("OpenClaw message tool send", () =>
        fixture.sendReply(),
      );
      expect(sent.details).toEqual({ ok: true, to: fixture.callerAddress });
      const delivery = yield* effectFromPromise("OpenClaw reply delivery", () =>
        input.dispatcherOptions.deliver(
          { text: "private final text" },
          { kind: "final" },
        ),
      );
      expect(delivery).toEqual({ visibleReplySent: false });
      yield* Deferred.succeed(fixture.responseSent, undefined);
      return {
        queuedFinal: false,
        counts: { tool: 1, block: 0, final: 1 },
        sourceReplyDeliveryMode: "message_tool_only",
      };
    }),
  );
}

/** Restores the process-global endpoint after the serial OpenClaw acceptance scope. */
function useDaemonEndpoint(fixture: DaemonProcessFixture) {
  return Effect.gen(function* () {
    vi.stubEnv("SHARNESS_MCP_URL", fixture.endpoint.href);
    vi.stubEnv("SHARNESS_MCP_CREDENTIAL_FILE", fixture.mcpCredentialFile);

    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        vi.unstubAllEnvs();
      }),
    );
  });
}
