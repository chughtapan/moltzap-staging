/** @file OpenClaw plugin that registers the Social Harness channel. */

import type {
  ChannelGatewayContext,
  ChannelLogSink,
  ChannelMessageActionAdapter,
  ChannelMessageActionContext,
  ChannelRuntimeSurface,
} from "openclaw/plugin-sdk/channel-contract";
import type { ChannelInboundTurnPlan } from "openclaw/plugin-sdk/channel-inbound";
import type { OpenClawPluginDefinition } from "openclaw/plugin-sdk/plugin-entry";
import type { ReplyPayload } from "openclaw/plugin-sdk/reply-payload";
import {
  acquireHarnessEndpoint,
  type AgentView,
  groupMembers,
  type GuidanceBodyOrigin,
  type HarnessEndpoint,
  type InboundDelivery,
  type InboundItem,
  type Message,
  MessageAddressInput,
  type MessageAddressInput as MessageAddressInputValue,
  type MessageTextError,
  parseMessageText,
  renderCollectiveRequest,
  renderCollectiveResult,
  renderContent,
  renderLocalGuidance,
  replyAddress,
  type SendInput,
  type SendResult,
} from "@moltzap/social-harness";
import {
  Config,
  ConfigError,
  Data,
  Effect,
  Option,
  Redacted,
  Schema,
  Stream,
  Struct,
} from "effect";
import { absurd } from "effect/Function";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- Native host plugins read one credential without requiring a platform service from the host.
import { readFile } from "node:fs/promises";
import { jsonResult } from "openclaw/plugin-sdk/channel-actions";
import {
  type ChannelPlugin,
  createChannelPluginBase,
  defineChannelPluginEntry,
  type OpenClawConfig,
  type PluginRuntime,
} from "openclaw/plugin-sdk/channel-core";
import {
  type ChannelMessageSendResult,
  type ChannelMessageSendTextContext,
  createMessageReceiptFromOutboundResults,
  defineChannelMessageAdapter,
  waitUntilAbort,
} from "openclaw/plugin-sdk/channel-outbound";

/** The sender OpenClaw records for a turn the endpoint itself emitted. */
const ENDPOINT_SENDER_NAME = "Social Harness";

const CHANNEL_ID = "sharness";
const TARGET_HINT =
  'Use an explicit "agent:<name>" or "group:<member>,<member>,..." address';
const LOG_PREVIEW_CHARS = 80;

/**
 * The message tool actions Social Harness handles. Both take the same text and send
 * it the same way; `reply` is where an answer belongs, because OpenClaw fills
 * its target with the current turn's conversation.
 */
const MESSAGE_ACTIONS = ["send", "reply"] as const;
type MessageAction = (typeof MESSAGE_ACTIONS)[number];

/**
 * The plugin entry point that produced an outbound send, named in its gateway
 * log line: the model's `message` tool action, or OpenClaw's own
 * `message.send.text` delivery.
 */
type OutboundOrigin = `message-action:${MessageAction}` | "send.text";

/** The operation an outbound log line names; an answer is a collective response. */
type OutboundOperation = "plain" | "gather" | "all_gather" | "answer";

type OpenClawTargetKind = "user" | "group";
type OpenClawOutboundFailure =
  | "account-not-connected"
  | "invalid-address"
  | "invalid-operation"
  | "unsupported-action";
type OpenClawInboundFailure = "turn-failed";

interface OpenClawAccountRuntime {
  readonly inbound: Pick<
    PluginRuntime["channel"]["inbound"],
    "buildContext" | "run"
  >;
  readonly routing: Pick<
    PluginRuntime["channel"]["routing"],
    "resolveAgentRoute"
  >;
}

const accountSchema = Schema.Struct({
  id: Schema.String,
  enabled: Schema.optional(Schema.Boolean),
  mode: Schema.optional(Schema.Literal("shared", "private")),
});

/** One OpenClaw account bound to the process-local MCP endpoint. */
type Account = Schema.Schema.Type<typeof accountSchema>;

const channelConfigSchema = Schema.Struct({
  accounts: Schema.optional(Schema.Tuple(accountSchema)),
});
const openClawConfigSchema = Schema.Struct({
  channels: Schema.optional(
    Schema.Struct({ sharness: Schema.optional(channelConfigSchema) }),
  ),
});

interface ResolvedMessageTarget {
  readonly to: MessageAddressInputValue;
  readonly kind: OpenClawTargetKind;
  readonly display: string;
}

/**
 * The account whose endpoint performs sends. `log` is the gateway log sink of
 * the account task, kept so outbound sends, which arrive outside that task,
 * log to the same `openclaw.log` as inbound turns.
 */
interface ConnectedAccount {
  readonly accountId: string;
  readonly endpoint: HarnessEndpoint;
  readonly log?: ChannelLogSink;
}

interface ConnectedAccountState {
  current?: ConnectedAccount;
}

/** Who a turn is from, as OpenClaw records the sender. */
interface TurnSender {
  readonly id: string;
  readonly name: string;
}

/**
 * One retained delivery becomes one host turn under its stable event id.
 * `address` selects the conversation for replies.
 */
interface HostTurn {
  readonly id: string;
  /** Whether a peer authored the turn or the endpoint emitted it. */
  readonly origin: GuidanceBodyOrigin;
  readonly kind: "direct" | "group";
  readonly address: MessageAddressInputValue;
  readonly sender: TurnSender;
  readonly members?: readonly string[];
  readonly body: string;
  readonly bodyForAgent?: string;
}

interface InboundTurnInput {
  readonly ctx: ChannelGatewayContext<Account>;
  readonly runtime: OpenClawAccountRuntime;
  readonly turn: HostTurn;
}

/** One host send before validation; the host supplies each field untyped. */
interface TextSend {
  readonly accountId?: string | null;
  readonly to: unknown;
  readonly text: unknown;
}

class OpenClawInboundError extends Data.TaggedError("OpenClawInboundError")<{
  readonly reason: OpenClawInboundFailure;
  readonly accountId: string;
  readonly turnId: string;
  readonly detail: string;
}> {
  override get message(): string {
    return `Social Harness inbound delivery failed for ${this.accountId}/${this.turnId}: ${this.reason}: ${this.detail}`;
  }
}

/** What each refused host send means, in words its model reads as the tool error. */
const outboundFailureText: Readonly<Record<OpenClawOutboundFailure, string>> = {
  "account-not-connected": "this agent is not connected to Social Harness",
  "invalid-address": "the address is not a valid agent: or group: address",
  "invalid-operation": "the message has no text",
  "unsupported-action": "Social Harness supports only send and reply",
};

class OpenClawOutboundError extends Data.TaggedError("OpenClawOutboundError")<{
  readonly reason: OpenClawOutboundFailure;
}> {
  override get message(): string {
    return `send failed: ${outboundFailureText[this.reason]}`;
  }
}

/**
 * A message tool send carrying OpenClaw's plural `targets`. A Social Harness send
 * goes to exactly one address, so the send fails rather than reach only
 * `target`.
 */
class OpenClawTargetsUnsupportedError extends Data.TaggedError(
  "OpenClawTargetsUnsupportedError",
) {
  override get message(): string {
    return "send failed: targets is not supported; a send goes to one target address";
  }
}

class OpenClawConfigurationError extends Data.TaggedError(
  "OpenClawConfigurationError",
)<{
  readonly detail: string;
}> {
  override get message(): string {
    return `Social Harness configuration is invalid: ${this.detail}`;
  }
}

class OpenClawRuntimeError extends Data.TaggedError("OpenClawRuntimeError")<{
  readonly reason: "abort-wait-failed" | "channel-runtime-unavailable";
  readonly accountId: string;
  readonly detail: string;
}> {
  override get message(): string {
    return `Social Harness account ${this.accountId} runtime failed: ${this.reason}: ${this.detail}`;
  }
}

const isMessageAddressInput = Schema.is(MessageAddressInput);

const carriesTargets = Schema.is(Schema.NonEmptyArray(Schema.Unknown));

/**
 * OpenClaw supplies a separate runtime for each account connection. Each
 * connection holds its endpoint until the stream ends or OpenClaw aborts it.
 * Inbound callback success permits acknowledgment of host handoff. The host
 * owns processing and retry policy, and final assistant text stays private.
 *
 * @returns The Social Harness channel plugin.
 * @internal
 */
export function createChannelPlugin(): ChannelPlugin<Account> {
  const connectedAccount: ConnectedAccountState = {};
  return {
    ...createChannelPluginBase<Account>({
      id: CHANNEL_ID,
      meta: createPluginMeta(),
    }),
    capabilities: { chatTypes: ["direct", "group"] },
    config: createConfigSection(),
    messaging: createMessagingSection(),
    gateway: {
      startAccount: (ctx) => startAccountConnection(ctx, connectedAccount),
    },
    actions: createMessageActions(connectedAccount),
    message: createMessageSection(connectedAccount),
  };
}

function createPluginMeta() {
  return {
    id: CHANNEL_ID,
    label: "Social Harness",
    selectionLabel: "Social Harness (agent messaging)",
    docsPath: "/channels/sharness",
    docsLabel: "sharness",
    blurb:
      "Agent-to-agent messaging through the local Social Harness endpoint.",
    detailLabel: "Social Harness",
    aliases: ["mz"],
    order: 200,
  };
}

function createMessagingSection() {
  return {
    normalizeTarget(raw: string): string | undefined {
      return normalizeMessageTarget(raw)?.to;
    },
    targetResolver: {
      looksLikeId: isExplicitMessageTarget,
      hint: TARGET_HINT,
      resolveTarget(params: { readonly normalized: string }) {
        const target = normalizeMessageTarget(params.normalized);
        return Promise.resolve(
          target === null ? null : { ...target, source: "normalized" as const },
        );
      },
    },
  };
}

function createConfigSection() {
  return {
    listAccountIds(cfg: OpenClawConfig): string[] {
      return resolveAccountList(cfg)
        .map((account) => account.id)
        .filter((id) => id.length > 0);
    },
    resolveAccount,
    isConfigured(account: Account): boolean {
      return account.enabled !== false && account.id.trim().length > 0;
    },
    unconfiguredReason(): string {
      return "A nonempty Social Harness OpenClaw account id is required";
    },
    isEnabled(account: Account): boolean {
      return account.enabled !== false;
    },
  };
}

/**
 * The message tool's `send` and `reply` actions for Social Harness. OpenClaw routes
 * every model `send` and `reply` here because the adapter defines no prepared
 * payload or gateway execution mode. Both read the text with the Client's one
 * parser, so a message's text states its operation; a `reply` with no
 * target gets the current turn's conversation from OpenClaw. The tool result
 * carries the local firewall's guidance on an admitted send, so the model
 * reads it with the result. `message.send.text` remains for the sends
 * OpenClaw's core makes itself.
 * @param connectedAccount The account whose endpoint performs the operation.
 * @returns The action adapter registered on the channel plugin.
 */
function createMessageActions(
  connectedAccount: ConnectedAccountState,
): ChannelMessageActionAdapter {
  return {
    describeMessageTool: () => ({ actions: [...MESSAGE_ACTIONS] }),
    supportsAction: ({ action }) => isMessageAction(action),
    handleAction: (ctx) =>
      runHostPromise(handleMessageAction(connectedAccount, ctx)),
  };
}

function handleMessageAction(
  connectedAccount: ConnectedAccountState,
  ctx: ChannelMessageActionContext,
) {
  const action = ctx.action;
  if (!isMessageAction(action)) {
    return Effect.fail(
      new OpenClawOutboundError({ reason: "unsupported-action" }),
    );
  }
  return refuseTargets(ctx).pipe(
    Effect.andThen(() =>
      sendText(connectedAccount, `message-action:${action}`, {
        accountId: ctx.accountId,
        to: ctx.params.to,
        text: ctx.params.message,
      }),
    ),
    Effect.map(({ input, result }) =>
      jsonResult({
        ok: true,
        to: input.to,
        ...(result.guidance === undefined
          ? {}
          : {
              guidance: {
                ...result.guidance,
                instructions: result.guidance.instructions.map((instruction) =>
                  Struct.omit(instruction, "middlewareId"),
                ),
              },
            }),
      }),
    ),
  );
}

function isMessageAction(action: string): action is MessageAction {
  return MESSAGE_ACTIONS.some((supported) => supported === action);
}

function refuseTargets(
  ctx: ChannelMessageActionContext,
): Effect.Effect<void, OpenClawTargetsUnsupportedError> {
  return carriesTargets(ctx.params.targets)
    ? Effect.fail(new OpenClawTargetsUnsupportedError())
    : Effect.void;
}

function createMessageSection(connectedAccount: ConnectedAccountState) {
  return defineChannelMessageAdapter({
    id: CHANNEL_ID,
    send: {
      text: (ctx) => runHostPromise(sendOpenClawText(connectedAccount, ctx)),
    },
  });
}

function resolveAccount(
  cfg: OpenClawConfig,
  accountId?: string | null,
): Account {
  if (accountId === undefined || accountId === null) {
    return { id: "", enabled: false };
  }
  return (
    resolveAccountList(cfg).find((account) => account.id === accountId) ?? {
      id: accountId,
      enabled: false,
    }
  );
}

function resolveAccountList(cfg: OpenClawConfig): readonly Account[] {
  return Option.match(Schema.decodeUnknownOption(openClawConfigSchema)(cfg), {
    onNone: () => [],
    onSome: (decoded) => decoded.channels?.sharness?.accounts ?? [],
  });
}

function isExplicitMessageTarget(raw: string): boolean {
  const target = raw.trim();
  return normalizeMessageTarget(target)?.to === target;
}

function normalizeMessageTarget(raw: string): ResolvedMessageTarget | null {
  const target = raw.trim();
  if (!isMessageAddressInput(target)) {
    return null;
  }
  return {
    to: target,
    kind: target.startsWith("group:") ? "group" : "user",
    display: target,
  };
}

function startAccountConnection(
  ctx: ChannelGatewayContext<Account>,
  connectedAccount: ConnectedAccountState,
) {
  if (ctx.abortSignal.aborted) {
    return Promise.resolve();
  }
  return runHostPromise(
    requireOpenClawAccountRuntime(ctx).pipe(
      Effect.flatMap((runtime) =>
        acquireAccountEndpoint().pipe(
          Effect.flatMap((endpoint) =>
            runAccountConnection(ctx, runtime, endpoint, connectedAccount),
          ),
        ),
      ),
      Effect.scoped,
    ),
  );
}

/**
 * Returns the OpenClaw services for an account task.
 *
 * OpenClaw creates a channel runtime for each account task. Reading it here
 * prevents the registered plugin from retaining another account's runtime.
 * @param ctx The account connection context supplied by OpenClaw.
 * @returns The routing and inbound services scoped to this account task.
 */
function requireOpenClawAccountRuntime(
  ctx: ChannelGatewayContext<Account>,
): Effect.Effect<OpenClawAccountRuntime, OpenClawRuntimeError> {
  if (
    ctx.channelRuntime !== undefined &&
    isOpenClawAccountRuntime(ctx.channelRuntime)
  ) {
    return Effect.succeed(ctx.channelRuntime);
  }
  return Effect.fail(
    new OpenClawRuntimeError({
      reason: "channel-runtime-unavailable",
      accountId: ctx.accountId,
      detail: "the account task did not receive OpenClaw channel services",
    }),
  );
}

function isOpenClawAccountRuntime(
  runtime: ChannelRuntimeSurface,
): runtime is ChannelRuntimeSurface & OpenClawAccountRuntime {
  return (
    hasOpenClawInboundRuntime(runtime) && hasOpenClawRoutingRuntime(runtime)
  );
}

function hasOpenClawInboundRuntime(runtime: ChannelRuntimeSurface): boolean {
  const inbound = runtime.inbound;
  if (typeof inbound !== "object" || inbound === null) {
    return false;
  }
  if (!("buildContext" in inbound)) {
    return false;
  }
  if (typeof inbound.buildContext !== "function") {
    return false;
  }
  return "run" in inbound && typeof inbound.run === "function";
}

function hasOpenClawRoutingRuntime(runtime: ChannelRuntimeSurface): boolean {
  const routing = runtime.routing;
  if (typeof routing !== "object" || routing === null) {
    return false;
  }
  return (
    "resolveAgentRoute" in routing &&
    typeof routing.resolveAgentRoute === "function"
  );
}

function acquireAccountEndpoint() {
  return Effect.gen(function* () {
    const endpoint = yield* Config.url("SHARNESS_MCP_URL");
    const credentialFile = yield* Config.redacted(
      "SHARNESS_MCP_CREDENTIAL_FILE",
    );
    const credential = yield* Effect.tryPromise({
      try: () => readFile(Redacted.value(credentialFile), "utf8"),
      catch: () =>
        new OpenClawConfigurationError({
          detail:
            "SHARNESS_MCP_CREDENTIAL_FILE must name a readable file. Docs: https://docs.moltzap.xyz/guides/run-sharnessd#mcp-credential-file",
        }),
    });
    return yield* acquireHarnessEndpoint(endpoint, {
      credential: Redacted.make(credential),
      receiveMode: "push",
    });
  });
}

function runAccountConnection(
  ctx: ChannelGatewayContext<Account>,
  runtime: OpenClawAccountRuntime,
  endpoint: HarnessEndpoint,
  connectedAccount: ConnectedAccountState,
) {
  return Effect.sync(() => {
    connectedAccount.current = {
      accountId: ctx.accountId,
      endpoint,
      ...(ctx.log === undefined ? {} : { log: ctx.log }),
    };
  }).pipe(
    Effect.zipRight(reportConnected(ctx)),
    Effect.zipRight(consumeInboundMessages(ctx, runtime, endpoint)),
    Effect.ensuring(
      removeConnectedEndpoint(connectedAccount, ctx.accountId, endpoint),
    ),
    Effect.ensuring(
      Effect.sync(() => {
        ctx.setStatus({
          ...ctx.getStatus(),
          accountId: ctx.accountId,
          connected: false,
          running: false,
        });
      }),
    ),
    Effect.tapError((cause) =>
      Effect.sync(() => {
        ctx.log?.error?.(
          `Social Harness: connection failed for ${ctx.accountId}: ${String(cause)}`,
        );
      }),
    ),
  );
}

/**
 * Consumes messages until the stream ends or OpenClaw aborts the account.
 *
 * The stream can complete or fail independently of the account abort signal.
 * Racing both operations sends every result through the cleanup in
 * `runAccountConnection`.
 * @param ctx The account task and abort signal supplied by OpenClaw.
 * @param runtime OpenClaw routing and inbound services for this account task.
 * @param endpoint The daemon-backed message stream for this account.
 * @returns An effect that ends with the stream or the abort signal.
 */
function consumeInboundMessages(
  ctx: ChannelGatewayContext<Account>,
  runtime: OpenClawAccountRuntime,
  endpoint: HarnessEndpoint,
) {
  return Effect.raceFirst(
    endpoint.messages.pipe(
      Stream.runForEach((delivery) =>
        handleInboundDelivery(ctx, runtime, delivery),
      ),
    ),
    Effect.tryPromise({
      try: () => waitUntilAbort(ctx.abortSignal),
      catch: (cause) =>
        new OpenClawRuntimeError({
          reason: "abort-wait-failed",
          accountId: ctx.accountId,
          detail: String(cause),
        }),
    }),
  );
}

function reportConnected(
  ctx: ChannelGatewayContext<Account>,
): Effect.Effect<void> {
  return Effect.sync(() => {
    ctx.log?.info?.(`Social Harness: connected for account ${ctx.accountId}`);
    ctx.setStatus({
      ...ctx.getStatus(),
      accountId: ctx.accountId,
      connected: true,
      running: true,
      lastConnectedAt: Date.now(),
    });
  });
}

function handleInboundDelivery(
  ctx: ChannelGatewayContext<Account>,
  runtime: OpenClawAccountRuntime,
  delivery: InboundDelivery,
) {
  const projected = inboundItemTurn(delivery.item, delivery.eventId);
  const guidance = delivery.guidance;
  const turn: HostTurn =
    guidance === undefined
      ? projected
      : {
          ...projected,
          bodyForAgent:
            delivery.item.kind === "collectiveRequest"
              ? renderCollectiveRequest(
                  delivery.item,
                  requestAnswerInstructions(
                    replyAddress(delivery.item.message),
                  ),
                  guidance,
                )
              : renderLocalGuidance(guidance, {
                  text: projected.body,
                  origin: projected.origin,
                }),
        };
  return logInbound(ctx, turn).pipe(
    Effect.zipRight(runOpenClawTurn(ctx, runtime, turn)),
    Effect.zipRight(delivery.acknowledge),
  );
}

/**
 * Render one inbound item as the turn its kind defines. Each kind has one
 * fixed form, the same for every agent. A request belongs to the conversation
 * it arrived in: the requester's for a gather, the group's for an all_gather.
 * A result or a failure is attributed to Social Harness, not to any member, and
 * belongs to the conversation its operation addressed.
 * @param item The item the endpoint delivered.
 * @returns The turn OpenClaw runs.
 */
function inboundItemTurn(item: InboundItem, eventId: string): HostTurn {
  switch (item.kind) {
    case "multicast":
      return multicastTurn(item.message, eventId);
    case "collectiveRequest":
      return addressedTurn(
        {
          id: eventId,
          origin: "peer",
          sender: agentSender(item.message.author),
        },
        replyAddress(item.message),
        renderCollectiveRequest(
          item,
          requestAnswerInstructions(replyAddress(item.message)),
        ),
      );
    case "collectiveResult":
      return endpointTurn(eventId, item.to, renderCollectiveResult(item));
    case "operationFailed":
      return endpointTurn(eventId, item.to, item.error);
    default:
      return absurd(item);
  }
}

/** Reply instructions belong to the endpoint rather than the peer's quoted question. */
function requestAnswerInstructions(to: MessageAddressInputValue): string {
  return `Send the answer once as the whole message text, with the message tool's reply action or send to ${to}.`;
}

function multicastTurn(message: Message, eventId: string): HostTurn {
  return addressedTurn(
    {
      id: eventId,
      origin: "peer",
      sender: agentSender(message.author),
    },
    replyAddress(message),
    renderContent(message.content),
  );
}

function endpointTurn(
  id: string,
  address: MessageAddressInputValue,
  body: string,
): HostTurn {
  return addressedTurn(
    {
      id,
      origin: "endpoint",
      sender: { id: `sharness:${id}`, name: ENDPOINT_SENDER_NAME },
    },
    address,
    body,
  );
}

/**
 * A turn in the conversation `address` names: a group address makes a group
 * turn listing its members.
 */
function addressedTurn(
  author: Pick<HostTurn, "id" | "origin" | "sender">,
  address: MessageAddressInputValue,
  body: string,
): HostTurn {
  const members = groupMembers(address);
  return members.length === 0
    ? { ...author, kind: "direct", address, body }
    : { ...author, kind: "group", address, members, body };
}

function agentSender(agent: AgentView): TurnSender {
  return { id: `agent:${agent.agentName}`, name: agent.agentName };
}

function logInbound(
  ctx: ChannelGatewayContext<Account>,
  turn: HostTurn,
): Effect.Effect<void> {
  return Effect.sync(() => {
    ctx.log?.info?.(
      `Social Harness: inbound from ${turn.sender.id}: ${turn.body.slice(0, LOG_PREVIEW_CHARS)}`,
    );
    ctx.setStatus({
      ...ctx.getStatus(),
      accountId: ctx.accountId,
      lastInboundAt: Date.now(),
      lastEventAt: Date.now(),
    });
  });
}

function runOpenClawTurn(
  ctx: ChannelGatewayContext<Account>,
  runtime: OpenClawAccountRuntime,
  turn: HostTurn,
): Effect.Effect<void, OpenClawInboundError> {
  return Effect.tryPromise({
    try: () =>
      runtime.inbound.run({
        channel: CHANNEL_ID,
        accountId: ctx.accountId,
        raw: { turn },
        adapter: {
          ingest: () => ({
            id: turn.id,
            rawText: turn.body,
            textForAgent: modelBody(turn),
            textForCommands: turn.body,
            raw: turn,
          }),
          resolveTurn: () => buildRoutedTurnPlan({ ctx, runtime, turn }),
        },
      }),
    catch: (cause) =>
      new OpenClawInboundError({
        reason: "turn-failed",
        accountId: ctx.accountId,
        turnId: turn.id,
        detail: String(cause),
      }),
  });
}

function buildRoutedTurnPlan(input: InboundTurnInput): ChannelInboundTurnPlan {
  const { ctx, turn, runtime } = input;
  const peer = inboundRoutePeer(turn);
  const route = runtime.routing.resolveAgentRoute({
    cfg: ctx.cfg,
    channel: CHANNEL_ID,
    accountId: ctx.accountId,
    peer,
  });
  const sessionKey =
    (ctx.account.mode ?? "shared") === "shared"
      ? route.mainSessionKey
      : route.sessionKey;
  const ctxPayload = buildInboundContext(input, route, sessionKey);
  return {
    cfg: ctx.cfg,
    channel: CHANNEL_ID,
    accountId: ctx.accountId,
    route: {
      agentId: route.agentId,
      sessionKey,
      ...(route.dmScope === undefined ? {} : { dmScope: route.dmScope }),
    },
    ctxPayload,
    delivery: { deliver: (payload) => withholdFinalText(ctx, payload) },
    record: {
      updateLastRoute: {
        sessionKey,
        channel: CHANNEL_ID,
        to: turn.address,
        accountId: ctx.accountId,
      },
    },
    messageId: turn.id,
  };
}

function buildInboundContext(
  input: InboundTurnInput,
  route: ReturnType<OpenClawAccountRuntime["routing"]["resolveAgentRoute"]>,
  sessionKey: string,
) {
  const { ctx, turn, runtime } = input;
  return runtime.inbound.buildContext({
    channel: CHANNEL_ID,
    accountId: ctx.accountId,
    provider: CHANNEL_ID,
    surface: CHANNEL_ID,
    messageId: turn.id,
    from: turn.sender.id,
    sender: inboundSenderFacts(turn),
    conversation: inboundConversationFacts(turn),
    route: {
      agentId: route.agentId,
      accountId: ctx.accountId,
      routeSessionKey: sessionKey,
      dispatchSessionKey: sessionKey,
      persistedSessionKey: sessionKey,
      mainSessionKey: route.mainSessionKey,
    },
    reply: inboundReplyFacts(turn),
    message: {
      body: turn.body,
      rawBody: turn.body,
      bodyForAgent: modelBody(turn),
      commandBody: turn.body,
    },
    extra: inboundGroupFacts(turn),
  });
}

/** The host keeps its raw body while Client renders the model-facing guidance. */
function modelBody(turn: HostTurn): string {
  return turn.bodyForAgent ?? turn.body;
}

/**
 * Every Social Harness sender is another principal's agent or the collective layer,
 * so the sender is marked as a bot. OpenClaw records that as the
 * participant's `senderKind` in session and transcript metadata; it does not
 * change routing, reply mode, or admit the sender's text as instructions.
 */
function inboundSenderFacts(turn: HostTurn) {
  return { ...turn.sender, isBot: true };
}

function inboundConversationFacts(turn: HostTurn) {
  const routePeer = inboundRoutePeer(turn);
  return {
    kind: turn.kind,
    id: turn.address,
    label: turn.address,
    routePeer,
  };
}

function inboundRoutePeer(turn: HostTurn) {
  const prefix = turn.kind === "group" ? "group:" : "agent:";
  return {
    kind: turn.kind,
    id: turn.address.slice(prefix.length),
  };
}

function inboundReplyFacts(turn: HostTurn) {
  return {
    to: turn.address,
    originatingTo: turn.address,
    replyTarget: turn.address,
    deliveryTarget: turn.address,
  };
}

function inboundGroupFacts(turn: HostTurn) {
  return turn.members === undefined
    ? undefined
    : { GroupMembers: turn.members.join(",") };
}

/**
 * Final assistant text never becomes a Social Harness message. A launcher's OpenClaw
 * configuration selects tool-only visible replies, and this callback withholds
 * anything that still reaches it, so the `message` tool with an explicit
 * target is the only send path. Non-empty text reaching here means OpenClaw
 * fell back to automatic delivery, which it does when the `message` tool is
 * outside the tool policy, or the model wrote a reply without the tool; the
 * warning is the only trace of that, so it names the size, never the text.
 */
function withholdFinalText(
  ctx: ChannelGatewayContext<Account>,
  payload: ReplyPayload,
) {
  if (payload.text !== undefined && payload.text.length > 0) {
    ctx.log?.warn?.(
      `Social Harness: withheld ${payload.text.length} chars of final text; visible replies use the message tool`,
    );
  }
  return Promise.resolve({ visibleReplySent: false as const });
}

function removeConnectedEndpoint(
  connectedAccount: ConnectedAccountState,
  accountId: string,
  active: HarnessEndpoint,
): Effect.Effect<void> {
  return Effect.sync(() => {
    if (
      connectedAccount.current?.accountId === accountId &&
      connectedAccount.current.endpoint === active
    ) {
      connectedAccount.current = undefined;
    }
  });
}

function sendOpenClawText(
  connectedAccount: ConnectedAccountState,
  ctx: ChannelMessageSendTextContext,
) {
  return sendText(connectedAccount, "send.text", {
    accountId: ctx.accountId,
    text: ctx.text,
    to: ctx.to,
  }).pipe(Effect.map(({ result }) => makeMessageSendResult(result)));
}

/**
 * Perform one host send as the one Client operation its text states. A
 * refused send fails with an error whose message OpenClaw returns to the
 * model as the tool error: the text's failing fields, or the Client's
 * refusal. Each send, completed or refused, writes one gateway log line
 * naming `origin`, so a run's log tells a model tool send from a send
 * OpenClaw made itself; a refusal before the text parses names no operation.
 * @param connectedAccount The account whose endpoint performs the operation.
 * @param origin The plugin entry point the send arrived through.
 * @param params The host's account, address and text.
 * @returns The parsed input and the endpoint's result.
 */
function sendText(
  connectedAccount: ConnectedAccountState,
  origin: OutboundOrigin,
  params: TextSend,
) {
  return readSendInput(params).pipe(
    Effect.tapError((error) =>
      logOutboundFailure(
        connectedAccount,
        outboundLogPrefix(origin, params, "-"),
        error,
      ),
    ),
    Effect.flatMap((input) =>
      requireEndpoint(connectedAccount, params).pipe(
        Effect.flatMap((endpoint) => endpoint.send(input)),
        Effect.tapBoth({
          onFailure: (error) =>
            logOutboundFailure(
              connectedAccount,
              outboundLogPrefix(origin, params, outboundOperation(input)),
              error,
            ),
          onSuccess: (result) =>
            logOutbound(
              connectedAccount,
              `${outboundLogPrefix(origin, params, outboundOperation(input))} operationId=${result.operationId ?? "-"} msgId=${result.msgId ?? "-"} chars=${textLength(params)}: ${textPreview(params)}`,
            ),
        }),
        Effect.map((result) => ({ input, result })),
      ),
    ),
  );
}

function outboundOperation(input: SendInput): OutboundOperation {
  if ("collectiveResponse" in input) {
    return "answer";
  }
  const op = input.collective?.op;
  return op === undefined || op === "multicast" ? "plain" : op;
}

function outboundLogPrefix(
  origin: OutboundOrigin,
  params: TextSend,
  operation: OutboundOperation | "-",
): string {
  const to =
    typeof params.to === "string" ? params.to.slice(0, LOG_PREVIEW_CHARS) : "-";
  return `Social Harness: outbound via ${origin} to ${to} op=${operation}`;
}

function textLength(params: TextSend): number {
  return typeof params.text === "string" ? params.text.length : 0;
}

function textPreview(params: TextSend): string {
  return typeof params.text === "string"
    ? params.text.slice(0, LOG_PREVIEW_CHARS)
    : "";
}

function logOutboundFailure(
  connectedAccount: ConnectedAccountState,
  prefix: string,
  error: Error,
): Effect.Effect<void> {
  return Effect.sync(() => {
    connectedAccount.current?.log?.warn?.(`${prefix} failed: ${error.message}`);
  });
}

/**
 * Write one completed send's line to the connected account's gateway log. A
 * send while no account is connected has no gateway log to write to.
 */
function logOutbound(
  connectedAccount: ConnectedAccountState,
  line: string,
): Effect.Effect<void> {
  return Effect.sync(() => {
    connectedAccount.current?.log?.info?.(line);
  });
}

function requireEndpoint(
  connectedAccount: ConnectedAccountState,
  params: TextSend,
): Effect.Effect<HarnessEndpoint, OpenClawOutboundError> {
  const endpoint = connectedEndpoint(connectedAccount, params.accountId);
  return endpoint === undefined
    ? Effect.fail(
        new OpenClawOutboundError({ reason: "account-not-connected" }),
      )
    : Effect.succeed(endpoint);
}

/**
 * Validate OpenClaw's optional target and text fields before handing them to
 * the shared message-text parser.
 */
function readSendInput({
  to,
  text,
}: TextSend): Effect.Effect<
  SendInput,
  OpenClawOutboundError | MessageTextError
> {
  if (!isMessageAddressInput(to)) {
    return Effect.fail(
      new OpenClawOutboundError({ reason: "invalid-address" }),
    );
  }
  if (typeof text !== "string") {
    return Effect.fail(
      new OpenClawOutboundError({ reason: "invalid-operation" }),
    );
  }
  return parseMessageText(to, text);
}

/**
 * Accepted messages report their author-scoped id; collective questions report
 * their operation id. OpenClaw records this outgoing result separately from
 * the retained event identity of an incoming turn.
 * @param result The daemon's accepted send.
 * @returns The send result OpenClaw records.
 */
function makeMessageSendResult(result: SendResult): ChannelMessageSendResult {
  const messageId = result.msgId ?? result.operationId;
  return {
    ...(messageId === undefined ? {} : { messageId }),
    receipt: createMessageReceiptFromOutboundResults({
      results: [
        {
          channel: CHANNEL_ID,
          ...(messageId === undefined ? {} : { messageId }),
          ...(result.address === undefined
            ? {}
            : { conversationId: result.address }),
        },
      ],
      kind: "text",
    }),
  };
}

function connectedEndpoint(
  connectedAccount: ConnectedAccountState,
  accountId?: string | null,
): HarnessEndpoint | undefined {
  const active = connectedAccount.current;
  if (active === undefined) {
    return undefined;
  }
  const requested = accountId?.trim();
  if (
    requested !== undefined &&
    requested.length > 0 &&
    requested !== active.accountId
  ) {
    return undefined;
  }
  return active.endpoint;
}

function runHostPromise<A, E extends Error | ConfigError.ConfigError>(
  effect: Effect.Effect<A, E>,
) {
  return Effect.runPromise(Effect.mapError(effect, hostPromiseError));
}

function hostPromiseError(error: Error | ConfigError.ConfigError): Error {
  if (ConfigError.isConfigError(error)) {
    return new OpenClawConfigurationError({
      detail: ConfigError.isInvalidData(error)
        ? `${error.path.join(".")}: ${error.message}`
        : error.message,
    });
  }
  return error;
}

const plugin: OpenClawPluginDefinition &
  Required<Pick<OpenClawPluginDefinition, "id" | "register">> =
  defineChannelPluginEntry({
    id: "social-harness",
    name: "Social Harness",
    description:
      "Agent-to-agent messaging through the local Social Harness endpoint",
    plugin: createChannelPlugin(),
  });

// eslint-disable-next-line import-x/no-default-export -- OpenClaw discovers plugins through a required default export.
export default plugin;
