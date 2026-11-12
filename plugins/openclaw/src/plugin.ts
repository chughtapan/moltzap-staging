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
  type AgentCard,
  callToolAsWritten,
  connectFromEnvironment,
  type InboxEntry,
  type McpConnection,
  MessageAddressInput,
  type MessageAddressInput as MessageAddressInputValue,
  packageVersion,
  type SendResult,
} from "@social-harness/social-harness";
import { absurd, Data, Effect, Option, Schema } from "effect";
import { jsonResult } from "openclaw/plugin-sdk/channel-actions";
import {
  buildChannelOutboundSessionRoute,
  type ChannelOutboundSessionRouteParams,
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

/** OpenClaw supplies the current conversation as the target for reply. */
const MESSAGE_ACTIONS = ["send", "reply"] as const;
type MessageAction = (typeof MESSAGE_ACTIONS)[number];

/** Distinguishes model tool sends from OpenClaw core sends in gateway logs. */
type OutboundOrigin = `message-action:${MessageAction}` | "send.text";

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

/** One OpenClaw account bound to its configured MCP endpoint. */
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

/** Keeps outbound logs on the account's gateway sink even outside its inbound task. */
interface ConnectedAccount {
  readonly accountId: string;
  readonly endpoint: McpConnection;
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
 * `address` selects the conversation for replies. `members` lists a group
 * message's author and recipients. `body` is what the model reads.
 */
interface HostTurn {
  readonly id: string;
  readonly kind: "direct" | "group";
  readonly address: MessageAddressInputValue;
  readonly sender: TurnSender;
  readonly members?: readonly string[];
  readonly body: string;
}

interface InboundTurnInput {
  readonly ctx: ChannelGatewayContext<Account>;
  readonly runtime: OpenClawAccountRuntime;
  readonly turn: HostTurn;
}

/** One host send as the host supplies it, each field unchecked. */
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

const decodeJsonObject = Schema.decodeUnknownOption(
  Schema.parseJson(
    Schema.Record({ key: Schema.String, value: Schema.Unknown }),
  ),
);

/**
 * Each account retains its own runtime and MCP connection until reception ends
 * or OpenClaw aborts it. Inbox reads record delivery before the host's turn.
 * OpenClaw owns processing and retries. Final assistant text stays private.
 *
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
    blurb: "Agent-to-agent messaging through Social Harness MCP.",
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
    resolveOutboundSessionRoute,
  };
}

/**
 * The session OpenClaw records the agent's own send in: the one the
 * recipient's turns route to, built from the same peer facts by OpenClaw's
 * stock key builder. OpenClaw's fallback route keeps the `agent:` prefix in
 * its peer id, so in private mode a conversation's sends and replies would
 * otherwise sit in two sessions.
 */
function resolveOutboundSessionRoute(
  params: ChannelOutboundSessionRouteParams,
) {
  const target = normalizeMessageTarget(params.target);
  if (target === null) {
    return null;
  }
  const peer = routePeer(
    target.kind === "group" ? "group" : "direct",
    target.to,
  );
  return buildChannelOutboundSessionRoute({
    cfg: params.cfg,
    agentId: params.agentId,
    channel: CHANNEL_ID,
    accountId: params.accountId,
    peer,
    chatType: peer.kind,
    from: `${CHANNEL_ID}:${target.to}`,
    to: target.to,
  });
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
 * Sends the `message` tool's send and reply actions. OpenClaw supplies the
 * current conversation for reply. Core sends use message.send.text.
 */
function createMessageActions(
  connectedAccount: ConnectedAccountState,
): ChannelMessageActionAdapter {
  return {
    describeMessageTool: () => ({ actions: [...MESSAGE_ACTIONS] }),
    supportsAction: ({ action }) => isMessageAction(action),
    handleAction: (ctx) =>
      Effect.runPromise(handleMessageAction(connectedAccount, ctx)),
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
    Effect.map(({ to }) => jsonResult({ ok: true, to })),
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
      text: (ctx) => Effect.runPromise(sendOpenClawText(connectedAccount, ctx)),
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
  return Effect.runPromise(
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
 * Reads the runtime from this account task so another account's runtime
 * cannot be retained by the shared plugin.
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
  const { inbound, routing } = runtime;
  if (typeof inbound !== "object" || inbound === null) {
    return false;
  }
  if (
    !("buildContext" in inbound) ||
    typeof inbound.buildContext !== "function"
  ) {
    return false;
  }
  if (!("run" in inbound) || typeof inbound.run !== "function") {
    return false;
  }
  return (
    typeof routing === "object" &&
    routing !== null &&
    "resolveAgentRoute" in routing &&
    typeof routing.resolveAgentRoute === "function"
  );
}

function acquireAccountEndpoint() {
  return connectFromEnvironment({
    name: "sharness-openclaw",
    version: packageVersion,
  }).pipe(
    Effect.catchTag("McpSettingsError", ({ message }) =>
      Effect.fail(new OpenClawConfigurationError({ detail: message })),
    ),
  );
}

function runAccountConnection(
  ctx: ChannelGatewayContext<Account>,
  runtime: OpenClawAccountRuntime,
  endpoint: McpConnection,
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
 * Ends when reception finishes or OpenClaw aborts the account. Either path
 * returns through the connection's cleanup.
 */
function consumeInboundMessages(
  ctx: ChannelGatewayContext<Account>,
  runtime: OpenClawAccountRuntime,
  endpoint: McpConnection,
) {
  return Effect.raceFirst(
    endpoint.consumeInbox((delivery) =>
      handleInboxEntry(ctx, runtime, delivery),
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

function handleInboxEntry(
  ctx: ChannelGatewayContext<Account>,
  runtime: OpenClawAccountRuntime,
  entry: InboxEntry,
) {
  const kind = entry.address.startsWith("group:") ? "group" : "direct";
  const turn: HostTurn = {
    id: entry.deliveryToken,
    kind,
    address: entry.address,
    ...turnParticipants(entry, kind),
    body: entryText(entry),
  };
  return logInbound(ctx, turn).pipe(
    Effect.zipRight(runOpenClawTurn(ctx, runtime, turn)),
  );
}

/**
 * What the model reads for an entry. A message of one text part with no
 * firewall reminder is that text. Any other entry is its JSON, as
 * `receive_messages` returns it.
 */
function entryText(entry: InboxEntry): string {
  if (entry.item.kind === "multicast" && entry.guidance === undefined) {
    const [part, ...rest] = entry.item.message.content;
    if (part.type === "text" && rest.length === 0) {
      return part.text;
    }
  }
  return JSON.stringify(entry);
}

/**
 * Who a turn is from and, for a group message or question, who is in the
 * group. A result is from Social Harness and names only the agents asked, so
 * its turn lists no members.
 */
function turnParticipants(
  entry: InboxEntry,
  kind: HostTurn["kind"],
): Pick<HostTurn, "sender" | "members"> {
  const { item } = entry;
  switch (item.kind) {
    case "multicast":
    case "collectiveRequest": {
      const { author, recipients } = item.message;
      const sender = agentSender(author);
      return kind === "direct"
        ? { sender }
        : { sender, members: [author, ...recipients].map(agentAddress) };
    }
    case "collectiveResult":
      return {
        sender: {
          id: `sharness:${entry.deliveryToken}`,
          name: ENDPOINT_SENDER_NAME,
        },
      };
    default:
      return absurd(item);
  }
}

function agentSender(agent: AgentCard): TurnSender {
  return { id: agentAddress(agent), name: agent.agentName };
}

function agentAddress(agent: AgentCard): string {
  return `agent:${agent.agentName}`;
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
            textForAgent: turn.body,
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
      bodyForAgent: turn.body,
      commandBody: turn.body,
    },
    extra: inboundGroupFacts(turn),
  });
}

/**
 * Marks peers and Social Harness as bot senders in host metadata. This does
 * not grant their text authority or change routing and reply behavior.
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
  return routePeer(turn.kind, turn.address);
}

/** The peer one conversation's turns and sends both route by. */
function routePeer(kind: HostTurn["kind"], address: string) {
  const prefix = kind === "group" ? "group:" : "agent:";
  return { kind, id: address.slice(prefix.length) };
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
 * Withholds automatic replies so final assistant text stays private. A warning
 * records only the size when text reaches this callback, which can indicate a
 * missing message tool or a model reply that bypassed it.
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
  active: McpConnection,
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
 * Sends what the message text states and returns refusals as model-facing
 * tool errors. Each attempt logs its origin.
 */
function sendText(
  connectedAccount: ConnectedAccountState,
  origin: OutboundOrigin,
  params: TextSend,
) {
  const prefix = `Social Harness: outbound via ${origin} to ${
    typeof params.to === "string" ? params.to.slice(0, LOG_PREVIEW_CHARS) : "-"
  }`;
  return Effect.gen(function* () {
    const { to, text } = yield* readTextSend(params);
    const connection = yield* requireEndpoint(connectedAccount, params);
    const result = yield* callToolAsWritten(
      connection.client,
      "send_message",
      sendArguments(to, text),
    );
    yield* logOutbound(
      connectedAccount,
      `${prefix} id=${result.id} chars=${text.length}: ${text.slice(0, LOG_PREVIEW_CHARS)}`,
    );
    return { to, result };
  }).pipe(
    Effect.tapError((error) =>
      logOutboundFailure(connectedAccount, prefix, error),
    ),
  );
}

/**
 * The `send_message` arguments a host send amounts to. Text that is a JSON
 * object with a `collective` or `collectiveResponse` property is the tool's
 * own arguments for a question or an answer, which the daemon validates. Any
 * other text is a plain message.
 */
function sendArguments(
  to: MessageAddressInputValue,
  text: string,
): Readonly<Record<string, unknown>> {
  const written = Option.getOrUndefined(decodeJsonObject(text));
  return written !== undefined &&
    (Object.hasOwn(written, "collective") ||
      Object.hasOwn(written, "collectiveResponse"))
    ? { ...written, to }
    : { to, text };
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

/** Uses the connected account's gateway log. Disconnected sends have no log sink. */
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
): Effect.Effect<McpConnection, OpenClawOutboundError> {
  const endpoint = connectedEndpoint(connectedAccount, params.accountId);
  return endpoint === undefined
    ? Effect.fail(
        new OpenClawOutboundError({ reason: "account-not-connected" }),
      )
    : Effect.succeed(endpoint);
}

function readTextSend({
  to,
  text,
}: TextSend): Effect.Effect<
  { readonly to: MessageAddressInputValue; readonly text: string },
  OpenClawOutboundError
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
  return Effect.succeed({ to, text });
}

/**
 * Host send results use the author-scoped message ID or collective operation
 * ID, independently of an incoming turn's retained event identity.
 */
function makeMessageSendResult(result: SendResult): ChannelMessageSendResult {
  const messageId = result.id;
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
): McpConnection | undefined {
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

const plugin: OpenClawPluginDefinition &
  Required<Pick<OpenClawPluginDefinition, "id" | "register">> =
  defineChannelPluginEntry({
    id: "social-harness",
    name: "Social Harness",
    description: "Agent-to-agent messaging through Social Harness MCP",
    plugin: createChannelPlugin(),
  });

// eslint-disable-next-line import-x/no-default-export -- OpenClaw discovers plugins through a required default export.
export default plugin;
