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
  connectFromEnvironment,
  type InboxEntry,
  type McpConnection,
  MessageAddressInput,
  packageVersion,
  sendMessage,
  type SendResult,
  toolDefinitions,
} from "@social-harness/social-harness";
import { Data, Effect, Option, Schema } from "effect";
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
  | "targets-unsupported"
  | "text-missing"
  | "unsupported-action";

interface ChannelServices {
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

/** One OpenClaw account as the operator lists it under `channels.sharness.accounts`. */
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
  readonly to: MessageAddressInput;
  readonly kind: OpenClawTargetKind;
  readonly display: string;
}

/**
 * The account whose connection sends go out on. `log` is the account's gateway
 * sink, which keeps outbound logs on it outside the account task.
 */
interface ConnectedAccount {
  readonly accountId: string;
  readonly connection: McpConnection;
  readonly log?: ChannelLogSink;
}

/** Holds the one connected account, or nothing while none is connected. */
interface ConnectedAccountSlot {
  current?: ConnectedAccount;
}

/** Who an entry is from, as OpenClaw records the sender. */
interface EntrySender {
  readonly id: string;
  readonly name: string;
}

/**
 * What OpenClaw receives for one inbox entry, which it runs as one turn. `id`
 * is the message id. `address` is the conversation replies go to.
 * `members` lists a group message's author and recipients. `text` is what the
 * model reads.
 */
interface ProjectedEntry {
  readonly id: string;
  readonly kind: "direct" | "group";
  readonly address: MessageAddressInput;
  readonly sender: EntrySender;
  readonly members?: readonly string[];
  readonly text: string;
}

/** What one OpenClaw turn is built from. */
interface InboundTurn {
  readonly task: ChannelGatewayContext<Account>;
  readonly services: ChannelServices;
  readonly projected: ProjectedEntry;
}

/** One host send as the host supplies it, each field unchecked. */
interface TextSend {
  readonly replyToMsgId?: string;
  readonly accountId?: string | null;
  readonly to: unknown;
  readonly text: unknown;
}

/**
 * Why a host callback's Promise rejects. `message` is all the host reads: for
 * a refused or failed send, the text its model reads as the tool error.
 */
class OpenClawChannelError extends Data.TaggedError("OpenClawChannelError")<{
  readonly message: string;
}> {}

/** What each refused host send means, in words its model reads as the tool error. */
const outboundFailureText: Readonly<Record<OpenClawOutboundFailure, string>> = {
  "account-not-connected": "this agent is not connected to Social Harness",
  "invalid-address": "the address is not a valid agent: or group: address",
  "targets-unsupported":
    "targets is not supported; a send goes to one target address",
  "text-missing": "the message has no text",
  "unsupported-action": "Social Harness supports only send and reply",
};

const isMessageAddressInput = Schema.is(MessageAddressInput);

const carriesTargets = Schema.is(Schema.NonEmptyArray(Schema.Unknown));

const decodeJsonObject = Schema.decodeUnknownOption(
  Schema.parseJson(
    Schema.Record({ key: Schema.String, value: Schema.Unknown }),
  ),
);

/**
 * Each account retains its own channel services and daemon connection until
 * reception ends or OpenClaw aborts it. A page of inbox entries is committed after
 * the host's turn for each. OpenClaw owns processing and retries. Final assistant text stays
 * private.
 *
 * @internal
 */
export function createChannelPlugin(): ChannelPlugin<Account> {
  const accountSlot: ConnectedAccountSlot = {};
  return {
    ...createChannelPluginBase<Account>({
      id: CHANNEL_ID,
      meta: createPluginMeta(),
    }),
    capabilities: { chatTypes: ["direct", "group"] },
    config: createAccountConfig(),
    messaging: createTargetResolution(),
    gateway: {
      startAccount: (task) => startAccountConnection(task, accountSlot),
    },
    actions: createMessageToolActions(accountSlot),
    message: createCoreTextSend(accountSlot),
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
    order: 200,
  };
}

function createTargetResolution() {
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

function createAccountConfig() {
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
function createMessageToolActions(
  accountSlot: ConnectedAccountSlot,
): ChannelMessageActionAdapter {
  return {
    describeMessageTool: () => ({ actions: [...MESSAGE_ACTIONS] }),
    supportsAction: ({ action }) => isMessageAction(action),
    handleAction: (ctx) =>
      Effect.runPromise(handleMessageAction(accountSlot, ctx)),
  };
}

function handleMessageAction(
  accountSlot: ConnectedAccountSlot,
  ctx: ChannelMessageActionContext,
) {
  const action = ctx.action;
  if (!isMessageAction(action)) {
    return Effect.fail(refusedSend("unsupported-action"));
  }
  return refuseTargets(ctx).pipe(
    Effect.andThen(() =>
      sendText(accountSlot, `message-action:${action}`, {
        accountId: ctx.accountId,
        to: ctx.params.to,
        text: ctx.params.message,
        ...(ctx.reply?.source === "explicit"
          ? { replyToMsgId: ctx.reply.replyToId }
          : {}),
      }),
    ),
    Effect.map(({ to }) => jsonResult({ ok: true, to })),
  );
}

function isMessageAction(action: string): action is MessageAction {
  return MESSAGE_ACTIONS.some((supported) => supported === action);
}

/**
 * Refuses a message tool send carrying OpenClaw's plural `targets`. A Social
 * Harness send goes to exactly one address, so the send fails rather than
 * reach only `target`.
 */
function refuseTargets(
  ctx: ChannelMessageActionContext,
): Effect.Effect<void, OpenClawChannelError> {
  return carriesTargets(ctx.params.targets)
    ? Effect.fail(refusedSend("targets-unsupported"))
    : Effect.void;
}

function createCoreTextSend(accountSlot: ConnectedAccountSlot) {
  return defineChannelMessageAdapter({
    id: CHANNEL_ID,
    send: {
      text: (ctx) => Effect.runPromise(sendCoreText(accountSlot, ctx)),
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
  task: ChannelGatewayContext<Account>,
  accountSlot: ConnectedAccountSlot,
) {
  if (task.abortSignal.aborted) {
    return Promise.resolve();
  }
  return Effect.runPromise(
    requireChannelServices(task).pipe(
      Effect.flatMap((services) =>
        connectToDaemon().pipe(
          Effect.flatMap((connection) =>
            runAccountConnection(task, services, connection, accountSlot),
          ),
        ),
      ),
      Effect.scoped,
    ),
  );
}

/**
 * Reads the channel services from this account task so the shared plugin
 * cannot retain another account's.
 */
function requireChannelServices(
  task: ChannelGatewayContext<Account>,
): Effect.Effect<ChannelServices, OpenClawChannelError> {
  if (
    task.channelRuntime !== undefined &&
    hasChannelServices(task.channelRuntime)
  ) {
    return Effect.succeed(task.channelRuntime);
  }
  return Effect.fail(
    new OpenClawChannelError({
      message: `Social Harness account ${task.accountId} runtime failed: channel-runtime-unavailable: the account task did not receive OpenClaw channel services`,
    }),
  );
}

function hasChannelServices(
  channelRuntime: ChannelRuntimeSurface,
): channelRuntime is ChannelRuntimeSurface & ChannelServices {
  const { inbound, routing } = channelRuntime;
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

function connectToDaemon() {
  return connectFromEnvironment({
    name: "sharness-openclaw",
    version: packageVersion,
  }).pipe(
    Effect.catchTag("McpSettingsError", ({ message }) =>
      Effect.fail(
        new OpenClawChannelError({
          message: `Social Harness configuration is invalid: ${message}`,
        }),
      ),
    ),
  );
}

function runAccountConnection(
  task: ChannelGatewayContext<Account>,
  services: ChannelServices,
  connection: McpConnection,
  accountSlot: ConnectedAccountSlot,
) {
  return Effect.sync(() => {
    accountSlot.current = {
      accountId: task.accountId,
      connection,
      ...(task.log === undefined ? {} : { log: task.log }),
    };
  }).pipe(
    Effect.zipRight(reportConnected(task)),
    Effect.zipRight(handleInboxEntries(task, services, connection)),
    Effect.ensuring(clearConnectedAccount(accountSlot, connection)),
    Effect.ensuring(
      Effect.sync(() => {
        task.setStatus({
          ...task.getStatus(),
          accountId: task.accountId,
          connected: false,
          running: false,
        });
      }),
    ),
    Effect.tapError((cause) =>
      Effect.sync(() => {
        task.log?.error?.(
          `Social Harness: connection failed for ${task.accountId}: ${String(cause)}`,
        );
      }),
    ),
  );
}

/**
 * Ends when reception finishes or OpenClaw aborts the account. Either path
 * returns through the connection's cleanup.
 */
function handleInboxEntries(
  task: ChannelGatewayContext<Account>,
  services: ChannelServices,
  connection: McpConnection,
) {
  return Effect.raceFirst(
    connection.consumeInbox((entry) => handleInboxEntry(task, services, entry)),
    Effect.tryPromise({
      try: () => waitUntilAbort(task.abortSignal),
      catch: (cause) =>
        new OpenClawChannelError({
          message: `Social Harness account ${task.accountId} runtime failed: abort-wait-failed: ${String(cause)}`,
        }),
    }),
  );
}

function reportConnected(
  task: ChannelGatewayContext<Account>,
): Effect.Effect<void> {
  return Effect.sync(() => {
    task.log?.info?.(`Social Harness: connected for account ${task.accountId}`);
    task.setStatus({
      ...task.getStatus(),
      accountId: task.accountId,
      connected: true,
      running: true,
      lastConnectedAt: Date.now(),
    });
  });
}

function handleInboxEntry(
  task: ChannelGatewayContext<Account>,
  services: ChannelServices,
  entry: InboxEntry,
) {
  const kind = entry.address.startsWith("group:") ? "group" : "direct";
  const projected: ProjectedEntry = {
    id: entry.message.msgId,
    kind,
    address: entry.address,
    ...entryParticipants(entry, kind),
    text: entryText(entry),
  };
  return logInbound(task, projected).pipe(
    Effect.zipRight(runOpenClawTurn(task, services, projected)),
  );
}

/**
 * What the model reads for an entry. A message of one text part with no
 * guidance is that text. Any other entry is its JSON, as
 * `receive_messages` returns it.
 */
function entryText(entry: InboxEntry): string {
  if (entry.guidance === undefined) {
    const [part, ...rest] = entry.message.content;
    if (part.type === "text" && rest.length === 0) {
      return part.text;
    }
  }
  return JSON.stringify(entry);
}

/** Who an entry is from and, for a group message, who is in the group. */
function entryParticipants(
  entry: InboxEntry,
  kind: ProjectedEntry["kind"],
): Pick<ProjectedEntry, "sender" | "members"> {
  const { author, recipients } = entry.message;
  const sender = agentSender(author);
  return kind === "direct"
    ? { sender }
    : { sender, members: [author, ...recipients].map(agentAddress) };
}

function agentSender(agent: AgentCard): EntrySender {
  return { id: agentAddress(agent), name: agent.agentName };
}

function agentAddress(agent: AgentCard): string {
  return `agent:${agent.agentName}`;
}

function logInbound(
  task: ChannelGatewayContext<Account>,
  projected: ProjectedEntry,
): Effect.Effect<void> {
  return Effect.sync(() => {
    task.log?.info?.(
      `Social Harness: inbound from ${projected.sender.id}: ${projected.text.slice(0, LOG_PREVIEW_CHARS)}`,
    );
    task.setStatus({
      ...task.getStatus(),
      accountId: task.accountId,
      lastInboundAt: Date.now(),
      lastEventAt: Date.now(),
    });
  });
}

function runOpenClawTurn(
  task: ChannelGatewayContext<Account>,
  services: ChannelServices,
  projected: ProjectedEntry,
): Effect.Effect<void, OpenClawChannelError> {
  return Effect.tryPromise({
    try: () =>
      services.inbound.run({
        channel: CHANNEL_ID,
        accountId: task.accountId,
        raw: { turn: projected },
        adapter: {
          ingest: () => ({
            id: projected.id,
            rawText: projected.text,
            textForAgent: projected.text,
            textForCommands: projected.text,
            raw: projected,
          }),
          resolveTurn: () => buildRoutedTurnPlan({ task, services, projected }),
        },
      }),
    catch: (cause) =>
      new OpenClawChannelError({
        message: `Social Harness inbound delivery failed for ${task.accountId}/${projected.id}: turn-failed: ${String(cause)}`,
      }),
  });
}

function buildRoutedTurnPlan(turn: InboundTurn): ChannelInboundTurnPlan {
  const { task, services, projected } = turn;
  const peer = inboundRoutePeer(projected);
  const route = services.routing.resolveAgentRoute({
    cfg: task.cfg,
    channel: CHANNEL_ID,
    accountId: task.accountId,
    peer,
  });
  const sessionKey =
    (task.account.mode ?? "shared") === "shared"
      ? route.mainSessionKey
      : route.sessionKey;
  const ctxPayload = buildInboundContext(turn, route, sessionKey);
  return {
    cfg: task.cfg,
    channel: CHANNEL_ID,
    accountId: task.accountId,
    route: {
      agentId: route.agentId,
      sessionKey,
      ...(route.dmScope === undefined ? {} : { dmScope: route.dmScope }),
    },
    ctxPayload,
    delivery: { deliver: (payload) => withholdFinalText(task, payload) },
    record: {
      updateLastRoute: {
        sessionKey,
        channel: CHANNEL_ID,
        to: projected.address,
        accountId: task.accountId,
      },
    },
    messageId: projected.id,
  };
}

function buildInboundContext(
  turn: InboundTurn,
  route: ReturnType<ChannelServices["routing"]["resolveAgentRoute"]>,
  sessionKey: string,
) {
  const { task, services, projected } = turn;
  return services.inbound.buildContext({
    channel: CHANNEL_ID,
    accountId: task.accountId,
    provider: CHANNEL_ID,
    surface: CHANNEL_ID,
    messageId: projected.id,
    from: projected.sender.id,
    sender: inboundSenderFacts(projected),
    conversation: inboundConversationFacts(projected),
    route: {
      agentId: route.agentId,
      accountId: task.accountId,
      routeSessionKey: sessionKey,
      dispatchSessionKey: sessionKey,
      persistedSessionKey: sessionKey,
      mainSessionKey: route.mainSessionKey,
    },
    reply: inboundReplyFacts(projected),
    message: {
      body: projected.text,
      rawBody: projected.text,
      bodyForAgent: projected.text,
      commandBody: projected.text,
    },
    extra: inboundGroupFacts(projected),
  });
}

/**
 * Marks peers and Social Harness as bot senders in host metadata. This does
 * not grant their text authority or change routing and reply behavior.
 */
function inboundSenderFacts(projected: ProjectedEntry) {
  return { ...projected.sender, isBot: true };
}

function inboundConversationFacts(projected: ProjectedEntry) {
  const routePeer = inboundRoutePeer(projected);
  return {
    kind: projected.kind,
    id: projected.address,
    label: projected.address,
    routePeer,
  };
}

function inboundRoutePeer(projected: ProjectedEntry) {
  return routePeer(projected.kind, projected.address);
}

/** The peer one conversation's turns and sends both route by. */
function routePeer(kind: ProjectedEntry["kind"], address: string) {
  const prefix = kind === "group" ? "group:" : "agent:";
  return { kind, id: address.slice(prefix.length) };
}

function inboundReplyFacts(projected: ProjectedEntry) {
  return {
    to: projected.address,
    originatingTo: projected.address,
    replyTarget: projected.address,
    deliveryTarget: projected.address,
  };
}

function inboundGroupFacts(projected: ProjectedEntry) {
  return projected.members === undefined
    ? undefined
    : { GroupMembers: projected.members.join(",") };
}

/**
 * Withholds automatic replies so final assistant text stays private. A warning
 * records only the size when text reaches this callback, which can indicate a
 * missing message tool or a model reply that bypassed it.
 */
function withholdFinalText(
  task: ChannelGatewayContext<Account>,
  payload: ReplyPayload,
) {
  if (payload.text !== undefined && payload.text.length > 0) {
    task.log?.warn?.(
      `Social Harness: withheld ${payload.text.length} chars of final text; visible replies use the message tool`,
    );
  }
  return Promise.resolve({ visibleReplySent: false as const });
}

function clearConnectedAccount(
  accountSlot: ConnectedAccountSlot,
  connection: McpConnection,
): Effect.Effect<void> {
  return Effect.sync(() => {
    if (accountSlot.current?.connection === connection) {
      accountSlot.current = undefined;
    }
  });
}

function sendCoreText(
  accountSlot: ConnectedAccountSlot,
  ctx: ChannelMessageSendTextContext,
) {
  return sendText(accountSlot, "send.text", {
    accountId: ctx.accountId,
    text: ctx.text,
    to: ctx.to,
    ...(ctx.replyToIdSource === "explicit" && ctx.replyToId != null
      ? { replyToMsgId: ctx.replyToId }
      : {}),
  }).pipe(Effect.map(({ sent }) => makeMessageSendResult(sent)));
}

/**
 * Sends what the message text states and returns refusals as model-facing
 * tool errors. Each attempt logs its origin.
 */
function sendText(
  accountSlot: ConnectedAccountSlot,
  origin: OutboundOrigin,
  params: TextSend,
) {
  const prefix = `Social Harness: outbound via ${origin} to ${
    typeof params.to === "string" ? params.to.slice(0, LOG_PREVIEW_CHARS) : "-"
  }`;
  return Effect.gen(function* () {
    const { to, text } = yield* readTextSend(params);
    const connection = yield* requireConnection(accountSlot, params);
    const result = yield* sendMessage(
      connection.mcp,
      sendArguments(to, text, params.replyToMsgId),
    );
    if (result.isError === true) {
      return yield* new OpenClawChannelError({
        message: result.content
          .flatMap((part) => (part.type === "text" ? [part.text] : []))
          .join("\n"),
      });
    }
    const sent = yield* Schema.decodeUnknown(
      toolDefinitions.send_message.output,
    )(result.structuredContent).pipe(Effect.orDie);
    yield* logOutbound(
      accountSlot,
      `${prefix} id=${sent.id} chars=${text.length}: ${text.slice(0, LOG_PREVIEW_CHARS)}`,
    );
    return { to, sent };
  }).pipe(
    Effect.tapError((error) => logOutboundFailure(accountSlot, prefix, error)),
  );
}

/**
 * The `send_message` arguments a host send amounts to. Text that is a JSON
 * object with a `replyToMsgId` property is the tool's own arguments for a
 * reply, which the daemon validates. Any other text is a message's text, sent
 * with the reply reference the host gave.
 */
function sendArguments(
  to: MessageAddressInput,
  text: string,
  replyToMsgId?: string,
): Readonly<Record<string, unknown>> {
  const written = Option.getOrUndefined(decodeJsonObject(text));
  return written !== undefined && Object.hasOwn(written, "replyToMsgId")
    ? { ...written, to }
    : { to, text, ...(replyToMsgId === undefined ? {} : { replyToMsgId }) };
}

function logOutboundFailure(
  accountSlot: ConnectedAccountSlot,
  prefix: string,
  error: Error,
): Effect.Effect<void> {
  return Effect.sync(() => {
    accountSlot.current?.log?.warn?.(`${prefix} failed: ${error.message}`);
  });
}

/** Uses the connected account's gateway log. Disconnected sends have no log sink. */
function logOutbound(
  accountSlot: ConnectedAccountSlot,
  line: string,
): Effect.Effect<void> {
  return Effect.sync(() => {
    accountSlot.current?.log?.info?.(line);
  });
}

function requireConnection(
  accountSlot: ConnectedAccountSlot,
  params: TextSend,
): Effect.Effect<McpConnection, OpenClawChannelError> {
  const connection = connectionFor(accountSlot, params.accountId);
  return connection === undefined
    ? Effect.fail(refusedSend("account-not-connected"))
    : Effect.succeed(connection);
}

function readTextSend({
  to,
  text,
}: TextSend): Effect.Effect<
  { readonly to: MessageAddressInput; readonly text: string },
  OpenClawChannelError
> {
  if (!isMessageAddressInput(to)) {
    return Effect.fail(refusedSend("invalid-address"));
  }
  if (typeof text !== "string") {
    return Effect.fail(refusedSend("text-missing"));
  }
  return Effect.succeed({ to, text });
}

function refusedSend(reason: OpenClawOutboundFailure): OpenClawChannelError {
  return new OpenClawChannelError({
    message: `send failed: ${outboundFailureText[reason]}`,
  });
}

/** The host's send result carries the message id the send returned. */
function makeMessageSendResult(sent: SendResult): ChannelMessageSendResult {
  return {
    messageId: sent.id,
    receipt: createMessageReceiptFromOutboundResults({
      results: [
        {
          channel: CHANNEL_ID,
          messageId: sent.id,
          conversationId: sent.address,
        },
      ],
      kind: "text",
    }),
  };
}

function connectionFor(
  accountSlot: ConnectedAccountSlot,
  accountId?: string | null,
): McpConnection | undefined {
  const account = accountSlot.current;
  if (account === undefined) {
    return undefined;
  }
  const requested = accountId?.trim();
  if (
    requested !== undefined &&
    requested.length > 0 &&
    requested !== account.accountId
  ) {
    return undefined;
  }
  return account.connection;
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
