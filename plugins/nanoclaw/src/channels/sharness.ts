/** @file Projects the public Social Harness endpoint capability into NanoClaw. */
import {
  type AgentCard,
  callToolAsWritten,
  connectFromEnvironment,
  type DeliveryAcknowledgeError,
  type FirewallRefusal,
  type InboxEntry,
  type McpConnectError,
  type McpConnection,
  MessageAddressInput,
  packageVersion,
  type RequestError,
  type SendError,
} from "@social-harness/social-harness";
import {
  absurd,
  Config,
  ConfigProvider,
  Data,
  Deferred,
  Effect,
  Exit,
  Option,
  Schema,
  Scope,
} from "effect";
import { randomUUID } from "node:crypto";
import type { ChannelSetup, InboundMessage } from "./adapter.js";
import { registerChannelAdapter } from "./channel-registry.js";

/* eslint-disable agent-code-guard/promise-type, @typescript-eslint/no-invalid-void-type -- NanoClaw's mirrored ChannelAdapter lifecycle is Promise-based. */

/** A NanoClaw host value cannot cross the addressed Client boundary. */
class ChannelError extends Data.TaggedError("ChannelError")<{
  readonly reason: string;
}> {
  override get message(): string {
    return this.reason;
  }
}

const CHANNEL_ID = "sharness";
const NANOCLAW_MAIN_CHANNEL = "cli";
const NANOCLAW_MAIN_PLATFORM_ID = "local";
const CHANNEL_DEFAULTS = Object.freeze({
  dm: {
    engageMode: "pattern" as const,
    engagePattern: ".",
    threads: false,
    unknownSenderPolicy: "public" as const,
  },
  group: {
    engageMode: "pattern" as const,
    engagePattern: ".",
    threads: false,
    unknownSenderPolicy: "public" as const,
  },
  mentions: "platform" as const,
});

/**
 * Client exposes no message time, so a fixed placeholder keeps replay payloads
 * identical.
 */
const INBOUND_TIMESTAMP = "1970-01-01T00:00:00.000Z";

interface Activation {
  readonly endpoint: McpConnection;
  readonly finished: Deferred.Deferred<undefined>;
  readonly scope: Scope.CloseableScope;
  state: "active" | "stopping";
}

interface OutboundFile {
  readonly filename: string;
  readonly data: Uint8Array;
}

interface OutboundMessage {
  readonly kind: string;
  readonly content: unknown;
  readonly files?: readonly OutboundFile[];
}

/** One outbound row's address and text. */
interface OutboundText {
  readonly to: MessageAddressInput;
  readonly text: string;
}

/**
 * What NanoClaw's inbox shows the model for one item or refusal. `members`
 * lists a group message's author and recipients.
 */
interface ProjectedItem {
  readonly id: string;
  readonly address: MessageAddressInput;
  readonly sender: string;
  readonly members?: readonly string[];
  readonly text: string;
}

const decodeJsonObject = Schema.decodeUnknownOption(
  Schema.parseJson(
    Schema.Record({ key: Schema.String, value: Schema.Unknown }),
  ),
);

/**
 * The `send_message` arguments an outbound row amounts to. Text that is a
 * JSON object with a `collective` or `collectiveResponse` property is the
 * tool's own arguments for a question or an answer, which the daemon
 * validates. Any other text is a plain message.
 */
function sendArguments({
  to,
  text,
}: OutboundText): Readonly<Record<string, unknown>> {
  const written = Option.getOrUndefined(decodeJsonObject(text));
  return written !== undefined &&
    (Object.hasOwn(written, "collective") ||
      Object.hasOwn(written, "collectiveResponse"))
    ? { ...written, to }
    : { to, text };
}

/** Reads one outbound row's address and text. */
function decodeOutboundText(
  address: string,
  message: OutboundMessage,
): Effect.Effect<OutboundText, ChannelError> {
  return decodeOutboundOperation(message).pipe(
    Effect.flatMap((text) =>
      Schema.decodeUnknown(MessageAddressInput)(address).pipe(
        Effect.map((to) => ({ to, text })),
      ),
    ),
    Effect.catchTag("ParseError", () =>
      Effect.fail(
        new ChannelError({
          reason:
            "Social Harness outbound delivery requires an explicit agent or group address",
        }),
      ),
    ),
  );
}

function decodeOutboundOperation(
  message: OutboundMessage,
): Effect.Effect<string, ChannelError> {
  if (message.kind !== "chat") {
    return Effect.fail(
      new ChannelError({
        reason:
          "Social Harness outbound messages must use NanoClaw chat delivery",
      }),
    );
  }
  if (message.files !== undefined && message.files.length > 0) {
    return Effect.fail(
      new ChannelError({
        reason: "Social Harness outbound messages do not accept native files",
      }),
    );
  }
  const text = extractOutboundText(message);
  return text === null
    ? Effect.fail(
        new ChannelError({
          reason: "Social Harness outbound messages require text content",
        }),
      )
    : Effect.succeed(text);
}

/** Accepts a bare string or the text field NanoClaw's send_message writes. */
function extractOutboundText(message: OutboundMessage): string | null {
  const content = message.content;
  if (typeof content === "string") {
    return content;
  }
  if (content === null || typeof content !== "object") {
    return null;
  }
  return "text" in content && typeof content.text === "string"
    ? content.text
    : null;
}

/**
 * Completes uncertain sends even if feedback fails, because a host retry
 * would create a new send. The model can choose to resend deliberately.
 */
const POSTED_REFUSALS: ReadonlySet<SendError["reason"]> = new Set([
  "outcome-unknown",
]);

/**
 * Reports these failures to the model instead of asking NanoClaw to retry.
 * Invalid input would fail again. POSTED_REFUSALS covers uncertain sends.
 */
const FINAL_REFUSALS: ReadonlySet<SendError["reason"]> = new Set([
  "invalid-address",
  "unknown-agent",
  "membership-invalid",
  "content-invalid",
  ...POSTED_REFUSALS,
]);

/** The sender NanoClaw records for an item the endpoint itself emitted. */
const ENDPOINT_SENDER = "Social Harness";

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
 * Who an entry is from and, for a group message or question, who is in the
 * group. A result is from Social Harness and names only the agents asked, so
 * it lists no members.
 */
function entryParticipants(
  entry: InboxEntry,
): Pick<ProjectedItem, "sender" | "members"> {
  const { item } = entry;
  switch (item.kind) {
    case "multicast":
    case "collectiveRequest": {
      const { author, recipients } = item.message;
      const sender = agentAddress(author);
      return isGroup(entry.address)
        ? { sender, members: [sender, ...recipients.map(agentAddress)] }
        : { sender };
    }
    case "collectiveResult":
      return { sender: ENDPOINT_SENDER };
    default:
      return absurd(item);
  }
}

/** NanoClaw's inbox shape, with the native group flag for a group address. */
function hostMessage(input: ProjectedItem): InboundMessage {
  return {
    id: input.id,
    kind: "chat",
    timestamp: INBOUND_TIMESTAMP,
    isMention: true,
    content: {
      text: input.text,
      address: input.address,
      sender: input.sender,
      senderId: input.sender,
      ...(input.members === undefined ? {} : { members: input.members }),
    },
    isGroup: isGroup(input.address),
  };
}

function agentAddress(agent: AgentCard): string {
  return `agent:${agent.agentName}`;
}

function isGroup(address: MessageAddressInput): boolean {
  return address.startsWith("group:");
}

/**
 * One MCP connection lives from setup until reception ends or teardown completes.
 * Inbox reads record delivery before native callbacks. NanoClaw owns subsequent
 * processing and retry policy.
 */
class HarnessChannelAdapter {
  readonly name = CHANNEL_ID;
  readonly channelType = CHANNEL_ID;
  readonly supportsThreads = false;

  private readonly lifecycleGate = Effect.runSync(Effect.makeSemaphore(1));
  private activation: Activation | null = null;
  private setupConfig: ChannelSetup | null = null;

  setup(config: ChannelSetup): Promise<void> {
    return Effect.runPromise(
      this.lifecycleGate.withPermits(1)(
        Effect.suspend(() => this.start(config)),
      ),
    );
  }

  teardown(): Promise<void> {
    return Effect.runPromise(
      this.lifecycleGate.withPermits(1)(Effect.suspend(() => this.stop())),
    );
  }

  isConnected(): boolean {
    return this.activation?.state === "active";
  }

  deliver(
    platformId: string,
    ...args: [threadId: string | null, message: OutboundMessage]
  ): Promise<string | undefined> {
    return Effect.runPromise(
      this.deliverEffect(platformId, args[1]).pipe(Effect.as(undefined)),
    );
  }

  private start(
    config: ChannelSetup,
  ): Effect.Effect<void, McpConnectError | ChannelError> {
    const current = this.activation;
    if (current?.state === "active") {
      this.setupConfig = config;
      return Effect.void;
    }
    if (current?.state === "stopping") {
      return Deferred.await(current.finished).pipe(
        Effect.zipRight(Effect.suspend(() => this.start(config))),
      );
    }
    return Effect.gen(
      function* (this: HarnessChannelAdapter) {
        const scope = yield* Scope.make();
        const endpoint = yield* this.acquireEndpoint(scope).pipe(
          Effect.onError(() => Scope.close(scope, Exit.void)),
        );
        const activation: Activation = {
          endpoint,
          finished: yield* Deferred.make<undefined>(),
          scope,
          state: "active",
        };
        this.setupConfig = config;
        this.activation = activation;
        yield* endpoint
          .consumeInbox((delivery) => this.handleDelivery(delivery))
          .pipe(
            Effect.ensuring(this.finishActivation(activation)),
            Effect.forkDaemon,
          );
      }.bind(this),
    ).pipe(Effect.asVoid);
  }

  private stop(): Effect.Effect<void> {
    const activation = this.activation;
    return activation === null
      ? Effect.void
      : this.beginStopping(activation).pipe(
          Effect.zipRight(Scope.close(activation.scope, Exit.void)),
          Effect.zipRight(Deferred.await(activation.finished)),
        );
  }

  private finishActivation(activation: Activation): Effect.Effect<void> {
    return this.beginStopping(activation).pipe(
      Effect.zipRight(Scope.close(activation.scope, Exit.void)),
      Effect.zipRight(
        Effect.sync(() => {
          if (this.activation === activation) {
            this.activation = null;
            this.setupConfig = null;
          }
        }),
      ),
      Effect.ensuring(Deferred.succeed(activation.finished, undefined)),
      Effect.asVoid,
    );
  }

  private beginStopping(activation: Activation): Effect.Effect<void> {
    return Effect.sync(() => {
      if (this.activation === activation) {
        activation.state = "stopping";
        this.setupConfig = null;
      }
    });
  }

  private acquireEndpoint(
    scope: Scope.CloseableScope,
  ): Effect.Effect<McpConnection, McpConnectError | ChannelError> {
    return connectFromEnvironment({
      name: "sharness-nanoclaw",
      version: packageVersion,
    }).pipe(
      Effect.catchTag("McpSettingsError", ({ message }) =>
        Effect.fail(new ChannelError({ reason: message })),
      ),
      Scope.extend(scope),
    );
  }

  private deliverEffect(
    address: string,
    message: OutboundMessage,
  ): Effect.Effect<
    void,
    ChannelError | SendError | RequestError | FirewallRefusal
  > {
    const activation = this.activation;
    const config = this.setupConfig;
    if (activation?.state !== "active" || config === null) {
      return Effect.fail(
        new ChannelError({
          reason: "Social Harness channel is not connected",
        }),
      );
    }
    return decodeOutboundText(address, message).pipe(
      Effect.flatMap((outbound) =>
        callToolAsWritten(
          activation.endpoint.client,
          "send_message",
          sendArguments(outbound),
        ).pipe(
          Effect.asVoid,
          Effect.catchTag("RequestError", (error) =>
            this.reportRefusedText(config, outbound.to, error.message),
          ),
          Effect.catchTag("FirewallRefusal", (error) =>
            this.reportRefusedText(config, outbound.to, error.message),
          ),
          Effect.catchTag(
            "SendError",
            (error): Effect.Effect<void, ChannelError | SendError> =>
              FINAL_REFUSALS.has(error.reason)
                ? this.reportFinalRefusal(config, outbound.to, error)
                : Effect.fail(error),
          ),
        ),
      ),
    );
  }

  /**
   * Completes POSTED_REFUSALS even when feedback fails, so NanoClaw cannot
   * repeat a send that may already have reached its destination.
   */
  private reportFinalRefusal(
    config: ChannelSetup,
    to: MessageAddressInput,
    error: SendError,
  ): Effect.Effect<void, ChannelError> {
    const report = this.reportRefusedText(config, to, error.message);
    return POSTED_REFUSALS.has(error.reason)
      ? report.pipe(
          Effect.catchAll((unreported) =>
            Effect.logWarning(
              `Social Harness completed a ${error.reason} send to ${to} without reporting it: ${unreported.message}`,
            ),
          ),
        )
      : report;
  }

  /**
   * Returns refused sends to the model through the captured host callback, even
   * if the channel stops meanwhile. The tool has already returned. Successful
   * feedback completes the row instead of retrying unchanged refused input.
   */
  private reportRefusedText(
    config: ChannelSetup,
    to: MessageAddressInput,
    report: string,
  ): Effect.Effect<void, ChannelError> {
    return this.handToHost(
      config,
      to,
      hostMessage({
        id: `refused:${randomUUID()}`,
        address: to,
        sender: ENDPOINT_SENDER,
        text: report,
      }),
    );
  }

  private handleDelivery(
    delivery: InboxEntry,
  ): Effect.Effect<void, ChannelError | DeliveryAcknowledgeError> {
    const config = this.setupConfig;
    if (config === null) {
      return Effect.void;
    }
    return this.handToHost(
      config,
      delivery.address,
      hostMessage({
        id: delivery.deliveryToken,
        address: delivery.address,
        ...entryParticipants(delivery),
        text: entryText(delivery),
      }),
    );
  }

  /**
   * Awaits handoff to the main session with a reply route to the Social Harness
   * conversation.
   */
  private handToHost(
    config: ChannelSetup,
    address: string,
    inbound: InboundMessage,
  ): Effect.Effect<void, ChannelError> {
    const content = JSON.stringify(inbound.content);
    if (content === undefined) {
      return Effect.fail(
        new ChannelError({
          reason: `NanoClaw could not serialize inbound content for ${address}`,
        }),
      );
    }
    return Effect.tryPromise({
      try: () => {
        config.onMetadata(address, address, inbound.isGroup === true);
        return Promise.resolve(
          config.onInboundEvent({
            channelType: NANOCLAW_MAIN_CHANNEL,
            instance: NANOCLAW_MAIN_CHANNEL,
            platformId: NANOCLAW_MAIN_PLATFORM_ID,
            threadId: null,
            message: {
              ...inbound,
              content,
            },
            replyTo: {
              channelType: CHANNEL_ID,
              platformId: address,
              threadId: null,
            },
          }),
        );
      },
      catch: (cause) =>
        new ChannelError({
          reason: `NanoClaw inbound callback failed for ${address}: ${String(cause)}`,
        }),
    }).pipe(Effect.asVoid);
  }
}

/** The channel exists only in a host that is told where its daemon is. */
function makeHarnessChannelAdapter(): HarnessChannelAdapter | null {
  const configured = Effect.runSync(
    Config.option(Config.string("SHARNESS_MCP_URL")).pipe(
      Effect.withConfigProvider(ConfigProvider.fromEnv()),
    ),
  );
  return Option.isNone(configured) ? null : new HarnessChannelAdapter();
}

registerChannelAdapter(CHANNEL_ID, {
  defaults: CHANNEL_DEFAULTS,
  factory: makeHarnessChannelAdapter,
});

/* eslint-enable agent-code-guard/promise-type, @typescript-eslint/no-invalid-void-type -- Restore Effect-first defaults after the host boundary. */
