/** @file NanoClaw channel that hands inbox entries to the host and sends what its model writes. */
import {
  type AgentCard,
  connectFromEnvironment,
  type InboxEntry,
  type McpConnectError,
  type McpConnection,
  MessageAddressInput,
  packageVersion,
  sendMessage,
} from "@social-harness/social-harness";
import {
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

/** The channel cannot send what NanoClaw supplied, or hand NanoClaw an inbox entry. */
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
 * An inbox entry carries no message time, so a fixed placeholder keeps replay
 * payloads identical.
 */
const INBOUND_TIMESTAMP = "1970-01-01T00:00:00.000Z";

interface Activation {
  readonly connection: McpConnection;
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
 * What NanoClaw receives for one inbox entry or one refused send. `id` is the
 * message id, or a fresh id for a refused send. `address` is the
 * conversation replies go to. `members` lists a group message's author and
 * recipients. `text` is what the model reads.
 */
interface ProjectedEntry {
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
 * JSON object with a `replyToMsgId` property is the tool's own arguments for
 * a reply, which the daemon validates. Any other text is a message's text.
 */
function sendArguments({
  to,
  text,
}: OutboundText): Readonly<Record<string, unknown>> {
  const written = Option.getOrUndefined(decodeJsonObject(text));
  return written !== undefined && Object.hasOwn(written, "replyToMsgId")
    ? { ...written, to }
    : { to, text };
}

/** Reads one outbound row's address and text. */
function decodeOutboundText(
  address: string,
  message: OutboundMessage,
): Effect.Effect<OutboundText, ChannelError> {
  return decodeChatText(message).pipe(
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

function decodeChatText(
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
 * The reasons of a failed send that is reported to the model instead of
 * failing NanoClaw's queue item, which NanoClaw would retry. A refusal or
 * invalid input would fail again, and a retry of an `outcome-unknown` send
 * would create a new send.
 */
const FINAL_REFUSALS: ReadonlySet<string> = new Set([
  "invalid-arguments",
  "unknown-agent",
  "membership-invalid",
  "content-invalid",
  "firewall-refused",
  "outcome-unknown",
]);

/** Reads the reason a failed result's structured content names. */
const failureReason = Schema.decodeUnknownOption(
  Schema.Struct({ reason: Schema.String }),
);

/** The sender NanoClaw records for what no agent authored: a refused send. */
const HARNESS_SENDER_NAME = "Social Harness";

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
): Pick<ProjectedEntry, "sender" | "members"> {
  const { author, recipients } = entry.message;
  const sender = agentAddress(author);
  return isGroup(entry.address)
    ? { sender, members: [sender, ...recipients.map(agentAddress)] }
    : { sender };
}

/** NanoClaw's inbox shape, with the native group flag for a group address. */
function hostMessage(projected: ProjectedEntry): InboundMessage {
  return {
    id: projected.id,
    kind: "chat",
    timestamp: INBOUND_TIMESTAMP,
    isMention: true,
    content: {
      text: projected.text,
      address: projected.address,
      sender: projected.sender,
      senderId: projected.sender,
      ...(projected.members === undefined
        ? {}
        : { members: projected.members }),
    },
    isGroup: isGroup(projected.address),
  };
}

function agentAddress(agent: AgentCard): string {
  return `agent:${agent.agentName}`;
}

function isGroup(address: MessageAddressInput): boolean {
  return address.startsWith("group:");
}

/**
 * One daemon connection lives from setup until reception ends or teardown completes.
 * A page of inbox entries is committed after the native callback has returned
 * for each. NanoClaw owns subsequent processing and retry policy.
 */
class HarnessChannelAdapter {
  readonly name = CHANNEL_ID;
  readonly channelType = CHANNEL_ID;
  readonly supportsThreads = false;

  private readonly lifecycleGate = Effect.runSync(Effect.makeSemaphore(1));
  private activation: Activation | null = null;
  private host: ChannelSetup | null = null;

  setup(host: ChannelSetup): Promise<void> {
    return Effect.runPromise(
      this.lifecycleGate.withPermits(1)(Effect.suspend(() => this.start(host))),
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
      this.sendOutbound(platformId, args[1]).pipe(Effect.as(undefined)),
    );
  }

  private start(
    host: ChannelSetup,
  ): Effect.Effect<void, McpConnectError | ChannelError> {
    const current = this.activation;
    if (current?.state === "active") {
      this.host = host;
      return Effect.void;
    }
    if (current?.state === "stopping") {
      return Deferred.await(current.finished).pipe(
        Effect.zipRight(Effect.suspend(() => this.start(host))),
      );
    }
    return Effect.gen(
      function* (this: HarnessChannelAdapter) {
        const scope = yield* Scope.make();
        const connection = yield* this.connectToDaemon(scope).pipe(
          Effect.onError(() => Scope.close(scope, Exit.void)),
        );
        const activation: Activation = {
          connection,
          finished: yield* Deferred.make<undefined>(),
          scope,
          state: "active",
        };
        this.host = host;
        this.activation = activation;
        yield* connection
          .consumeInbox((entry) => this.handleInboxEntry(entry))
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
            this.host = null;
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
        this.host = null;
      }
    });
  }

  private connectToDaemon(
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

  private sendOutbound(
    address: string,
    message: OutboundMessage,
  ): Effect.Effect<void, ChannelError> {
    const activation = this.activation;
    const host = this.host;
    if (activation?.state !== "active" || host === null) {
      return Effect.fail(
        new ChannelError({
          reason: "Social Harness channel is not connected",
        }),
      );
    }
    return Effect.gen(
      function* (this: HarnessChannelAdapter) {
        const outbound = yield* decodeOutboundText(address, message);
        const result = yield* sendMessage(
          activation.connection.mcp,
          sendArguments(outbound),
        );
        if (result.isError === true) {
          yield* this.reportFailedSend(host, outbound.to, result);
        }
      }.bind(this),
    );
  }

  /**
   * Tells the model what a failed send's result says. A failure whose reason
   * is not final fails the queue item instead. An `outcome-unknown` send
   * completes even when the report fails, so NanoClaw cannot repeat a send
   * that may already have reached its recipients; the model can choose to
   * resend deliberately.
   */
  private reportFailedSend(
    host: ChannelSetup,
    to: MessageAddressInput,
    result: Effect.Effect.Success<ReturnType<typeof sendMessage>>,
  ): Effect.Effect<void, ChannelError> {
    const text = result.content
      .flatMap((part) => (part.type === "text" ? [part.text] : []))
      .join("\n");
    const reason = Option.getOrUndefined(
      failureReason(result.structuredContent),
    )?.reason;
    if (reason === undefined || !FINAL_REFUSALS.has(reason)) {
      return Effect.fail(new ChannelError({ reason: text }));
    }
    const report = this.reportRefusedText(host, to, text);
    return reason === "outcome-unknown"
      ? report.pipe(
          Effect.catchAll((unreported) =>
            Effect.logWarning(
              `Social Harness completed a ${reason} send to ${to} without reporting it: ${unreported.message}`,
            ),
          ),
        )
      : report;
  }

  /**
   * Returns refused sends to the model through the captured host callback, even
   * if the channel stops meanwhile. The tool has already returned. A successful
   * report completes the row instead of retrying the same refused text.
   */
  private reportRefusedText(
    host: ChannelSetup,
    to: MessageAddressInput,
    report: string,
  ): Effect.Effect<void, ChannelError> {
    return this.handToHost(
      host,
      to,
      hostMessage({
        id: `refused:${randomUUID()}`,
        address: to,
        sender: HARNESS_SENDER_NAME,
        text: report,
      }),
    );
  }

  private handleInboxEntry(
    entry: InboxEntry,
  ): Effect.Effect<void, ChannelError> {
    const host = this.host;
    if (host === null) {
      return Effect.void;
    }
    return this.handToHost(
      host,
      entry.address,
      hostMessage({
        id: entry.message.msgId,
        address: entry.address,
        ...entryParticipants(entry),
        text: entryText(entry),
      }),
    );
  }

  /**
   * Awaits handoff to the main session with a reply route to the Social Harness
   * conversation.
   */
  private handToHost(
    host: ChannelSetup,
    address: string,
    inbound: InboundMessage,
  ): Effect.Effect<void, ChannelError> {
    return Effect.tryPromise({
      try: () => {
        host.onMetadata(address, address, inbound.isGroup === true);
        return Promise.resolve(
          host.onInboundEvent({
            channelType: NANOCLAW_MAIN_CHANNEL,
            instance: NANOCLAW_MAIN_CHANNEL,
            platformId: NANOCLAW_MAIN_PLATFORM_ID,
            threadId: null,
            message: {
              ...inbound,
              content: JSON.stringify(inbound.content),
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
