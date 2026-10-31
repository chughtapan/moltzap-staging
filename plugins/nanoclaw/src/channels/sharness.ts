/** @file Projects the public Social Harness endpoint capability into NanoClaw. */
import {
  acquireHarnessEndpoint,
  type CollectiveError,
  type ConnectError,
  type DeliveryAcknowledgeError,
  type FirewallSendError,
  groupMembers,
  type GuidanceBodyOrigin,
  type HarnessEndpoint,
  type InboundDelivery,
  type InboundItem,
  MessageAddressInput,
  parseMessageText,
  renderCollectiveRequest,
  renderCollectiveResult,
  renderContent,
  renderLocalGuidance,
  replyAddress,
  SendError,
} from "@moltzap/social-harness";
import {
  Config,
  ConfigProvider,
  Data,
  Deferred,
  Effect,
  Exit,
  Option,
  Redacted,
  Schema,
  Scope,
  Stream,
} from "effect";
import { absurd } from "effect/Function";
import { randomUUID } from "node:crypto";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- Native host plugins read one credential without requiring a platform service from the host.
import { readFile } from "node:fs/promises";
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

const channelEnv = Config.all({
  mcpEndpoint: Config.option(Config.string("SHARNESS_MCP_URL")).pipe(
    Config.map(Option.getOrNull),
  ),
});

interface Activation {
  readonly endpoint: HarnessEndpoint;
  readonly finished: Deferred.Deferred<undefined>;
  readonly scope: Scope.CloseableScope;
  readonly stopSignal: Deferred.Deferred<undefined>;
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

/** One outbound row's address and text, ready for the Client's parser. */
interface OutboundText {
  readonly to: MessageAddressInput;
  readonly text: string;
}

interface ProjectedItem {
  readonly origin: GuidanceBodyOrigin;
  readonly id: string;
  readonly address: MessageAddressInput;
  readonly sender: string;
  readonly text: string;
}

/**
 * Read one outbound row's address and text. The text is parsed separately,
 * so a refused operation text can reach the model rather than fail the row.
 */
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

/**
 * Read the text from NanoClaw's `messages_out` content: a bare string, or the
 * `text` of the object `send_message` writes.
 * @param message One outbound row as NanoClaw delivers it.
 * @returns The text, or null when the row carries none.
 */
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
 * Send failures whose message the daemon still delivers, `delivery-pending`, or
 * may already have sent, `outcome-unknown`. NanoClaw's retry is a new send
 * and would send the message a second time, so such a row completes even
 * when the host callback that reports it fails; the model, told the message
 * may have been sent, can resend it deliberately.
 */
const POSTED_REFUSALS: ReadonlySet<SendError["reason"]> = new Set([
  "delivery-pending",
  "outcome-unknown",
]);

/**
 * Send failures NanoClaw's retry must not repeat. The model hears of these;
 * any other failure stays with that retry. An invalid address or content
 * fails the same way again, and `POSTED_REFUSALS` says why the rest do.
 */
const FINAL_REFUSALS: ReadonlySet<SendError["reason"]> = new Set([
  "invalid-address",
  "unknown-agent",
  "membership-invalid",
  "content-invalid",
  ...POSTED_REFUSALS,
]);

function isFinalRefusal(error: unknown): error is SendError {
  return error instanceof SendError && FINAL_REFUSALS.has(error.reason);
}

/** The sender NanoClaw records for an item the endpoint itself emitted. */
const ENDPOINT_SENDER = "Social Harness";

/** A message Social Harness itself sends the model: a result or a refusal. */
function fromEndpoint(
  id: string,
  address: MessageAddressInput,
  text: string,
): InboundMessage {
  return endpointInbound({
    id,
    address,
    sender: ENDPOINT_SENDER,
    text,
    origin: "endpoint",
  });
}

/**
 * NanoClaw's inbox shape for any delivered message, question, result or
 * failure. A group address keeps the native group flag and lists its members.
 */
function endpointInbound(input: ProjectedItem): InboundMessage {
  const members = groupMembers(input.address);
  const isGroup = members.length > 0;
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
      ...(isGroup ? { members } : {}),
    },
    isGroup,
  };
}

/**
 * One endpoint scope lives from setup until the receive stream ends or
 * teardown completes. Successful host callbacks permit acknowledgment of
 * handoff. NanoClaw owns subsequent processing and retry policy.
 */
class HarnessChannelAdapter {
  readonly name = CHANNEL_ID;
  readonly channelType = CHANNEL_ID;
  readonly supportsThreads = false;

  private readonly lifecycleGate = Effect.runSync(Effect.makeSemaphore(1));
  private readonly mcpEndpoint: string;
  private activation: Activation | null = null;
  private setupConfig: ChannelSetup | null = null;

  constructor(mcpEndpoint: string) {
    this.mcpEndpoint = mcpEndpoint;
  }

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
  ): Effect.Effect<void, ConnectError | ChannelError> {
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
          stopSignal: yield* Deferred.make<undefined>(),
        };
        this.setupConfig = config;
        this.activation = activation;
        yield* endpoint.messages.pipe(
          Stream.onDone(() => this.beginStopping(activation)),
          Stream.onError(() => this.beginStopping(activation)),
          Stream.runForEach((delivery) => this.handleDelivery(delivery)),
          Effect.raceFirst(Deferred.await(activation.stopSignal)),
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
          Effect.zipRight(Deferred.succeed(activation.stopSignal, undefined)),
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
  ): Effect.Effect<HarnessEndpoint, ConnectError | ChannelError> {
    return Effect.try({
      try: () => new URL(this.mcpEndpoint),
      catch: () =>
        new ChannelError({
          reason: "Social Harness channel MCP endpoint is invalid",
        }),
    }).pipe(
      Effect.flatMap((endpoint) =>
        Effect.gen(function* () {
          const credentialFile = yield* Config.redacted(
            "SHARNESS_MCP_CREDENTIAL_FILE",
          ).pipe(
            Effect.catchTag("ConfigError", () =>
              Effect.fail(
                new ChannelError({
                  reason:
                    "SHARNESS_MCP_CREDENTIAL_FILE is required. Docs: https://docs.moltzap.xyz/guides/run-sharnessd#mcp-credential-file",
                }),
              ),
            ),
          );
          const credential = yield* Effect.tryPromise({
            try: () => readFile(Redacted.value(credentialFile), "utf8"),
            catch: () =>
              new ChannelError({
                reason:
                  "SHARNESS_MCP_CREDENTIAL_FILE must name a readable file. Docs: https://docs.moltzap.xyz/guides/run-sharnessd#mcp-credential-file",
              }),
          });
          return yield* acquireHarnessEndpoint(endpoint, {
            credential: Redacted.make(credential),
            receiveMode: "push",
          });
        }),
      ),
      Scope.extend(scope),
    );
  }

  private deliverEffect(
    address: string,
    message: OutboundMessage,
  ): Effect.Effect<
    void,
    ChannelError | SendError | CollectiveError | FirewallSendError
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
      Effect.flatMap(({ to, text }) =>
        parseMessageText(to, text).pipe(
          Effect.matchEffect({
            onFailure: (error) =>
              this.reportRefusedText(config, to, error.message),
            onSuccess: (input) =>
              activation.endpoint.send(input).pipe(
                Effect.asVoid,
                Effect.catchTag("CollectiveError", (error) =>
                  this.reportRefusedText(config, to, error.message),
                ),
                Effect.catchTag("FirewallSendError", (error) =>
                  this.reportRefusedText(config, to, error.message),
                ),
                Effect.catchIf(isFinalRefusal, (error) =>
                  this.reportFinalRefusal(config, to, error),
                ),
              ),
          }),
        ),
      ),
    );
  }

  /**
   * Report a send refused for a reason in `FINAL_REFUSALS`. A send in
   * `POSTED_REFUSALS` completes even when its report cannot reach the host,
   * since failing its row would hand it to NanoClaw's retry.
   * @param config The host callbacks the delivery started under.
   * @param to The conversation the text was sent to.
   * @param error The send's refusal.
   * @returns Completion once the report is handed to the host or, for a
   *     sent refusal, logged as unreported.
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
   * Hand a refused send back to the model as a Social Harness message in the
   * conversation it was sent to. `send_message` has already returned, so this
   * is how the model learns of it. The row completes rather than reaching
   * NanoClaw's retry: a text the parser refuses would fail the same way again,
   * and a send refused for a reason in `FINAL_REFUSALS` must not be repeated,
   * for the reasons it gives.
   * @param config The host callbacks the delivery started under. The channel
   *     can stop while the send runs, as when the daemon stops, and the report
   *     still reaches the host rather than failing the row into a retry.
   * @param to The conversation the text was sent to.
   * @param report The refusal's text.
   * @returns Completion after the host callback completed.
   */
  private reportRefusedText(
    config: ChannelSetup,
    to: MessageAddressInput,
    report: string,
  ): Effect.Effect<void, ChannelError> {
    return this.handToHost(
      config,
      to,
      fromEndpoint(`refused:${randomUUID()}`, to, report),
    );
  }

  private handleDelivery(
    delivery: InboundDelivery,
  ): Effect.Effect<void, ChannelError | DeliveryAcknowledgeError> {
    const config = this.setupConfig;
    if (config === null) {
      return Effect.void;
    }
    const projected = projectItem(delivery.item, delivery.eventId);
    let text = projected.text;
    if (delivery.guidance !== undefined) {
      text =
        delivery.item.kind === "collectiveRequest"
          ? renderCollectiveRequest(
              delivery.item,
              `Send the answer once as the whole text of send_message to ${replyAddress(delivery.item.message)}.`,
              delivery.guidance,
            )
          : renderLocalGuidance(delivery.guidance, {
              text: projected.text,
              origin: projected.origin,
            });
    }
    return this.handToHost(
      config,
      projected.address,
      endpointInbound({
        ...projected,
        text,
      }),
    ).pipe(Effect.zipRight(delivery.acknowledge));
  }

  /**
   * Hand one projected message to NanoClaw's main session, replying to its
   * Social Harness address.
   * @param config The host callbacks from the active setup.
   * @param address The Social Harness conversation the message belongs to.
   * @param inbound The message in NanoClaw's inbox shape.
   * @returns Completion after the host callback completed.
   */
  private handToHost(
    config: ChannelSetup,
    address: string,
    inbound: InboundMessage,
  ): Effect.Effect<void, ChannelError> {
    const isGroup = inbound.isGroup === true;
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
        config.onMetadata(address, address, isGroup);
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

/** Each item keeps its canonical attribution and conversation before guidance is added. */
function projectItem(item: InboundItem, eventId: string): ProjectedItem {
  switch (item.kind) {
    case "multicast":
      return {
        origin: "peer",
        id: eventId,
        address: replyAddress(item.message),
        sender: `agent:${item.message.author.agentName}`,
        text: renderContent(item.message.content),
      };
    case "collectiveRequest":
      return {
        origin: "peer",
        id: eventId,
        address: replyAddress(item.message),
        sender: `agent:${item.message.author.agentName}`,
        text: renderCollectiveRequest(
          item,
          `Send the answer once as the whole text of send_message to ${replyAddress(item.message)}.`,
        ),
      };
    case "collectiveResult":
      return {
        origin: "endpoint",
        id: eventId,
        address: item.to,
        sender: ENDPOINT_SENDER,
        text: renderCollectiveResult(item),
      };
    case "operationFailed":
      return {
        origin: "endpoint",
        id: eventId,
        address: item.to,
        sender: ENDPOINT_SENDER,
        text: item.error,
      };
    default:
      return absurd(item);
  }
}

function makeHarnessChannelAdapter(): HarnessChannelAdapter | null {
  const { mcpEndpoint } = Effect.runSync(
    channelEnv.pipe(Effect.withConfigProvider(ConfigProvider.fromEnv())),
  );
  return mcpEndpoint === null ? null : new HarnessChannelAdapter(mcpEndpoint);
}

registerChannelAdapter(CHANNEL_ID, {
  defaults: CHANNEL_DEFAULTS,
  factory: makeHarnessChannelAdapter,
});

/* eslint-enable agent-code-guard/promise-type, @typescript-eslint/no-invalid-void-type -- Restore Effect-first defaults after the host boundary. */
