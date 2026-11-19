/**
 * @file The members of NanoClaw v2.3.0's channel adapter interface that this
 * channel uses. The host ABI carries Node's `Buffer`, and the preserved types
 * directive below travels into the published declaration so a consumer
 * resolves it from this package's own `@types/node`.
 */

/* eslint-disable agent-code-guard/promise-type, @typescript-eslint/no-invalid-void-type, jsdoc/require-description-complete-sentence -- This file mirrors NanoClaw's Promise-native host ABI and upstream API prose. */
// eslint-disable-next-line @typescript-eslint/triple-slash-reference -- An import of `Buffer` is erased from the emitted declaration; only a preserved directive reaches consumers.
/// <reference types="node" preserve="true" />

/** Passed to the adapter at setup time. */
export interface ChannelSetup {
  /**
   * Called by admin-transport adapters (CLI) that want to route a message to
   * an arbitrary channel/platform and optionally redirect replies elsewhere.
   * Regular chat adapters should use `onInbound`; `onInboundEvent` skips the
   * adapter-channel-type injection so the caller can target any wired mg.
   */
  onInboundEvent(event: InboundEvent): void | Promise<void>;

  /** Called when the adapter discovers metadata about a conversation. */
  onMetadata(platformId: string, name?: string, isGroup?: boolean): void;
}

/** Delivery address used for reply-to overrides and (normally) the inbound's own origin. */
interface DeliveryAddress {
  channelType: string;
  platformId: string;
  threadId: string | null;
}

/**
 * Full inbound event handed to the router.
 *
 * `channelType` + `platformId` + `threadId` identify which messaging group /
 * session receives the message. `replyTo`, when set, overrides where the
 * agent's reply is delivered — used by the CLI admin transport when the
 * operator wants a message routed to one channel but replies echoed back to
 * their terminal. Agents cannot set `replyTo`; it is a router-layer concept
 * set only by external adapters carrying operator intent.
 */
interface InboundEvent {
  channelType: string;
  /** Receiving adapter instance; stamped host-side (src/index.ts onInbound) or
   *  carried by the CLI admin transport's `to.instance`. Absent means the
   *  default instance (= channelType). */
  instance?: string;
  platformId: string;
  threadId: string | null;
  message: {
    id: string;
    kind: "chat" | "chat-sdk";
    content: string; // JSON blob
    timestamp: string;
    /**
     * Platform-confirmed bot-mention signal forwarded from the adapter.
     * See InboundMessage.isMention for the full explanation.
     */
    isMention?: boolean;
    /** True when the source is a group/channel thread, false for DMs. */
    isGroup?: boolean;
  };
  replyTo?: DeliveryAddress;
}

/** Inbound message from adapter to host. */
export interface InboundMessage {
  id: string;
  kind: "chat" | "chat-sdk";
  content: unknown; // JS object — host will JSON.stringify before writing to session DB
  timestamp: string;
  /**
   * Platform-confirmed signal that this message is a mention of the bot.
   *
   * Set by adapters that know the platform's own mention semantics — e.g.
   * the Chat SDK bridge sets it true from `onNewMention` / `onDirectMessage`
   * and forwards `message.isMention` from `onSubscribedMessage`. Use this
   * in the router instead of agent-name regex matching, which breaks on
   * platforms where the mention text is the bot's platform username (e.g.
   * Telegram's `@nanoclaw_v2_refactr_1_bot`) rather than the agent_group
   * display name (e.g. `@Andy`).
   *
   * Adapters that omit it leave it undefined. The router treats undefined as
   * "not a mention" (`isMention === true` check, src/router.ts); there is no
   * text-match fallback.
   */
  isMention?: boolean;
  /** True when the source is a group/channel thread, false for DMs. */
  isGroup?: boolean;
}

/** A file attachment to deliver alongside a message. */
interface OutboundFile {
  filename: string;
  data: Buffer;
}

/** Outbound message from host to adapter. */
interface OutboundMessage {
  kind: string;
  content: unknown; // parsed JSON from messages_out
  files?: OutboundFile[]; // file attachments from the session outbox
}

/** Wiring/mg defaults for one conversation context (DM vs group/channel). */
interface ChannelContextDefaults {
  /** Default engage_mode for wirings created in this context. */
  engageMode: "pattern" | "mention" | "mention-sticky";
  /**
   * Default engage_pattern when engageMode === 'pattern'. May contain the
   * literal token `{name}`: creation helpers replace it with the regex-escaped
   * agent_group name (for platforms with no group-mention metadata, e.g.
   * iMessage/DeltaChat groups, WhatsApp shared-number mode). Required iff
   * engageMode === 'pattern'.
   */
  engagePattern?: string;
  /**
   * Whether thread ids are honored in this context by default.
   *  true  — inbound thread ids flow into messages_in and (in groups) force
   *          per-thread session identity; replies, typing, and cards land
   *          in-thread.
   *  false — thread ids are nulled per-wiring at router fanout; sessions
   *          collapse; replies land top-level.
   * MUST be false when `supportsThreads` is false (capability bound; the
   * router treats supportsThreads=false as a hard pre-strip regardless).
   * Per-wiring override: messaging_group_agents.threads (NULL = inherit).
   */
  threads: boolean;
  /**
   * unknown_sender_policy stamped on messaging_groups rows auto-created by
   * the router or created by wizard/CLI paths in this context.
   * 'decline_notify' (DM-shaped channels): the host politely declines the
   * unknown sender in-channel and sends the owner a one-line FYI — no
   * approval card; access grants stay explicit (`ncl members add`).
   */
  unknownSenderPolicy:
    | "strict"
    | "request_approval"
    | "decline_notify"
    | "public";
}

/**
 * Static per-channel declaration of wiring-time defaults. Exactly two levels
 * exist: this declaration, and the per-wiring/per-mg values chosen at
 * creation. Install-wide changes = edit the adapter copy (skill-installed,
 * user-owned). Never persisted to the central DB.
 */
interface ChannelDefaults {
  dm: ChannelContextDefaults;
  group: ChannelContextDefaults;
  /**
   * Which mention signal the adapter emits (InboundMessage.isMention):
   *  'platform' — platform-confirmed mentions in groups; DMs flagged too.
   *  'dm-only'  — only DMs flagged (no group mention metadata).
   *  'never'    — isMention never set: auto-create/registration card never
   *               fires; 'mention'/'mention-sticky' wirings never engage.
   * Creation surfaces must reject/warn on mention modes that can never fire.
   */
  mentions: "platform" | "dm-only" | "never";
}

/** The v2 channel adapter contract. */
interface ChannelAdapter {
  name: string;
  channelType: string;

  /**
   * Whether this adapter models conversations as threads.
   *
   * true  — adapter's platform uses threads as the primary conversation unit
   *         (Discord, Slack, Linear, GitHub). One thread = one session; the
   *         agent replies into the originating thread.
   * false — adapter's platform treats the channel itself as the conversation
   *         (Telegram, WhatsApp, iMessage). Thread ids are stripped at the
   *         router; agent replies go to the channel.
   */
  supportsThreads: boolean;

  // Lifecycle
  setup(config: ChannelSetup): Promise<void>;
  teardown(): Promise<void>;
  isConnected(): boolean;

  // Outbound delivery — returns the platform message ID if available
  deliver(
    platformId: string,
    threadId: string | null,
    message: OutboundMessage,
  ): Promise<string | undefined>;
}

/** Factory function that creates a channel adapter (returns null if credentials missing). */
type ChannelAdapterFactory = () =>
  | ChannelAdapter
  | Promise<ChannelAdapter>
  | null;

/** Registration entry for a channel adapter. */
export interface ChannelRegistration {
  factory: ChannelAdapterFactory;
  /**
   * The channel's wiring-time defaults, resolvable WITHOUT instantiating the
   * adapter — offline creation paths (setup/register.ts,
   * scripts/init-first-agent.ts, ncl against a host where the factory
   * returned null for missing creds) read it from the registry.
   */
  defaults?: ChannelDefaults;
}

/* eslint-enable agent-code-guard/promise-type, @typescript-eslint/no-invalid-void-type, jsdoc/require-description-complete-sentence -- Restore repository defaults after the host ABI mirror. */
