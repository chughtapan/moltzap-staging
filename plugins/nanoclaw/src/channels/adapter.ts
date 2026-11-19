/**
 * @file The members of NanoClaw v2.3.0's channel adapter interface that this
 * channel uses. The agent image compiles the channel against NanoClaw's own
 * module of this name; this one lets the package compile alone.
 */

/* eslint-disable agent-code-guard/promise-type, @typescript-eslint/no-invalid-void-type -- This file mirrors NanoClaw's Promise-native host ABI. */

/** The host callbacks an adapter receives at setup. */
export interface ChannelSetup {
  /** Routes a message to any channel and platform, with replies sent to `replyTo`. */
  onInboundEvent(event: InboundEvent): void | Promise<void>;
  onMetadata(platformId: string, name?: string, isGroup?: boolean): void;
}

interface DeliveryAddress {
  channelType: string;
  platformId: string;
  threadId: string | null;
}

/**
 * `channelType`, `instance`, `platformId` and `threadId` name the session that
 * receives the message. `content` is JSON text.
 */
interface InboundEvent {
  channelType: string;
  instance?: string;
  platformId: string;
  threadId: string | null;
  message: Omit<InboundMessage, "content"> & { content: string };
  replyTo?: DeliveryAddress;
}

/** A message from an adapter to the host. The host routes by `isMention`, never by matching names in the text. */
export interface InboundMessage {
  id: string;
  kind: "chat" | "chat-sdk";
  content: unknown;
  timestamp: string;
  isMention?: boolean;
  isGroup?: boolean;
}

interface OutboundFile {
  filename: string;
  data: Uint8Array;
}

/** `content` is the parsed JSON of a `messages_out` row. */
interface OutboundMessage {
  kind: string;
  content: unknown;
  files?: OutboundFile[];
}

/** The defaults of a wiring created for a direct or a group conversation. */
interface ChannelContextDefaults {
  engageMode: "pattern" | "mention" | "mention-sticky";
  engagePattern?: string;
  threads: boolean;
  unknownSenderPolicy:
    | "strict"
    | "request_approval"
    | "decline_notify"
    | "public";
}

interface ChannelDefaults {
  dm: ChannelContextDefaults;
  group: ChannelContextDefaults;
  mentions: "platform" | "dm-only" | "never";
}

interface ChannelAdapter {
  name: string;
  channelType: string;
  supportsThreads: boolean;
  setup(config: ChannelSetup): Promise<void>;
  teardown(): Promise<void>;
  isConnected(): boolean;
  /** Resolves to the platform's message id when it has one. */
  deliver(
    platformId: string,
    threadId: string | null,
    message: OutboundMessage,
  ): Promise<string | undefined>;
}

/** Returns null when the channel is not configured. */
type ChannelAdapterFactory = () =>
  | ChannelAdapter
  | Promise<ChannelAdapter>
  | null;

/** What a channel registers: its factory, and the defaults the host reads without creating the adapter. */
export interface ChannelRegistration {
  factory: ChannelAdapterFactory;
  defaults?: ChannelDefaults;
}

/* eslint-enable agent-code-guard/promise-type, @typescript-eslint/no-invalid-void-type -- Restore repository defaults after the host ABI mirror. */
