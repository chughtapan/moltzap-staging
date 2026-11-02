/** @license MIT. Effect contributors. See LICENSE and README.md in this directory. */
/** @file Defines MCP requests, results and notifications. */

import * as Rpc from "@effect/rpc/Rpc";
import * as RpcGroup from "@effect/rpc/RpcGroup";
import * as RpcMiddleware from "@effect/rpc/RpcMiddleware";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

/** A string or numeric JSON-RPC request ID. */
export const RequestId: Schema.Union<
  [typeof Schema.String, typeof Schema.Number]
> = Schema.Union(Schema.String, Schema.Number);

/** A string or numeric JSON-RPC request ID. */
export type RequestId = typeof RequestId.Type;

/** Associates progress notifications with the original request. */
export const ProgressToken: Schema.Union<
  [typeof Schema.String, typeof Schema.Number]
> = Schema.Union(Schema.String, Schema.Number);

/** Associates progress notifications with the original request. */
export type ProgressToken = typeof ProgressToken.Type;

/** Optional metadata attached to a client request. */
export class RequestMeta extends Schema.Struct({
  _meta: Schema.optional(
    Schema.Struct({
      /** Requests optional progress notifications carrying this token. */
      progressToken: Schema.optional(ProgressToken),
    }),
  ),
}) {}

/** Metadata that a server can attach to a result. */
export class ResultMeta extends Schema.Struct({
  _meta: Schema.optional(
    Schema.Record({ key: Schema.String, value: Schema.Unknown }),
  ),
}) {}

/** Metadata attached to a notification. */
export class NotificationMeta extends Schema.Struct({
  _meta: Schema.optional(
    Schema.Record({ key: Schema.String, value: Schema.Unknown }),
  ),
}) {}

/** An opaque continuation returned by a paginated operation. */
export const Cursor: typeof Schema.String = Schema.String;

/** An opaque continuation returned by a paginated operation. */
export type Cursor = typeof Cursor.Type;

/** Resumes a page after the supplied cursor. */
export class PaginatedRequestMeta extends Schema.Struct({
  ...RequestMeta.fields,
  /** Returns results after this cursor. */
  cursor: Schema.optional(Cursor),
}) {}

/** Continues pagination when nextCursor is present. */
export class PaginatedResultMeta extends Schema.Struct({
  ...ResultMeta.fields,
  /** Resumes after the last returned result. Presence indicates more results may exist. */
  nextCursor: Schema.optional(Cursor),
}) {}

/** Identifies a user or assistant as a content sender or intended audience. */
export const Role: Schema.Literal<["user", "assistant"]> = Schema.Literal(
  "user",
  "assistant",
);

/** Identifies a user or assistant as a content sender or intended audience. */
export type Role = typeof Role.Type;

/** Hints for choosing and displaying content. */
export class Annotations extends Schema.Struct({
  /** Lists the participants for whom this content is useful. */
  audience: Schema.optional(Schema.Array(Role)),
  /** Importance from 0 for optional content to 1 for essential content. */
  priority: Schema.optional(Schema.Number.pipe(Schema.between(0, 1))),
}) {}

/** Identifies a client or server during initialization. */
export class Implementation extends Schema.Struct({
  name: Schema.String,
  title: Schema.optional(Schema.String),
  version: Schema.String,
}) {}

/** Advertises client capabilities, including experimental extensions. */
export class ClientCapabilities extends Schema.Class<ClientCapabilities>(
  "@effect/ai/McpSchema/ClientCapabilities",
)({
  /** Experimental, non-standard capabilities that the client supports. */
  experimental: Schema.optional(
    Schema.Record({
      key: Schema.String,
      value: Schema.Struct({}),
    }),
  ),
  /** Present if the client supports listing roots. */
  roots: Schema.optional(
    Schema.Struct({
      /** Whether the client supports notifications for changes to the roots list. */
      listChanged: Schema.optional(Schema.Boolean),
    }),
  ),
  /** Present if the client supports sampling from an LLM. */
  sampling: Schema.optional(Schema.Struct({})),
  /** Present if the client supports elicitation from the server. */
  elicitation: Schema.optional(Schema.Struct({})),
}) {}

/** Advertises server capabilities, including experimental extensions. */
export class ServerCapabilities extends Schema.Struct({
  /** Experimental, non-standard capabilities that the server supports. */
  experimental: Schema.optional(
    Schema.Record({
      key: Schema.String,
      value: Schema.Struct({}),
    }),
  ),
  /** Present if the server supports sending log messages to the client. */
  logging: Schema.optional(Schema.Struct({})),
  /** Present if the server supports argument autocompletion suggestions. */
  completions: Schema.optional(Schema.Struct({})),
  /** Present if the server offers any prompt templates. */
  prompts: Schema.optional(
    Schema.Struct({
      /** Whether this server supports notifications for changes to the prompt list. */
      listChanged: Schema.optional(Schema.Boolean),
    }),
  ),
  /** Present if the server offers any resources to read. */
  resources: Schema.optional(
    Schema.Struct({
      /** Whether this server supports subscribing to resource updates. */
      subscribe: Schema.optional(Schema.Boolean),
      /** Whether this server supports notifications for changes to the resource list. */
      listChanged: Schema.optional(Schema.Boolean),
    }),
  ),
  /** Present if the server offers any tools to call. */
  tools: Schema.optional(
    Schema.Struct({
      /** Whether this server supports notifications for changes to the tool list. */
      listChanged: Schema.optional(Schema.Boolean),
    }),
  ),
}) {}

/** A JSON-RPC error code, message and optional sender-defined details. */
export class McpError extends Schema.Class<McpError>(
  "@effect/ai/McpSchema/McpError",
)({
  /** The JSON-RPC error code. */
  code: Schema.Number,
  /** Describes the failure in one sentence. */
  message: Schema.String,
  /** Failure details whose structure the sender defines. */
  data: Schema.optional(Schema.Unknown),
}) {}
/** JSON-RPC code for invalid method arguments. */
export const INVALID_PARAMS_ERROR_CODE = -32602 as const;
/** JSON-RPC code for an internal server failure. */
export const INTERNAL_ERROR_CODE = -32603 as const;

/** Reports invalid arguments to an MCP operation. */
export class InvalidParams extends Schema.TaggedError<InvalidParams>()(
  "InvalidParams",
  {
    ...McpError.fields,
    code: Schema.tag(INVALID_PARAMS_ERROR_CODE),
  },
) {}

/** Reports a server failure while handling a request. */
export class InternalError extends Schema.TaggedError<InternalError>()(
  "InternalError",
  {
    ...McpError.fields,
    code: Schema.tag(INTERNAL_ERROR_CODE),
  },
) {
  static readonly notImplemented = new InternalError({
    message: "Not implemented",
  });
}

/** Checks that the peer is alive. A peer that fails to respond promptly may be disconnected. */
export class Ping extends Rpc.make("ping", {
  success: Schema.Struct({}),
  error: McpError,
  payload: Schema.UndefinedOr(RequestMeta),
}) {}

/** Returns the server version, capabilities and optional usage instructions. */
export class InitializeResult extends Schema.Class<InitializeResult>(
  "@effect/ai/McpSchema/InitializeResult",
)({
  ...ResultMeta.fields,
  /** The selected protocol version. A client that cannot support it must disconnect. */
  protocolVersion: Schema.String,
  capabilities: ServerCapabilities,
  serverInfo: Implementation,
  /** Usage instructions that a client may include in its model prompt. */
  instructions: Schema.optional(Schema.String),
}) {}

/** Starts a connection by exchanging protocol versions and capabilities. */
export class Initialize extends Rpc.make("initialize", {
  success: InitializeResult,
  error: McpError,
  payload: {
    ...RequestMeta.fields,
    /** The latest supported protocol version. The client may also support older versions. */
    protocolVersion: Schema.String,
    /** Advertises client capabilities, including experimental extensions. */
    capabilities: ClientCapabilities,
    /** Identifies a client or server during initialization. */
    clientInfo: Implementation,
  },
}) {}

/** Confirms that client initialization has finished. */
export class InitializedNotification extends Rpc.make(
  "notifications/initialized",
  {
    payload: Schema.UndefinedOr(NotificationMeta),
  },
) {}

/** Cancels a request previously sent in the same direction. */
export class CancelledNotification extends Rpc.make("notifications/cancelled", {
  payload: {
    ...NotificationMeta.fields,
    /** Identifies a request previously issued in the same direction. */
    requestId: RequestId,
    /** A cancellation reason that may be logged or shown to the user. */
    reason: Schema.optional(Schema.String),
  },
}) {}

/** Reports progress for a long-running request. */
export class ProgressNotification extends Rpc.make("notifications/progress", {
  payload: {
    ...NotificationMeta.fields,
    /** The progress token supplied in the original request. */
    progressToken: ProgressToken,
    /** Increases as work completes, even when total is unknown. */
    progress: Schema.optional(Schema.Number),
    /** Total work to complete, if known. */
    total: Schema.optional(Schema.Number),

    message: Schema.optional(Schema.String),
  },
}) {}

/** A known resource that the server is capable of reading. */
export class Resource extends Schema.Class<Resource>(
  "@effect/ai/McpSchema/Resource",
)({
  uri: Schema.String,
  /** A display name for the resource. */
  name: Schema.String,
  title: Schema.optional(Schema.String),
  /** Explains the resource to the client and its model. */
  description: Schema.optional(Schema.String),

  mimeType: Schema.optional(Schema.String),

  annotations: Schema.optional(Annotations),
  /** Raw content size in bytes, before base64 encoding or tokenization. */
  size: Schema.optional(Schema.Number),
}) {}

/** A template description for resources available on the server. */
export class ResourceTemplate extends Schema.Class<ResourceTemplate>(
  "@effect/ai/McpSchema/ResourceTemplate",
)({
  /** A URI template (according to RFC 6570) that can be used to construct resource URIs. */
  uriTemplate: Schema.String,
  /** A display name for resources matching this template. */
  name: Schema.String,
  title: Schema.optional(Schema.String),
  /** Explains which resources the template represents. */
  description: Schema.optional(Schema.String),

  /** Present only when every matching resource has this MIME type. */
  mimeType: Schema.optional(Schema.String),

  annotations: Schema.optional(Annotations),
}) {}

/** The contents of a specific resource or sub-resource. */
export class ResourceContents extends Schema.Struct({
  uri: Schema.String,

  mimeType: Schema.optional(Schema.String),
}) {}

/** The contents of a text resource, which can be represented as a string. */
export class TextResourceContents extends Schema.Struct({
  ...ResourceContents.fields,
  /** Content that can be represented as text. */
  text: Schema.String,
}) {}

/** Binary resource content, decoded from base64. */
export class BlobResourceContents extends Schema.Struct({
  ...ResourceContents.fields,

  blob: Schema.Uint8ArrayFromBase64,
}) {}

/** The server's response to a resources/list request from the client. */
export class ListResourcesResult extends Schema.Class<ListResourcesResult>(
  "@effect/ai/McpSchema/ListResourcesResult",
)({
  ...PaginatedResultMeta.fields,
  resources: Schema.Array(Resource),
}) {}

/** Sent from the client to request a list of resources the server has. */
export class ListResources extends Rpc.make("resources/list", {
  success: ListResourcesResult,
  error: McpError,
  payload: Schema.UndefinedOr(PaginatedRequestMeta),
}) {}

/** The server's response to a resources/templates/list request from the client. */
export class ListResourceTemplatesResult extends Schema.Class<ListResourceTemplatesResult>(
  "@effect/ai/McpSchema/ListResourceTemplatesResult",
)({
  ...PaginatedResultMeta.fields,
  resourceTemplates: Schema.Array(ResourceTemplate),
}) {}

/** Sent from the client to request a list of resource templates the server has. */
export class ListResourceTemplates extends Rpc.make(
  "resources/templates/list",
  {
    success: ListResourceTemplatesResult,
    error: McpError,
    payload: Schema.UndefinedOr(PaginatedRequestMeta),
  },
) {}

/** The server's response to a resources/read request from the client. */
export class ReadResourceResult extends Schema.Struct({
  ...ResultMeta.fields,
  contents: Schema.Array(
    Schema.Union(TextResourceContents, BlobResourceContents),
  ),
}) {}

/** Sent from the client to the server, to read a specific resource URI. */
export class ReadResource extends Rpc.make("resources/read", {
  success: ReadResourceResult,
  error: McpError,
  payload: {
    ...RequestMeta.fields,
    /** A resource URI interpreted by the server. */
    uri: Schema.String,
  },
}) {}

/**
 * Sent from the client to request resources/updated notifications from the
 * server whenever a particular resource changes.
 */
export class Subscribe extends Rpc.make("resources/subscribe", {
  error: McpError,
  payload: {
    ...RequestMeta.fields,
    /** A resource URI interpreted by the server. */
    uri: Schema.String,
  },
}) {}

/**
 * Sent from the client to request cancellation of resources/updated
 * notifications from the server. This should follow a previous
 * resources/subscribe request.
 */
export class Unsubscribe extends Rpc.make("resources/unsubscribe", {
  error: McpError,
  payload: {
    ...RequestMeta.fields,
    /** A resource URI interpreted by the server. */
    uri: Schema.String,
  },
}) {}

/** Describes an argument that a prompt can accept. */
export class PromptArgument extends Schema.Struct({
  name: Schema.String,
  title: Schema.optional(Schema.String),

  description: Schema.optional(Schema.String),

  required: Schema.optional(Schema.Boolean),
}) {}

/** A prompt or prompt template that the server offers. */
export class Prompt extends Schema.Class<Prompt>("@effect/ai/McpSchema/Prompt")(
  {
    name: Schema.String,
    title: Schema.optional(Schema.String),

    description: Schema.optional(Schema.String),
    /** A list of arguments to use for templating the prompt. */
    arguments: Schema.optional(Schema.Array(PromptArgument)),
  },
) {}

/** Text provided to or from an LLM. */
export class TextContent extends Schema.Struct({
  type: Schema.tag("text"),

  text: Schema.String,

  annotations: Schema.optional(Annotations),
}) {}

/** An image provided to or from an LLM. */
export class ImageContent extends Schema.Struct({
  type: Schema.tag("image"),
  /** Decoded image bytes, encoded as base64 on the wire. */
  data: Schema.Uint8ArrayFromBase64,

  mimeType: Schema.String,

  annotations: Schema.optional(Annotations),
}) {}

/** Audio provided to or from an LLM. */
export class AudioContent extends Schema.Struct({
  type: Schema.tag("audio"),
  /** Decoded audio bytes, encoded as base64 on the wire. */
  data: Schema.Uint8ArrayFromBase64,

  mimeType: Schema.String,

  annotations: Schema.optional(Annotations),
}) {}

/** Resource content embedded in a prompt or tool result. The client chooses its presentation. */
export class EmbeddedResource extends Schema.Struct({
  type: Schema.tag("resource"),
  resource: Schema.Union(TextResourceContents, BlobResourceContents),

  annotations: Schema.optional(Annotations),
}) {}

/** A resource link in a prompt or tool result. It may be absent from resources/list. */
export class ResourceLink extends Schema.Struct({
  ...Resource.fields,
  type: Schema.tag("resource_link"),
}) {}

/** Content carried by prompt messages and tool results. */
export class ContentBlock extends Schema.Union(
  TextContent,
  ImageContent,
  AudioContent,
  EmbeddedResource,
  ResourceLink,
) {}

/** A prompt message containing text, media or embedded resources. */
export class PromptMessage extends Schema.Struct({
  role: Role,
  content: ContentBlock,
}) {}

/** The server's response to a prompts/list request from the client. */
export class ListPromptsResult extends Schema.Class<ListPromptsResult>(
  "@effect/ai/McpSchema/ListPromptsResult",
)({
  ...PaginatedResultMeta.fields,
  prompts: Schema.Array(Prompt),
}) {}

/** Sent from the client to request a list of prompts and prompt templates the server has. */
export class ListPrompts extends Rpc.make("prompts/list", {
  success: ListPromptsResult,
  error: McpError,
  payload: Schema.UndefinedOr(PaginatedRequestMeta),
}) {}

/** The server's response to a prompts/get request from the client. */
export class GetPromptResult extends Schema.Class<GetPromptResult>(
  "@effect/ai/McpSchema/GetPromptResult",
)({
  ...ResultMeta.fields,
  messages: Schema.Array(PromptMessage),
  /** An optional description for the prompt. */
  description: Schema.optional(Schema.String),
}) {}

/** Used by the client to get a prompt provided by the server. */
export class GetPrompt extends Rpc.make("prompts/get", {
  success: GetPromptResult,
  error: McpError,
  payload: {
    ...RequestMeta.fields,

    name: Schema.String,
    title: Schema.optional(Schema.String),
    /** Arguments to use for templating the prompt. */
    arguments: Schema.optional(
      Schema.Record({
        key: Schema.String,
        value: Schema.String,
      }),
    ),
  },
}) {}

/** Tool behavior hints. Clients must not use hints from untrusted servers as a basis for tool-use decisions. */
export class ToolAnnotations extends Schema.Class<ToolAnnotations>(
  "@effect/ai/McpSchema/ToolAnnotations",
)({
  title: Schema.optional(Schema.String),
  /** Claims the tool leaves its environment unchanged. Defaults to false. */
  readOnlyHint: Schema.optionalWith(Schema.Boolean, { default: () => false }),
  /** Claims writes may be destructive when true and additive only when false. Applies when readOnlyHint is false. Defaults to true. */
  destructiveHint: Schema.optionalWith(Schema.Boolean, { default: () => true }),
  /** Claims repeated calls with the same arguments have no additional effect. Applies when readOnlyHint is false. Defaults to false. */
  idempotentHint: Schema.optionalWith(Schema.Boolean, { default: () => false }),
  /** Claims the tool may interact with external entities. False limits interactions to a known set. Defaults to true. */
  openWorldHint: Schema.optionalWith(Schema.Boolean, { default: () => true }),
}) {}

/** Definition for a tool the client can call. */
export class Tool extends Schema.Class<Tool>("@effect/ai/McpSchema/Tool")({
  name: Schema.String,
  title: Schema.optional(Schema.String),
  /** Explains the tool to the client and its model. */
  description: Schema.optional(Schema.String),
  /** A JSON Schema object defining the expected parameters for the tool. */
  inputSchema: Schema.Unknown,
  outputSchema: Schema.optional(Schema.Unknown),

  annotations: Schema.optional(ToolAnnotations),
}) {}

/** The server's response to a tools/list request from the client. */
export class ListToolsResult extends Schema.Class<ListToolsResult>(
  "@effect/ai/McpSchema/ListToolsResult",
)({
  ...PaginatedResultMeta.fields,
  tools: Schema.Array(Tool),
}) {}

/** Sent from the client to request a list of tools the server has. */
export class ListTools extends Rpc.make("tools/list", {
  success: ListToolsResult,
  error: McpError,
  payload: Schema.UndefinedOr(PaginatedRequestMeta),
}) {}

/** Returns tool output or a tool failure marked with isError. Unknown tools and unsupported calls use a JSON-RPC error response. */
export class CallToolResult extends Schema.Class<CallToolResult>(
  "@effect/ai/McpSchema/CallToolResult",
)({
  ...ResultMeta.fields,
  content: Schema.Array(ContentBlock),
  structuredContent: Schema.optional(Schema.Unknown),
  /** Marks a tool failure. Omission means success. */
  isError: Schema.optional(Schema.Boolean),
}) {}

/** Used by the client to invoke a tool provided by the server. */
export class CallTool extends Rpc.make("tools/call", {
  success: CallToolResult,
  error: McpError,
  payload: Schema.Struct({
    ...RequestMeta.fields,
    name: Schema.String,
    arguments: Schema.optionalWith(
      Schema.Record({
        key: Schema.String,
        value: Schema.Unknown,
      }),
      { default: () => ({}) },
    ),
  }),
}) {}

/** Log severity following RFC 5424 syslog levels. */
export const LoggingLevel: Schema.Literal<
  [
    "debug",
    "info",
    "notice",
    "warning",
    "error",
    "critical",
    "alert",
    "emergency",
  ]
> = Schema.Literal(
  "debug",
  "info",
  "notice",
  "warning",
  "error",
  "critical",
  "alert",
  "emergency",
);

/** Log severity following RFC 5424 syslog levels. */
export type LoggingLevel = typeof LoggingLevel.Type;

/** A request from the client to the server, to enable or adjust logging. */
export class SetLevel extends Rpc.make("logging/setLevel", {
  payload: {
    ...RequestMeta.fields,
    /** Receives logs at this severity or higher through notifications/message. */
    level: LoggingLevel,
  },
  error: McpError,
}) {}

/** A reference to a resource or resource template definition. */
export class ResourceReference extends Schema.Struct({
  type: Schema.tag("ref/resource"),
  /** The URI or URI template of the resource. */
  uri: Schema.String,
}) {}

/** Identifies a prompt. */
export class PromptReference extends Schema.Struct({
  type: Schema.tag("ref/prompt"),
  /** The name of the prompt or prompt template */
  name: Schema.String,
  title: Schema.optional(Schema.String),
}) {}

/** The server's response to a completion/complete request */
export class CompleteResult extends Schema.Class<CompleteResult>(
  "@effect/ai/McpSchema/CompleteResult",
)({
  completion: Schema.Struct({
    /** At most 100 completion values. */
    values: Schema.Array(Schema.String),
    /** Total available options, including those outside this result. */
    total: Schema.optional(Schema.Number),
    /** Whether more options exist, even when total is unknown. */
    hasMore: Schema.optional(Schema.Boolean),
  }),
}) {
  /** A completed result with no completion options. */
  static readonly empty = CompleteResult.make({
    completion: {
      values: [],
      total: 0,
      hasMore: false,
    },
  });
}

/** A request from the client to the server, to ask for completion options. */
export class Complete extends Rpc.make("completion/complete", {
  success: CompleteResult,
  error: McpError,
  payload: Schema.Struct({
    ref: Schema.Union(PromptReference, ResourceReference),

    argument: Schema.Struct({
      name: Schema.String,
      /** The value of the argument to use for completion matching. */
      value: Schema.String,
    }),

    context: Schema.optionalWith(
      Schema.Struct({
        /** Previously-resolved variables in a URI template or prompt. */
        arguments: Schema.optionalWith(
          Schema.Record({
            key: Schema.String,
            value: Schema.String,
          }),
          { default: () => ({}) },
        ),
      }),
      { default: () => ({ arguments: {} }) },
    ),
  }),
}) {}

/** Tells the server to relist roots after the client adds, changes or removes one. */
export class RootsListChangedNotification extends Rpc.make(
  "notifications/roots/list_changed",
  {
    payload: Schema.UndefinedOr(NotificationMeta),
  },
) {}

/** Transport context for the current MCP request. */
export class McpServerClient extends Context.Tag(
  "@effect/ai/McpSchema/McpServerClient",
)<
  McpServerClient,
  {
    readonly clientId: number;
    readonly protocolVersion?: string;
    readonly requestId?: string | number;
    readonly notify?: (
      method: string,
      params: Record<string, unknown>,
    ) => Effect.Effect<void>;
  }
>() {}

/** Supplies transport context to request handlers. */
export class McpServerClientMiddleware extends RpcMiddleware.Tag<McpServerClientMiddleware>()(
  "@effect/ai/McpSchema/McpServerClientMiddleware",
  {
    provides: McpServerClient,
  },
) {}

/** Negotiated clients discover supported protocol versions before selecting a session. */
export class Discover extends Rpc.make("server/discover", {
  payload: Schema.Unknown,
  success: Schema.Unknown,
  error: McpError,
}) {}

/** Selects notifications for a listening request. */
export const SubscriptionFilter = Schema.Struct({
  toolsListChanged: Schema.optional(Schema.Boolean),
  promptsListChanged: Schema.optional(Schema.Boolean),
  resourcesListChanged: Schema.optional(Schema.Boolean),
  resourceSubscriptions: Schema.optional(Schema.Array(Schema.String)),
});

/** Acknowledgment and notifications share the listening request's lifetime. */
export class SubscriptionsListen extends Rpc.make("subscriptions/listen", {
  payload: Schema.Struct({
    ...RequestMeta.fields,
    notifications: SubscriptionFilter,
  }),
  success: Schema.Unknown,
  error: McpError,
}) {}

/** Lists the events available to the client. */
export class EventsList extends Rpc.make("events/list", {
  payload: Schema.Unknown,
  success: Schema.Unknown,
  error: McpError,
}) {}
/** A stream publishes request-bound notifications until cancellation. */
export class EventsStream extends Rpc.make("events/stream", {
  payload: Schema.Unknown,
  success: Schema.Unknown,
  error: McpError,
}) {}
/** Registers a callback for event delivery. */
export class EventsSubscribe extends Rpc.make("events/subscribe", {
  payload: Schema.Unknown,
  success: Schema.Unknown,
  error: McpError,
}) {}
/** Removes an event callback subscription. */
export class EventsUnsubscribe extends Rpc.make("events/unsubscribe", {
  payload: Schema.Unknown,
  success: Schema.Unknown,
  error: McpError,
}) {}

/** Requests dispatched with per-request client context. */
export class ClientRequestRpcs extends RpcGroup.make(
  Discover,
  SubscriptionsListen,
  Ping,
  Initialize,
  Complete,
  SetLevel,
  GetPrompt,
  ListPrompts,
  ListResources,
  ListResourceTemplates,
  ReadResource,
  Subscribe,
  Unsubscribe,
  CallTool,
  ListTools,
  EventsList,
  EventsStream,
  EventsSubscribe,
  EventsUnsubscribe,
).middleware(McpServerClientMiddleware) {}

/** Client notifications that expect no response. */
export class ClientNotificationRpcs extends RpcGroup.make(
  CancelledNotification,
  ProgressNotification,
  InitializedNotification,
  RootsListChangedNotification,
) {}

/** Client requests and notifications handled by the RPC engine. */
export class ClientRpcs extends ClientRequestRpcs.merge(
  ClientNotificationRpcs,
) {}

/** A protocol failure whose details may be sent to the client. */
export class ProtocolError extends McpError {
  constructor(code: number, message: string, data?: unknown) {
    super({ code, message, ...(data === undefined ? {} : { data }) });
  }
  static isInstance(value: unknown): value is ProtocolError {
    return value instanceof ProtocolError;
  }
}
