/** @file Canonical MCP tools, Events and wire errors share their owning schemas. */

import { Effect, Either, Option, type ParseResult, Schema } from "effect";
import type { ProtocolError } from "../../vendor/effect-mcp/McpSchema.js";
import { SendError, sendFailureReasons } from "../../communication/errors.js";
import {
  CollectiveId,
  decodeCollectiveFailure,
  Message,
  MessageReference,
  PaginatedRequest,
  PaginatedResult,
  RequestError,
  wellFormedString,
} from "../../communication/messages.js";
import {
  compareAscii,
  exactStruct,
  isStrictlyAscending,
} from "../../encoding/index.js";
import {
  checkedGuidanceSchema,
  firewallDecisionsRequestSchema,
  firewallDecisionsSchema,
  FirewallPolicyError,
  FirewallRefusal,
  firewallRefusalReasonSchema,
  LocalGuidance,
  PermittedItem,
  policyReadSchema,
  policyUpdateResultSchema,
  policyUpdateSchema,
} from "../../firewall/index.js";
import { AgentId, AgentName } from "../../identity/agent-cards.js";
import {
  DeliveryToken,
  InboxError,
  InboxPage,
  MessageAddress,
} from "../inbox.js";
import {
  SendArguments,
  SendResultSchema,
  TextSendArguments,
} from "../message-format.js";
import { HistoryError } from "../social-harness.js";

/** Maximum complete UTF-8 callback body accepted by the event transport. */
export const maximumEventBytes = 256 * 1024;

/** Callback failures expose no upstream response or destination details. */
export const callbackReasons = [
  "connection_refused",
  "timeout",
  "tls_error",
  "http_4xx",
  "http_5xx",
  "challenge_failed",
] as const;
/** Owner-visible diagnostics omit callback addresses and signing authority. */
const webhookStatusSchema = Schema.Struct({
  mode: Schema.Literal("none", "webhook"),
  id: Schema.optional(Schema.String),
  refreshBefore: Schema.optional(Schema.String),
  stalled: Schema.optional(
    Schema.NullOr(Schema.Literal("callback", "terminal")),
  ),
  lastError: Schema.optional(Schema.NullOr(Schema.Literal(...callbackReasons))),
});

/** The callback subscription as the status tool reports it, without its address or secret. */
export type WebhookStatus = typeof webhookStatusSchema.Type;

const emptyArguments = Schema.Record({
  key: Schema.String,
  value: Schema.Never,
});
/** Catalog cursors are independent of event replay positions. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- MCP handlers consume the owning event schema.
export const eventListInput = Schema.Struct({
  cursor: Schema.optionalWith(Schema.String, { exact: true }),
});
/** Validates subscription shape before authorization and callback URL checks. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- MCP handlers consume the owning event schema.
export const eventSubscribeInput = Schema.Struct({
  name: Schema.String,
  arguments: emptyArguments,
  /** Omitting the cursor requests the draft's explicit null position. */
  cursor: Schema.optionalWith(Schema.NullOr(Schema.String), { exact: true }),
  maxAgeMs: Schema.optionalWith(Schema.NonNegativeInt, { exact: true }),
  delivery: Schema.Struct({
    mode: Schema.String,
    url: Schema.String,
    secret: Schema.String,
  }),
  ttlMs: Schema.optionalWith(Schema.NullOr(Schema.NonNegativeInt), {
    exact: true,
  }),
});
/** Cleanup identifies the registration without disclosing its signing secret. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- MCP handlers consume the owning event schema.
export const eventUnsubscribeInput = Schema.Struct({
  name: Schema.String,
  arguments: emptyArguments,
  delivery: Schema.Struct({ url: Schema.String }),
});

/** Validated webhook subscription parameters before authorization and URL checks. */
export type EventSubscribeInput = typeof eventSubscribeInput.Type;
type EventUnsubscribeInput = typeof eventUnsubscribeInput.Type;
interface EventSubscriptionGrant {
  readonly id: string;
  readonly refreshBefore: string;
  readonly cursor: null;
  readonly truncated: boolean;
}

/** One agent's callback subscription for inbox wake-ups. */
export interface HarnessWebhookEvents {
  readonly subscribe: (
    input: EventSubscribeInput,
  ) => Effect.Effect<EventSubscriptionGrant, ProtocolError>;
  readonly unsubscribe: (
    input: EventUnsubscribeInput,
  ) => Effect.Effect<void, ProtocolError>;
  readonly status: Effect.Effect<WebhookStatus>;
}

/** A content-free wakeup to read the pending inbox. */
export const INBOX_PENDING_EVENT = "org.social-harness/inbox.pending";

/** How the event catalogue lists the wake-up. */
export const inboxPendingCatalogEntry = {
  name: INBOX_PENDING_EVENT,
  description: "A wakeup to read the pending inbox.",
  delivery: ["webhook"],
  inputSchema: { type: "object", properties: {}, additionalProperties: false },
  payloadSchema: {
    type: "object",
    properties: { pendingCount: { type: "integer", minimum: 0 } },
    required: ["pendingCount"],
  },
};

/**
 * Refusals disclose the check, the deciding chain rule, registered feedback and
 * the rule's matcher detail, without assessment evidence.
 */
const firewallRefusalSchema = exactStruct({
  checkId: checkedGuidanceSchema.fields.checkId,
  refusal: firewallRefusalReasonSchema,
  rule: Schema.optionalWith(Schema.NonEmptyString, { exact: true }),
  feedback: Schema.Array(exactStruct({ text: Schema.NonEmptyString })),
  detail: Schema.optionalWith(Schema.Unknown, { exact: true }),
});

const savedItemSchema = Schema.typeSchema(PermittedItem);

const historyRecord = Schema.Union(
  Schema.Struct({
    kind: Schema.Literal("sent"),
    message: Schema.typeSchema(Message),
  }),
  Schema.Struct({
    kind: Schema.Literal("received"),
    address: MessageAddress,
    message: MessageReference,
    item: savedItemSchema,
    guidance: Schema.optionalWith(LocalGuidance, { exact: true }),
  }),
  Schema.Struct({
    kind: Schema.Literal("result"),
    address: MessageAddress,
    deliveryToken: DeliveryToken,
    item: savedItemSchema,
    guidance: Schema.optionalWith(LocalGuidance, { exact: true }),
  }),
);

/** This agent's identifier and name, readable while communication is still starting. */
const managementStatusResultSchema = Schema.Struct({
  agentId: AgentId,
  agentName: AgentName,
  webhook: webhookStatusSchema,
});

/** Canonically ordered local-conversation page selector. */
const managementListConversationsRequestSchema = exactStruct({
  ...PaginatedRequest.fields,
});

/** At most fifty canonical addresses and an opaque position for the next page. */
const managementListConversationsResultSchema = Schema.Struct({
  addresses: Schema.Array(MessageAddress).pipe(Schema.maxItems(50)),
  ...PaginatedResult.fields,
}).pipe(Schema.filter((page) => addressesAreOrdered(page.addresses)));

const managementReadConversationRequestSchema = exactStruct({
  address: MessageAddress,
  ...PaginatedRequest.fields,
});

const managementReadConversationResultSchema = Schema.Struct({
  records: Schema.Array(historyRecord).pipe(Schema.maxItems(50)),
  ...PaginatedResult.fields,
});

/**
 * The longest a receive_messages call may wait. It stays under the time after
 * which hosts and proxies give up on a tool call.
 */
export const MAXIMUM_WAIT_SECONDS = 50;

const inboxRequestSchema = exactStruct({
  ...PaginatedRequest.fields,
  wait: Schema.optional(
    Schema.Number.pipe(
      Schema.positive(),
      Schema.lessThanOrEqualTo(MAXIMUM_WAIT_SECONDS),
    ).annotations({
      description: `a number of seconds, at most ${String(MAXIMUM_WAIT_SECONDS)}, to wait for a message when none is pending. Call receive_messages again to keep waiting. Without wait the call returns at once.`,
    }),
  ),
});
const inboxResultSchema = Schema.typeSchema(InboxPage);
/** What the daemon answers when a call's arguments are not what the tool takes. */
const invalidArgumentsSchema = Schema.Struct({
  issues: Schema.Array(
    Schema.Struct({
      path: Schema.Array(Schema.Union(Schema.String, Schema.Number)),
      message: Schema.String,
    }),
  ),
});

/**
 * Preserves typed send and collective failures without disclosing unrelated
 * requests. New fields are ignored. Missing firewall or collective fields cannot
 * decode as a plain send reason.
 */
const harnessSendErrorDataSchema = Schema.Union(
  invalidArgumentsSchema,
  Schema.Struct({
    reason: Schema.Literal("firewall-refused"),
    ...firewallRefusalSchema.fields,
  }),
  Schema.Struct({
    reason: Schema.Literal("collective-failed"),
    operationId: Schema.optionalWith(CollectiveId, { exact: true }),
    failure: Schema.Unknown,
  }),
  Schema.Struct({
    reason: Schema.Literal(...sendFailureReasons),
    detail: Schema.optionalWith(Schema.String, { exact: true }),
  }),
);

/**
 * Decoded error data of one refused `send_message` call.
 * @inline
 */
type HarnessSendErrorData =
  | typeof invalidArgumentsSchema.Type
  | (typeof firewallRefusalSchema.Type & {
      readonly reason: "firewall-refused";
    })
  | Readonly<{ reason: SendError["reason"]; detail?: string }>
  | Readonly<{
      reason: "collective-failed";
      operationId?: CollectiveId;
      failure: Effect.Effect.Success<
        ReturnType<typeof decodeCollectiveFailure>
      >;
    }>;

/**
 * Preserves the typed refusal's details, operation id and failing members or
 * fields in JSON-RPC error data.
 */
export function sendErrorData(
  error: SendError | RequestError | FirewallRefusal,
): HarnessSendErrorData {
  if (error._tag === "FirewallRefusal") {
    return {
      reason: "firewall-refused",
      checkId: error.checkId,
      refusal: error.refusal,
      ...(error.rule === undefined ? {} : { rule: error.rule }),
      feedback: error.feedback,
      ...(error.detail === undefined ? {} : { detail: error.detail }),
    };
  }
  if (error._tag === "RequestError") {
    return {
      reason: "collective-failed",
      ...(error.operationId === undefined
        ? {}
        : { operationId: error.operationId }),
      failure: error.failure,
    };
  }
  if (error.detail === undefined) {
    return { reason: error.reason };
  }
  return { reason: error.reason, detail: error.detail };
}

/** Decodes JSON-RPC refusal data while preserving send and collective details. */
function decodeSendErrorData(
  value: unknown,
): Effect.Effect<HarnessSendErrorData, ParseResult.ParseError> {
  return Schema.decodeUnknown(harnessSendErrorDataSchema)(value).pipe(
    Effect.flatMap(
      (data): Effect.Effect<HarnessSendErrorData, ParseResult.ParseError> =>
        "failure" in data
          ? decodeCollectiveFailure(data.failure).pipe(
              Effect.map((failure) => ({ ...data, failure })),
            )
          : Effect.succeed(data),
    ),
  );
}

/**
 * How a tool call ended without a result, as its caller saw it. `refused`
 * carries the error data the daemon answered with. `unanswered` says whether
 * the request can have reached the daemon. `unreadable` is an answer that is
 * not the tool's result.
 */
export type ToolCallFailure =
  | { readonly kind: "refused"; readonly data: unknown }
  | { readonly kind: "unanswered"; readonly sent: boolean }
  | { readonly kind: "unreadable" };

/** A tool whose refusals carry `data`, which `make` turns into its typed failure. */
const failsWith = <A, I, E>(data: Schema.Schema<A, I>, make: (data: A) => E) =>
  ({
    total: false,
    read: (failure: ToolCallFailure): Option.Option<E> =>
      failure.kind === "refused"
        ? Option.map(Schema.decodeUnknownOption(data)(failure.data), make)
        : Option.none(),
  }) as const;

/** A tool with no typed failure of its own. */
const failsUntyped = {
  total: false,
  read: (): Option.Option<never> => Option.none(),
} as const;

/**
 * A send reads every failure. One that may have reached the daemon without a
 * readable answer has an unknown outcome, so its caller must not send again.
 */
const sendFails = {
  total: true,
  read: (
    failure: ToolCallFailure,
  ): Option.Option<SendError | RequestError | FirewallRefusal> => {
    if (failure.kind === "unanswered" && !failure.sent) {
      return Option.some(new SendError({ reason: "network-unavailable" }));
    }
    if (failure.kind !== "refused") {
      return Option.some(new SendError({ reason: "outcome-unknown" }));
    }
    return Option.some(
      Either.match(
        Effect.runSync(Effect.either(decodeSendErrorData(failure.data))),
        {
          onLeft: () => new SendError({ reason: "outcome-unknown" }),
          onRight: (data) => {
            if ("issues" in data) {
              return new SendError({
                reason: "content-invalid",
                detail: "the arguments are invalid",
                issues: data.issues,
              });
            }
            if ("refusal" in data) {
              return new FirewallRefusal(data);
            }
            if ("failure" in data) {
              return new RequestError({
                failure: data.failure,
                ...(data.operationId === undefined
                  ? {}
                  : { operationId: data.operationId }),
              });
            }
            return new SendError(data);
          },
        },
      ),
    );
  },
} as const;

const policyFailure = Schema.Struct({
  reason: FirewallPolicyError.fields.reason,
  ruleId: FirewallPolicyError.fields.ruleId,
  path: FirewallPolicyError.fields.path,
  problem: FirewallPolicyError.fields.problem,
});

const empty = Schema.Struct({}).annotations({
  jsonSchema: { type: "object", properties: {}, additionalProperties: false },
});
const read = { readOnlyHint: true, openWorldHint: false } as const;

/** Each held norm's name, description and protocol, and the document that states its acts. */
const readNormsResultSchema = exactStruct({
  norms: Schema.Array(
    exactStruct({
      name: Schema.String,
      description: Schema.String,
      protocol: Schema.optionalWith(Schema.String, { exact: true }),
      document: Schema.String,
    }),
  ),
});

const historyFails = failsWith(
  Schema.Struct({ reason: HistoryError.fields.reason }),
  (data) => new HistoryError(data),
);
const policyFails = failsWith(
  policyFailure,
  (data) => new FirewallPolicyError(data),
);

/**
 * Each tool's contract: its description, argument and result schemas, and how
 * its failures read. Discovery, the server and callers all derive from it.
 */
export const toolDefinitions = {
  send_message: {
    failure: sendFails,
    description:
      "Send a message, ask a question, or answer one. Returns the accepted send id and canonical conversation address. A refusal names the argument or rule to correct. An outcome-unknown failure must not be retried as a new send.",
    input: SendArguments,
    output: SendResultSchema,
    annotations: { readOnlyHint: false, openWorldHint: true },
  },
  receive_messages: {
    failure: failsWith(
      Schema.Struct({ reason: InboxError.fields.reason }),
      (data) => new InboxError(data),
    ),
    description:
      "Receive pending inbox items, each with its conversation address and any saved firewall reminder. A multicast item is a peer's message, a collectiveRequest a question a peer asks, and a collectiveResult each agent's answer to a question. Follow nextCursor until absent. Returned items are marked delivered. Pass wait to wait up to that many seconds when nothing is pending; the call returns as soon as an item arrives, or empty when the wait ends.",
    input: inboxRequestSchema,
    output: inboxResultSchema,
    annotations: {
      readOnlyHint: false,
      destructiveHint: false,
      openWorldHint: false,
    },
  },
  status: {
    failure: failsUntyped,
    description:
      "Read this agent's identifier and name, and the state of the inbox wake-up callback if one is subscribed. Other agents address this agent as agent:<agentName>.",
    input: empty,
    output: managementStatusResultSchema,
    annotations: read,
  },
  list_conversations: {
    failure: historyFails,
    description:
      "List canonical conversation addresses with completed messages or saved local results. Follow nextCursor until absent. Use read_conversation to read one address.",
    input: managementListConversationsRequestSchema,
    output: managementListConversationsResultSchema,
    annotations: read,
  },
  read_conversation: {
    failure: historyFails,
    description:
      "Read saved sent messages, permitted incoming items and local results in one conversation. Keep address unchanged while following nextCursor. The first page fixes the history boundary.",
    input: managementReadConversationRequestSchema,
    output: managementReadConversationResultSchema,
    annotations: read,
  },
  read_policy: {
    failure: policyFails,
    description:
      "Read active editable rules, fixed rules, the names of the norms this agent holds, and the policy revision. Fixed rules include the English rules of held norms.",
    input: empty,
    output: policyReadSchema,
    annotations: read,
  },
  update_policy: {
    failure: policyFails,
    description:
      "Add, remove or replace editable rules and held norms. A behavior is a complete BEHAVIOR.md document, and {norm} adds a complete norm document; remove a norm with {norm: name}. Remove and add the same id or norm to replace it. Fixed rules refuse edits. Returns after the complete edit is active; saved decisions remain unchanged.",
    input: policyUpdateSchema,
    output: policyUpdateResultSchema,
    annotations: { readOnlyHint: false, openWorldHint: false },
  },
  read_firewall_decisions: {
    failure: policyFails,
    description:
      "Read completed firewall decision summaries since inclusive Unix milliseconds. Summaries include the reason and registered advice without message content or private model evidence.",
    input: firewallDecisionsRequestSchema,
    output: firewallDecisionsSchema,
    annotations: read,
  },
  read_norms: {
    failure: failsUntyped,
    description:
      "List the norms this agent holds, each with its document: the norm's roles, when each act may be sent and the fields it takes. Send an act as (performative :ontology <norm> :field value ...) in the conversation it belongs to. Agree on a norm through norm-agreement before its first act; ask a member for a norm you lack through norm-discovery.",
    input: empty,
    output: readNormsResultSchema,
    annotations: read,
  },
} as const;

/**
 * How two tools read for a client in client/server mode. A client talks only
 * to the server agent, so it names no recipient and no conversation.
 */
export const clientToolDefinitions = {
  send_message: {
    ...toolDefinitions.send_message,
    description:
      "Send a message to the server agent. Returns the accepted send id. An outcome-unknown failure must not be retried as a new send.",
    input: exactStruct({
      text: Schema.propertySignature(wellFormedString).annotations({
        description: "The message text.",
      }),
    }),
  },
  read_conversation: {
    ...toolDefinitions.read_conversation,
    description:
      "Read the messages you sent and received in your conversation with the server agent. Follow nextCursor until absent. The first page fixes the history boundary.",
    input: exactStruct({ ...PaginatedRequest.fields }),
  },
  read_norms: {
    ...toolDefinitions.read_norms,
    description:
      "List the norms the server agent holds, each with its document: the norm's roles, when each act may be sent and the fields it takes. The server agent checks your messages against them. Send an act as (performative :ontology <norm> :field value ...).",
  },
} as const;

/** How `send_message` reads for an agent that sends text only, with no questions or answers. */
export const textToolDefinitions = {
  send_message: {
    ...toolDefinitions.send_message,
    description:
      "Send a message. Returns the accepted send id and canonical conversation address. An outcome-unknown failure must not be retried as a new send.",
    input: TextSendArguments,
  },
} as const;

/** Names the tools published by the daemon's MCP catalog. */
export type ToolName = keyof typeof toolDefinitions;

/** Arguments are derived from the server's owning Effect schema. */
export type ToolRequest<Name extends ToolName = ToolName> = {
  readonly [Tool in Name]: {
    readonly name: Tool;
    readonly arguments: Schema.Schema.Type<
      (typeof toolDefinitions)[Tool]["input"]
    >;
  };
}[Name];

/** Results are derived from the schema the server publishes for this tool. */
export type ToolResult<Name extends ToolName> = Schema.Schema.Type<
  (typeof toolDefinitions)[Name]["output"]
>;

function addressesAreOrdered(addresses: readonly string[]): boolean {
  return isStrictlyAscending(addresses, compareAscii);
}
