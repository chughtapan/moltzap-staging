/** @file Canonical MCP tools, Events and wire errors share their owning schemas. */

import { Effect, JSONSchema, type ParseResult, Schema } from "effect";
import type { ProtocolError } from "../../vendor/effect-mcp/McpSchema.js";
import {
  type SendError,
  sendFailureReasons,
} from "../../communication/errors.js";
import {
  CollectiveId,
  decodeCollectiveFailure,
  Message,
  MessageReference,
  PaginatedRequest,
  PaginatedResult,
  type RequestError,
} from "../../communication/service.js";
import { exactStruct } from "../../encoding/canonical-json.js";
import {
  compareAscii,
  isStrictlyAscending,
} from "../../encoding/identifier.js";
import { checkedGuidanceSchema } from "../../firewall/check.js";
import {
  firewallDecisionsRequestSchema,
  firewallDecisionsSchema,
  FirewallPolicyError,
  firewallRefusalReasonSchema,
  policyReadSchema,
  policyUpdateResultSchema,
  policyUpdateSchema,
} from "../../firewall/rules.js";
import {
  type FirewallRefusal,
  LocalGuidance,
  PermittedItem,
} from "../../firewall/service.js";
import {
  AgentIdentityCard,
  AgentName,
  PrincipalId,
} from "../../identity/agent-cards.js";
import { RegistrationId } from "../../identity/registry.js";
import { RegistrationError } from "../../identity/service.js";
import {
  DeliveryToken,
  InboxError,
  InboxEvent,
  InboxPage,
  MessageAddress,
} from "../inbox.js";
import { SendRequest, SendResultSchema } from "../message-format.js";
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

/** Secret-free diagnostics shared with the MCP tool. */
export type WebhookStatus = typeof webhookStatusSchema.Type;

const emptyArguments = Schema.Record({
  key: Schema.String,
  value: Schema.Never,
});
/** Omitting the cursor requests the draft's explicit null position. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- MCP handlers consume the owning event schema.
export const eventStreamInput = Schema.Struct({
  name: Schema.String,
  arguments: emptyArguments,
  cursor: Schema.optionalWith(Schema.NullOr(Schema.String), { exact: true }),
  maxAgeMs: Schema.optionalWith(Schema.NonNegativeInt, { exact: true }),
});
/** Catalog cursors are independent of event replay positions. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- MCP handlers consume the owning event schema.
export const eventListInput = Schema.Struct({
  cursor: Schema.optionalWith(Schema.String, { exact: true }),
});
/** Validates subscription shape before authorization and callback URL checks. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- MCP handlers consume the owning event schema.
export const eventSubscribeInput = Schema.Struct({
  ...eventStreamInput.fields,
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

/** Durable callback delivery for the daemon's authenticated clients. */
export interface HarnessWebhookEvents {
  readonly subscribe: (
    input: EventSubscribeInput,
    authorize?: Effect.Effect<void, ProtocolError>,
  ) => Effect.Effect<EventSubscriptionGrant, ProtocolError>;
  readonly unsubscribe: (
    input: EventUnsubscribeInput,
  ) => Effect.Effect<void, ProtocolError>;
  readonly status: Effect.Effect<WebhookStatus, ProtocolError>;
  readonly revoke: Effect.Effect<void, ProtocolError>;
  readonly resume: Effect.Effect<void, ProtocolError>;
}

/** A content-free wakeup to read the pending inbox. */
export const INBOX_PENDING_EVENT = "org.social-harness/inbox.pending";

/** Signed callbacks carry saved content or its lookup reference. */
export const INBOX_ITEM_EVENT = "org.social-harness/inbox.item";

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

const encodedAgentCard = Schema.encodedBoundSchema(AgentIdentityCard);

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

const managementRegisterRequestSchema = exactStruct({
  operationId: Schema.propertySignature(RegistrationId).annotations({
    description:
      "Registration retry identity (opn_ plus canonical base64url for 16 bytes); reuse it for the same registration attempt.",
  }),
  principalId: Schema.propertySignature(PrincipalId).annotations({
    description:
      "Principal represented by this agent (prn_ plus canonical base64url for 16 bytes).",
  }),
  agentName: Schema.propertySignature(AgentName).annotations({
    description:
      "Immutable Registry-wide handle: 3 to 32 lowercase letters, digits, or single separating hyphens.",
  }),
});

const managementRegisterResultSchema = encodedAgentCard;

/** Saved identity is visible while communication activation proceeds. */
const managementStatusResultSchema = Schema.Union(
  Schema.Struct({ kind: Schema.Literal("unregistered") }),
  Schema.Struct({
    kind: Schema.Literal("registered"),
    identityCard: encodedAgentCard,
  }),
);

/** Canonically ordered local-conversation page selector. */
export const managementSearchConversationsRequestSchema = exactStruct({
  ...PaginatedRequest.fields,
});

/** At most fifty canonical addresses and an opaque position for the next page. */
export const managementSearchConversationsResultSchema = Schema.Struct({
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

const itemEventDataSchema = Schema.Union(
  exactStruct({
    kind: Schema.Literal("item"),
    address: MessageAddress,
    item: savedItemSchema,
    guidance: Schema.optionalWith(LocalGuidance, { exact: true }),
  }),
  exactStruct({
    kind: Schema.Literal("reference"),
    itemKind: Schema.Literal(
      "multicast",
      "collectiveRequest",
      "collectiveResult",
      "operationFailed",
    ),
    bytes: Schema.NonNegativeInt,
  }),
);
/** Callback data uses the same saved payload returned by read_event. */
export const itemEventDataJsonSchema = JSONSchema.make(itemEventDataSchema, {
  target: "jsonSchema2020-12",
});

const readEventRequestSchema = exactStruct({
  eventId: Schema.propertySignature(DeliveryToken).annotations({
    description:
      "The eventId of an org.social-harness/inbox.item event, or one a host's wakeup carries from an inbox item; retained content remains readable after acknowledgment.",
  }),
});
const readEventResultSchema = Schema.typeSchema(InboxEvent);

const harnessSendArgumentsSchema = exactStruct({
  input: Schema.propertySignature(SendRequest).annotations({
    description:
      "One send to agent:<name> or group:<members>: text with an optional collective operation, or a collectiveResponse to an open request.",
  }),
});

const inboxRequestSchema = exactStruct({
  ...PaginatedRequest.fields,
  wait: Schema.optional(
    Schema.Positive.annotations({
      description:
        "Seconds to wait for an item when none is pending. Without it the read returns at once.",
    }),
  ),
});
const inboxResultSchema = Schema.typeSchema(InboxPage);
const invocationValidationErrorSchema = Schema.Struct({
  reason: Schema.Literal("content-invalid"),
  detail: Schema.optionalWith(Schema.String, { exact: true }),
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
  invocationValidationErrorSchema,
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
  | typeof invocationValidationErrorSchema.Type
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
export function decodeHarnessSendErrorData(
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

/** A tool's declaration is shared by discovery, the server and optional plugin helpers. */
export const toolDefinitions = {
  event_subscription_status: {
    description:
      "Read callback subscription and retry status without callback credentials.",
    input: empty,
    output: webhookStatusSchema,
    annotations: read,
  },
  revoke_event_subscription: {
    description:
      "Revoke the retained callback subscription. Pending inbox items remain available.",
    input: empty,
    output: empty,
    annotations: { readOnlyHint: false, openWorldHint: false },
  },
  resume_event_subscription: {
    description:
      "Resume callback retries after a recoverable failure. A terminal rejection requires revocation and reconfiguration.",
    input: empty,
    output: empty,
    annotations: { readOnlyHint: false, openWorldHint: false },
  },
  send_message: {
    description:
      "Send a message, ask a question, or answer a request. Returns the accepted send id and canonical conversation address. A delivery-pending or outcome-unknown failure must not be retried as a new send.",
    input: harnessSendArgumentsSchema,
    output: SendResultSchema,
    annotations: { readOnlyHint: false, openWorldHint: true },
  },
  read_inbox: {
    description:
      "Read pending inbox items and their saved advice. Follow nextCursor until absent. Reading marks returned items delivered. Pass wait to wait that many seconds for an item when none is pending; the read returns as soon as one arrives, or empty when the wait ends. Use read_event to retrieve a known item after delivery.",
    input: inboxRequestSchema,
    output: inboxResultSchema,
    annotations: {
      readOnlyHint: false,
      destructiveHint: false,
      openWorldHint: false,
    },
  },
  read_event: {
    description:
      "Retrieve a saved inbox item and its advice by eventId, including after delivery or restart. This leaves pending delivery unchanged.",
    input: readEventRequestSchema,
    output: readEventResultSchema,
    annotations: read,
  },
  register: {
    description:
      "Register this daemon's configured key for a principal and immutable agent name. Reuse operationId when retrying the same registration. Supply the network's admission credential through daemon setup.",
    input: managementRegisterRequestSchema,
    output: managementRegisterResultSchema,
    annotations: {
      readOnlyHint: false,
      destructiveHint: false,
      idempotentHint: true,
      openWorldHint: true,
    },
  },
  status: {
    description:
      "Read saved registration status and the Registry-issued identity card, when registered.",
    input: empty,
    output: managementStatusResultSchema,
    annotations: read,
  },
  search_conversations: {
    description:
      "List canonical conversation addresses with completed messages or saved local results. Follow nextCursor until absent. Use read_conversation to read one address.",
    input: managementSearchConversationsRequestSchema,
    output: managementSearchConversationsResultSchema,
    annotations: read,
  },
  read_conversation: {
    description:
      "Read saved sent messages, permitted incoming items and local results in one conversation. Keep address unchanged while following nextCursor. The first page fixes the history boundary.",
    input: managementReadConversationRequestSchema,
    output: managementReadConversationResultSchema,
    annotations: read,
  },
  policy_read: {
    description:
      "Read active editable rules, fixed rules and the policy revision.",
    input: empty,
    output: policyReadSchema,
    annotations: read,
  },
  policy_update: {
    description:
      "Add, remove or replace editable rules. A behavior is a complete BEHAVIOR.md document. Remove and add the same id to replace a rule. Fixed rules refuse edits. Returns after the complete edit is active; saved decisions remain unchanged.",
    input: policyUpdateSchema,
    output: policyUpdateResultSchema,
    annotations: { readOnlyHint: false, openWorldHint: false },
  },
  firewall_decisions: {
    description:
      "Read completed firewall decision summaries since inclusive Unix milliseconds. Summaries include the reason and registered advice without message content or private model evidence.",
    input: firewallDecisionsRequestSchema,
    output: firewallDecisionsSchema,
    annotations: read,
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

/** Failure data derives from each service's error schema without a second reason vocabulary. */
export const toolFailureSchemas = {
  read_inbox: Schema.Struct({ reason: InboxError.fields.reason }),
  read_event: Schema.Struct({ reason: InboxError.fields.reason }),
  search_conversations: Schema.Struct({ reason: HistoryError.fields.reason }),
  read_conversation: Schema.Struct({ reason: HistoryError.fields.reason }),
  register: Schema.Struct({ reason: RegistrationError.fields.reason }),
  policy_read: policyFailure,
  policy_update: policyFailure,
  firewall_decisions: policyFailure,
} as const;

function addressesAreOrdered(addresses: readonly string[]): boolean {
  return isStrictlyAscending(addresses, compareAscii);
}
