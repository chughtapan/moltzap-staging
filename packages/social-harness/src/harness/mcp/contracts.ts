/** @file Canonical MCP tool names, argument schemas and published result schemas. */
import { Schema } from "effect";
import {
  firewallDecisionsRequestSchema,
  firewallDecisionsSchema,
  FirewallPolicyError,
  policyReadSchema,
  policyUpdateResultSchema,
  policyUpdateSchema,
} from "../../firewall/rules/config.js";
import {
  managementReadConversationRequestSchema,
  managementReadConversationResultSchema,
  managementRegisterRequestSchema,
  managementRegisterResultSchema,
  managementSearchConversationsRequestSchema,
  managementSearchConversationsResultSchema,
  managementStatusResultSchema,
} from "../../host-protocol/daemon-tools.js";
import { webhookStatusSchema } from "../../host-protocol/event-schemas.js";
import {
  harnessAcknowledgeDeliveryRequestSchema,
  harnessSendArgumentsSchema,
  inboxRequestSchema,
  inboxResultSchema,
  readEventRequestSchema,
  readEventResultSchema,
} from "../../host-protocol/operations.js";
import { SendResultSchema } from "../../host-protocol/send-result.js";
import { RegistrationError } from "../../identity/service.js";
import { DeliveryAcknowledgeError, InboxError } from "../inbox.js";
import { HistoryError } from "../service.js";

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
      "Read pending inbox items and their saved advice. Follow nextCursor until absent. Reading does not acknowledge delivery. Use read_event to retrieve a known item after acknowledgment.",
    input: inboxRequestSchema,
    output: inboxResultSchema,
    annotations: read,
  },
  read_event: {
    description:
      "Retrieve a saved inbox item and its advice by eventId, including after acknowledgment or restart. This does not acknowledge delivery.",
    input: readEventRequestSchema,
    output: readEventResultSchema,
    annotations: read,
  },
  acknowledge_delivery: {
    description:
      "Acknowledge an inbox item after the host accepts it. Repeating acknowledgment is harmless. Saved content remains available through read_event and history.",
    input: harnessAcknowledgeDeliveryRequestSchema,
    output: empty,
    annotations: {
      readOnlyHint: false,
      destructiveHint: false,
      idempotentHint: true,
      openWorldHint: false,
    },
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

/** The actual MCP tool names, rather than a second semantic client API. */
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
  acknowledge_delivery: Schema.Struct({
    reason: DeliveryAcknowledgeError.fields.reason,
  }),
  search_conversations: Schema.Struct({ reason: HistoryError.fields.reason }),
  read_conversation: Schema.Struct({ reason: HistoryError.fields.reason }),
  register: Schema.Struct({ reason: RegistrationError.fields.reason }),
  policy_read: policyFailure,
  policy_update: policyFailure,
  firewall_decisions: policyFailure,
} as const;
