/** @file Private MCP schemas for daemon management and history reads: exact requests, and results a reader decodes additively. */

import { Schema } from "effect";
import { Message, MessageReference } from "../communication/message.js";
import { AgentAddress, GroupAddress } from "../communication/records/index.js";
import { DeliveryToken } from "../communication/store/types.js";
import { exactParseOptions, exactStruct } from "../encoding/exact.js";
import { compareAscii, isStrictlyAscending } from "../encoding/identifier.js";
import { AgentCard, AgentView } from "../identity/cards.js";
import { AgentName, PrincipalId } from "../identity/identifiers.js";
import { OperationId } from "../identity/registry/contract.js";
import { localGuidanceSchema } from "./advice.js";
import { InboundItem } from "./items.js";

const encodedAgentCard = Schema.encodedSchema(AgentCard);

const messageAddress = Schema.Union(AgentAddress, GroupAddress).annotations({
  identifier: "MessageAddress",
});

function addressesAreOrdered(addresses: readonly string[]): boolean {
  return isStrictlyAscending(addresses, compareAscii);
}

const historyContinuation = Schema.NonEmptyString;

const historyRecord = Schema.Union(
  Schema.Struct({
    kind: Schema.Literal("sent"),
    message: Message,
  }),
  Schema.Struct({
    kind: Schema.Literal("received"),
    message: MessageReference,
    item: InboundItem,
    guidance: Schema.optionalWith(localGuidanceSchema, { exact: true }),
  }),
  Schema.Struct({
    kind: Schema.Literal("result"),
    deliveryToken: DeliveryToken,
    item: InboundItem,
    guidance: Schema.optionalWith(localGuidanceSchema, { exact: true }),
  }),
);

/** Closed local registration input; configured secrets are never tool fields. */
export const managementRegisterRequestSchema = exactStruct({
  operationId: Schema.propertySignature(OperationId).annotations({
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

/** Registry domain outcomes returned by daemon registration. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- The MCP catalog projects this result to JSON Schema; readers decode it additively with this schema.
export const managementRegisterResultSchema = Schema.Union(
  Schema.Struct({
    kind: Schema.Literal("registered"),
    agentCard: encodedAgentCard,
    agent: AgentView,
  }),
  Schema.Struct({ kind: Schema.Literal("name_taken") }),
  Schema.Struct({ kind: Schema.Literal("key_already_registered") }),
  Schema.Struct({ kind: Schema.Literal("idempotency_conflict") }),
);

/** The daemon lifecycle visible to its MCP clients. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- The MCP catalog projects this result to JSON Schema; readers decode it additively with this schema.
export const managementStatusResultSchema = Schema.Union(
  Schema.Struct({ kind: Schema.Literal("unregistered") }),
  Schema.Struct({
    kind: Schema.Literal("active"),
    agentCard: encodedAgentCard,
    agent: AgentView,
  }),
);

/** Canonically ordered local-conversation page selector. */
export const managementSearchConversationsRequestSchema = exactStruct({
  afterAddress: Schema.optional(messageAddress).annotations({
    description:
      "Exclusive canonical address cursor from the previous page; omit to start at the first local conversation.",
  }),
});

/** Canonically ordered local-conversation address page. */
export const managementSearchConversationsResultSchema = Schema.Struct({
  kind: Schema.Literal("page"),
  addresses: Schema.Array(messageAddress).pipe(Schema.maxItems(50)),
  hasMore: Schema.Boolean,
}).pipe(
  Schema.filter(
    (page) =>
      addressesAreOrdered(page.addresses) &&
      (!page.hasMore || page.addresses.length > 0),
  ),
);

/** Starts a fixed-head history read or resumes its saved position. */
export const managementReadConversationRequestSchema = Schema.Union(
  exactStruct({
    address: Schema.propertySignature(messageAddress).annotations({
      description: "Conversation address returned by search_conversations.",
    }),
  }),
  exactStruct({
    continuation: Schema.propertySignature(historyContinuation).annotations({
      description:
        "Continuation from the previous history page; reusable while that history is retained, including after restart.",
    }),
  }),
).annotations({ parseOptions: exactParseOptions });

/** Saved permitted messages and locally authored content for one conversation. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- The MCP catalog projects this result to JSON Schema; readers decode it additively with this schema.
export const managementReadConversationResultSchema = Schema.Struct({
  kind: Schema.Literal("page"),
  records: Schema.Array(historyRecord).pipe(Schema.maxItems(50)),
  continuation: Schema.NullOr(historyContinuation),
});

/** Decoded local registration request. */
export type ManagementRegisterRequest =
  typeof managementRegisterRequestSchema.Type;
/** Encoded Registry registration result crossing MCP. */
export type ManagementRegisterResult =
  typeof managementRegisterResultSchema.Type;
/** Encoded daemon status crossing MCP. */
export type ManagementStatusResult = typeof managementStatusResultSchema.Type;
/** Decoded local conversation search request. */
export type ManagementSearchConversationsRequest =
  typeof managementSearchConversationsRequestSchema.Type;
/** Encoded local conversation search result. */
export type ManagementSearchConversationsResult =
  typeof managementSearchConversationsResultSchema.Type;
/** Decoded local certified-history request. */
export type ManagementReadConversationRequest =
  typeof managementReadConversationRequestSchema.Type;
/** Saved peer views and guidance remain unchanged by later policy edits. */
export type ManagementReadConversationResult =
  typeof managementReadConversationResultSchema.Type;
