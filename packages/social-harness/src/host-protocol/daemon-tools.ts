/** @file Private MCP schemas for daemon management and history reads: exact requests, and results a reader decodes additively. */

import { Schema } from "effect";
import {
  Message,
  MessageReference,
  PaginatedRequest,
  PaginatedResult,
} from "../communication/index.js";
import { AgentAddress, GroupAddress } from "../communication/records/values.js";
import { exactStruct } from "../encoding/exact.js";
import { compareAscii, isStrictlyAscending } from "../encoding/identifier.js";
import { LocalGuidance, PermittedItem } from "../firewall/service.js";
import { DeliveryToken } from "../harness/inbox.js";
import {
  AgentIdentityCard,
  AgentName,
  PrincipalId,
} from "../identity/agent.js";
import { RegistrationId } from "../identity/registry/contract.js";
const savedItemSchema = Schema.typeSchema(PermittedItem);

const encodedAgentCard = Schema.encodedBoundSchema(AgentIdentityCard);

const messageAddress = Schema.Union(AgentAddress, GroupAddress).annotations({
  identifier: "MessageAddress",
});

function addressesAreOrdered(addresses: readonly string[]): boolean {
  return isStrictlyAscending(addresses, compareAscii);
}

const historyRecord = Schema.Union(
  Schema.Struct({
    kind: Schema.Literal("sent"),
    message: Schema.typeSchema(Message),
  }),
  Schema.Struct({
    kind: Schema.Literal("received"),
    address: messageAddress,
    message: MessageReference,
    item: savedItemSchema,
    guidance: Schema.optionalWith(LocalGuidance, { exact: true }),
  }),
  Schema.Struct({
    kind: Schema.Literal("result"),
    address: messageAddress,
    deliveryToken: DeliveryToken,
    item: savedItemSchema,
    guidance: Schema.optionalWith(LocalGuidance, { exact: true }),
  }),
);

/** Closed local registration input; configured secrets are never tool fields. */
export const managementRegisterRequestSchema = exactStruct({
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

/** The signed identity document returned after successful registration. */
export const managementRegisterResultSchema = encodedAgentCard;

/** Saved identity is visible while communication activation proceeds. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- The catalog and clients share this owning wire schema.
export const managementStatusResultSchema = Schema.Union(
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
  addresses: Schema.Array(messageAddress).pipe(Schema.maxItems(50)),
  ...PaginatedResult.fields,
}).pipe(Schema.filter((page) => addressesAreOrdered(page.addresses)));

/** Keeps one conversation address while following its opaque history cursor. */
export const managementReadConversationRequestSchema = exactStruct({
  address: messageAddress,
  ...PaginatedRequest.fields,
});

/** Saved messages and results at the traversal's fixed history boundaries. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- The catalog and clients share this owning wire schema.
export const managementReadConversationResultSchema = Schema.Struct({
  records: Schema.Array(historyRecord).pipe(Schema.maxItems(50)),
  ...PaginatedResult.fields,
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
