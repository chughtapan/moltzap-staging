/** @file Exact closed schemas for every Client-owned conversation value. */

import { Schema } from "effect";
import { exactStruct } from "../../encoding/exact.js";
import { isCanonicalIdentifier } from "../../encoding/identifier.js";
import { AgentCard } from "../../identity/cards.js";
import { SignedMessage } from "../../identity/envelope/signed-message.js";
import { AgentId } from "../../identity/identifiers.js";
import { wireVersions } from "../../identity/wire-version.js";
import { RouterInstanceId } from "../router/api/contract.js";
import { Content, maximumMembers, MsgId } from "./values.js";

/* eslint-disable jsdoc/require-jsdoc -- These package-private Schema names are the exact closed protocol vocabulary. */

const versionAndKind = <const Kind extends string>(kind: Kind) => ({
  sharnessVersion: wireVersions.history,
  kind: Schema.Literal(kind),
});

const canonicalIdentifier = <const Name extends string>(
  name: Name,
  prefix: string,
) =>
  Schema.String.pipe(
    Schema.filter((value) => isCanonicalIdentifier(prefix, value), {
      identifier: name,
      description: `${name} canonical representation`,
    }),
    Schema.brand(name),
    Schema.annotations({
      identifier: name,
      description: `${name} canonical representation`,
    }),
  );

/* eslint-disable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Private Effect Schemas share their domain names with the values they decode. */

export const ConversationId = canonicalIdentifier("ConversationId", "cnv_");
export type ConversationId = typeof ConversationId.Type;
export const MembershipHash = canonicalIdentifier("MembershipHash", "mbr_");
export type MembershipHash = typeof MembershipHash.Type;
export const MessageIntentHash = canonicalIdentifier(
  "MessageIntentHash",
  "mit_",
);
export type MessageIntentHash = typeof MessageIntentHash.Type;
export const AnchorHash = canonicalIdentifier("AnchorHash", "anc_");
export type AnchorHash = typeof AnchorHash.Type;
export const ActionHash = canonicalIdentifier("ActionHash", "ach_");
export type ActionHash = typeof ActionHash.Type;
export const RecordHash = canonicalIdentifier("RecordHash", "rch_");
export type RecordHash = typeof RecordHash.Type;

const encodedAgentCard = Schema.encodedBoundSchema(AgentCard);
const encodedSignedMessage = Schema.encodedBoundSchema(SignedMessage);
const MemberAgentIds = Schema.Tuple([AgentId, AgentId], AgentId).pipe(
  Schema.maxItems(maximumMembers),
);
const MembershipMembers = Schema.Tuple(
  [encodedAgentCard, encodedAgentCard],
  encodedAgentCard,
).pipe(Schema.maxItems(maximumMembers));
const EvidenceMessages = Schema.NonEmptyArray(encodedSignedMessage).pipe(
  Schema.maxItems(maximumMembers),
);

export const ConversationIdentityInput = exactStruct({
  kind: Schema.Literal("conversation_identity"),
  memberAgentIds: MemberAgentIds,
});
export type ConversationIdentityInput = typeof ConversationIdentityInput.Type;

export const MembershipDescriptor = exactStruct({
  ...versionAndKind("membership_descriptor"),
  conversationId: ConversationId,
  members: MembershipMembers,
});
export type MembershipDescriptor = typeof MembershipDescriptor.Type;

export const MessageIntent = exactStruct({
  ...versionAndKind("message_intent"),
  conversationId: ConversationId,
  membershipHash: MembershipHash,
  authorAgentId: AgentId,
  msgId: MsgId,
  content: Content,
});
export type MessageIntent = typeof MessageIntent.Type;

export const InitialAnchorBody = exactStruct({
  ...versionAndKind("initial_anchor_body"),
  conversationId: ConversationId,
  membershipHash: MembershipHash,
  routerInstanceId: RouterInstanceId,
});
export type InitialAnchorBody = typeof InitialAnchorBody.Type;

export const ReanchorBody = exactStruct({
  ...versionAndKind("reanchor_body"),
  conversationId: ConversationId,
  membershipHash: MembershipHash,
  previousAnchorHash: AnchorHash,
  selectedRecordHash: RecordHash,
  routerInstanceId: RouterInstanceId,
});
export type ReanchorBody = typeof ReanchorBody.Type;

// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- Private hash code composes the complete closed anchor union.
export const AnchorBody = Schema.Union(InitialAnchorBody, ReanchorBody);
export type AnchorBody = typeof AnchorBody.Type;

export const FirstMessageActionCore = exactStruct({
  ...versionAndKind("FIRST_MESSAGE"),
  conversationId: ConversationId,
  membership: MembershipDescriptor,
  anchor: InitialAnchorBody,
  previousRecordHash: Schema.Null,
  messageIntent: MessageIntent,
  messageIntentHash: MessageIntentHash,
});
export type FirstMessageActionCore = typeof FirstMessageActionCore.Type;

export const MessageActionCore = exactStruct({
  ...versionAndKind("MESSAGE"),
  conversationId: ConversationId,
  membershipHash: MembershipHash,
  anchorHash: AnchorHash,
  previousRecordHash: RecordHash,
  messageIntent: MessageIntent,
  messageIntentHash: MessageIntentHash,
});
export type MessageActionCore = typeof MessageActionCore.Type;

// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- Private protocol code composes the complete closed action union.
export const ActionCore = Schema.Union(
  FirstMessageActionCore,
  MessageActionCore,
);
export type ActionCore = typeof ActionCore.Type;

const FirstMessageRecordCore = exactStruct({
  ...versionAndKind("record_core"),
  membership: MembershipDescriptor,
  anchorHash: AnchorHash,
  action: FirstMessageActionCore,
  actionHash: ActionHash,
});
const MessageRecordCore = exactStruct({
  ...versionAndKind("record_core"),
  membershipHash: MembershipHash,
  anchorHash: AnchorHash,
  action: MessageActionCore,
  actionHash: ActionHash,
});
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- Private record decoding and hashing compose the complete closed record-core union.
export const RecordCore = Schema.Union(
  FirstMessageRecordCore,
  MessageRecordCore,
);
export type RecordCore = typeof RecordCore.Type;

export const ActionSignatureStatement = exactStruct({
  ...versionAndKind("action_signature"),
  signerAgentId: AgentId,
  actionHash: ActionHash,
});
export type ActionSignatureStatement = typeof ActionSignatureStatement.Type;

const DurabilityVoteStatement = exactStruct({
  ...versionAndKind("durability_vote"),
  signerAgentId: AgentId,
  conversationId: ConversationId,
  membershipHash: MembershipHash,
  recordHash: RecordHash,
});

export const ReanchorVoteStatement = exactStruct({
  ...versionAndKind("reanchor_vote"),
  signerAgentId: AgentId,
  anchorHash: AnchorHash,
  reanchor: ReanchorBody,
});
export type ReanchorVoteStatement = typeof ReanchorVoteStatement.Type;

const ActionCertificate = exactStruct({
  ...versionAndKind("action_certificate"),
  actionHash: ActionHash,
  signatures: EvidenceMessages,
});

const DurabilityCertificate = exactStruct({
  ...versionAndKind("durability_certificate"),
  recordHash: RecordHash,
  votes: EvidenceMessages,
});

const ReanchorCertificate = exactStruct({
  ...versionAndKind("reanchor_certificate"),
  anchorHash: AnchorHash,
  votes: EvidenceMessages,
});

export const CompletedReanchor = exactStruct({
  ...versionAndKind("completed_reanchor"),
  anchorHash: AnchorHash,
  reanchor: ReanchorBody,
  certificate: ReanchorCertificate,
});
export type CompletedReanchor = typeof CompletedReanchor.Type;

// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- Private record verification composes the complete closed anchor union.
export const RouterAnchor = Schema.Union(InitialAnchorBody, CompletedReanchor);
export type RouterAnchor = typeof RouterAnchor.Type;

export const ActionCertifiedRecord = exactStruct({
  ...versionAndKind("action_certified_record"),
  recordHash: RecordHash,
  recordCore: RecordCore,
  routerAnchor: RouterAnchor,
  actionCertificate: ActionCertificate,
});
export type ActionCertifiedRecord = typeof ActionCertifiedRecord.Type;

export const CertifiedRecord = exactStruct({
  ...versionAndKind("certified_record"),
  actionCertifiedRecord: ActionCertifiedRecord,
  durabilityCertificate: DurabilityCertificate,
});
export type CertifiedRecord = typeof CertifiedRecord.Type;

export const CatchUpRequest = exactStruct({
  ...versionAndKind("catch_up_request"),
  conversationId: ConversationId,
  membershipHash: MembershipHash,
  requesterAgentId: AgentId,
  knownRecordHash: Schema.NullOr(RecordHash),
  knownAnchorHash: Schema.NullOr(AnchorHash),
}).pipe(
  Schema.filter(
    (request) =>
      (request.knownRecordHash === null) === (request.knownAnchorHash === null),
  ),
);
export type CatchUpRequest = typeof CatchUpRequest.Type;

const CatchUpItem = Schema.Union(CertifiedRecord, CompletedReanchor);

export const CatchUpAttestationStatement = exactStruct({
  ...versionAndKind("catch_up_attestation"),
  signerAgentId: AgentId,
  request: CatchUpRequest,
  itemKind: Schema.Literal(
    "certified_record",
    "completed_reanchor",
    "incomplete",
  ),
  itemHash: Schema.NullOr(Schema.Union(RecordHash, AnchorHash)),
  hasMore: Schema.Boolean,
});
export type CatchUpAttestationStatement =
  typeof CatchUpAttestationStatement.Type;

export const CatchUpPage = exactStruct({
  ...versionAndKind("catch_up_page"),
  request: CatchUpRequest,
  item: CatchUpItem,
  hasMore: Schema.Boolean,
  attestation: encodedSignedMessage,
});
export type CatchUpPage = typeof CatchUpPage.Type;

export const CatchUpIncomplete = exactStruct({
  ...versionAndKind("catch_up_incomplete"),
  request: CatchUpRequest,
  attestation: encodedSignedMessage,
});
export type CatchUpIncomplete = typeof CatchUpIncomplete.Type;

export const ActionProposal = exactStruct({
  ...versionAndKind("action_proposal"),
  action: ActionCore,
});
export type ActionProposal = typeof ActionProposal.Type;

// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- Private signing and verification compose the complete closed evidence union.
export const EvidenceStatement = Schema.Union(
  ActionSignatureStatement,
  DurabilityVoteStatement,
  ReanchorVoteStatement,
  CatchUpAttestationStatement,
);
export type EvidenceStatement = typeof EvidenceStatement.Type;

// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- Private Router-envelope codecs compose the complete closed packet union.
export const DirectPacket = Schema.Union(
  ActionProposal,
  ActionCertifiedRecord,
  CertifiedRecord,
  CompletedReanchor,
  CatchUpRequest,
  CatchUpPage,
  CatchUpIncomplete,
);
export type DirectPacket = typeof DirectPacket.Type;

/* eslint-enable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Restore the package naming rules. */

/* eslint-enable jsdoc/require-jsdoc -- Restore package documentation rules. */
