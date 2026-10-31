/** @file Package-private vocabulary used by Client protocol consumers. */
// safer-arch-ignore no-inventory-barrel: Sibling modules implement one closed protocol representation.

/* eslint-disable jsdoc/require-jsdoc -- Re-exported private symbols retain their owning-module documentation. */

export {
  type DecodedOuterBody,
  decodeOuterBody,
  deriveConversationId,
  deriveEvidenceEnvelopeId,
  hashAction,
  hashAnchor,
  hashMembershipDescriptor,
  hashMessageIntent,
  hashRecord,
  mintMsgId,
  type OuterBodyReader,
  signEvidenceMessage,
  signOuterEvidence,
  signOuterPacket,
} from "./encoding.js";
export {
  ActionCertifiedRecord,
  ActionCore,
  ActionHash,
  ActionProposal,
  ActionSignatureStatement,
  AnchorHash,
  CatchUpIncomplete,
  CatchUpPage,
  CatchUpRequest,
  CertifiedRecord,
  CompletedReanchor,
  ConversationId,
  DirectPacket,
  EvidenceStatement,
  FirstMessageActionCore,
  InitialAnchorBody,
  MembershipDescriptor,
  MembershipHash,
  MessageActionCore,
  MessageIntent,
  ReanchorBody,
  ReanchorVoteStatement,
  RecordCore,
  RecordHash,
  RouterAnchor,
} from "./schemas.js";
export {
  AgentAddress,
  compareAgentIds,
  Content,
  GroupAddress,
  maximumContentBytes,
  maximumMembers,
  MsgId,
} from "./values.js";

export {
  memberCard,
  quorumThreshold,
  type VerifiedEvidence,
  type VerifiedMembership,
  verifyActionCertifiedRecord,
  verifyActionCore,
  verifyActionProposal,
  verifyCatchUpIncomplete,
  verifyCatchUpPage,
  verifyCertifiedRecord,
  verifyCompletedReanchor,
  verifyDeliveredEvidence,
  verifyMembershipDescriptor,
  verifyOuterMessage,
  verifyRecordCore,
  verifyStableEvidence,
} from "./verification.js";

/* eslint-enable jsdoc/require-jsdoc -- Restore package documentation rules. */
