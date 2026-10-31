/** @file Communication state operations using the daemon’s shared SQL client. */
// safer-arch-ignore no-inventory-barrel: Named SQL operations expose the communication state contract through one boundary without a second store interface.

/** Failures exposed by communication persistence. */
export { EndpointStoreError } from "./database/values.js";
/** Saved communication records and recovery state. */
export type {
  CertifiedRecord,
  ConversationFoundation,
  EmptyConversationRestart,
  EndpointRecovery,
  MessageIntent,
  OutboundMessageInput,
  ProposalLock,
  ProtocolEvidence,
  RestartedEmptyConversation,
  StagedReanchor,
  StagedRecord,
  StoredMembership,
  StoredOutboundMessage,
  StoreMutation,
} from "./types.js";
/** Stable opaque durable-delivery identity. */
export { DeliveryToken } from "./types.js";
/** Whether a store error refuses its input rather than reporting a failed store. */
export { isSemanticStoreRejection } from "./types.js";
/** Canonical runtime persistence shares the store error vocabulary. */
export { decodeRuntimeValue, encodeRuntimeValue } from "./runtime-codec.js";
/** Pending envelopes cannot be re-signed when their live window expires. */
export { OutboxTooOldError } from "./database/values.js";
/** Protocol writes and reads use the caller's shared SQL environment. */
export {
  applyCatchUpReanchor,
  bindMessageIntent,
  completeReanchor,
  lockFirstMessageProposal,
  lockProposal,
  putConversationFoundation,
  restartEmptyConversation,
  stageReanchor,
} from "./anchors.js";
/** Accepted work, certified history and network retry obligations share SQL transactions. */
export {
  beginOutbound,
  completeOutbound,
  discardOutbound,
  enqueueOutbound,
  readRecordOutbound,
} from "./queues/outbound.js";
/** History pages remain anchored while later records are appended. */
export {
  consumeRecord,
  readStoredConversation as readConversation,
  readMemberships,
  readRecentConversation,
  readUnconsumedRecords,
  recoverStoredState as recover,
} from "./reads.js";
/** Record acceptance retains exact bytes and separately attributed evidence. */
export {
  applyCertifiedRecord,
  mergeEvidence,
  promoteRecord,
  stageRecordAndOutbound,
  supersedeProposalLock,
} from "./records.js";

/** Accepted message payloads and terminal outcomes share the caller's transaction. */
export {
  acceptMessage,
  failMessage,
  hasPendingMessages,
  readAcceptedMessage,
  readUnpreparedMessages,
} from "./messages.js";
