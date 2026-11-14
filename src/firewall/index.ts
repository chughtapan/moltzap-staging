/** @file The message firewall's values: the chain other layers extend, message-policy loading and what a host reads of a check. Its service is `service.ts`. */

/** The chain and the matcher contract a module implements to add checks to it. */
export {
  type Chain,
  chainRuleFileSchema,
  type FirewallEvidence,
  type Match,
  type Matcher,
  type MessageToMatch,
} from "./chain.js";
/** Guidance read with a checked message, what a check permits or refuses, and the decision provider that judges text rules. */
export {
  checkedGuidanceSchema,
  type DecisionProvider,
  DecisionProviderError,
  type DecisionRequest,
  type DecisionResponse,
  FirewallRefusal,
  LocalGuidance,
  PermittedItem,
} from "./check.js";
/** Norm documents an agent's editable policy holds. */
export type { NormDocuments } from "./editable-policy.js";
/** Text rules, message-policy loading from its file, and the schemas of policy reads, updates and decisions. */
export {
  firewallDecisionsRequestSchema,
  firewallDecisionsSchema,
  FirewallPolicyError,
  firewallRefusalReasonSchema,
  loadedMessagePolicySchema,
  loadMessagePolicy,
  policyReadSchema,
  policyRevisionSchema,
  policyUpdateSchema,
  readPolicyCredential,
  TextRule,
} from "./rules.js";
