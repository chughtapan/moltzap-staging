/** @file The message firewall: its service, the rule chain other layers extend, policy loading and the results a host shows. */

/** The rule chain and the matcher contract a module implements to add checks to it. */
export {
  type Chain,
  chainRuleFileSchema,
  type FirewallEvidence,
  type Matcher,
  type MatchInput,
  type MatchResult,
} from "./chain.js";
/** Guidance shown with a checked message, and the decision provider that assesses text rules. */
export {
  type CheckedGuidance,
  checkedGuidanceSchema,
  type DecisionProvider,
  DecisionProviderError,
  type DecisionRequest,
  type DecisionResponse,
  type Message,
} from "./check.js";
/** Norm documents an agent's editable policy holds. */
export type { NormDocuments } from "./editable-policy.js";
/** Text rules, policy loading from configuration, and the schemas of policy reads, updates and decisions. */
export {
  firewallDecisionsRequestSchema,
  firewallDecisionsSchema,
  FirewallPolicyError,
  firewallRefusalReasonSchema,
  installedMessagePolicy,
  loadMessagePolicy,
  policyReadSchema,
  policyUpdateResultSchema,
  policyUpdateSchema,
  readPolicyCredential,
  TextRule,
} from "./rules.js";
/** The firewall service, its options, and the permitted, withheld and refused results of a check. */
export {
  DecisionProviderOverride,
  Firewall,
  type FirewallOptions,
  FirewallRefusal,
  type IncomingDecision,
  LocalGuidance,
  PermittedItem,
  type ResultEvidence,
  Withheld,
} from "./service.js";
