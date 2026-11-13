/** @file The message firewall's values: the rule chain other layers extend, policy loading and the results a host shows. Its service is `service.ts`. */

/** The rule chain and the matcher contract a module implements to add checks to it. */
export { type FirewallEvidence } from "./chain.js";
/** Guidance shown with a checked message, the permitted, withheld and refused results of a check, and the decision provider that assesses text rules. */
export {
  type CheckedGuidance,
  checkedGuidanceSchema,
  type DecisionProvider,
  DecisionProviderError,
  type DecisionRequest,
  type DecisionResponse,
  FirewallRefusal,
  LocalGuidance,
  PermittedItem,
  Withheld,
} from "./check.js";

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
} from "./rules.js";

/** Immutable contracts configure structural and conversation checks. */
export {
  type CompiledContract,
  compiledContractSchema,
  type MessageForm,
} from "./contract.js";
