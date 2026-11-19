/** @file The message firewall's values: message-policy loading and what a host reads of a check. Its service is `service.ts`. */

/** Guidance read with a checked message, what a check permits or refuses, and the decision provider that judges text rules. */
export {
  type DecisionProvider,
  type DecisionRequest,
  type DecisionResponse,
  FirewallRefusal,
  LocalGuidance,
} from "./check.js";
/** A compiled norm, which the norms layer produces and the firewall checks messages against. */
export type { CompiledContract } from "./contract.js";
/** One edit of an agent's editable policy, with each norm document it adds compiled. */
export type { PolicyEdit } from "./editable-policy.js";
/** Text rules, message-policy loading from its file, and the schemas of policy reads, updates and decisions. */
export {
  firewallDecisionsRequestSchema,
  firewallDecisionsSchema,
  FirewallPolicyError,
  type LoadedMessagePolicy,
  loadMessagePolicy,
  policyReadSchema,
  policyRevisionSchema,
  policyUpdateSchema,
} from "./rules.js";
