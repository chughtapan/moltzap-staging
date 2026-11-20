/** @file The message firewall's values: message-policy loading and what a host reads of a check. Its service is `service.ts`. */

/** Guidance read with a checked message, what a check permits or refuses, the decision provider that judges text rules, and the structure check a higher layer supplies. */
export {
  type DecisionProvider,
  type DecisionRequest,
  type DecisionResponse,
  type EarlierMessage,
  FirewallRefusal,
  LocalGuidance,
  type StructureCheck,
} from "./check.js";
/** Text rules, message-policy loading from its file, and the schemas of decisions. */
export {
  firewallDecisionsRequestSchema,
  firewallDecisionsSchema,
  FirewallPolicyError,
  type LoadedMessagePolicy,
  loadMessagePolicy,
  TextRule,
} from "./rules.js";
