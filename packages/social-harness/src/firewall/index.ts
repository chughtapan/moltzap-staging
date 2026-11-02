/** @file Message policy exposes permitted content and keeps assessment details private. */

/** Policy checks retain private evidence separately from the agent's permitted content. */
export {
  CollectiveResult,
  type CompletedDecision,
  Firewall,
  type FirewallEvidence,
  type FirewallOptions,
  FirewallRefusal,
  type IncomingDecision,
  loadMessagePolicy,
  LocalGuidance,
  MessagePolicy,
  type OutgoingDecision,
  PermittedItem,
  type ResultEvidence,
  Withheld,
} from "./service.js";
