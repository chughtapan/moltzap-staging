/** @file Completed policy decisions and editable rule persistence. */

/** Stored policy and decision values are interpreted by their domain owners. */
export type { FirewallActivation, FirewallCompletion } from "./types.js";
/** Rules and completed checks share the daemon's SQL client. */
export {
  readDecisions,
  readFirewallActivations,
  readFirewallDecisions,
  readFirewallPolicy,
  saveCheckDecision,
  withoutTraces,
  writeFirewallPolicy,
} from "./firewall.js";
