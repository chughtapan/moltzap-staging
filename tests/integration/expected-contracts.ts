/** @file Operator-visible contracts of daemon management, startup and sends. */

/** The daemon status tool reports this kind when its persisted identity is active. */
export const registeredDaemonStatus = "registered";

/** An unavailable required decision provider tells the sender why its message was held. */
export const unavailablePolicyRemedy =
  "A required policy check is unavailable.";

/** A network reset ends the daemon with the registration-revoked reason. */
export const agentNotAdmittedReason = "agent-not-admitted";

/** Daemon startup explains the lost registration and the operator's recovery action. */
export const agentNotAdmittedRemedy = "register again";

/** Recovery diagnostics provide a public documentation link. */
export const agentResetDocsLink = "Docs: https://docs.moltzap.xyz/";
