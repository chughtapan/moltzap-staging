/** @file Operator-visible contracts of daemon management, startup and sends. */

/** The daemon status tool reports this kind when its persisted identity is active. */
export const registeredDaemonStatus = "registered";

/** Daemon startup reports this phase when its persisted state or identity row refuses this binary. */
export const storageStartupPhase = "storage";

/** DaemonStartupError.phase reports this value when startup configuration is invalid. */
export const configurationStartupPhase = "configuration";

/** Daemon startup reports this reason when retained outbound bytes have left the live window. */
export const outboxTooOldReason = "outbox-too-old";

/** Daemon startup tells operators to drain retained outbound bytes with the previous release. */
export const outboxTooOldRemedy = "previous release";

/** Daemon startup links this heading when an outbox requires a reset release. */
export const resetReleaseDocsAnchor = "guides/upgrades#apply-a-reset-release";

/** Daemon startup names this setting when its message-policy document is invalid. */
export const messagePolicyFileSetting = "SHARNESS_MESSAGE_POLICY_FILE";

/** An unavailable required decision provider tells the sender why its message was held. */
export const unavailablePolicyRemedy =
  "A required policy check is unavailable.";

/** A network reset ends the daemon with the registration-revoked reason. */
export const agentNotAdmittedReason = "agent-not-admitted";

/** Daemon startup explains the lost registration and the operator's recovery action. */
export const agentNotAdmittedRemedy = "register again";

/** Recovery diagnostics provide a public documentation link. */
export const agentResetDocsLink = "Docs: https://docs.moltzap.xyz/";
