/** @file Operator-visible contracts of daemon management, startup and sends. */

/** The daemon status tool reports this kind when its persisted identity is active. */
export const registeredDaemonStatus = "registered";

/** An unavailable required decision provider tells the sender why its message was held. */
export const unavailablePolicyRemedy =
  "A required policy check is unavailable.";

/** A database the daemon cannot open with its key tells the operator what to prepare. */
export const incompatibleDatabaseRemedy =
  "prepare a compatible database with its existing unlock key";
