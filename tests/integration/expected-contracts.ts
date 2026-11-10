/** @file Operator-visible contracts of daemon management, startup and sends. */

/** A database the daemon cannot open with its key tells the operator what to prepare. */
export const incompatibleDatabaseRemedy =
  "prepare a compatible database with its existing unlock key";
