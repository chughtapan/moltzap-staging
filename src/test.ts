/** @file Offline evaluation and process fixtures under the test import condition. */

/** Replaces a configured decision provider around the daemon's own composition. */
export { DecisionProviderOverride } from "./firewall/service.js";
/** Offline firewall checks with a caller-supplied provider. */
export type { DecisionProvider, DecisionRequest } from "./firewall/index.js";

/** Database selection uses the production driver composition. */
export { databaseConnection, databaseUrl } from "./database/index.js";
/** An isolated, prepared database for one test, and the login a daemon serves it through. */
export { openTestDatabase, restrictedLogin } from "./database/testing.js";
