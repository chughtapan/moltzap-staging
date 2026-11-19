/** @file Offline evaluation and process fixtures under the test import condition. */

/** Database selection uses the production driver composition. */
export { databaseConnection, databaseUrl } from "./database/index.js";
/** An isolated, prepared database for one test, and the login a daemon serves it through. */
export { openTestDatabase, restrictedLogin } from "./database/testing.js";
/** An OAuth issuer on this machine and the tokens that sign its subjects in. */
export { issuerFixture, requiredScope } from "./daemon/testing.js";
