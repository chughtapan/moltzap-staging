/** @file What `main.ts` puts the `sharnessd` process together from. */

/** The owner's agent, which the server agent's row is read with at startup. */
export { getOrCreateOwnedAgent } from "./agent.js";
/** The app that answers every request, and the listener that serves it. */
export { makeMcpHttpApp, serveMcp } from "./server.js";
/** The settings file's reader, a startup refusal, the span exporter and the package's version. */
export {
  loadDaemonSettings,
  packageVersion,
  refused,
  spanExport,
} from "./settings.js";
/** The database connection and the preparation of its tables. */
export { databaseConnection, databaseUrl, prepareDatabase } from "./store.js";
