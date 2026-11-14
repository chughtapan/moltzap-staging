/** @file SQL access for every domain store: the connection, queries, transactions and migrations. */

/** Prepares a database and checks that one is prepared. */
export { checkDatabase, migrateDaemon, prepareDatabase } from "./migrations.js";
/** The configured connection, its query builders, and transactions with work to run once one commits. */
export {
  afterCommit,
  type Compilable,
  databaseConnection,
  databaseUrl,
  executeQuery,
  type Generated,
  makeQueries,
  transaction,
  withAgentRows,
} from "./sql.js";
