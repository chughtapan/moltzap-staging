/** @file SQL access for every domain store: the connection, queries, transactions, migrations and sealed credentials. */

/** Prepares a database and checks that one is prepared. */
export { checkDatabase, migrateDaemon, prepareDatabase } from "./migrations.js";
/** The configured connection, its query builders, and transactions with work to run once one commits. */
export {
  afterCommit,
  type Compilable,
  databaseAccess,
  databaseConnection,
  executeQuery,
  type Generated,
  makeQueries,
  transaction,
  withAgentTransaction,
} from "./sql.js";
/** Seals a credential for storage and opens it again. */
