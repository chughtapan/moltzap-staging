/** @file A server's norms: loading a norms directory and the structure check built from its norms. */

/** Loads a norms directory as the norms of its documents, or says why it cannot. */
export { loadNorms, NormsConfigurationError } from "./compiler.js";
/** The norms a server holds, and whether any has English rules. */
export { initialNormsHaveRules, type InstalledNorms } from "./norm.js";
/** The firewall's structure check for an agent that holds norms. */
export { structureCheck } from "./structure-check.js";
