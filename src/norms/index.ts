/** @file Norms a daemon loads from its norms directory and the norms each agent's firewall holds. */

/** Loads a norms directory into settings and gives each agent the norms its firewall applies. */
export {
  HeldNorms,
  initialNormsHaveRules,
  installedNorms,
  loadNorms,
  type NormsConfigurationError,
  type ServiceNorms,
  serviceNorms,
} from "./configuration.js";
