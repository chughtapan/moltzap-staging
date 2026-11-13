/** @file Agent identifiers, names and cards, and the service that names the agents of a daemon. */

/** An agent's identifier, name, principal and card. */
export {
  AGENT_NAME_RULE,
  AgentCard,
  AgentId,
  AgentName,
  PrincipalId,
} from "./agent-cards.js";
/** This agent's card and the other agents of its daemon by name. */
export { Identity, IdentityError } from "./service.js";
