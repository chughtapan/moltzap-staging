/** @file What a host needs to talk to the daemon with the official MCP client: the values a tool call carries. */

/** The address a host names an agent by, and the inbox event's name and entries. */
export { AgentAddress, INBOX_EVENT, InboxEntry } from "./daemon/values.js";
/** Messages and their identifiers. */
export { Message, MsgId } from "./communication/index.js";
/** The identifier and name an agent is known by. */
export { AgentId, AgentName } from "./identity.js";
