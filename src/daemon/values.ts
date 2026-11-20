/**
 * @file The values of the daemon that the package's main entry gives a host:
 * the address grammar, the inbox entry and the inbox event's name. The file
 * loads neither the database nor the listener, so a host that imports the
 * entry loads neither.
 */

import { Schema } from "effect";
import { Message } from "../communication/index.js";
import { LocalGuidance } from "../firewall/index.js";
import { AGENT_NAME_RULE, AgentName } from "../identity.js";

/** The scheme of an address naming one agent. */
export const AGENT_ADDRESS_PREFIX = "agent:";

const isAgentName = Schema.is(AgentName);

/** The agent name an `agent:` address names, or undefined when the value is not an agent address. */
const parseAgentAddress = (value: string): AgentName | undefined => {
  if (!value.startsWith(AGENT_ADDRESS_PREFIX)) {
    return undefined;
  }
  const name = value.slice(AGENT_ADDRESS_PREFIX.length);
  return isAgentName(name) ? name : undefined;
};

/** The address of one agent. */
export const AgentAddress = Schema.String.pipe(
  Schema.filter((value) => parseAgentAddress(value) !== undefined, {
    description: `agent:<name>; a name is ${AGENT_NAME_RULE}`,
  }),
  Schema.brand("AgentAddress"),
  Schema.annotations({ identifier: "AgentAddress" }),
);
/**
 * A validated agent address.
 * @useDeclaredType
 */
export type AgentAddress = typeof AgentAddress.Type;

/** One message as the host reads it. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- The inbox owns this grammar shared by persistence and MCP schema composition.
export const InboxEntry = Schema.Struct({
  address: AgentAddress,
  message: Message,
  guidance: Schema.optionalWith(LocalGuidance, { exact: true }),
});
/** @useDeclaredType */
export type InboxEntry = typeof InboxEntry.Type;

/** The name of the event whose occurrences are an agent's inbox entries, in read order. */
export const INBOX_EVENT = "org.social-harness/inbox.pending";
