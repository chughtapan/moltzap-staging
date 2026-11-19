/** @file The values a host sends and receives through the tools and the inbox event, the address grammar, and the failures a send and a read answer with. */

import { Data, Effect, Schema } from "effect";
import {
  Message,
  MsgId,
  type Page,
  wellFormedString,
} from "../communication/index.js";
import { exactStruct } from "../encoding.js";
import { LocalGuidance } from "../firewall/index.js";
import { AGENT_NAME_RULE, type AgentCard, AgentName } from "../identity.js";

/** The scheme of an address naming one agent. */
const AGENT_ADDRESS_PREFIX = "agent:";

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

/** What a send and a history read both say of an address they cannot use. */
const addressFailureText = {
  "unknown-agent": "an agent the address names is not a known agent",
} as const;

/**
 * What the agent is told for each reason a send fails with. A send with an
 * unknown outcome may already have happened, so a new send can duplicate it.
 */
const sendFailureText = {
  ...addressFailureText,
  "membership-invalid": "the address names only you",
  "content-invalid": "the message content is invalid",
  "message-conflict":
    "the message id already belongs to different content or recipients",
  "network-unavailable": "Social Harness is unavailable (network unavailable)",
  "outcome-unknown":
    "the send did not complete; the message may or may not have been sent",
} as const;

/**
 * Check `reason` before retrying. A send with `outcome-unknown` may already
 * have happened, so a new send can duplicate it. The host may show `message`
 * to its agent.
 */
export class SendError extends Data.TaggedError("SendError")<{
  readonly reason: keyof typeof sendFailureText;
  readonly detail?: string;
}> {
  override get message(): string {
    return `send failed: ${this.detail ?? sendFailureText[this.reason]}`;
  }
}

/** The agent `to` names. An address that names the local agent is refused. */
export function addressedAgent(
  to: AgentAddress,
  localAgentName: AgentName,
): Effect.Effect<AgentName, SendError> {
  const name = AgentName.make(to.slice(AGENT_ADDRESS_PREFIX.length));
  return name === localAgentName
    ? Effect.fail(new SendError({ reason: "membership-invalid" }))
    : Effect.succeed(name);
}

/** The address, as the local agent states it, of its conversation with the other of `members`. */
export function addressOf(
  members: readonly AgentCard[],
  localAgentId: AgentCard["agentId"],
): AgentAddress {
  const other = members.find((member) => member.agentId !== localAgentId);
  return AgentAddress.make(`${AGENT_ADDRESS_PREFIX}${other?.agentName}`);
}

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

/** Host addresses are resolved by SocialHarness before communication preparation. */
export const SendRequest = exactStruct({
  to: Schema.propertySignature(AgentAddress).annotations({
    description: "The recipient: agent:<name>.",
  }),
  text: Schema.propertySignature(wellFormedString).annotations({
    description: "The message text.",
  }),
  replyToMsgId: Schema.optional(MsgId).annotations({
    description:
      "The ID of the message this replies to. Omit when starting a new norm interaction.",
  }),
}).annotations({ identifier: "SendRequest" });
/** @useDeclaredType */
export type SendRequest = typeof SendRequest.Type;

/**
 * What a send returns: the message id and the conversation's canonical
 * address. Accepted work continues independently of the caller's lifetime.
 */
export const SendResult = Schema.Struct({
  id: MsgId,
  address: AgentAddress,
}).annotations({ identifier: "SendResult" });
/** @useDeclaredType */
export type SendResult = typeof SendResult.Type;

const historyFailureText = {
  ...addressFailureText,
  "invalid-address": "the address names you, not another agent",
} as const;

/** History failure never exposes private protocol or persistence diagnostics. */
export class HistoryError extends Schema.TaggedError<HistoryError>()(
  "HistoryError",
  {
    reason: Schema.Literal("invalid-address", "unknown-agent"),
  },
) {
  override get message(): string {
    return `reading failed: ${historyFailureText[this.reason]}`;
  }
}

/**
 * One record of a conversation's history: a message this agent sent, or the
 * stored inbox entry of a message it received.
 */
export type HistoryRecord =
  | { readonly kind: "sent"; readonly message: Message }
  | ({ readonly kind: "received" } & InboxEntry);

/** Entries in read order, with the cursor that reads on from the last of them. */
export interface InboxPage {
  readonly entries: readonly InboxEntry[];
  readonly nextCursor: string;
}

/** Every conversation's address, in order. */
export interface Conversations {
  readonly addresses: readonly AgentAddress[];
}

/** Records of one conversation, oldest first. */
export interface HistoryRecordsPage extends Page {
  readonly records: readonly HistoryRecord[];
}
