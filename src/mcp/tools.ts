/** @file The tool contract: the values a host sends and receives, the address grammar, and the MCP tools, Events and wire errors built from them. */

import { Data, Effect, Schema } from "effect";
import {
  maximumMembers,
  Message,
  MsgId,
  Page,
  PAGE_SIZE,
  PaginatedRequest,
  wellFormedString,
} from "../communication/index.js";
import {
  canonicalIdentifier,
  compareAscii,
  exactStruct,
  isStrictlyAscending,
} from "../encoding.js";
import {
  firewallDecisionsRequestSchema,
  firewallDecisionsSchema,
  type FirewallPolicyError,
  type FirewallRefusal,
  LocalGuidance,
  policyReadSchema,
  policyRevisionSchema,
  policyUpdateSchema,
} from "../firewall/index.js";
import {
  AGENT_NAME_RULE,
  type AgentCard,
  AgentId,
  AgentName,
} from "../identity.js";
import { GroupAddress, groupAddressOf, groupMemberNames } from "./groups.js";

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

/** Either address as a host may write it, with a group's names in any order. */
export const MessageAddressInput = Schema.String.pipe(
  Schema.filter(
    (value) =>
      parseAgentAddress(value) !== undefined ||
      groupMemberNames(value) !== undefined,
    {
      identifier: "MessageAddressInput",
      description: `agent:<name>, or group: then agent names separated by commas, with no spaces and no agent: prefix; a name is ${AGENT_NAME_RULE}`,
    },
  ),
  Schema.brand("MessageAddressInput"),
);
/**
 * A validated address as a host wrote it.
 * @useDeclaredType
 */
export type MessageAddressInput = typeof MessageAddressInput.Type;

/** The address of one agent. */
export const AgentAddress = MessageAddressInput.pipe(
  Schema.filter((value) => parseAgentAddress(value) !== undefined),
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
  "invalid-address": "the address is not a valid agent: or group: address",
  "unknown-agent": "an agent the address names is not a known agent",
} as const;

/**
 * What the agent is told for each reason a send fails with. A send with an
 * unknown outcome may already have happened, so a new send can duplicate it.
 */
const sendFailureText = {
  ...addressFailureText,
  "membership-invalid": "the address does not name a valid set of agents",
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

/** Canonical host address of a direct or group conversation. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- The inbox owns this grammar shared by persistence and MCP schema composition.
export const MessageAddress = Schema.Union(AgentAddress, GroupAddress);
/** @useDeclaredType */
export type MessageAddress = typeof MessageAddress.Type;

/** A destination's canonical address and the agents it names other than the local one. */
interface Destination {
  readonly address: MessageAddress;
  readonly others: readonly AgentName[];
}

/**
 * Canonicalizes a destination. A group includes the local agent and names no
 * agent twice, and a group of one other agent is that agent's address. An
 * address that names only the local agent is refused.
 */
export function canonicalMessageAddress(
  to: MessageAddressInput,
  localAgentName: AgentName,
): Effect.Effect<Destination, SendError> {
  const direct = (other: AgentName): Effect.Effect<Destination, SendError> =>
    other === localAgentName
      ? Effect.fail(
          new SendError({
            reason: "membership-invalid",
            detail: "the address names only you",
          }),
        )
      : Effect.succeed({
          address: AgentAddress.make(`${AGENT_ADDRESS_PREFIX}${other}`),
          others: [other],
        });
  const named = parseAgentAddress(to);
  if (named !== undefined) {
    return direct(named);
  }
  return Effect.flatMap(groupMembers(to, localAgentName), (members) => {
    const others = members.filter((name) => name !== localAgentName);
    const [other, ...rest] = others;
    return other !== undefined && rest.length === 0
      ? direct(other)
      : Effect.succeed({ address: groupAddressOf(members), others });
  });
}

/** The canonical address, as the local agent states it, of the conversation among `members`. */
export function addressOf(
  members: readonly AgentCard[],
  localAgentId: AgentCard["agentId"],
): MessageAddress {
  const others = members.filter((member) => member.agentId !== localAgentId);
  return others.length === 1
    ? AgentAddress.make(`${AGENT_ADDRESS_PREFIX}${others[0]?.agentName}`)
    : groupAddressOf(members.map((member) => member.agentName));
}

/** Identifies one inbox entry: what a host names to mark that entry delivered. */
export const DeliveryToken = canonicalIdentifier("DeliveryToken", "dlv_", 32);
/** @useDeclaredType */
export type DeliveryToken = typeof DeliveryToken.Type;

/** An inbox entry as the inbox stores it: everything but its delivery token. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- The inbox owns this grammar shared by persistence and MCP schema composition.
export const StoredInboxEntry = Schema.Struct({
  address: MessageAddress,
  message: Message,
  guidance: Schema.optionalWith(LocalGuidance, { exact: true }),
});
/** @useDeclaredType */
export type StoredInboxEntry = typeof StoredInboxEntry.Type;

/** One message as the host reads it. It keeps its delivery token once delivered. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- The inbox owns this grammar shared by persistence and MCP schema composition.
export const InboxEntry = Schema.Struct({
  ...StoredInboxEntry.fields,
  deliveryToken: DeliveryToken,
});
/** @useDeclaredType */
export type InboxEntry = typeof InboxEntry.Type;

/** Reading leaves entries pending. Absence of nextCursor ends the page traversal. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- The inbox owns this grammar shared by persistence and MCP schema composition.
export const InboxPage = Schema.Struct({
  ...Page.fields,
  entries: Schema.Array(InboxEntry),
});
/** @useDeclaredType */
export type InboxPage = typeof InboxPage.Type;

const inboxFailureText = {
  "invalid-cursor":
    "the cursor is not one this inbox returned; call again without a cursor",
} as const;

/** An inbox read rejects a cursor it did not return. */
export class InboxError extends Schema.TaggedError<InboxError>()("InboxError", {
  reason: Schema.Literal("invalid-cursor"),
}) {
  override get message(): string {
    return `receiving failed: ${inboxFailureText[this.reason]}`;
  }
}

/** Host addresses are resolved by SocialHarness before communication preparation. */
export const SendRequest = exactStruct({
  to: Schema.propertySignature(MessageAddressInput).annotations({
    description: "The recipient: agent:<name>, or group:<members>.",
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

/** What a send returns: the message id and the conversation's canonical address. */
const SendResultSchema = Schema.Struct({
  id: MsgId,
  address: MessageAddress,
}).annotations({ identifier: "SendResult" });

/** Accepted work continues independently of the caller's lifetime. */
export type SendResult = typeof SendResultSchema.Type;

const historyFailureText = {
  ...addressFailureText,
  "invalid-cursor":
    "the cursor is not one this conversation returned; call again without a cursor",
} as const;

/** History failure never exposes private protocol or persistence diagnostics. */
export class HistoryError extends Schema.TaggedError<HistoryError>()(
  "HistoryError",
  {
    reason: Schema.Literal(
      "invalid-address",
      "unknown-agent",
      "invalid-cursor",
    ),
  },
) {
  override get message(): string {
    return `reading failed: ${historyFailureText[this.reason]}`;
  }
}

/** Callback failures expose no upstream response or destination details. */
export const callbackReasons = [
  "connection_refused",
  "timeout",
  "tls_error",
  "http_4xx",
  "http_5xx",
  "challenge_failed",
] as const;
/** Owner-visible diagnostics omit callback addresses and signing authority. */
const subscriptionStatusSchema = Schema.Struct({
  mode: Schema.Literal("none", "webhook"),
  id: Schema.optional(Schema.String),
  refreshBefore: Schema.optional(Schema.String),
  stalled: Schema.optional(
    Schema.NullOr(Schema.Literal("callback", "terminal")),
  ),
  lastError: Schema.optional(Schema.NullOr(Schema.Literal(...callbackReasons))),
});

/** A subscription as the `status` tool reports it, without its callback URL or secret. */
export type SubscriptionStatus = typeof subscriptionStatusSchema.Type;

/** A wake-up to read the pending inbox. It carries no content. */
export const INBOX_PENDING_EVENT = "org.social-harness/inbox.pending";

const historyRecordSchema = Schema.Union(
  Schema.Struct({
    kind: Schema.Literal("sent"),
    message: Schema.typeSchema(Message),
  }),
  Schema.Struct({
    kind: Schema.Literal("received"),
    address: MessageAddress,
    message: Schema.typeSchema(Message),
    guidance: Schema.optionalWith(LocalGuidance, { exact: true }),
  }),
);

/**
 * One record of a conversation's history: a message this agent sent, or the
 * stored inbox entry of a message it received.
 */
export type HistoryRecord = typeof historyRecordSchema.Type;

/** This agent's identifier and name, readable while communication is still starting. */
const statusOutput = Schema.Struct({
  agentId: AgentId,
  agentName: AgentName,
  subscription: subscriptionStatusSchema,
});

/** Canonically ordered local-conversation page selector. */
const listConversationsInput = exactStruct({
  ...PaginatedRequest.fields,
});

/** At most fifty canonical addresses and an opaque position for the next page. */
const listConversationsOutput = Schema.Struct({
  addresses: Schema.Array(MessageAddress).pipe(Schema.maxItems(PAGE_SIZE)),
  ...Page.fields,
}).pipe(Schema.filter((page) => addressesAreOrdered(page.addresses)));

const readConversationInput = exactStruct({
  address: MessageAddress,
  ...PaginatedRequest.fields,
});

const readConversationOutput = Schema.Struct({
  records: Schema.Array(historyRecordSchema).pipe(Schema.maxItems(PAGE_SIZE)),
  ...Page.fields,
});

/**
 * The longest a receive_messages call may wait. It stays under the time after
 * which hosts and proxies give up on a tool call.
 */
export const MAXIMUM_WAIT_SECONDS = 50;

const receiveMessagesInput = exactStruct({
  ...PaginatedRequest.fields,
  wait: Schema.optional(
    Schema.Number.pipe(
      Schema.positive(),
      Schema.lessThanOrEqualTo(MAXIMUM_WAIT_SECONDS),
    ).annotations({
      description: `a number of seconds, at most ${String(MAXIMUM_WAIT_SECONDS)}, to wait for a message when none is pending. Call receive_messages again to keep waiting. Without wait the call returns at once.`,
    }),
  ),
});
const receiveMessagesOutput = Schema.typeSchema(InboxPage);
/** Writes the typed refusal's details as the structured content of a failed send. */
function sendErrorData(error: SendError | FirewallRefusal) {
  if (error._tag === "FirewallRefusal") {
    return {
      reason: "firewall-refused",
      checkId: error.checkId,
      refusal: error.refusal,
      ...(error.rule === undefined ? {} : { rule: error.rule }),
      feedback: error.feedback,
      ...(error.detail === undefined ? {} : { detail: error.detail }),
    };
  }
  if (error.detail === undefined) {
    return { reason: error.reason };
  }
  return { reason: error.reason, detail: error.detail };
}

/** The structured content of a failure that tells its caller only a reason. */
const reasonData = <Reason>(error: { readonly reason: Reason }) => ({
  reason: error.reason,
});

const noArguments = Schema.Record({
  key: Schema.String,
  value: Schema.Never,
}).annotations({
  jsonSchema: { type: "object", properties: {}, additionalProperties: false },
});
const readOnlyAnnotations = {
  readOnlyHint: true,
  openWorldHint: false,
} as const;

/** Each held norm's name and description, and the document that states its acts. */
const readNormsOutput = exactStruct({
  norms: Schema.Array(
    exactStruct({
      name: Schema.String,
      description: Schema.String,
      document: Schema.String,
    }),
  ),
});

const historyFails = { toData: (error: HistoryError) => reasonData(error) };
const policyFails = {
  toData: (error: FirewallPolicyError) => reasonData(error),
};

/**
 * Each tool's contract: its description, its argument and result schemas, and
 * the structured content a failed call's result carries. `unexpected` is the
 * failure a call answers when it ends in a way the tool does not declare; a
 * send's is `outcome-unknown`, because the message may have been sent.
 * Discovery, the MCP handler and callers all derive from it.
 */
export const toolDefinitions = {
  send_message: {
    failure: {
      toData: sendErrorData,
      unexpected: new SendError({ reason: "outcome-unknown" }),
    },
    description:
      "Send a message. Returns id, the message id, and address, the canonical conversation address. A refusal names the argument or rule to correct. An outcome-unknown failure must not be retried as a new send. Use receive_messages to read what others send.",
    input: SendRequest,
    output: SendResultSchema,
    annotations: { readOnlyHint: false, openWorldHint: true },
  },
  receive_messages: {
    failure: { toData: (error: InboxError) => reasonData(error) },
    description:
      "Receive pending inbox entries, each with its conversation address, its message and any guidance. Follow nextCursor until absent. Returned entries are marked delivered. Pass wait to wait up to that many seconds when nothing is pending; the call returns as soon as an entry arrives, or empty when the wait ends.",
    input: receiveMessagesInput,
    output: receiveMessagesOutput,
    annotations: {
      readOnlyHint: false,
      destructiveHint: false,
      openWorldHint: false,
    },
  },
  status: {
    failure: { toData: (error: never) => error },
    description:
      "Read this agent's identifier and name. subscription reports whether a callback is subscribed for inbox wake-ups, when the subscription must be refreshed, whether it is stalled and its last error. Other agents address this agent as agent:<agentName>.",
    input: noArguments,
    output: statusOutput,
    annotations: readOnlyAnnotations,
  },
  list_conversations: {
    failure: historyFails,
    description:
      "List canonical conversation addresses with completed messages. Follow nextCursor until absent. Use read_conversation to read one address.",
    input: listConversationsInput,
    output: listConversationsOutput,
    annotations: readOnlyAnnotations,
  },
  read_conversation: {
    failure: historyFails,
    description:
      "Read one conversation's records: the messages you sent and the inbox entries of the messages you received. Keep address unchanged while following nextCursor. The first page fixes the history boundary.",
    input: readConversationInput,
    output: readConversationOutput,
    annotations: readOnlyAnnotations,
  },
  read_policy: {
    failure: policyFails,
    description:
      "Read active editable rules, fixed rules, the names of the norms this agent holds, and the policy revision. Fixed rules include the English rules of held norms.",
    input: noArguments,
    output: policyReadSchema,
    annotations: readOnlyAnnotations,
  },
  update_policy: {
    failure: {
      toData: (error: FirewallPolicyError) => ({
        reason: error.reason,
        ...error.data,
      }),
    },
    description:
      "Add, remove or replace editable rules and held norms. A behavior is a complete BEHAVIOR.md document, and {norm} adds a complete norm document; remove a norm with {norm: name}. Remove and add the same id or norm to replace it. Fixed rules refuse edits. Returns after the complete edit is active; saved decisions remain unchanged.",
    input: policyUpdateSchema,
    output: policyRevisionSchema,
    annotations: { readOnlyHint: false, openWorldHint: false },
  },
  read_firewall_decisions: {
    failure: policyFails,
    description:
      "Read completed firewall decision summaries since inclusive Unix milliseconds. Each gives the check's judgment (allow, reject or hold), the decision applied (allowed, refused or withheld), the reason and the deciding rules' registered advice, without message content or private model evidence.",
    input: firewallDecisionsRequestSchema,
    output: firewallDecisionsSchema,
    annotations: readOnlyAnnotations,
  },
  read_norms: {
    failure: policyFails,
    description:
      "List the installed communication norms and their complete documents. Participants already share these norms. Start with an opening act from a document; for a continuation, supply replyToMsgId in send_message.",
    input: noArguments,
    output: readNormsOutput,
    annotations: readOnlyAnnotations,
  },
} as const;

/** The address of one agent, on a daemon that offers no groups. */
const oneAgent = AgentAddress.annotations({
  description: "agent:<name>",
  message: () =>
    "this daemon does not offer groups; address one agent as agent:<name>",
});

/**
 * How the two tools that take an address read on a daemon that offers no
 * groups: the address names one agent.
 */
export const directToolDefinitions = {
  send_message: {
    ...toolDefinitions.send_message,
    input: exactStruct({
      ...SendRequest.fields,
      to: Schema.propertySignature(oneAgent).annotations({
        description: "The recipient: agent:<name>.",
      }),
    }),
  },
  read_conversation: {
    ...toolDefinitions.read_conversation,
    input: exactStruct({ ...readConversationInput.fields, address: oneAgent }),
  },
} as const;

/**
 * How three tools read for a client in client/server mode. A client talks only
 * to the server agent, so it names no recipient and no conversation.
 */
export const clientToolDefinitions = {
  send_message: {
    ...toolDefinitions.send_message,
    description:
      "Send a message to the server agent. Returns the message id. An outcome-unknown failure must not be retried as a new send.",
    input: exactStruct({
      text: Schema.propertySignature(wellFormedString).annotations({
        description: "The message text.",
      }),
      replyToMsgId: Schema.optional(MsgId),
    }),
  },
  read_conversation: {
    ...toolDefinitions.read_conversation,
    description:
      "Read the messages you sent and received in your conversation with the server agent. Follow nextCursor until absent. The first page fixes the history boundary.",
    input: exactStruct({ ...PaginatedRequest.fields }),
  },
  read_norms: {
    ...toolDefinitions.read_norms,
    description:
      "List the norms the server agent holds, each with its document: the norm's roles, when each act may be sent and the fields it takes. The server agent checks your messages against them. Send an act as (performative :ontology <norm> :field value ...).",
  },
} as const;

/** Names the tools published by the daemon's MCP catalog. */
export type ToolName = keyof typeof toolDefinitions;

/** Arguments are derived from the daemon's owning Effect schema. */
export type ToolRequest<Name extends ToolName = ToolName> = {
  readonly [Tool in Name]: {
    readonly name: Tool;
    readonly arguments: Schema.Schema.Type<
      (typeof toolDefinitions)[Tool]["input"]
    >;
  };
}[Name];

/** Results are derived from the schema the daemon publishes for this tool. */
export type ToolResult<Name extends ToolName> = Schema.Schema.Type<
  (typeof toolDefinitions)[Name]["output"]
>;

function addressesAreOrdered(addresses: readonly string[]): boolean {
  return isStrictlyAscending(addresses, compareAscii);
}

/**
 * The members a `group:` destination names, with the local agent among them.
 * A name given twice, no agent but the local one, and more than
 * `maximumMembers` members are refused.
 */
function groupMembers(
  to: string,
  localAgentName: AgentName,
): Effect.Effect<readonly AgentName[], SendError> {
  const names = groupMemberNames(to);
  if (names === undefined) {
    return Effect.fail(new SendError({ reason: "invalid-address" }));
  }
  const refused = (detail: string) =>
    Effect.fail(new SendError({ reason: "membership-invalid", detail }));
  if (new Set(names).size !== names.length) {
    return refused("the address names an agent more than once");
  }
  const members = names.includes(localAgentName)
    ? names
    : [...names, localAgentName];
  if (members.length === 1) {
    return refused("the address names only you");
  }
  return members.length > maximumMembers
    ? refused(`a group has at most ${String(maximumMembers)} members`)
    : Effect.succeed(members);
}
