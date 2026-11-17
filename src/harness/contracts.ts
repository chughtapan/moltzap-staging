/** @file The tool contract: the values a host sends and receives, the address grammar, and the MCP tools, Events and wire errors built from them. */

import { Effect, Option, Schema } from "effect";
import {
  AGENT_ADDRESS_PREFIX,
  AgentAddress,
  CollectiveOp,
  CollectiveResponse,
  DeadlineSeconds,
  decodeCollectiveFailure,
  GROUP_ADDRESS_PREFIX,
  GroupAddress,
  maximumMembers,
  Message,
  MessageAddressInput,
  MessageReference,
  MsgId,
  Page,
  PaginatedRequest,
  RequestedSchema,
  RequestError,
  SendError,
  sendFailureReasons,
  wellFormedString,
} from "../communication/index.js";
import {
  canonicalIdentifier,
  compareAscii,
  exactStruct,
  isStrictlyAscending,
} from "../encoding.js";
import {
  checkedGuidanceSchema,
  firewallDecisionsRequestSchema,
  firewallDecisionsSchema,
  FirewallPolicyError,
  FirewallRefusal,
  firewallRefusalReasonSchema,
  LocalGuidance,
  PermittedItem,
  policyReadSchema,
  policyRevisionSchema,
  policyUpdateSchema,
} from "../firewall/index.js";
import { type AgentCard, AgentId, AgentName } from "../identity.js";

/** Canonical host address of a direct or group conversation. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- The inbox owns this grammar shared by persistence and MCP schema composition.
export const MessageAddress = Schema.Union(AgentAddress, GroupAddress);
/** @useDeclaredType */
export type MessageAddress = typeof MessageAddress.Type;

/** A destination in canonical form, before its names are resolved. */
type CanonicalMessageAddress =
  | {
      readonly kind: "direct";
      readonly address: AgentAddress;
      readonly remoteName: AgentName;
    }
  | {
      readonly kind: "group";
      readonly address: GroupAddress;
      readonly memberNames: readonly AgentName[];
    };

/**
 * Canonicalizes a destination before its names are resolved. Groups include the local
 * agent, reject duplicate names, and sort names in unsigned ASCII order.
 * One peer becomes a direct address. Larger groups contain 3 to `maximumMembers`
 * agents. Invalid names and membership return `invalid-address` or
 * `membership-invalid`.
 */
export function canonicalMessageAddress(
  to: MessageAddressInput,
  localAgentName: AgentName,
): Effect.Effect<CanonicalMessageAddress, SendError> {
  return to.startsWith(AGENT_ADDRESS_PREFIX)
    ? canonicalDirect(to, localAgentName)
    : canonicalGroup(to, localAgentName);
}

/** The members of a canonical destination other than the local agent. */
export function otherMemberNames(
  destination: CanonicalMessageAddress,
  localAgentName: AgentName,
): readonly AgentName[] {
  return destination.kind === "direct"
    ? [destination.remoteName]
    : destination.memberNames.filter((name) => name !== localAgentName);
}

/** The canonical address, as the local agent states it, of the conversation among `members`. */
export function addressOf(
  members: readonly AgentCard[],
  localAgentId: AgentCard["agentId"],
): MessageAddress {
  const others = members.filter((member) => member.agentId !== localAgentId);
  return others.length === 1
    ? AgentAddress.make(`${AGENT_ADDRESS_PREFIX}${others[0]?.agentName}`)
    : GroupAddress.make(
        `${GROUP_ADDRESS_PREFIX}${members
          .map((member) => member.agentName)
          .sort(compareAscii)
          .join(",")}`,
      );
}

/** Identifies one inbox entry: what a host names to mark that entry delivered. */
export const DeliveryToken = canonicalIdentifier("DeliveryToken", "dlv_", 32);
/** @useDeclaredType */
export type DeliveryToken = typeof DeliveryToken.Type;

/** An inbox entry as the inbox stores it: everything but its delivery token. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- The inbox owns this grammar shared by persistence and MCP schema composition.
export const StoredInboxEntry = Schema.Struct({
  address: MessageAddress,
  item: PermittedItem,
  guidance: Schema.optionalWith(LocalGuidance, { exact: true }),
});
/** @useDeclaredType */
export type StoredInboxEntry = typeof StoredInboxEntry.Type;

/** One message or collective result as the host reads it. It keeps its delivery token once delivered. */
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
  "persistence-failed": "the inbox could not be read; try again",
} as const;

/** An inbox read rejects a cursor it did not return. */
export class InboxError extends Schema.TaggedError<InboxError>()("InboxError", {
  reason: Schema.Literal("invalid-cursor", "persistence-failed"),
}) {
  override get message(): string {
    return `receiving failed: ${inboxFailureText[this.reason]}`;
  }
}

/** Acknowledging a delivery moves a pending inbox entry to delivered and leaves its content as stored. */
export class DeliveryAcknowledgeError extends Schema.TaggedError<DeliveryAcknowledgeError>()(
  "DeliveryAcknowledgeError",
  {
    reason: Schema.Literal("unknown-delivery", "persistence-failed"),
  },
) {}

const collectiveRequest = exactStruct({
  op: CollectiveOp,
  deadline: DeadlineSeconds,
  requestedSchema: RequestedSchema,
});

const MessageSend = exactStruct({
  to: MessageAddressInput,
  text: wellFormedString,
});
const CollectiveRequestSend = exactStruct({
  to: MessageAddressInput,
  text: wellFormedString,
  collectiveRequest,
});
const CollectiveResponseSend = exactStruct({
  to: MessageAddressInput,
  collectiveResponse: CollectiveResponse,
});
/** Host addresses are resolved by SocialHarness before communication preparation. */
export const SendRequest = Schema.Union(
  MessageSend,
  CollectiveRequestSend,
  CollectiveResponseSend,
).annotations({ identifier: "SendRequest" });
/** A collective request's deadline includes policy assessment time.
 * @useDeclaredType
 */
export type SendRequest = typeof SendRequest.Type;

/**
 * One send written as a single object, which is the shape a tool's arguments
 * must have. Decoding yields the {@link SendRequest} the fields amount to and
 * refuses any other combination of them.
 */
const SendArguments = exactStruct({
  to: Schema.propertySignature(MessageAddressInput).annotations({
    description: "The recipient: agent:<name>, or group:<members>.",
  }),
  text: Schema.optionalWith(wellFormedString, { exact: true }).annotations({
    description:
      "The message text. Required unless collectiveResponse is given.",
  }),
  collectiveRequest: Schema.optionalWith(collectiveRequest, {
    exact: true,
  }).annotations({
    description:
      "With text: makes the message a collective request, and text its question. gather asks each agent the address names in a private message and returns one result to you. all_gather asks a group in one message and returns the same result to every member. deadline is seconds from now, and requestedSchema is the form each response fills in. The result arrives as a collectiveResult inbox entry whose requestId is the id this send returned.",
  }),
  collectiveResponse: Schema.optionalWith(CollectiveResponse, {
    exact: true,
  }).annotations({
    description:
      "In place of text: your one response to a collective request, which arrives as a collectiveRequest inbox entry. Send it to that entry's address. requestId is that entry's item.message.msgId. accept takes content that fits item.message.collectiveRequest.requestedSchema. decline takes none.",
  }),
}).pipe(Schema.compose(Schema.typeSchema(SendRequest), { strict: false }));

/** The arguments of a send when collective requests are turned off: text to a recipient. */
const TextSendArguments = exactStruct({
  to: Schema.propertySignature(MessageAddressInput).annotations({
    description: "The recipient: agent:<name>, or group:<members>.",
  }),
  text: Schema.propertySignature(wellFormedString).annotations({
    description: "The message text.",
  }),
});

/**
 * What a send returns: the message id and the conversation's canonical
 * address. For a gather to several recipients the id is that of the private
 * copy sent to the first recipient in agent-name order.
 */
const SendResultSchema = Schema.Struct({
  id: MsgId,
  address: MessageAddress,
}).annotations({ identifier: "SendResult" });

/** Accepted work continues independently of the caller's lifetime. */
export type SendResult = typeof SendResultSchema.Type;

/** The most inbox entries, history records or conversation addresses one page holds. */
export const PAGE_SIZE = 50;

/**
 * One record of a conversation's history: a message this agent sent, the
 * stored inbox entry of a message it received, or the stored inbox entry of a
 * collective result.
 */
export type HistoryRecord =
  | { readonly kind: "sent"; readonly message: Message }
  | ({
      readonly kind: "received";
      readonly message: {
        readonly authorAgentId: Message["author"]["agentId"];
        readonly msgId: Message["msgId"];
      };
    } & StoredInboxEntry)
  | ({
      readonly kind: "collectiveResult";
      readonly deliveryToken: DeliveryToken;
    } & StoredInboxEntry);

const historyFailureText = {
  "invalid-address": "the address is not a valid agent: or group: address",
  "unknown-agent": "an agent the address names is not a known agent",
  "invalid-cursor":
    "the cursor is not one this conversation returned; call again without a cursor",
  "persistence-failed": "the conversation could not be read; try again",
} as const;

/** History failure never exposes private protocol or persistence diagnostics. */
export class HistoryError extends Schema.TaggedError<HistoryError>()(
  "HistoryError",
  {
    reason: Schema.Literal(
      "invalid-address",
      "unknown-agent",
      "invalid-cursor",
      "persistence-failed",
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

/**
 * Refusals disclose the check, the deciding chain rule, registered feedback and
 * the rule's matcher detail, without assessment evidence.
 */
const firewallRefusalSchema = exactStruct({
  checkId: checkedGuidanceSchema.fields.checkId,
  refusal: firewallRefusalReasonSchema,
  rule: Schema.optionalWith(Schema.NonEmptyString, { exact: true }),
  feedback: Schema.Array(exactStruct({ text: Schema.NonEmptyString })),
  detail: Schema.optionalWith(Schema.Unknown, { exact: true }),
});

const savedItemSchema = Schema.typeSchema(PermittedItem);

const historyRecordSchema = Schema.Union(
  Schema.Struct({
    kind: Schema.Literal("sent"),
    message: Schema.typeSchema(Message),
  }),
  Schema.Struct({
    kind: Schema.Literal("received"),
    address: MessageAddress,
    message: MessageReference,
    item: savedItemSchema,
    guidance: Schema.optionalWith(LocalGuidance, { exact: true }),
  }),
  Schema.Struct({
    kind: Schema.Literal("collectiveResult"),
    address: MessageAddress,
    deliveryToken: DeliveryToken,
    item: savedItemSchema,
    guidance: Schema.optionalWith(LocalGuidance, { exact: true }),
  }),
);

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
  addresses: Schema.Array(MessageAddress).pipe(Schema.maxItems(50)),
  ...Page.fields,
}).pipe(Schema.filter((page) => addressesAreOrdered(page.addresses)));

const readConversationInput = exactStruct({
  address: MessageAddress,
  ...PaginatedRequest.fields,
});

const readConversationOutput = Schema.Struct({
  records: Schema.Array(historyRecordSchema).pipe(Schema.maxItems(50)),
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
/** What the daemon answers when a call's arguments are not what the tool takes. */
const invalidArgumentsSchema = Schema.Struct({
  issues: Schema.Array(
    Schema.Struct({
      path: Schema.Array(Schema.Union(Schema.String, Schema.Number)),
      message: Schema.String,
    }),
  ),
});

/**
 * Preserves typed send and collective failures without disclosing unrelated
 * requests. New fields are ignored. Missing firewall or collective fields cannot
 * decode as a plain send reason.
 */
const sendErrorDataSchema = Schema.Union(
  invalidArgumentsSchema,
  Schema.Struct({
    reason: Schema.Literal("firewall-refused"),
    ...firewallRefusalSchema.fields,
  }),
  Schema.Struct({
    reason: Schema.Literal("collective-failed"),
    requestId: Schema.optionalWith(MsgId, { exact: true }),
    failure: Schema.Unknown,
  }),
  Schema.Struct({
    reason: Schema.Literal(...sendFailureReasons),
    detail: Schema.optionalWith(Schema.String, { exact: true }),
  }),
);

/**
 * Preserves the typed refusal's details, the collective request's message id
 * and failing members or fields in JSON-RPC error data.
 */
function sendErrorData(
  error: SendError | RequestError | FirewallRefusal,
): typeof sendErrorDataSchema.Encoded {
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
  if (error._tag === "RequestError") {
    return {
      reason: "collective-failed",
      ...(error.requestId === undefined ? {} : { requestId: error.requestId }),
      failure: error.failure,
    };
  }
  if (error.detail === undefined) {
    return { reason: error.reason };
  }
  return { reason: error.reason, detail: error.detail };
}

/** Reads a refused send's JSON-RPC error data back as the typed failure, with its send and collective details. */
function sendErrorFromData(value: unknown) {
  return Effect.gen(function* () {
    const data = yield* Schema.decodeUnknown(sendErrorDataSchema)(value);
    if ("issues" in data) {
      return new SendError({
        reason: "content-invalid",
        detail: "the arguments are invalid",
        issues: data.issues,
      });
    }
    if ("refusal" in data) {
      return new FirewallRefusal(data);
    }
    if ("failure" in data) {
      return new RequestError({
        failure: yield* decodeCollectiveFailure(data.failure),
        ...(data.requestId === undefined ? {} : { requestId: data.requestId }),
      });
    }
    return new SendError(data);
  });
}

/**
 * How a tool call ended without a result, as its caller saw it. `refused`
 * carries the error data the daemon answered with. `unanswered` says whether
 * the request can have reached the daemon. `unreadable` is an answer that is
 * not the tool's result.
 */
export type ToolCallFailure =
  | { readonly kind: "refused"; readonly data: unknown }
  | { readonly kind: "unanswered"; readonly sent: boolean }
  | { readonly kind: "unreadable" };

/** The error data of a failure that tells its caller only a reason. */
const reasonData = <Reason>(error: { readonly reason: Reason }) => ({
  reason: error.reason,
});

/**
 * A tool whose refusals carry `data`: `toData` writes a typed failure as that
 * data, and `make` turns the data a caller receives back into the failure.
 */
const failsWith = <A, I, E>(
  data: Schema.Schema<A, I>,
  make: (data: A) => E,
  toData: (error: E) => I,
) =>
  ({
    readsEveryFailure: false,
    toData,
    toError: (failure: ToolCallFailure): Option.Option<E> =>
      failure.kind === "refused"
        ? Option.map(Schema.decodeUnknownOption(data)(failure.data), make)
        : Option.none(),
  }) as const;

/** A tool whose callers read no typed failure, whichever failure `toData` writes. */
const failsUntyped = <E>(toData: (error: E) => unknown) =>
  ({
    readsEveryFailure: false,
    toData,
    toError: (): Option.Option<never> => Option.none(),
  }) as const;

/**
 * A send reads every failure. One that may have reached the daemon without a
 * readable answer has an unknown outcome, so its caller must not send again;
 * the daemon answers the same for a send that ends in a way it does not declare.
 */
const sendFails = {
  readsEveryFailure: true,
  toData: sendErrorData,
  unexpected: new SendError({ reason: "outcome-unknown" }),
  toError: (
    failure: ToolCallFailure,
  ): Option.Option<SendError | RequestError | FirewallRefusal> => {
    if (failure.kind === "unanswered" && !failure.sent) {
      return Option.some(new SendError({ reason: "network-unavailable" }));
    }
    if (failure.kind !== "refused") {
      return Option.some(sendFails.unexpected);
    }
    return Option.some(
      Effect.runSync(
        sendErrorFromData(failure.data).pipe(
          Effect.orElseSucceed(() => sendFails.unexpected),
        ),
      ),
    );
  },
} as const;

const policyFailure = Schema.Struct({
  reason: FirewallPolicyError.fields.reason,
  ruleId: FirewallPolicyError.fields.ruleId,
  path: FirewallPolicyError.fields.path,
  problem: FirewallPolicyError.fields.problem,
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

/** Each held norm's name, description and protocol, and the document that states its acts. */
const readNormsOutput = exactStruct({
  norms: Schema.Array(
    exactStruct({
      name: Schema.String,
      description: Schema.String,
      protocol: Schema.optionalWith(Schema.String, { exact: true }),
      document: Schema.String,
    }),
  ),
});

const historyFails = failsWith(
  Schema.Struct({ reason: HistoryError.fields.reason }),
  (data) => new HistoryError(data),
  reasonData,
);
const policyFails = failsWith(
  policyFailure,
  (data) => new FirewallPolicyError(data),
  reasonData,
);

/**
 * Each tool's contract: its description, argument and result schemas, and how
 * its failures are written and read. Discovery, the MCP app and callers all
 * derive from it.
 */
export const toolDefinitions = {
  send_message: {
    failure: sendFails,
    description:
      "Send a message. With collectiveRequest the message is a collective request, which asks its recipients to respond in a form by a deadline; with collectiveResponse it is your response to one you received. Returns id, the message id, and address, the canonical conversation address. For a collective request, id is the requestId of the collectiveResult you will receive. A refusal names the argument or rule to correct: a collective-failed refusal carries failure.kind, one of schema-invalid, deadline-passed, request-none, request-responded, request-expired or response-invalid, and the requestId when the request is known. An outcome-unknown failure must not be retried as a new send. Use receive_messages to read what others send.",
    input: SendArguments,
    output: SendResultSchema,
    annotations: { readOnlyHint: false, openWorldHint: true },
  },
  receive_messages: {
    failure: failsWith(
      Schema.Struct({ reason: InboxError.fields.reason }),
      (data) => new InboxError(data),
      reasonData,
    ),
    description:
      "Receive pending inbox entries, each with its conversation address, its item and any guidance. item.kind is message for a message, collectiveRequest for a message that asks you to respond, or collectiveResult for the outcome of each recipient of a collective request. A collectiveRequest's item.message.collectiveRequest holds its op, question, requestedSchema and deadlineAt; respond with send_message, naming item.message.msgId as requestId. A collectiveResult's requestId is the id the request's send returned. Follow nextCursor until absent. Returned entries are marked delivered. Pass wait to wait up to that many seconds when nothing is pending; the call returns as soon as an entry arrives, or empty when the wait ends.",
    input: receiveMessagesInput,
    output: receiveMessagesOutput,
    annotations: {
      readOnlyHint: false,
      destructiveHint: false,
      openWorldHint: false,
    },
  },
  status: {
    failure: failsUntyped((error: never) => error),
    description:
      "Read this agent's identifier and name. subscription reports whether a callback is subscribed for inbox wake-ups, when the subscription must be refreshed, whether it is stalled and its last error. Other agents address this agent as agent:<agentName>.",
    input: noArguments,
    output: statusOutput,
    annotations: readOnlyAnnotations,
  },
  list_conversations: {
    failure: historyFails,
    description:
      "List canonical conversation addresses with completed messages or collective results. Follow nextCursor until absent. Use read_conversation to read one address.",
    input: listConversationsInput,
    output: listConversationsOutput,
    annotations: readOnlyAnnotations,
  },
  read_conversation: {
    failure: historyFails,
    description:
      "Read one conversation's records: the messages you sent, the inbox entries of the messages you received, and your collective results. Keep address unchanged while following nextCursor. The first page fixes the history boundary.",
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
    failure: failsWith(
      policyFailure,
      (data) => new FirewallPolicyError(data),
      (error) => ({ reason: error.reason, ...error.data }),
    ),
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
    failure: failsUntyped((error: FirewallPolicyError) => reasonData(error)),
    description:
      "List the norms this agent holds, each with its document: the norm's roles, when each act may be sent and the fields it takes. Send an act as (performative :ontology <norm> :field value ...) in the conversation it belongs to. Agree on a norm through norm-agreement before its first act; ask a member for a norm you lack through norm-discovery.",
    input: noArguments,
    output: readNormsOutput,
    annotations: readOnlyAnnotations,
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

/** How `send_message` reads for an agent that sends text only, with no collective requests or responses. */
export const textOnlyToolDefinitions = {
  send_message: {
    ...toolDefinitions.send_message,
    description:
      "Send a message. Returns the message id and canonical conversation address. An outcome-unknown failure must not be retried as a new send.",
    input: TextSendArguments,
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

function invalidAddress(): SendError {
  return new SendError({ reason: "invalid-address" });
}

function invalidMembership(detail: string): SendError {
  return new SendError({ reason: "membership-invalid", detail });
}

function decodeAgentName(name: string): Effect.Effect<AgentName, SendError> {
  return Schema.decodeUnknown(AgentName)(name).pipe(
    Effect.mapError(invalidAddress),
  );
}

function canonicalDirect(
  to: MessageAddressInput,
  localAgentName: AgentName,
): Effect.Effect<CanonicalMessageAddress, SendError> {
  return Effect.gen(function* () {
    const remoteName = yield* decodeAgentName(
      to.slice(AGENT_ADDRESS_PREFIX.length),
    );
    if (remoteName === localAgentName) {
      return yield* invalidMembership("the address names only you");
    }
    const address = yield* Schema.decodeUnknown(AgentAddress)(to).pipe(
      Effect.mapError(invalidAddress),
    );
    return { kind: "direct", address, remoteName };
  });
}

function canonicalGroup(
  to: MessageAddressInput,
  localAgentName: AgentName,
): Effect.Effect<CanonicalMessageAddress, SendError> {
  return Effect.gen(function* () {
    const explicitNames = yield* Effect.forEach(
      to.slice(GROUP_ADDRESS_PREFIX.length).split(","),
      decodeAgentName,
      { concurrency: 1 },
    );
    if (new Set(explicitNames).size !== explicitNames.length) {
      return yield* invalidMembership(
        "the address names an agent more than once",
      );
    }
    const memberNames = explicitNames.includes(localAgentName)
      ? explicitNames.slice()
      : [...explicitNames, localAgentName];
    const others = memberNames.filter((name) => name !== localAgentName);
    const [onlyOther] = others;
    if (onlyOther === undefined) {
      return yield* invalidMembership("the address names only you");
    }
    if (others.length === 1) {
      const address = yield* Schema.decodeUnknown(AgentAddress)(
        `${AGENT_ADDRESS_PREFIX}${onlyOther}`,
      ).pipe(Effect.mapError(invalidAddress));
      return { kind: "direct", address, remoteName: onlyOther };
    }
    if (memberNames.length > maximumMembers) {
      return yield* invalidMembership(
        `a group has at most ${String(maximumMembers)} members`,
      );
    }
    const names = [...memberNames].sort(compareAscii);
    const address = yield* Schema.decodeUnknown(GroupAddress)(
      `${GROUP_ADDRESS_PREFIX}${names.join(",")}`,
    ).pipe(Effect.mapError(invalidAddress));
    return { kind: "group", address, memberNames: names };
  });
}

function addressesAreOrdered(addresses: readonly string[]): boolean {
  return isStrictlyAscending(addresses, compareAscii);
}
