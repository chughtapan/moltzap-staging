/** @file Composes the loopback MCP catalog, authenticated handler and listener. */

import {
  createMcpHandler,
  type Implementation,
  type McpHttpHandler,
  McpServer,
  ProtocolError,
  ProtocolErrorCode,
  type StandardSchemaV1,
  type StandardSchemaWithJSON,
  type ToolAnnotations,
} from "@modelcontextprotocol/server";
import {
  Cause,
  type Context,
  Effect,
  Either,
  Exit,
  Layer,
  Option,
  type ParseResult,
  Redacted,
  type Runtime,
  Schema,
  Scope,
} from "effect";
import { createHash, timingSafeEqual } from "node:crypto";
import type { CollectiveError } from "../../communication/collectives/contracts.js";
import type { FirewallSendError } from "../../host-protocol/advice.js";
import type { EventStore } from "../inbox/store.js";
import {
  issuesText,
  type SendError,
} from "../../communication/conversations/errors.js";
import {
  firewallDecisionsRequestSchema,
  firewallDecisionsSchema,
  FirewallPolicyError,
  policyReadSchema,
  policyUpdateResultSchema,
  policyUpdateSchema,
} from "../../firewall/config.js";
import {
  requestSchema,
  type ResultSchema,
  resultSchema,
} from "../../host-protocol/catalog-schemas.js";
import {
  decodeHarnessSendCall,
  type HarnessEmptyResult,
  harnessEmptyResultSchema,
  harnessFailureReasons,
  type ManagementReadConversationRequest,
  managementReadConversationRequestSchema,
  type ManagementReadConversationResult,
  managementReadConversationResultSchema,
  type ManagementRegisterRequest,
  managementRegisterRequestSchema,
  type ManagementRegisterResult,
  managementRegisterResultSchema,
  type ManagementSearchConversationsRequest,
  managementSearchConversationsRequestSchema,
  type ManagementSearchConversationsResult,
  managementSearchConversationsResultSchema,
  type ManagementStatusResult,
  managementStatusResultSchema,
  registrationAdmissionRefusalMessage,
  validationIssues,
  webhookStatusResult,
} from "../../host-protocol/index.js";
import {
  HARNESS_ACKNOWLEDGE_DELIVERY_TOOL,
  HARNESS_READ_EVENT_TOOL,
  HARNESS_READ_INBOX_TOOL,
  HARNESS_SEND_TOOL,
} from "../../host-protocol/names.js";
import {
  checkHarnessSendResult,
  type DeliveryOperations,
  harnessAcknowledgeDeliveryRequestSchema,
  harnessSendArgumentsSchema,
  type HarnessSendErrorData,
  type HarnessSendResult,
  inboxRequestSchema,
  inboxResultSchema,
  readEventRequestSchema,
  readEventResultSchema,
  sendErrorData,
} from "../../host-protocol/operations.js";
import { SendResultSchema } from "../../host-protocol/send-result.js";
import { Identity } from "../../identity/local.js";
import { type ConsumerOwner, makeConsumerOwner } from "./consumer.js";
import {
  type HarnessEvents,
  makeHarnessEvents,
  makeHarnessPollEvents,
  makeWebhookEvents,
  runEventOperation,
  runRequest,
  webhookHttpClientLayer,
} from "./events.js";

/* eslint-disable agent-code-guard/async-keyword -- Official MCP factories and callbacks are Promise-native. */

const credentialDigest = (value: string): Uint8Array =>
  createHash("sha256").update(value).digest();

/** Every HTTP request authenticates before discovery, SDK parsing or event dispatch. */
const authenticateHarnessRequest = (
  request: Request,
  credential: Redacted.Redacted,
): boolean => {
  const authorization = request.headers.get("authorization");
  return (
    authorization !== null &&
    authorization.startsWith("Bearer ") &&
    timingSafeEqual(
      credentialDigest(authorization.slice("Bearer ".length)),
      credentialDigest(Redacted.value(credential)),
    )
  );
};

const REGISTER_TOOL = "register";
const STATUS_TOOL = "status";
const SEARCH_CONVERSATIONS_TOOL = "search_conversations";
const READ_CONVERSATION_TOOL = "read_conversation";

type ClosedOperationError = Readonly<{ readonly reason: string }>;

/** Structural daemon operations projected onto the loopback MCP boundary. */
export interface HarnessMcpOperations extends DeliveryOperations {
  readonly readPolicy?: () => Effect.Effect<
    typeof policyReadSchema.Type,
    FirewallPolicyError
  >;
  readonly updatePolicy?: (
    request: typeof policyUpdateSchema.Type,
  ) => Effect.Effect<typeof policyUpdateResultSchema.Type, FirewallPolicyError>;
  readonly readDecisions?: (
    request: typeof firewallDecisionsRequestSchema.Type,
  ) => Effect.Effect<typeof firewallDecisionsSchema.Type, FirewallPolicyError>;
  readonly readStatus: () => Effect.Effect<
    ManagementStatusResult,
    ClosedOperationError
  >;
  readonly register: (
    input: ManagementRegisterRequest,
  ) => Effect.Effect<ManagementRegisterResult, ClosedOperationError>;
  readonly searchConversations: (
    input: ManagementSearchConversationsRequest,
  ) => Effect.Effect<ManagementSearchConversationsResult, ClosedOperationError>;
  readonly readConversation: (
    input: ManagementReadConversationRequest,
  ) => Effect.Effect<ManagementReadConversationResult, ClosedOperationError>;
}

/** Official handler with one content-free daemon notification edge. */
export interface HarnessMcpEventHandler extends McpHttpHandler {
  readonly notifyPending: () => boolean;
}

interface HarnessMcpHandlerOptions {
  readonly eventStore: EventStore;
  readonly credential: Redacted.Redacted;
  readonly implementation: Implementation;
  readonly operations: HarnessMcpOperations;
  readonly onerror?: (error: Error) => void;
}

interface RunOperationOptions<Value extends Readonly<Record<string, unknown>>> {
  readonly operation: Effect.Effect<
    Value,
    ClosedOperationError | ProtocolError
  >;
  readonly label: string;
  readonly allowedReasons: ReadonlySet<string>;
  readonly fallbackReason: string;
  /** The cause and the error data fields a failure names beyond its reason, when it has them. */
  readonly describe?: (
    failure: unknown,
  ) => { message: string; data: object } | undefined;
  readonly signal: AbortSignal;
  readonly requestScope: Scope.CloseableScope;
  readonly services: Runtime.Runtime<never>;
}

const emptyRequestSchema = Schema.Record({
  key: Schema.String,
  value: Schema.Never,
});
const registerInput = requestSchema(managementRegisterRequestSchema);
const registerOutput = resultSchema(managementRegisterResultSchema);
const emptyInput = requestSchema(emptyRequestSchema);
const statusOutput = resultSchema(managementStatusResultSchema);
const policyReadOutput = resultSchema(policyReadSchema);
const policyUpdateInput = requestSchema(policyUpdateSchema);
const policyUpdateOutput = resultSchema(policyUpdateResultSchema);
const decisionsInput = requestSchema(firewallDecisionsRequestSchema);
const decisionsOutput = resultSchema(firewallDecisionsSchema);
const searchConversationsInput = requestSchema(
  managementSearchConversationsRequestSchema,
);
const searchConversationsOutput = resultSchema(
  managementSearchConversationsResultSchema,
);
const readConversationInput = requestSchema(
  managementReadConversationRequestSchema,
);
const readConversationOutput = resultSchema(
  managementReadConversationResultSchema,
);
const sendInput = requestSchema(harnessSendArgumentsSchema);
const acknowledgeDeliveryInput = requestSchema(
  harnessAcknowledgeDeliveryRequestSchema,
);
const emptyOutput = resultSchema(harnessEmptyResultSchema);
const sendOutput = resultSchema(SendResultSchema);
const readEventInput = requestSchema(readEventRequestSchema);
const readEventOutput = resultSchema(readEventResultSchema);
const readInboxInput = requestSchema(inboxRequestSchema);
const readInboxOutput = resultSchema(inboxResultSchema);
const {
  RUNTIME_READ_REASONS,
  REGISTER_REASONS,
  STATUS_REASONS,
  SEARCH_CONVERSATIONS_REASONS,
  READ_CONVERSATION_REASONS,
  ACKNOWLEDGE_DELIVERY_REASONS,
} = harnessFailureReasons;

const operationReason = (
  cause: unknown,
  allowed: ReadonlySet<string>,
  fallback: string,
): string => {
  if (typeof cause !== "object" || cause === null || !("reason" in cause)) {
    return fallback;
  }
  const reason: unknown = cause.reason;
  return typeof reason === "string" && allowed.has(reason) ? reason : fallback;
};

const toolResult = <Value extends Readonly<Record<string, unknown>>>(
  structuredContent: Value,
) => ({
  content: [{ type: "text" as const, text: JSON.stringify(structuredContent) }],
  structuredContent,
});

/**
 * JSON Schema decides structural admission; Effect supplies field diagnostics
 * only for rejected inputs, leaving semantic refusals with their operations.
 */
const decodeToolInput = async <Value, A, I>(
  schema: StandardSchemaV1<unknown, Value>,
  diagnosticsSchema: Schema.Schema<A, I>,
  value: unknown,
  toolName: string,
) => {
  const decoded = await schema["~standard"].validate(value);
  if (!("value" in decoded)) {
    const diagnostics = Schema.decodeUnknownEither(diagnosticsSchema, {
      errors: "all",
    })(value);
    const issues = Either.match(diagnostics, {
      onLeft: validationIssues,
      onRight: () => [{ path: [], message: "Expected tool argument schema" }],
    });
    throw new ProtocolError(
      ProtocolErrorCode.InvalidParams,
      `Invalid arguments for tool ${toolName}: ${issuesText(issues)}`,
      {
        issues,
      },
    );
  }
  return decoded.value;
};

/** An expected failure's closed reason, and the cause and data it names when the tool describes it. */
const operationError = <Value extends Readonly<Record<string, unknown>>>(
  failure: unknown,
  options: RunOperationOptions<Value>,
) => {
  const reason = operationReason(
    failure,
    options.allowedReasons,
    options.fallbackReason,
  );
  const described =
    failure === undefined ? undefined : options.describe?.(failure);
  const message = [`${options.label} failed`, described?.message]
    .filter(Boolean)
    .join(": ");
  return new ProtocolError(
    ProtocolErrorCode.InternalError,
    reason === "admission-refused"
      ? registrationAdmissionRefusalMessage
      : message,
    { ...described?.data, reason },
  );
};

const runOperation = async <Value extends Readonly<Record<string, unknown>>>(
  options: RunOperationOptions<Value>,
) => {
  const outcome = await runRequest(
    options.operation,
    options.signal,
    options.requestScope,
    options.services,
  );
  if (Exit.isFailure(outcome)) {
    const expectedFailure = Option.match(Cause.dieOption(outcome.cause), {
      onNone: () => Cause.failureOption(outcome.cause),
      onSome: () => Option.none(),
    });
    const failure = Option.getOrUndefined(expectedFailure);
    if (ProtocolError.isInstance(failure)) {
      throw failure;
    }
    throw operationError(failure, options);
  }
  return toolResult(outcome.value);
};

/**
 * Run one send and answer its typed failure as JSON-RPC error data, with the
 * typed error's own message, which names its cause for a host to hand its
 * model. A send that ends without one, interrupted or by a defect, may
 * already have queued its message, so it answers `outcome-unknown`.
 * @param operation A multicast, collective send or collective response.
 * @param signal The request's abort signal, which interrupts the send.
 * @param requestScope The handler lifetime that joins request finalizers before closing.
 * @param services The trusted daemon services captured when the handler is acquired.
 * @returns The send's tool result.
 */
const runSendOperation = async (
  operation: Effect.Effect<
    HarnessSendResult,
    SendError | CollectiveError | FirewallSendError
  >,
  signal: AbortSignal,
  requestScope: Scope.CloseableScope,
  services: Runtime.Runtime<never>,
) => {
  const outcome = await runRequest(operation, signal, requestScope, services);
  if (Exit.isFailure(outcome)) {
    const failure = Cause.failureOption(outcome.cause);
    throw new ProtocolError(
      ProtocolErrorCode.InternalError,
      Option.match(failure, {
        onNone: () => "Operation send failed",
        onSome: (error) => error.message,
      }),
      Option.match(failure, {
        onNone: (): HarnessSendErrorData => ({ reason: "outcome-unknown" }),
        onSome: sendErrorData,
      }),
    );
  }
  return toolResult(outcome.value);
};

const validateToolOutput = async <
  Value,
  Result extends Readonly<{ structuredContent: Value }>,
>(
  schema: StandardSchemaV1<unknown, Value>,
  result: Result,
  toolName: string,
) => {
  const decoded = await schema["~standard"].validate(result.structuredContent);
  if (!("value" in decoded)) {
    throw new ProtocolError(
      ProtocolErrorCode.InvalidParams,
      `Invalid result from tool ${toolName}`,
    );
  }
  return result;
};

const runValidatedOperation = async <
  Value extends Readonly<Record<string, unknown>>,
>(
  schema: StandardSchemaV1<unknown, Value>,
  toolName: string,
  options: RunOperationOptions<Value>,
) => await validateToolOutput(schema, await runOperation(options), toolName);

/**
 * The MCP annotations a host reads to decide which calls need its user's
 * approval. MCP defaults an unset `destructiveHint` to true and
 * `openWorldHint` to true, so a write that changes nothing it cannot undo, and
 * a tool that stays on this host, say so.
 */
type ToolHints = ToolAnnotations &
  Required<Pick<ToolAnnotations, "readOnlyHint" | "openWorldHint">>;

/** A tool's catalog entry: its description, schemas and annotations. */
interface ToolListing {
  readonly description: string;
  readonly inputSchema: StandardSchemaWithJSON;
  readonly outputSchema?: ResultSchema;
  readonly annotations: ToolHints;
}

/** A tool that reads this daemon's own state. */
const localRead: ToolHints = { readOnlyHint: true, openWorldHint: false };

/**
 * Every tool the catalog can list. `installToolCallHandler` replaces the
 * SDK's tools/call dispatch, so `toolHandlers` serves every call and a listing
 * only names a tool, its schemas and its annotations.
 */
const toolListings = {
  policy_read: {
    description:
      "Read editable natural-language rules and fixed rules with the active revision.",
    inputSchema: emptyInput,
    outputSchema: policyReadOutput,
    annotations: localRead,
  },
  policy_update: {
    description:
      "Add, remove or replace natural-language rules. Add a whole rule or a shared norm's BEHAVIOR.md text; remove and add the same id to replace it. Returns activeRevision after the complete edit is active. Existing admissions remain unchanged. An invalid-rule error identifies invalid text, conflicting or missing ids, or a rule limit. Fixed rules reject edits with fixed-rule. A not-configured error means the owner must install a text policy first.",
    inputSchema: policyUpdateInput,
    outputSchema: policyUpdateOutput,
    annotations: { readOnlyHint: false, openWorldHint: false },
  },
  firewall_decisions: {
    description:
      "Read metadata for the latest 100 completed checks since inclusive Unix milliseconds: each outcome's cause and deciding rules with their registered advice; no message text. Groups have one row per peer.",
    inputSchema: decisionsInput,
    outputSchema: decisionsOutput,
    annotations: localRead,
  },
  [STATUS_TOOL]: {
    description:
      "Inspect whether this daemon is registered: unregistered, or active with its bound AgentCard and agent, the card's agentId, agentName and principalId.",
    inputSchema: emptyInput,
    outputSchema: statusOutput,
    annotations: localRead,
  },
  [REGISTER_TOOL]: {
    description:
      "Register this daemon's configured key under an immutable agent name for a principal, reusing operationId for retries. Returns registered with the AgentCard and agent (agentId, agentName, principalId), or name_taken, key_already_registered or idempotency_conflict. Fails with admission-refused when the Registry refuses this daemon's admission credential: supply this network's credential and register again.",
    inputSchema: registerInput,
    outputSchema: registerOutput,
    annotations: {
      readOnlyHint: false,
      destructiveHint: false,
      idempotentHint: true,
      openWorldHint: true,
    },
  },
  event_subscription_status: {
    description:
      "Inspect the inbox consumer and stalled delivery without exposing callback credentials.",
    inputSchema: emptyInput,
    outputSchema: webhookStatusResult,
    annotations: localRead,
  },
  revoke_event_subscription: {
    description:
      "Release the active inbox consumer while leaving pending inbox items unread.",
    inputSchema: emptyInput,
    outputSchema: emptyOutput,
    annotations: { readOnlyHint: false, openWorldHint: false },
  },
  resume_event_subscription: {
    description:
      "Resume transient callback retries after a recoverable delivery failure. A terminal rejection requires revocation and reconfiguration.",
    inputSchema: emptyInput,
    outputSchema: emptyOutput,
    annotations: {
      readOnlyHint: false,
      destructiveHint: false,
      openWorldHint: true,
    },
  },
  [SEARCH_CONVERSATIONS_TOOL]: {
    description:
      "List canonical addresses of local conversations in order, optionally after an exclusive address cursor.",
    inputSchema: searchConversationsInput,
    outputSchema: searchConversationsOutput,
    annotations: localRead,
  },
  [READ_CONVERSATION_TOOL]: {
    description:
      "Read a frozen page of retained conversation messages with their recorded visibility and guidance. For pending deliveries, use read_inbox.",
    inputSchema: readConversationInput,
    outputSchema: readConversationOutput,
    annotations: { readOnlyHint: true, openWorldHint: true },
  },
  [HARNESS_READ_INBOX_TOOL]: {
    description:
      "Read a page of pending classified inbox items without acknowledging delivery.",
    inputSchema: readInboxInput,
    outputSchema: readInboxOutput,
    annotations: localRead,
  },
  [HARNESS_SEND_TOOL]: {
    description:
      "Send a multicast, gather, all_gather, or collective response to an agent or fixed-member group. A gather or all_gather returns its operationId. A multicast or response returns its message's msgId and canonical conversation address. read_conversation records that msgId, and recipients see it on multicast items. A response also returns the answered request's operationId. Answer in the conversation where the request arrived and use the operationId from its collectiveRequest item. Every collective response requires that request's operationId. A firewall refusal returns the check id, reason, and next action without sending. Success means the daemon retained the work and will continue sending it across restart. Do not repeat a successful send.",
    inputSchema: sendInput,
    outputSchema: sendOutput,
    annotations: {
      readOnlyHint: false,
      destructiveHint: false,
      openWorldHint: true,
    },
  },
  [HARNESS_READ_EVENT_TOOL]: {
    description:
      "Read the original full content and guidance of a Social Harness event by eventId without acknowledging delivery. An inbox item's eventId is its deliveryToken and remains readable after acknowledgment.",
    inputSchema: readEventInput,
    outputSchema: readEventOutput,
    annotations: localRead,
  },
  [HARNESS_ACKNOWLEDGE_DELIVERY_TOOL]: {
    description:
      "Acknowledge a delivery token after the host has accepted its item under the host-specific handoff contract.",
    inputSchema: acknowledgeDeliveryInput,
    outputSchema: emptyOutput,
    annotations: {
      readOnlyHint: false,
      destructiveHint: false,
      idempotentHint: true,
      openWorldHint: false,
    },
  },
} as const satisfies Readonly<Record<string, ToolListing>>;

type ToolName = keyof typeof toolListings;

/** Recovery operations remain available before identity registration. */
const eventTools = [
  "event_subscription_status",
  "revoke_event_subscription",
  "resume_event_subscription",
] as const;

const inactiveTools = [STATUS_TOOL, ...eventTools, REGISTER_TOOL] as const;
const activeTools = [
  STATUS_TOOL,
  ...eventTools,
  "policy_read",
  "policy_update",
  "firewall_decisions",
  SEARCH_CONVERSATIONS_TOOL,
  READ_CONVERSATION_TOOL,
  HARNESS_READ_INBOX_TOOL,
  HARNESS_SEND_TOOL,
  HARNESS_READ_EVENT_TOOL,
  HARNESS_ACKNOWLEDGE_DELIVERY_TOOL,
] as const;

interface ToolCallInput {
  readonly name: string;
  readonly toolArguments: unknown;
  readonly metadata: unknown;
  readonly signal: AbortSignal;
  readonly requestScope: Scope.CloseableScope;
  readonly services: Runtime.Runtime<never>;
}

const toolNotFound = (name: string): never => {
  throw new ProtocolError(
    ProtocolErrorCode.InvalidParams,
    `Tool ${name} not found`,
  );
};

const requestRuntime = (input: ToolCallInput) => ({
  signal: input.signal,
  requestScope: input.requestScope,
  services: input.services,
});

const decodeInvocationInput = <A>(
  input: Effect.Effect<A, ParseResult.ParseError>,
  signal: AbortSignal,
  requestScope: Scope.CloseableScope,
  services: Runtime.Runtime<never>,
) =>
  runEventOperation(
    input.pipe(
      Effect.catchTag("ParseError", (error) => {
        const issues = validationIssues(error);
        return Effect.fail(
          new ProtocolError(
            -32602,
            `Invalid invocation arguments: ${issuesText(issues)}`,
            { reason: "content-invalid", issues },
          ),
        );
      }),
    ),
    signal,
    requestScope,
    services,
  );

/**
 * Run one send call. A send that returns a result outside its output schema
 * has already run, so that result is a defect, which `runSendOperation`
 * answers as `outcome-unknown`.
 * @param input The `send_message` arguments, metadata and abort signal.
 * @param operations The daemon operations that run the send.
 * @returns The send's validated tool result.
 */
const handleSendToolCall = async (
  input: ToolCallInput,
  operations: HarnessMcpOperations,
) => {
  const decoded = await decodeInvocationInput(
    decodeHarnessSendCall(input.toolArguments),
    input.signal,
    input.requestScope,
    input.services,
  );
  const send = operations
    .send(decoded)
    .pipe(
      Effect.flatMap((result) =>
        checkHarnessSendResult(result).pipe(Effect.orDie),
      ),
    );
  return await runSendOperation(
    send,
    input.signal,
    input.requestScope,
    input.services,
  );
};

const POLICY_REASONS = new Set([
  "not-configured",
  "invalid-rule",
  "fixed-rule",
  "build-failed",
  "persistence-failed",
]);

/** The operations a tool call runs: the daemon's and the event subscription's. */
interface ToolOperations extends HarnessMcpOperations {
  readonly identity: Context.Tag.Service<typeof Identity>;
  readonly events: HarnessEvents;
  readonly requestScope: Scope.CloseableScope;
  readonly services: Runtime.Runtime<never>;
}

const handleEventOperation = async <
  Value extends Readonly<Record<string, unknown>>,
>(
  operation: Effect.Effect<Value, ProtocolError>,
  outputSchema: StandardSchemaV1<unknown, Value>,
  input: ToolCallInput,
) => {
  await decodeToolInput(
    emptyInput,
    emptyRequestSchema,
    input.toolArguments,
    input.name,
  );
  return await validateToolOutput(
    outputSchema,
    toolResult(
      await runEventOperation(
        operation,
        input.signal,
        input.requestScope,
        input.services,
      ),
    ),
    input.name,
  );
};

/** Serves one admitted call of the tool it is keyed by in `toolHandlers`. */
type ToolHandler = (
  input: ToolCallInput,
  operations: ToolOperations,
  // eslint-disable-next-line agent-code-guard/promise-type -- MCP tool handlers are Promise callbacks of the official SDK.
) => Promise<ReturnType<typeof toolResult>>;

/** Couples each decoded request to its operation and validated result. */
const makeToolHandler =
  <
    Request,
    Diagnostics,
    Encoded,
    Value extends Readonly<Record<string, unknown>>,
  >(
    inputSchema: StandardSchemaV1<unknown, Request>,
    diagnosticsSchema: Schema.Schema<Diagnostics, Encoded>,
    outputSchema: StandardSchemaV1<unknown, Value>,
    options: Pick<
      RunOperationOptions<Value>,
      "label" | "allowedReasons" | "fallbackReason" | "describe"
    > & {
      readonly consumesInbox?: boolean;
      readonly execute: (
        operations: HarnessMcpOperations,
        request: Request,
      ) => Effect.Effect<Value, ClosedOperationError>;
    },
  ): ToolHandler =>
  async (input, operations) => {
    const request = await decodeToolInput(
      inputSchema,
      diagnosticsSchema,
      input.toolArguments,
      input.name,
    );
    return await runValidatedOperation(outputSchema, input.name, {
      ...options,
      operation: options.consumesInbox
        ? operations.events.nativeOperation(
            input.metadata,
            options.execute(operations, request),
          )
        : options.execute(operations, request),
      ...requestRuntime(input),
    });
  };

/**
 * The handler for each tool the catalog can list, keyed like `toolListings`,
 * so a listed tool without a handler does not compile.
 */
const toolHandlers = {
  policy_read: makeToolHandler(
    emptyInput,
    emptyRequestSchema,
    policyReadOutput,
    {
      execute: (operations) =>
        operations.readPolicy?.() ?? Effect.fail({ reason: "not-configured" }),
      label: "Policy read",
      allowedReasons: POLICY_REASONS,
      fallbackReason: "persistence-failed",
    },
  ),
  policy_update: makeToolHandler(
    policyUpdateInput,
    policyUpdateSchema,
    policyUpdateOutput,
    {
      execute: (operations, request) =>
        operations.updatePolicy?.(request) ??
        Effect.fail({ reason: "not-configured" }),
      label: "Policy update",
      allowedReasons: POLICY_REASONS,
      fallbackReason: "build-failed",
      describe: (failure) =>
        failure instanceof FirewallPolicyError
          ? { message: failure.message, data: failure.data }
          : undefined,
    },
  ),
  firewall_decisions: makeToolHandler(
    decisionsInput,
    firewallDecisionsRequestSchema,
    decisionsOutput,
    {
      execute: (operations, request) =>
        operations.readDecisions?.(request) ??
        Effect.fail({ reason: "not-configured" }),
      label: "Firewall decisions",
      allowedReasons: POLICY_REASONS,
      fallbackReason: "persistence-failed",
    },
  ),
  [STATUS_TOOL]: makeToolHandler(emptyInput, emptyRequestSchema, statusOutput, {
    execute: (operations) => operations.readStatus(),
    label: "Status",
    allowedReasons: STATUS_REASONS,
    fallbackReason: "incompatible-daemon",
  }),
  [REGISTER_TOOL]: makeToolHandler(
    registerInput,
    managementRegisterRequestSchema,
    registerOutput,
    {
      execute: (operations, request) => operations.register(request),
      label: "Registration",
      allowedReasons: REGISTER_REASONS,
      fallbackReason: "dependency-unavailable",
    },
  ),
  event_subscription_status: (input, operations) =>
    handleEventOperation(operations.events.status, webhookStatusResult, input),
  revoke_event_subscription: (input, operations) =>
    handleEventOperation(
      operations.events.revoke.pipe(Effect.as({})),
      emptyOutput,
      input,
    ),
  resume_event_subscription: (input, operations) =>
    handleEventOperation(
      operations.events.resume.pipe(Effect.as({})),
      emptyOutput,
      input,
    ),
  [SEARCH_CONVERSATIONS_TOOL]: makeToolHandler(
    searchConversationsInput,
    managementSearchConversationsRequestSchema,
    searchConversationsOutput,
    {
      execute: (operations, request) => operations.searchConversations(request),
      label: "Conversation search",
      allowedReasons: SEARCH_CONVERSATIONS_REASONS,
      fallbackReason: "persistence-failed",
    },
  ),
  [READ_CONVERSATION_TOOL]: makeToolHandler(
    readConversationInput,
    managementReadConversationRequestSchema,
    readConversationOutput,
    {
      execute: (operations, request) => operations.readConversation(request),
      label: "Conversation read",
      allowedReasons: READ_CONVERSATION_REASONS,
      fallbackReason: "persistence-failed",
    },
  ),
  [HARNESS_READ_INBOX_TOOL]: makeToolHandler(
    readInboxInput,
    inboxRequestSchema,
    readInboxOutput,
    {
      consumesInbox: true,
      execute: (operations, request) => operations.readInbox(request),
      label: "Inbox read",
      allowedReasons: RUNTIME_READ_REASONS,
      fallbackReason: "persistence-failed",
    },
  ),
  [HARNESS_SEND_TOOL]: handleSendToolCall,
  [HARNESS_READ_EVENT_TOOL]: makeToolHandler(
    readEventInput,
    readEventRequestSchema,
    readEventOutput,
    {
      execute: (operations, request) => operations.readEvent(request),
      label: "Event read",
      allowedReasons: RUNTIME_READ_REASONS,
      fallbackReason: "persistence-failed",
    },
  ),
  [HARNESS_ACKNOWLEDGE_DELIVERY_TOOL]: makeToolHandler(
    acknowledgeDeliveryInput,
    harnessAcknowledgeDeliveryRequestSchema,
    emptyOutput,
    {
      consumesInbox: true,
      execute: (operations, request) =>
        operations
          .acknowledgeDelivery(request.deliveryToken)
          .pipe(Effect.as<HarnessEmptyResult>({})),
      label: "Delivery acknowledgment",
      allowedReasons: ACKNOWLEDGE_DELIVERY_REASONS,
      fallbackReason: "transport-failed",
    },
  ),
} satisfies Readonly<Record<ToolName, ToolHandler>>;

/** Registration and installed operations determine one catalog for every client. */
const currentTools = (operations: ToolOperations) =>
  operations.identity.me().pipe(
    Effect.map((status) =>
      (status.kind === "registered" ? activeTools : inactiveTools).filter(
        (name) => {
          if (name === "policy_read") {
            return operations.readPolicy !== undefined;
          }
          if (name === "policy_update") {
            return operations.updatePolicy !== undefined;
          }
          if (name === "firewall_decisions") {
            return operations.readDecisions !== undefined;
          }
          return true;
        },
      ),
    ),
  );

const handleToolCall = async (
  input: ToolCallInput,
  operations: ToolOperations,
) => {
  const name = (await Effect.runPromise(currentTools(operations))).find(
    (tool) => tool === input.name,
  );
  if (name === undefined) {
    return toolNotFound(input.name);
  }
  return await toolHandlers[name](input, operations);
};

/** Schema failures and refused operations stay on the JSON-RPC error channel. */
const installToolCallHandler = (
  server: McpServer,
  operations: ToolOperations,
): void => {
  server.server.setRequestHandler("tools/call", (request, context) => {
    const input = {
      name: request.params.name,
      toolArguments: request.params.arguments ?? {},
      metadata: context.mcpReq._meta,
      signal: context.mcpReq.signal,
      requestScope: operations.requestScope,
      services: operations.services,
    };
    return handleToolCall(input, operations);
  });
};
const runtimeOperations = (
  operations: HarnessMcpOperations,
  events: HarnessEvents,
  requestScope: Scope.CloseableScope,
  captured: Pick<ToolOperations, "services" | "identity">,
): ToolOperations => ({
  ...operations,
  ...captured,
  events,
  requestScope,
  acknowledgeDelivery: (token) =>
    operations
      .acknowledgeDelivery(token)
      .pipe(Effect.tap(() => Effect.sync(() => events.notifyPending()))),
});
/** Each request receives the catalog for the daemon's current registration state. */
const makeServer = async (
  options: HarnessMcpHandlerOptions,
  operations: ToolOperations,
) => {
  const capabilities = { tools: {}, events: {} };
  const server = new McpServer(options.implementation, { capabilities });
  for (const name of await Effect.runPromise(currentTools(operations))) {
    const listing: ToolListing = toolListings[name];
    server.registerTool(name, listing, () => toolResult({}));
  }
  installToolCallHandler(server, operations);
  operations.events.install(server.server);
  return server;
};
const officialHandler = (
  options: HarnessMcpHandlerOptions,
  events: HarnessEvents,
  requestScope: Scope.CloseableScope,
  captured: Pick<ToolOperations, "services" | "identity">,
) =>
  createMcpHandler(
    () =>
      makeServer(
        options,
        runtimeOperations(options.operations, events, requestScope, captured),
      ),
    { responseMode: "auto", onerror: options.onerror },
  );

const guardedHandler = (
  options: HarnessMcpHandlerOptions,
  delegate: McpHttpHandler,
  events: HarnessEvents,
  scope: Scope.CloseableScope,
): HarnessMcpEventHandler => ({
  ...delegate,
  notifyPending: events.notifyPending,
  fetch: (request, requestOptions) => {
    if (!authenticateHarnessRequest(request, options.credential)) {
      return Promise.resolve(
        new Response("Unauthorized", {
          status: 401,
          headers: { "www-authenticate": "Bearer" },
        }),
      );
    }
    return delegate.fetch(request, requestOptions);
  },
  close: () =>
    Effect.runPromise(
      Effect.all([events.close, Scope.close(scope, Exit.void)], {
        concurrency: 2,
        discard: true,
      }).pipe(
        Effect.ensuring(
          Effect.tryPromise(() => delegate.close()).pipe(Effect.ignore),
        ),
      ),
    ),
});
const acquireWebhook = (
  options: HarnessMcpHandlerOptions,
  scope: Scope.CloseableScope,
  gate: Effect.Semaphore,
  owner: ConsumerOwner,
) =>
  Effect.gen(function* () {
    const context = yield* Layer.buildWithScope(webhookHttpClientLayer, scope);
    return yield* makeWebhookEvents(options.eventStore, gate, owner).pipe(
      Effect.provide(context),
      Scope.extend(scope),
      Effect.mapError(() => ({ reason: "persistence-failed" })),
    );
  });
const acquireHandler = (
  options: HarnessMcpHandlerOptions,
  scope: Scope.CloseableScope,
) =>
  Effect.gen(function* () {
    const identity = yield* Identity;
    const services = yield* Effect.runtime();
    const gate = yield* Effect.makeSemaphore(1);
    const owner = yield* makeConsumerOwner;
    const webhook = yield* acquireWebhook(options, scope, gate, owner);
    const poll = yield* makeHarnessPollEvents(options.eventStore, gate, owner);
    const events = yield* makeHarnessEvents({
      owner,
      webhook,
      poll,
      summary: options.operations.readInboxSummary,
      gate,
    });
    return guardedHandler(
      options,
      officialHandler(options, events, scope, { services, identity }),
      events,
      scope,
    );
  });

/**
 * Create one official MCP handler with explicit event resource ownership.
 * @param options Daemon operations, the bearer credential and callback persistence.
 * @returns Handler whose catalog follows the daemon's protocol state.
 */
export const makeHarnessMcpHttpHandler = (
  options: HarnessMcpHandlerOptions,
): Effect.Effect<HarnessMcpEventHandler, ClosedOperationError, Identity> =>
  Effect.gen(function* () {
    const scope = yield* Scope.make();
    return yield* acquireHandler(options, scope).pipe(
      Effect.onError(() => Scope.close(scope, Exit.void)),
    );
  }).pipe(Effect.withSpan("makeHarnessMcpHttpHandler"));

/* eslint-enable agent-code-guard/async-keyword -- Restore repository defaults after the MCP boundary. */

/** The loopback Streamable HTTP listener. */
export { acquireHarnessMcpHttpServer, LOOPBACK_HOST } from "./http.js";
/** Management results exposed by the catalog. */
export type {
  ManagementRegisterRequest,
  ManagementRegisterResult,
  ManagementStatusResult,
} from "../../host-protocol/index.js";
