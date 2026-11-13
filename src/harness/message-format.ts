/** @file What a host sends and receives as values: sends and their canonical destinations, inbox entries, history records, and what a refused send or read reports. */

import {
  Array,
  Effect,
  Option,
  type ParseResult,
  Schema,
  SchemaAST,
} from "effect";
import {
  AcceptResponse,
  AGENT_ADDRESS_PREFIX,
  AgentAddress,
  CollectiveId,
  DeadlineSeconds,
  DeclineResponse,
  GROUP_ADDRESS_PREFIX,
  GroupAddress,
  maximumMembers,
  type Message,
  MessageAddressInput,
  MsgId,
  PaginatedResult,
  RequestedSchema,
  SendError,
  type ValidationIssue,
  wellFormedString,
} from "../communication/index.js";
import { canonicalIdentifier, compareAscii, exactStruct } from "../encoding.js";
import { LocalGuidance, PermittedItem } from "../firewall/index.js";
import { AgentName } from "../identity.js";

/** Canonical host address of a direct or group conversation. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- The inbox owns this grammar shared by persistence and MCP schema composition.
export const MessageAddress = Schema.Union(AgentAddress, GroupAddress);
/** @useDeclaredType */
export type MessageAddress = typeof MessageAddress.Type;

/** Stable lookup and acknowledgment identity for one saved incoming item. */
export const DeliveryToken = canonicalIdentifier("DeliveryToken", "dlv_", 32);
/** @useDeclaredType */
export type DeliveryToken = typeof DeliveryToken.Type;

/** Canonical host conversation containing this permitted item. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- The inbox owns this grammar shared by persistence and MCP schema composition.
export const InboxEvent = Schema.Struct({
  address: MessageAddress,
  item: PermittedItem,
  guidance: Schema.optionalWith(LocalGuidance, { exact: true }),
});
/** @useDeclaredType */
export type InboxEvent = typeof InboxEvent.Type;

/** Pending deliveries retain their token after acknowledgment. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- The inbox owns this grammar shared by persistence and MCP schema composition.
export const InboxEntry = Schema.Struct({
  ...InboxEvent.fields,
  deliveryToken: DeliveryToken,
});
/** @useDeclaredType */
export type InboxEntry = typeof InboxEntry.Type;

/** Reading leaves entries pending. Absence of nextCursor ends the page traversal. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- The inbox owns this grammar shared by persistence and MCP schema composition.
export const InboxPage = Schema.Struct({
  ...PaginatedResult.fields,
  items: Schema.Array(InboxEntry),
});
/** @useDeclaredType */
export type InboxPage = typeof InboxPage.Type;

const inboxFailureText = {
  "invalid-cursor":
    "the cursor is not one this inbox returned; call again without a cursor",
  "invalid-event": "a saved inbox item could not be read",
  "persistence-failed": "the inbox could not be read; try again",
} as const;

/** Inbox reads reject unusable cursors and unavailable retained items. */
export class InboxError extends Schema.TaggedError<InboxError>()("InboxError", {
  reason: Schema.Literal(
    "invalid-cursor",
    "invalid-event",
    "persistence-failed",
  ),
}) {
  override get message(): string {
    return `receiving failed: ${inboxFailureText[this.reason]}`;
  }
}

/** Acknowledgment changes pending delivery, not the saved content. */
export class DeliveryAcknowledgeError extends Schema.TaggedError<DeliveryAcknowledgeError>()(
  "DeliveryAcknowledgeError",
  {
    reason: Schema.Literal(
      "unknown-delivery",
      "delivery-conflict",
      "persistence-failed",
    ),
  },
) {}

const multicast = exactStruct({
  op: Schema.optionalWith(Schema.Literal("multicast"), { exact: true }),
});
const question = exactStruct({
  op: Schema.Literal("gather", "all_gather"),
  deadline: DeadlineSeconds,
  requestedSchema: RequestedSchema,
});
const response = Schema.Union(AcceptResponse, DeclineResponse);

const MessageSendRequest = exactStruct({
  to: MessageAddressInput,
  text: wellFormedString,
  collective: Schema.optionalWith(multicast, { exact: true }),
});
const QuestionSendRequest = exactStruct({
  to: MessageAddressInput,
  text: wellFormedString,
  collective: question,
});
const AnswerSendRequest = exactStruct({
  to: MessageAddressInput,
  collectiveResponse: response,
});
/** Host addresses are resolved by SocialHarness before communication preparation. */
export const SendRequest = Schema.Union(
  MessageSendRequest,
  QuestionSendRequest,
  AnswerSendRequest,
).annotations({ identifier: "SendRequest" });
/** Question deadlines include policy assessment time.
 * @useDeclaredType
 */
export type SendRequest = typeof SendRequest.Type;

/**
 * One send written as a single object, which is the shape a tool's arguments
 * must have. Decoding yields the {@link SendRequest} the fields amount to and
 * refuses any other combination of them.
 */
export const SendArguments = exactStruct({
  to: Schema.propertySignature(MessageAddressInput).annotations({
    description: "The recipient: agent:<name>, or group:<members>.",
  }),
  text: Schema.optionalWith(wellFormedString, { exact: true }).annotations({
    description:
      "The message text. Required unless collectiveResponse is given.",
  }),
  collective: Schema.optionalWith(Schema.Union(multicast, question), {
    exact: true,
  }).annotations({
    description:
      "With text: asks the text as a question. gather asks each agent the address names privately and returns one result to you. all_gather asks a group and returns the same result to every member. deadline is seconds from now, and requestedSchema is the form each answer fills in. The result arrives as a collectiveResult inbox item.",
  }),
  collectiveResponse: Schema.optionalWith(response, {
    exact: true,
  }).annotations({
    description:
      "In place of text: your one answer to a question, which arrives as a collectiveRequest inbox item. Send it to that item's address with the operationId in item.message.operation. accept takes content that fits that operation's requestedSchema. decline takes none.",
  }),
}).pipe(Schema.compose(Schema.typeSchema(SendRequest), { strict: false }));

/** The arguments of a send when collectives are turned off: text to a recipient. */
export const TextSendArguments = exactStruct({
  to: Schema.propertySignature(MessageAddressInput).annotations({
    description: "The recipient: agent:<name>, or group:<members>.",
  }),
  text: Schema.propertySignature(wellFormedString).annotations({
    description: "The message text.",
  }),
});

/** Accepted ordinary messages, replies and questions share one receipt shape. */
export const SendResultSchema = Schema.Struct({
  id: Schema.Union(MsgId, CollectiveId),
  address: MessageAddress,
}).annotations({ identifier: "SendResult" });

/** Accepted work continues independently of the caller's lifetime. */
export type SendResult = typeof SendResultSchema.Type;

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

const MAXIMUM_VALIDATION_ISSUES = 20;

/**
 * Returns at most 20 distinct schema expectations and field paths. Submitted
 * values and custom decoder messages stay private. Union errors describe the
 * arms selected by the input. Named values report their full constraint.
 */
export function validationIssues(error: ParseResult.ParseError) {
  const issues: ValidationIssue[] = [];
  appendIssues(error.issue, [], issues);
  return issues;
}

function invalidAddress(): SendError {
  return new SendError({ reason: "invalid-address" });
}

function invalidMembership(detail: string): SendError {
  return new SendError({ reason: "membership-invalid", detail });
}

function decodeAgentName(value: string): Effect.Effect<AgentName, SendError> {
  return Schema.decodeUnknown(AgentName)(value).pipe(
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

function appendIssues(
  issue: ParseResult.ParseIssue,
  path: ReadonlyArray<string | number>,
  issues: ValidationIssue[],
): void {
  if (issues.length >= MAXIMUM_VALIDATION_ISSUES) {
    return;
  }
  if (issue._tag === "Pointer") {
    appendIssues(
      issue.issue,
      [...path, ...Array.ensure(issue.path).map(pathSegment)],
      issues,
    );
    return;
  }
  if (issue._tag === "Composite" && issue.ast._tag === "Union") {
    appendUnion(issue, path, issues);
    return;
  }
  if (issue._tag === "Composite") {
    appendChildren(Array.ensure(issue.issues), path, issues);
    return;
  }
  if (issue._tag === "Refinement" || issue._tag === "Transformation") {
    appendRefined(issue, path, issues);
    return;
  }
  addIssue(issues, { path, message: expectedMessage(issue) });
}

/** Named values report their full constraint. Other refinements report the inner failure. */
function appendRefined(
  issue: ParseResult.Refinement | ParseResult.Transformation,
  path: ReadonlyArray<string | number>,
  issues: ValidationIssue[],
): void {
  if (issue._tag === "Refinement" && namedValue(issue)) {
    addIssue(issues, { path, message: expectedSchemaMessage(issue.ast) });
    return;
  }
  appendIssues(issue.issue, path, issues);
}

/**
 * Reports selected union arms once when they reject the whole value, or reports
 * their failing fields when they match its structure.
 */
function appendUnion(
  issue: ParseResult.Composite,
  path: ReadonlyArray<string | number>,
  issues: ValidationIssue[],
): void {
  const arms = selectedArms(Array.ensure(issue.issues), issue.actual);
  const inside = arms.filter((arm) => !leafFailure(arm));
  if (inside.length === 0 && arms.length > 1) {
    addIssue(issues, { path, message: expectedSchemaMessage(issue.ast) });
    return;
  }
  appendChildren(inside.length === 0 ? arms : inside, path, issues);
}

function appendChildren(
  children: readonly ParseResult.ParseIssue[],
  path: ReadonlyArray<string | number>,
  issues: ValidationIssue[],
): void {
  for (const child of children) {
    appendIssues(child, path, issues);
    if (issues.length >= MAXIMUM_VALIDATION_ISSUES) {
      break;
    }
  }
}

/**
 * Selects arms by matching literal fields, then required and forbidden fields.
 * An absent-value arm cannot explain a present value. If none match, all report.
 */
function selectedArms(
  arms: readonly ParseResult.ParseIssue[],
  actual: unknown,
): readonly ParseResult.ParseIssue[] {
  const present = arms.filter(
    (arm) => !(arm._tag === "Type" && arm.ast._tag === "UndefinedKeyword"),
  );
  const candidates = present.length === 0 ? arms : present;
  const chosen = candidates.filter((arm) => literalsMatch(arm, actual));
  if (chosen.length > 0) {
    return chosen;
  }
  const fitting = candidates.filter((arm) => !fieldsMismatch(arm));
  return fitting.length > 0 ? fitting : candidates;
}

function literalsMatch(
  issue: ParseResult.ParseIssue,
  actual: unknown,
): boolean {
  if (issue._tag === "Refinement" || issue._tag === "Transformation") {
    return literalsMatch(issue.issue, actual);
  }
  if (issue._tag !== "Composite") {
    return false;
  }
  if (issue.ast._tag === "Union") {
    return Array.ensure(issue.issues).some((arm) => literalsMatch(arm, actual));
  }
  if (issue.ast._tag !== "TypeLiteral" || !isRecord(actual)) {
    return false;
  }
  const literals = issue.ast.propertySignatures.flatMap((signature) =>
    signature.type._tag === "Literal"
      ? [{ name: signature.name, literal: signature.type.literal }]
      : [],
  );
  return (
    literals.length > 0 &&
    literals.every(({ name, literal }) => actual[name] === literal)
  );
}

function fieldsMismatch(issue: ParseResult.ParseIssue): boolean {
  if (issue._tag === "Refinement" || issue._tag === "Transformation") {
    return fieldsMismatch(issue.issue);
  }
  if (issue._tag !== "Composite") {
    return false;
  }
  const children = Array.ensure(issue.issues);
  if (issue.ast._tag === "Union") {
    return children.every(fieldsMismatch);
  }
  return children.some(
    (child) =>
      child._tag === "Pointer" &&
      (child.issue._tag === "Missing" || child.issue._tag === "Unexpected"),
  );
}

function namedValue(issue: ParseResult.Refinement): boolean {
  return (
    Option.isSome(SchemaAST.getIdentifierAnnotation(issue.ast)) &&
    Option.isSome(SchemaAST.getDescriptionAnnotation(issue.ast)) &&
    leafFailure(issue)
  );
}

function leafFailure(issue: ParseResult.ParseIssue): boolean {
  if (issue._tag === "Refinement" || issue._tag === "Transformation") {
    return leafFailure(issue.issue);
  }
  return issue._tag !== "Composite" && issue._tag !== "Pointer";
}

function addIssue(issues: ValidationIssue[], issue: ValidationIssue): void {
  const repeated = issues.some(
    (other) =>
      other.message === issue.message &&
      other.path.join("\0") === issue.path.join("\0"),
  );
  if (!repeated) {
    issues.push(issue);
  }
}

function isRecord(
  value: unknown,
): value is Readonly<Record<PropertyKey, unknown>> {
  return typeof value === "object" && value !== null;
}

function pathSegment(key: PropertyKey): string | number {
  return typeof key === "number" ? key : String(key);
}

function expectedMessage(
  issue:
    | ParseResult.Type
    | ParseResult.Forbidden
    | ParseResult.Missing
    | ParseResult.Unexpected,
): string {
  switch (issue._tag) {
    case "Unexpected":
      return "Unexpected field";
    case "Missing":
      return "Required field is missing";
    case "Type":
      if (issue.ast._tag === "NeverKeyword") {
        return "Unexpected field";
      }
      return expectedSchemaMessage(issue.ast);
    case "Forbidden":
      return expectedSchemaMessage(issue.ast);
    default:
      return issue satisfies never;
  }
}

function expectedSchemaMessage(ast: SchemaAST.AST): string {
  return `Expected ${Option.getOrElse(
    SchemaAST.getDescriptionAnnotation(ast),
    () => String(ast),
  )}`;
}

/** History preserves sent messages and the saved view of received content. */
export type HistoryRecord =
  | { readonly kind: "sent"; readonly message: Message }
  | ({
      readonly kind: "received";
      readonly message: {
        readonly authorAgentId: Message["author"]["agentId"];
        readonly msgId: Message["msgId"];
      };
    } & InboxEvent)
  | ({
      readonly kind: "result";
      readonly deliveryToken: DeliveryToken;
    } & InboxEvent);

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
