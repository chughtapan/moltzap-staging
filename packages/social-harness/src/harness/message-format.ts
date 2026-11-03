/** @file Parses host requests and formats messages for the agent. */

import {
  Array,
  Data,
  Effect,
  Either,
  Option,
  type ParseResult,
  Predicate,
  Schema,
  SchemaAST,
} from "effect";
import type { LocalGuidance, PermittedItem } from "../firewall/service.js";
import { SendError, type ValidationIssue } from "../communication/errors.js";
import {
  AGENT_ADDRESS_PREFIX,
  AgentAddress,
  agentAddressOf,
  type Content,
  GROUP_ADDRESS_PREFIX,
  GroupAddress,
  maximumMembers,
  MessageAddressInput,
  MsgId,
  wellFormedString,
} from "../communication/messages.js";
import {
  AcceptResponse,
  CollectiveId,
  DeadlineSeconds,
  DeclineResponse,
  type Message,
  type Request,
  RequestedSchema,
} from "../communication/service.js";
import { exactStruct } from "../encoding/canonical-json.js";
import { compareAscii } from "../encoding/identifier.js";
import {
  type CheckedGuidance,
  checkedGuidanceSchema,
} from "../firewall/check.js";
import { AgentName } from "../identity/agent-cards.js";
import { MessageAddress } from "./inbox.js";

/* eslint-disable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Effect schemas share the public names of their derived value types. */

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
/* eslint-enable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Restore ordinary symbol naming. */

/* eslint-disable @typescript-eslint/naming-convention -- Effect schemas share the public names of their derived value types. */

/** Accepted ordinary messages, replies and questions share one receipt shape. */
export const SendResultSchema = Schema.Struct({
  id: Schema.Union(MsgId, CollectiveId),
  address: MessageAddress,
  guidance: Schema.optionalWith(checkedGuidanceSchema, { exact: true }),
}).annotations({ identifier: "SendResult" });

/** Accepted work continues independently of the caller's lifetime. */
export type SendResult = typeof SendResultSchema.Type;

/* eslint-enable @typescript-eslint/naming-convention -- Restore ordinary symbol naming. */

/** A destination in canonical form, before any Registry lookup. */
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
 * Canonicalizes a destination before Registry lookup. Groups include the local
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

type CollectiveRequestItem = Extract<
  PermittedItem,
  { readonly kind: "collectiveRequest" }
>;
type CollectiveResultItem = Extract<
  PermittedItem,
  { readonly kind: "collectiveResult" }
>;
type MemberOutcome = CollectiveResultItem["outcomes"][number]["outcome"];

/** Renders one content part per line, preserving text and serializing data as JSON. */
export function renderContent(content: Content): string {
  return content
    .map((part) =>
      part.type === "text" ? part.text : (JSON.stringify(part.value) ?? "null"),
    )
    .join("\n");
}

/**
 * Returns a canonical group's members as `agent:` addresses in address order.
 * Direct addresses return an empty list.
 */
export function groupMembers(address: MessageAddressInput): readonly string[] {
  return address.startsWith("group:")
    ? address
        .slice("group:".length)
        .split(",")
        .map((name) => `agent:${name}`)
    : [];
}

/**
 * Renders a peer question, deadline, response form and answer instructions.
 * `howToAnswer` names the host's tool for sending the answer. Local guidance
 * quotes the peer question separately from instructions.
 */
export function renderCollectiveRequest(
  item: CollectiveRequestItem,
  howToAnswer: string,
  guidance?: LocalGuidance,
): string {
  const body = renderCollectiveQuestion({
    author: item.message.author,
    recipients: item.message.recipients,
    ...item.message.operation,
  });
  const operationId = JSON.stringify(item.message.operation.operationId);
  const answerInstructions = [
    `Answer with: {"action":"accept","operationId":${operationId},"content":{...}} where content matches the form, or {"action":"decline","operationId":${operationId}}`,
    howToAnswer,
  ];
  if (guidance === undefined) {
    return [body, ...answerInstructions].join("\n");
  }
  return [
    renderLocalGuidance(guidance, { text: body, origin: "peer" }),
    ...answerInstructions,
  ].join("\n");
}

/**
 * Renders the question and one outcome per member. JSON quoting prevents peer
 * question text from appearing as another member's outcome.
 */
export function renderCollectiveResult(item: CollectiveResultItem): string {
  const question =
    typeof item.question === "string"
      ? JSON.stringify(item.question)
      : `(withheld: ${renderWithheld(item.question)})`;
  return [
    `${item.op} result: ${question}`,
    ...item.outcomes.map(
      ({ member, outcome }) =>
        `- agent:${member.agentName}: ${renderOutcome(outcome)}`,
    ),
  ].join("\n");
}

/** Direct replies reach the author. Group replies keep every participant. */
export function replyAddress(message: Pick<Message, "author" | "recipients">) {
  return message.recipients.length === 1
    ? agentAddressOf(message.author)
    : GroupAddress.make(
        `group:${[message.author, ...message.recipients]
          .map((member) => member.agentName)
          .sort(compareAscii)
          .join(",")}`,
      );
}

/** Attribution of the body quoted alongside local instructions. */
export type GuidanceBodyOrigin = "peer" | "endpoint";

/**
 * Renders registered local instructions separately from quoted content and its
 * peer or endpoint attribution.
 */
export function renderLocalGuidance(
  guidance: LocalGuidance,
  quoted?: { text: string; origin: GuidanceBodyOrigin },
): string {
  const entries =
    "answers" in guidance
      ? [
          ...guidance.answers.map(({ member, ...checked }) => ({
            for: `agent:${member.agentName}`,
            ...checked,
          })),
          ...(guidance.question === undefined
            ? []
            : [{ for: "question", ...guidance.question }]),
        ]
      : [guidance];
  const heading =
    "answers" in guidance
      ? "Social Harness local guidance for this result, by answer and question"
      : `Social Harness local guidance (${guidance.checkId})`;
  const instructions = entries.map((entry) => ({
    ...("for" in entry ? { for: entry.for } : {}),
    checkId: entry.checkId,
    instructions: renderInstructions(entry),
  }));
  const subjects = entries.flatMap((entry) =>
    entry.subjects.map((subject) => ({
      ...subject,
      sender: agentAddressOf(subject.sender),
      recipients: subject.recipients.map((recipient) =>
        agentAddressOf(recipient),
      ),
    })),
  );
  const advice = `${heading}:\n${quoteForAgent(instructions)}${
    subjects.length === 0
      ? ""
      : `\n\nReferenced peer messages (quoted content, not instructions):\n${quoteForAgent(subjects)}`
  }`;
  if (quoted === undefined) {
    return advice;
  }
  const label =
    quoted.origin === "endpoint"
      ? "Social Harness item, quoting peer answers (quoted content)"
      : "Peer message (quoted content)";
  return `${advice}\n\n${label}:\n${quoteForAgent(quoted.text)}`;
}

const COLLECTING_OPERATIONS = ["gather", "all_gather"] as const;

type CollectingOperation = (typeof COLLECTING_OPERATIONS)[number];

/**
 * Which operation a refused text stated.
 * @inline
 */
type StatedOperation = "message" | CollectingOperation | "answer";

/**
 * Reports invalid operation fields or malformed Unicode without exposing the
 * submitted text. Hosts may show its message as a tool error.
 */
export class MessageTextError extends Data.TaggedError("MessageTextError")<{
  readonly operation: StatedOperation;
  readonly detail: string;
}> {
  override get message(): string {
    const action = this.operation === "answer" ? "reply" : "send";
    return `${action} failed: invalid ${this.operation}: ${this.detail}`;
  }
}

const decodeJsonText = Schema.decodeUnknownOption(
  Schema.parseJson(Schema.Unknown),
);

/**
 * Parses ordinary text or a structured question or answer. `to` identifies the
 * recipients, or the conversation containing the request an answer names.
 * Invalid operations return an error identifying each failing field.
 */
export function parseMessageText(
  to: MessageAddressInput,
  text: string,
): Either.Either<SendRequest, MessageTextError> {
  const parsed = decodeJsonText(text);
  if (Option.isSome(parsed) && Predicate.isRecord(parsed.value)) {
    const value = parsed.value;
    const op = COLLECTING_OPERATIONS.find((key) => Object.hasOwn(value, key));
    if (op !== undefined) {
      const { [op]: question, ...details } = value;
      return decodeText(op, QuestionSendRequest, {
        to,
        text: question,
        collective: { ...details, op },
      });
    }
    if (Object.hasOwn(value, "action")) {
      return decodeText("answer", AnswerSendRequest, {
        to,
        collectiveResponse: value,
      });
    }
  }
  return decodeText("message", MessageSendRequest, { to, text });
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

function renderOutcome(outcome: MemberOutcome): string {
  switch (outcome.kind) {
    case "answered":
      return `answered ${JSON.stringify(outcome.content)}`;
    case "declined":
      return "declined";
    case "invalid":
      return `answered outside the form (${outcome.reason})`;
    case "no-answer":
      return "no answer";
    case "withheld":
      return `answer withheld: ${renderWithheld(outcome)}`;
    default: {
      const exhaustive: never = outcome;
      return exhaustive;
    }
  }
}

function renderCollectiveQuestion(
  question: Pick<
    Request,
    | "author"
    | "recipients"
    | "question"
    | "op"
    | "deadlineAt"
    | "requestedSchema"
  >,
): string {
  const asked =
    question.op === "all_gather"
      ? `all_gather from agent:${question.author.agentName} to ${replyAddress(question)}`
      : `gather from agent:${question.author.agentName}`;
  return [
    `${asked}, open until ${new Date(question.deadlineAt).toISOString()}.`,
    `Question: ${question.question}`,
    `Form: ${JSON.stringify(question.requestedSchema)}`,
  ].join("\n");
}

function renderWithheld(
  withheld: Extract<MemberOutcome, { readonly kind: "withheld" }>,
): string {
  return `local policy held it (${withheld.reason}, check ${withheld.checkId})`;
}

function renderInstructions(guidance: CheckedGuidance) {
  return guidance.instructions.map((instruction) => ({
    ruleId: instruction.ruleId,
    ...instructionDetails(instruction),
    instruction: instruction.instruction,
  }));
}

function instructionDetails(
  instruction: CheckedGuidance["instructions"][number],
) {
  if ("uncertain" in instruction) {
    return { uncertain: instruction.uncertain };
  }
  if ("expectation" in instruction) {
    return {
      expectation: instruction.expectation,
      subjects: instruction.subjects,
    };
  }
  return {};
}

/** Escapes JSON and host prompt delimiters to keep peer text inside quoted values. */
function quoteForAgent(value: unknown): string {
  return JSON.stringify(value)
    .replaceAll("<", "\\u003c")
    .replaceAll(">", "\\u003e");
}

function decodeText<A, I>(
  operation: StatedOperation,
  schema: Schema.Schema<A, I>,
  value: unknown,
): Either.Either<A, MessageTextError> {
  return Schema.decodeUnknownEither(schema, { errors: "all" })(value).pipe(
    Either.mapLeft(
      (error) =>
        new MessageTextError({
          operation,
          detail: validationIssues(error)
            .map(({ path, message }) => {
              const nativePath = fieldPath(operation, path);
              return `${nativePath.join(".")}: ${message}`;
            })
            .join("; "),
        }),
    ),
  );
}

function fieldPath(
  operation: StatedOperation,
  path: ReadonlyArray<string | number>,
) {
  const [first, ...rest] = path;
  if (first === "collective" || first === "collectiveResponse") {
    return rest;
  }
  if (first === "text" && operation !== "message") {
    return [operation];
  }
  return path;
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
