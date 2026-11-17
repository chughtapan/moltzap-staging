/** @file One agent's MCP app: its tools and its inbox events. */

import { HttpServerResponse } from "@effect/platform";
import {
  Array as Arr,
  Cause,
  type Context,
  Deferred,
  Effect,
  JSONSchema,
  Option,
  type ParseResult,
  Schema,
  SchemaAST,
  Stream,
} from "effect";
import type { HarnessWebhookEvents } from "./webhook.js";
import {
  issuesText,
  type MessageAddressInput,
  type ValidationIssue,
} from "../communication/index.js";
import { Firewall } from "../firewall/service.js";
import { Identity } from "../identity.js";
import { HeldNorms } from "../norms/index.js";
import {
  CallToolResult,
  ProtocolError,
  Tool,
  ToolAnnotations,
} from "../vendor/effect-mcp/McpSchema.js";
import { makeHttpApp, McpServer } from "../vendor/effect-mcp/McpServer.js";
import {
  clientToolDefinitions,
  textOnlyToolDefinitions,
  toolDefinitions,
} from "./contracts.js";
import { limitResponseStream } from "./http.js";
import { SocialHarness } from "./social-harness.js";

interface AgentMcpAppSettings {
  readonly subscription: HarnessWebhookEvents;
  /** Leaves collective requests and responses out of send_message, which then sends text only. */
  readonly hideCollectives: boolean;
  /**
   * Set when this agent is a client in client/server mode: the address of the
   * server agent, the only agent it exchanges messages with.
   */
  readonly serverAgentAddress: Option.Option<MessageAddressInput>;
  readonly stopped: Deferred.Deferred<undefined>;
  readonly implementation: { readonly name: string; readonly version: string };
}

type Harness = Context.Tag.Service<typeof SocialHarness>;

/**
 * Serves one agent's tools and inbox events until the agent stops, after
 * which every request, and every stream still open, gets 503.
 */
export const makeAgentMcpApp = Effect.fn("makeAgentMcpApp")(function* (
  settings: AgentMcpAppSettings,
) {
  const harness = yield* SocialHarness;
  const firewall = yield* Firewall;
  const mcp = yield* McpServer.make;
  mcp.events.current = settings.subscription;
  yield* Effect.forEach(
    [
      yield* statusTool(settings.subscription),
      ...Option.match(settings.serverAgentAddress, {
        onNone: () => peerTools(harness, firewall, settings),
        onSome: (serverAgentAddress) =>
          clientTools(harness, serverAgentAddress),
      }),
      ...Option.toArray(yield* Effect.serviceOption(HeldNorms)).map(
        (heldNorms) =>
          normsTool(heldNorms, Option.isSome(settings.serverAgentAddress)),
      ),
    ],
    mcp.addTool,
    { concurrency: 1, discard: true },
  );
  const app = yield* makeHttpApp(settings.implementation).pipe(
    Effect.provideService(McpServer, mcp),
  );
  const stopped = Deferred.await(settings.stopped);
  const unavailable = stopped.pipe(
    Effect.interruptible,
    Effect.as(HttpServerResponse.text("Agent unavailable", { status: 503 })),
  );
  return Deferred.isDone(settings.stopped).pipe(
    Effect.flatMap((done) =>
      done ? unavailable : app.pipe(Effect.raceFirst(unavailable)),
    ),
    Effect.flatMap((response) =>
      limitResponseStream(response, Stream.interruptWhen(stopped)),
    ),
  );
});

/** The tools of a peer and of the server agent: each addresses any agent and owns its message policy. */
function peerTools(
  harness: Harness,
  firewall: Context.Tag.Service<typeof Firewall>,
  settings: Pick<AgentMcpAppSettings, "hideCollectives">,
) {
  const policyConfigured = firewall
    .readPolicy()
    .pipe(Effect.match({ onFailure: () => false, onSuccess: () => true }));
  const send = { name: "send_message", execute: harness.send };
  return [
    settings.hideCollectives
      ? bind({ ...send, definition: textOnlyToolDefinitions.send_message })
      : bind({ ...send, definition: toolDefinitions.send_message }),
    receiveTool(harness),
    bind({
      name: "list_conversations",
      definition: toolDefinitions.list_conversations,
      execute: harness.listConversations,
    }),
    bind({
      name: "read_conversation",
      definition: toolDefinitions.read_conversation,
      execute: harness.readHistory,
    }),
    bind({
      name: "read_policy",
      definition: toolDefinitions.read_policy,
      execute: () => firewall.readPolicy(),
      visible: policyConfigured,
    }),
    bind({
      name: "update_policy",
      definition: toolDefinitions.update_policy,
      execute: firewall.updatePolicy,
      visible: policyConfigured,
    }),
    bind({
      name: "read_firewall_decisions",
      definition: toolDefinitions.read_firewall_decisions,
      execute: firewall.readDecisions,
    }),
  ];
}

/**
 * The tools of a client in client/server mode. It sends to and reads its
 * conversation with the server agent, and names neither.
 */
function clientTools(
  harness: Harness,
  serverAgentAddress: MessageAddressInput,
) {
  return [
    bind({
      name: "send_message",
      definition: clientToolDefinitions.send_message,
      execute: ({ text }) => harness.send({ to: serverAgentAddress, text }),
    }),
    receiveTool(harness),
    bind({
      name: "read_conversation",
      definition: clientToolDefinitions.read_conversation,
      execute: (page) =>
        harness.readHistory({ ...page, address: serverAgentAddress }),
    }),
  ];
}

function receiveTool(harness: Harness) {
  return bind({
    name: "receive_messages",
    definition: toolDefinitions.receive_messages,
    execute: harness.receive,
  });
}

function statusTool(subscription: HarnessWebhookEvents) {
  return Effect.map(Identity, (identity) =>
    bind({
      name: "status",
      definition: toolDefinitions.status,
      execute: () =>
        Effect.map(subscription.status, (status) => ({
          agentId: identity.me.agentId,
          agentName: identity.me.agentName,
          subscription: status,
        })),
    }),
  );
}

/** `read_norms` lists the norms held: this agent's, or for a client the server agent's. */
function normsTool(
  heldNorms: Context.Tag.Service<typeof HeldNorms>,
  forClient: boolean,
) {
  return bind({
    name: "read_norms",
    definition: forClient
      ? clientToolDefinitions.read_norms
      : toolDefinitions.read_norms,
    execute: () =>
      heldNorms.read().pipe(
        Effect.map((held) => ({
          norms: held.map((norm) => ({
            name: norm.name,
            description: norm.description,
            ...(norm.protocolName === undefined
              ? {}
              : { protocol: norm.protocolName }),
            document: norm.document,
          })),
        })),
      ),
  });
}

/** What a tool definition says about a tool before any operation is attached. */
interface Definition<A, I, B, J, E> {
  readonly description: string;
  readonly input: Schema.Schema<A, I>;
  readonly output: Schema.Schema<B, J>;
  readonly annotations: {
    readonly readOnlyHint: boolean;
    readonly openWorldHint: boolean;
    readonly destructiveHint?: boolean;
    readonly idempotentHint?: boolean;
  };
  readonly failure: {
    readonly toData: (error: E) => unknown;
    /** The failure to answer when the tool ends in a way it does not declare. */
    readonly unexpected?: E;
  };
}

/**
 * Turns a tool definition and the operation behind it into a registered
 * tool: arguments are decoded with the definition's input, the result is
 * encoded with its output, and a failure is answered with its message and
 * the data the definition writes for it.
 */
function bind<A, I, B, J, E extends { readonly message: string }>(binding: {
  readonly name: string;
  readonly definition: Definition<A, I, B, J, E>;
  readonly execute: (input: A) => Effect.Effect<B, E>;
  /** Checked on every listing and call. A tool without it is always offered. */
  readonly visible?: Effect.Effect<boolean>;
}) {
  const { name, definition, execute, visible } = binding;
  const refusal = (error: E) =>
    new ProtocolError(-32603, error.message, definition.failure.toData(error));
  const unexpected =
    definition.failure.unexpected === undefined
      ? new ProtocolError(
          -32603,
          "The daemon could not complete this call. Try again; if it keeps failing, check the daemon's log.",
        )
      : refusal(definition.failure.unexpected);
  return {
    tool: new Tool({
      name,
      description: definition.description,
      inputSchema: jsonSchema(definition.input),
      outputSchema: openObjects(jsonSchema(definition.output)),
      annotations: new ToolAnnotations({
        destructiveHint: true,
        idempotentHint: false,
        ...definition.annotations,
      }),
    }),
    ...(visible === undefined ? {} : { visible }),
    handle: (input: unknown) =>
      Effect.gen(function* () {
        const decoded = yield* Schema.decodeUnknown(definition.input, {
          onExcessProperty: "error",
          errors: "all",
        })(input).pipe(Effect.mapError(invalidArguments));
        const output = yield* execute(decoded).pipe(Effect.mapError(refusal));
        const encoded = yield* Schema.encode(definition.output)(output, {
          onExcessProperty: "error",
        }).pipe(Effect.mapError(() => unexpected));
        return new CallToolResult({
          content: [{ type: "text", text: JSON.stringify(encoded) }],
          structuredContent: encoded,
        });
      }).pipe(
        Effect.catchAllCause((cause) =>
          Effect.fail(
            Option.getOrElse(Cause.failureOption(cause), () => unexpected),
          ),
        ),
      ),
  };
}

/** A refusal of arguments that names each wrong field in its message and its data. */
function invalidArguments(error: Parameters<typeof validationIssues>[0]) {
  const issues = validationIssues(error);
  return new ProtocolError(
    -32602,
    `Invalid tool arguments: ${issuesText(issues)}`,
    { issues },
  );
}

const MAXIMUM_VALIDATION_ISSUES = 20;

/**
 * Returns at most 20 distinct schema expectations and field paths. Submitted
 * values and custom decoder messages stay private. Union errors describe the
 * arms selected by the input. Named values report their full constraint.
 */
function validationIssues(error: ParseResult.ParseError) {
  const issues: ValidationIssue[] = [];
  appendIssues(error.issue, [], issues);
  return issues;
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
      [...path, ...Arr.ensure(issue.path).map(pathSegment)],
      issues,
    );
    return;
  }
  if (issue._tag === "Composite" && issue.ast._tag === "Union") {
    appendUnion(issue, path, issues);
    return;
  }
  if (issue._tag === "Composite") {
    appendChildren(Arr.ensure(issue.issues), path, issues);
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
  const arms = selectedArms(Arr.ensure(issue.issues), issue.actual);
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
    return Arr.ensure(issue.issues).some((arm) => literalsMatch(arm, actual));
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
  const children = Arr.ensure(issue.issues);
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

function jsonSchema<A, I>(schema: Schema.Schema<A, I>) {
  const definitions: Record<string, JSONSchema.JsonSchema7> = {};
  const root = JSONSchema.fromAST(schema.ast, {
    definitions,
    target: "jsonSchema2020-12",
    topLevelReferenceStrategy: "skip",
  });
  return {
    ...root,
    ...(Object.keys(definitions).length === 0 ? {} : { $defs: definitions }),
  };
}

/** Equivalent single-item tuple notation also supports hosts whose validators use draft-07. */
function openObjects(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(openObjects);
  }
  if (value === null || typeof value !== "object") {
    return value;
  }
  const entries = Object.entries(value);
  const prefixItems: unknown = entries.find(
    ([key]) => key === "prefixItems",
  )?.[1];
  const singleTuple =
    Array.isArray(prefixItems) &&
    prefixItems.length === 1 &&
    entries.some(([key, nested]) => key === "items" && nested === false);
  return Object.fromEntries([
    ...entries.flatMap(([key, nested]) => {
      if (key === "additionalProperties" && nested === false) {
        return [];
      }
      if (singleTuple && (key === "items" || key === "maxItems")) {
        return [];
      }
      if (singleTuple && key === "prefixItems") {
        return [["items", openObjects(prefixItems[0])]];
      }
      return [[key, openObjects(nested)]];
    }),
    ...(singleTuple ? [["maxItems", 1]] : []),
  ]);
}
