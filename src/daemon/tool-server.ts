/**
 * @file The daemon's MCP handler and the tools it serves. Each request is
 * answered from one agent: a call's arguments are decoded, its operation run,
 * its result encoded and its failure worded by the tool's definition.
 */

import { HttpApp } from "@effect/platform";
import {
  createMcpHandler,
  McpServer,
  ProtocolError,
} from "@modelcontextprotocol/server";
import {
  Array as Arr,
  Cause,
  Data,
  Effect,
  Exit,
  JSONSchema,
  Option,
  type ParseResult,
  Runtime,
  Schema,
  SchemaAST,
} from "effect";
import { issuesText, type ValidationIssue } from "../communication/index.js";

/** What a request is answered from: an agent's tools, in listing order, and its Events calls. */
export interface AgentCalls {
  readonly tools: ReadonlyArray<ReturnType<typeof bind>>;
  readonly events: Readonly<
    Record<
      "list" | "subscribe" | "unsubscribe",
      (
        params: unknown,
      ) => Effect.Effect<Readonly<Record<string, unknown>>, JsonRpcError>
    >
  >;
}

/** The JSON-RPC error a tool call or an Events call is answered with. */
export class JsonRpcError extends Data.TaggedError("JsonRpcError")<{
  readonly code: number;
  readonly message: string;
  readonly data?: unknown;
}> {
  constructor(code: number, message: string, data?: unknown) {
    super({ code, message, data });
  }
}

/**
 * One admitted HTTP request: the agent that answers it, the runtime of the
 * fiber that carries it and the signal that it was aborted. It travels through
 * the MCP SDK as the request's authentication info, which the SDK reads
 * nothing from and hands to the server it builds for the request.
 */
class AdmittedRequest extends Data.Class<{
  readonly agent: AgentCalls;
  readonly runtime: Runtime.Runtime<never>;
  readonly signal: AbortSignal;
}> {}

/** The Events capability, which the SDK's capability type does not name. */
const eventsCapability = { events: {} };

const anyParams = Schema.standardSchemaV1(Schema.Unknown);

/** A tool's arguments and its result are objects, which is the JSON Schema MCP lists a tool with. */
const objectSchema = Schema.decodeUnknownSync(
  Schema.Struct(
    { type: Schema.Literal("object") },
    Schema.Record({ key: Schema.String, value: Schema.Unknown }),
  ),
);

/**
 * The daemon's one MCP handler. The returned function gives the HTTP app that
 * answers the current request from `agent`; the SDK parses the request,
 * negotiates the protocol revision, dispatches the method and frames the
 * response, and no MCP server outlives a request. The app is interruptible so
 * that a stopped agent or an aborted request ends a call in flight.
 */
export function makeMcpHandler(implementation: {
  readonly name: string;
  readonly version: string;
}) {
  const handler = createMcpHandler(({ authInfo }) =>
    requestServer(implementation, authInfo?.extra?.admitted),
  );
  return (agent: AgentCalls) =>
    Effect.runtime().pipe(
      Effect.flatMap((runtime) =>
        HttpApp.fromWebHandler((request) =>
          handler.fetch(request, {
            authInfo: {
              token: "",
              clientId: "",
              scopes: [],
              extra: {
                admitted: new AdmittedRequest({
                  agent,
                  runtime,
                  signal: request.signal,
                }),
              },
            },
          }),
        ),
      ),
      Effect.interruptible,
    );
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
 * Turns a tool definition and the operation behind it into a tool to serve.
 * `visible` is checked on every listing and call; a tool without it is
 * always offered.
 */
export function bind<A, I, B, J, E extends { readonly message: string }>(
  name: string,
  definition: Definition<A, I, B, J, E>,
  execute: (input: A) => Effect.Effect<B, E>,
  visible?: Effect.Effect<boolean>,
) {
  return {
    tool: {
      name,
      description: definition.description,
      inputSchema: objectSchema(jsonSchema(definition.input)),
      outputSchema: objectSchema(openObjects(jsonSchema(definition.output))),
      annotations: {
        destructiveHint: true,
        idempotentHint: false,
        ...definition.annotations,
      },
    },
    ...(visible === undefined ? {} : { visible }),
    handle: handler(definition, execute),
  };
}

/** The MCP server for one request, answering from the agent `admitted` names. */
function requestServer(
  implementation: { readonly name: string; readonly version: string },
  admitted: unknown,
) {
  const mcp = new McpServer(implementation, {
    capabilities: { tools: { listChanged: true }, ...eventsCapability },
  });
  if (!(admitted instanceof AdmittedRequest)) {
    return mcp;
  }
  const { agent } = admitted;
  const visible = (tool: AgentCalls["tools"][number]) =>
    tool.visible ?? Effect.succeed(true);
  mcp.server.setRequestHandler("tools/list", () =>
    run(
      admitted,
      Effect.filter(agent.tools, visible).pipe(
        Effect.map((tools) => ({ tools: tools.map(({ tool }) => tool) })),
      ),
    ),
  );
  mcp.server.setRequestHandler("tools/call", ({ params }) =>
    run(
      admitted,
      Effect.gen(function* () {
        const called = agent.tools.find(
          ({ tool }) => tool.name === params.name,
        );
        if (called === undefined || !(yield* visible(called))) {
          return yield* Effect.fail(new JsonRpcError(-32602, "Tool not found"));
        }
        return yield* called.handle(params.arguments ?? {});
      }),
    ),
  );
  for (const call of ["list", "subscribe", "unsubscribe"] as const) {
    mcp.server.setRequestHandler(
      `events/${call}`,
      { params: anyParams },
      (params) => run(admitted, agent.events[call](params)),
    );
  }
  return mcp;
}

/**
 * Runs the Effect that answers one MCP request in the runtime of the HTTP
 * request's fiber, so its spans keep their parent, and interrupts it when the
 * request is aborted. A failure rejects with the SDK's error for it, which the
 * SDK answers as the JSON-RPC error.
 */
function run<A>(
  { runtime, signal }: AdmittedRequest,
  effect: Effect.Effect<A, JsonRpcError>,
) {
  return new Promise<A>((resolve, reject) => {
    const interrupt = Runtime.runCallback(runtime)(effect, {
      onExit: Exit.match({
        onSuccess: resolve,
        onFailure: (cause) => {
          const { code, message, data } = Option.getOrElse(
            Cause.failureOption(cause),
            () => new JsonRpcError(-32603, "Internal error"),
          );
          reject(new ProtocolError(code, message, data));
        },
      }),
    });
    signal.addEventListener("abort", () => {
      interrupt();
    });
  });
}

/**
 * Answers a call of a tool: arguments are decoded with the definition's
 * input, the result is encoded with its output, and a failure is answered
 * with its message and the data the definition writes for it.
 */
function handler<A, I, B, J, E extends { readonly message: string }>(
  definition: Definition<A, I, B, J, E>,
  execute: (input: A) => Effect.Effect<B, E>,
) {
  const refusal = (error: E) =>
    new JsonRpcError(-32603, error.message, definition.failure.toData(error));
  const unexpected =
    definition.failure.unexpected === undefined
      ? new JsonRpcError(
          -32603,
          "The daemon could not complete this call. Try again; if it keeps failing, check the daemon's log.",
        )
      : refusal(definition.failure.unexpected);
  return (input: unknown) =>
    Effect.gen(function* () {
      const decoded = yield* Schema.decodeUnknown(definition.input, {
        onExcessProperty: "error",
        errors: "all",
      })(input).pipe(Effect.mapError(invalidArguments));
      const output = yield* execute(decoded).pipe(Effect.mapError(refusal));
      const encoded = yield* Schema.encode(definition.output)(output, {
        onExcessProperty: "error",
      }).pipe(Effect.mapError(() => unexpected));
      return {
        content: [{ type: "text" as const, text: JSON.stringify(encoded) }],
        structuredContent: encoded,
      };
    }).pipe(
      Effect.catchAllCause((cause) =>
        Effect.fail(
          Option.getOrElse(Cause.failureOption(cause), () => unexpected),
        ),
      ),
    );
}

/** A refusal of arguments that names each wrong field in its message and its data. */
function invalidArguments(error: Parameters<typeof validationIssues>[0]) {
  const issues = validationIssues(error);
  return new JsonRpcError(
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
