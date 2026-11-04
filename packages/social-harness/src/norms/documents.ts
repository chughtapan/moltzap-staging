/**
 * @file Norm documents. A document names the norm, the FIPA interaction
 * protocol its acts follow and the roles that protocol gives the initiator and
 * the participants. It gives each performative a form, in the MCP form-mode
 * grammar the collective layer uses, and states English rules. Documents use
 * the act syntax and parser, with `;` comments.
 */

import type { ASTNode, Expression } from "@thi.ng/sexpr/api";
import { parse } from "@thi.ng/sexpr/parse";
import { Data, Either, ParseResult, Schema } from "effect";
import { RequestedSchema } from "../communication/messages.js";

/** The FIPA interaction protocols a norm's acts can follow. */
const protocolNames = [
  "fipa-propose",
  "fipa-query",
  "fipa-request",
  "fipa-contract-net",
] as const;

/** One FIPA interaction protocol. */
export type ProtocolName = (typeof protocolNames)[number];

/**
 * One parsed norm. A norm without a protocol, such as `chat`, has acts that
 * stand alone. `source` is the document as written, which norm discovery
 * sends to peers.
 */
export interface Norm {
  readonly name: string;
  readonly protocol?: ProtocolName;
  readonly roles?: { readonly initiator: string; readonly participant: string };
  readonly description: string;
  readonly contents: Readonly<Record<string, RequestedSchema>>;
  readonly rules: readonly string[];
  readonly source: string;
}

/** A document that does not describe a norm, and where. */
export class NormDocumentError extends Data.TaggedError("NormDocumentError")<{
  readonly reason: string;
}> {
  override get message(): string {
    return `The norm document is invalid: ${this.reason}.`;
  }
}

const name = /^[a-z][a-z0-9-]{0,63}$/u;

const fieldTypes = new Set([
  "string",
  "number",
  "integer",
  "boolean",
  "select",
  "multi-select",
]);

/**
 * Reads one norm document: one `(norm ...)` form, a `(content ...)` form for
 * each performative that carries fields, and any `(rule ...)` forms.
 */
export function readNorm(
  source: string,
): Either.Either<Norm, NormDocumentError> {
  return Either.try({
    try: () => parse(source).children,
    catch: (error) =>
      error instanceof Error ? error.message : "the document does not parse",
  }).pipe(
    Either.flatMap((forms) => normOf(forms, source)),
    Either.mapLeft((reason) => new NormDocumentError({ reason })),
  );
}

function normOf(
  forms: readonly ASTNode[],
  source: string,
): Either.Either<Norm, string> {
  const named = forms.map((form) => ({ form, head: headOf(form) }));
  const of = (head: string) =>
    named.flatMap((entry) =>
      entry.head === head && entry.form.type === "expr" ? [entry.form] : [],
    );
  const [declaration, ...extra] = of("norm");
  if (declaration === undefined || extra.length !== 0) {
    return Either.left("a document has exactly one (norm ...) form");
  }
  if (
    named.some(({ head }) => !["norm", "content", "rule"].includes(head ?? ""))
  ) {
    return Either.left("each form is (norm ...), (content ...) or (rule ...)");
  }
  return Either.all({
    header: headerOf(declaration),
    contents: Either.all(of("content").map((form) => contentOf(form))),
    rules: Either.all(of("rule").map((form) => ruleOf(form))),
  }).pipe(
    Either.flatMap(({ header, contents, rules }) => {
      const performatives = contents.map(([performative]) => performative);
      return new Set(performatives).size === performatives.length
        ? Either.right({
            ...header,
            contents: Object.fromEntries(contents),
            rules,
            source,
          })
        : Either.left("each performative has at most one (content ...) form");
    }),
  );
}

/** The symbol a form opens with, such as `norm` or `content`. */
function headOf(form: ASTNode): string | undefined {
  const [head] = form.type === "expr" ? form.children : [];
  return head?.type === "sym" ? head.value : undefined;
}

/**
 * Reads the `(norm ...)` form: the norm's name, then `:description` and, for a
 * norm with a protocol, `:protocol`, `:initiator` and `:participant`.
 */
function headerOf(
  form: Expression,
): Either.Either<Omit<Norm, "contents" | "rules" | "source">, string> {
  const [, label, ...pairs] = form.children;
  const normName = nameOf(label);
  if (normName === undefined) {
    return Either.left("(norm ...) starts with the norm's lowercase name");
  }
  return options(pairs, `norm ${normName}`).pipe(
    Either.flatMap((given) => {
      const description = given.get("description");
      const protocol = given.get("protocol");
      const initiator = given.get("initiator");
      const participant = given.get("participant");
      const unknown = [...given.keys()].filter(
        (key) =>
          !["description", "protocol", "initiator", "participant"].includes(
            key,
          ),
      );
      if (unknown.length !== 0 || typeof description !== "string") {
        return Either.left(
          `norm ${normName} takes :description and optionally :protocol, :initiator and :participant`,
        );
      }
      if (protocol === undefined) {
        return Either.right({ name: normName, description });
      }
      const named = protocolNames.find((known) => known === protocol);
      if (
        named === undefined ||
        typeof initiator !== "string" ||
        typeof participant !== "string"
      ) {
        return Either.left(
          `norm ${normName} names a FIPA protocol (${protocolNames.join(", ")}) with :initiator and :participant roles`,
        );
      }
      return Either.right({
        name: normName,
        description,
        protocol: named,
        roles: { initiator, participant },
      });
    }),
  );
}

/** Reads a `(content ...)` form: a performative, then one `(field ...)` form per field. */
function contentOf(
  form: Expression,
): Either.Either<readonly [string, RequestedSchema], string> {
  const [, label, ...fields] = form.children;
  const performative = nameOf(label);
  if (performative === undefined) {
    return Either.left("(content ...) starts with a performative");
  }
  return Either.all(fields.map((field) => fieldOf(field, performative))).pipe(
    Either.flatMap((entries) =>
      Schema.decodeUnknownEither(RequestedSchema)({
        type: "object",
        properties: Object.fromEntries(
          entries.map(({ field, property }) => [field, property]),
        ),
        required: entries.flatMap(({ field, required }) =>
          required ? [field] : [],
        ),
      }).pipe(
        Either.mapLeft(
          (error) =>
            `content ${performative}: ${ParseResult.TreeFormatter.formatErrorSync(error)}`,
        ),
      ),
    ),
    Either.map((schema) => [performative, schema] as const),
  );
}

/**
 * Reads a `(field ...)` form, a name and a `:type`, as one MCP form property.
 * Every field takes `:description` and `:optional true`. `select` and
 * `multi-select` take `:choices (...)`, text takes `:min-length` and
 * `:max-length`, and numbers take `:minimum` and `:maximum`.
 */
function fieldOf(node: ASTNode, performative: string) {
  const [head, label, ...pairs] = node.type === "expr" ? node.children : [];
  const field = nameOf(label);
  if (head?.type !== "sym" || head.value !== "field" || field === undefined) {
    return Either.left(
      `content ${performative} lists (field <name> :type <type>) forms`,
    );
  }
  return options(pairs, `field ${field}`).pipe(
    Either.flatMap((given) => {
      const type = given.get("type");
      if (typeof type !== "string" || !fieldTypes.has(type)) {
        return Either.left(
          `field ${field} has :type ${[...fieldTypes].join(", ")}`,
        );
      }
      const choices = given.get("choices");
      const keywords: Record<string, unknown> = {
        ...(given.has("description")
          ? { description: given.get("description") }
          : {}),
        ...(given.has("min-length")
          ? { minLength: given.get("min-length") }
          : {}),
        ...(given.has("max-length")
          ? { maxLength: given.get("max-length") }
          : {}),
        ...(given.has("minimum") ? { minimum: given.get("minimum") } : {}),
        ...(given.has("maximum") ? { maximum: given.get("maximum") } : {}),
      };
      const property =
        type === "select" || type === "multi-select"
          ? selectProperty(type, choices, keywords)
          : { type, ...keywords };
      return Either.right({
        field,
        property,
        required: given.get("optional") !== "true",
      });
    }),
  );
}

function selectProperty(
  type: "select" | "multi-select",
  choices: unknown,
  keywords: Record<string, unknown>,
) {
  const options = Array.isArray(choices) ? choices : [];
  return type === "select"
    ? { type: "string", enum: options, ...keywords }
    : { type: "array", items: { type: "string", enum: options }, ...keywords };
}

/** Reads a `(rule ...)` form, which quotes one English rule. */
function ruleOf(form: Expression): Either.Either<string, string> {
  const [, text, ...rest] = form.children;
  return text?.type === "str" && text.value.trim() !== "" && rest.length === 0
    ? Either.right(text.value)
    : Either.left('(rule "...") holds one English rule in quotes');
}

/** The keyword arguments of one form, by name without its colon. */
function options(
  pairs: readonly ASTNode[],
  subject: string,
): Either.Either<
  ReadonlyMap<string, string | number | readonly string[]>,
  string
> {
  const given = new Map<string, string | number | readonly string[]>();
  for (let index = 0; index < pairs.length; index += 2) {
    const key = pairs[index];
    const keyword =
      key?.type === "sym" && key.value.startsWith(":")
        ? key.value.slice(1)
        : undefined;
    const parsed = optionValue(pairs[index + 1]);
    if (keyword === undefined || parsed === undefined || given.has(keyword)) {
      return Either.left(`${subject} takes distinct :keyword value pairs`);
    }
    given.set(keyword, parsed);
  }
  return Either.right(given);
}

/** A string, a number, or a list of them, which reads as strings. */
function optionValue(
  node?: ASTNode,
): string | number | readonly string[] | undefined {
  if (node === undefined) {
    return undefined;
  }
  if (node.type !== "expr") {
    return atom(node);
  }
  const items = node.children.flatMap((item) => {
    const value = atom(item);
    return value === undefined ? [] : [String(value)];
  });
  return items.length === node.children.length ? items : undefined;
}

/** A lowercase name: of a norm, a performative or a field. */
function nameOf(node?: ASTNode): string | undefined {
  if (node === undefined) {
    return undefined;
  }
  const value = atom(node);
  return typeof value === "string" && name.test(value) ? value : undefined;
}

function atom(node: ASTNode): string | number | undefined {
  return node.type === "str" || node.type === "sym" || node.type === "num"
    ? node.value
    : undefined;
}
