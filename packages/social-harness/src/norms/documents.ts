/**
 * @file Norm documents. A document names the norm and the roles of the member
 * who starts an instance and the others. Its acts follow either a FIPA
 * interaction protocol the document names, or turns the document gives each
 * act: who sends it, the facts that must and must not hold for it, the facts
 * it starts and ends, and how often it may occur. It gives each performative
 * a form, in the MCP form-mode grammar the collective layer uses, and states
 * English rules, each tied to the acts it governs. Documents use the act
 * syntax and parser, with `;` comments.
 */

import type { ASTNode, Expression } from "@thi.ng/sexpr/api";
import { parse } from "@thi.ng/sexpr/parse";
import { Data, Either, Option, ParseResult, Schema } from "effect";
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
 * When one act of a norm's own turns may be sent, and how it changes the
 * facts that hold in its instance. Facts are names the norm chooses. The act
 * ends its `terminates` facts, then starts its `initiates` facts.
 */
export interface Turn {
  /** The side that sends it: the member who started the instance, or another. */
  readonly by: "initiator" | "participant";
  /** Facts that must hold for it to be sent. */
  readonly when: readonly string[];
  /** Facts that must not hold for it to be sent. */
  readonly unless: readonly string[];
  readonly initiates: readonly string[];
  readonly terminates: readonly string[];
  /** The most times it may occur in one instance. */
  readonly max?: number;
}

/** A content form's turn keywords as written, before its `:by` role names a side. */
interface WrittenTurn extends Omit<Turn, "by"> {
  readonly by?: string;
}

const turnKeywords = ["by", "when", "unless", "initiates", "terminates", "max"];

/** A norm's own turns, by performative. */
export type Turns = Readonly<Record<string, Turn>>;

/**
 * One parsed norm. A norm with neither a protocol nor its own turns, such as
 * `chat`, has acts that stand alone. `source` is the document as written,
 * which norm discovery sends to peers.
 */
export interface Norm {
  readonly name: string;
  readonly protocol?: ProtocolName;
  readonly turns?: Turns;
  readonly roles?: { readonly initiator: string; readonly participant: string };
  readonly description: string;
  readonly contents: Readonly<Record<string, RequestedSchema>>;
  readonly rules: readonly NormRule[];
  readonly source: string;
}

/** One English rule and the performatives of the acts it governs. */
interface NormRule {
  readonly acts: readonly string[];
  readonly text: string;
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

/** A keyword's value in a document form: text, a number, or a list of items. */
type OptionValue = string | number | readonly string[];

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
      const performatives = contents.map(({ performative }) => performative);
      if (new Set(performatives).size !== performatives.length) {
        return Either.left(
          "each performative has at most one (content ...) form",
        );
      }
      const ungoverned = rules
        .flatMap(({ acts }) => acts)
        .find((act) => !performatives.includes(act));
      if (ungoverned !== undefined) {
        return Either.left(
          `a rule governs ${ungoverned}, which has no (content ...) form`,
        );
      }
      return turnsOf(header, contents).pipe(
        Either.map((turns) => ({
          ...header,
          ...(turns === undefined ? {} : { turns }),
          contents: Object.fromEntries(
            contents.map(({ performative, schema }) => [performative, schema]),
          ),
          rules,
          source,
        })),
      );
    }),
  );
}

/** One `(content ...)` form: its performative, its form, and its turn if it gives one. */
interface Content {
  readonly performative: string;
  readonly schema: RequestedSchema;
  readonly turn?: WrittenTurn;
}

/**
 * The norm's own turns, from the keywords its content forms give. A norm that
 * gives turns names no protocol and has both roles. Every act but `cancel`,
 * which any member may send to end the instance, gives `:by` one of those
 * roles. An act the initiator sends with no `:when` facts can start an
 * instance, and there is at least one. Every fact an act names is one that
 * some act initiates.
 */
function turnsOf(
  header: Omit<Norm, "contents" | "rules" | "source">,
  contents: readonly Content[],
): Either.Either<Turns | undefined, string> {
  const subject = `norm ${header.name}`;
  if (contents.every(({ turn }) => turn === undefined)) {
    return header.protocol === undefined && header.roles !== undefined
      ? Either.left(
          `${subject} names roles, so it names a protocol or gives its acts :by`,
        )
      : Either.right(undefined);
  }
  const { roles } = header;
  if (header.protocol !== undefined || roles === undefined) {
    return Either.left(
      `${subject} gives its acts turns, so it names :initiator and :participant and no :protocol`,
    );
  }
  const cancel = contents.find(({ performative }) => performative === "cancel");
  if (cancel?.turn !== undefined) {
    return Either.left(
      "content cancel takes no turn keywords, since any member may cancel an instance",
    );
  }
  return Either.all(
    contents.flatMap(({ performative, turn }) =>
      performative === "cancel" ? [] : [turnOf(performative, roles, turn)],
    ),
  ).pipe(Either.flatMap((entries) => connected(subject, roles, entries)));
}

/** One act's turn, with its `:by` role read as the side that sends it. */
function turnOf(
  performative: string,
  roles: NonNullable<Norm["roles"]>,
  written?: WrittenTurn,
): Either.Either<readonly [string, Turn], string> {
  const sides = new Map<string, Turn["by"]>([
    [roles.initiator, "initiator"],
    [roles.participant, "participant"],
  ]);
  const by = written?.by === undefined ? undefined : sides.get(written.by);
  if (written === undefined || by === undefined) {
    return Either.left(
      `content ${performative} gives :by ${roles.initiator} or :by ${roles.participant}`,
    );
  }
  const entry: readonly [string, Turn] = [performative, { ...written, by }];
  return Either.right(entry);
}

/** The turns, once some act starts an instance and every fact an act names is initiated. */
function connected(
  subject: string,
  roles: NonNullable<Norm["roles"]>,
  entries: ReadonlyArray<readonly [string, Turn]>,
): Either.Either<Turns, string> {
  const turns = entries.map(([, turn]) => turn);
  if (!turns.some(({ by, when }) => by === "initiator" && when.length === 0)) {
    return Either.left(
      `${subject} has an act :by ${roles.initiator} without :when, which starts an instance`,
    );
  }
  const initiated = new Set(turns.flatMap(({ initiates }) => initiates));
  const unknown = turns
    .flatMap(({ when, unless, terminates }) => [
      ...when,
      ...unless,
      ...terminates,
    ])
    .find((fact) => !initiated.has(fact));
  return unknown === undefined
    ? Either.right(Object.fromEntries(entries))
    : Either.left(
        `${subject} names the fact ${unknown}, which no act initiates`,
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
      const roles =
        typeof initiator === "string" &&
        typeof participant === "string" &&
        initiator !== participant
          ? { initiator, participant }
          : undefined;
      if ((initiator ?? participant) !== undefined && roles === undefined) {
        return Either.left(
          `norm ${normName} names two different roles, :initiator and :participant`,
        );
      }
      if (protocol === undefined) {
        return Either.right({
          name: normName,
          description,
          ...(roles === undefined ? {} : { roles }),
        });
      }
      const named = protocolNames.find((known) => known === protocol);
      if (named === undefined || roles === undefined) {
        return Either.left(
          `norm ${normName} names a FIPA protocol (${protocolNames.join(", ")}) with :initiator and :participant roles`,
        );
      }
      return Either.right({
        name: normName,
        description,
        protocol: named,
        roles,
      });
    }),
  );
}

/**
 * Reads a `(content ...)` form: a performative, then its turn keywords for a
 * norm that gives its acts turns, then one `(field ...)` form per field.
 */
function contentOf(form: Expression): Either.Either<Content, string> {
  const [, label, ...rest] = form.children;
  const performative = nameOf(label);
  if (performative === undefined) {
    return Either.left("(content ...) starts with a performative");
  }
  const split = keywordCount(rest);
  const fields = rest.slice(split);
  return Either.all({
    turn: options(rest.slice(0, split), `content ${performative}`).pipe(
      Either.flatMap((given) => contentTurn(performative, given)),
    ),
    entries: Either.all(fields.map((field) => fieldOf(field, performative))),
  }).pipe(
    Either.flatMap(({ turn, entries }) =>
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
        Either.map((schema) => ({
          performative,
          schema,
          ...(turn === undefined ? {} : { turn }),
        })),
      ),
    ),
  );
}

/**
 * Reads a `(rule ...)` form: `:act` and the performative, or list of
 * performatives, of the acts it governs, then one English rule in quotes.
 */
function ruleOf(form: Expression): Either.Either<NormRule, string> {
  const [, ...parts] = form.children;
  const usage =
    '(rule :act <performative> "...") ties one English rule in quotes to the acts it governs';
  const split = keywordCount(parts);
  const [text, ...rest] = parts.slice(split);
  if (text?.type !== "str" || text.value.trim() === "" || rest.length !== 0) {
    return Either.left(usage);
  }
  return options(parts.slice(0, split), "rule").pipe(
    Either.flatMap((given) =>
      given.size === 1
        ? Either.fromOption(namesOf(given.get("act")), () => usage)
        : Either.left(usage),
    ),
    Either.filterOrLeft(
      (acts) => acts.length !== 0,
      () => usage,
    ),
    Either.map((acts) => ({ acts, text: text.value })),
  );
}

/** How many of `nodes`, from the start, are `:keyword value` pairs. */
function keywordCount(nodes: readonly ASTNode[]): number {
  const firstField = nodes.findIndex(
    (node, index) =>
      index % 2 === 0 && !(node.type === "sym" && node.value.startsWith(":")),
  );
  return firstField === -1 ? nodes.length : firstField;
}

/**
 * A content form's turn keywords, if it gives any: `:by` and a role, the
 * fact lists `:when`, `:unless`, `:initiates` and `:terminates`, and `:max`
 * and a count.
 */
function contentTurn(
  performative: string,
  given: ReadonlyMap<string, OptionValue>,
): Either.Either<WrittenTurn | undefined, string> {
  const usage = `content ${performative} takes :by <role>, :when, :unless, :initiates and :terminates (<fact> ...), and :max <count>`;
  const by = given.get("by");
  const max = given.get("max");
  const countable =
    max === undefined ||
    (typeof max === "number" && Number.isInteger(max) && max >= 1);
  const known = [...given.keys()].every((key) => turnKeywords.includes(key));
  if (!known || !countable || (by !== undefined && typeof by !== "string")) {
    return Either.left(usage);
  }
  if (given.size === 0) {
    return Either.right(undefined);
  }
  return Option.all({
    when: namesOf(given.get("when")),
    unless: namesOf(given.get("unless")),
    initiates: namesOf(given.get("initiates")),
    terminates: namesOf(given.get("terminates")),
  }).pipe(
    Either.fromOption(() => usage),
    Either.map((facts) => ({
      ...facts,
      ...(by === undefined ? {} : { by }),
      ...(max === undefined ? {} : { max }),
    })),
  );
}

/**
 * A keyword's value, one name or a list of names, as a list: the facts of a
 * turn keyword or the performatives a rule governs.
 */
function namesOf(value?: OptionValue): Option.Option<readonly string[]> {
  if (typeof value === "number") {
    return Option.none();
  }
  const names = typeof value === "string" ? [value] : (value ?? []);
  return names.every((each) => name.test(each))
    ? Option.some(names)
    : Option.none();
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

/** The keyword arguments of one form, by name without its colon. */
function options(
  pairs: readonly ASTNode[],
  subject: string,
): Either.Either<ReadonlyMap<string, OptionValue>, string> {
  const given = new Map<string, OptionValue>();
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
function optionValue(node?: ASTNode): OptionValue | undefined {
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
