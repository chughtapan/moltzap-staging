/**
 * @file A norm as compiled: its acts' forms, turn rules and text rules. A
 * message is read as one act, written as parenthesized data, and the act is
 * fitted to the form its norm declares for it.
 */

import { Either, Schema, Struct } from "effect";
import type { Message } from "../messages.js";
import { TextRule } from "../firewall/index.js";

const name = Schema.String.pipe(
  Schema.pattern(/^[a-zA-Z][a-zA-Z0-9-]{0,63}$/u),
);
const names = Schema.Array(name);
const actReference = Schema.Union(name, Schema.Struct({ fact: name }));

const bound = Schema.optionalWith(Schema.Number.pipe(Schema.finite()), {
  exact: true,
});

/** What every field carries. A description explains a field; it does not constrain its value. */
const described = {
  name,
  optional: Schema.Boolean,
  description: Schema.optionalWith(Schema.String, { exact: true }),
};

/**
 * One field of a form: a string, an integer or a selection of several listed
 * strings, with its bounds. An act must state it unless it is optional.
 */
const fieldSchema = Schema.Union(
  Schema.Struct({
    ...described,
    type: Schema.Literal("string"),
    minLength: bound,
    maxLength: bound,
  }),
  Schema.Struct({
    ...described,
    type: Schema.Literal("integer"),
    minimum: bound,
    maximum: bound,
  }),
  Schema.Struct({
    ...described,
    type: Schema.Literal("multi-select"),
    choices: Schema.Array(Schema.String),
    minItems: bound,
    maxItems: bound,
  }),
);

const fieldSource = Schema.Struct({
  kind: Schema.Literal("field"),
  act: actReference,
  field: name,
});
const sourceSchema = Schema.Union(
  fieldSource,
  Schema.Struct({
    kind: Schema.Literal("present"),
    act: actReference,
    fields: names,
  }),
  Schema.Struct({
    kind: Schema.Literal("values"),
    act: actReference,
    keys: fieldSource,
  }),
  Schema.Struct({
    kind: Schema.Literal("field-values"),
    act: actReference,
    fields: Schema.NonEmptyArray(name),
  }),
);
const formSchema = Schema.Struct({
  act: name,
  by: Schema.Union(
    names,
    Schema.Struct({ recipient: Schema.Struct({ fact: name }) }),
  ),
  fields: Schema.Array(fieldSchema),
  starts: Schema.Boolean,
  ends: Schema.Boolean,
  when: names,
  initiates: names,
  terminates: names,
  maximum: Schema.optional(Schema.Positive.pipe(Schema.int())),
  delay: Schema.optional(Schema.Struct({ fact: name, field: name })),
  relations: Schema.Array(
    Schema.Struct({
      operator: Schema.Literal("equals", "member", "subset"),
      field: name,
      source: sourceSchema,
    }),
  ),
});

/**
 * The form of one performative: its fields, who may send it, and the facts it
 * needs, sets and clears.
 */
export type MessageForm = typeof formSchema.Type;

/**
 * One norm as compiled. `digest` is the SHA-256 of `source`, the norm document
 * it was compiled from. A text rule's `act` names the one performative it
 * governs; a rule without it governs every act of the norm.
 */
const normSchema = Schema.Struct({
  name,
  digest: Schema.String.pipe(Schema.pattern(/^[a-f0-9]{64}$/u)),
  source: Schema.String,
  description: Schema.String,
  roles: Schema.Struct({ initiator: name, participant: name }),
  forms: Schema.Array(formSchema),
  textRules: Schema.Array(
    Schema.Struct({
      ...Struct.pick(TextRule.fields, "id", "text", "feedback"),
      act: Schema.optional(name),
    }),
  ),
  onRefusal: Schema.optional(name),
});

/** A compiled norm: data messages are checked against, with no code of its own. */
export type Norm = typeof normSchema.Type;

/** Reads a value as a norm, or says where it is not one. */
export const decodeNorm = Schema.decodeUnknownEither(normSchema);

/** The settings' copy of a norms directory: its compiled norms. */
export type InstalledNorms = readonly Norm[];

/**
 * One act as a message states it: its performative in `name`, the norm its
 * `:ontology` names and its fields, each with the type its text gave it.
 */
export interface Act {
  readonly name: string;
  readonly norm: string;
  readonly fields: Readonly<
    Record<string, string | number | readonly string[]>
  >;
}

/** Why a message fails its norm: the stage that failed, and the text its sender reads. */
export interface NormProblem {
  readonly stage: "structural" | "protocol";
  readonly reason: "rule-violation" | "unresolved-evidence";
  readonly detail: string;
  readonly field?: string;
}

/**
 * One value of parenthesized data, as norm documents and acts are written: a
 * symbol, a quoted string or a list. It is inert: nothing in it is evaluated.
 */
export type SExpression =
  | { readonly kind: "symbol"; readonly value: string }
  | { readonly kind: "string"; readonly value: string }
  | { readonly kind: "list"; readonly values: readonly SExpression[] };

/** Why text is not parenthesized data, with the offset of the token at fault. */
interface SExpressionError {
  readonly offset: number;
  readonly detail: string;
}

/** Whether the norms have English rules, which need a decision provider. */
export function initialNormsHaveRules(installed: InstalledNorms): boolean {
  return installed.some((norm) => norm.textRules.length > 0);
}

/** Reads a message as one act: the whole text is one parenthesized act. */
export function readAct(message: Message): Either.Either<Act, NormProblem> {
  return readSExpressions(message.text, false).pipe(
    Either.mapLeft((error) => structural(error.detail)),
    Either.flatMap(([form, ...extra]) => {
      if (form?.kind !== "list" || extra.length !== 0) {
        return Either.left(
          structural(
            "Error: Reply does not contain exactly one parenthesized speech act.",
          ),
        );
      }
      const [head, ...parameters] = form.values;
      return head?.kind !== "symbol" ||
        !/^[a-z][a-z0-9-]{0,63}$/u.test(head.value) ||
        parameters.length % 2 !== 0
        ? Either.left(
            structural(
              "An act names a performative followed by :field value pairs.",
            ),
          )
        : readParameters(head.value, parameters);
    }),
  );
}

/** The form `norm` declares for `act`, once the act's fields fit it. */
export function checkStructure(
  norm: Norm,
  act: Act,
): Either.Either<MessageForm, NormProblem> {
  const form = norm.forms.find((entry) => entry.act === act.name);
  if (norm.name !== act.norm) {
    return Either.left({
      stage: "protocol",
      reason: "rule-violation",
      detail: `Error: ${act.name} is not valid for the active norm ${norm.name}.`,
    });
  }
  if (form === undefined) {
    return Either.left(
      structural(`Error: ${act.name} is not a valid act in ${act.norm}.`),
    );
  }
  const failures = [
    ...form.fields.flatMap((field) => {
      const value = Object.hasOwn(act.fields, field.name)
        ? act.fields[field.name]
        : undefined;
      if (value === undefined) {
        return field.optional
          ? []
          : [
              `Error: ${field.name} is required for ${act.name} in ${act.norm}.`,
            ];
      }
      return accepts(field, value)
        ? []
        : [`Error: ${field.name} is not valid for ${act.name} in ${act.norm}.`];
    }),
    ...Object.keys(act.fields)
      .filter((stated) => form.fields.every((field) => field.name !== stated))
      .map(
        (stated) =>
          `Error: ${stated} is not a valid field of ${act.name} in ${act.norm}.`,
      ),
  ];
  return failures.length === 0
    ? Either.right(form)
    : Either.left(structural(failures.join(" ")));
}

/** An act as message text, with strings quoted as a norm document quotes them. */
export function formatAct(act: Act): string {
  const values: SExpression[] = [
    { kind: "symbol", value: act.name },
    { kind: "symbol", value: ":ontology" },
    { kind: "symbol", value: act.norm },
  ];
  for (const [field, value] of Object.entries(act.fields)) {
    values.push({ kind: "symbol", value: `:${field}` }, valueExpression(value));
  }
  return writeSExpression({ kind: "list", values });
}

/**
 * Reads every expression of `source`. A document may carry `;` comments; a
 * reader of a message passes `comments` false, so that every token of the
 * text is accounted for.
 */
export function readSExpressions(
  source: string,
  comments = true,
): Either.Either<readonly SExpression[], SExpressionError> {
  const roots: SExpression[] = [];
  const stack: SExpression[][] = [roots];
  const tokens = /\s+|;[^\r\n]*|[()]|"(?:[^"\\]|\\[\s\S])*"|[^\s();"]+/uy;
  let offset = 0;
  while (offset < source.length) {
    tokens.lastIndex = offset;
    const token = tokens.exec(source)?.[0];
    const refusal =
      token === undefined
        ? "Expected a complete token."
        : acceptToken(token, stack, comments);
    if (refusal !== undefined) {
      return Either.left({ offset, detail: refusal });
    }
    offset = tokens.lastIndex;
  }
  return stack.length === 1
    ? Either.right(roots)
    : Either.left({ offset, detail: "An opening parenthesis is not closed." });
}

/** The text of one expression, with strings quoted as JSON, which `readSExpressions` reads back. */
export function writeSExpression(value: SExpression): string {
  switch (value.kind) {
    case "symbol":
      return value.value;
    case "string":
      return JSON.stringify(value.value);
    case "list":
      return `(${value.values.map((item) => writeSExpression(item)).join(" ")})`;
    default: {
      const exhaustive: never = value;
      return exhaustive;
    }
  }
}

/** Whether `value` has the type of `field` and lies within its bounds. A string's length counts code points. */
function accepts(
  field: MessageForm["fields"][number],
  value: Act["fields"][string],
): boolean {
  switch (field.type) {
    case "string":
      return (
        typeof value === "string" &&
        within(Array.from(value).length, field.minLength, field.maxLength)
      );
    case "integer":
      return (
        typeof value === "number" &&
        Number.isInteger(value) &&
        within(value, field.minimum, field.maximum)
      );
    case "multi-select":
      return (
        typeof value === "object" &&
        value.every((choice) => field.choices.includes(choice)) &&
        within(value.length, field.minItems, field.maxItems)
      );
    default: {
      const exhaustive: never = field;
      return exhaustive;
    }
  }
}

function within(value: number, minimum?: number, maximum?: number) {
  return (
    (minimum === undefined || value >= minimum) &&
    (maximum === undefined || value <= maximum)
  );
}

function readParameters(
  performative: string,
  parameters: readonly SExpression[],
): Either.Either<Act, NormProblem> {
  return Either.gen(function* () {
    const fields = new Map<string, Act["fields"][string]>();
    for (let index = 0; index < parameters.length; index += 2) {
      const key = parameters[index];
      const value = parameters[index + 1];
      if (
        key?.kind !== "symbol" ||
        !/^:[a-z][a-z0-9-]{0,63}$/u.test(key.value) ||
        value === undefined
      ) {
        return yield* Either.left(
          structural(
            "Parameters must have distinct :names and one value each.",
          ),
        );
      }
      const field = key.value.slice(1);
      if (fields.has(field)) {
        return yield* Either.left(
          structural(`Error: ${field} appears more than once.`, field),
        );
      }
      fields.set(field, yield* readValue(value));
    }
    const norm = fields.get("ontology");
    if (typeof norm !== "string") {
      return yield* Either.left(
        structural(":ontology must name an installed norm.", "ontology"),
      );
    }
    fields.delete("ontology");
    return { name: performative, norm, fields: Object.fromEntries(fields) };
  });
}

function readValue(
  node: SExpression,
): Either.Either<Act["fields"][string], NormProblem> {
  if (node.kind === "string") {
    return Either.right(node.value);
  }
  if (node.kind === "list") {
    const choices = node.values.flatMap((choice) =>
      choice.kind === "list" ? [] : [choice.value],
    );
    return choices.length === node.values.length
      ? Either.right(choices)
      : Either.left(structural("A list field contains scalar choices."));
  }
  if (!/^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?$/u.test(node.value)) {
    return Either.right(node.value);
  }
  return Number.isFinite(Number(node.value))
    ? Either.right(Number(node.value))
    : Either.left(structural("Numbers must be finite."));
}

function valueExpression(value: Act["fields"][string]): SExpression {
  if (typeof value === "string") {
    return { kind: "string", value };
  }
  if (typeof value === "number") {
    return { kind: "symbol", value: String(value) };
  }
  return {
    kind: "list",
    values: value.map((choice) => ({ kind: "string", value: choice })),
  };
}

function structural(detail: string, field?: string): NormProblem {
  return {
    stage: "structural",
    reason: "rule-violation",
    detail,
    ...(field === undefined ? {} : { field }),
  };
}

/** Adds one token to the innermost open list, and gives why the token cannot stand where it does. */
function acceptToken(
  token: string,
  stack: SExpression[][],
  comments: boolean,
): string | undefined {
  const parent = stack.at(-1);
  if (parent === undefined || /^\s/u.test(token)) {
    return undefined;
  }
  if (token.startsWith(";")) {
    return comments
      ? undefined
      : "A message cannot carry a comment outside its declared fields.";
  }
  if (token === "(") {
    const values: SExpression[] = [];
    parent.push({ kind: "list", values });
    stack.push(values);
    return undefined;
  }
  if (token === ")") {
    if (stack.length === 1) {
      return "Unexpected closing parenthesis.";
    }
    stack.pop();
    return undefined;
  }
  if (!token.startsWith('"')) {
    parent.push({ kind: "symbol", value: token });
    return undefined;
  }
  const value = Either.getOrUndefined(
    Either.try((): unknown => JSON.parse(token)),
  );
  if (value === undefined) {
    return "Use JSON escaping inside quoted strings.";
  }
  if (typeof value !== "string" || /[\uD800-\uDFFF]/u.test(value)) {
    return "Expected a well-formed Unicode string.";
  }
  parent.push({ kind: "string", value });
  return undefined;
}
