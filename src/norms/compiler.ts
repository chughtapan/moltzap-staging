/**
 * @file Compiles norm documents into norms: reads a document's declarations,
 * builds the norm they state and checks that its names and protocol
 * predicates refer to declared things. A norms directory loads as the norms
 * of its documents.
 */

import { FileSystem } from "@effect/platform";
import { Data, Effect, Either, Order } from "effect";
import { createHash } from "node:crypto";
import { join } from "node:path";
import {
  decodeNorm,
  type InstalledNorms,
  type MessageForm,
  type Norm,
  readSExpressions,
  type SExpression,
} from "./norm.js";

/** A compiled value, or why the document is not a norm, in words that do not echo the document's text. */
type Compiled<Value> = Either.Either<Value, string>;

/** A declaration separates named attributes from its child forms and text. */
interface Declaration {
  readonly kind: string;
  readonly name?: string;
  readonly attributes: ReadonlyMap<string, SExpression>;
  readonly children: readonly SExpression[];
}

/** The attributes a field of each type takes besides those every field takes. */
const attributes: Readonly<Record<string, readonly string[]>> = {
  string: [":min-length", ":max-length"],
  integer: [":minimum", ":maximum"],
  "multi-select": [":choices", ":min-items", ":max-items"],
};

/** A norms directory the server cannot use, and why. */
export class NormsConfigurationError extends Data.TaggedError(
  "NormsConfigurationError",
)<{ readonly reason: string }> {
  override get message(): string {
    return `The norms directory is invalid: ${this.reason}`;
  }
}

/**
 * Compiles every `.lisp` norm document directly in `directory`, in name
 * order. Norm names are unique.
 */
export const loadNorms = Effect.fn("loadNorms")(
  function* (directory: string) {
    const fs = yield* FileSystem.FileSystem;
    const files = yield* fs.readDirectory(directory);
    const norms: InstalledNorms = yield* Effect.forEach(
      files.filter((file) => file.endsWith(".lisp")).sort(Order.string),
      (file) =>
        fs
          .readFileString(join(directory, file))
          .pipe(
            Effect.flatMap((source) =>
              Either.mapLeft(
                compileNorm(source),
                (reason) => new NormsConfigurationError({ reason }),
              ),
            ),
          ),
      { concurrency: 1 },
    );
    if (new Set(norms.map((norm) => norm.name)).size !== norms.length) {
      return yield* new NormsConfigurationError({
        reason: "two documents name the same norm",
      });
    }
    return norms;
  },
  Effect.catchTags({
    BadArgument: (error) =>
      Effect.fail(new NormsConfigurationError({ reason: error.message })),
    SystemError: (error) =>
      Effect.fail(new NormsConfigurationError({ reason: error.message })),
  }),
);

/** The norm `source` declares. Its digest names the exact document bytes supplied to both participants. */
export function compileNorm(source: string): Compiled<Norm> {
  return Either.gen(function* () {
    const { header, body } = yield* readDocument(source);
    yield* checkAttributes(header, [
      ":initiator",
      ":participant",
      ":description",
      ":on-refusal",
    ]);
    const initiator = atom(header.attributes.get(":initiator"));
    const participant = atom(header.attributes.get(":participant"));
    if (
      header.name === undefined ||
      !/^[a-z][a-z0-9-]{0,63}$/u.test(header.name)
    ) {
      return yield* Either.left(
        "A norm names two distinct roles and a lowercase norm name.",
      );
    }
    if (
      initiator === undefined ||
      participant === undefined ||
      initiator === participant ||
      header.children.length !== 0
    ) {
      return yield* Either.left(
        "A norm names two distinct roles without child declarations.",
      );
    }
    const onRefusal = atom(header.attributes.get(":on-refusal"));
    const norm = yield* decodeNorm({
      name: header.name,
      digest: createHash("sha256").update(source, "utf8").digest("hex"),
      source,
      description: atom(header.attributes.get(":description")) ?? "",
      roles: { initiator, participant },
      forms: yield* Either.all(
        body.filter((form) => form.kind === "content").map(compileForm),
      ),
      textRules: yield* Either.all(
        body
          .filter((form) => form.kind === "rule")
          .map((form, index) =>
            compileRule(form, `${header.name}-rule-${index + 1}`),
          ),
      ),
      ...(onRefusal === undefined ? {} : { onRefusal }),
    }).pipe(
      Either.mapLeft(
        () => "A declaration has an invalid name, value, bound or rule scope.",
      ),
    );
    yield* validateReferences(norm);
    return norm;
  });
}

function compileForm(form: Declaration) {
  return Either.gen(function* () {
    yield* checkAttributes(form, [
      ":by",
      ":starts",
      ":ends",
      ":when",
      ":initiates",
      ":terminates",
      ":max",
      ":after",
      ":delay",
      ":unit",
    ]);
    const children = yield* Either.all(form.children.map(readDeclaration));
    if (
      children.some(
        (child) => child.kind !== "field" && child.kind !== "relation",
      )
    ) {
      return yield* Either.left(
        "Content declarations contain fields and relations.",
      );
    }
    const fields = yield* Either.all(
      children.filter((child) => child.kind === "field").map(compileField),
    );
    if (new Set(fields.map((field) => field.name)).size !== fields.length) {
      return yield* Either.left("Field names must be unique within a form.");
    }
    const maximum = yield* numberAttribute(form, ":max");
    const delay = yield* compileDelay(form);
    return {
      act: form.name ?? "",
      by: yield* compileSender(form),
      fields,
      starts: yield* booleanAttribute(form, ":starts"),
      ends: yield* booleanAttribute(form, ":ends"),
      when: yield* names(form.attributes.get(":when")),
      initiates: yield* names(form.attributes.get(":initiates")),
      terminates: yield* names(form.attributes.get(":terminates")),
      ...(maximum === undefined ? {} : { maximum }),
      ...(delay === undefined ? {} : { delay }),
      relations: yield* Either.all(
        children
          .filter((child) => child.kind === "relation")
          .map(compileRelation),
      ),
    };
  });
}

function compileSender(form: Declaration): Compiled<MessageForm["by"]> {
  const node = form.attributes.get(":by");
  if (
    node?.kind !== "list" ||
    atom(node.values[0]) !== "recipient" ||
    node.values[1]?.kind !== "list"
  ) {
    return names(node);
  }
  const reference = node.values[1];
  const fact =
    reference.values.length === 2 && atom(reference.values[0]) === "fact"
      ? atom(reference.values[1])
      : undefined;
  return node.values.length !== 2 || fact === undefined
    ? Either.left("A recipient sender selector names one fact.")
    : Either.right({ recipient: { fact } });
}

function compileRule(form: Declaration, id: string) {
  return Either.gen(function* () {
    yield* checkAttributes(form, [":act", ":feedback"]);
    const [text, ...rest] = form.children;
    if (text?.kind !== "string" || rest.length !== 0) {
      return yield* Either.left(
        "An English rule contains one quoted rule text.",
      );
    }
    const act = atom(form.attributes.get(":act"));
    const feedback = atom(form.attributes.get(":feedback"));
    return {
      id,
      text: text.value,
      ...(act === undefined ? {} : { act }),
      ...(feedback === undefined ? {} : { feedback }),
    };
  });
}

function compileRelation(form: Declaration) {
  return Either.gen(function* () {
    yield* checkAttributes(form, []);
    const [field, source, ...rest] = form.children;
    if (field?.kind !== "symbol" || source === undefined || rest.length !== 0) {
      return yield* Either.left(
        "A relation names its operator, candidate field and value source.",
      );
    }
    return {
      operator: form.name,
      field: field.value,
      source: yield* compileSource(source),
    };
  });
}

type Source = MessageForm["relations"][number]["source"];

function compileSource(node: SExpression): Compiled<Source> {
  return Either.gen(function* () {
    if (node.kind !== "list") {
      return yield* Either.left(
        "A value source is a previous field, present fields, or selected values.",
      );
    }
    const [head, act, ...rest] = node.values;
    if (act === undefined) {
      return yield* Either.left("An act reference names one act or one fact.");
    }
    const name = yield* compileActReference(act);
    if (
      atom(head) === "previous" &&
      rest.length === 1 &&
      atom(rest[0]) !== undefined
    ) {
      return { kind: "field", act: name, field: atom(rest[0]) ?? "" };
    }
    if (atom(head) === "present") {
      return {
        kind: "present",
        act: name,
        fields: yield* names({ kind: "list", values: rest }),
      };
    }
    if (atom(head) === "values") {
      if (rest.length === 1 && rest[0]?.kind === "list") {
        const keys = yield* compileSource(rest[0]);
        if (keys.kind === "field") {
          return { kind: "values", act: name, keys };
        }
      } else {
        const [first, ...others] = yield* names({ kind: "list", values: rest });
        if (first !== undefined) {
          return {
            kind: "field-values",
            act: name,
            fields: [first, ...others],
          };
        }
      }
    }
    return yield* Either.left("Invalid value source.");
  });
}

/** Relations may read the same fields from alternative forms of a referenced exchange. */
function compileActReference(node: SExpression): Compiled<Source["act"]> {
  if (node.kind === "list" && atom(node.values[0]) === "fact") {
    const fact = atom(node.values[1]);
    return node.values.length !== 2 || fact === undefined
      ? Either.left("A fact source names one declared fact.")
      : Either.right({ fact });
  }
  const act = atom(node);
  return act === undefined
    ? Either.left("An act reference names one act or one fact.")
    : Either.right(act);
}

function compileDelay(form: Declaration) {
  const fact = atom(form.attributes.get(":after"));
  const field = atom(form.attributes.get(":delay"));
  const unit = atom(form.attributes.get(":unit"));
  if (fact === undefined && field === undefined && unit === undefined) {
    return Either.right(undefined);
  }
  return fact === undefined || field === undefined || unit !== "seconds"
    ? Either.left("A delay supplies :after, :delay and :unit seconds.")
    : Either.right({ fact, field });
}

function compileField(form: Declaration) {
  return Either.gen(function* () {
    const type = atom(form.attributes.get(":type")) ?? "";
    const allowed = Object.hasOwn(attributes, type)
      ? attributes[type]
      : undefined;
    if (
      allowed === undefined ||
      form.name === undefined ||
      !/^[a-z][a-z0-9-]{0,63}$/u.test(form.name)
    ) {
      return yield* Either.left(
        "A field needs a lowercase name and a supported primitive type.",
      );
    }
    yield* checkAttributes(form, [
      ":type",
      ":optional",
      ":description",
      ...allowed,
    ]);
    if (form.children.length !== 0 || form.name === "ontology") {
      return yield* Either.left(
        "A field cannot redefine envelope metadata or contain child declarations.",
      );
    }
    const choices = yield* names(form.attributes.get(":choices"));
    if (
      type === "multi-select" &&
      (choices.length === 0 || new Set(choices).size !== choices.length)
    ) {
      return yield* Either.left(
        "Choice fields need distinct, nonempty :choices.",
      );
    }
    const description = atom(form.attributes.get(":description"));
    return {
      name: form.name,
      optional: yield* booleanAttribute(form, ":optional"),
      type,
      ...(type === "multi-select" ? { choices } : {}),
      ...(yield* readBounds(form)),
      ...(description === undefined ? {} : { description }),
    };
  });
}

function readBounds(form: Declaration) {
  return Either.gen(function* () {
    const bounds: Record<string, number> = {};
    for (const [attribute, key] of [
      [":min-length", "minLength"],
      [":max-length", "maxLength"],
      [":minimum", "minimum"],
      [":maximum", "maximum"],
      [":min-items", "minItems"],
      [":max-items", "maxItems"],
    ] as const) {
      const value = yield* numberAttribute(form, attribute);
      if (value !== undefined) {
        bounds[key] = value;
      }
    }
    for (const [minimum, maximum] of [
      ["minLength", "maxLength"],
      ["minItems", "maxItems"],
      ["minimum", "maximum"],
    ] as const) {
      const low = bounds[minimum];
      const high = bounds[maximum];
      const ordered = low === undefined || high === undefined || low <= high;
      const integral =
        minimum === "minimum" ||
        [low, high].every(
          (value) =>
            value === undefined || (Number.isInteger(value) && value >= 0),
        );
      if (!ordered || !integral) {
        return yield* Either.left(
          "Field bounds must be ordered; length and item bounds are nonnegative integers.",
        );
      }
    }
    return bounds;
  });
}

/** All declarations are data; a document has one leading norm header. */
function readDocument(source: string) {
  return Either.gen(function* () {
    const parsed = yield* readSExpressions(source).pipe(
      Either.mapLeft((error) => `${error.detail} Offset ${error.offset}.`),
    );
    const [header, ...body] = yield* Either.all(parsed.map(readDeclaration));
    if (
      header?.kind !== "norm" ||
      body.some((item) => item.kind !== "content" && item.kind !== "rule")
    ) {
      return yield* Either.left(
        "A document starts with one norm declaration followed by content and rule declarations.",
      );
    }
    return { header, body };
  });
}

/** Attribute names are checked by the declaration that consumes them. */
function readDeclaration(node: SExpression): Compiled<Declaration> {
  if (node.kind !== "list") {
    return Either.left("Expected a parenthesized declaration.");
  }
  const [head, ...tail] = node.values;
  if (head?.kind !== "symbol") {
    return Either.left("A declaration starts with a symbol.");
  }
  const named = ["norm", "content", "field", "relation"].includes(head.value);
  const first = tail[0];
  if (named && (first?.kind !== "symbol" || first.value.startsWith(":"))) {
    return Either.left("The declaration needs a name.");
  }
  return Either.map(
    declarationFields(named ? tail.slice(1) : tail),
    (fields): Declaration => ({
      kind: head.value,
      ...(named && first?.kind === "symbol" ? { name: first.value } : {}),
      ...fields,
    }),
  );
}

/** A declaration's items after its name: each `:attribute` with the value that follows it, and the rest as children. */
function declarationFields(
  items: readonly SExpression[],
): Compiled<Pick<Declaration, "attributes" | "children">> {
  const attributes = new Map<string, SExpression>();
  const children: SExpression[] = [];
  for (let index = 0; index < items.length; index += 1) {
    const item = items[index];
    if (item?.kind === "symbol" && item.value.startsWith(":")) {
      const value = items[index + 1];
      if (
        value === undefined ||
        attributes.has(item.value) ||
        (value.kind === "symbol" && value.value.startsWith(":"))
      ) {
        return Either.left(
          "Each attribute needs one value and must appear once.",
        );
      }
      attributes.set(item.value, value);
      index += 1;
    } else if (item !== undefined) {
      children.push(item);
    }
  }
  return Either.right({ attributes, children });
}

/** Reject typos where their declaration owns the accepted vocabulary. */
function checkAttributes(
  form: Declaration,
  allowed: readonly string[],
): Either.Either<void, string> {
  const unknown = [...form.attributes.keys()].find(
    (key) => !allowed.includes(key),
  );
  return unknown === undefined
    ? Either.void
    : Either.left(`Unknown attribute ${unknown}.`);
}

/** A scalar or list supplies names without comma or whitespace guessing. */
function names(node?: SExpression): Compiled<readonly string[]> {
  if (node === undefined) {
    return Either.right([]);
  }
  const values = (node.kind === "list" ? node.values : [node]).flatMap(
    (value) => atom(value) ?? [],
  );
  return node.kind === "list" && values.length !== node.values.length
    ? Either.left("Expected a name or a list of names.")
    : Either.right(values);
}

/** Missing booleans default to false; other values are rejected. */
function booleanAttribute(form: Declaration, key: string): Compiled<boolean> {
  const node = form.attributes.get(key);
  const value = atom(node);
  if (node === undefined || value === "false") {
    return Either.right(false);
  }
  return value === "true"
    ? Either.right(true)
    : Either.left(`${key} takes true or false.`);
}

/** Numeric constraints cannot carry nonfinite values. */
function numberAttribute(form: Declaration, key: string) {
  const node = form.attributes.get(key);
  if (node === undefined) {
    return Either.right<number | undefined>(undefined);
  }
  const text = atom(node);
  const value =
    text === undefined || text.trim() === "" ? Number.NaN : Number(text);
  return Number.isFinite(value)
    ? Either.right(value)
    : Either.left(`${key} takes a finite number.`);
}

/** Symbols and strings both name configuration values; quoted prose stays unchanged. */
function atom(node?: SExpression): string | undefined {
  return node?.kind === "symbol" || node?.kind === "string"
    ? node.value
    : undefined;
}

/** Invalid references fail installation instead of becoming unreachable runtime forms. */
function validateReferences(norm: Norm): Either.Either<void, string> {
  const forms = new Map(norm.forms.map((form) => [form.act, form]));
  if (forms.size !== norm.forms.length) {
    return Either.left("An act has exactly one declared form.");
  }
  if (!norm.forms.some((form) => form.starts)) {
    return Either.left("Declare at least one opening act with :starts true.");
  }
  if (
    norm.textRules.some(
      (rule) => rule.act !== undefined && !forms.has(rule.act),
    )
  ) {
    return Either.left("An English rule names an undeclared act.");
  }
  return Either.all([
    ...norm.forms.map((form) => validateForm(norm, form, forms)),
    validateRefusal(norm, forms),
  ]).pipe(Either.map(() => undefined));
}

function validateForm(
  norm: Norm,
  form: MessageForm,
  forms: ReadonlyMap<string, MessageForm>,
): Either.Either<void, string> {
  const roles = Object.values(norm.roles);
  const facts = new Set(norm.forms.flatMap((item) => item.initiates));
  if (
    !("recipient" in form.by) &&
    (form.by.length === 0 || form.by.some((role) => !roles.includes(role)))
  ) {
    return Either.left("Every sender role must be declared.");
  }
  if ("recipient" in form.by && !facts.has(form.by.recipient.fact)) {
    return Either.left("A recipient sender selector names an undeclared fact.");
  }
  if (
    !/^[a-z][a-z0-9-]{0,63}$/u.test(form.act) ||
    [...form.when, ...form.terminates].some((fact) => !facts.has(fact))
  ) {
    return Either.left(
      "Act names are lowercase; fact guards refer to declared facts.",
    );
  }
  if (form.starts && !opens(norm, form)) {
    return Either.left(
      "An opening act permits the initiator and cannot depend on an earlier interaction.",
    );
  }
  if (form.delay !== undefined && !anchored(norm, form.delay)) {
    return Either.left(
      "A delay references a required nonnegative numeric field on every act that initiates its fact.",
    );
  }
  return Either.all(
    form.relations.map((relation) => validateRelation(form, relation, forms)),
  ).pipe(Either.map(() => undefined));
}

function validateRefusal(
  norm: Norm,
  forms: ReadonlyMap<string, MessageForm>,
): Either.Either<void, string> {
  if (norm.onRefusal === undefined) {
    return Either.void;
  }
  const form = forms.get(norm.onRefusal);
  if (form === undefined) {
    return Either.left("The refusal act must have a declared form.");
  }
  const others = form.fields.filter((field) => field.name !== "reason");
  return fieldOf(form, "reason")?.type !== "string" ||
    others.some((field) => !field.optional)
    ? Either.left(
        "Automatic refusal supplies a reason string and no other required fields.",
      )
    : Either.void;
}

/** Whether a form that starts an instance lets the initiator send it and needs nothing earlier. */
function opens(norm: Norm, form: MessageForm) {
  const needsHistory =
    form.when.length !== 0 ||
    form.delay !== undefined ||
    form.relations.length !== 0;
  return (
    !("recipient" in form.by) &&
    form.by.includes(norm.roles.initiator) &&
    !needsHistory
  );
}

/** Whether every act that initiates a delay's fact requires the delay's field as a nonnegative integer. */
function anchored(norm: Norm, delay: NonNullable<MessageForm["delay"]>) {
  const anchors = norm.forms.filter((item) =>
    item.initiates.includes(delay.fact),
  );
  return (
    anchors.length !== 0 &&
    anchors.every((anchor) => {
      const field = fieldOf(anchor, delay.field);
      return (
        field?.type === "integer" &&
        !field.optional &&
        field.minimum !== undefined &&
        field.minimum >= 0
      );
    })
  );
}

function validateRelation(
  form: MessageForm,
  relation: MessageForm["relations"][number],
  forms: ReadonlyMap<string, MessageForm>,
): Either.Either<void, string> {
  const candidate = fieldOf(form, relation.field);
  if (candidate === undefined) {
    return Either.left("A relation refers to undeclared acts or fields.");
  }
  if (relation.operator === "subset" && candidate.type !== "multi-select") {
    return Either.left("A subset relation requires a list field.");
  }
  const targets = referenceNames(relation.source.act, forms);
  if (targets.length === 0) {
    return Either.left("A relation refers to an undeclared source.");
  }
  return Either.all(
    targets.map((act) => {
      const target = forms.get(act);
      return target === undefined
        ? Either.left("A relation refers to undeclared acts or fields.")
        : validateSource(relation.source, target, forms);
    }),
  ).pipe(Either.map(() => undefined));
}

function validateSource(
  source: Source,
  target: MessageForm,
  forms: ReadonlyMap<string, MessageForm>,
): Either.Either<void, string> {
  if (source.kind === "values") {
    return validateSelectedValues(source.keys, target, forms);
  }
  const fields = source.kind === "field" ? [source.field] : source.fields;
  if (fields.some((field) => fieldOf(target, field) === undefined)) {
    return Either.left("A relation names an undeclared source field.");
  }
  return source.kind === "field-values" &&
    fields.some((field) => fieldOf(target, field)?.type !== "string")
    ? Either.left("Direct values require declared string fields.")
    : Either.void;
}

function validateSelectedValues(
  keys: Extract<Source, { readonly kind: "field" }>,
  target: MessageForm,
  forms: ReadonlyMap<string, MessageForm>,
): Either.Either<void, string> {
  const targets = referenceNames(keys.act, forms);
  if (targets.length === 0) {
    return Either.left("A relation refers to an undeclared source.");
  }
  for (const act of targets) {
    const field = forms
      .get(act)
      ?.fields.find((declared) => declared.name === keys.field);
    if (field?.type !== "multi-select") {
      return Either.left(
        "Selected values require a choice list of declared string field names.",
      );
    }
    if (field.choices.some((key) => fieldOf(target, key)?.type !== "string")) {
      return Either.left("Selected values name an undeclared string field.");
    }
  }
  return Either.void;
}

function referenceNames(
  reference: Source["act"],
  forms: ReadonlyMap<string, MessageForm>,
) {
  if (typeof reference === "string") {
    return [reference];
  }
  return [...forms.values()]
    .filter((form) => form.initiates.includes(reference.fact))
    .map((form) => form.act);
}

/** The field `name` of a form. */
function fieldOf(form: MessageForm, name: string) {
  return form.fields.find((field) => field.name === name);
}
