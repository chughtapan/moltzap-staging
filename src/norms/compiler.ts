/**
 * @file Compiles declarative norms into the firewall's immutable contract
 * grammar: reads a document's declarations, builds the contract they state
 * and checks that its names and protocol predicates refer to declared things.
 */

import { Data, Effect, Either, Schema } from "effect";
import { createHash } from "node:crypto";
import { readSExpressions, type SExpression } from "../encoding.js";
import {
  type CompiledContract,
  compiledContractSchema,
  type MessageForm,
  RequestedSchema,
} from "../firewall/index.js";

/** Source diagnostics identify a declaration without echoing private document text. */
class NormDocumentError extends Data.TaggedError("NormDocumentError")<{
  readonly detail: string;
  readonly declaration?: string;
}> {}

/** A declaration separates named attributes from its child forms and text. */
interface Declaration {
  readonly kind: string;
  readonly name?: string;
  readonly attributes: ReadonlyMap<string, SExpression>;
  readonly children: readonly SExpression[];
}

const attributes: Readonly<Record<string, readonly string[]>> = {
  string: [":min-length", ":max-length"],
  integer: [":minimum", ":maximum"],
  "multi-select": [":choices", ":min-items", ":max-items"],
};

/** The digest names the exact document bytes supplied to both participants. */
export const compileNorm = Effect.fn("compileNorm")(function* (source: string) {
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
    return yield* new NormDocumentError({
      detail: "A norm names two distinct roles and a lowercase norm name.",
    });
  }
  if (
    initiator === undefined ||
    participant === undefined ||
    initiator === participant ||
    header.children.length !== 0
  ) {
    return yield* new NormDocumentError({
      detail: "A norm names two distinct roles without child declarations.",
    });
  }
  const roles = { initiator, participant };
  const forms = yield* Effect.forEach(
    body.filter((form) => form.kind === "content"),
    (form) => compileForm(form),
    { concurrency: 1 },
  );
  const textRules = yield* Effect.forEach(
    body.filter((form) => form.kind === "rule"),
    (form, index) => compileRule(form, `${header.name}-rule-${index + 1}`),
    { concurrency: 1 },
  );
  const onRefusal = atom(header.attributes.get(":on-refusal"));
  const contract = yield* Schema.decodeUnknown(compiledContractSchema)({
    name: header.name,
    digest: createHash("sha256").update(source, "utf8").digest("hex"),
    source,
    description: atom(header.attributes.get(":description")) ?? "",
    roles,
    forms,
    textRules,
    ...(onRefusal === undefined ? {} : { onRefusal }),
  }).pipe(
    Effect.catchTag("ParseError", () =>
      Effect.fail(
        new NormDocumentError({
          detail:
            "A declaration has an invalid name, value, bound or rule scope.",
        }),
      ),
    ),
  );
  yield* validateReferences(contract);
  return contract;
});

function compileForm(form: Declaration) {
  return Effect.gen(function* () {
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
    const children = yield* Effect.forEach(
      form.children,
      (node) => readDeclaration(node),
      { concurrency: 1 },
    );
    if (
      children.some(
        (child) => child.kind !== "field" && child.kind !== "relation",
      )
    ) {
      return yield* new NormDocumentError({
        detail: "Content declarations contain fields and relations.",
      });
    }
    const fields = yield* Effect.forEach(
      children.filter((child) => child.kind === "field"),
      (child) => compileField(child),
      { concurrency: 1 },
    );
    if (new Set(fields.map((field) => field.name)).size !== fields.length) {
      return yield* new NormDocumentError({
        detail: "Field names must be unique within a form.",
      });
    }
    const requested = yield* Schema.decodeUnknown(RequestedSchema)({
      type: "object",
      properties: Object.fromEntries(
        fields.map((field) => [field.name, field.schema]),
      ),
      required: fields
        .filter((field) => !field.optional)
        .map((field) => field.name),
    }).pipe(
      Effect.catchTag("ParseError", () =>
        Effect.fail(
          new NormDocumentError({
            detail: "Invalid field schema.",
            declaration: form.name,
          }),
        ),
      ),
    );
    const sender = form.attributes.get(":by");
    if (sender === undefined) {
      return yield* new NormDocumentError({
        detail: "Every sender role must be declared.",
        declaration: form.name,
      });
    }
    const by = yield* compileSender(sender);
    const maximum = yield* numberAttribute(form, ":max");
    const delay = yield* compileDelay(form);
    return {
      act: form.name ?? "",
      by,
      fields: requested,
      starts: yield* booleanAttribute(form, ":starts"),
      ends: yield* booleanAttribute(form, ":ends"),
      when: yield* names(form.attributes.get(":when")),
      initiates: yield* names(form.attributes.get(":initiates")),
      terminates: yield* names(form.attributes.get(":terminates")),
      ...(maximum === undefined ? {} : { maximum }),
      ...(delay === undefined ? {} : { delay }),
      relations: yield* Effect.forEach(
        children.filter((child) => child.kind === "relation"),
        (child) => compileRelation(child),
        { concurrency: 1 },
      ),
    };
  });
}

function compileSender(
  node: SExpression,
): Effect.Effect<MessageForm["by"], NormDocumentError> {
  if (
    node.kind !== "list" ||
    atom(node.values[0]) !== "recipient" ||
    node.values[1]?.kind !== "list"
  ) {
    return names(node);
  }
  const reference = node.values[1];
  const validReference =
    reference?.kind === "list" &&
    reference.values.length === 2 &&
    atom(reference.values[0]) === "fact";
  const fact =
    reference?.kind === "list" ? atom(reference.values[1]) : undefined;
  if (node.values.length !== 2 || !validReference || fact === undefined) {
    return Effect.fail(
      new NormDocumentError({
        detail: "A recipient sender selector names one fact.",
      }),
    );
  }
  return Effect.succeed({ recipient: { fact } });
}

function compileRule(form: Declaration, id: string) {
  return Effect.gen(function* () {
    yield* checkAttributes(form, [":act", ":feedback"]);
    const [text, ...rest] = form.children;
    if (text?.kind !== "string" || rest.length !== 0) {
      return yield* new NormDocumentError({
        detail: "An English rule contains one quoted rule text.",
      });
    }
    const act = atom(form.attributes.get(":act"));
    const feedback = atom(form.attributes.get(":feedback"));
    return {
      id,
      text: text.value,
      applies: "both",
      ...(act === undefined ? {} : { act }),
      ...(feedback === undefined ? {} : { feedback }),
    };
  });
}

function compileRelation(form: Declaration) {
  return Effect.gen(function* () {
    yield* checkAttributes(form, []);
    const [field, source, ...rest] = form.children;
    if (field?.kind !== "symbol" || source === undefined || rest.length !== 0) {
      return yield* new NormDocumentError({
        detail:
          "A relation names its operator, candidate field and value source.",
      });
    }
    return {
      operator: form.name,
      field: field.value,
      source: yield* compileSource(source),
    };
  });
}

function compileSource(
  node: SExpression,
): Effect.Effect<
  MessageForm["relations"][number]["source"],
  NormDocumentError
> {
  return Effect.gen(function* () {
    if (node.kind !== "list") {
      return yield* new NormDocumentError({
        detail:
          "A value source is a previous field, present fields, or selected values.",
      });
    }
    const [head, act, ...rest] = node.values;
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
        const fields = yield* names({ kind: "list", values: rest });
        const [first, ...others] = fields;
        if (first !== undefined) {
          return {
            kind: "field-values",
            act: name,
            fields: [first, ...others],
          };
        }
      }
    }
    return yield* new NormDocumentError({ detail: "Invalid value source." });
  });
}

/** Relations may read the same fields from alternative forms of a referenced exchange. */
function compileActReference(
  node?: SExpression,
): Effect.Effect<
  MessageForm["relations"][number]["source"]["act"],
  NormDocumentError
> {
  return Effect.gen(function* () {
    if (node?.kind === "list" && atom(node.values[0]) === "fact") {
      const fact = atom(node.values[1]);
      if (node.values.length !== 2 || fact === undefined) {
        return yield* new NormDocumentError({
          detail: "A fact source names one declared fact.",
        });
      }
      return { fact };
    }
    const act = atom(node);
    if (act === undefined) {
      return yield* new NormDocumentError({
        detail: "An act reference names one act or one fact.",
      });
    }
    return act;
  });
}

function compileDelay(form: Declaration) {
  return Effect.gen(function* () {
    const fact = atom(form.attributes.get(":after"));
    const field = atom(form.attributes.get(":delay"));
    const unit = atom(form.attributes.get(":unit"));
    if (fact === undefined && field === undefined && unit === undefined) {
      return undefined;
    }
    if (fact === undefined || field === undefined || unit !== "seconds") {
      return yield* new NormDocumentError({
        detail: "A delay supplies :after, :delay and :unit seconds.",
      });
    }
    return { fact, field };
  });
}

/** Field constraints compile into the existing public primitive form grammar. */
function compileField(form: Declaration) {
  return Effect.gen(function* () {
    const type = atom(form.attributes.get(":type")) ?? "";
    const allowed = Object.hasOwn(attributes, type)
      ? attributes[type]
      : undefined;
    if (
      allowed === undefined ||
      form.name === undefined ||
      !/^[a-z][a-z0-9-]{0,63}$/u.test(form.name)
    ) {
      return yield* new NormDocumentError({
        detail:
          "A field needs a lowercase name and a supported primitive type.",
      });
    }
    yield* checkAttributes(form, [
      ":type",
      ":optional",
      ":description",
      ...allowed,
    ]);
    if (form.children.length !== 0 || form.name === "ontology") {
      return yield* new NormDocumentError({
        detail:
          "A field cannot redefine envelope metadata or contain child declarations.",
      });
    }
    const choices = yield* names(form.attributes.get(":choices"));
    if (
      type === "multi-select" &&
      (choices.length === 0 || new Set(choices).size !== choices.length)
    ) {
      return yield* new NormDocumentError({
        detail: "Choice fields need distinct, nonempty :choices.",
      });
    }
    const bounds = yield* readBounds(form);
    const description = atom(form.attributes.get(":description"));
    return {
      name: form.name,
      optional: yield* booleanAttribute(form, ":optional"),
      schema: {
        ...(type === "multi-select"
          ? { type: "array", items: { type: "string", enum: choices } }
          : { type }),
        ...bounds,
        ...(description === undefined ? {} : { description }),
      },
    };
  }).pipe(Effect.withSpan("compileField"));
}

function readBounds(form: Declaration) {
  return Effect.gen(function* () {
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
        return yield* new NormDocumentError({
          detail:
            "Field bounds must be ordered; length and item bounds are nonnegative integers.",
        });
      }
    }
    return bounds;
  });
}

/** All declarations are data; a document has one leading norm header. */
function readDocument(source: string) {
  return Effect.gen(function* () {
    const parsed = yield* readSExpressions(source).pipe(
      Either.mapLeft(
        (error) =>
          new NormDocumentError({
            detail: `${error.detail} Offset ${error.offset}.`,
          }),
      ),
    );
    const declarations = yield* Effect.forEach(
      parsed,
      (node) => readDeclaration(node),
      { concurrency: 1 },
    );
    const [header, ...body] = declarations;
    if (
      header?.kind !== "norm" ||
      body.some((item) => item.kind !== "content" && item.kind !== "rule")
    ) {
      return yield* new NormDocumentError({
        detail:
          "A document starts with one norm declaration followed by content and rule declarations.",
      });
    }
    return { header, body };
  }).pipe(Effect.withSpan("readDocument"));
}

/** Attribute names are checked by the declaration that consumes them. */
function readDeclaration(node: SExpression) {
  return Effect.gen(function* () {
    if (node.kind !== "list") {
      return yield* new NormDocumentError({
        detail: "Expected a parenthesized declaration.",
      });
    }
    const [head, ...tail] = node.values;
    if (head?.kind !== "symbol") {
      return yield* new NormDocumentError({
        detail: "A declaration starts with a symbol.",
      });
    }
    const named = ["norm", "content", "field", "relation"].includes(head.value);
    const first = tail[0];
    if (named && (first?.kind !== "symbol" || first.value.startsWith(":"))) {
      return yield* new NormDocumentError({
        detail: "The declaration needs a name.",
        declaration: head.value,
      });
    }
    const fields = yield* declarationFields(
      named ? tail.slice(1) : tail,
      head.value,
    );
    return {
      kind: head.value,
      ...(named && first?.kind === "symbol" ? { name: first.value } : {}),
      ...fields,
    } satisfies Declaration;
  }).pipe(Effect.withSpan("readDeclaration"));
}

function declarationFields(items: readonly SExpression[], declaration: string) {
  return Effect.gen(function* () {
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
          return yield* new NormDocumentError({
            detail: "Each attribute needs one value and must appear once.",
            declaration,
          });
        }
        attributes.set(item.value, value);
        index += 1;
      } else if (item !== undefined) {
        children.push(item);
      }
    }
    return { attributes, children };
  });
}

/** Reject typos where their declaration owns the accepted vocabulary. */
function checkAttributes(form: Declaration, allowed: readonly string[]) {
  const unknown = [...form.attributes.keys()].find(
    (key) => !allowed.includes(key),
  );
  return unknown === undefined
    ? Effect.void
    : Effect.fail(
        new NormDocumentError({
          detail: `Unknown attribute ${unknown}.`,
          declaration: form.kind,
        }),
      );
}

/** A scalar or list supplies names without comma or whitespace guessing. */
function names(
  node?: SExpression,
): Effect.Effect<readonly string[], NormDocumentError> {
  if (node === undefined) {
    return Effect.succeed([]);
  }
  const values = node.kind === "list" ? node.values : [node];
  return Effect.forEach(
    values,
    (value) => {
      const name = atom(value);
      return name === undefined
        ? Effect.fail(
            new NormDocumentError({
              detail: "Expected a name or a list of names.",
            }),
          )
        : Effect.succeed(name);
    },
    { concurrency: 1 },
  );
}

/** Missing booleans default to false; other values are rejected. */
function booleanAttribute(form: Declaration, key: string) {
  const node = form.attributes.get(key);
  const value = atom(node);
  if (node === undefined || value === "false") {
    return Effect.succeed(false);
  }
  return value === "true"
    ? Effect.succeed(true)
    : Effect.fail(
        new NormDocumentError({ detail: `${key} takes true or false.` }),
      );
}

/** Numeric constraints cannot carry nonfinite values. */
function numberAttribute(form: Declaration, key: string) {
  const node = form.attributes.get(key);
  if (node === undefined) {
    return Effect.succeed(undefined);
  }
  const text = atom(node);
  const value =
    text === undefined || text.trim() === "" ? Number.NaN : Number(text);
  return Number.isFinite(value)
    ? Effect.succeed(value)
    : Effect.fail(
        new NormDocumentError({ detail: `${key} takes a finite number.` }),
      );
}

/** Symbols and strings both name configuration values; quoted prose stays unchanged. */
function atom(node?: SExpression): string | undefined {
  return node?.kind === "symbol" || node?.kind === "string"
    ? node.value
    : undefined;
}

/** Invalid references fail installation instead of becoming unreachable runtime forms. */
function validateReferences(contract: CompiledContract) {
  return Effect.gen(function* () {
    const forms = new Map(contract.forms.map((form) => [form.act, form]));
    if (forms.size !== contract.forms.length) {
      return yield* new NormDocumentError({
        detail: "An act has exactly one declared form.",
      });
    }
    if (!contract.forms.some((form) => form.starts)) {
      return yield* new NormDocumentError({
        detail: "Declare at least one opening act with :starts true.",
      });
    }
    if (
      contract.textRules.some(
        (rule) => rule.act !== undefined && !forms.has(rule.act),
      )
    ) {
      return yield* new NormDocumentError({
        detail: "An English rule names an undeclared act.",
      });
    }
    for (const form of contract.forms) {
      yield* validateForm(contract, form, forms);
    }
    yield* validateRefusal(contract, forms);
  }).pipe(Effect.withSpan("validateReferences"));
}

function validateForm(
  contract: CompiledContract,
  form: MessageForm,
  forms: ReadonlyMap<string, MessageForm>,
) {
  return Effect.gen(function* () {
    const roles = Object.values(contract.roles);
    const facts = new Set(contract.forms.flatMap((item) => item.initiates));
    if (
      !("recipient" in form.by) &&
      (form.by.length === 0 || form.by.some((role) => !roles.includes(role)))
    ) {
      return yield* new NormDocumentError({
        detail: "Every sender role must be declared.",
        declaration: form.act,
      });
    }
    if ("recipient" in form.by && !facts.has(form.by.recipient.fact)) {
      return yield* new NormDocumentError({
        detail: "A recipient sender selector names an undeclared fact.",
        declaration: form.act,
      });
    }
    if (
      !/^[a-z][a-z0-9-]{0,63}$/u.test(form.act) ||
      [...form.when, ...form.terminates].some((fact) => !facts.has(fact))
    ) {
      return yield* new NormDocumentError({
        detail: "Act names are lowercase; fact guards refer to declared facts.",
        declaration: form.act,
      });
    }
    if (form.starts) {
      yield* validateOpening(contract, form);
    }
    if (form.delay !== undefined) {
      yield* validateDelay(contract, form.delay);
    }
    for (const relation of form.relations) {
      yield* validateRelation(form, relation, forms);
    }
  });
}

function validateRefusal(
  contract: CompiledContract,
  forms: ReadonlyMap<string, MessageForm>,
) {
  if (contract.onRefusal === undefined) {
    return Effect.void;
  }
  const form = forms.get(contract.onRefusal);
  if (form === undefined) {
    return Effect.fail(
      new NormDocumentError({
        detail: "The refusal act must have a declared form.",
      }),
    );
  }
  return form.fields.properties.reason?.type !== "string" ||
    form.fields.required.some((field) => field !== "reason")
    ? Effect.fail(
        new NormDocumentError({
          detail:
            "Automatic refusal supplies a reason string and no other required fields.",
        }),
      )
    : Effect.void;
}

function validateOpening(contract: CompiledContract, form: MessageForm) {
  const needsHistory =
    form.when.length !== 0 ||
    form.delay !== undefined ||
    form.relations.length !== 0;
  return "recipient" in form.by ||
    !form.by.includes(contract.roles.initiator) ||
    needsHistory
    ? Effect.fail(
        new NormDocumentError({
          detail:
            "An opening act permits the initiator and cannot depend on an earlier interaction.",
          declaration: form.act,
        }),
      )
    : Effect.void;
}

function validateDelay(
  contract: CompiledContract,
  delay: NonNullable<MessageForm["delay"]>,
) {
  const anchors = contract.forms.filter((item) =>
    item.initiates.includes(delay.fact),
  );
  const valid =
    anchors.length !== 0 &&
    anchors.every((anchor) => {
      const field = anchor.fields.properties[delay.field];
      return (
        field?.type === "integer" &&
        anchor.fields.required.includes(delay.field) &&
        field.minimum !== undefined &&
        field.minimum >= 0
      );
    });
  return valid
    ? Effect.void
    : Effect.fail(
        new NormDocumentError({
          detail:
            "A delay references a required nonnegative numeric field on every act that initiates its fact.",
        }),
      );
}

function validateRelation(
  form: MessageForm,
  relation: MessageForm["relations"][number],
  forms: ReadonlyMap<string, MessageForm>,
) {
  return Effect.gen(function* () {
    const candidate = form.fields.properties[relation.field];
    if (candidate === undefined) {
      return yield* new NormDocumentError({
        detail: "A relation refers to undeclared acts or fields.",
        declaration: form.act,
      });
    }
    if (relation.operator === "subset" && candidate.type !== "array") {
      return yield* new NormDocumentError({
        detail: "A subset relation requires a list field.",
        declaration: form.act,
      });
    }
    const targets = referenceNames(relation.source.act, forms);
    if (targets.length === 0) {
      return yield* new NormDocumentError({
        detail: "A relation refers to an undeclared source.",
      });
    }
    for (const act of targets) {
      const target = forms.get(act);
      if (target === undefined) {
        return yield* new NormDocumentError({
          detail: "A relation refers to undeclared acts or fields.",
          declaration: form.act,
        });
      }
      yield* validateSource(form, relation.source, target, forms);
    }
  });
}

function validateSource(
  form: MessageForm,
  source: MessageForm["relations"][number]["source"],
  target: MessageForm,
  forms: ReadonlyMap<string, MessageForm>,
) {
  return Effect.gen(function* () {
    if (source.kind === "values") {
      return yield* validateSelectedValues(source.keys, target, forms);
    }
    const fields = source.kind === "field" ? [source.field] : source.fields;
    if (
      fields.some((field) => !Object.hasOwn(target.fields.properties, field))
    ) {
      return yield* new NormDocumentError({
        detail: "A relation names an undeclared source field.",
        declaration: form.act,
      });
    }
    if (
      source.kind === "field-values" &&
      fields.some((field) => target.fields.properties[field]?.type !== "string")
    ) {
      return yield* new NormDocumentError({
        detail: "Direct values require declared string fields.",
        declaration: form.act,
      });
    }
  });
}

function validateSelectedValues(
  keys: Extract<
    MessageForm["relations"][number]["source"],
    { readonly kind: "field" }
  >,
  target: MessageForm,
  forms: ReadonlyMap<string, MessageForm>,
) {
  return Effect.gen(function* () {
    const targets = referenceNames(keys.act, forms);
    if (targets.length === 0) {
      return yield* new NormDocumentError({
        detail: "A relation refers to an undeclared source.",
      });
    }
    for (const act of targets) {
      const field = forms.get(act)?.fields.properties[keys.field];
      if (field?.type !== "array") {
        return yield* new NormDocumentError({
          detail:
            "Selected values require a choice list of declared string field names.",
        });
      }
      if (
        field.items.enum.some(
          (key) => target.fields.properties[key]?.type !== "string",
        )
      ) {
        return yield* new NormDocumentError({
          detail: "Selected values name an undeclared string field.",
        });
      }
    }
  });
}

function referenceNames(
  reference: MessageForm["relations"][number]["source"]["act"],
  forms: ReadonlyMap<string, MessageForm>,
) {
  if (typeof reference === "string") {
    return [reference];
  }
  return [...forms.values()]
    .filter((form) => form.initiates.includes(reference.fact))
    .map((form) => form.act);
}
