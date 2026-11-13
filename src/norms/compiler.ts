/** @file Compiles declarative norms into the firewall's immutable contract grammar. */

import { Effect, Schema } from "effect";
import { createHash } from "node:crypto";
import type { SExpression } from "../sexpr.js";
import { RequestedSchema } from "../communication/index.js";
import {
  type CompiledContract,
  compiledContractSchema,
  type MessageForm,
} from "../firewall/index.js";
import {
  atom,
  booleanAttribute,
  checkAttributes,
  type Declaration,
  names,
  NormDocumentError,
  numberAttribute,
  readDeclaration,
  readDocument,
} from "./document.js";
import { validateReferences } from "./validation.js";

const attributes: Readonly<Record<string, readonly string[]>> = {
  string: [":min-length", ":max-length"],
  number: [":minimum", ":maximum"],
  integer: [":minimum", ":maximum"],
  select: [":choices"],
  "multi-select": [":choices", ":min-items", ":max-items"],
  boolean: [],
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
    (form) => compileForm(form, roles),
    { concurrency: 1 },
  );
  const semanticRules = yield* Effect.forEach(
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
    semanticRules,
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

function compileForm(form: Declaration, roles: CompiledContract["roles"]) {
  return Effect.gen(function* () {
    yield* checkAttributes(form, [
      ":by",
      ":starts",
      ":ends",
      ":when",
      ":unless",
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
    const by = yield* compileSender(roles, form.attributes.get(":by"));
    const maximum = yield* numberAttribute(form, ":max");
    const delay = yield* compileDelay(form);
    return {
      act: form.name ?? "",
      by,
      fields: requested,
      starts: yield* booleanAttribute(form, ":starts"),
      ends: yield* booleanAttribute(form, ":ends"),
      when: yield* names(form.attributes.get(":when")),
      unless: yield* names(form.attributes.get(":unless")),
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
  roles: CompiledContract["roles"],
  node?: SExpression,
): Effect.Effect<MessageForm["by"], NormDocumentError> {
  if (node === undefined) {
    return Effect.succeed([roles.initiator, roles.participant]);
  }
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
    yield* checkAttributes(form, [
      ":act",
      ":applies",
      ":guidance",
      ":feedback",
      ":action",
    ]);
    const [text, ...rest] = form.children;
    if (text?.kind !== "string" || rest.length !== 0) {
      return yield* new NormDocumentError({
        detail: "An English rule contains one quoted rule text.",
      });
    }
    const act = atom(form.attributes.get(":act"));
    const guidance = atom(form.attributes.get(":guidance"));
    const feedback = atom(form.attributes.get(":feedback"));
    const action = atom(form.attributes.get(":action"));
    return {
      id,
      text: text.value,
      applies: atom(form.attributes.get(":applies")) ?? "both",
      ...(act === undefined ? {} : { act }),
      ...(guidance === undefined ? {} : { guidance }),
      ...(feedback === undefined ? {} : { feedback: { text: feedback } }),
      ...(action === undefined ? {} : { action }),
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
    const references = yield* names(node);
    const [first, ...rest] = references;
    if (first === undefined || new Set(references).size !== references.length) {
      return yield* new NormDocumentError({
        detail: "An act reference requires distinct, nonempty act names.",
      });
    }
    return rest.length === 0 ? first : ([first, ...rest] as const);
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
    if (
      fact === undefined ||
      field === undefined ||
      (unit !== "seconds" && unit !== "milliseconds")
    ) {
      return yield* new NormDocumentError({
        detail:
          "A delay supplies :after, :delay and :unit seconds or milliseconds.",
      });
    }
    return { fact, field, millisecondsPerUnit: unit === "seconds" ? 1000 : 1 };
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
      (type === "select" || type === "multi-select") &&
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
        ...fieldShape(type, choices),
        ...bounds,
        ...(description === undefined ? {} : { description }),
      },
    };
  }).pipe(Effect.withSpan("compileField"));
}

function fieldShape(type: string, choices: readonly string[]) {
  switch (type) {
    case "multi-select":
      return { type: "array", items: { type: "string", enum: choices } };
    case "select":
      return { type: "string", enum: choices };
    default:
      return { type };
  }
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
