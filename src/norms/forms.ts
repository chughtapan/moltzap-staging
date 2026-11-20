/** @file The form a filled set of fields is checked against: the field grammar a norm compiles to and the check of a filled form's content. */

import { Ajv } from "ajv";
import { Effect, Schema } from "effect";
import { exactStruct } from "../encoding.js";

/** Why one field of a filled form failed the form's schema. */
interface ResponseFieldFailure {
  readonly field: string;
  readonly reason: "missing" | "unexpected" | "invalid";
}

/** Every field of a filled form that fails the form's schema. */
type ResponseFieldFailures = readonly [
  ResponseFieldFailure,
  ...ResponseFieldFailure[],
];

const optionalNumber = Schema.optionalWith(
  Schema.Number.pipe(Schema.finite()),
  { exact: true },
);

/** Explains a field; it does not constrain a response. */
const description = Schema.optionalWith(Schema.String, { exact: true });

const UntitledMultiSelectEnumSchema = exactStruct({
  type: Schema.Literal("array"),
  description,
  minItems: optionalNumber,
  maxItems: optionalNumber,
  items: exactStruct({
    type: Schema.Literal("string"),
    enum: Schema.Array(Schema.String),
  }),
});

const StringSchema = exactStruct({
  type: Schema.Literal("string"),
  description,
  minLength: optionalNumber,
  maxLength: optionalNumber,
});

const NumberSchema = exactStruct({
  type: Schema.Literal("integer"),
  description,
  minimum: optionalNumber,
  maximum: optionalNumber,
});

/**
 * A form's schema: a flat object of fields, each a string, an integer or a
 * selection of several listed strings, with the names a filled form must
 * carry. Each field is the JSON Schema of its value.
 */
export const RequestedSchema = exactStruct({
  type: Schema.Literal("object"),
  properties: Schema.Record({
    key: Schema.String,
    value: Schema.Union(
      UntitledMultiSelectEnumSchema,
      StringSchema,
      NumberSchema,
    ),
  }),
  required: Schema.Array(Schema.String),
});

/** A validated form schema. */
export type RequestedSchema = typeof RequestedSchema.Type;

/**
 * Judges a filled field's value against its field as JSON Schema. A field is
 * admitted by `RequestedSchema` before it reaches this validator, which
 * therefore does not check the field against the JSON Schema meta-schema.
 */
const responseValidator = new Ajv({ strict: false, validateSchema: false });

/**
 * Check a filled form against its form schema.
 * @param requestedSchema The form schema.
 * @param content The filled form.
 * @returns The content unchanged, or every failing field.
 */
export function validateResponseContent(
  requestedSchema: RequestedSchema,
  content: Readonly<Record<string, unknown>>,
): Effect.Effect<Readonly<Record<string, unknown>>, ResponseFieldFailures> {
  const fields = new Set([
    ...Object.keys(requestedSchema.properties),
    ...Object.keys(content),
  ]);
  const [first, ...rest] = [...fields].flatMap((field) => {
    const failure = fieldFailure(requestedSchema, content, field);
    return failure === undefined ? [] : [failure];
  });
  return first === undefined
    ? Effect.succeed(content)
    : Effect.fail([first, ...rest]);
}

function fieldFailure(
  requestedSchema: RequestedSchema,
  content: Readonly<Record<string, unknown>>,
  field: string,
): ResponseFieldFailure | undefined {
  const definition = ownValue(requestedSchema.properties, field);
  if (definition === undefined) {
    return { field, reason: "unexpected" };
  }
  const value = ownValue(content, field);
  if (value === undefined) {
    return requestedSchema.required.includes(field)
      ? { field, reason: "missing" }
      : undefined;
  }
  const check = responseValidator.compile(definition);
  const valid = check(value);
  responseValidator.removeSchema(definition);
  return valid ? undefined : { field, reason: "invalid" };
}

/**
 * Field names come from peers, so a lookup ignores the prototype chain: a
 * field named `constructor` must not find `Object.prototype.constructor`.
 */
function ownValue<Value>(
  record: Readonly<Record<string, Value>>,
  key: string,
): Value | undefined {
  return Object.hasOwn(record, key) ? record[key] : undefined;
}
