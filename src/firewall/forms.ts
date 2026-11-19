/** @file The form a filled set of fields is checked against: the MCP form-mode schema grammar and the check of a filled form's content. */

import type { ParseOptions } from "effect/SchemaAST";
import { Ajv } from "ajv";
import { fullFormats } from "ajv-formats/dist/formats.js";
import {
  Array as Arr,
  Effect,
  Either,
  JSONSchema,
  Option,
  ParseResult,
  Predicate,
  Schema,
} from "effect";
import { exactStruct, issuesText } from "../encoding.js";

/**
 * What a filled form carries, field by field. Its value space is MCP
 * `ElicitResult` content's; the form's schema decides which fields apply.
 */
export const ResponseContent = Schema.Record({
  key: Schema.String,
  value: Schema.Union(
    Schema.String,
    Schema.Number.pipe(Schema.finite()),
    Schema.Boolean,
    Schema.Array(Schema.String),
  ),
}).annotations({
  description: "The filled form, one field per property of its schema",
});
/** Structurally valid content of a filled form. */
export type ResponseContent = typeof ResponseContent.Type;

/**
 * Why one field of a filled form failed the form's schema; `detail` names
 * each keyword an `invalid` field fails.
 */
interface ResponseFieldFailure {
  readonly field: string;
  readonly reason: "missing" | "unexpected" | "invalid";
  readonly detail?: string;
}

/**
 * One line per issue of a decode failure, each led by its path, such as
 * `requestedSchema.type: Expected "object", actual "obj"`, joined by `; `.
 * A model repairs its input from this text alone.
 * @param error The failed decode.
 * @returns The issues as text.
 */
const describeIssues = (error: ParseResult.ParseError): string =>
  issuesText(ParseResult.ArrayFormatter.formatErrorSync(error));

/** Every field of a filled form that fails the form's schema. */
type ResponseFieldFailures = readonly [
  ResponseFieldFailure,
  ...ResponseFieldFailure[],
];

/**
 * A keyword whose failure, ill-typed or missing, names what it expects.
 * `whole` reports a failure inside the keyword's value as one failure of the
 * keyword; a nested object keeps its own keywords' paths instead.
 */
const expecting = <A, I>(
  schema: Schema.Schema<A, I>,
  expected: string,
  whole = true,
): Schema.Schema<A, I> =>
  schema.annotations({
    message: () => ({ message: expectedText(expected), override: whole }),
  });

const FiniteNumber = Schema.Number.pipe(Schema.finite());

const StringList = Schema.Array(Schema.String);

const STRING_LIST = "an array of strings";

/** Labels every field shape may carry; neither constrains a response. */
const labels = {
  title: optionalKeyword(Schema.String, "string"),
  description: optionalKeyword(Schema.String, "string"),
};

const stringType = keyword(Schema.Literal("string"), '"string"');

const arrayType = keyword(Schema.Literal("array"), '"array"');

const optionalString = optionalKeyword(Schema.String, "string");

const optionalNumber = optionalKeyword(FiniteNumber, "number");

/** One titled option of a titled select: the value and its display title. */
const titledOptions = keyword(
  Schema.Array(Schema.Struct({ const: Schema.String, title: Schema.String })),
  'an array of {"const": string, "title": string}',
);

const UntitledSingleSelectEnumSchema = Schema.Struct({
  type: stringType,
  ...labels,
  enum: keyword(StringList, STRING_LIST),
  default: optionalString,
});

const TitledSingleSelectEnumSchema = Schema.Struct({
  type: stringType,
  ...labels,
  oneOf: titledOptions,
  default: optionalString,
});

const multiSelect = {
  type: arrayType,
  ...labels,
  minItems: optionalNumber,
  maxItems: optionalNumber,
  default: optionalKeyword(StringList, STRING_LIST),
};

const UntitledMultiSelectEnumSchema = Schema.Struct({
  ...multiSelect,
  items: nestedKeyword(
    Schema.Struct({
      type: stringType,
      enum: keyword(StringList, STRING_LIST),
    }),
  ),
});

const TitledMultiSelectEnumSchema = Schema.Struct({
  ...multiSelect,
  items: nestedKeyword(Schema.Struct({ anyOf: titledOptions })),
});

const BooleanSchema = Schema.Struct({
  type: keyword(Schema.Literal("boolean"), '"boolean"'),
  ...labels,
  default: optionalKeyword(Schema.Boolean, "boolean"),
});

const StringSchema = Schema.Struct({
  type: stringType,
  ...labels,
  minLength: optionalNumber,
  maxLength: optionalNumber,
  format: optionalKeyword(
    Schema.Literal("email", "uri", "date", "date-time"),
    'one of "email", "uri", "date" or "date-time"',
  ),
  default: optionalString,
});

const NumberSchema = Schema.Struct({
  type: keyword(Schema.Literal("number", "integer"), '"number" or "integer"'),
  ...labels,
  minimum: optionalNumber,
  maximum: optionalNumber,
  default: optionalNumber,
});

/** One admitted form field: a primitive property of a form-mode schema. */
type FormField =
  | typeof UntitledSingleSelectEnumSchema.Type
  | typeof TitledSingleSelectEnumSchema.Type
  | typeof UntitledMultiSelectEnumSchema.Type
  | typeof TitledMultiSelectEnumSchema.Type
  | typeof BooleanSchema.Type
  | typeof StringSchema.Type
  | typeof NumberSchema.Type;

/** Shape outputs keep only declared keywords; failures report every keyword. */
const shapeOptions: ParseOptions = {
  onExcessProperty: "ignore",
  errors: "all",
};

type ShapeDecoder = (
  property: unknown,
) => Either.Either<FormField, ParseResult.ParseError>;

/** The shapes in the specification's union order; the first match wins. */
const fieldShapes: readonly ShapeDecoder[] = [
  decoderOf(UntitledSingleSelectEnumSchema),
  decoderOf(TitledSingleSelectEnumSchema),
  decoderOf(UntitledMultiSelectEnumSchema),
  decoderOf(TitledMultiSelectEnumSchema),
  decoderOf(BooleanSchema),
  decoderOf(StringSchema),
  decoderOf(NumberSchema),
];

/**
 * One form-mode shape: what a requester should write, its schema, and which
 * properties it is the intended shape of.
 */
interface FormFieldShape {
  readonly expected: string;
  readonly decode: ShapeDecoder;
  readonly intends: (property: unknown) => boolean;
}

/** Shapes by intent, each listed before any shape it refines. */
const intendedShapes: readonly FormFieldShape[] = [
  {
    expected:
      'a titled single-select is {"type":"string","oneOf":[{"const":"a","title":"A"}]}',
    decode: decoderOf(TitledSingleSelectEnumSchema),
    intends: (property) =>
      hasType(property, "string") && Predicate.hasProperty(property, "oneOf"),
  },
  {
    expected: 'a single-select is {"type":"string","enum":["a","b"]}',
    decode: decoderOf(UntitledSingleSelectEnumSchema),
    intends: (property) =>
      hasType(property, "string") && Predicate.hasProperty(property, "enum"),
  },
  {
    expected:
      'a string is {"type":"string"} with optional minLength, maxLength or format',
    decode: decoderOf(StringSchema),
    intends: (property) => hasType(property, "string"),
  },
  {
    expected:
      'a number is {"type":"number"} or {"type":"integer"} with optional minimum and maximum',
    decode: decoderOf(NumberSchema),
    intends: (property) => hasType(property, "number", "integer"),
  },
  {
    expected: 'a boolean is {"type":"boolean"}',
    decode: decoderOf(BooleanSchema),
    intends: (property) => hasType(property, "boolean"),
  },
  {
    expected:
      'a titled multi-select is {"type":"array","items":{"anyOf":[{"const":"a","title":"A"}]}}',
    decode: decoderOf(TitledMultiSelectEnumSchema),
    intends: (property) =>
      hasType(property, "array") &&
      Predicate.hasProperty(property, "items") &&
      Predicate.hasProperty(property.items, "anyOf"),
  },
  {
    expected:
      'a multi-select is {"type":"array","items":{"type":"string","enum":["a","b"]}}',
    decode: decoderOf(UntitledMultiSelectEnumSchema),
    intends: (property) => hasType(property, "array"),
  },
];

const AdmittedFormField = Schema.declare(
  (value): value is FormField =>
    Either.match(admitFormField(value), {
      onLeft: () => false,
      onRight: () => true,
    }),
  {
    identifier: "PrimitiveSchemaDefinition",
    jsonSchema: JSONSchema.make(
      Schema.Union(
        UntitledSingleSelectEnumSchema,
        TitledSingleSelectEnumSchema,
        UntitledMultiSelectEnumSchema,
        TitledMultiSelectEnumSchema,
        BooleanSchema,
        StringSchema,
        NumberSchema,
      ),
    ),
  },
);

/**
 * One property of an MCP form-mode `requestedSchema`. Decoding keeps only the
 * keywords of the shape that admitted the property.
 */
const FormFieldSchema = Schema.transformOrFail(
  Schema.Unknown,
  AdmittedFormField,
  {
    strict: true,
    decode: (value) =>
      Either.match(admitFormField(value), {
        onLeft: (message) =>
          ParseResult.fail(
            new ParseResult.Type(AdmittedFormField.ast, value, message),
          ),
        onRight: (field) => ParseResult.succeed(field),
      }),
    encode: (field) => ParseResult.succeed(field),
  },
);

/**
 * The MCP form-mode `requestedSchema`: a flat object of primitive properties.
 * Every `required` name must be a declared property, since a filled form can
 * carry no other field.
 */
export const RequestedSchema = exactStruct({
  $schema: Schema.optionalWith(Schema.String, { exact: true }),
  type: Schema.Literal("object"),
  properties: Schema.Record({ key: Schema.String, value: FormFieldSchema }),
  required: Schema.optionalWith(Schema.Array(Schema.String), { exact: true }),
}).pipe(
  Schema.filter(
    (schema) => {
      const undeclared = (schema.required ?? []).filter(
        (name) => !Object.hasOwn(schema.properties, name),
      );
      return (
        undeclared.length === 0 || {
          path: ["required"],
          message: `names ${undeclared.map((name) => JSON.stringify(name)).join(", ")}, which properties does not declare`,
        }
      );
    },
    {
      identifier: "RequestedSchema",
      description: "Form-mode schema whose required names are properties",
    },
  ),
);

/** A validated form-mode schema. */
export type RequestedSchema = typeof RequestedSchema.Type;

type ResponseFieldValue = ResponseContent[string];

/**
 * Judges a filled field's value against its field as JSON Schema, reporting
 * every failing keyword. Formats are the full `ajv-formats` definitions. A
 * field is admitted by `RequestedSchema` before it reaches this validator,
 * which therefore does not check the field against the JSON Schema
 * meta-schema.
 */
const responseValidator = new Ajv({
  allErrors: true,
  strict: false,
  validateSchema: false,
  formats: {
    date: fullFormats.date,
    "date-time": fullFormats["date-time"],
    email: fullFormats.email,
    uri: fullFormats.uri,
  },
});

/**
 * Check a filled form against its form-mode schema.
 * @param requestedSchema The form-mode schema.
 * @param content The filled form.
 * @returns The content unchanged, or every failing field.
 */
export function validateResponseContent(
  requestedSchema: RequestedSchema,
  content: ResponseContent,
): Effect.Effect<ResponseContent, ResponseFieldFailures> {
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

function keyword<A, I>(schema: Schema.Schema<A, I>, expected: string) {
  return Schema.propertySignature(expecting(schema, expected)).annotations({
    missingMessage: () => expectedText(expected),
  });
}

/** A keyword whose value is an object that keeps its own keywords' failure paths. */
function nestedKeyword<A, I>(schema: Schema.Schema<A, I>) {
  const expected = "an object";
  return Schema.propertySignature(
    expecting(schema, expected, false),
  ).annotations({ missingMessage: () => expectedText(expected) });
}

function optionalKeyword<A, I>(schema: Schema.Schema<A, I>, expected: string) {
  return Schema.optionalWith(expecting(schema, expected), { exact: true });
}

function decoderOf<A extends FormField, I>(
  schema: Schema.Schema<A, I>,
): ShapeDecoder {
  return (property) =>
    Schema.decodeUnknownEither(schema)(property, shapeOptions);
}

function hasType(property: unknown, ...types: readonly string[]): boolean {
  return (
    Predicate.hasProperty(property, "type") &&
    types.some((type) => type === property.type)
  );
}

/**
 * Admit one property under the first shape that matches it.
 * @param property An untrusted property of a requested schema.
 * @returns The admitted field, or the text naming each failing keyword.
 */
function admitFormField(property: unknown): Either.Either<FormField, string> {
  return Option.match(
    Arr.findFirst(fieldShapes, (decode) => Either.getRight(decode(property))),
    {
      onNone: () => Either.left(formFieldViolation(property)),
      onSome: refuseEmptySelect,
    },
  );
}

/**
 * Say why no shape admits a property: each keyword its intended shape
 * rejects, by path within the property, then that shape.
 * @param property A property every shape rejected.
 * @returns For example `items.type: Invalid input: expected "string"; a
 *   multi-select is {"type":"array","items":{"type":"string","enum":["a","b"]}}`.
 */
function formFieldViolation(property: unknown): string {
  const shape = intendedShapes.find(({ intends }) => intends(property));
  if (shape === undefined) {
    return '"type" must be "string", "number", "integer", "boolean" or "array"';
  }
  return Either.match(shape.decode(property), {
    onLeft: (error) => `${describeIssues(error)}; ${shape.expected}`,
    onRight: () => shape.expected,
  });
}

/**
 * Refuse an admitted select that lists no option to choose. JSON Schema
 * requires a non-empty `enum`, so such a field could accept no response.
 */
function refuseEmptySelect(field: FormField): Either.Either<FormField, string> {
  const noOptions = (path: readonly string[]) =>
    Either.left(`${path.join(".")}: ${expectedText("at least one option")}`);
  if ("enum" in field && field.enum.length === 0) {
    return noOptions(["enum"]);
  }
  if ("items" in field && "enum" in field.items) {
    return field.items.enum.length === 0
      ? noOptions(["items", "enum"])
      : Either.right(field);
  }
  return Either.right(field);
}

function expectedText(expected: string): string {
  return `Invalid input: expected ${expected}`;
}

function fieldFailure(
  requestedSchema: RequestedSchema,
  content: ResponseContent,
  field: string,
): ResponseFieldFailure | undefined {
  const definition = ownValue(requestedSchema.properties, field);
  if (definition === undefined) {
    return { field, reason: "unexpected" };
  }
  const value = ownValue(content, field);
  if (value === undefined) {
    return requestedSchema.required?.includes(field) === true
      ? { field, reason: "missing" }
      : undefined;
  }
  const issues = responseFieldIssues(definition, value);
  return issues.length === 0
    ? undefined
    : { field, reason: "invalid", detail: issues.join(", ") };
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

/**
 * Every failing keyword of one filled field's value, as `keyword:
 * requirement`, with a selected item's keyword led by its position, such as
 * `items[1].enum`. A titled single-select's value must equal exactly one
 * option.
 */
function responseFieldIssues(
  field: FormField,
  value: ResponseFieldValue,
): readonly string[] {
  const check = responseValidator.compile(field);
  check(value);
  responseValidator.removeSchema(field);
  return (check.errors ?? [])
    .filter(({ keyword }) => keyword !== "const")
    .map(({ instancePath, keyword, message }) => {
      const item =
        instancePath === "" ? "" : `items[${instancePath.slice(1)}].`;
      return `${item}${keyword}: ${message ?? "is invalid"}`;
    });
}
