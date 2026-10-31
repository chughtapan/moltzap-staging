/**
 * @file Collective-layer values carried inside a certified message's content.
 *
 * A collective value travels as one `data` part whose value is an object with
 * the key `COLLECTIVE_DATA_KEY`. `Content` stays a generic container; only
 * endpoints read the part, and the Router sees only the envelope. Every value
 * carries a `kind` discriminant so an endpoint classifies a part without trying
 * each shape in turn.
 */

import type { ParseOptions } from "effect/SchemaAST";
import {
  Array as Arr,
  Data,
  Effect,
  Either,
  Option,
  ParseResult,
  Predicate,
  Schema,
} from "effect";
import { randomBytes } from "node:crypto";
import { exactStruct } from "../../encoding/exact.js";
import { AgentId } from "../../identity/identifiers.js";
import { WIRE_VERSION } from "../../identity/wire-version.js";
import { MessageReference } from "../message.js";
import { Content } from "../records/index.js";
import { wireProfiles } from "../records/values.js";
import {
  AcceptResponse,
  collectiveClosure,
  CollectiveId,
  DeclineResponse,
  describeIssues,
  EpochMillis,
  RequestedSchema,
} from "./contracts.js";

/* eslint-disable @typescript-eslint/naming-convention -- Effect Schemas take the names of the MCP specification shapes they mirror. */

const expectedText = (expected: string): string =>
  `Invalid input: expected ${expected}`;

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

function keyword<A, I>(
  schema: Schema.Schema<A, I>,
  expected: string,
  whole = true,
) {
  return Schema.propertySignature(
    expecting(schema, expected, whole),
  ).annotations({ missingMessage: () => expectedText(expected) });
}

function optionalKeyword<A, I>(schema: Schema.Schema<A, I>, expected: string) {
  return Schema.optionalWith(expecting(schema, expected), { exact: true });
}

const FiniteNumber = Schema.Number.pipe(Schema.finite());
const StringList = Schema.Array(Schema.String);
const STRING_LIST = "an array of strings";

/** Labels every field shape may carry; neither constrains an answer. */
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
  items: keyword(
    Schema.Struct({
      type: stringType,
      enum: keyword(StringList, STRING_LIST),
    }),
    "an object",
    false,
  ),
});

const TitledMultiSelectEnumSchema = Schema.Struct({
  ...multiSelect,
  items: keyword(Schema.Struct({ anyOf: titledOptions }), "an object", false),
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
export type FormField =
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

const decoderOf =
  <A extends FormField, I>(schema: Schema.Schema<A, I>): ShapeDecoder =>
  (property) =>
    Schema.decodeUnknownEither(schema)(property, shapeOptions);

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

const declares = (property: unknown, ...types: readonly string[]): boolean =>
  Predicate.hasProperty(property, "type") &&
  types.some((type) => type === property.type);

/** Shapes by intent, each listed before any shape it refines. */
const intendedShapes: readonly FormFieldShape[] = [
  {
    expected:
      'a titled single-select is {"type":"string","oneOf":[{"const":"a","title":"A"}]}',
    decode: decoderOf(TitledSingleSelectEnumSchema),
    intends: (property) =>
      declares(property, "string") && Predicate.hasProperty(property, "oneOf"),
  },
  {
    expected: 'a single-select is {"type":"string","enum":["a","b"]}',
    decode: decoderOf(UntitledSingleSelectEnumSchema),
    intends: (property) =>
      declares(property, "string") && Predicate.hasProperty(property, "enum"),
  },
  {
    expected:
      'a string is {"type":"string"} with optional minLength, maxLength or format',
    decode: decoderOf(StringSchema),
    intends: (property) => declares(property, "string"),
  },
  {
    expected:
      'a number is {"type":"number"} or {"type":"integer"} with optional minimum and maximum',
    decode: decoderOf(NumberSchema),
    intends: (property) => declares(property, "number", "integer"),
  },
  {
    expected: 'a boolean is {"type":"boolean"}',
    decode: decoderOf(BooleanSchema),
    intends: (property) => declares(property, "boolean"),
  },
  {
    expected:
      'a titled multi-select is {"type":"array","items":{"anyOf":[{"const":"a","title":"A"}]}}',
    decode: decoderOf(TitledMultiSelectEnumSchema),
    intends: (property) =>
      declares(property, "array") &&
      Predicate.hasProperty(property, "items") &&
      Predicate.hasProperty(property.items, "anyOf"),
  },
  {
    expected:
      'a multi-select is {"type":"array","items":{"type":"string","enum":["a","b"]}}',
    decode: decoderOf(UntitledMultiSelectEnumSchema),
    intends: (property) => declares(property, "array"),
  },
];

const FormFieldValue = Schema.declare(
  (value): value is FormField =>
    Either.match(admitFormField(value), {
      onLeft: () => false,
      onRight: () => true,
    }),
  { identifier: "PrimitiveSchemaDefinition" },
);

/**
 * One property of an MCP form-mode `requestedSchema`. Decoding keeps only the
 * keywords of the shape that admitted the property.
 */
const FormFieldSchema = Schema.transformOrFail(Schema.Unknown, FormFieldValue, {
  strict: true,
  decode: (value) =>
    Either.match(admitFormField(value), {
      onLeft: (message) =>
        ParseResult.fail(
          new ParseResult.Type(FormFieldValue.ast, value, message),
        ),
      onRight: (field) => ParseResult.succeed(field),
    }),
  encode: (field) => ParseResult.succeed(field),
});

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
      onSome: offersOptions,
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
 * requires a non-empty `enum`, so such a field could accept no answer.
 */
function offersOptions(field: FormField): Either.Either<FormField, string> {
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

/* eslint-enable @typescript-eslint/naming-convention -- Restore the package naming rules after the shapes. */

/* eslint-disable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Effect Schemas share their domain names with the values they decode. */

type MessageContent = typeof Content.Type;

/** The `data` part key under which the collective layer carries its value. */
export const COLLECTIVE_DATA_KEY = wireProfiles[WIRE_VERSION].collectiveDataKey;

/**
 * The MCP form-mode `requestedSchema`: a flat object of primitive properties.
 * Every `required` name must be a declared property, since an answer can carry
 * no other field.
 */
export const FormModeSchema = exactStruct({
  ...RequestedSchema.fields,
  properties: Schema.Record({ key: Schema.String, value: FormFieldSchema }),
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
/** A validated form-mode schema for one collective question. */
export type FormModeSchema = typeof FormModeSchema.Type;

const decodeCollectiveId = Schema.decodeUnknownSync(CollectiveId);
const decodeAgentId = Schema.decodeUnknownSync(AgentId);

/**
 * Read the requester embedded in an operation ID. Request admission compares
 * this identity with the certified author before accepting the operation.
 */
export const collectiveRequesterOf = (operationId: CollectiveId): AgentId =>
  decodeAgentId(
    `agt_${Buffer.from(operationId.slice(4), "base64url")
      .subarray(0, 16)
      .toString("base64url")}`,
  );

/** Distinct requesters retain distinct operation IDs even when their random bytes match. */
export const mintCollectiveId = (requester: AgentId) =>
  Effect.sync(() =>
    decodeCollectiveId(
      `col_${Buffer.concat([
        Buffer.from(requester.slice(4), "base64url"),
        randomBytes(32),
      ]).toString("base64url")}`,
    ),
  );

/**
 * A question to every member. `operationId` embeds the requester's immutable
 * identity. `deadlineAt` is absolute epoch milliseconds: the
 * sending endpoint converts the model's relative duration at send, and
 * endpoints assume zero clock skew.
 */
const CollectingOperation = exactStruct({
  kind: Schema.Literal("operation"),
  op: Schema.Literal("gather", "all_gather"),
  operationId: CollectiveId,
  deadlineAt: EpochMillis,
  requestedSchema: FormModeSchema,
});

/** The request a member's reply answers. */
const responseHead = {
  kind: Schema.Literal("response"),
  operationId: CollectiveId,
};

/** A member's reply to one request; only `accept` carries content. */
const CollectiveResponse = Schema.Union(
  exactStruct({
    ...responseHead,
    action: AcceptResponse.fields.action,
    content: AcceptResponse.fields.content,
  }),
  exactStruct({ ...responseHead, action: DeclineResponse.fields.action }),
);

/** The requester's completion decision and counted answer identities. */
const CollectiveClose = exactStruct({
  kind: Schema.Literal("close"),
  operationId: CollectiveId,
  ...collectiveClosure.fields,
  included: Schema.Array(MessageReference),
});

/** Every value the collective layer carries, discriminated by `kind`. */
const CollectiveValue = Schema.Union(
  CollectingOperation,
  CollectiveResponse,
  CollectiveClose,
);
/** A validated collective value. */
export type CollectiveValue = typeof CollectiveValue.Type;

/** An accepted or declined answer carried in a certified message. */
export type ResponseValue = Extract<
  CollectiveValue,
  { readonly kind: "response" }
>;

/** Content carries a collective part that is duplicated or does not decode. */
export class CollectivePartInvalidError extends Data.TaggedError(
  "CollectivePartInvalidError",
)<{
  readonly reason: "duplicate" | "malformed";
}> {}

/** A collective value and its text do not form content within the 32,768-byte canonical limit. */
export class CollectiveContentError extends Data.TaggedError(
  "CollectiveContentError",
) {}

/**
 * Any object value that carries the collective key; other keys are ignored.
 * `exact` keeps a missing key from decoding as `undefined`.
 */
const CollectiveCarrier = Schema.Struct({
  [COLLECTIVE_DATA_KEY]: Schema.Unknown,
}).annotations({ parseOptions: { exact: true } });

function collectivePartValue(
  part: MessageContent[number],
): Option.Option<unknown> {
  return part.type === "data"
    ? Schema.decodeUnknownOption(CollectiveCarrier)(part.value).pipe(
        Option.map((carrier) => carrier[COLLECTIVE_DATA_KEY]),
      )
    : Option.none();
}

/**
 * Decode one untrusted collective value.
 * @param value The value found under `COLLECTIVE_DATA_KEY`.
 * @returns The validated value.
 */
export const decodeCollectiveValue = Schema.decodeUnknown(CollectiveValue);

/**
 * Find and decode the collective value in one message's content.
 * @param content Certified message content.
 * @returns The decoded value, or none when the content carries no collective part.
 */
export const readCollectiveValue = (
  content: MessageContent,
): Effect.Effect<
  Option.Option<CollectiveValue>,
  CollectivePartInvalidError
> => {
  const [only, ...rest] = content.flatMap((part) =>
    Option.toArray(collectivePartValue(part)),
  );
  if (only === undefined) {
    return Effect.succeed(Option.none());
  }
  if (rest.length > 0) {
    return Effect.fail(new CollectivePartInvalidError({ reason: "duplicate" }));
  }
  return decodeCollectiveValue(only).pipe(
    Effect.map(Option.some),
    Effect.catchTag("ParseError", () =>
      Effect.fail(new CollectivePartInvalidError({ reason: "malformed" })),
    ),
  );
};

/**
 * Build message content from a collective value and optional message text. The
 * public `Content` schema applies the one canonical size limit.
 * @param value The collective value to carry in the data part.
 * @param text The message body; for gather and all_gather, the question.
 * @returns Content with the text first and the collective part last.
 */
export const encodeCollectiveContent = (
  value: CollectiveValue,
  text?: string,
): Effect.Effect<MessageContent, CollectiveContentError> =>
  Schema.encode(CollectiveValue)(value).pipe(
    Effect.flatMap((encoded) =>
      Schema.decodeUnknown(Content)([
        ...(text === undefined ? [] : [{ type: "text", text }]),
        { type: "data", value: { [COLLECTIVE_DATA_KEY]: encoded } },
      ]),
    ),
    Effect.catchTag("ParseError", () =>
      Effect.fail(new CollectiveContentError()),
    ),
  );

/* eslint-enable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Restore the package naming rules after the Schema/type pairs. */
