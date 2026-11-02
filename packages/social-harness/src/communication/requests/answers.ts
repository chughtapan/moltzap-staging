/** @file Request form normalization, reply validation and the private operation wire format. */

import type { ParseOptions } from "effect/SchemaAST";

import {
  Array as Arr,
  Data,
  Effect,
  Either,
  JSONSchema,
  Option,
  ParseResult,
  Predicate,
  Schema,
} from "effect";
import { randomBytes } from "node:crypto";

import type { AnswerOutcome } from "./index.js";

import { exactStruct } from "../../encoding/canonical-json.js";
import { AgentId } from "../../identity/agent.js";
import { WIRE_VERSION } from "../../identity/wire-version.js";
import { Content, MessageReference, wireProfiles } from "../records/values.js";
import {
  AcceptResponse,
  type AnswerContent,
  type AnswerFieldFailures,
  answerFieldsText,
  collectiveClosure,
  CollectiveId,
  DeclineResponse,
  describeIssues,
  EpochMillis,
  RequestedSchemaInput,
  RequestOperation,
  type CollectiveResponse as Response,
} from "./contracts.js";

/* eslint-disable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Effect schemas preserve their specification names beside derived types. */
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

type MessageContent = typeof Content.Type;

/** The `data` part key under which the collective layer carries its value. */
export const COLLECTIVE_DATA_KEY = wireProfiles[WIRE_VERSION].collectiveDataKey;

/**
 * The MCP form-mode `requestedSchema`: a flat object of primitive properties.
 * Every `required` name must be a declared property, since an answer can carry
 * no other field.
 */
export const RequestedSchema = exactStruct({
  ...RequestedSchemaInput.fields,
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
export type RequestedSchema = typeof RequestedSchema.Type;

const decodeCollectiveId = Schema.decodeUnknownSync(CollectiveId);

const decodeAgentId = Schema.decodeUnknownSync(AgentId);

/**
 * A question to every member. `operationId` embeds the requester's immutable
 * identity. `deadlineAt` is absolute epoch milliseconds: the
 * sending endpoint converts the model's relative duration at send, and
 * endpoints assume zero clock skew.
 */
const CollectingOperation = exactStruct({
  kind: Schema.Literal("operation"),
  op: RequestOperation,
  operationId: CollectiveId,
  deadlineAt: EpochMillis,
  requestedSchema: RequestedSchema,
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

/**
 * Any object value that carries the collective key; other keys are ignored.
 * `exact` keeps a missing key from decoding as `undefined`.
 */
const CollectiveCarrier = Schema.Struct({
  [COLLECTIVE_DATA_KEY]: Schema.Unknown,
}).annotations({ parseOptions: { exact: true } });

/**
 * Decode one untrusted collective value.
 * @param value The value found under `COLLECTIVE_DATA_KEY`.
 * @returns The validated value.
 */
export const decodeCollectiveValue = Schema.decodeUnknown(CollectiveValue);

/**
 * The string formats a form-mode string field may require of an answer.
 *
 * Every endpoint must judge one answer the same way, since an all_gather's
 * members agree on its result, so each format is the JSON Schema 2020-12
 * format exactly as the full-mode `ajv-formats` validator judges it: `date`
 * and `date-time` per RFC 3339 with a time zone required and a leap second
 * only at 23:59 UTC, `email` as an RFC 5322 dot-atom at a dotted host name,
 * and `uri` as an RFC 3986 absolute URI. The URI grammar also admits a single
 * slash before an authority, which that validator admits.
 */

/** The formats a form-mode string field may name. */
export type AnswerFormat = "email" | "uri" | "date" | "date-time";

const DAYS_IN_MONTH = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

const DATE = /^(\d{4})-(\d{2})-(\d{2})$/;

const TIME =
  /^(\d{2}):(\d{2}):(\d{2}(?:\.\d+)?)(z|([+-])(\d{2})(?::?(\d{2}))?)$/i;

const DATE_TIME_SEPARATOR = /t|\s/i;

const atom = "[a-z0-9!#$%&'*+/=?^_`{|}~-]+";

const hostLabel = "[a-z0-9](?:[a-z0-9-]*[a-z0-9])?";

const EMAIL = new RegExp(
  `^${atom}(?:\\.${atom})*@(?:${hostLabel}\\.)+${hostLabel}$`,
  "i",
);

const hex = "[0-9a-f]";

const pctEncoded = `%${hex}{2}`;

const unreserved = "a-z0-9\\-._~";

const subDelims = "!$&'()*+;=";

const decOctet = "(?:25[0-5]|2[0-4]\\d|[01]?\\d\\d?)";

const ipv4 = `(?:${decOctet}\\.){3}${decOctet}`;

const h16 = `${hex}{1,4}`;

const ls32 = `(?:${h16}:${h16}|${ipv4})`;

const ipv6 = [
  `(?:${h16}:){6}${ls32}`,
  `::(?:${h16}:){5}${ls32}`,
  `(?:${h16})?::(?:${h16}:){4}${ls32}`,
  `(?:(?:${h16}:){0,1}${h16})?::(?:${h16}:){3}${ls32}`,
  `(?:(?:${h16}:){0,2}${h16})?::(?:${h16}:){2}${ls32}`,
  `(?:(?:${h16}:){0,3}${h16})?::${h16}:${ls32}`,
  `(?:(?:${h16}:){0,4}${h16})?::${ls32}`,
  `(?:(?:${h16}:){0,5}${h16})?::${h16}`,
  `(?:(?:${h16}:){0,6}${h16})?::`,
].join("|");

const ipFuture = `v${hex}+\\.[${unreserved}${subDelims}:]+`;

const host = `(?:\\[(?:${ipv6}|${ipFuture})\\]|${ipv4}|(?:[${unreserved}${subDelims}]|${pctEncoded})*)`;

const userinfo = `(?:[${unreserved}${subDelims}:]|${pctEncoded})*`;

const pchar = `(?:[${unreserved}${subDelims}:@]|${pctEncoded})`;

const segments = `(?:\\/${pchar}*)*`;

const hierPart = [
  `\\/?\\/(?:${userinfo}@)?${host}(?::\\d*)?${segments}`,
  `\\/(?:${pchar}+${segments})?`,
  `${pchar}+${segments}`,
].join("|");

const queryOrFragment = `(?:[${unreserved}${subDelims}:@/?]|${pctEncoded})*`;

const URI = new RegExp(
  `^[a-z][a-z0-9+\\-.]*:(?:${hierPart})(?:\\?${queryOrFragment})?(?:#${queryOrFragment})?$`,
  "i",
);

/** A full-time's clock reading and its offset from UTC. */
interface ClockTime {
  readonly hour: number;
  readonly minute: number;
  readonly second: number;
  readonly offsetSign: number;
  readonly offsetHour: number;
  readonly offsetMinute: number;
}

type AnswerValue = AnswerContent[string];

type MultiSelectItems = Extract<
  FormField,
  { readonly items: unknown }
>["items"];

/** One keyword's check: each failure it finds, as `keyword: requirement`. */
type KeywordCheck = (field: FormField, value: AnswerValue) => readonly string[];

const ALLOWED_VALUES = "must be one of the allowed values";

/** Every JSON Schema check a form field's keywords make of an answer. */
const keywordChecks: readonly KeywordCheck[] = [
  typeIssues,
  limitIssues,
  formatIssues,
  enumIssues,
  oneOfIssues,
  itemIssues,
];

/**
 * Why one answer field failed its request's schema; `detail` names each
 * keyword an `invalid` field fails.
 */
type CollectiveFieldFailure = AnswerFieldFailures[number];

/**
 * Read the requester embedded in an operation ID. Request admission compares
 * this identity with the certified author before accepting the operation.
 */
export function collectiveRequesterOf(operationId: CollectiveId): AgentId {
  return decodeAgentId(
    `agt_${Buffer.from(operationId.slice(4), "base64url")
      .subarray(0, 16)
      .toString("base64url")}`,
  );
}

/** Distinct requesters retain distinct operation IDs even when their random bytes match. */
export function mintCollectiveId(requester: AgentId) {
  return Effect.sync(() =>
    decodeCollectiveId(
      `col_${Buffer.concat([
        Buffer.from(requester.slice(4), "base64url"),
        randomBytes(32),
      ]).toString("base64url")}`,
    ),
  );
}

/**
 * Find and decode the collective value in one message's content.
 * @param content Certified message content.
 * @returns The decoded value, or none when the content carries no collective part.
 */
export function readCollectiveValue(
  content: MessageContent,
): Effect.Effect<Option.Option<CollectiveValue>, CollectivePartInvalidError> {
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
}

/**
 * Build message content from a collective value and optional message text. The
 * public `Content` schema applies the one canonical size limit.
 * @param value The collective value to carry in the data part.
 * @param text The message body; for gather and all_gather, the question.
 * @returns Content with the text first and the collective part last.
 */
export function encodeCollectiveContent(
  value: CollectiveValue,
  text?: string,
): Effect.Effect<MessageContent, CollectiveContentError> {
  return Schema.encode(CollectiveValue)(value).pipe(
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
}

/**
 * Whether a string satisfies a format.
 * @param format The format a string field names.
 * @param text The answer's string.
 * @returns True when the string is in the format.
 */
export function matchesFormat(format: AnswerFormat, text: string): boolean {
  switch (format) {
    case "email":
      return EMAIL.test(text);
    case "uri":
      return URI.test(text);
    case "date":
      return isDate(text);
    case "date-time":
      return isDateTime(text);
    default: {
      const exhaustive: never = format;
      return exhaustive;
    }
  }
}

/**
 * Record a received response as the member's outcome. An `accept` whose content
 * fails the request's schema is recorded as invalid rather than rejected.
 * @param requestedSchema The schema the request carried.
 * @param response The member's decoded response.
 * @returns The member's outcome.
 */
export function outcomeOfResponse(
  requestedSchema: RequestedSchema,
  response: Response,
): Effect.Effect<AnswerOutcome> {
  switch (response.action) {
    case "accept":
      return validateAnswer(requestedSchema, response.content).pipe(
        Effect.match({
          onFailure: (fields): AnswerOutcome => ({
            kind: "invalid",
            reason: answerFieldsText(fields),
          }),
          onSuccess: (content): AnswerOutcome => ({
            kind: "answered",
            content,
          }),
        }),
      );
    case "decline":
      return Effect.succeed({ kind: "declined" });
    default: {
      const exhaustive: never = response;
      return exhaustive;
    }
  }
}

/**
 * Check an answer against the schema its request carried.
 * @param requestedSchema The request's form-mode schema.
 * @param content The member's answer.
 * @returns The answer unchanged, or every failing field.
 */
export function validateAnswer(
  requestedSchema: RequestedSchema,
  content: AnswerContent,
): Effect.Effect<AnswerContent, AnswerFieldFailures> {
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

function decoderOf<A extends FormField, I>(
  schema: Schema.Schema<A, I>,
): ShapeDecoder {
  return (property) =>
    Schema.decodeUnknownEither(schema)(property, shapeOptions);
}

function declares(property: unknown, ...types: readonly string[]): boolean {
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

function expectedText(expected: string): string {
  return `Invalid input: expected ${expected}`;
}

function collectivePartValue(
  part: MessageContent[number],
): Option.Option<unknown> {
  return part.type === "data"
    ? Schema.decodeUnknownOption(CollectiveCarrier)(part.value).pipe(
        Option.map((carrier) => carrier[COLLECTIVE_DATA_KEY]),
      )
    : Option.none();
}

/** An RFC 3339 date and time, split at `T` or whitespace. */
function isDateTime(text: string): boolean {
  const parts = text.split(DATE_TIME_SEPARATOR);
  if (parts.length !== 2) {
    return false;
  }
  const [date = "", time = ""] = parts;
  return isDate(date) && isTime(time);
}

function isDate(text: string): boolean {
  const match = DATE.exec(text);
  return (
    match !== null &&
    isCalendarDay(Number(match[1]), Number(match[2]), Number(match[3]))
  );
}

function isCalendarDay(year: number, month: number, day: number): boolean {
  return month >= 1 && month <= 12 && day >= 1 && day <= daysIn(year, month);
}

function daysIn(year: number, month: number): number {
  return month === 2 && isLeapYear(year) ? 29 : (DAYS_IN_MONTH[month] ?? 0);
}

function isLeapYear(year: number): boolean {
  return year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
}

/** An RFC 3339 full-time, offset included. */
function isTime(text: string): boolean {
  const match = TIME.exec(text);
  if (match === null) {
    return false;
  }
  const time: ClockTime = {
    hour: Number(match[1]),
    minute: Number(match[2]),
    second: Number(match[3]),
    offsetSign: match[5] === "-" ? -1 : 1,
    offsetHour: Number(match[6] ?? 0),
    offsetMinute: Number(match[7] ?? 0),
  };
  return (
    time.offsetHour <= 23 &&
    time.offsetMinute <= 59 &&
    (isOrdinaryTime(time) || isLeapSecond(time))
  );
}

function isOrdinaryTime({ hour, minute, second }: ClockTime): boolean {
  return hour <= 23 && minute <= 59 && second < 60;
}

/**
 * A second numbered 60 is a leap second only when the reading, shifted to
 * UTC, is 23:59; -1 is that instant reached by borrowing across midnight.
 */
function isLeapSecond(time: ClockTime): boolean {
  const utcMinute = time.minute - time.offsetMinute * time.offsetSign;
  const utcHour =
    time.hour - time.offsetHour * time.offsetSign - (utcMinute < 0 ? 1 : 0);
  const lastHour = utcHour === 23 || utcHour === -1;
  const lastMinute = utcMinute === 59 || utcMinute === -1;
  return lastHour && lastMinute && time.second < 61;
}

function fieldFailure(
  requestedSchema: RequestedSchema,
  content: AnswerContent,
  field: string,
): CollectiveFieldFailure | undefined {
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
  const issues = answerIssues(definition, value);
  return issues.length === 0
    ? undefined
    : { field, reason: "invalid", detail: issues.join(", ") };
}

/**
 * Field names come from peers, so a lookup ignores the prototype chain: an
 * answer field named `constructor` must not find `Object.prototype.constructor`.
 */
function ownValue<Value>(
  record: Readonly<Record<string, Value>>,
  key: string,
): Value | undefined {
  return Object.hasOwn(record, key) ? record[key] : undefined;
}

/**
 * Every failing keyword of one answer value. A titled single-select's value
 * must equal exactly one option, so options repeating a `const` admit
 * neither.
 * @param field The field's admitted property schema.
 * @param value The answer's value for the field.
 * @returns Each failing keyword with what it requires; empty when the value
 *   satisfies the field.
 */
function answerIssues(field: FormField, value: AnswerValue): readonly string[] {
  return keywordChecks.flatMap((check) => check(field, value));
}

function typeIssues(field: FormField, value: AnswerValue): readonly string[] {
  return hasType(field, value) ? [] : [`type: must be ${field.type}`];
}

function hasType(field: FormField, value: AnswerValue): boolean {
  switch (field.type) {
    case "string":
      return typeof value === "string";
    case "number":
      return typeof value === "number";
    case "integer":
      return typeof value === "number" && Number.isInteger(value);
    case "boolean":
      return typeof value === "boolean";
    case "array":
      return Array.isArray(value);
    default: {
      const exhaustive: never = field;
      return exhaustive;
    }
  }
}

/**
 * Report every failed bound without skipping later diagnostics. String bounds
 * count Unicode code points, so an astral character counts once.
 */
function limitIssues(field: FormField, value: AnswerValue): readonly string[] {
  switch (field.type) {
    case "string":
      return stringLimitIssues(field, value);
    case "number":
    case "integer":
      return numberLimitIssues(field, value);
    case "array":
      return arrayLimitIssues(field, value);
    case "boolean":
      return [];
    default: {
      const exhaustive: never = field;
      return exhaustive;
    }
  }
}

function stringLimitIssues(
  field: Extract<FormField, { readonly type: "string" }>,
  value: AnswerValue,
): readonly string[] {
  if (typeof value !== "string" || "enum" in field || "oneOf" in field) {
    return [];
  }
  const length = Array.from(value).length;
  return [
    ...(field.minLength !== undefined && length < field.minLength
      ? [`minLength: must have at least ${field.minLength} characters`]
      : []),
    ...(field.maxLength !== undefined && length > field.maxLength
      ? [`maxLength: must have at most ${field.maxLength} characters`]
      : []),
  ];
}

function numberLimitIssues(
  field: Extract<FormField, { readonly type: "number" | "integer" }>,
  value: AnswerValue,
): readonly string[] {
  if (typeof value !== "number") {
    return [];
  }
  return [
    ...(field.minimum !== undefined && value < field.minimum
      ? [`minimum: must be >= ${field.minimum}`]
      : []),
    ...(field.maximum !== undefined && value > field.maximum
      ? [`maximum: must be <= ${field.maximum}`]
      : []),
  ];
}

function arrayLimitIssues(
  field: Extract<FormField, { readonly type: "array" }>,
  value: AnswerValue,
): readonly string[] {
  if (typeof value !== "object") {
    return [];
  }
  return [
    ...(field.minItems !== undefined && value.length < field.minItems
      ? [`minItems: must have at least ${field.minItems} items`]
      : []),
    ...(field.maxItems !== undefined && value.length > field.maxItems
      ? [`maxItems: must have at most ${field.maxItems} items`]
      : []),
  ];
}

function formatIssues(field: FormField, value: AnswerValue): readonly string[] {
  if (
    typeof value !== "string" ||
    !("format" in field) ||
    field.format === undefined
  ) {
    return [];
  }
  return matchesFormat(field.format, value)
    ? []
    : [`format: must match format "${field.format}"`];
}

function enumIssues(field: FormField, value: AnswerValue): readonly string[] {
  return "enum" in field && !field.enum.some((allowed) => allowed === value)
    ? [`enum: ${ALLOWED_VALUES}`]
    : [];
}

function oneOfIssues(field: FormField, value: AnswerValue): readonly string[] {
  return "oneOf" in field &&
    field.oneOf.filter((option) => option.const === value).length !== 1
    ? ["oneOf: must equal exactly one option's const"]
    : [];
}

function itemIssues(field: FormField, value: AnswerValue): readonly string[] {
  if (typeof value !== "object" || !("items" in field)) {
    return [];
  }
  const { items } = field;
  return value.flatMap((item, index) => selectedItemIssues(items, item, index));
}

function selectedItemIssues(
  items: MultiSelectItems,
  item: string,
  index: number,
): readonly string[] {
  if ("enum" in items) {
    return items.enum.includes(item)
      ? []
      : [`items[${index}].enum: ${ALLOWED_VALUES}`];
  }
  return items.anyOf.some((option) => option.const === item)
    ? []
    : [`items[${index}].anyOf: must equal some option's const`];
}

/* eslint-enable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Restore naming after the schema owner. */
