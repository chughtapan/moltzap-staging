/** @file Messages as validated values, with their content grammar and constructors, and the form a filled set of fields is checked against. */

import type { ParseOptions } from "effect/SchemaAST";
import { Ajv } from "ajv";
import { fullFormats } from "ajv-formats/dist/formats.js";
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
import { createHash, randomBytes } from "node:crypto";
import {
  canonicalIdentifier,
  compareAgentIds,
  compareAscii,
  encodeJson,
  exactStruct,
  isCanonicalIdentifier,
  isStrictlyAscending,
  issuesText,
  type JsonRepresentationError,
  representationFailure,
} from "../encoding.js";
import { AgentCard, AgentId } from "../identity.js";

/** A refused send or read names one of these reasons. */
export class CommunicationError extends Data.TaggedError("CommunicationError")<{
  readonly reason:
    | "membership-invalid"
    | "content-invalid"
    | "persistence-failed"
    | "message-conflict"
    | "invalid-cursor";
}> {}

/** The largest membership of one conversation. */
export const maximumMembers = 32;

/** The byte limit of one message's content as JSON text. */
const maximumContentBytes = 32_768;
const utf8Encoder = new TextEncoder();

/**
 * One author-scoped message id. Retrying a persisted message reuses it; a new
 * message receives a fresh one.
 */
export const MsgId = Schema.String.pipe(
  Schema.filter((value) => isCanonicalIdentifier("msg_", value), {
    identifier: "MsgId",
    description: "Canonical author-scoped message id",
  }),
  Schema.brand("MsgId"),
  Schema.annotations({ identifier: "MsgId" }),
);
/**
 * A validated author-scoped message id.
 * @useDeclaredType
 */
export type MsgId = typeof MsgId.Type;

/** The author and its message id identify the same message through retries and recovery. */
export const MessageReference = exactStruct({
  authorAgentId: AgentId,
  msgId: MsgId,
});
/**
 * An author's message id identifies the same message through delivery and restart.
 * @useDeclaredType
 */
export type MessageReference = typeof MessageReference.Type;

/** A string without lone surrogates, so it survives UTF-8. */
export const wellFormedString = Schema.String.pipe(
  Schema.filter(hasWellFormedUnicode, {
    identifier: "WellFormedUnicodeString",
  }),
);

/* eslint-disable agent-code-guard/no-nullish-type-aliases -- JSON includes null as a first-class value. */
/** A value accepted by the closed semantic content boundary. */
type JsonValue =
  | null
  | boolean
  | number
  | string
  | readonly JsonValue[]
  | { readonly [key: string]: JsonValue };
/* eslint-enable agent-code-guard/no-nullish-type-aliases -- Restore the absence rule outside JSON values. */

/** Runtime validation for the closed recursive JSON value. */
const JsonValue: Schema.Schema<JsonValue> = Schema.suspend(() =>
  Schema.Union(
    Schema.Null,
    Schema.Boolean,
    Schema.JsonNumber,
    wellFormedString,
    Schema.Array(JsonValue),
    Schema.Record({ key: wellFormedString, value: JsonValue }),
  ),
).annotations({ identifier: "JsonValue" });

/** One exact semantic part of a message. */
export const ContentPart = Schema.Union(
  exactStruct({ type: Schema.Literal("text"), text: wellFormedString }),
  exactStruct({ type: Schema.Literal("data"), value: JsonValue }),
).annotations({ identifier: "ContentPart" });
/**
 * A validated semantic message part.
 * @useDeclaredType
 */
export type ContentPart = typeof ContentPart.Type;

const contentStructure = Schema.NonEmptyArray(ContentPart);

/** Nonempty semantic content whose JSON text is at most 32,768 bytes. */
export const Content = contentStructure.pipe(
  Schema.filter(contentFits),
  Schema.annotations({ identifier: "Content" }),
);
/** Validated nonempty semantic content. */
export type Content = typeof Content.Type;

const ConversationId = canonicalIdentifier("ConversationId", "cnv_", 32);
const ConversationMembers = exactStruct({
  kind: Schema.Literal("conversation_identity"),
  memberAgentIds: Schema.Tuple([AgentId, AgentId], AgentId).pipe(
    Schema.maxItems(maximumMembers),
  ),
});

/**
 * The identifier of the conversation among these members: a hash of their
 * agent identifiers, which must be given in ascending order with no repeats.
 */
export const deriveConversationId = (
  memberAgentIds: (typeof ConversationMembers.Type)["memberAgentIds"],
): Effect.Effect<typeof ConversationId.Type, JsonRepresentationError> =>
  isStrictlyAscending(memberAgentIds, compareAgentIds)
    ? encodeJson(ConversationMembers, {
        kind: "conversation_identity" as const,
        memberAgentIds,
      }).pipe(
        Effect.map(
          (members) =>
            `cnv_${createHash("sha256")
              .update("org.social-harness/client/v2/conversation\0")
              .update(members)
              .digest("base64url")}`,
        ),
        Effect.flatMap(Schema.decodeUnknown(ConversationId)),
        Effect.mapError(representationFailure),
      )
    : Effect.fail(representationFailure());

/** A fresh author-scoped id uses 32 random bytes before any assessment or send. */
const mintMsgId = (): Effect.Effect<MsgId, JsonRepresentationError> =>
  Effect.try({
    try: () => `msg_${randomBytes(32).toString("base64url")}`,
    catch: representationFailure,
  }).pipe(
    Effect.flatMap(Schema.decodeUnknown(MsgId)),
    Effect.mapError(representationFailure),
  );

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

/** Omit cursor to begin and pass the returned nextCursor unchanged. */
export const PaginatedRequest = exactStruct({
  cursor: Schema.optional(Schema.String),
});
/** Selection arguments remain unchanged between pages.
 * @useDeclaredType
 */
export type PaginatedRequest = typeof PaginatedRequest.Type;
/** Absence of nextCursor marks the end of the listing. */
export const Page = exactStruct({
  nextCursor: Schema.optional(Schema.String),
});
/** Cursors confer no access and are valid only for the read that returned them.
 * @useDeclaredType
 */
export type Page = typeof Page.Type;

/** The most inbox entries, history records or conversation addresses one page holds. */
export const PAGE_SIZE = 50;

/** Recipients exclude the author and appear once each in agent-name order. */
const AuthorAndRecipients = exactStruct({
  author: AgentCard,
  recipients: Schema.NonEmptyArray(AgentCard).pipe(
    Schema.maxItems(maximumMembers - 1),
  ),
}).pipe(
  Schema.filter(
    (message) =>
      message.recipients.every(
        (recipient, index) =>
          recipient.agentId !== message.author.agentId &&
          recipient.agentName !== message.author.agentName &&
          (index === 0 ||
            compareAscii(
              message.recipients[index - 1]?.agentName ?? "",
              recipient.agentName,
            ) < 0),
      ) &&
      new Set(message.recipients.map((recipient) => recipient.agentId)).size ===
        message.recipients.length,
  ),
  Schema.annotations({ identifier: "AuthorAndRecipients" }),
);

/** `replyToMsgId` is the id of the message this one answers, when its sender named one. */
const messageSchema = AuthorAndRecipients.pipe(
  Schema.extend(
    exactStruct({
      msgId: MsgId,
      replyToMsgId: Schema.optional(MsgId),
      content: Content,
    }),
  ),
  Schema.annotations({ identifier: "Message" }),
);
/**
 * Authorship, recipients and content remain fixed through delivery and
 * restart.
 */
export type Message = typeof messageSchema.Type;

/** Owns construction of a message. */
export const Message = Object.assign(messageSchema, {
  create(
    input: Pick<Message, "author" | "recipients" | "content" | "replyToMsgId">,
  ) {
    return Effect.gen(function* () {
      return yield* messageWithId(
        input.author,
        input.recipients,
        { msgId: yield* mintMsgId(), replyToMsgId: input.replyToMsgId },
        input.content,
      );
    }).pipe(
      Effect.withSpan("Message.create"),
      Effect.catchTags({
        ParseError: () => new CommunicationError({ reason: "content-invalid" }),
        JsonRepresentationError: () =>
          new CommunicationError({ reason: "content-invalid" }),
      }),
    );
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

/**
 * A message with a message id its caller chose, to recipients in any order.
 * `replyToMsgId` names the message it answers.
 */
export function messageWithId(
  author: AgentCard,
  recipients: readonly AgentCard[],
  { msgId, replyToMsgId }: Pick<Message, "msgId" | "replyToMsgId">,
  content: Content,
) {
  return Effect.try({
    try: () =>
      structuredClone({
        author,
        recipients: [...recipients].sort((left, right) =>
          compareAscii(left.agentName, right.agentName),
        ),
        msgId,
        ...(replyToMsgId === undefined ? {} : { replyToMsgId }),
        content,
      }),
    catch: () =>
      new ParseResult.ParseError({
        issue: new ParseResult.Type(
          Message.ast,
          undefined,
          "Message cannot be captured",
        ),
      }),
  }).pipe(
    Effect.flatMap((captured) => Schema.decodeUnknown(Message)(captured)),
  );
}

/** Includes the author and every recipient in agent-name order. */
export function messageMembers(
  message: Pick<Message, "author" | "recipients">,
): readonly AgentCard[] {
  return [message.author, ...message.recipients].sort((left, right) =>
    compareAscii(left.agentName, right.agentName),
  );
}

function hasWellFormedUnicode(value: string): boolean {
  for (let index = 0; index < value.length; index += 1) {
    const codeUnit = value.charCodeAt(index);
    if (isTrailingSurrogate(codeUnit)) {
      return false;
    }
    if (!isLeadingSurrogate(codeUnit)) {
      continue;
    }
    index += 1;
    if (!isTrailingSurrogate(value.charCodeAt(index))) {
      return false;
    }
  }
  return true;
}

function isLeadingSurrogate(codeUnit: number): boolean {
  return codeUnit >= 0xd800 && codeUnit <= 0xdbff;
}

function isTrailingSurrogate(codeUnit: number): boolean {
  return codeUnit >= 0xdc00 && codeUnit <= 0xdfff;
}

function contentFits(
  content: readonly [ContentPart, ...ContentPart[]],
): boolean {
  return Either.match(
    Either.try(() => JSON.stringify(content)),
    {
      onLeft: () => false,
      onRight: (text) =>
        utf8Encoder.encode(text).byteLength <= maximumContentBytes,
    },
  );
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
