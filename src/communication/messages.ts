/** @file Messages, collective requests and collective responses as validated values, with their content grammar and constructors. */

import type { ParseOptions } from "effect/SchemaAST";
import { Ajv } from "ajv";
import { fullFormats } from "ajv-formats/dist/formats.js";
import {
  Array as Arr,
  Clock,
  Data,
  Effect,
  Either,
  JSONSchema,
  Option,
  ParseResult,
  Predicate,
  Schema,
  SchemaAST,
} from "effect";
import { createHash, randomBytes } from "node:crypto";
import {
  canonicalIdentifier,
  type ClientRepresentationError,
  compareAgentIds,
  compareAscii,
  encodeJson,
  exactStruct,
  isCanonicalIdentifier,
  isStrictlyAscending,
  representationFailure,
} from "../encoding.js";
import { AGENT_NAME_RULE, AgentCard, AgentId, AgentName } from "../identity.js";
import { CommunicationError, issuesText } from "./errors.js";

/** The byte limit of one message's content as JSON text. */
const maximumContentBytes = 32_768;
const utf8Encoder = new TextEncoder();

/**
 * One author-scoped message identity. Retrying a persisted message reuses it;
 * a new message receives a fresh identity.
 */
export const MsgId = Schema.String.pipe(
  Schema.filter((value) => isCanonicalIdentifier("msg_", value), {
    identifier: "MsgId",
    description: "Canonical author-scoped message identity",
  }),
  Schema.brand("MsgId"),
  Schema.annotations({ identifier: "MsgId" }),
);
/**
 * A validated author-scoped message identity.
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

/** The largest group membership a conversation address or descriptor admits. */
export const maximumMembers = 32;

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
): Effect.Effect<typeof ConversationId.Type, ClientRepresentationError> =>
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

/** The scheme of an address naming one agent. */
export const AGENT_ADDRESS_PREFIX = "agent:";
/** The scheme of an address listing a group's members. */
export const GROUP_ADDRESS_PREFIX = "group:";

const isAgentName = Schema.is(AgentName);

/**
 * The agent name an `agent:` address names.
 * @param value A candidate address.
 * @returns The name, or undefined when the value is not an agent address.
 */
const parseAgentAddress = (value: string): string | undefined => {
  if (!value.startsWith(AGENT_ADDRESS_PREFIX)) {
    return undefined;
  }
  const name = value.slice(AGENT_ADDRESS_PREFIX.length);
  return isAgentName(name) ? name : undefined;
};

/**
 * The names a `group:` address lists, in the order given.
 * @param value A candidate address.
 * @returns The names, or undefined when the value is not a group address.
 */
const parseGroupAddress = (value: string): readonly string[] | undefined => {
  if (!value.startsWith(GROUP_ADDRESS_PREFIX)) {
    return undefined;
  }
  const names = value.slice(GROUP_ADDRESS_PREFIX.length).split(",");
  return names.length > 0 && names.every((name) => isAgentName(name))
    ? names
    : undefined;
};

const addressInput = Schema.String.pipe(
  Schema.filter(
    (value) =>
      parseAgentAddress(value) !== undefined ||
      parseGroupAddress(value) !== undefined,
    {
      identifier: "MessageAddressInput",
      description: `agent:<name>, or group: then agent names separated by commas, with no spaces and no agent: prefix; a name is ${AGENT_NAME_RULE}`,
    },
  ),
  Schema.brand("MessageAddressInput"),
);

/** An explicit direct destination using one agent name. */
export const AgentAddress = addressInput.pipe(
  Schema.filter((value) => parseAgentAddress(value) !== undefined),
  Schema.brand("AgentAddress"),
  Schema.annotations({ identifier: "AgentAddress" }),
);
/**
 * A validated direct destination.
 * @useDeclaredType
 */
export type AgentAddress = typeof AgentAddress.Type;

/** A complete fixed-member group address in unsigned ASCII name order. */
export const GroupAddress = addressInput.pipe(
  Schema.filter(isCanonicalGroupAddress),
  Schema.brand("GroupAddress"),
  Schema.annotations({ identifier: "GroupAddress" }),
);
/**
 * A validated canonical complete group destination.
 * @useDeclaredType
 */
export type GroupAddress = typeof GroupAddress.Type;

/** Either accepted destination input, including noncanonical group order. */
export const MessageAddressInput = addressInput;
/**
 * A validated explicit destination input.
 * @useDeclaredType
 */
export type MessageAddressInput = typeof MessageAddressInput.Type;

/** A fresh author-scoped id uses 32 random bytes before any assessment or send. */
const mintMsgId = (): Effect.Effect<MsgId, ClientRepresentationError> =>
  Effect.try({
    try: () => `msg_${randomBytes(32).toString("base64url")}`,
    catch: representationFailure,
  }).pipe(
    Effect.flatMap(Schema.decodeUnknown(MsgId)),
    Effect.mapError(representationFailure),
  );

/** A gather keeps responses private to the requester; an all_gather shares them with the group. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- Collective request and result schemas share this closed pair.
export const CollectiveOp = Schema.Literal("gather", "all_gather");
/** How a collective request asks its recipients, fixed for its lifetime. */
export type CollectiveOp = typeof CollectiveOp.Type;

/**
 * What an accepting collective response carries, field by field. Its value
 * space is MCP `ElicitResult` content's; the request's schema decides which
 * fields apply.
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
  description: "The response, one field per requestedSchema property",
});
/** Structurally valid content of an accepting collective response. */
export type ResponseContent = typeof ResponseContent.Type;

/** The longest deadline a collective request may state: 30 days, in seconds. */
const MAXIMUM_DEADLINE_SECONDS = 2_592_000;

/** A collective request's relative deadline: whole seconds, up to 30 days. */
export const DeadlineSeconds = Schema.Number.pipe(
  Schema.int(),
  Schema.between(1, MAXIMUM_DEADLINE_SECONDS),
);

/** The latest instant a `Date` represents, in epoch milliseconds. */
const MAXIMUM_EPOCH_MILLIS = 8_640_000_000_000_000;

/** Positive Unix milliseconds within the range a `Date` can render. */
const EpochMillis = Schema.Int.pipe(Schema.between(1, MAXIMUM_EPOCH_MILLIS));

/** An absolute deadline on the daemon's clock. */
type EpochMillis = typeof EpochMillis.Type;

/** The requester's first completion decision, retained before closing or delivering a result. */
export const collectiveClosure = exactStruct({
  closedAt: Schema.Int.pipe(
    Schema.between(0, MAXIMUM_EPOCH_MILLIS),
  ).annotations({
    description: "Unix milliseconds when the requester first froze completion",
  }),
  closedBy: Schema.Literal("settled", "deadline").annotations({
    description:
      "Whether every recipient had an outcome or the deadline left recipients without one",
  }),
});

/** An accepting collective response, the only one that carries content. */
const acceptFields = {
  requestId: MsgId,
  action: Schema.Literal("accept"),
  content: ResponseContent,
};

const declineFields = {
  requestId: MsgId,
  action: Schema.Literal("decline"),
};

/**
 * A recipient's one response to a collective request open in the conversation
 * it is sent to: the requester's direct conversation for a gather, the group
 * for an all_gather. `requestId` is the message id of the request the
 * recipient received. Only `accept` carries content, which must satisfy that
 * request's `requestedSchema`.
 */
const collectiveResponseSchema = Schema.Union(
  exactStruct(acceptFields),
  exactStruct(declineFields),
);
/** A validated collective response. */
export type CollectiveResponse = typeof collectiveResponseSchema.Type;

/**
 * Why a collective send was refused, discriminated by `kind`: a collective
 * request whose schema is outside the form-mode grammar or whose deadline
 * passed while its check ran; or a collective response its author cannot send.
 */
const collectiveFailure = Schema.Union(
  exactStruct({
    kind: Schema.Literal("schema-invalid"),
    detail: Schema.String,
  }),
  exactStruct({
    kind: Schema.Literal("answer-invalid"),
    fields: Schema.NonEmptyArray(
      exactStruct({
        field: Schema.String,
        reason: Schema.Literal("missing", "unexpected", "invalid"),
        detail: Schema.optionalWith(Schema.String, { exact: true }),
      }),
    ),
  }),
  exactStruct({ kind: Schema.Literal("request-none") }),
  exactStruct({ kind: Schema.Literal("request-answered") }),
  exactStruct({ kind: Schema.Literal("request-expired") }),
  exactStruct({ kind: Schema.Literal("deadline-passed") }),
);
/** A validated collective failure. */
export type CollectiveFailure = typeof collectiveFailure.Type;

/**
 * Decode a collective failure carried across the loopback MCP boundary.
 * @param value Untrusted failure detail from a refused send's error data.
 * @returns The validated failure.
 */
export const decodeCollectiveFailure = Schema.decodeUnknown(collectiveFailure);

/**
 * One line per issue of a decode failure, each led by its path, such as
 * `requestedSchema.type: Expected "object", actual "obj"`, joined by `; `.
 * A model repairs its input from this text alone.
 * @param error The failed decode.
 * @returns The issues as text.
 */
const describeIssues = (error: ParseResult.ParseError): string =>
  issuesText(ParseResult.ArrayFormatter.formatErrorSync(error));

/** What each response-field problem means. */
const responseFieldText = {
  missing: "is missing",
  unexpected: "is not in the form",
  invalid: "is invalid",
} as const;

/** Every field of a collective response that fails its request's schema. */
export type ResponseFieldFailures = Extract<
  CollectiveFailure,
  { readonly kind: "answer-invalid" }
>["fields"];

/** A failure that carries nothing beyond its kind. */
type FixedFailureKind = Exclude<
  CollectiveFailure["kind"],
  "schema-invalid" | "answer-invalid"
>;

/** The text of each failure that carries nothing beyond its kind. */
const fixedFailureText: Record<FixedFailureKind, string> = {
  "request-none":
    "response failed: no collective request is open in this conversation",
  "request-answered":
    "response failed: the collective request was already answered",
  "request-expired":
    "response failed: the collective request's deadline has passed",
  "deadline-passed":
    "send failed: the deadline passed before the collective request was sent",
};

/** A collective request or response failed before it could be sent. `requestId` is the request's message id. */
export class RequestError extends Data.TaggedError("RequestError")<{
  readonly requestId?: MsgId;
  readonly failure: CollectiveFailure;
}> {
  override get message(): string {
    return describeCollectiveFailure(this.failure);
  }
}

/** Content carries a collective part that is duplicated or does not decode. */
export class CollectivePartInvalidError extends Data.TaggedError(
  "CollectivePartInvalidError",
)<{
  readonly reason: "duplicate" | "malformed";
}> {}

/** A collective part and its text do not form content within the 32,768-byte limit. */
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

/** The key of the `data` part that makes a message a collective request, response or close. */
const COLLECTIVE_DATA_KEY = "org.social-harness/collective";

/**
 * The MCP form-mode `requestedSchema`: a flat object of primitive properties.
 * Every `required` name must be a declared property, since a response can
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

/** A validated form-mode schema for one collective request. */
export type RequestedSchema = typeof RequestedSchema.Type;

/**
 * What a collective request's content says beside its question text. The
 * request is identified by its message id. `deadlineAt` is absolute epoch
 * milliseconds: the daemon converts the requester's relative deadline when
 * the request is created.
 */
const requestPart = exactStruct({
  kind: Schema.Literal("request"),
  op: CollectiveOp,
  deadlineAt: EpochMillis,
  requestedSchema: RequestedSchema,
});

const responseKind = { kind: Schema.Literal("response") };

/** A collective response as content carries it. */
const responsePart = Schema.Union(
  exactStruct({ ...responseKind, ...acceptFields }),
  exactStruct({ ...responseKind, ...declineFields }),
);

/**
 * What a collective close's content says: when and why the requester
 * completed the all_gather `requestId` names, and the responses that count.
 */
const CollectiveClose = exactStruct({
  kind: Schema.Literal("close"),
  requestId: MsgId,
  included: Schema.Array(MessageReference),
  ...collectiveClosure.fields,
});
/** A validated collective close. */
export type CollectiveClose = typeof CollectiveClose.Type;

/** The collective part of a message's content, discriminated by `kind`. */
const CollectivePart = Schema.Union(requestPart, responsePart, CollectiveClose);

/** A validated collective part. */
export type CollectivePart = typeof CollectivePart.Type;

/**
 * Any object value that carries the collective key; other keys are ignored.
 * `exact` keeps a missing key from decoding as `undefined`.
 */
const CollectiveCarrier = Schema.Struct({
  [COLLECTIVE_DATA_KEY]: Schema.Unknown,
}).annotations({ parseOptions: { exact: true } });

/**
 * Decode one untrusted collective part.
 * @param value The value found under `COLLECTIVE_DATA_KEY`.
 * @returns The validated part.
 */
const decodeCollectivePart = Schema.decodeUnknown(CollectivePart);

type ResponseFieldValue = ResponseContent[string];

/**
 * Judges a response field's value against its field as JSON Schema, reporting
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
 * Why one response field failed its request's schema; `detail` names each
 * keyword an `invalid` field fails.
 */
type ResponseFieldFailure = ResponseFieldFailures[number];

/** Omit cursor to begin and pass the returned nextCursor unchanged. */
export const PaginatedRequest = exactStruct({
  cursor: Schema.optional(Schema.String),
});
/** Selection arguments remain unchanged between pages.
 * @useDeclaredType
 */
export type PaginatedRequest = typeof PaginatedRequest.Type;
/** Absence of nextCursor marks the end of the listing. */
export const PaginatedResult = exactStruct({
  nextCursor: Schema.optional(Schema.String),
});
/** Cursors confer no access and are valid only for the read that returned them.
 * @useDeclaredType
 */
export type PaginatedResult = typeof PaginatedResult.Type;

/** Recipients exclude the author and appear once each in agent-name order. */
const MessageParticipants = exactStruct({
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
  Schema.annotations({ identifier: "MessageParticipants" }),
);

/** Adds the author-scoped identity to the captured participants. */
const MessageHeader = MessageParticipants.pipe(
  Schema.extend(exactStruct({ msgId: MsgId })),
);

/** What a collective request asks: its `op`, question, form and deadline. */
const requestTerms = {
  op: CollectiveOp,
  question: wellFormedString,
  requestedSchema: RequestedSchema,
  deadlineAt: EpochMillis,
};

const encodedMessage = MessageHeader.pipe(
  Schema.extend(exactStruct({ content: Content })),
);
const decodedMessage = encodedMessage.pipe(
  Schema.extend(
    exactStruct({
      collectiveRequest: Schema.optional(exactStruct(requestTerms)),
      collectiveResponse: Schema.optional(collectiveResponseSchema),
    }),
  ),
);
/**
 * Authorship, recipients and content remain fixed through delivery and
 * restart. A collective request or response also carries what its content
 * means, decoded, as `collectiveRequest` or `collectiveResponse`.
 */
export type Message = typeof decodedMessage.Type;

type DecodedCollectivePart = Either.Either<
  Option.Option<CollectivePart>,
  CollectivePartInvalidError
>;

/** The collective part read from each decoded message's content, or why it could not be read. */
const collectivePartByMessage = new WeakMap<Message, DecodedCollectivePart>();

const messageSchema = Schema.transformOrFail(
  encodedMessage,
  Schema.declare<Message>(Schema.is(decodedMessage), {
    [SchemaAST.SurrogateAnnotationId]: Schema.typeSchema(decodedMessage).ast,
  }),
  {
    strict: true,
    decode: (message) =>
      readCollectivePart(message.content).pipe(
        Effect.either,
        Effect.map((part) => {
          const decoded: Message = {
            ...message,
            ...collectiveMeaning(part, message.content),
          };
          collectivePartByMessage.set(decoded, part);
          return decoded;
        }),
      ),
    encode: (message) =>
      ParseResult.succeed({
        author: message.author,
        recipients: message.recipients,
        msgId: message.msgId,
        content: message.content,
      }),
  },
).annotations({ identifier: "Message" });

/** Owns construction of a message and the one decoding of its collective part. */
export const Message = Object.assign(messageSchema, {
  create(input: Pick<Message, "author" | "recipients" | "content">) {
    return Effect.gen(function* () {
      return yield* messageWithId(
        input.author,
        input.recipients,
        yield* mintMsgId(),
        input.content,
      );
    }).pipe(
      Effect.withSpan("Message.create"),
      Effect.catchTags({
        ParseError: () => new CommunicationError({ reason: "content-invalid" }),
        ClientRepresentationError: () =>
          new CommunicationError({ reason: "content-invalid" }),
      }),
    );
  },
});

type Participants = Pick<Message, "author" | "recipients">;

/**
 * A collective request as its requester states it, captured before policy
 * assessment: every recipient, the question and the absolute deadline. An
 * all_gather is sent as one group message with this `msgId`. A gather is sent
 * as one private copy per recipient, and `msgId` is the id of the copy sent to
 * the first recipient in agent-name order.
 */
const collectiveRequestSchema = MessageHeader.pipe(
  Schema.extend(
    exactStruct({
      kind: Schema.Literal("collectiveRequest"),
      ...requestTerms,
    }),
  ),
);
/** A validated collective request with every recipient. */
export type CollectiveRequest = typeof collectiveRequestSchema.Type;

/** Captures a complete collective request and its deadline before policy assessment. */
export const CollectiveRequest = Object.assign(collectiveRequestSchema, {
  create(
    input: Pick<
      CollectiveRequest,
      "author" | "recipients" | "question" | "op" | "requestedSchema"
    > & { readonly deadline: number },
  ) {
    return Effect.gen(function* () {
      const seconds = yield* Schema.decodeUnknown(DeadlineSeconds)(
        input.deadline,
      );
      const request = yield* Schema.validate(collectiveRequestSchema)({
        kind: "collectiveRequest",
        msgId: yield* mintMsgId(),
        author: input.author,
        recipients: [...input.recipients].sort((left, right) =>
          compareAscii(left.agentName, right.agentName),
        ),
        op: input.op,
        question: input.question,
        requestedSchema: input.requestedSchema,
        deadlineAt: (yield* Clock.currentTimeMillis) + seconds * 1000,
      });
      yield* collectiveRequestContent(request);
      if (request.op === "all_gather" && request.recipients.length < 2) {
        return yield* new CommunicationError({ reason: "membership-invalid" });
      }
      return request;
    }).pipe(
      Effect.withSpan("CollectiveRequest.create"),
      Effect.catchTags({
        ParseError: () => new CommunicationError({ reason: "content-invalid" }),
        ClientRepresentationError: () =>
          new CommunicationError({ reason: "content-invalid" }),
        CollectiveContentError: () =>
          new CommunicationError({ reason: "content-invalid" }),
      }),
    );
  },
});

/** A message that is a collective request: its content decoded as `collectiveRequest`. */
export const CollectiveRequestMessage = Message.pipe(
  Schema.filter(
    (
      message,
    ): message is Message & {
      readonly collectiveRequest: NonNullable<Message["collectiveRequest"]>;
    } => message.collectiveRequest !== undefined,
  ),
);
/** A collective request as one of its recipients received it. */
export type CollectiveRequestMessage = typeof CollectiveRequestMessage.Type;

/** What one recipient contributes to a collective result: its counted response, or none. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- Firewall extends complete raw outcomes with its local disclosure decision.
export const CollectiveOutcome = Schema.Union(
  exactStruct({ kind: Schema.Literal("answered"), content: ResponseContent }),
  exactStruct({ kind: Schema.Literal("declined") }),
  exactStruct({ kind: Schema.Literal("invalid"), reason: Schema.String }),
  exactStruct({ kind: Schema.Literal("no-answer") }),
);
/** A complete outcome precedes local disclosure policy. */
export type CollectiveOutcome = typeof CollectiveOutcome.Type;

/** A completed collective request with one outcome for each recipient, before policy or host addressing. */
export const CollectiveResult = exactStruct({
  request: collectiveRequestSchema,
  closedAt: Schema.Int.pipe(Schema.nonNegative()),
  closedBy: Schema.Literal("settled", "deadline"),
  outcomes: Schema.NonEmptyArray(
    exactStruct({ recipient: AgentCard, outcome: CollectiveOutcome }),
  ),
});
/** Every recipient of the request has exactly one outcome. */
export type CollectiveResult = typeof CollectiveResult.Type;

/** Validates a collective response and fixes its recipients and message id before assessment. */
export const CollectiveResponse = Object.assign(collectiveResponseSchema, {
  create(input: {
    readonly author: AgentCard;
    readonly request: CollectiveRequestMessage;
    readonly response: CollectiveResponse;
  }) {
    return Effect.gen(function* () {
      if (
        !input.request.recipients.some(
          (recipient) => recipient.agentId === input.author.agentId,
        )
      ) {
        return yield* new CommunicationError({ reason: "membership-invalid" });
      }
      if (input.response.action === "accept") {
        const fields = yield* validateResponseContent(
          input.request.collectiveRequest.requestedSchema,
          input.response.content,
        ).pipe(Effect.flip, Effect.option);
        if (Option.isSome(fields)) {
          return yield* new RequestError({
            requestId: input.request.msgId,
            failure: { kind: "answer-invalid", fields: fields.value },
          });
        }
      }
      const [first, ...rest] = messageMembers(input.request).filter(
        (member) => member.agentId !== input.author.agentId,
      );
      if (first === undefined) {
        return yield* new CommunicationError({ reason: "membership-invalid" });
      }
      return yield* Message.create({
        author: input.author,
        recipients: [first, ...rest],
        content: yield* encodeCollectiveContent({
          kind: "response",
          ...input.response,
          requestId: input.request.msgId,
        }),
      });
    }).pipe(
      Effect.withSpan("CollectiveResponse.create"),
      Effect.catchTag(
        "CollectiveContentError",
        () => new CommunicationError({ reason: "content-invalid" }),
      ),
    );
  },
});

/** The request and counted response messages that supply a collective result's peer-controlled text. */
export const CollectiveResultSources = exactStruct({
  responses: Schema.Array(Message).pipe(Schema.maxItems(maximumMembers - 1)),
  request: Schema.optionalWith(CollectiveRequestMessage, { exact: true }),
});
/** This agent's own response remains a source but needs no inbound policy check. */
export type CollectiveResultSources = typeof CollectiveResultSources.Type;

/**
 * Build the content of a collective request: its question, then its
 * collective part. Fails when the two exceed the content size limit.
 */
export function collectiveRequestContent(
  request: Pick<CollectiveRequest, keyof typeof requestTerms>,
): Effect.Effect<Content, CollectiveContentError> {
  return encodeCollectiveContent(
    {
      kind: "request",
      op: request.op,
      deadlineAt: request.deadlineAt,
      requestedSchema: request.requestedSchema,
    },
    request.question,
  );
}

/**
 * Build message content from a collective part and optional message text. The
 * public `Content` schema applies the one canonical size limit.
 * @param part The collective part to carry in the data part.
 * @param text The message text; for a collective request, its question.
 * @returns Content with the text first and the collective part last.
 */
export function encodeCollectiveContent(
  part: CollectivePart,
  text?: string,
): Effect.Effect<Content, CollectiveContentError> {
  return Schema.encode(CollectivePart)(part).pipe(
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
 * The outcome a counted response gives its author. An `accept` whose content
 * fails the request's schema is an invalid outcome rather than a refusal.
 * @param requestedSchema The schema the request carried.
 * @param response The recipient's decoded response.
 * @returns The recipient's outcome.
 */
export function outcomeOfResponse(
  requestedSchema: RequestedSchema,
  response: CollectiveResponse,
): Effect.Effect<CollectiveOutcome> {
  switch (response.action) {
    case "accept":
      return validateResponseContent(requestedSchema, response.content).pipe(
        Effect.match({
          onFailure: (fields): CollectiveOutcome => ({
            kind: "invalid",
            reason: responseFieldsText(fields),
          }),
          onSuccess: (content): CollectiveOutcome => ({
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
 * Name each failing response field, such as `field "slot" is invalid (enum)`.
 * A refused response and a recipient's `invalid` outcome read the same. Field
 * names come from peers, so each is quoted as JSON: no name can close its
 * quotes or start a new line of the text a model reads.
 * @param fields The failing fields, in validation order.
 * @returns One clause per field, joined by `; `.
 */
export function responseFieldsText(fields: ResponseFieldFailures): string {
  return fields
    .map(({ field, reason, detail }) => {
      const named = `field ${JSON.stringify(field)} ${responseFieldText[reason]}`;
      return detail === undefined ? named : `${named} (${detail})`;
    })
    .join("; ");
}

/**
 * Check a response's content against the schema its request carried.
 * @param requestedSchema The request's form-mode schema.
 * @param content The content of the recipient's accepting response.
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

/** The collective part a message's content carries, read once for each decoded message. */
export function collectivePartOf(
  message: Message,
): Effect.Effect<Option.Option<CollectivePart>, CollectivePartInvalidError> {
  const retained = collectivePartByMessage.get(message);
  if (retained !== undefined) {
    return Either.match(retained, {
      onLeft: Effect.fail,
      onRight: Effect.succeed,
    });
  }
  return readCollectivePart(message.content).pipe(
    Effect.tapBoth({
      onFailure: (error) =>
        Effect.sync(() =>
          collectivePartByMessage.set(message, Either.left(error)),
        ),
      onSuccess: (part) =>
        Effect.sync(() =>
          collectivePartByMessage.set(message, Either.right(part)),
        ),
    }),
  );
}

/** A message with a message id its caller chose, to recipients in any order. */
export function messageWithId(
  author: AgentCard,
  recipients: readonly AgentCard[],
  msgId: MsgId,
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
export function messageMembers(message: Participants): readonly AgentCard[] {
  return [message.author, ...message.recipients].sort((left, right) =>
    compareAscii(left.agentName, right.agentName),
  );
}

/** A key that orders a conversation by its members' names, direct conversations before groups. */
export function conversationNameKey(
  names: readonly AgentName[],
  localName?: AgentName,
) {
  const ordered = [...names].sort(compareAscii);
  return ordered.length === 2
    ? "0:" + ordered.filter((name) => name !== localName).join(",")
    : "1:" + ordered.join(",");
}

/** Whether a message is a collective request. */
export function isCollectiveRequestMessage(
  message: Message,
): message is CollectiveRequestMessage {
  return message.collectiveRequest !== undefined;
}

/**
 * The fields a decoded message gains from its collective part: a request with
 * its question, which is the content's text, or a response. A close, an
 * unreadable part or none adds nothing.
 */
function collectiveMeaning(
  decoded: DecodedCollectivePart,
  content: Content,
): Pick<Message, "collectiveRequest" | "collectiveResponse"> {
  const part = Either.match(decoded, {
    onLeft: () => undefined,
    onRight: Option.getOrUndefined,
  });
  if (part?.kind === "request") {
    return {
      collectiveRequest: {
        op: part.op,
        question: content
          .flatMap((entry) => (entry.type === "text" ? [entry.text] : []))
          .join("\n"),
        requestedSchema: part.requestedSchema,
        deadlineAt: part.deadlineAt,
      },
    };
  }
  if (part?.kind === "response") {
    return {
      collectiveResponse:
        part.action === "accept"
          ? {
              requestId: part.requestId,
              action: part.action,
              content: part.content,
            }
          : { requestId: part.requestId, action: part.action },
    };
  }
  return {};
}

/**
 * Find and decode the collective part in one message's content.
 * @param content Message content.
 * @returns The decoded part, or none when the content carries no collective part.
 */
function readCollectivePart(
  content: Content,
): Effect.Effect<Option.Option<CollectivePart>, CollectivePartInvalidError> {
  const [only, ...rest] = content.flatMap((part) =>
    Option.toArray(collectivePartValue(part)),
  );
  if (only === undefined) {
    return Effect.succeed(Option.none());
  }
  if (rest.length > 0) {
    return Effect.fail(new CollectivePartInvalidError({ reason: "duplicate" }));
  }
  return decodeCollectivePart(only).pipe(
    Effect.map(Option.some),
    Effect.catchTag("ParseError", () =>
      Effect.fail(new CollectivePartInvalidError({ reason: "malformed" })),
    ),
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

function isCanonicalGroupAddress(value: string): boolean {
  const names = parseGroupAddress(value);
  if (
    names === undefined ||
    names.length < 3 ||
    names.length > maximumMembers
  ) {
    return false;
  }
  return isStrictlyAscending(names, compareAscii);
}

function describeCollectiveFailure(failure: CollectiveFailure): string {
  if (isFixedFailure(failure)) {
    return fixedFailureText[failure.kind];
  }
  switch (failure.kind) {
    case "schema-invalid":
      return `send failed: the form is invalid (${failure.detail})`;
    case "answer-invalid":
      return `response failed: ${responseFieldsText(failure.fields)}`;
    default: {
      const exhaustive: never = failure;
      return exhaustive;
    }
  }
}

function isFixedFailure(
  failure: CollectiveFailure,
): failure is Extract<CollectiveFailure, { readonly kind: FixedFailureKind }> {
  return Object.hasOwn(fixedFailureText, failure.kind);
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

function collectivePartValue(part: ContentPart): Option.Option<unknown> {
  return part.type === "data"
    ? Schema.decodeUnknownOption(CollectiveCarrier)(part.value).pipe(
        Option.map((carrier) => carrier[COLLECTIVE_DATA_KEY]),
      )
    : Option.none();
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
 * response field named `constructor` must not find `Object.prototype.constructor`.
 */
function ownValue<Value>(
  record: Readonly<Record<string, Value>>,
  key: string,
): Value | undefined {
  return Object.hasOwn(record, key) ? record[key] : undefined;
}

/**
 * Every failing keyword of one response field's value, as `keyword:
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
