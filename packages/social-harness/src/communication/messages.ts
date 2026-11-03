/** @file Immutable messages, questions and replies share their content grammar and constructors. */

import type { ParseOptions } from "effect/SchemaAST";
import canonicalize from "canonicalize";
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
import { randomBytes } from "node:crypto";
import {
  type ClientRepresentationError,
  exactStruct,
  representationFailure,
} from "../encoding/canonical-json.js";
import {
  canonicalIdentifier,
  compareAgentIds as compareAgentIdBytes,
  compareAscii,
  isCanonicalIdentifier,
  isStrictlyAscending,
} from "../encoding/identifier.js";
import {
  AGENT_NAME_RULE,
  AgentCard,
  AgentId,
  AgentName,
} from "../identity/agent-cards.js";
import {
  CommunicationError,
  communicationFailure,
  issuesText,
} from "./errors.js";

/** Order validated agent identities by their decoded identifier bytes. */
export const compareAgentIds: (left: AgentId, right: AgentId) => number =
  compareAgentIdBytes;

/* eslint-disable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Effect Schemas share their domain names with the nominal values they decode. */

/** The canonical JSON byte limit of one message's content. */
export const maximumContentBytes = 32_768;
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

/** A string without lone surrogates, so it survives canonical JSON. */
export const wellFormedString = Schema.String.pipe(
  Schema.filter(hasWellFormedUnicode, {
    identifier: "WellFormedUnicodeString",
  }),
);

/* eslint-disable agent-code-guard/no-nullish-type-aliases -- JSON includes null as a first-class value. */
/** A value accepted by the closed semantic content boundary. */
export type JsonValue =
  | null
  | boolean
  | number
  | string
  | readonly JsonValue[]
  | { readonly [key: string]: JsonValue };
/* eslint-enable agent-code-guard/no-nullish-type-aliases -- Restore the absence rule outside JSON values. */

/** Runtime validation for the closed recursive JSON value. */
export const JsonValue: Schema.Schema<JsonValue> = Schema.suspend(() =>
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

/** Nonempty semantic content whose canonical JSON is at most 32,768 bytes. */
export const Content = contentStructure.pipe(
  Schema.filter(contentFits),
  Schema.annotations({ identifier: "Content" }),
);
/** Validated nonempty semantic content. */
export type Content = typeof Content.Type;

/** The largest group membership a conversation address or descriptor admits. */
export const maximumMembers = 32;

/** The scheme of an address naming one agent. */
export const AGENT_ADDRESS_PREFIX = "agent:";
/** The scheme of an address listing a group's members. */
export const GROUP_ADDRESS_PREFIX = "group:";

const isAgentName = Schema.is(AgentName);

/**
 * The registry name an `agent:` address names.
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

/** An explicit direct destination using one canonical Registry name. */
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

/** The direct destination of an agent with a validated immutable Registry name. */
export const agentAddressOf = (agent: {
  readonly agentName: AgentName;
}): AgentAddress =>
  AgentAddress.make(`${AGENT_ADDRESS_PREFIX}${agent.agentName}`);

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

/* eslint-enable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Restore the package naming rules after the Schema/type pairs. */

/** A fresh author-scoped id uses 32 random bytes before any assessment or send. */
export const mintMsgId = (): Effect.Effect<MsgId, ClientRepresentationError> =>
  Effect.try({
    try: () => `msg_${randomBytes(32).toString("base64url")}`,
    catch: representationFailure,
  }).pipe(
    Effect.flatMap(Schema.decodeUnknown(MsgId)),
    Effect.mapError(representationFailure),
  );

/* eslint-disable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Effect Schemas share their domain names with the nominal values they decode. */

/**
 * Identity of one gather or all_gather, minted by the requesting endpoint.
 * The request, each member's answer message, an all_gather's close, the result
 * and any failure after selecting an operation carry it.
 */
export const CollectiveId = canonicalIdentifier("CollectiveId", "col_", 48);
/** A validated operation identity that includes its requester. */
export type CollectiveId = typeof CollectiveId.Type;

/** Gather keeps replies private to the requester; all_gather shares them with the group. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- Request and result schemas share one closed audience operation.
export const RequestOperation = Schema.Literal("gather", "all_gather");
/** The request fixes one reply audience for its lifetime. */
export type RequestOperation = typeof RequestOperation.Type;

/** One property of a form-mode schema as a caller states it. */
const formProperty = Schema.Record({ key: Schema.String, value: JsonValue });

/**
 * The MCP form-mode `requestedSchema` of a collecting operation: a flat object
 * of primitive properties. This schema fixes its outline; the sending endpoint
 * checks each property against the MCP form-mode grammar before any message.
 */
const RequestedSchemaInput = exactStruct({
  $schema: Schema.optionalWith(Schema.String, { exact: true }),
  type: Schema.Literal("object"),
  properties: Schema.Record({ key: Schema.String, value: formProperty }),
  required: Schema.optionalWith(Schema.Array(Schema.String), { exact: true }),
});
/** A form-mode schema as stated by its requester. */
type RequestedSchemaInput = typeof RequestedSchemaInput.Type;

/**
 * One answer to a collecting operation, field by field. Its value space is MCP
 * `ElicitResult` content's; the request's schema decides which fields apply.
 */
export const AnswerContent = Schema.Record({
  key: Schema.String,
  value: Schema.Union(
    Schema.String,
    Schema.Number.pipe(Schema.finite()),
    Schema.Boolean,
    Schema.Array(Schema.String),
  ),
}).annotations({
  description: "The answer, one field per requestedSchema property",
});
/** A structurally valid answer. */
export type AnswerContent = typeof AnswerContent.Type;

/** The longest deadline a collecting operation may state: 30 days, in seconds. */
export const MAXIMUM_DEADLINE_SECONDS = 2_592_000;

/** A collecting operation's relative deadline: whole seconds, up to 30 days. */
export const DeadlineSeconds = Schema.Number.pipe(
  Schema.int(),
  Schema.between(1, MAXIMUM_DEADLINE_SECONDS),
);

/** The latest instant a `Date` represents, in epoch milliseconds. */
const MAXIMUM_EPOCH_MILLIS = 8_640_000_000_000_000;

/** Positive Unix milliseconds within the range a `Date` can render. */
const EpochMillis = Schema.Int.pipe(Schema.between(1, MAXIMUM_EPOCH_MILLIS));

/** Absolute request deadlines use the endpoint clock. */
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
      "Whether every requested member had an outcome or the deadline left members unresolved",
  }),
});
/** The request an answer names: the `col_` operationId its `collectiveRequest` item carries. */
const answeredRequest = {
  operationId: CollectiveId,
};

/** An `accept` answer, the only one that carries content. */
export const AcceptResponse = exactStruct({
  action: Schema.Literal("accept"),
  content: AnswerContent,
  ...answeredRequest,
});

/** A `decline` answer. */
export const DeclineResponse = exactStruct({
  action: Schema.Literal("decline"),
  ...answeredRequest,
});

/**
 * A member's answer to a request open in the conversation it is sent to: the
 * requester's direct conversation for a gather, the group for an all_gather.
 * `operationId` names the received request. Only `accept` carries content,
 * which must satisfy that request's answer schema.
 */
const CollectiveResponse = Schema.Union(AcceptResponse, DeclineResponse);
/** A validated collective response. */
export type CollectiveResponse = typeof CollectiveResponse.Type;

/** Members a send could not reach. */
const unreachableMembers = Schema.NonEmptyArray(
  exactStruct({
    member: AgentCard,
    reason: communicationFailure,
    detail: Schema.optionalWith(Schema.String, { exact: true }),
  }),
);

/**
 * Why a collective send was refused, discriminated by `kind`: a gather or
 * all_gather whose schema is outside the form-mode grammar, whose request
 * message some member could not receive, or whose deadline passed while its
 * check ran; or a response the member's endpoint cannot send.
 */
const collectiveFailure = Schema.Union(
  exactStruct({
    kind: Schema.Literal("members-unreachable"),
    members: unreachableMembers,
  }),
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

/** What each answer-field problem means. */
const answerFieldText = {
  missing: "is missing",
  unexpected: "is not in the form",
  invalid: "is invalid",
} as const;

/** Every field of an answer that fails its request's schema. */
export type AnswerFieldFailures = Extract<
  CollectiveFailure,
  { readonly kind: "answer-invalid" }
>["fields"];

/** A failure that carries nothing beyond its kind. */
type FixedFailureKind = Exclude<
  CollectiveFailure["kind"],
  "members-unreachable" | "schema-invalid" | "answer-invalid"
>;

/** The text of each failure that carries nothing beyond its kind. */
const fixedFailureText: Record<FixedFailureKind, string> = {
  "request-none": "reply failed: no question is open in this conversation",
  "request-answered": "reply failed: the question was already answered",
  "request-expired": "reply failed: the question's deadline has passed",
  "deadline-passed":
    "send failed: the deadline passed before the question was sent",
};

/** A request or answer failed before it could be accepted. */
export class RequestError extends Data.TaggedError("RequestError")<{
  readonly operationId?: CollectiveId;
  readonly failure: CollectiveFailure;
}> {
  override get message(): string {
    return describeCollectiveFailure(this.failure);
  }
}

/* eslint-enable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Restore the package naming rules after the Schema/type pairs. */

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
export const COLLECTIVE_DATA_KEY = "org.social-harness/collective";

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
const answersCollectiveResponse = Schema.Union(
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
  answersCollectiveResponse,
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

/* eslint-enable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Restore naming after the schema owner. */

// safer-arch-ignore no-module-cycle: Request form parsing imports only the result type from its owner; runtime imports have no cycle.

/* eslint-disable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Effect Schemas share their names with the values they decode. */

/** Omit cursor to begin and pass the returned nextCursor unchanged. */
export const PaginatedRequest = exactStruct({
  cursor: Schema.optional(Schema.String),
});
/** Selection arguments remain unchanged between pages.
 * @useDeclaredType
 */
export type PaginatedRequest = typeof PaginatedRequest.Type;
/** Absence of nextCursor marks the end of an operation's named collection. */
export const PaginatedResult = exactStruct({
  nextCursor: Schema.optional(Schema.String),
});
/** Cursors confer no access and remain private to their operation.
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

/** Decoded request and reply meaning stays outside signed content bytes. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- MCP composes the owning decoded operation schema without parsing reserved content.
export const MessageOperation = Schema.Union(
  exactStruct({
    kind: Schema.Literal("request"),
    op: RequestOperation,
    operationId: CollectiveId,
    question: Schema.String,
    requestedSchema: RequestedSchema,
    deadlineAt: EpochMillis,
  }),
  exactStruct({ kind: Schema.Literal("response"), ...AcceptResponse.fields }),
  exactStruct({ kind: Schema.Literal("response"), ...DeclineResponse.fields }),
);
/** Communication derives operation metadata from reserved content. */
export type MessageOperation = typeof MessageOperation.Type;

const encodedMessage = MessageHeader.pipe(
  Schema.extend(exactStruct({ content: Content })),
);
const decodedMessage = encodedMessage.pipe(
  Schema.extend(exactStruct({ operation: Schema.optional(MessageOperation) })),
);
/** Authorship, recipients and content remain fixed through delivery and restart. */
export type Message = typeof decodedMessage.Type;

const interpretations = new WeakMap<
  Message,
  Either.Either<Option.Option<CollectiveValue>, CollectivePartInvalidError>
>();

const messageSchema = Schema.transformOrFail(
  encodedMessage,
  Schema.declare<Message>(Schema.is(decodedMessage), {
    [SchemaAST.SurrogateAnnotationId]: Schema.typeSchema(decodedMessage).ast,
  }),
  {
    strict: true,
    decode: (message) =>
      readCollectiveValue(message.content).pipe(
        Effect.either,
        Effect.map((interpretation) => {
          const value = Either.match(interpretation, {
            onLeft: () => undefined,
            onRight: Option.getOrUndefined,
          });
          let operation: MessageOperation | undefined;
          if (value?.kind === "operation") {
            operation = {
              kind: "request",
              op: value.op,
              operationId: value.operationId,
              question: message.content
                .flatMap((part) => (part.type === "text" ? [part.text] : []))
                .join("\n"),
              requestedSchema: value.requestedSchema,
              deadlineAt: value.deadlineAt,
            };
          } else if (value?.kind === "response") {
            operation = value;
          }
          const result: Message = {
            ...message,
            ...(operation === undefined ? {} : { operation }),
          };
          interpretations.set(result, interpretation);
          return result;
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

/** Owns construction and the one decoded interpretation of reserved content. */
export const Message = Object.assign(messageSchema, {
  create(input: Pick<Message, "author" | "recipients" | "content">) {
    return Effect.gen(function* () {
      return yield* messageFromParticipants(input.author, input.recipients, {
        msgId: yield* mintMsgId(),
        content: input.content,
      });
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

/* eslint-enable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Restore naming after Schema/type pairs. */

type Participants = Pick<Message, "author" | "recipients">;

/* eslint-disable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Effect Schemas share their names with their decoded types. */

/** The operation is captured before assessment, including its absolute deadline and participants. */
const requestSchema = MessageParticipants.pipe(
  Schema.extend(
    exactStruct({
      kind: Schema.Literal("request"),
      operationId: CollectiveId,
      op: RequestOperation,
      question: wellFormedString,
      requestedSchema: RequestedSchema,
      deadlineAt: EpochMillis,
    }),
  ),
);
/** A gather sends a separate request to each recipient; an all_gather shares one group request. */
export type Request = typeof requestSchema.Type;

/** Captures a complete question and its deadline before policy assessment. */
export const Request = Object.assign(requestSchema, {
  create(
    input: Pick<Request, "author" | "recipients" | "question" | "op"> & {
      readonly requestedSchema: RequestedSchema;
      readonly deadline: number;
    },
  ) {
    return Effect.gen(function* () {
      const requestedSchema = input.requestedSchema;
      const seconds = yield* Schema.decodeUnknown(DeadlineSeconds)(
        input.deadline,
      );
      const request = yield* Schema.validate(requestSchema)({
        author: input.author,
        question: input.question,
        op: input.op,
        recipients: [...input.recipients].sort((left, right) =>
          compareAscii(left.agentName, right.agentName),
        ),
        kind: "request",
        operationId: yield* mintCollectiveId(input.author.agentId),
        requestedSchema,
        deadlineAt: (yield* Clock.currentTimeMillis) + seconds * 1000,
      });
      yield* encodeCollectiveContent(
        {
          kind: "operation",
          op: request.op,
          operationId: request.operationId,
          deadlineAt: request.deadlineAt,
          requestedSchema: request.requestedSchema,
        },
        request.question,
      );
      if (request.op === "all_gather" && request.recipients.length < 2) {
        return yield* new CommunicationError({ reason: "membership-invalid" });
      }
      return request;
    }).pipe(
      Effect.withSpan("Request.create"),
      Effect.catchTags({
        ParseError: () => new CommunicationError({ reason: "content-invalid" }),
        CollectiveContentError: () =>
          new CommunicationError({ reason: "content-invalid" }),
      }),
    );
  },
});

/** A received request retains the ordinary message and its decoded meaning. */
export const ReceivedRequest = Message.pipe(
  Schema.filter(
    (
      message,
    ): message is Message & {
      readonly operation: Extract<
        NonNullable<Message["operation"]>,
        { readonly kind: "request" }
      >;
    } => message.operation?.kind === "request",
  ),
);
/** A request Message exposes a complete decoded response form. */
export type ReceivedRequest = typeof ReceivedRequest.Type;

/** A member contributes one selected reply or a final absence. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- Firewall extends complete raw outcomes with its local disclosure decision.
export const AnswerOutcome = Schema.Union(
  exactStruct({ kind: Schema.Literal("answered"), content: AnswerContent }),
  exactStruct({ kind: Schema.Literal("declined") }),
  exactStruct({ kind: Schema.Literal("invalid"), reason: Schema.String }),
  exactStruct({ kind: Schema.Literal("no-answer") }),
);
/** A complete outcome precedes local disclosure policy. */
export type AnswerOutcome = typeof AnswerOutcome.Type;

/** Complete request outcomes remain independent of policy or host addressing. */
export const RequestResult = exactStruct({
  request: requestSchema,
  closedAt: Schema.Int.pipe(Schema.nonNegative()),
  closedBy: Schema.Literal("settled", "deadline"),
  outcomes: Schema.NonEmptyArray(
    exactStruct({ member: AgentCard, outcome: AnswerOutcome }),
  ),
});
/** Every original respondent contributes one selected outcome. */
export type RequestResult = typeof RequestResult.Type;

/** A decline contains no form data. */
export type AnswerInput =
  | {
      readonly action: "accept";
      readonly content: AnswerContent;
    }
  | { readonly action: "decline" };

/** Validates a reply and fixes its recipients and message identity before assessment. */
export const Answer = {
  create(input: {
    readonly author: Message["author"];
    readonly request: ReceivedRequest;
    readonly response: AnswerInput;
  }) {
    return Effect.gen(function* () {
      if (
        !input.request.recipients.some(
          (member) => member.agentId === input.author.agentId,
        )
      ) {
        return yield* new CommunicationError({ reason: "membership-invalid" });
      }
      if (input.response.action === "accept") {
        const fields = yield* validateAnswer(
          input.request.operation.requestedSchema,
          input.response.content,
        ).pipe(Effect.flip, Effect.option);
        if (Option.isSome(fields)) {
          return yield* new RequestError({
            operationId: input.request.operation.operationId,
            failure: { kind: "answer-invalid", fields: fields.value },
          });
        }
      }
      const members = [input.request.author, ...input.request.recipients];
      const [first, ...rest] = members.filter(
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
          operationId: input.request.operation.operationId,
          ...input.response,
        }),
      });
    }).pipe(
      Effect.withSpan("Answer.create"),
      Effect.catchTag(
        "CollectiveContentError",
        () => new CommunicationError({ reason: "content-invalid" }),
      ),
    );
  },
};

/** Original request and selected answer messages supply the peer-controlled result text. */
export const ResultSources = exactStruct({
  answers: Schema.Array(Message).pipe(Schema.maxItems(maximumMembers - 1)),
  request: Schema.optionalWith(ReceivedRequest, { exact: true }),
});
/** Own answers remain sources but do not need an inbound policy check. */
export type ResultSources = typeof ResultSources.Type;

/** The item and its sources use one durable result identity through retries and restart. */
export const ResultDelivery = exactStruct({
  operationId: CollectiveId,
  item: Schema.Union(
    exactStruct({ kind: Schema.Literal("result"), result: RequestResult }),
    exactStruct({
      kind: Schema.Literal("request-failed"),
      request: requestSchema,
      error: Schema.String,
    }),
  ),
  sources: ResultSources,
});
/** Public item bytes and private source bytes are bounded separately. */
export type ResultDelivery = typeof ResultDelivery.Type;

/* eslint-enable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Restore naming after schema/type pairs. */

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
  response: CollectiveResponse,
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
 * Name each failing answer field, such as `field "slot" is invalid (enum)`.
 * A refused reply and a member's `invalid` outcome read the same. Field
 * names come from peers, so each is quoted as JSON: no name can close its
 * quotes or start a new line of the text a model reads.
 * @param fields The failing fields, in validation order.
 * @returns One clause per field, joined by `; `.
 */
export function answerFieldsText(fields: AnswerFieldFailures): string {
  return fields
    .map(({ field, reason, detail }) => {
      const named = `field ${JSON.stringify(field)} ${answerFieldText[reason]}`;
      return detail === undefined ? named : `${named} (${detail})`;
    })
    .join("; ");
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

/** Private protocol consumers reuse the interpretation made by the owning reader. */
export function messageInterpretation(
  message: Message,
): Effect.Effect<Option.Option<CollectiveValue>, CollectivePartInvalidError> {
  const retained = interpretations.get(message);
  if (retained !== undefined) {
    return Either.match(retained, {
      onLeft: Effect.fail,
      onRight: Effect.succeed,
    });
  }
  return readCollectiveValue(message.content).pipe(
    Effect.tapBoth({
      onFailure: (error) =>
        Effect.sync(() => interpretations.set(message, Either.left(error))),
      onSuccess: (value) =>
        Effect.sync(() => interpretations.set(message, Either.right(value))),
    }),
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

/** Captures the known author and recipients without reconstructing a member list. */
export function messageFromParticipants(
  author: Message["author"],
  recipients: ReadonlyArray<Message["author"]>,
  intent: Pick<Message, "msgId" | "content">,
) {
  return Effect.try({
    try: () =>
      structuredClone({
        author,
        recipients: [...recipients].sort((left, right) =>
          compareAscii(left.agentName, right.agentName),
        ),
        ...intent,
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

/** Constructs one message from fixed participants and the author's retained content. */
export function messageFromIntent(
  members: readonly AgentCard[],
  intent: {
    readonly authorAgentId: AgentId;
    readonly msgId: MsgId;
    readonly content: Content;
  },
) {
  return Schema.decodeUnknown(Message)({
    msgId: intent.msgId,
    author: members.find((member) => member.agentId === intent.authorAgentId),
    recipients: members
      .filter((member) => member.agentId !== intent.authorAgentId)
      .sort((left, right) => compareAscii(left.agentName, right.agentName)),
    content: intent.content,
  });
}

/** Includes the author and every recipient in agent-name order. */
export function messageMembers(message: Participants): readonly AgentCard[] {
  return [message.author, ...message.recipients].sort((left, right) =>
    compareAscii(left.agentName, right.agentName),
  );
}

/** Private semantic index keys put directs before group member-name tuples. */
export function conversationSortKey(
  members: ReadonlyArray<Message["author"]>,
  localAgentId: Message["author"]["agentId"],
) {
  return conversationNameKey(
    members.map((member) => member.agentName),
    members.find((member) => member.agentId === localAgentId)?.agentName,
  );
}

/** The retained membership index orders direct peers before group participant tuples. */
export function conversationNameKey(
  names: ReadonlyArray<Message["author"]["agentName"]>,
  localName?: Message["author"]["agentName"],
) {
  const ordered = [...names].sort(compareAscii);
  return ordered.length === 2
    ? "0:" + ordered.filter((name) => name !== localName).join(",")
    : "1:" + ordered.join(",");
}

/** Request narrowing preserves the Message and its owning decoder's private classification. */
export function isReceivedRequest(
  message: Message,
): message is ReceivedRequest {
  return message.operation?.kind === "request";
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
    Either.try(() => canonicalize(content)),
    {
      onLeft: () => false,
      onRight: (canonical) =>
        canonical !== undefined &&
        utf8Encoder.encode(canonical).byteLength <= maximumContentBytes,
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
    case "members-unreachable":
      return `send failed: ${failure.members
        .map(
          ({ member, reason, detail }) =>
            `${member.agentName} could not be reached: ${detail ?? reason}`,
        )
        .join("; ")}`;
    case "schema-invalid":
      return `send failed: the form is invalid (${failure.detail})`;
    case "answer-invalid":
      return `reply failed: ${answerFieldsText(failure.fields)}`;
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
