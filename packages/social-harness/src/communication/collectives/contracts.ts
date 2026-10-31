/** @file Collective operations, answers and the closed errors of a refused collective send. */

import { Data, ParseResult, Schema } from "effect";
import { exactStruct } from "../../encoding/exact.js";
import { canonicalIdentifier } from "../../encoding/identifier.js";
import {
  issuesText,
  sendFailureReasons,
  sendFailureText,
} from "../conversations/errors.js";
import {
  AgentAddress,
  JsonValue,
  MessageAddressInput,
  wellFormedString,
} from "../records/values.js";

/* eslint-disable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Effect Schemas share their domain names with the nominal values they decode. */

/**
 * Identity of one gather or all_gather, minted by the requesting endpoint.
 * The request, each member's answer message, an all_gather's close, the result
 * and any failure after selecting an operation carry it.
 */
export const CollectiveId = canonicalIdentifier("CollectiveId", "col_", 48);
/** A validated operation identity that includes its requester. */
export type CollectiveId = typeof CollectiveId.Type;

/** One property of a form-mode schema as a caller states it. */
const formProperty = Schema.Record({ key: Schema.String, value: JsonValue });

/**
 * The MCP form-mode `requestedSchema` of a collecting operation: a flat object
 * of primitive properties. This schema fixes its outline; the sending endpoint
 * checks each property against the MCP form-mode grammar before any message.
 */
export const RequestedSchema = exactStruct({
  $schema: Schema.optionalWith(Schema.String, { exact: true }),
  type: Schema.Literal("object"),
  properties: Schema.Record({ key: Schema.String, value: formProperty }),
  required: Schema.optionalWith(Schema.Array(Schema.String), { exact: true }),
});
/** A form-mode schema as stated by its requester. */
export type RequestedSchema = typeof RequestedSchema.Type;

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
export const EpochMillis = Schema.Int.pipe(
  Schema.between(1, MAXIMUM_EPOCH_MILLIS),
);

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
/**
 * One accepted message to a direct or group address. Omitting `op` or
 * `collective` selects multicast.
 */
const multicastOperation = exactStruct({
  op: Schema.optionalWith(Schema.Literal("multicast"), { exact: true }),
});

/**
 * A collecting operation: the text is a question to every member of `to`.
 * A gather sends one request message to each member, in that member's direct
 * conversation with the requester, and only the requester receives the
 * result. An all_gather sends one request message to the `group:` conversation
 * `to` names; members answer there, no model sees a peer's answer before the
 * requester's close, and every member receives the same result. The deadline
 * is relative, in whole seconds; the sending endpoint converts it to an
 * absolute time at send.
 */
const collectingOperation = exactStruct({
  op: Schema.Literal("gather", "all_gather"),
  deadline: DeadlineSeconds,
  requestedSchema: RequestedSchema,
});

/** The collective operation one send performs, discriminated by `op`. */
const CollectiveOperation = Schema.Union(
  multicastOperation,
  collectingOperation,
);
/** A validated collective operation. */
export type CollectiveOperation = typeof CollectiveOperation.Type;

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

/**
 * One send to one address: text with its collective operation, multicast
 * when `collective` is omitted, or an answer to the request open in that
 * address's conversation. `parseMessageText` reads both from a message's
 * text.
 */
export const SendInput = Schema.Union(
  exactStruct({
    to: MessageAddressInput,
    text: wellFormedString,
    collective: Schema.optionalWith(CollectiveOperation, { exact: true }),
  }),
  exactStruct({
    to: MessageAddressInput,
    collectiveResponse: CollectiveResponse,
  }),
).annotations({ identifier: "SendInput" });
/**
 * Answers use a received request's `operationId` and original conversation.
 * Question deadlines are in seconds and include policy assessment time.
 * @useDeclaredType
 */
export type SendInput = typeof SendInput.Type;

/** Members a send could not reach. */
const unreachableMembers = Schema.NonEmptyArray(
  exactStruct({
    member: AgentAddress,
    reason: Schema.Literal(...sendFailureReasons),
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
export const describeIssues = (error: ParseResult.ParseError): string =>
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

function describeCollectiveFailure(failure: CollectiveFailure): string {
  if (isFixedFailure(failure)) {
    return fixedFailureText[failure.kind];
  }
  switch (failure.kind) {
    case "members-unreachable":
      return `send failed: ${failure.members
        .map(({ member, reason, detail }) =>
          reason === "unknown-agent"
            ? `${member} is not a known agent`
            : `${member} could not be reached: ${detail ?? sendFailureText[reason]}`,
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

/**
 * A gather, all_gather or answer was refused. The message is what a host
 * hands its model as the tool error: the failed action and its cause, naming
 * each unreachable member or failing field. For an answer that named no
 * request while several were open, it names each open request's operationId; for a
 * `request-none`, `request-answered` or `request-expired` refusal, the
 * questions open in other conversations and where each is answered. The
 * operationId is present only after the invocation selects an operation.
 */
export class CollectiveError extends Data.TaggedError("CollectiveError")<{
  readonly operationId?: CollectiveId;
  readonly failure: CollectiveFailure;
}> {
  override get message(): string {
    return describeCollectiveFailure(this.failure);
  }
}

/* eslint-enable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Restore the package naming rules after the Schema/type pairs. */

/** Operation failures remain distinct from messaging and storage failures. */
export class CollectivesError extends Data.TaggedError("CollectivesError")<{
  readonly reason:
    | "not-registered"
    | "invalid-question"
    | "request-none"
    | "request-expired"
    | "request-answered"
    | "answer-invalid"
    | "persistence-failed"
    | "message-failed";
  readonly detail?: string;
}> {}
