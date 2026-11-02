/** @file The values harness operations take, return and persist: sends, inbox pages, events and acknowledgments. */

import { Effect, JSONSchema, type ParseResult, Schema } from "effect";
import {
  type SendError,
  sendFailureReasons,
} from "../communication/conversations/errors.js";
import {
  CollectiveId,
  decodeCollectiveFailure,
  PaginatedRequest,
  type RequestError,
} from "../communication/index.js";
import { AgentAddress, GroupAddress } from "../communication/records/values.js";
import { exactStruct } from "../encoding/exact.js";
import { LocalGuidance, PermittedItem } from "../firewall/service.js";
import { DeliveryToken, InboxEvent, InboxPage } from "../harness/inbox.js";
import { type FirewallRefusal, firewallRefusalSchema } from "./advice.js";
import { SendRequest } from "./send-request.js";
const savedItemSchema = Schema.typeSchema(PermittedItem);

const itemEventDataSchema = Schema.Union(
  exactStruct({
    kind: Schema.Literal("item"),
    address: Schema.Union(AgentAddress, GroupAddress),
    item: savedItemSchema,
    guidance: Schema.optionalWith(LocalGuidance, { exact: true }),
  }),
  exactStruct({
    kind: Schema.Literal("reference"),
    itemKind: Schema.Literal(
      "multicast",
      "collectiveRequest",
      "collectiveResult",
      "operationFailed",
    ),
    bytes: Schema.NonNegativeInt,
  }),
);
/** Callback data uses the same saved payload returned by read_event. */
export const itemEventDataJsonSchema = JSONSchema.make(itemEventDataSchema, {
  target: "jsonSchema2020-12",
});

/** Read-only content lookup never acknowledges or retries delivery. */
export const readEventRequestSchema = exactStruct({
  eventId: Schema.propertySignature(DeliveryToken).annotations({
    description:
      "The eventId of an org.social-harness/inbox.item event, or one a host's wakeup carries from an inbox item; retained content remains readable after acknowledgment.",
  }),
});
/** The original classified payload, including after its handoff. */
export const readEventResultSchema = Schema.typeSchema(InboxEvent);

/** A delivery acknowledgment names the delivery it retires. */
export const harnessAcknowledgeDeliveryRequestSchema = exactStruct({
  deliveryToken: Schema.propertySignature(DeliveryToken).annotations({
    description:
      "Stable delivery token from the inbox item accepted by the host.",
  }),
});

/** The model-visible arguments of one send. */
export const harnessSendArgumentsSchema = exactStruct({
  input: Schema.propertySignature(SendRequest).annotations({
    description:
      "One send to agent:<name> or group:<members>: text with an optional collective operation, or a collectiveResponse to an open request.",
  }),
});

/** One page request over the classified inbox. */
export const inboxRequestSchema = exactStruct(PaginatedRequest.fields);
/** One page of classified deliveries and the cursor after it. */
export const inboxResultSchema = Schema.typeSchema(InboxPage);
/** Validation diagnostics remain decodable as a send refusal at the runtime edge. */
const invocationValidationErrorSchema = Schema.Struct({
  reason: Schema.Literal("content-invalid"),
  detail: Schema.optionalWith(Schema.String, { exact: true }),
  issues: Schema.Array(
    Schema.Struct({
      path: Schema.Array(Schema.Union(Schema.String, Schema.Number)),
      message: Schema.String,
    }),
  ),
});

/**
 * The JSON-RPC error data of a refused `send_message`: a `SendError` reason
 * with the detail naming its specific cause, or a refused collective send
 * with its selected operationId and the failure naming each
 * unreachable member or failing field, which `decodeCollectiveFailure`
 * validates. Named-request refusals disclose no unrelated request or
 * conversation. Fields a newer daemon adds are ignored; a firewall or
 * collective refusal that misses its own fields never decodes as a plain
 * reason.
 */
const harnessSendErrorDataSchema = Schema.Union(
  invocationValidationErrorSchema,
  Schema.Struct({
    reason: Schema.Literal("firewall-refused"),
    ...firewallRefusalSchema.fields,
  }),
  Schema.Struct({
    reason: Schema.Literal("collective-failed"),
    operationId: Schema.optionalWith(CollectiveId, { exact: true }),
    failure: Schema.Unknown,
  }),
  Schema.Struct({
    reason: Schema.Literal(...sendFailureReasons),
    detail: Schema.optionalWith(Schema.String, { exact: true }),
  }),
);

/**
 * Decoded error data of one refused `send_message` call.
 * @inline
 */
export type HarnessSendErrorData =
  | typeof invocationValidationErrorSchema.Type
  | (typeof firewallRefusalSchema.Type & {
      readonly reason: "firewall-refused";
    })
  | Readonly<{ reason: SendError["reason"]; detail?: string }>
  | Readonly<{
      reason: "collective-failed";
      operationId?: CollectiveId;
      failure: Effect.Effect.Success<
        ReturnType<typeof decodeCollectiveFailure>
      >;
    }>;

/**
 * The error data a refused send carries across the daemon boundary: a
 * collective failure keeps its operationId and the members or fields it names, and a
 * send failure keeps the detail naming its cause, so the receiving side
 * rebuilds the same typed error and message.
 * @param error The refused send's typed error.
 * @returns The JSON error data.
 */
export function sendErrorData(
  error: SendError | RequestError | FirewallRefusal,
): HarnessSendErrorData {
  if (error._tag === "FirewallRefusal") {
    return {
      reason: "firewall-refused",
      checkId: error.checkId,
      refusal: error.refusal,
      feedback: error.feedback,
    };
  }
  if (error._tag === "RequestError") {
    return {
      reason: "collective-failed",
      ...(error.operationId === undefined
        ? {}
        : { operationId: error.operationId }),
      failure: error.failure,
    };
  }
  if (error.detail === undefined) {
    return { reason: error.reason };
  }
  return { reason: error.reason, detail: error.detail };
}

/**
 * Decode the error data of one refused `send_message` call.
 * @param value Untrusted JSON-RPC error data.
 * @returns The collective failure or send reason it carries.
 */
export function decodeHarnessSendErrorData(
  value: unknown,
): Effect.Effect<HarnessSendErrorData, ParseResult.ParseError> {
  return Schema.decodeUnknown(harnessSendErrorDataSchema)(value).pipe(
    Effect.flatMap(
      (data): Effect.Effect<HarnessSendErrorData, ParseResult.ParseError> =>
        "failure" in data
          ? decodeCollectiveFailure(data.failure).pipe(
              Effect.map((failure) => ({ ...data, failure })),
            )
          : Effect.succeed(data),
    ),
  );
}
