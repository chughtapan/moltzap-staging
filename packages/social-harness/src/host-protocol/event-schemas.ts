/** @file Shared schema and ports for the experimental MCP Events edge. */

import type { ProtocolError } from "@modelcontextprotocol/server";
import { type Effect, Schema } from "effect";
import { requestSchema, resultSchema } from "./catalog-schemas.js";
/** Daemon event delivery uses the same host-safe wire values as polling receivers. */
export {
  type ConsumerMetadata,
  decodeConsumerMetadata,
  decodePollResult,
  itemEventDataJsonSchema,
  type PollResult,
} from "./operations.js";

/** Maximum complete UTF-8 callback body accepted by the event transport. */
export const maximumEventBytes = 256 * 1024;

/** Callback failures expose no upstream response or destination details. */
export const callbackReasons = [
  "connection_refused",
  "timeout",
  "tls_error",
  "http_4xx",
  "http_5xx",
  "challenge_failed",
] as const;
/** Owner-visible diagnostics omit callback addresses and signing authority. */
const webhookStatusSchema = Schema.Struct({
  mode: Schema.Literal("none", "push", "poll", "webhook"),
  id: Schema.optional(Schema.String),
  refreshBefore: Schema.optional(Schema.String),
  pendingCount: Schema.optional(Schema.NonNegativeInt),
  stalled: Schema.optional(
    Schema.NullOr(Schema.Literal("callback", "terminal")),
  ),
  lastError: Schema.optional(Schema.NullOr(Schema.Literal(...callbackReasons))),
});

/** Secret-free diagnostics shared with the MCP tool. */
export type WebhookStatus = typeof webhookStatusSchema.Type;

/** The result schema of the secret-free event status tool. */
export const webhookStatusResult = resultSchema(webhookStatusSchema);

const emptyArguments = Schema.Record({
  key: Schema.String,
  value: Schema.Never,
});
/** An absent cursor has the draft's explicit null meaning. */
const subscriptionSchema = Schema.Struct({
  name: Schema.String,
  arguments: emptyArguments,
  cursor: Schema.optionalWith(Schema.NullOr(Schema.String), { exact: true }),
  maxAgeMs: Schema.optionalWith(Schema.NonNegativeInt, { exact: true }),
});
const pollSchema = Schema.Struct({
  ...subscriptionSchema.fields,
  _meta: Schema.optional(
    Schema.Record({ key: Schema.String, value: Schema.Unknown }),
  ),
  maxEvents: Schema.optionalWith(Schema.Int.pipe(Schema.positive()), {
    exact: true,
  }),
});

/** Catalog pagination uses its own cursor rather than event replay state. */
const listSchema = Schema.Struct({
  cursor: Schema.optionalWith(Schema.String, { exact: true }),
});
/** Delivery policy checks follow SDK shape validation. */
const subscribeSchema = Schema.Struct({
  ...subscriptionSchema.fields,
  delivery: Schema.Struct({
    mode: Schema.String,
    url: Schema.String,
    secret: Schema.String,
  }),
  ttlMs: Schema.optionalWith(Schema.NullOr(Schema.NonNegativeInt), {
    exact: true,
  }),
});
/** Cleanup identifies the registration without disclosing its signing secret. */
const unsubscribeSchema = Schema.Struct({
  name: Schema.String,
  arguments: emptyArguments,
  delivery: Schema.Struct({ url: Schema.String }),
});

/** Validated webhook subscription parameters before authorization and URL checks. */
export type EventSubscribeInput = typeof subscribeSchema.Type;
/** Exact subscription key supplied for eager webhook cleanup. */
export type EventUnsubscribeInput = typeof unsubscribeSchema.Type;
/** Server grant for one webhook registration. */
interface EventSubscriptionGrant {
  readonly id: string;
  readonly refreshBefore: string;
  readonly cursor: null;
  readonly truncated: boolean;
}

/** Durable callback delivery for the daemon's authenticated clients. */
export interface HarnessWebhookEvents {
  readonly subscribe: (
    input: EventSubscribeInput,
    authorize?: Effect.Effect<void, ProtocolError>,
  ) => Effect.Effect<EventSubscriptionGrant, ProtocolError>;
  readonly unsubscribe: (
    input: EventUnsubscribeInput,
  ) => Effect.Effect<void, ProtocolError>;
  readonly observe: () => Effect.Effect<void, ProtocolError>;
  readonly status: Effect.Effect<WebhookStatus, ProtocolError>;
  readonly revoke: Effect.Effect<void, ProtocolError>;
  readonly resume: Effect.Effect<void, ProtocolError>;
}

/** Native streaming uses the same event and replay grammar as callbacks. */
export type EventStreamInput = typeof subscriptionSchema.Type;
/** SDK boundary validation keeps schema constructors private. */
export const eventStreamInput = requestSchema(subscriptionSchema);
/** Polling adds a bounded occurrence count to the shared replay grammar. */
export type EventPollInput = typeof pollSchema.Type;
/** The official SDK validates poll requests before acquiring a consumer. */
export const eventPollInput = requestSchema(pollSchema);
/** The event catalog has its own pagination grammar. */
export const eventListInput = requestSchema(listSchema);
/** Callback registration includes the signing authority and lease request. */
export const eventSubscribeInput = requestSchema(subscribeSchema);
/** Cleanup uses only the subscription's public key fields. */
export const eventUnsubscribeInput = requestSchema(unsubscribeSchema);
