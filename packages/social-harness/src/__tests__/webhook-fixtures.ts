/** @file A scripted Dot webhook callback and the inbox items it receives. */

import {
  type HttpBody,
  HttpClient,
  HttpClientResponse,
} from "@effect/platform";
import { Effect, Match, Schema } from "effect";
import type { openInboxFixture } from "./inbox-fixture.js";
import {
  DeliveryToken,
  encodeRuntimeValue,
} from "../communication/store/index.js";
import { makeConsumerOwner } from "../daemon/mcp/consumer.js";
import { makeWebhookEvents } from "../daemon/mcp/webhook.js";
import { localGuidanceSchema } from "../host-protocol/advice.js";
import { InboundItem } from "../host-protocol/items.js";
import { INBOX_ITEM_EVENT } from "../host-protocol/names.js";
import { digest, testAgentView } from "./agent-card-fixtures.js";
import { collectiveOperationId } from "./collective-id-fixtures.js";
type InboxFixture = Effect.Effect.Success<ReturnType<typeof openInboxFixture>>;

/** The signing secret of the scripted subscription. */
export const secret = `whsec_${Buffer.alloc(32, 1).toString("base64")}`;

/** The callback address of the scripted subscription. */
export const url = "https://callback.example/events";

/** A runtime subscription to inbox items, delivered to the scripted callback. */
export const input = {
  name: INBOX_ITEM_EVENT,
  arguments: {},
  cursor: null,
  delivery: { mode: "webhook", url, secret },
};

/** The verification challenge a subscription's callback must echo. */
const verification = Schema.Struct({
  type: Schema.Literal("verification"),
  challenge: Schema.String,
});

/** One event occurrence posted to the callback, by value or by reference. */
const occurrence = Schema.Struct({
  eventId: Schema.String,
  name: Schema.String,
  timestamp: Schema.String,
  data: Schema.Union(
    Schema.Struct({
      kind: Schema.Literal("item"),
      item: InboundItem,
      guidance: Schema.optional(localGuidanceSchema),
    }),
    Schema.Struct({
      kind: Schema.Literal("reference"),
      itemKind: Schema.String,
      bytes: Schema.Number,
    }),
  ),
  cursor: Schema.Null,
});

/** Any body the callback receives, decoded from its JSON. */
const callback = Schema.parseJson(Schema.Union(verification, occurrence));

/** Decode one event occurrence from the JSON text the callback received. */
export const decodeOccurrence = Schema.decodeUnknownSync(
  Schema.parseJson(occurrence),
);

/**
 * The exact bytes of a callback request, as text.
 * @param body The request body the webhook sent.
 * @returns The body's text.
 */
const requestBody = (body: HttpBody.HttpBody): string =>
  Match.value(body).pipe(
    Match.tag("Uint8Array", (value) => new TextDecoder().decode(value.body)),
    Match.orElse(() => {
      throw new Error("Expected exact callback bytes");
    }),
  );

const callbackResponse = (
  request: Parameters<typeof HttpClientResponse.fromWeb>[0],
  decoded: typeof callback.Type,
  status: number,
  challengeValid: boolean,
) =>
  HttpClientResponse.fromWeb(
    request,
    new Response(
      "type" in decoded
        ? JSON.stringify({
            challenge: challengeValid ? decoded.challenge : "incorrect",
          })
        : "receipt".repeat(1024),
      { status },
    ),
  );

/**
 * The delivery token the scripted inbox item `byte` carries.
 * @param byte Distinguishes one fixture token from another.
 * @returns A delivery token that differs for each `byte`.
 */
export const token = (byte: number) =>
  Schema.decodeUnknownSync(DeliveryToken)(digest("dlv_", byte));

/**
 * A failed-operation inbox item carrying `text`.
 * @param text The failure the item reports.
 * @returns An inbox item that reports `text` as its failure.
 */
export const item = (text = "failed request") =>
  Schema.decodeUnknownSync(InboundItem)({
    kind: "operationFailed",
    operationId: collectiveOperationId(testAgentView("agent:alice").agentId, 1),
    to: "agent:bob",
    error: text,
  });

/**
 * Put the scripted inbox item `byte` into `store`.
 * @param store The store the webhook reads.
 * @param byte Distinguishes the item's token.
 * @param text The failure the item reports.
 * @returns The store's mutation.
 */
export const enqueue = (store: InboxFixture, byte: number, text?: string) =>
  encodeRuntimeValue(item(text ?? "failed request")).pipe(
    Effect.flatMap((canonicalItem) =>
      store.putInboxItem({ deliveryToken: token(byte), canonicalItem }),
    ),
  );

/**
 * Webhook events over `store` whose callback is scripted: it records each
 * post, echoes verification challenges, and answers with the status the test
 * sets, running `onEvent` before it answers an event.
 * @param store The store the webhook reads.
 * @returns The scripted callback's controls and the events to acquire.
 */
export const scriptedWebhook = Effect.fn("scriptedWebhook")(function* (
  store: InboxFixture,
) {
  const owner = yield* makeConsumerOwner;
  const gate = Effect.unsafeMakeSemaphore(1);
  let status = 200;
  let challengeValid = true;
  let onEvent = Effect.void;
  const posts: Array<{
    readonly body: string;
    readonly headers: Readonly<Record<string, string>>;
  }> = [];
  const client = HttpClient.make((request) =>
    Effect.gen(function* () {
      const body = requestBody(request.body);
      posts.push({ body, headers: { ...request.headers } });
      const decoded = Schema.decodeUnknownSync(callback)(body);
      if (!("type" in decoded)) {
        yield* onEvent;
      }
      return callbackResponse(request, decoded, status, challengeValid);
    }).pipe(Effect.withSpan("webhookCallback")),
  );
  return {
    gate,
    owner,
    posts,
    status: (value: number) => {
      status = value;
    },
    challenge: (value: boolean) => {
      challengeValid = value;
    },
    onEvent: (effect: Effect.Effect<void>) => {
      onEvent = effect;
    },
    acquire: makeWebhookEvents(store, gate, owner).pipe(
      Effect.provideService(HttpClient.HttpClient, client),
    ),
  };
});
