/** @file A scripted Dot webhook callback and the inbox items it receives. */

import {
  type HttpBody,
  HttpClient,
  HttpClientResponse,
} from "@effect/platform";
import { SqlClient } from "@effect/sql";
import { Effect, Match, Schema, Stream } from "effect";
import { digest, testAgentCard } from "../../__tests__/agents.js";
import { collectiveOperationId } from "../../communication/__tests__/request-ids.js";
import { openTestDatabase } from "../../communication/__tests__/store-fixtures.js";
import { LocalGuidance, PermittedItem } from "../../firewall/service.js";
import { DeliveryToken, InboxError, InboxEvent } from "../inbox.js";
import { INBOX_ITEM_EVENT } from "../mcp/contracts.js";
import { makeWebhookEvents } from "../mcp/webhook.js";
import { SocialHarness } from "../social-harness.js";
type InboxFixture = Effect.Effect.Success<ReturnType<typeof openCallbackInbox>>;

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
      address: Schema.String,
      item: Schema.typeSchema(PermittedItem),
      guidance: Schema.optional(LocalGuidance),
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
  Schema.decodeUnknownSync(PermittedItem)({
    kind: "operationFailed",
    operationId: collectiveOperationId(testAgentCard("agent:alice").agentId, 1),
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
  store.save(token(byte), item(text ?? "failed request"));

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
  const gate = yield* Effect.makeSemaphore(1);
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
    acquire: makeWebhookEvents(gate).pipe(
      Effect.provideService(HttpClient.HttpClient, client),
      Effect.provideService(SocialHarness, store.harness),
      Effect.provideService(SqlClient.SqlClient, store.sql),
    ),
  };
});

/** A SQL-backed service fixture exercises webhook transactions without reading harness-owned tables. */
export const openCallbackInbox = Effect.fn("openCallbackInbox")(function* (
  directory: string,
) {
  const database = yield* openTestDatabase(directory);
  const sql = database.sql;
  yield* sql.unsafe(
    "CREATE TABLE IF NOT EXISTS callback_fixture (token TEXT PRIMARY KEY, payload TEXT NOT NULL, acknowledged INTEGER NOT NULL DEFAULT 0)",
  );
  const payload = Schema.parseJson(Schema.typeSchema(InboxEvent));
  const save = (
    deliveryToken: DeliveryToken,
    item: typeof PermittedItem.Type,
    guidance?: typeof LocalGuidance.Type,
  ) =>
    Schema.encode(payload)({
      address: Schema.decodeUnknownSync(InboxEvent.fields.address)("agent:bob"),
      item,
      ...(guidance === undefined ? {} : { guidance }),
    }).pipe(
      Effect.flatMap(
        (bytes) =>
          sql`INSERT INTO callback_fixture(token,payload) VALUES (${deliveryToken},${bytes})`,
      ),
      Effect.asVoid,
    );
  const getInboxEvent = (deliveryToken: DeliveryToken) =>
    sql<{
      readonly payload: string;
    }>`SELECT payload FROM callback_fixture WHERE token=${deliveryToken}`.pipe(
      Effect.flatMap((rows) =>
        rows[0] === undefined
          ? Effect.fail(new InboxError({ reason: "unknown-event" }))
          : Schema.decodeUnknown(payload)(rows[0].payload).pipe(
              Effect.catchTag("ParseError", () =>
                Effect.fail(new InboxError({ reason: "persistence-failed" })),
              ),
            ),
      ),
      Effect.catchTag("SqlError", () =>
        Effect.fail(new InboxError({ reason: "persistence-failed" })),
      ),
    );
  const acknowledgeDelivery = (deliveryToken: DeliveryToken) =>
    sql`UPDATE callback_fixture SET acknowledged=1 WHERE token=${deliveryToken}`.pipe(
      Effect.asVoid,
      Effect.orDie,
    );
  const decodeRow = (row: {
    readonly token: string;
    readonly payload: string;
  }) =>
    Schema.decodeUnknown(payload)(row.payload).pipe(
      Effect.map((event) => ({
        ...event,
        deliveryToken: Schema.decodeUnknownSync(DeliveryToken)(row.token),
      })),
    );
  const harness = SocialHarness.of({
    register: () => Effect.dieMessage("Unexpected registration"),
    send: () => Effect.dieMessage("Unexpected send"),
    listInbox: (request = {}) =>
      sql<{
        readonly token: string;
        readonly payload: string;
      }>`SELECT token,payload FROM callback_fixture WHERE acknowledged=0 AND token>${request.cursor ?? ""} ORDER BY token LIMIT 51`.pipe(
        Effect.flatMap((rows) =>
          Effect.forEach(rows.slice(0, 50), decodeRow, { concurrency: 1 }),
        ),
        Effect.map((items) => ({
          items,
          ...(items.length === 50
            ? { nextCursor: items.at(-1)?.deliveryToken }
            : {}),
        })),
        Effect.catchTags({
          SqlError: () =>
            Effect.fail(new InboxError({ reason: "persistence-failed" })),
          ParseError: () =>
            Effect.fail(new InboxError({ reason: "persistence-failed" })),
        }),
      ),
    getInboxEvent,
    acknowledgeDelivery,
    listConversations: () => Effect.succeed({ addresses: [] }),
    readHistory: () => Effect.succeed({ records: [] }),
    inboxChanged: Stream.never,
  });
  const pending = sql<{
    readonly total: number;
  }>`SELECT COUNT(*) AS total FROM callback_fixture WHERE acknowledged=0`.pipe(
    Effect.map((rows) => ({ pendingCount: rows[0]?.total ?? 0 })),
  );
  return { ...database, harness, save, pending };
});
