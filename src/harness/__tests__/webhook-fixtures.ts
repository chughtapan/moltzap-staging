/** @file A scripted webhook callback and an inbox whose entries wake it. */

import {
  type HttpBody,
  HttpClient,
  HttpClientResponse,
} from "@effect/platform";
import { SqlClient } from "@effect/sql";
import {
  Effect,
  Exit,
  Match,
  PubSub,
  Ref,
  Schema,
  Scope,
  Stream,
} from "effect";
import { digest, testAgentCard } from "../../__tests__/agents.js";
import { openTestDatabase } from "../../communication/__tests__/store-fixtures.js";
import { PermittedItem } from "../../firewall/index.js";
import { INBOX_PENDING_EVENT } from "../contracts.js";
import {
  DeliveryToken,
  InboxError,
  StoredInboxEntry,
} from "../message-format.js";
import { SocialHarness } from "../social-harness.js";
import { makeWebhookEvents } from "../webhook.js";
type InboxFixture = Effect.Effect.Success<ReturnType<typeof openCallbackInbox>>;

/** The signing secret of the scripted subscription. */
export const secret = `whsec_${Buffer.alloc(32, 1).toString("base64")}`;

/** The callback address of the scripted subscription. */
const url = "https://callback.example/events";

/** A subscription that wakes the scripted callback when the inbox has pending entries. */
export const input = {
  name: INBOX_PENDING_EVENT,
  arguments: {},
  cursor: null,
  delivery: { mode: "webhook", url, secret },
};

/** The verification challenge a subscription's callback must echo. */
const verification = Schema.Struct({
  type: Schema.Literal("verification"),
  challenge: Schema.String,
});

/** One wake-up posted to the callback. */
const wakeUp = Schema.Struct({
  eventId: Schema.String,
  name: Schema.String,
  timestamp: Schema.String,
  data: Schema.Struct({ pendingCount: Schema.Number }),
  cursor: Schema.Null,
});

/** Any body the callback receives, decoded from its JSON. */
const callback = Schema.parseJson(Schema.Union(verification, wakeUp));

/** Decode one wake-up from the JSON text the callback received. */
export const decodeWakeUp = Schema.decodeUnknownSync(Schema.parseJson(wakeUp));

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
const token = (byte: number) =>
  Schema.decodeUnknownSync(DeliveryToken)(digest("dlv_", byte));

/** A message from Alice to Bob, as the inbox holds it. */
const item = Schema.decodeUnknownSync(PermittedItem)({
  kind: "message",
  message: {
    author: testAgentCard("agent:alice"),
    recipients: [testAgentCard("agent:bob")],
    msgId: digest("msg_", 1),
    content: [{ type: "text", text: "hello" }],
  },
});

/**
 * Put the scripted inbox item `byte` into `store`.
 * @param store The store the webhook reads.
 * @param byte Distinguishes the item's token.
 * @returns The store's mutation.
 */
export const enqueue = (store: InboxFixture, byte: number) =>
  store.save(token(byte), item);

/**
 * Webhook events over `store` whose callback is scripted: it records each
 * post, echoes verification challenges, and answers with the status the test
 * sets.
 */
export const scriptedWebhook = Effect.fn("scriptedWebhook")(function* (
  store: InboxFixture,
  agentKey?: string,
) {
  let status = 200;
  let challengeValid = true;
  const currentScope = yield* Ref.make<Scope.CloseableScope | undefined>(
    undefined,
  );
  const posts: Array<{
    readonly body: string;
    readonly headers: Readonly<Record<string, string>>;
  }> = [];
  const client = HttpClient.make((request) =>
    Effect.sync(() => {
      const body = requestBody(request.body);
      posts.push({ body, headers: { ...request.headers } });
      const decoded = Schema.decodeUnknownSync(callback)(body);
      return callbackResponse(request, decoded, status, challengeValid);
    }).pipe(Effect.withSpan("webhookCallback")),
  );
  return {
    posts,
    status: (value: number) => {
      status = value;
    },
    challenge: (value: boolean) => {
      challengeValid = value;
    },
    acquire: Effect.gen(function* () {
      const scope = yield* Scope.make();
      const previous = yield* Ref.getAndSet(currentScope, scope);
      if (previous !== undefined) {
        yield* Scope.close(previous, Exit.void);
      }
      yield* Effect.addFinalizer((exit) => Scope.close(scope, exit));
      return yield* makeWebhookEvents(agentKey ?? store.directory).pipe(
        Scope.extend(scope),
        Effect.provideService(HttpClient.HttpClient, client),
        Effect.provideService(SocialHarness, store.harness),
        Effect.provideService(SqlClient.SqlClient, store.sql),
      );
    }),
  };
});

/** A SQL-backed service fixture exercises webhook transactions without reading harness-owned tables. */
export const openCallbackInbox = Effect.fn("openCallbackInbox")(function* (
  directory: string,
) {
  const database = yield* openTestDatabase(directory);
  const sql = database.sql;
  yield* sql.unsafe(
    "CREATE TABLE IF NOT EXISTS callback_fixture (token TEXT PRIMARY KEY, payload TEXT NOT NULL, delivered INTEGER NOT NULL DEFAULT 0)",
  );
  const payload = Schema.parseJson(Schema.typeSchema(StoredInboxEntry));
  const changes = yield* PubSub.unbounded<undefined>();
  const save = (
    deliveryToken: DeliveryToken,
    item: typeof PermittedItem.Type,
  ) =>
    Schema.encode(payload)({
      address: Schema.decodeUnknownSync(StoredInboxEntry.fields.address)(
        "agent:bob",
      ),
      item,
    }).pipe(
      Effect.flatMap(
        (bytes) =>
          sql`INSERT INTO callback_fixture(token,payload) VALUES (${deliveryToken},${bytes})`,
      ),
      Effect.zipRight(PubSub.publish(changes, undefined)),
      Effect.asVoid,
    );
  const acknowledgeDelivery = (deliveryToken: DeliveryToken) =>
    sql`UPDATE callback_fixture SET delivered=1 WHERE token=${deliveryToken}`.pipe(
      Effect.asVoid,
      Effect.orDie,
    );
  const decodeRow = (row: {
    readonly token: string;
    readonly payload: string;
  }) =>
    Schema.decodeUnknown(payload)(row.payload).pipe(
      Effect.map((stored) => ({
        ...stored,
        deliveryToken: Schema.decodeUnknownSync(DeliveryToken)(row.token),
      })),
    );
  const harness = SocialHarness.of({
    send: () => Effect.dieMessage("Unexpected send"),
    listInbox: (request = {}) =>
      sql<{
        readonly token: string;
        readonly payload: string;
      }>`SELECT token,payload FROM callback_fixture WHERE delivered=0 AND token>${request.cursor ?? ""} ORDER BY token LIMIT 51`.pipe(
        Effect.flatMap((rows) =>
          Effect.forEach(rows.slice(0, 50), decodeRow, { concurrency: 1 }),
        ),
        Effect.map((entries) => ({
          entries,
          ...(entries.length === 50
            ? { nextCursor: entries.at(-1)?.deliveryToken }
            : {}),
        })),
        Effect.catchTags({
          SqlError: () =>
            Effect.fail(new InboxError({ reason: "persistence-failed" })),
          ParseError: () =>
            Effect.fail(new InboxError({ reason: "persistence-failed" })),
        }),
      ),
    acknowledgeDelivery,
    listConversations: () => Effect.succeed({ addresses: [] }),
    readHistory: () => Effect.succeed({ records: [] }),
    inboxChanged: Stream.fromPubSub(changes),
  });
  const pending = sql<{
    readonly total: number;
  }>`SELECT COUNT(*) AS total FROM callback_fixture WHERE delivered=0`.pipe(
    Effect.map((rows) => ({ pendingCount: rows[0]?.total ?? 0 })),
  );
  return { ...database, harness, save, pending };
});
