/** @file A scripted callback and an inbox whose entries wake it. */

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
import { openTestDatabase } from "../../test.js";
import { SocialHarness } from "../agent.js";
import { INBOX_PENDING_EVENT, makeWebhookEvents } from "../subscriptions.js";
type InboxFixture = Effect.Effect.Success<ReturnType<typeof openCallbackInbox>>;

/** The signing secret of the scripted subscription. */
export const secret = `whsec_${Buffer.alloc(32, 1).toString("base64")}`;

/** The callback address of the scripted subscription. */
const url = "https://callback.example/events";

/** The request that subscribes the scripted callback to wake-ups for inbox entries. */
export const subscribeRequest = {
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
  data: Schema.Struct({}),
  cursor: Schema.Null,
});

/** Any body the callback receives, decoded from its JSON. */
const callback = Schema.parseJson(Schema.Union(verification, wakeUp));

/** Decode one wake-up from the JSON text the callback received. */
export const decodeWakeUp = Schema.decodeUnknownSync(Schema.parseJson(wakeUp));

/**
 * The exact bytes of a callback request, as text.
 * @param body The request body the daemon posted.
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

/** Puts one more entry into `store`, the inbox wake-ups follow. */
export const enqueue = (store: InboxFixture) => store.save;

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
  const callbackHttp = HttpClient.make((request) =>
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
      return yield* makeWebhookEvents(agentKey ?? "alice").pipe(
        Scope.extend(scope),
        Effect.provideService(HttpClient.HttpClient, callbackHttp),
        Effect.provideService(SocialHarness, store.harness),
        Effect.provideService(SqlClient.SqlClient, store.sql),
      );
    }),
  };
});

/** A SQL-backed service fixture exercises subscription transactions without reading harness-owned tables. */
export const openCallbackInbox = Effect.fn("openCallbackInbox")(function* () {
  const database = yield* openTestDatabase();
  const sql = database.sql;
  yield* sql.unsafe("CREATE TABLE callback_fixture (entry SERIAL PRIMARY KEY)");
  const changes = yield* PubSub.unbounded<undefined>();
  const save = sql`INSERT INTO callback_fixture DEFAULT VALUES`.pipe(
    Effect.zipRight(PubSub.publish(changes, undefined)),
    Effect.asVoid,
  );
  const harness = SocialHarness.of({
    send: () => Effect.dieMessage("Unexpected send"),
    inboxEnd: sql<{
      readonly total: number;
    }>`SELECT COUNT(*)::int AS total FROM callback_fixture`.pipe(
      Effect.map((rows) => rows[0]?.total ?? 0),
    ),
    receive: () => Effect.dieMessage("Unexpected receive"),
    listConversations: () => Effect.succeed({ addresses: [] }),
    readHistory: () => Effect.succeed({ records: [] }),
    inboxChanged: Stream.fromPubSub(changes),
  });
  return { ...database, harness, save };
});
