/** @file Inbox wake-ups posted to a subscribed callback. */

import { FileSystem } from "@effect/platform";
import { NodeContext } from "@effect/platform-node";
import { it } from "@effect/vitest";
import {
  Array,
  type Duration,
  Effect,
  Redacted,
  type Scope,
  TestClock,
  TestContext,
  TestServices,
} from "effect";
import * as fc from "fast-check";
import { createHmac } from "node:crypto";
import { describe, expect } from "vitest";
import {
  decodeWakeUp,
  enqueue,
  input,
  openCallbackInbox,
  scriptedWebhook,
  secret,
  url,
} from "../__tests__/webhook-fixtures.js";
import { INBOX_PENDING_EVENT } from "./contracts.js";
import {
  isPublicWebhookAddress,
  sendWebhook,
  webhookHttpClientLayer,
} from "./webhook-client.js";

type Store = Effect.Effect.Success<ReturnType<typeof openCallbackInbox>>;
type Callback = Effect.Effect.Success<ReturnType<typeof scriptedWebhook>>;
type Events = Effect.Effect.Success<Callback["acquire"]>;

const accepted = 200;
const unavailable = 503;
const gone = 410;
const redirected = 302;
const callbackVerificationFailed = -32015;
const anotherCallbackSubscribed = -32013;
const subscriptionNotFound = -32011;
const serverError = "http_5xx";
const connectionRefused = "connection_refused";
const callbackGone = "terminal";
const callbackFailing = "callback";
const notSubscribed = "none";
const subscribed = "webhook";
const otherCallback = {
  ...input,
  delivery: { ...input.delivery, url: "https://other.example/events" },
};

/** The wake-ups the callback has received, leaving out verification challenges. */
const wakeUps = (callback: Callback) =>
  callback.posts.filter((post) =>
    post.headers["webhook-id"]?.startsWith("wake_"),
  );

/** Lets work that is already running reach the callback, in real time. */
const settle = TestServices.provideLive(Effect.sleep("40 millis"));

/** Moves the daemon's clock, which is what makes it look at the inbox again. */
const advance = (span: Duration.DurationInput = "1 second") =>
  TestClock.adjust(span).pipe(Effect.zipRight(settle));

/** Advances a second at a time until the callback has received `count` wake-ups. */
const awaitWakeUps = (callback: Callback, count: number) =>
  advance().pipe(
    Effect.repeat({
      until: () => wakeUps(callback).length >= count,
      times: 20,
    }),
    Effect.zipRight(
      Effect.sync(() => {
        expect(wakeUps(callback)).toHaveLength(count);
      }),
    ),
  );

/** Runs a test body with a new inbox, a scripted callback and its webhook. */
const scenario = <A, E>(
  body: (context: {
    readonly store: Store;
    readonly callback: Callback;
    readonly events: Events;
  }) => Effect.Effect<A, E, Scope.Scope>,
) =>
  Effect.gen(function* () {
    const files = yield* FileSystem.FileSystem;
    const store = yield* openCallbackInbox(
      yield* files.makeTempDirectoryScoped(),
    );
    const callback = yield* scriptedWebhook(store);
    const events = yield* callback.acquire;
    return yield* body({ store, callback, events });
  }).pipe(Effect.scoped, Effect.provide(NodeContext.layer));

/** With `count` items pending, the first wake-up reports exactly `count`. */
const reportsPendingCount = (count: number) =>
  scenario(({ store, callback, events }) =>
    Effect.gen(function* () {
      yield* Effect.forEach(
        Array.makeBy(count, (index) => index + 1),
        (byte) => enqueue(store, byte),
        { concurrency: 1 },
      );
      yield* events.subscribe(input);
      yield* awaitWakeUps(callback, 1);
      expect(decodeWakeUp(wakeUps(callback)[0]?.body).data).toEqual({
        pendingCount: count,
      });
    }),
  ).pipe(Effect.provide(TestContext.TestContext), Effect.runPromise);

describe("a subscribed callback", () => {
  it.effect("is not posted to while the inbox is empty", () =>
    scenario(({ callback, events }) =>
      Effect.gen(function* () {
        yield* events.subscribe(input);
        yield* advance();
        yield* advance();
        expect(wakeUps(callback)).toEqual([]);
      }),
    ),
  );

  it.effect(
    "receives one signed wake-up when an item arrives, and the item stays in the inbox",
    () =>
      scenario(({ store, callback, events }) =>
        Effect.gen(function* () {
          yield* events.subscribe(input);
          yield* enqueue(store, 1);
          yield* awaitWakeUps(callback, 1);
          const [post] = wakeUps(callback);
          const signature = createHmac("sha256", Buffer.alloc(32, 1))
            .update(
              `${post?.headers["webhook-id"]}.${post?.headers["webhook-timestamp"]}.${post?.body}`,
            )
            .digest("base64");
          expect(post?.headers["webhook-signature"]).toBe(`v1,${signature}`);
          const event = decodeWakeUp(post?.body);
          expect(event.name).toBe(INBOX_PENDING_EVENT);
          expect(event.eventId).toBe(post?.headers["webhook-id"]);
          expect(event.data).toEqual(yield* store.pending);
          yield* advance();
          yield* advance();
          expect(wakeUps(callback)).toHaveLength(1);
        }),
      ),
  );

  it.live("is told how many items are pending, for any number of them", () =>
    Effect.tryPromise(() =>
      fc.assert(
        fc.asyncProperty(fc.integer({ min: 1, max: 60 }), reportsPendingCount),
        { numRuns: 5 },
      ),
    ),
  );

  it.effect("is woken again when a later item arrives", () =>
    scenario(({ store, callback, events }) =>
      Effect.gen(function* () {
        yield* events.subscribe(input);
        yield* enqueue(store, 1);
        yield* awaitWakeUps(callback, 1);
        yield* enqueue(store, 2);
        yield* awaitWakeUps(callback, 2);
        const [first, second] = wakeUps(callback);
        expect(second?.headers["webhook-id"]).not.toBe(
          first?.headers["webhook-id"],
        );
        expect(decodeWakeUp(second?.body).data).toEqual(yield* store.pending);
      }),
    ),
  );
});

describe("a callback that fails", () => {
  it.effect(
    "gets the same wake-up again after a failure, until it accepts one",
    () =>
      scenario(({ store, callback, events }) =>
        Effect.gen(function* () {
          yield* events.subscribe(input);
          callback.status(unavailable);
          yield* enqueue(store, 1);
          yield* awaitWakeUps(callback, 1);
          expect((yield* events.status).lastError).toBe(serverError);
          yield* advance("1 minute");
          yield* advance("5 minutes");
          const failed = wakeUps(callback);
          expect(failed.length).toBeGreaterThan(1);
          expect(new Set(failed.map((post) => post.body)).size).toBe(1);
          callback.status(accepted);
          yield* advance("5 minutes");
          const delivered = wakeUps(callback).length;
          expect((yield* events.status).lastError).toBeNull();
          yield* advance("5 minutes");
          expect(wakeUps(callback)).toHaveLength(delivered);
        }),
      ),
  );
  it.effect(
    "is left alone once it answers that it is gone, until it subscribes again",
    () =>
      scenario(({ store, callback, events }) =>
        Effect.gen(function* () {
          yield* events.subscribe(input);
          callback.status(gone);
          yield* enqueue(store, 1);
          yield* awaitWakeUps(callback, 1);
          expect((yield* events.status).stalled).toBe(callbackGone);
          yield* advance("1 minute");
          expect(wakeUps(callback)).toHaveLength(1);
          const restarted = yield* callback.acquire;
          expect((yield* restarted.status).stalled).toBe(callbackGone);
          callback.status(accepted);
          yield* restarted.subscribe(input);
          expect((yield* restarted.status).stalled).toBeNull();
          yield* awaitWakeUps(callback, 2);
        }),
      ),
  );

  it.effect(
    "is left alone after a hundred failures over an hour, until it subscribes again",
    () =>
      scenario(({ store, callback, events }) =>
        Effect.gen(function* () {
          yield* events.subscribe(input);
          callback.status(unavailable);
          yield* enqueue(store, 1);
          yield* TestClock.adjust("5 minutes").pipe(
            Effect.repeat({
              until: () => wakeUps(callback).length >= 100,
              times: 150,
            }),
          );
          yield* advance();
          expect((yield* events.status).stalled).toBe(callbackFailing);
          const stalledAt = wakeUps(callback).length;
          yield* advance("10 minutes");
          expect(wakeUps(callback)).toHaveLength(stalledAt);
          callback.status(accepted);
          yield* events.subscribe(input);
          yield* awaitWakeUps(callback, stalledAt + 1);
        }),
      ),
  );
});

describe("a callback across restarts and leases", () => {
  it.effect(
    "is woken after a restart while items are still pending, with its subscription kept",
    () =>
      scenario(({ store, callback, events }) =>
        Effect.gen(function* () {
          const granted = yield* events.subscribe(input);
          yield* enqueue(store, 1);
          yield* awaitWakeUps(callback, 1);
          const restarted = yield* callback.acquire;
          expect((yield* restarted.status).id).toBe(granted.id);
          yield* awaitWakeUps(callback, 2);
        }),
      ),
  );

  it.effect(
    "is not posted to after it unsubscribes or its lease runs out",
    () =>
      scenario(({ store, callback, events }) =>
        Effect.gen(function* () {
          yield* events.subscribe(input);
          yield* events.unsubscribe(input);
          yield* enqueue(store, 1);
          yield* advance();
          expect(wakeUps(callback)).toEqual([]);
          expect((yield* events.status).mode).toBe(notSubscribed);
          expect(
            (yield* events.unsubscribe(input).pipe(Effect.flip)).code,
          ).toBe(subscriptionNotFound);

          yield* events.subscribe({ ...input, ttlMs: 60_000 });
          yield* awaitWakeUps(callback, 1);
          yield* advance("2 minutes");
          yield* enqueue(store, 2);
          yield* advance();
          yield* advance();
          expect(wakeUps(callback)).toHaveLength(1);
        }),
      ),
  );
});

describe("subscribing", () => {
  it.effect(
    "is refused when the callback does not echo the challenge or redirects",
    () =>
      scenario(({ callback, events }) =>
        Effect.gen(function* () {
          callback.challenge(false);
          expect((yield* events.subscribe(input).pipe(Effect.flip)).code).toBe(
            callbackVerificationFailed,
          );
          callback.challenge(true);
          callback.status(redirected);
          expect((yield* events.subscribe(input).pipe(Effect.flip)).code).toBe(
            callbackVerificationFailed,
          );
          expect((yield* events.status).mode).toBe(notSubscribed);
        }),
      ),
  );

  it.effect("a second callback is refused until the first unsubscribes", () =>
    scenario(({ events }) =>
      Effect.gen(function* () {
        yield* events.subscribe(input);
        expect(
          (yield* events.subscribe(otherCallback).pipe(Effect.flip)).code,
        ).toBe(anotherCallbackSubscribed);
        yield* events.unsubscribe(input);
        yield* events.subscribe(otherCallback);
        expect((yield* events.status).mode).toBe(subscribed);
      }),
    ),
  );

  it.effect(
    "keeps each agent's subscription separate and sealed in one database",
    () =>
      scenario(({ store }) =>
        Effect.gen(function* () {
          const first = yield* scriptedWebhook(store, "first");
          const second = yield* scriptedWebhook(store, "second");
          const firstEvents = yield* first.acquire;
          const secondEvents = yield* second.acquire;
          yield* firstEvents.subscribe(input);
          expect((yield* secondEvents.status).mode).toBe(notSubscribed);
          yield* secondEvents.subscribe(input);
          yield* firstEvents.unsubscribe(input);
          expect((yield* (yield* first.acquire).status).mode).toBe(
            notSubscribed,
          );
          expect((yield* (yield* second.acquire).status).mode).toBe(subscribed);
          const saved = JSON.stringify(
            yield* store.sql<
              Record<"state", string>
            >`SELECT state FROM mcp_webhook_state`,
          );
          expect(saved).not.toContain(secret);
          expect(saved).not.toContain(url);
          const wrongKey = yield* scriptedWebhook(
            store,
            "second",
            Redacted.make(new Uint8Array(32).fill(9)),
          );
          expect(yield* wrongKey.acquire.pipe(Effect.isFailure)).toBe(true);
        }),
      ),
  );
});

const octet = fc.integer({ min: 0, max: 255 });
/** IPv4 addresses a callback must never reach: private, loopback and link-local ranges. */
const privateIpv4 = fc
  .tuple(
    fc.constantFrom("10", "127", "169.254", "192.168"),
    octet,
    octet,
    octet,
  )
  .map(([prefix, second, third, fourth]) =>
    prefix.includes(".")
      ? `${prefix}.${third}.${fourth}`
      : `${prefix}.${second}.${third}.${fourth}`,
  );
/** 127.0.0.1 written as the last two groups of an IPv6 address. */
const loopbackAsHex = "7f00:1";

describe("the callback client", () => {
  it("treats every private, loopback and link-local IPv4 address as not public", () => {
    fc.assert(
      fc.property(privateIpv4, (address) => {
        expect(isPublicWebhookAddress(address)).toBe(false);
        expect(isPublicWebhookAddress(`::ffff:${address}`)).toBe(false);
      }),
    );
  });

  it.each(["::1", "2001:db8::1", `64:ff9b::${loopbackAsHex}`])(
    "treats %s as not public",
    (address) => {
      expect(isPublicWebhookAddress(address)).toBe(false);
    },
  );

  it("treats addresses in a public range as public", () => {
    fc.assert(
      fc.property(octet, octet, octet, (second, third, fourth) => {
        expect(isPublicWebhookAddress(`8.${second}.${third}.${fourth}`)).toBe(
          true,
        );
      }),
    );
  });

  it.live.each(["https://127.0.0.1/private", "https://localhost/private"])(
    "refuses to post to %s",
    (privateUrl) =>
      Effect.gen(function* () {
        const refused = yield* sendWebhook({
          url: privateUrl,
          secret,
          messageId: "wake_test",
          subscriptionId: "sub_test",
          body: "{}",
          timestamp: 0,
        }).pipe(Effect.provide(webhookHttpClientLayer), Effect.flip);
        expect(refused.reason).toBe(connectionRefused);
      }),
  );
});
