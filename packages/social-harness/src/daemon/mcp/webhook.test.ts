/** @file Local Dot callback conformance through Effect's HttpClient service. */

import * as NodeSdk from "@effect/opentelemetry/NodeSdk";
import { live as it } from "@effect/vitest";
import {
  InMemorySpanExporter,
  SimpleSpanProcessor,
} from "@opentelemetry/sdk-trace-base";
import { Clock, Context, Deferred, Duration, Effect, Fiber } from "effect";
import { createHmac } from "node:crypto";
import { describe, expect } from "vitest";
import { openInboxFixture } from "../../__tests__/inbox-fixture.js";
import { localGuidance } from "../../__tests__/message-policy-fixtures.js";
import { stateDirectory } from "../../__tests__/store-schema-fixtures.js";
import {
  decodeOccurrence,
  enqueue,
  input,
  item,
  scriptedWebhook,
  secret,
  token,
  url,
} from "../../__tests__/webhook-fixtures.js";
import { encodeRuntimeValue } from "../../communication/store/index.js";
import { maximumEventBytes } from "../../host-protocol/index.js";
import { readRuntimeEvent } from "../inbox/inbox.js";
import {
  isPublicWebhookAddress,
  sendWebhook,
  webhookHttpClientLayer,
} from "./event-signing.js";
import { makeHarnessPollEvents } from "./events.js";

/* eslint-disable agent-code-guard/no-hardcoded-assertion-literals, sonarjs/no-hardcoded-ip -- Protocol codes and deliberately unsafe IP fixtures pin the callback trust boundary. */

/** Moves the clock of a scenario that runs `onSteppedClock`. */
class SteppedClock extends Context.Tag("SteppedClock")<
  SteppedClock,
  (span: Duration.DurationInput) => Effect.Effect<void>
>() {}

/** Moves the clock of the running scenario by `span`, at once. */
const stepClock = (span: Duration.DurationInput) =>
  Effect.flatMap(SteppedClock, (step) => step(span));

/**
 * Runs `scenario` on its own clock, starting at the epoch, which only
 * `stepClock` moves, at once. These scenarios only read the time, and the
 * clock's sleeps never end because a scripted callback answers before its
 * bound.
 * @param scenario Scenario that reads and steps the clock.
 * @returns The scenario on a fresh stepped clock.
 */
const onSteppedClock = <Value, Failure>(
  scenario: Effect.Effect<Value, Failure, SteppedClock>,
): Effect.Effect<Value, Failure> =>
  Effect.suspend(() => {
    let millis = 0;
    const clock: Clock.Clock = {
      [Clock.ClockTypeId]: Clock.ClockTypeId,
      unsafeCurrentTimeMillis: () => millis,
      currentTimeMillis: Effect.sync(() => millis),
      unsafeCurrentTimeNanos: () => BigInt(millis) * 1_000_000n,
      currentTimeNanos: Effect.sync(() => BigInt(millis) * 1_000_000n),
      sleep: () => Effect.never,
    };
    return scenario.pipe(
      Effect.withClock(clock),
      Effect.provideService(SteppedClock, (span) =>
        Effect.sync(() => {
          millis += Duration.toMillis(span);
        }),
      ),
    );
  });

/** Select callbacks by their signed event identity, independently of verification traffic. */
const eventPosts = (
  posts: Effect.Effect.Success<ReturnType<typeof scriptedWebhook>>["posts"],
  byte: number,
) => posts.filter((post) => post.headers["webhook-id"] === token(byte));

/** Verification challenges have their own signed identity prefix. */
const verificationPosts = (
  posts: Effect.Effect.Success<ReturnType<typeof scriptedWebhook>>["posts"],
) =>
  posts.filter((post) =>
    post.headers["webhook-id"]?.startsWith("msg_verification_"),
  );

const rotatedSubscription = {
  ...input,
  delivery: {
    ...input.delivery,
    secret: `whsec_${Buffer.alloc(32, 2).toString("base64")}`,
  },
};
const signsVerifiesAndRotates = () =>
  Effect.scoped(
    Effect.gen(function* () {
      const store = yield* openInboxFixture(stateDirectory());
      const test = yield* scriptedWebhook(store);
      const events = yield* test.acquire;
      const grant = yield* events.subscribe(input);
      const control = test.posts[0];
      expect(control?.headers["webhook-id"]).toMatch(/^msg_verification_/u);
      const expected = createHmac("sha256", Buffer.alloc(32, 1))
        .update(
          `${control?.headers["webhook-id"]}.${control?.headers["webhook-timestamp"]}.${control?.body}`,
        )
        .digest("base64");
      expect(control?.headers["webhook-signature"]).toBe(`v1,${expected}`);
      yield* enqueue(store, 1);
      yield* events.observe();
      expect(decodeOccurrence(eventPosts(test.posts, 1)[0]?.body).data).toEqual(
        {
          kind: "item",
          item: item(),
        },
      );
      expect((yield* events.status).pendingCount).toBe(0);
      const rotated = rotatedSubscription;
      expect((yield* events.subscribe(rotated)).id).toBe(grant.id);
      test.challenge(false);
      yield* stepClock("11 minutes");
      expect((yield* events.subscribe(input).pipe(Effect.flip)).code).toBe(
        -32015,
      );
      expect(
        (yield* events
          .unsubscribe({
            ...input,
            delivery: { url: "https://other.example/events" },
          })
          .pipe(Effect.flip)).code,
      ).toBe(-32011);
      yield* events.unsubscribe({ ...input, delivery: { url } });
      expect((yield* readRuntimeEvent(token(1)).pipe(store.run)).item).toEqual(
        item(),
      );
    }),
  ).pipe(onSteppedClock);

const retainsTerminalRejections = () =>
  Effect.scoped(
    Effect.gen(function* () {
      for (const status of [410, 413]) {
        const store = yield* openInboxFixture(stateDirectory());
        const test = yield* scriptedWebhook(store);
        const events = yield* test.acquire;
        yield* enqueue(store, 1);
        yield* events.subscribe(input);
        test.status(status);
        yield* events.observe();
        expect((yield* events.status).stalled).toBe("terminal");
        const restarted = yield* test.acquire;
        test.status(200);
        yield* restarted.subscribe(input);
        expect((yield* restarted.resume.pipe(Effect.flip)).code).toBe(-32014);
        yield* stepClock("1 day");
        yield* restarted.subscribe(input);
        yield* restarted.observe();
        expect(eventPosts(test.posts, 1)).toHaveLength(1);
        expect((yield* store.readInboxSummary()).pendingCount).toBe(1);
        yield* restarted.revoke;
        yield* restarted.subscribe(input);
        yield* restarted.observe();
        expect((yield* store.readInboxSummary()).pendingCount).toBe(0);
      }
    }),
  ).pipe(onSteppedClock);

it("delivers local guidance and its subject map through a signed webhook", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const store = yield* openInboxFixture(stateDirectory());
      const test = yield* scriptedWebhook(store);
      const events = yield* test.acquire;
      const guidance = localGuidance(1);
      yield* store.putInboxItem({
        deliveryToken: token(1),
        canonicalItem: yield* encodeRuntimeValue(item()),
        canonicalGuidance: yield* encodeRuntimeValue(guidance),
      });
      yield* events.subscribe(input);
      yield* events.observe();
      expect(decodeOccurrence(eventPosts(test.posts, 1)[0]?.body).data).toEqual(
        {
          kind: "item",
          item: item(),
          guidance,
        },
      );
      expect(
        (yield* readRuntimeEvent(token(1)).pipe(store.run)).guidance,
      ).toEqual(guidance);
    }),
  ).pipe(onSteppedClock));

const verifiesRotatedSecret = () =>
  Effect.scoped(
    Effect.gen(function* () {
      const store = yield* openInboxFixture(stateDirectory());
      const test = yield* scriptedWebhook(store);
      const events = yield* test.acquire;
      const grant = yield* events.subscribe(input);
      const rotated = {
        ...input,
        delivery: {
          ...input.delivery,
          secret: `whsec_${Buffer.alloc(32, 2).toString("base64")}`,
        },
      };
      test.challenge(false);
      expect((yield* events.subscribe(rotated).pipe(Effect.flip)).code).toBe(
        -32015,
      );
      expect(verificationPosts(test.posts)).toHaveLength(2);
      test.challenge(true);
      expect((yield* events.subscribe(rotated)).id).toBe(grant.id);
      expect(verificationPosts(test.posts)).toHaveLength(3);
      yield* events.subscribe(rotated);
      expect(verificationPosts(test.posts)).toHaveLength(3);
    }),
  ).pipe(onSteppedClock);

const permitsReadAndRevokeDuringDelivery = () =>
  Effect.scoped(
    Effect.gen(function* () {
      const store = yield* openInboxFixture(stateDirectory());
      const test = yield* scriptedWebhook(store);
      const events = yield* test.acquire;
      yield* enqueue(store, 1);
      yield* events.subscribe(input);
      const entered = yield* Deferred.make<undefined>();
      const release = yield* Deferred.make<undefined>();
      test.onEvent(
        Deferred.succeed(entered, undefined).pipe(
          Effect.zipRight(Deferred.await(release)),
          Effect.asVoid,
        ),
      );
      const delivery = yield* events.observe().pipe(Effect.fork);
      yield* Deferred.await(entered);
      expect((yield* readRuntimeEvent(token(1)).pipe(store.run)).item).toEqual(
        item(),
      );
      yield* events.revoke;
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(delivery);
      expect((yield* events.status).mode).toBe("none");
      expect((yield* store.readInboxSummary()).pendingCount).toBe(1);
    }),
  );

const refusesExpiredReceipt = () =>
  Effect.scoped(
    Effect.gen(function* () {
      const store = yield* openInboxFixture(stateDirectory());
      const test = yield* scriptedWebhook(store);
      const events = yield* test.acquire;
      yield* enqueue(store, 1);
      yield* events.subscribe({ ...input, ttlMs: 60_000 });
      const entered = yield* Deferred.make<undefined>();
      const release = yield* Deferred.make<undefined>();
      test.onEvent(
        Deferred.succeed(entered, undefined).pipe(
          Effect.zipRight(Deferred.await(release)),
          Effect.asVoid,
        ),
      );
      const delivery = yield* events.observe().pipe(Effect.forkScoped);
      yield* Deferred.await(entered);
      yield* stepClock("61 seconds");
      const poll = yield* makeHarnessPollEvents(store, test.gate, test.owner);
      yield* poll.poll(
        { name: input.name, arguments: {}, cursor: null },
        {},
        Effect.void,
      );
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(delivery);
      expect((yield* poll.status).mode).toBe("poll");
      expect((yield* store.readInboxSummary()).pendingCount).toBe(1);
    }),
  ).pipe(onSteppedClock);

it(
  "does not retire an item when an expired callback answers after polling takes over",
  refusesExpiredReceipt,
);

const refreshPreservesTerminalReceipt = () =>
  Effect.scoped(
    Effect.gen(function* () {
      const store = yield* openInboxFixture(stateDirectory());
      const test = yield* scriptedWebhook(store);
      const events = yield* test.acquire;
      yield* enqueue(store, 1);
      yield* events.subscribe(input);
      const entered = yield* Deferred.make<undefined>();
      const release = yield* Deferred.make<undefined>();
      test.status(413);
      test.onEvent(
        Deferred.succeed(entered, undefined).pipe(
          Effect.zipRight(Deferred.await(release)),
          Effect.asVoid,
        ),
      );
      const delivery = yield* events.observe().pipe(Effect.fork);
      yield* Deferred.await(entered);
      yield* events.subscribe(input);
      yield* events.resume;
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(delivery);
      expect((yield* events.status).stalled).toBe("terminal");
      expect((yield* store.readInboxSummary()).pendingCount).toBe(1);
    }),
  ).pipe(onSteppedClock);

const retriesAcrossRestart = () => {
  const path = stateDirectory();
  let original: string | undefined;
  return Effect.gen(function* () {
    yield* Effect.scoped(
      Effect.gen(function* () {
        const store = yield* openInboxFixture(path);
        const test = yield* scriptedWebhook(store);
        const events = yield* test.acquire;
        yield* enqueue(store, 1);
        yield* events.subscribe(input);
        test.status(503);
        yield* events.observe();
        original = eventPosts(test.posts, 1)[0]?.body;
        expect((yield* store.readInboxSummary()).pendingCount).toBe(1);
      }),
    );
    yield* stepClock("3 seconds");
    yield* Effect.scoped(
      Effect.gen(function* () {
        const store = yield* openInboxFixture(path);
        const test = yield* scriptedWebhook(store);
        const events = yield* test.acquire;
        yield* events.observe();
        expect(eventPosts(test.posts, 1)[0]?.body).toBe(original);
        expect(eventPosts(test.posts, 1)[0]?.headers["webhook-id"]).toBe(
          token(1),
        );
        expect((yield* store.readInboxSummary()).pendingCount).toBe(0);
        yield* stepClock("10 minutes");
        yield* events.observe();
        expect(eventPosts(test.posts, 1)).toHaveLength(1);
      }),
    );
    yield* Effect.scoped(
      Effect.gen(function* () {
        const store = yield* openInboxFixture(path);
        expect(
          (yield* readRuntimeEvent(token(1)).pipe(store.run)).item,
        ).toEqual(item());
      }),
    );
  }).pipe(onSteppedClock);
};

/** Records every span the scenario finishes in `exporter`. */
const tracedInto = (exporter: InMemorySpanExporter) =>
  Effect.provide(
    NodeSdk.layer(() => ({ spanProcessor: new SimpleSpanProcessor(exporter) })),
  );

/** The `webhook` marker spans `exporter` finished, in order. */
const webhookMarkers = (exporter: InMemorySpanExporter) =>
  exporter
    .getFinishedSpans()
    .filter(
      (span) =>
        span.name.startsWith("retry.") && span.attributes.kind === "webhook",
    )
    .map((span) => ({ name: span.name, ...span.attributes }));

/** The `webhook` pending marker for event `id`, as `webhookMarkers` lists it. */
const pendingMarker = (id: string) => ({
  name: "retry.pending",
  kind: "webhook",
  id,
});

/** The `webhook` settled marker for event `id`, as `webhookMarkers` lists it. */
const settledMarker = (id: string, outcome: string) => ({
  name: "retry.settled",
  kind: "webhook",
  id,
  outcome,
});

/**
 * A delivery waiting out its backoff after a failed callback reads busy: the
 * daemon marks it pending and settles it as interrupted when it stops, the
 * daemon that retains it after a restart marks it again, and its receipt
 * settles it as delivered.
 * @returns The scenario.
 */
const marksADeliveryBusyUntilItsReceipt = () => {
  const path = stateDirectory();
  const exporter = new InMemorySpanExporter();
  const id = token(1);
  const pending = pendingMarker(id);
  const settled = (outcome: string) => settledMarker(id, outcome);
  return Effect.gen(function* () {
    yield* Effect.scoped(
      Effect.gen(function* () {
        const store = yield* openInboxFixture(path);
        const test = yield* scriptedWebhook(store);
        const events = yield* test.acquire;
        yield* enqueue(store, 1);
        yield* events.subscribe(input);
        test.status(503);
        yield* events.observe();
      }),
    );
    expect(webhookMarkers(exporter)).toEqual([pending, settled("interrupted")]);
    yield* stepClock("3 seconds");
    yield* Effect.scoped(
      Effect.gen(function* () {
        const store = yield* openInboxFixture(path);
        const events = yield* (yield* scriptedWebhook(store)).acquire;
        yield* events.observe();
      }),
    );
    expect(webhookMarkers(exporter)).toEqual([
      pending,
      settled("interrupted"),
      pending,
      settled("delivered"),
    ]);
  }).pipe(onSteppedClock, tracedInto(exporter));
};

/**
 * A waiting delivery whose subscription lapses settles as given up at the
 * next drain, since nothing retries it any more.
 * @returns The scenario.
 */
const settlesALapsedDeliveryAsGivenUp = () => {
  const exporter = new InMemorySpanExporter();
  const id = token(1);
  return Effect.scoped(
    Effect.gen(function* () {
      const store = yield* openInboxFixture(stateDirectory());
      const test = yield* scriptedWebhook(store);
      const events = yield* test.acquire;
      yield* enqueue(store, 1);
      yield* events.subscribe(input);
      test.status(503);
      yield* events.observe();
      yield* stepClock("25 hours");
      yield* events.observe();

      expect(webhookMarkers(exporter)).toEqual([
        pendingMarker(id),
        settledMarker(id, "gave-up"),
      ]);
    }),
  ).pipe(onSteppedClock, tracedInto(exporter));
};

/**
 * Revoking the subscription of a waiting delivery settles it as interrupted.
 * @returns The scenario.
 */
const settlesARevokedDeliveryAsInterrupted = () => {
  const exporter = new InMemorySpanExporter();
  const id = token(1);
  return Effect.scoped(
    Effect.gen(function* () {
      const store = yield* openInboxFixture(stateDirectory());
      const test = yield* scriptedWebhook(store);
      const events = yield* test.acquire;
      yield* enqueue(store, 1);
      yield* events.subscribe(input);
      test.status(503);
      yield* events.observe();
      yield* events.revoke;

      expect(webhookMarkers(exporter)).toEqual([
        pendingMarker(id),
        settledMarker(id, "interrupted"),
      ]);
    }),
  ).pipe(onSteppedClock, tracedInto(exporter));
};

const resumesExhaustedCallbacks = () =>
  Effect.scoped(
    Effect.gen(function* () {
      const store = yield* openInboxFixture(stateDirectory());
      const test = yield* scriptedWebhook(store);
      const events = yield* test.acquire;
      yield* enqueue(store, 1);
      yield* events.subscribe(input);
      test.status(503);
      yield* Effect.replicateEffect(
        events.observe().pipe(Effect.zipRight(stepClock("5 minutes"))),
        99,
        { discard: true },
      );
      expect((yield* events.status).stalled).toBeNull();
      yield* events.observe();
      expect((yield* events.status).stalled).toBe("callback");
      expect((yield* store.readInboxSummary()).pendingCount).toBe(1);
      const restarted = yield* test.acquire;
      test.status(200);
      yield* stepClock("5 minutes");
      yield* restarted.observe();
      expect(eventPosts(test.posts, 1)).toHaveLength(100);
      yield* restarted.resume;
      yield* restarted.observe();
      expect(eventPosts(test.posts, 1)).toHaveLength(101);
      expect(eventPosts(test.posts, 1).at(-1)?.body).toBe(
        eventPosts(test.posts, 1)[0]?.body,
      );
      expect((yield* events.status).pendingCount).toBe(0);
      expect((yield* restarted.status).stalled).toBeNull();
    }),
  ).pipe(onSteppedClock);

const boundsPayloadAndDrains = () =>
  Effect.scoped(
    Effect.gen(function* () {
      const store = yield* openInboxFixture(stateDirectory());
      const test = yield* scriptedWebhook(store);
      const events = yield* test.acquire;
      const large = "世界".repeat(100_000);
      yield* enqueue(store, 1, large);
      for (let index = 2; index < 55; index += 1) {
        yield* enqueue(store, index);
      }
      yield* events.subscribe(input);
      yield* events.observe();
      expect(
        test.posts.filter((post) =>
          post.headers["webhook-id"]?.startsWith("dlv_"),
        ),
      ).toHaveLength(54);
      const body = eventPosts(test.posts, 1)[0]?.body ?? "";
      expect(Buffer.byteLength(body)).toBeLessThanOrEqual(maximumEventBytes);
      expect(decodeOccurrence(body).data.kind).toBe("reference");
      expect((yield* readRuntimeEvent(token(1)).pipe(store.run)).item).toEqual(
        item(large),
      );
      expect((yield* store.readInboxSummary()).pendingCount).toBe(0);
      yield* enqueue(store, 55);
      yield* stepClock("1 day");
      yield* events.observe();
      expect((yield* events.status).mode).toBe("webhook");
      expect((yield* store.readInboxSummary()).pendingCount).toBe(1);
    }),
  ).pipe(onSteppedClock);

const removesRetiredOccurrence = () =>
  Effect.scoped(
    Effect.gen(function* () {
      const store = yield* openInboxFixture(stateDirectory());
      const test = yield* scriptedWebhook(store);
      const events = yield* test.acquire;
      yield* enqueue(store, 1);
      yield* events.subscribe(input);
      test.status(503);
      yield* events.observe();
      const canonicalItem = yield* encodeRuntimeValue(item("replacement"));
      yield* store.acknowledgeInboxItem(token(1));
      yield* store.putInboxItem({ deliveryToken: token(2), canonicalItem });
      const restarted = yield* test.acquire;
      test.status(200);
      yield* restarted.observe();
      expect(eventPosts(test.posts, 1)).toHaveLength(1);
      expect(eventPosts(test.posts, 2)).toHaveLength(1);
      expect((yield* readRuntimeEvent(token(1)).pipe(store.run)).item).toEqual(
        item(),
      );
      expect(
        (yield* readRuntimeEvent(token(3)).pipe(store.run).pipe(Effect.flip))
          .reason,
      ).toBe("unknown-event");
    }),
  ).pipe(onSteppedClock);

const rejectsUnsafeCallbacks = () =>
  Effect.gen(function* () {
    for (const address of [
      "127.0.0.1",
      "10.1.2.3",
      "169.254.169.254",
      "::1",
      "::ffff:127.0.0.1",
      "2001:db8::1",
      "64:ff9b::7f00:1",
    ]) {
      expect(isPublicWebhookAddress(address)).toBe(false);
    }
    expect(isPublicWebhookAddress("8.8.8.8")).toBe(true);
    expect(isPublicWebhookAddress("2606:4700:4700::1111")).toBe(true);
    const request = {
      url: "https://127.0.0.1/private",
      secret,
      messageId: "dlv_test",
      subscriptionId: "sub_test",
      body: "{}",
      timestamp: 0,
    };
    const blocked = yield* sendWebhook(request).pipe(
      Effect.provide(webhookHttpClientLayer),
      Effect.flip,
    );
    expect(blocked.reason).toBe("connection_refused");
    const dnsBlocked = yield* sendWebhook({
      ...request,
      url: "https://localhost/private",
    }).pipe(Effect.provide(webhookHttpClientLayer), Effect.flip);
    expect(dnsBlocked.reason).toBe("connection_refused");
    const test = yield* scriptedWebhook(
      yield* openInboxFixture(stateDirectory()),
    );
    test.status(302);
    const events = yield* test.acquire;
    expect((yield* events.subscribe(input).pipe(Effect.flip)).code).toBe(
      -32015,
    );
    expect(verificationPosts(test.posts)).toHaveLength(1);
  }).pipe(Effect.scoped);

// @agent-code-guard/regression-only: External callback signatures, crash recovery and trust boundaries require conformance coverage.
describe("local Dot webhook conformance", () => {
  it(
    "verifies a rotated secret before caching its challenge",
    verifiesRotatedSecret,
  );
  it(
    "retains exhausted callbacks until owner resume retries the same event",
    resumesExhaustedCallbacks,
  );
  it(
    "preserves a terminal response during concurrent refresh and resume",
    refreshPreservesTerminalReceipt,
  );
  it(
    "discards a retired occurrence while preserving its immutable read result",
    removesRetiredOccurrence,
  );
  it(
    "retains terminally rejected items across restart and refresh",
    retainsTerminalRejections,
  );
  it(
    "allows callback inbox reads and ignores a receipt after consumer revocation",
    permitsReadAndRevokeDuringDelivery,
  );
  it(
    "verifies signed callbacks, rotates secrets and binds subscriptions to the verified callback",
    signsVerifiesAndRotates,
  );
  it(
    "retries exact event bytes after database reopen and retires only on receipt",
    retriesAcrossRestart,
  );
  it(
    "bounds complete envelopes, retains referenced payloads and drains beyond one page",
    boundsPayloadAndDrains,
  );
  it(
    "rejects private destinations, DNS to loopback and redirects through Effect HttpClient",
    rejectsUnsafeCallbacks,
  );
});

describe("webhook delivery markers", () => {
  it(
    "marks a delivery busy from its failed callback until its receipt, across a restart",
    marksADeliveryBusyUntilItsReceipt,
  );
  it(
    "settles a waiting delivery as given up once its subscription lapses",
    settlesALapsedDeliveryAsGivenUp,
  );
  it(
    "settles a waiting delivery as interrupted when its subscription is revoked",
    settlesARevokedDeliveryAsInterrupted,
  );
});

/* eslint-enable agent-code-guard/no-hardcoded-assertion-literals, sonarjs/no-hardcoded-ip -- Restore repository defaults. */
