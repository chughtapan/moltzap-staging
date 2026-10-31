/** @file Durable MCP webhook registration and classified item handoff. */

import { HttpClient } from "@effect/platform";
import { ProtocolError } from "@modelcontextprotocol/server";
import { Effect, Schema, type Scope } from "effect";
import { createHash, randomUUID, timingSafeEqual } from "node:crypto";
import type { EventStore } from "../inbox/store.js";
import {
  retryPending,
  retrySettled,
} from "../../communication/conversations/index.js";
import {
  decodeRuntimeValue,
  DeliveryToken,
  encodeRuntimeValue,
} from "../../communication/store/index.js";
import {
  callbackReasons,
  type EventSubscribeInput,
  type HarnessWebhookEvents,
  type WebhookStatus,
} from "../../host-protocol/index.js";
import { INBOX_ITEM_EVENT } from "../../host-protocol/names.js";
import {
  consumerConflict,
  type ConsumerOwner,
  currentConsumer,
} from "./consumer.js";
import {
  readWebhookChallenge,
  sendWebhook,
  validWebhookSecret,
  type WebhookCallbackError,
  type WebhookFailureReason,
  webhookUrl,
} from "./event-signing.js";
import { makeItemEvent } from "./inbox-event.js";

const maximumRetryDelay = 5 * 60_000;
const maximumLease = 24 * 60 * 60_000;
const verificationLifetime = 10 * 60_000;
const outboxSchema = Schema.Struct({
  deliveryToken: DeliveryToken,
  id: Schema.String,
  body: Schema.String,
  attempts: Schema.NonNegativeInt,
  firstAttemptAt: Schema.NonNegativeInt,
  nextAttemptAt: Schema.NonNegativeInt,
});
const registrationSchema = Schema.Struct({
  id: Schema.String,
  url: Schema.String,
  secret: Schema.String,
  expiresAt: Schema.NonNegativeInt,
  stalled: Schema.NullOr(Schema.Literal("callback", "terminal")),
  lastError: Schema.NullOr(Schema.Literal(...callbackReasons)),
  outbox: Schema.NullOr(outboxSchema),
});
const stateSchema = Schema.Struct({
  registration: Schema.NullOr(registrationSchema),
});
const challengeSchema = Schema.parseJson(
  Schema.Struct({ challenge: Schema.String }),
);
type Registration = typeof registrationSchema.Type;
type State = typeof stateSchema.Type;

const persistenceFailure = () =>
  new ProtocolError(-32603, "Event persistence failed");
const callbackFailure = (reason: WebhookFailureReason) =>
  new ProtocolError(-32015, "Callback verification failed", { reason });
const subscriptionId = (url: string) =>
  `sub_${createHash("sha256")
    .update(JSON.stringify([url, INBOX_ITEM_EVENT, {}]))
    .digest("hex")}`;

interface WebhookRuntime {
  readonly store: EventStore;
  readonly client: HttpClient.HttpClient;
  readonly owner: ConsumerOwner;
  readonly gate: Effect.Semaphore;
  readonly deliveryGate: Effect.Semaphore;
  verified?: {
    readonly id: string;
    readonly secretDigest: string;
    readonly until: number;
  };
  generation: number;
  state: State;
  /** The event id whose `webhook` marker is open, if any. */
  retrying?: string;
}
const now = (runtime: WebhookRuntime) =>
  runtime.owner.clock.unsafeCurrentTimeMillis();
const active = (runtime: WebhookRuntime) => {
  const current = currentConsumer(runtime.owner);
  return (
    current.mode === "webhook" && current.lease === runtime.state.registration
  );
};
/**
 * Make a persisted registration current, keeping the `webhook` marker in
 * step; every state change goes through here.
 * @param runtime The webhook state to update.
 * @param state The state just persisted.
 * @returns Completion once the state and its marker are current.
 */
function adopt(runtime: WebhookRuntime, state: State): Effect.Effect<void> {
  return Effect.sync(() => {
    runtime.state = state;
    if (
      state.registration !== null &&
      state.registration.expiresAt > now(runtime)
    ) {
      runtime.owner.current = { mode: "webhook", lease: state.registration };
    } else if (currentConsumer(runtime.owner).mode === "webhook") {
      runtime.owner.current = { mode: "none" };
    }
  }).pipe(Effect.zipRight(markWaitingDelivery(runtime)));
}

/**
 * Settle an open `webhook` marker as `interrupted` when the events close, so
 * the daemon that reopens it after a restart does not leave this one unpaired.
 * @param runtime The webhook state whose marker may be open.
 * @returns Completion once no marker is open.
 */
function settleOnClose(runtime: WebhookRuntime): Effect.Effect<void> {
  return Effect.suspend(() => {
    const marked = runtime.retrying;
    delete runtime.retrying;
    return marked === undefined
      ? Effect.void
      : retrySettled("webhook", marked, "interrupted");
  });
}

/**
 * The event id of a delivery waiting out its backoff after a failed callback,
 * which the daemon retries on its own.
 * @param runtime The webhook state and clock.
 * @returns The event id, or `undefined` when no delivery waits.
 */
function waitingDelivery(runtime: WebhookRuntime): string | undefined {
  const registration = runtime.state.registration;
  if (registration === null || registration.stalled !== null) {
    return undefined;
  }
  const occurrence = registration.outbox;
  return active(runtime) && occurrence !== null && occurrence.attempts > 0
    ? occurrence.id
    : undefined;
}

/**
 * How the delivery whose marker is open stopped waiting.
 * @param registration The current subscription, or `null` once revoked.
 * @param marked The event id the open marker carries.
 * @returns `interrupted` for a revoked subscription, `gave-up` while the
 *     delivery is still the subscription's outbox, else `delivered`.
 */
function waitOutcome(
  registration: Registration | null,
  marked: string,
): "interrupted" | "gave-up" | "delivered" {
  if (registration === null) {
    return "interrupted";
  }
  return registration.outbox?.id === marked ? "gave-up" : "delivered";
}

/**
 * Keep the `webhook` marker in step with the delivery state. A delivery that
 * waits out its backoff is daemon work in flight: its next attempt can be
 * minutes away and the callback client records no spans. The marker settles
 * as `delivered` once the delivery leaves the outbox, `gave-up` when it
 * stalls or its subscription lapses, and `interrupted` when the subscription
 * is revoked. The bookkeeping and its spans change together, never split by
 * an interruption.
 * @param runtime The webhook state, clock and open marker.
 * @returns Completion once the marker matches the state.
 */
function markWaitingDelivery(runtime: WebhookRuntime): Effect.Effect<void> {
  return Effect.uninterruptible(
    Effect.suspend(() => {
      const marked = runtime.retrying;
      const waiting = waitingDelivery(runtime);
      if (marked === waiting) {
        return Effect.void;
      }
      runtime.retrying = waiting;
      return (
        marked === undefined
          ? Effect.void
          : retrySettled(
              "webhook",
              marked,
              waitOutcome(runtime.state.registration, marked),
            )
      ).pipe(
        Effect.zipRight(
          waiting === undefined
            ? Effect.void
            : retryPending("webhook", waiting),
        ),
      );
    }),
  );
}

const save = (runtime: WebhookRuntime, registration: Registration | null) =>
  encodeRuntimeValue({ registration }).pipe(
    Effect.flatMap(runtime.store.writeEventState),
    Effect.mapError(persistenceFailure),
    Effect.zipRight(adopt(runtime, { registration })),
    Effect.uninterruptible,
  );
const retire = (runtime: WebhookRuntime) =>
  save(runtime, null).pipe(
    Effect.tap(() =>
      Effect.sync(() => {
        runtime.generation += 1;
      }),
    ),
    Effect.uninterruptible,
  );
const post = (
  runtime: WebhookRuntime,
  input: Parameters<typeof sendWebhook>[0],
) =>
  sendWebhook(input).pipe(
    Effect.provideService(HttpClient.HttpClient, runtime.client),
  );

const verify = (
  runtime: WebhookRuntime,
  input: { readonly id: string; readonly url: string; readonly secret: string },
) =>
  Effect.gen(function* () {
    const secretDigest = createHash("sha256")
      .update(input.secret)
      .digest("hex");
    if (
      runtime.verified?.id === input.id &&
      runtime.verified.secretDigest === secretDigest &&
      runtime.verified.until > now(runtime)
    ) {
      return;
    }
    const challenge = randomUUID();
    const response = yield* readWebhookChallenge({
      ...input,
      subscriptionId: input.id,
      messageId: `msg_verification_${randomUUID()}`,
      body: JSON.stringify({ type: "verification", challenge }),
      timestamp: now(runtime),
    }).pipe(
      Effect.provideService(HttpClient.HttpClient, runtime.client),
      Effect.mapError((error) => callbackFailure(error.reason)),
    );
    const decoded = yield* Schema.decodeUnknown(challengeSchema)(response).pipe(
      Effect.mapError(() => callbackFailure("challenge_failed")),
    );
    const expected = Buffer.from(challenge);
    const received = Buffer.from(decoded.challenge);
    if (
      received.length !== expected.length ||
      !timingSafeEqual(received, expected)
    ) {
      return yield* Effect.fail(callbackFailure("challenge_failed"));
    }
    runtime.verified = {
      id: input.id,
      secretDigest,
      until: now(runtime) + verificationLifetime,
    };
  });

const resetAttempt = (
  registration: Registration,
  timestamp: number,
): Registration["outbox"] =>
  registration.outbox === null
    ? null
    : {
        ...registration.outbox,
        attempts: 0,
        firstAttemptAt: timestamp,
        nextAttemptAt: timestamp,
      };
const refresh = (
  previous: Registration,
  secret: string,
  expiresAt: number,
  timestamp: number,
): Registration => ({
  ...previous,
  secret,
  expiresAt,
  stalled: previous.stalled === "callback" ? null : previous.stalled,
  outbox:
    previous.stalled === "callback"
      ? resetAttempt(previous, timestamp)
      : previous.outbox,
});
const initialRegistration = (input: {
  readonly id: string;
  readonly url: string;
  readonly secret: string;
  readonly expiresAt: number;
}): Registration => ({
  ...input,
  stalled: null,
  lastError: null,
  outbox: null,
});

const checkRegistration = (runtime: WebhookRuntime, id: string) =>
  Effect.suspend(() => {
    const current = currentConsumer(runtime.owner);
    if (current.mode !== "none" && current.mode !== "webhook") {
      return Effect.fail(consumerConflict());
    }
    return runtime.state.registration !== null &&
      runtime.state.registration.id !== id
      ? Effect.fail(
          new ProtocolError(
            -32013,
            "Revoke the retained subscription before configuring another callback",
            { limit: "subscriptions", max: 1 },
          ),
        )
      : Effect.void;
  });

const activateRegistration = (
  runtime: WebhookRuntime,
  input: EventSubscribeInput,
  verification: {
    readonly generation: number;
    readonly authorize: Effect.Effect<void, ProtocolError>;
  },
) =>
  runtime.gate.withPermits(1)(
    Effect.gen(function* () {
      yield* verification.authorize;
      const { url, secret } = input.delivery;
      const id = subscriptionId(url);
      yield* checkRegistration(runtime, id);
      if (verification.generation !== runtime.generation) {
        return yield* Effect.fail(
          new ProtocolError(-32012, "Subscription changed during verification"),
        );
      }
      const previous = runtime.state.registration;
      const expiresAt =
        now(runtime) +
        Math.max(60_000, Math.min(input.ttlMs ?? maximumLease, maximumLease));
      yield* save(
        runtime,
        previous === null
          ? initialRegistration({ id, url, secret, expiresAt })
          : refresh(previous, secret, expiresAt, now(runtime)),
      );
      runtime.generation += 1;
      return {
        id,
        refreshBefore: new Date(expiresAt).toISOString(),
        cursor: null,
        truncated: false,
      } as const;
    }).pipe(Effect.uninterruptible),
  );

const subscribe = (
  runtime: WebhookRuntime,
  input: EventSubscribeInput,
  authorize: Effect.Effect<void, ProtocolError>,
) =>
  Effect.gen(function* () {
    const { url, secret } = input.delivery;
    if (webhookUrl(url) === undefined || !validWebhookSecret(secret)) {
      return yield* Effect.fail(
        new ProtocolError(-32602, "Invalid webhook URL or secret"),
      );
    }
    const id = subscriptionId(url);
    const generation = yield* runtime.gate.withPermits(1)(
      authorize.pipe(
        Effect.zipRight(checkRegistration(runtime, id)),
        Effect.map(() => runtime.generation),
      ),
    );
    yield* verify(runtime, { id, url, secret });
    return yield* activateRegistration(runtime, input, {
      generation,
      authorize,
    });
  });

const failedDelivery = (
  registration: Registration,
  occurrence: typeof outboxSchema.Type,
  timestamp: number,
  error: WebhookCallbackError,
): Registration => {
  const attempts = occurrence.attempts + 1;
  const suspended =
    attempts >= 100 && timestamp - occurrence.firstAttemptAt >= 60 * 60_000;
  const retryStalled = suspended ? "callback" : null;
  return {
    ...registration,
    lastError: error.reason,
    stalled: error.discardOccurrence === true ? "terminal" : retryStalled,
    outbox: {
      ...occurrence,
      attempts,
      nextAttemptAt:
        timestamp +
        Math.min(maximumRetryDelay, 1000 * 2 ** Math.min(attempts, 9)),
    },
  };
};

/** A successful receipt retires exactly the item bound to these persisted bytes. */
const commitReceipt = (
  runtime: WebhookRuntime,
  registration: Registration,
  occurrence: typeof outboxSchema.Type,
) =>
  Effect.gen(function* () {
    const next = { ...registration, outbox: null, lastError: null };
    const bytes = yield* encodeRuntimeValue({ registration: next });
    yield* runtime.store.completeWebhookDelivery(
      occurrence.deliveryToken,
      bytes,
    );
    yield* adopt(runtime, { registration: next });
    return true;
  }).pipe(Effect.mapError(persistenceFailure), Effect.uninterruptible);

/** Expiry, revocation and replacement fence responses from an earlier consumer. */
const completeDelivery = (
  runtime: WebhookRuntime,
  registration: Registration,
  occurrence: typeof outboxSchema.Type,
  error?: WebhookCallbackError,
) =>
  runtime.gate.withPermits(1)(
    Effect.suspend(() => {
      const current = runtime.state.registration;
      if (!active(runtime)) {
        return Effect.succeed(false);
      }
      if (
        current === null ||
        current.id !== registration.id ||
        current.outbox !== occurrence
      ) {
        return Effect.succeed(false);
      }
      return error === undefined
        ? commitReceipt(runtime, current, occurrence)
        : save(
            runtime,
            failedDelivery(current, occurrence, now(runtime), error),
          ).pipe(Effect.as(false));
    }),
  );

const deliver = (runtime: WebhookRuntime, registration: Registration) => {
  const occurrence = registration.outbox;
  if (
    occurrence === null ||
    occurrence.nextAttemptAt > now(runtime) ||
    registration.stalled !== null
  ) {
    return Effect.succeed(false);
  }
  return post(runtime, {
    url: registration.url,
    secret: registration.secret,
    subscriptionId: registration.id,
    messageId: occurrence.id,
    body: occurrence.body,
    timestamp: now(runtime),
  }).pipe(
    Effect.matchEffect({
      onSuccess: () => completeDelivery(runtime, registration, occurrence),
      onFailure: (error) =>
        completeDelivery(runtime, registration, occurrence, error),
    }),
  );
};

/** An acknowledged or retired inbox item no longer owns an outgoing occurrence. */
const removeRetiredOccurrence = (
  runtime: WebhookRuntime,
  registration: Registration,
) =>
  Effect.gen(function* () {
    if (registration.outbox === null) {
      return registration;
    }
    const retained = yield* runtime.store
      .readInboxItem(registration.outbox.deliveryToken)
      .pipe(Effect.mapError(persistenceFailure));
    if (retained !== undefined && !retained.acknowledged) {
      return registration;
    }
    const next = { ...registration, outbox: null };
    yield* save(runtime, next);
    return next;
  });

const prepareOccurrence = (runtime: WebhookRuntime) =>
  runtime.gate.withPermits(1)(
    Effect.gen(function* () {
      const current = runtime.state.registration;
      if (current === null || !active(runtime) || current.stalled !== null) {
        return null;
      }
      const registration = yield* removeRetiredOccurrence(runtime, current);
      if (registration.outbox !== null) {
        return registration;
      }
      const page = yield* runtime.store
        .readInbox({ limit: 1 })
        .pipe(Effect.mapError(persistenceFailure));
      const entry = page.items[0];
      if (entry === undefined) {
        return null;
      }
      const timestamp = now(runtime);
      const body = JSON.stringify(makeItemEvent(entry, timestamp));
      const next = {
        ...registration,
        outbox: {
          deliveryToken: entry.deliveryToken,
          id: entry.deliveryToken,
          body,
          attempts: 0,
          firstAttemptAt: timestamp,
          nextAttemptAt: timestamp,
        },
      };
      yield* save(runtime, next);
      return next;
    }),
  );

/**
 * Drain immediately after receipts; the timer is only a retry and expiry
 * wakeup, so each drain also settles the marker of a lapsed subscription.
 */
const observe = (runtime: WebhookRuntime) =>
  runtime.deliveryGate.withPermits(1)(
    Effect.gen(function* () {
      while (true) {
        const registration = yield* prepareOccurrence(runtime);
        if (registration === null || !(yield* deliver(runtime, registration))) {
          return;
        }
      }
    }).pipe(Effect.zipRight(markWaitingDelivery(runtime))),
  );

const status = (runtime: WebhookRuntime): WebhookStatus => {
  const registration = runtime.state.registration;
  if (registration === null) {
    return { mode: "none" };
  }
  return {
    mode: "webhook",
    id: registration.id,
    refreshBefore: new Date(registration.expiresAt).toISOString(),
    stalled: registration.stalled,
    lastError: registration.lastError,
  };
};
const resume = (runtime: WebhookRuntime) =>
  Effect.suspend(() => {
    const registration = runtime.state.registration;
    if (registration === null || registration.stalled === null) {
      return Effect.void;
    }
    if (registration.stalled === "terminal") {
      return Effect.fail(
        new ProtocolError(
          -32014,
          "Terminal callback rejection requires consumer reconfiguration",
        ),
      );
    }
    return save(runtime, {
      ...registration,
      stalled: null,
      outbox: resetAttempt(registration, now(runtime)),
    });
  });
const unsubscribe = (runtime: WebhookRuntime, url: string) =>
  Effect.suspend(() => {
    const registration = runtime.state.registration;
    if (registration === null || registration.id !== subscriptionId(url)) {
      return Effect.fail(
        new ProtocolError(-32011, "Subscription not found", {
          kind: "subscription",
        }),
      );
    }
    return retire(runtime);
  });

const webhookOperations = (runtime: WebhookRuntime): HarnessWebhookEvents => ({
  subscribe: (input, authorize) =>
    subscribe(runtime, input, authorize ?? Effect.void),
  unsubscribe: (input) =>
    runtime.gate.withPermits(1)(unsubscribe(runtime, input.delivery.url)),
  observe: () => observe(runtime),
  status: runtime.gate.withPermits(1)(
    runtime.store.readInboxSummary().pipe(
      Effect.map((summary) => {
        const result = status(runtime);
        return result.mode === "none"
          ? result
          : { ...result, pendingCount: summary.pendingCount };
      }),
      Effect.mapError(persistenceFailure),
    ),
  ),
  revoke: runtime.gate.withPermits(1)(retire(runtime)),
  resume: runtime.gate.withPermits(1)(resume(runtime)),
});

/**
 * Retain exact callback bytes across retries and retire inbox items on HTTP receipt.
 * State transitions share the push ownership gate; callback I/O runs outside it.
 * A delivery retained mid-backoff reopens its `webhook` marker in this process,
 * and closing the scope settles a marker still open.
 * @param store Daemon-owned registration and delivery persistence.
 * @param gate Serializes subscription state with native push ownership.
 * @param owner Shared push, poll and callback occupancy.
 * @returns Protocol operations using the current Effect HTTP client and clock.
 */
export const makeWebhookEvents = (
  store: EventStore,
  gate: Effect.Semaphore,
  owner: ConsumerOwner,
): Effect.Effect<
  HarnessWebhookEvents,
  ProtocolError,
  HttpClient.HttpClient | Scope.Scope
> =>
  Effect.gen(function* () {
    const client = (yield* HttpClient.HttpClient).pipe(
      HttpClient.withTracerDisabledWhen(() => true),
    );
    const retained = yield* store
      .readEventState()
      .pipe(Effect.mapError(persistenceFailure));
    const state =
      retained === undefined
        ? { registration: null }
        : yield* decodeRuntimeValue(stateSchema, retained).pipe(
            Effect.mapError(persistenceFailure),
          );
    const deliveryGate = yield* Effect.makeSemaphore(1);
    const runtime: WebhookRuntime = {
      store,
      client,
      owner,
      state,
      gate,
      deliveryGate,
      generation: 0,
    };
    yield* Effect.addFinalizer(() => settleOnClose(runtime));
    yield* adopt(runtime, state);
    return webhookOperations(runtime);
  }).pipe(Effect.withSpan("makeWebhookEvents"));
