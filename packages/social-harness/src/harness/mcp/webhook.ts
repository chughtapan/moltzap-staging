/** @file Durable MCP webhook registration and classified item handoff. */

import { HttpClient } from "@effect/platform";
import { SqlClient } from "@effect/sql";
import { type Clock, Effect, Schema } from "effect";
import { createHash, randomUUID, timingSafeEqual } from "node:crypto";
import { retryPending, retrySettled } from "../../tracing.js";
import { ProtocolError } from "../../vendor/effect-mcp/McpSchema.js";
import { DeliveryToken, type InboxEntry } from "../inbox.js";
import { SocialHarness } from "../social-harness.js";
import {
  callbackReasons,
  type EventSubscribeInput,
  type HarnessWebhookEvents,
  INBOX_ITEM_EVENT,
  maximumEventBytes,
  type WebhookStatus,
} from "./contracts.js";
import {
  readWebhookChallenge,
  sendWebhook,
  validWebhookSecret,
  type WebhookCallbackError,
  type WebhookFailureReason,
  webhookUrl,
} from "./webhook-client.js";

const maximumRetryDelay = 5 * 60_000;
const maximumLease = 24 * 60 * 60_000;
const verificationLifetime = 10 * 60_000;
const outboxSchema = Schema.Struct({
  deliveryToken: DeliveryToken,
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
  readonly sql: SqlClient.SqlClient;
  readonly harness: SocialHarness["Type"];
  readonly client: HttpClient.HttpClient;
  readonly clock: Clock.Clock;
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
  runtime.clock.unsafeCurrentTimeMillis();
const active = (runtime: WebhookRuntime) =>
  runtime.state.registration !== null &&
  runtime.state.registration.expiresAt > now(runtime);
/** Updates in-memory registration and retry telemetry after persistence. */
function adopt(runtime: WebhookRuntime, state: State): Effect.Effect<void> {
  return Effect.sync(() => {
    runtime.state = state;
  }).pipe(Effect.zipRight(markWaitingDelivery(runtime)));
}

/** Settles retry telemetry as interrupted when this server closes. */
function settleOnClose(runtime: WebhookRuntime): Effect.Effect<void> {
  return Effect.suspend(() => {
    const marked = runtime.retrying;
    delete runtime.retrying;
    return marked === undefined
      ? Effect.void
      : retrySettled("webhook", marked, "interrupted");
  });
}

function waitingDelivery(runtime: WebhookRuntime): string | undefined {
  const registration = runtime.state.registration;
  if (registration === null || registration.stalled !== null) {
    return undefined;
  }
  const occurrence = registration.outbox;
  return active(runtime) && occurrence !== null && occurrence.attempts > 0
    ? occurrence.deliveryToken
    : undefined;
}

function waitOutcome(
  registration: Registration | null,
  marked: string,
): "interrupted" | "gave-up" | "delivered" {
  if (registration === null) {
    return "interrupted";
  }
  return registration.outbox?.deliveryToken === marked
    ? "gave-up"
    : "delivered";
}

/**
 * Keeps retry telemetry open during backoff. Leaving the outbox settles it as
 * delivered. Stalling or expiry settles it as gave-up. Revocation interrupts it.
 * State and telemetry change without an interruption between them.
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
  writeState(runtime, { registration }).pipe(
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
    yield* runtime.sql.withTransaction(
      runtime.harness
        .acknowledgeDelivery(occurrence.deliveryToken)
        .pipe(Effect.zipRight(writeState(runtime, { registration: next }))),
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
    messageId: occurrence.deliveryToken,
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

const pendingEntry = (runtime: WebhookRuntime, token: DeliveryToken) =>
  Effect.gen(function* () {
    let cursor: string | undefined;
    do {
      const page = yield* runtime.harness.listInbox(
        cursor === undefined ? {} : { cursor },
      );
      const entry = page.items.find((entry) => entry.deliveryToken === token);
      if (entry !== undefined) {
        return entry;
      }
      cursor = page.nextCursor;
    } while (cursor !== undefined);
    return undefined;
  }).pipe(Effect.mapError(persistenceFailure));

const removeRetiredOccurrence = (
  runtime: WebhookRuntime,
  registration: Registration,
) =>
  Effect.gen(function* () {
    if (
      registration.outbox === null ||
      (yield* pendingEntry(runtime, registration.outbox.deliveryToken)) !==
        undefined
    ) {
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
      const page = yield* runtime.harness
        .listInbox()
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
 * Receipts trigger an immediate drain. Timers wake retries and settle expired
 * subscriptions' retry telemetry.
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
  status: runtime.gate.withPermits(1)(Effect.sync(() => status(runtime))),
  revoke: runtime.gate.withPermits(1)(retire(runtime)),
  resume: runtime.gate.withPermits(1)(resume(runtime)),
});

function writeState(runtime: Pick<WebhookRuntime, "sql">, state: State) {
  return Schema.encode(Schema.parseJson(stateSchema))(state).pipe(
    Effect.flatMap(
      (bytes) =>
        runtime.sql`INSERT INTO mcp_webhook_state (singleton, state) VALUES (1, ${bytes}) ON CONFLICT(singleton) DO UPDATE SET state = excluded.state`,
    ),
    Effect.asVoid,
  );
}

/** Preserves exact callback bytes and joins successful receipts to the inbox acknowledgment. */
export const makeWebhookEvents = (gate: Effect.Semaphore) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const harness = yield* SocialHarness;
    const client = (yield* HttpClient.HttpClient).pipe(
      HttpClient.withTracerDisabledWhen(() => true),
    );
    yield* sql`CREATE TABLE IF NOT EXISTS mcp_webhook_state (singleton INTEGER PRIMARY KEY CHECK(singleton = 1), state TEXT NOT NULL)`;
    const rows =
      yield* sql`SELECT state FROM mcp_webhook_state WHERE singleton = 1`;
    const row = rows[0];
    const state =
      row === undefined
        ? { registration: null }
        : yield* Schema.decodeUnknown(
            Schema.Struct({ state: Schema.parseJson(stateSchema) }),
          )(row).pipe(Effect.map((value) => value.state));
    const runtime: WebhookRuntime = {
      sql,
      harness,
      client,
      clock: yield* Effect.clock,
      state,
      gate,
      deliveryGate: yield* Effect.makeSemaphore(1),
      generation: 0,
    };
    yield* Effect.addFinalizer(() => settleOnClose(runtime));
    yield* adopt(runtime, state);
    return webhookOperations(runtime);
  }).pipe(
    Effect.mapError(persistenceFailure),
    Effect.withSpan("Mcp.makeWebhookEvents"),
  );

/**
 * Keeps content and local guidance together. Oversized envelopes reference the
 * saved event without truncation. Event identity stays stable across retries.
 * `timestamp` is the occurrence's Unix time in milliseconds.
 */
function makeItemEvent(entry: InboxEntry, timestamp: number) {
  const { address, item, guidance } = entry;
  const event = {
    eventId: entry.deliveryToken,
    name: INBOX_ITEM_EVENT,
    timestamp: new Date(timestamp).toISOString(),
    cursor: null,
    data: {
      kind: "item",
      address,
      item,
      ...(guidance === undefined ? {} : { guidance }),
    },
  } as const;
  return Buffer.byteLength(JSON.stringify(event), "utf8") <= maximumEventBytes
    ? event
    : {
        ...event,
        data: {
          kind: "reference" as const,
          itemKind: item.kind,
          bytes: Buffer.byteLength(
            JSON.stringify({ address, item, guidance }),
            "utf8",
          ),
        },
      };
}
