/**
 * @file Wakes a subscribed callback when the inbox has something to read.
 *
 * A host subscribes a callback URL through the MCP Events calls. While the
 * subscription is active and the inbox holds pending items, the daemon posts
 * a signed {@link INBOX_PENDING_EVENT} to that URL. The event carries only a
 * count; the host reads the items with the inbox tool, exactly as a host that
 * polls does.
 *
 * One wake-up is posted each time the inbox gains an item, and again after a
 * restart if items are still pending. A failed post is retried with growing
 * delays. After 100 failures over at least an hour, or a response that says
 * the callback is gone, the subscription stalls and nothing more is posted
 * until the host subscribes again.
 */

import { HttpClient } from "@effect/platform";
import { SqlClient } from "@effect/sql";
import {
  type Clock,
  Duration,
  Effect,
  Queue,
  Redacted,
  Schema,
  Stream,
} from "effect";
import { createHash, randomUUID, timingSafeEqual } from "node:crypto";
import { openCredential, sealCredential } from "../../database/secrets.js";
import { executeQuery, makeQueries } from "../../database/sql.js";
import { withAgentTransaction } from "../../database/transactions.js";
import { ProtocolError } from "../../vendor/effect-mcp/McpSchema.js";
import { SocialHarness } from "../social-harness.js";
import {
  callbackReasons,
  type EventSubscribeInput,
  type HarnessWebhookEvents,
  INBOX_PENDING_EVENT,
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
const registrationSchema = Schema.Struct({
  id: Schema.String,
  url: Schema.String,
  secret: Schema.String,
  expiresAt: Schema.NonNegativeInt,
  stalled: Schema.NullOr(Schema.Literal("callback", "terminal")),
  lastError: Schema.NullOr(Schema.Literal(...callbackReasons)),
});
const stateSchema = Schema.Struct({
  registration: Schema.NullOr(registrationSchema),
});
const challengeSchema = Schema.parseJson(
  Schema.Struct({ challenge: Schema.String }),
);
type Registration = typeof registrationSchema.Type;
type State = typeof stateSchema.Type;

/** One wake-up being posted: the same event on every attempt. */
interface Wake {
  readonly id: string;
  readonly body: string;
  readonly attempts: number;
  readonly firstAttemptAt: number;
  readonly nextAttemptAt: number;
}

const persistenceFailure = () =>
  new ProtocolError(-32603, "Event persistence failed");
const callbackFailure = (reason: WebhookFailureReason) =>
  new ProtocolError(-32015, "Callback verification failed", { reason });
const subscriptionId = (url: string) =>
  `sub_${createHash("sha256")
    .update(JSON.stringify([url, INBOX_PENDING_EVENT, {}]))
    .digest("hex")}`;

type WebhookTables = Record<
  "mcp_webhook_state",
  Record<"agent_key" | "state", string>
>;

interface WebhookRuntime {
  readonly agentKey: string;
  readonly unlockKey: Redacted.Redacted<Uint8Array>;
  readonly sql: SqlClient.SqlClient;
  readonly harness: SocialHarness["Type"];
  readonly client: HttpClient.HttpClient;
  readonly clock: Clock.Clock;
  /** Guards the registration. */
  readonly gate: Effect.Semaphore;
  /** Allows one post at a time. */
  readonly deliveryGate: Effect.Semaphore;
  verified?: {
    readonly id: string;
    readonly secretDigest: string;
    readonly until: number;
  };
  generation: number;
  state: State;
  /** The inbox may hold items the callback has not been told about. */
  unannounced: boolean;
  wake: Wake | null;
  /** Receives a value whenever there may be a wake-up to post. */
  readonly changed: Queue.Queue<undefined>;
}
const now = (runtime: WebhookRuntime) =>
  runtime.clock.unsafeCurrentTimeMillis();
const active = (runtime: WebhookRuntime) =>
  runtime.state.registration !== null &&
  runtime.state.registration.expiresAt > now(runtime);

const save = (runtime: WebhookRuntime, registration: Registration | null) =>
  writeState(runtime, { registration }).pipe(
    Effect.mapError(persistenceFailure),
    Effect.zipRight(
      Effect.sync(() => {
        runtime.state = { registration };
      }),
    ),
    Effect.uninterruptible,
  );

/** Notes that the inbox may have items the callback has not been told about. */
const announce = (runtime: WebhookRuntime) =>
  Effect.suspend(() => {
    runtime.unannounced = true;
    return Queue.offer(runtime.changed, undefined);
  });

/** Forgets the wake-up in flight, so the next one starts from the inbox as it is then. */
const restartWakes = (runtime: WebhookRuntime) =>
  Effect.suspend(() => {
    runtime.generation += 1;
    runtime.wake = null;
    return announce(runtime);
  });
const retire = (runtime: WebhookRuntime) =>
  save(runtime, null).pipe(
    Effect.zipRight(restartWakes(runtime)),
    Effect.uninterruptible,
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

const initialRegistration = (input: {
  readonly id: string;
  readonly url: string;
  readonly secret: string;
  readonly expiresAt: number;
}): Registration => ({ ...input, stalled: null, lastError: null });

const checkRegistration = (runtime: WebhookRuntime, id: string) =>
  Effect.suspend(() => {
    return runtime.state.registration !== null &&
      runtime.state.registration.id !== id
      ? Effect.fail(
          new ProtocolError(
            -32013,
            "Unsubscribe the current callback before subscribing another",
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
      const expiresAt =
        now(runtime) +
        Math.max(60_000, Math.min(input.ttlMs ?? maximumLease, maximumLease));
      yield* save(runtime, initialRegistration({ id, url, secret, expiresAt }));
      yield* restartWakes(runtime);
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

/** The number of items waiting in the inbox. */
export const countPendingInbox = (harness: SocialHarness["Type"]) =>
  Effect.gen(function* () {
    let total = 0;
    let cursor: string | undefined;
    do {
      const page = yield* harness.listInbox(
        cursor === undefined ? {} : { cursor },
      );
      total += page.items.length;
      cursor = page.nextCursor;
    } while (cursor !== undefined);
    return total;
  }).pipe(Effect.withSpan("countPendingInbox"));

/** The event that tells a host how many inbox items are waiting. */
export const inboxPendingEvent = (pendingCount: number, timestamp: number) =>
  ({
    eventId: `wake_${randomUUID()}`,
    name: INBOX_PENDING_EVENT,
    timestamp: new Date(timestamp).toISOString(),
    data: { pendingCount },
    cursor: null,
  }) as const;

/** The wake-up to post now, if the inbox has items the callback has not been told about. */
const nextWake = (runtime: WebhookRuntime) =>
  Effect.gen(function* () {
    if (runtime.wake !== null || !runtime.unannounced) {
      return runtime.wake;
    }
    runtime.unannounced = false;
    const pendingCount = yield* countPendingInbox(runtime.harness).pipe(
      Effect.mapError(persistenceFailure),
    );
    if (pendingCount === 0) {
      return null;
    }
    const timestamp = now(runtime);
    const event = inboxPendingEvent(pendingCount, timestamp);
    runtime.wake = {
      id: event.eventId,
      body: JSON.stringify(event),
      attempts: 0,
      firstAttemptAt: timestamp,
      nextAttemptAt: timestamp,
    };
    return runtime.wake;
  });

/** Whether a failed post ends the wake-ups: the callback is gone, or has failed for too long. */
const stallAfter = (
  error: WebhookCallbackError,
  exhausted: boolean,
): Registration["stalled"] => {
  if (error.terminal === true) {
    return "terminal";
  }
  return exhausted ? "callback" : null;
};

/** Records how a post ended, unless the subscription changed while it was in flight. */
const settle = (
  runtime: WebhookRuntime,
  posted: { readonly generation: number; readonly wake: Wake },
  error?: WebhookCallbackError,
) =>
  runtime.gate.withPermits(1)(
    Effect.suspend(() => {
      const registration = runtime.state.registration;
      if (registration === null || posted.generation !== runtime.generation) {
        return Effect.void;
      }
      if (error === undefined) {
        runtime.wake = null;
        return registration.lastError === null
          ? Effect.void
          : save(runtime, { ...registration, lastError: null });
      }
      const attempts = posted.wake.attempts + 1;
      const timestamp = now(runtime);
      const exhausted =
        attempts >= 100 &&
        timestamp - posted.wake.firstAttemptAt >= 60 * 60_000;
      const stalled = stallAfter(error, exhausted);
      runtime.wake =
        stalled === null
          ? {
              ...posted.wake,
              attempts,
              nextAttemptAt:
                timestamp +
                Math.min(maximumRetryDelay, 1000 * 2 ** Math.min(attempts, 9)),
            }
          : null;
      runtime.unannounced = runtime.unannounced || stalled !== null;
      return registration.lastError === error.reason && stalled === null
        ? Effect.void
        : save(runtime, { ...registration, lastError: error.reason, stalled });
    }),
  );

/** Posts the wake-up that is due, if there is one. */
const deliver = (runtime: WebhookRuntime) =>
  runtime.deliveryGate.withPermits(1)(
    Effect.gen(function* () {
      const due = yield* runtime.gate.withPermits(1)(
        Effect.gen(function* () {
          const registration = runtime.state.registration;
          if (
            registration === null ||
            !active(runtime) ||
            registration.stalled !== null
          ) {
            return null;
          }
          const wake = yield* nextWake(runtime);
          return wake === null || wake.nextAttemptAt > now(runtime)
            ? null
            : { registration, wake, generation: runtime.generation };
        }),
      );
      if (due === null) {
        return;
      }
      yield* sendWebhook({
        url: due.registration.url,
        secret: due.registration.secret,
        subscriptionId: due.registration.id,
        messageId: due.wake.id,
        body: due.wake.body,
        timestamp: now(runtime),
      }).pipe(
        Effect.provideService(HttpClient.HttpClient, runtime.client),
        Effect.matchEffect({
          onSuccess: () => settle(runtime, due),
          onFailure: (error) => settle(runtime, due, error),
        }),
      );
    }),
  );

/** Waits for a change, or for the time a failed wake-up is due again. */
const untilNextAttempt = (runtime: WebhookRuntime) =>
  Effect.suspend(() => {
    const changed = Queue.take(runtime.changed);
    return runtime.wake === null
      ? changed
      : Effect.raceFirst(
          changed,
          Effect.sleep(
            Duration.millis(
              Math.max(1000, runtime.wake.nextAttemptAt - now(runtime)),
            ),
          ),
        );
  });

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
  status: runtime.gate.withPermits(1)(Effect.sync(() => status(runtime))),
});

function writeState(
  runtime: Pick<WebhookRuntime, "sql" | "agentKey" | "unlockKey">,
  state: State,
) {
  return Effect.gen(function* () {
    const bytes = yield* Schema.encode(Schema.parseJson(stateSchema))(state);
    const sealed = yield* sealCredential(
      Redacted.make(bytes),
      `webhook/${runtime.agentKey}`,
      runtime.unlockKey,
    );
    const queries = yield* makeQueries<WebhookTables>();
    yield* withAgentTransaction(
      runtime.agentKey,
      executeQuery(
        queries
          .insertInto("mcp_webhook_state")
          .values({ agent_key: runtime.agentKey, state: sealed })
          .onConflict((conflict) =>
            conflict.column("agent_key").doUpdateSet({ state: sealed }),
          ),
      ),
    );
  }).pipe(Effect.provideService(SqlClient.SqlClient, runtime.sql));
}

/** Loads the agent's subscription and posts wake-ups for it until the scope closes. */
export const makeWebhookEvents = (
  agentKey: string,
  unlockKey: Redacted.Redacted<Uint8Array>,
) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const harness = yield* SocialHarness;
    const client = (yield* HttpClient.HttpClient).pipe(
      HttpClient.withTracerDisabledWhen(() => true),
    );
    const queries = yield* makeQueries<WebhookTables>();
    const rows = yield* withAgentTransaction(
      agentKey,
      executeQuery(
        queries
          .selectFrom("mcp_webhook_state")
          .select("state")
          .where("agent_key", "=", agentKey),
      ),
    );
    const row = rows[0];
    const state =
      row === undefined
        ? { registration: null }
        : yield* openCredential(
            row.state,
            `webhook/${agentKey}`,
            unlockKey,
          ).pipe(
            Effect.flatMap((value) =>
              Schema.decodeUnknown(Schema.parseJson(stateSchema))(
                Redacted.value(value),
              ),
            ),
          );
    const runtime: WebhookRuntime = {
      agentKey,
      unlockKey,
      sql,
      harness,
      client,
      clock: yield* Effect.clock,
      state,
      gate: yield* Effect.makeSemaphore(1),
      deliveryGate: yield* Effect.makeSemaphore(1),
      generation: 0,
      unannounced: true,
      wake: null,
      changed: yield* Queue.sliding<undefined>(1),
    };
    yield* harness.inboxChanged.pipe(
      Stream.runForEach(() => announce(runtime)),
      Effect.forkScoped,
    );
    yield* deliver(runtime).pipe(
      Effect.catchAll(() => Effect.logWarning("Webhook wake-up deferred")),
      Effect.zipRight(untilNextAttempt(runtime)),
      Effect.forever,
      Effect.forkScoped,
    );
    return webhookOperations(runtime);
  }).pipe(
    Effect.mapError(persistenceFailure),
    Effect.withSpan("Mcp.makeWebhookEvents"),
  );
