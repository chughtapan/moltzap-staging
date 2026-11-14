/**
 * @file Wakes a subscribed callback when the inbox has something to read.
 *
 * A host subscribes a callback URL through the MCP Events calls. While the
 * subscription is active and the inbox holds pending entries, the daemon
 * posts a signed {@link INBOX_PENDING_EVENT} to that URL. The event carries
 * only a count; the host reads the entries with the inbox tool, exactly as a
 * host that polls does.
 *
 * One wake-up is posted each time the inbox gains an entry, and again after a
 * restart if entries are still pending. A failed post is retried with growing
 * delays. After 100 failures over at least an hour, or a response that says
 * the callback is gone, the subscription stalls and nothing more is posted
 * until the host subscribes again.
 *
 * Each post is signed as MCP Events requires and is sent under the callback
 * policy: which addresses it may reach, how long it may take and how much it
 * may answer.
 */

import {
  HttpClient,
  HttpClientRequest,
  type HttpClientResponse,
  HttpIncomingMessage,
} from "@effect/platform";
import { NodeHttpClient } from "@effect/platform-node";
import { SqlClient } from "@effect/sql";
import {
  type Clock,
  Data,
  Duration,
  Effect,
  Layer,
  Option,
  Queue,
  Schema,
  type Scope,
  Stream,
} from "effect";
import {
  createHash,
  createHmac,
  randomUUID,
  timingSafeEqual,
} from "node:crypto";
import { lookup } from "node:dns";
import { BlockList, isIP, type LookupFunction } from "node:net";
import {
  executeQuery,
  makeQueries,
  withAgentTransaction,
} from "../database/index.js";
import { ProtocolError } from "../vendor/effect-mcp/McpSchema.js";
import {
  callbackReasons,
  type EventSubscribeRequest,
  type HarnessWebhookEvents,
  INBOX_PENDING_EVENT,
  maximumEventBytes,
  type SubscriptionStatus,
} from "./contracts.js";
import { SocialHarness } from "./social-harness.js";

/** Closed callback diagnostics, never an upstream body, header or network address. */
export type WebhookFailureReason =
  | "connection_refused"
  | "timeout"
  | "tls_error"
  | "http_4xx"
  | "http_5xx"
  | "challenge_failed";

/** Expected callback failure safe to report in the Events protocol. */
export class WebhookCallbackError extends Data.TaggedError(
  "WebhookCallbackError",
)<{
  readonly reason: WebhookFailureReason;
  readonly terminal?: boolean;
}> {}

/** One immutable event or verification body, signed separately on each attempt. */
export interface WebhookPost {
  readonly url: string;
  readonly secret: string;
  readonly messageId: string;
  readonly subscriptionId: string;
  readonly body: string;
  readonly timestamp: number;
}

/* eslint-disable sonarjs/no-hardcoded-ip -- These are excluded address ranges for callback SSRF defense, never configured endpoints. */
const blockedV4 = new BlockList();
for (const [address, prefix] of [
  ["0.0.0.0", 8],
  ["10.0.0.0", 8],
  ["100.64.0.0", 10],
  ["127.0.0.0", 8],
  ["169.254.0.0", 16],
  ["172.16.0.0", 12],
  ["192.0.0.0", 24],
  ["192.0.2.0", 24],
  ["192.88.99.0", 24],
  ["192.168.0.0", 16],
  ["198.18.0.0", 15],
  ["198.51.100.0", 24],
  ["203.0.113.0", 24],
  ["224.0.0.0", 4],
  ["240.0.0.0", 4],
] satisfies ReadonlyArray<readonly [string, number]>) {
  blockedV4.addSubnet(address, prefix, "ipv4");
}
const globalV6 = new BlockList();
globalV6.addSubnet("2000::", 3, "ipv6");
const blockedV6 = new BlockList();
blockedV6.addSubnet("2001::", 23, "ipv6");
blockedV6.addSubnet("2001:db8::", 32, "ipv6");
blockedV6.addSubnet("2002::", 16, "ipv6");
blockedV6.addSubnet("3fff::", 20, "ipv6");
/* eslint-enable sonarjs/no-hardcoded-ip -- Restore endpoint address checks outside the exclusion table. */

/**
 * Accepts a resolved IP address only if it is public. Excludes local, mapped,
 * transition, reserved and documentation ranges.
 */
export const isPublicWebhookAddress = (address: string): boolean => {
  const family = isIP(address);
  return family === 4
    ? !blockedV4.check(address, "ipv4")
    : family === 6 &&
        globalV6.check(address, "ipv6") &&
        !blockedV6.check(address, "ipv6");
};

/**
 * Returns a canonical HTTPS URL without userinfo or fragments, or `undefined`
 * when invalid. Verification and wake-ups validate URLs before connecting.
 */
const webhookUrl = (value: string): URL | undefined => {
  if (!URL.canParse(value)) {
    return undefined;
  }
  const url = new URL(value);
  if (url.protocol !== "https:") {
    return undefined;
  }
  return url.username === "" && url.password === "" && url.hash === ""
    ? url
    : undefined;
};

/** Requires `whsec_` followed by canonical base64 encoding of 24–64 bytes. */
const validWebhookSecret = (secret: string): boolean => {
  if (!secret.startsWith("whsec_")) {
    return false;
  }
  const encoded = secret.slice("whsec_".length);
  const bytes = Buffer.from(encoded, "base64");
  return (
    bytes.length >= 24 &&
    bytes.length <= 64 &&
    bytes.toString("base64") === encoded
  );
};

/** Signs retained bytes with a fresh timestamp and includes subscription routing. */
const webhookHeaders = (
  post: WebhookPost,
): Readonly<Record<string, string>> => {
  const timestamp = String(Math.floor(post.timestamp / 1000));
  const signature = createHmac(
    "sha256",
    Buffer.from(post.secret.slice("whsec_".length), "base64"),
  )
    .update(`${post.messageId}.${timestamp}.${post.body}`)
    .digest("base64");
  return {
    "content-type": "application/json",
    "webhook-id": post.messageId,
    "webhook-timestamp": timestamp,
    "webhook-signature": `v1,${signature}`,
    "x-mcp-subscription-id": post.subscriptionId,
  };
};

const callbackFailure = (reason: WebhookFailureReason) =>
  new WebhookCallbackError({ reason });

/** Node's connection-time resolver validates every address before selecting one. */
const publicLookup: LookupFunction = (hostname, options, callback) => {
  lookup(hostname, { all: true, verbatim: true }, (error, addresses) => {
    if (error !== null) {
      callback(new Error("Callback address unavailable"), "", 4);
      return;
    }
    const first = addresses[0];
    if (
      first === undefined ||
      !addresses.every(({ address }) => isPublicWebhookAddress(address))
    ) {
      callback(new Error("Callback address unavailable"), "", 4);
    } else if (options.all === true) {
      callback(null, addresses);
    } else {
      callback(null, first.address, first.family);
    }
  });
};

/**
 * Validates DNS addresses at connection time while preserving Host and TLS names.
 * Each attempt opens a fresh connection. Redirects are disabled. The scope owns
 * sockets and response decoding.
 */
export const webhookHttpClientLayer = NodeHttpClient.layerWithoutAgent.pipe(
  Layer.provide(
    NodeHttpClient.makeAgentLayer({ keepAlive: false, lookup: publicLookup }),
  ),
);

const validatePost = (post: WebhookPost) =>
  Effect.gen(function* () {
    const url = webhookUrl(post.url);
    if (
      url === undefined ||
      !validWebhookSecret(post.secret) ||
      Buffer.byteLength(post.body, "utf8") > maximumEventBytes
    ) {
      return yield* Effect.fail(callbackFailure("connection_refused"));
    }
    const host = url.hostname.replace(/^\[|\]$/gu, "");
    if (isIP(host) !== 0 && !isPublicWebhookAddress(host)) {
      return yield* Effect.fail(callbackFailure("connection_refused"));
    }
    return url;
  });
const transportFailure = (cause: unknown) => {
  if (
    !(cause instanceof Error) ||
    !("code" in cause) ||
    typeof cause.code !== "string"
  ) {
    return callbackFailure("connection_refused");
  }
  return callbackFailure(
    cause.code.includes("TLS") || cause.code.includes("CERT")
      ? "tls_error"
      : "connection_refused",
  );
};

const executeWebhook = (
  post: WebhookPost,
): Effect.Effect<
  HttpClientResponse.HttpClientResponse,
  WebhookCallbackError,
  HttpClient.HttpClient | Scope.Scope
> =>
  Effect.gen(function* () {
    const url = yield* validatePost(post);
    const http = yield* HttpClient.HttpClient;
    const response = yield* HttpClient.withScope(http)
      .execute(
        HttpClientRequest.post(url).pipe(
          HttpClientRequest.bodyText(post.body, "application/json"),
          HttpClientRequest.setHeaders(webhookHeaders(post)),
        ),
      )
      .pipe(Effect.mapError((error) => transportFailure(error.cause)));
    if (response.status < 200 || response.status >= 300) {
      return yield* Effect.fail(
        new WebhookCallbackError({
          reason: response.status >= 500 ? "http_5xx" : "http_4xx",
          terminal: response.status === 410 || response.status === 413,
        }),
      );
    }
    return response;
  });
const boundedCallback = <A>(
  operation: Effect.Effect<
    A,
    WebhookCallbackError,
    HttpClient.HttpClient | Scope.Scope
  >,
) =>
  Effect.scoped(operation).pipe(
    Effect.timeoutFail({
      duration: "5 seconds",
      onTimeout: () => callbackFailure("timeout"),
    }),
  );

/** Reads bounded challenge-response text and closes the connection on exit. */
const readWebhookChallenge = Effect.fn("readWebhookChallenge")(
  (post: WebhookPost) =>
    boundedCallback(
      executeWebhook(post).pipe(
        Effect.flatMap((response) => response.text),
        HttpIncomingMessage.withMaxBodySize(Option.some(4096)),
        Effect.catchTag("ResponseError", () =>
          Effect.fail(callbackFailure("challenge_failed")),
        ),
      ),
    ),
);

/**
 * Counts a post as received at 2xx headers and closes the response.
 * Response bodies cannot change that receipt.
 */
export const sendWebhook = Effect.fn("sendWebhook")((post: WebhookPost) =>
  boundedCallback(executeWebhook(post).pipe(Effect.asVoid)),
);

const maximumRetryDelay = 5 * 60_000;
const maximumLease = 24 * 60 * 60_000;
const verificationLifetime = 10 * 60_000;
const subscriptionSchema = Schema.Struct({
  id: Schema.String,
  url: Schema.String,
  secret: Schema.String,
  expiresAt: Schema.NonNegativeInt,
  stalled: Schema.NullOr(Schema.Literal("callback", "terminal")),
  lastError: Schema.NullOr(Schema.Literal(...callbackReasons)),
});
/** What `mcp_webhook_state` holds for one agent. */
const savedSubscriptionSchema = Schema.Struct({
  subscription: Schema.NullOr(subscriptionSchema),
});
const challengeSchema = Schema.parseJson(
  Schema.Struct({ challenge: Schema.String }),
);
type Subscription = typeof subscriptionSchema.Type;
type SavedSubscription = typeof savedSubscriptionSchema.Type;

/** One wake-up being posted: the same event on every attempt. */
interface WakeUp {
  readonly id: string;
  readonly body: string;
  readonly attempts: number;
  readonly firstAttemptAt: number;
  readonly nextAttemptAt: number;
}

const persistenceFailure = () =>
  new ProtocolError(-32603, "Event persistence failed");
const verificationFailure = (reason: WebhookFailureReason) =>
  new ProtocolError(-32015, "Callback verification failed", { reason });
const subscriptionId = (url: string) =>
  `sub_${createHash("sha256")
    .update(JSON.stringify([url, INBOX_PENDING_EVENT, {}]))
    .digest("hex")}`;

type WebhookTables = Record<
  "mcp_webhook_state",
  Record<"agent_key" | "state", string>
>;

/** One agent's subscription and the wake-up being posted for it. */
interface AgentWebhook {
  readonly agentKey: string;
  readonly sql: SqlClient.SqlClient;
  readonly harness: SocialHarness["Type"];
  readonly http: HttpClient.HttpClient;
  readonly clock: Clock.Clock;
  /** Guards the subscription. */
  readonly subscriptionLock: Effect.Semaphore;
  /** Allows one post at a time. */
  readonly postLock: Effect.Semaphore;
  verified?: {
    readonly id: string;
    readonly secretDigest: string;
    readonly until: number;
  };
  generation: number;
  saved: SavedSubscription;
  /** The inbox may hold entries the callback has not been told about. */
  inboxChangedSinceLastWakeUp: boolean;
  /** The delivery tokens of the pending entries the last wake-up counted. */
  tokensInLastWakeUp: ReadonlySet<string>;
  wakeUp: WakeUp | null;
  /** Receives a value whenever there may be a wake-up to post. */
  readonly wakeUpSignal: Queue.Queue<undefined>;
}
const now = (webhook: AgentWebhook) => webhook.clock.unsafeCurrentTimeMillis();
const active = (webhook: AgentWebhook) =>
  webhook.saved.subscription !== null &&
  webhook.saved.subscription.expiresAt > now(webhook);

const save = (webhook: AgentWebhook, subscription: Subscription | null) =>
  writeSavedSubscription(webhook, { subscription }).pipe(
    Effect.mapError(persistenceFailure),
    Effect.zipRight(
      Effect.sync(() => {
        webhook.saved = { subscription };
      }),
    ),
    Effect.uninterruptible,
  );

/** Notes that the inbox may have entries the callback has not been told about. */
const noteInboxChanged = (webhook: AgentWebhook) =>
  Effect.suspend(() => {
    webhook.inboxChangedSinceLastWakeUp = true;
    return Queue.offer(webhook.wakeUpSignal, undefined);
  });

/** Forgets the wake-up in flight, so the next one starts from the inbox as it is then. */
const restartWakeUps = (webhook: AgentWebhook) =>
  Effect.suspend(() => {
    webhook.generation += 1;
    webhook.wakeUp = null;
    webhook.tokensInLastWakeUp = new Set();
    return noteInboxChanged(webhook);
  });
const clearSubscription = (webhook: AgentWebhook) =>
  save(webhook, null).pipe(
    Effect.zipRight(restartWakeUps(webhook)),
    Effect.uninterruptible,
  );

const verify = (
  webhook: AgentWebhook,
  callback: {
    readonly id: string;
    readonly url: string;
    readonly secret: string;
  },
) =>
  Effect.gen(function* () {
    const secretDigest = createHash("sha256")
      .update(callback.secret)
      .digest("hex");
    if (
      webhook.verified?.id === callback.id &&
      webhook.verified.secretDigest === secretDigest &&
      webhook.verified.until > now(webhook)
    ) {
      return;
    }
    const challenge = randomUUID();
    const response = yield* readWebhookChallenge({
      ...callback,
      subscriptionId: callback.id,
      messageId: `msg_verification_${randomUUID()}`,
      body: JSON.stringify({ type: "verification", challenge }),
      timestamp: now(webhook),
    }).pipe(
      Effect.provideService(HttpClient.HttpClient, webhook.http),
      Effect.mapError((error) => verificationFailure(error.reason)),
    );
    const decoded = yield* Schema.decodeUnknown(challengeSchema)(response).pipe(
      Effect.mapError(() => verificationFailure("challenge_failed")),
    );
    const expected = Buffer.from(challenge);
    const received = Buffer.from(decoded.challenge);
    if (
      received.length !== expected.length ||
      !timingSafeEqual(received, expected)
    ) {
      return yield* Effect.fail(verificationFailure("challenge_failed"));
    }
    webhook.verified = {
      id: callback.id,
      secretDigest,
      until: now(webhook) + verificationLifetime,
    };
  });

const refuseSecondSubscription = (webhook: AgentWebhook, id: string) =>
  Effect.suspend(() => {
    return webhook.saved.subscription !== null &&
      webhook.saved.subscription.id !== id
      ? Effect.fail(
          new ProtocolError(
            -32013,
            "Unsubscribe the current callback before subscribing another",
            { limit: "subscriptions", max: 1 },
          ),
        )
      : Effect.void;
  });

const saveVerifiedSubscription = (
  webhook: AgentWebhook,
  request: EventSubscribeRequest,
  generation: number,
) =>
  webhook.subscriptionLock.withPermits(1)(
    Effect.gen(function* () {
      const { url, secret } = request.delivery;
      const id = subscriptionId(url);
      yield* refuseSecondSubscription(webhook, id);
      if (generation !== webhook.generation) {
        return yield* Effect.fail(
          new ProtocolError(-32012, "Subscription changed during verification"),
        );
      }
      const expiresAt =
        now(webhook) +
        Math.max(60_000, Math.min(request.ttlMs ?? maximumLease, maximumLease));
      yield* save(webhook, {
        id,
        url,
        secret,
        expiresAt,
        stalled: null,
        lastError: null,
      });
      yield* restartWakeUps(webhook);
      return {
        id,
        refreshBefore: new Date(expiresAt).toISOString(),
        cursor: null,
        truncated: false,
      } as const;
    }).pipe(Effect.uninterruptible),
  );

const subscribe = (webhook: AgentWebhook, request: EventSubscribeRequest) =>
  Effect.gen(function* () {
    const { url, secret } = request.delivery;
    if (webhookUrl(url) === undefined || !validWebhookSecret(secret)) {
      return yield* Effect.fail(
        new ProtocolError(-32602, "Invalid callback URL or secret"),
      );
    }
    const id = subscriptionId(url);
    const generation = yield* webhook.subscriptionLock.withPermits(1)(
      refuseSecondSubscription(webhook, id).pipe(
        Effect.map(() => webhook.generation),
      ),
    );
    yield* verify(webhook, { id, url, secret });
    return yield* saveVerifiedSubscription(webhook, request, generation);
  });

/** The delivery tokens of the pending inbox entries. */
const pendingInbox = (harness: SocialHarness["Type"]) =>
  Effect.gen(function* () {
    const tokens: string[] = [];
    let cursor: string | undefined;
    do {
      const page = yield* harness.listInbox(
        cursor === undefined ? {} : { cursor },
      );
      tokens.push(...page.entries.map((entry) => entry.deliveryToken));
      cursor = page.nextCursor;
    } while (cursor !== undefined);
    return tokens;
  }).pipe(Effect.withSpan("pendingInbox"));

/** The event that tells a host how many inbox entries are pending. */
const inboxPendingEvent = (pendingCount: number, timestamp: number) =>
  ({
    eventId: `wake_${randomUUID()}`,
    name: INBOX_PENDING_EVENT,
    timestamp: new Date(timestamp).toISOString(),
    data: { pendingCount },
    cursor: null,
  }) as const;

/**
 * The wake-up to post now, if the inbox has an entry the last wake-up did not
 * count. A change can be signalled while the inbox is being read, for an entry
 * that read already returned; such a signal adds nothing to wake the host for.
 */
const prepareWakeUp = (webhook: AgentWebhook) =>
  Effect.gen(function* () {
    if (webhook.wakeUp !== null || !webhook.inboxChangedSinceLastWakeUp) {
      return webhook.wakeUp;
    }
    webhook.inboxChangedSinceLastWakeUp = false;
    const pending = yield* pendingInbox(webhook.harness).pipe(
      Effect.mapError(persistenceFailure),
    );
    const tokensInLastWakeUp = webhook.tokensInLastWakeUp;
    webhook.tokensInLastWakeUp = new Set(pending);
    if (pending.every((token) => tokensInLastWakeUp.has(token))) {
      return null;
    }
    const timestamp = now(webhook);
    const event = inboxPendingEvent(pending.length, timestamp);
    webhook.wakeUp = {
      id: event.eventId,
      body: JSON.stringify(event),
      attempts: 0,
      firstAttemptAt: timestamp,
      nextAttemptAt: timestamp,
    };
    return webhook.wakeUp;
  });

/** Whether a failed post ends the wake-ups: the callback is gone, or has failed for too long. */
const stallAfter = (
  error: WebhookCallbackError,
  exhausted: boolean,
): Subscription["stalled"] => {
  if (error.terminal === true) {
    return "terminal";
  }
  return exhausted ? "callback" : null;
};

/** Records how a post ended, unless the subscription changed while it was in flight. */
const settle = (
  webhook: AgentWebhook,
  posted: { readonly generation: number; readonly wakeUp: WakeUp },
  error?: WebhookCallbackError,
) =>
  webhook.subscriptionLock.withPermits(1)(
    Effect.suspend(() => {
      const subscription = webhook.saved.subscription;
      if (subscription === null || posted.generation !== webhook.generation) {
        return Effect.void;
      }
      if (error === undefined) {
        webhook.wakeUp = null;
        return subscription.lastError === null
          ? Effect.void
          : save(webhook, { ...subscription, lastError: null });
      }
      const attempts = posted.wakeUp.attempts + 1;
      const timestamp = now(webhook);
      const exhausted =
        attempts >= 100 &&
        timestamp - posted.wakeUp.firstAttemptAt >= 60 * 60_000;
      const stalled = stallAfter(error, exhausted);
      webhook.wakeUp =
        stalled === null
          ? {
              ...posted.wakeUp,
              attempts,
              nextAttemptAt:
                timestamp +
                Math.min(maximumRetryDelay, 1000 * 2 ** Math.min(attempts, 9)),
            }
          : null;
      webhook.inboxChangedSinceLastWakeUp =
        webhook.inboxChangedSinceLastWakeUp || stalled !== null;
      return subscription.lastError === error.reason && stalled === null
        ? Effect.void
        : save(webhook, { ...subscription, lastError: error.reason, stalled });
    }),
  );

/** Posts the wake-up that is due, if there is one. */
const postDueWakeUp = (webhook: AgentWebhook) =>
  webhook.postLock.withPermits(1)(
    Effect.gen(function* () {
      const due = yield* webhook.subscriptionLock.withPermits(1)(
        Effect.gen(function* () {
          const subscription = webhook.saved.subscription;
          if (
            subscription === null ||
            !active(webhook) ||
            subscription.stalled !== null
          ) {
            return null;
          }
          const wakeUp = yield* prepareWakeUp(webhook);
          return wakeUp === null || wakeUp.nextAttemptAt > now(webhook)
            ? null
            : { subscription, wakeUp, generation: webhook.generation };
        }),
      );
      if (due === null) {
        return;
      }
      yield* sendWebhook({
        url: due.subscription.url,
        secret: due.subscription.secret,
        subscriptionId: due.subscription.id,
        messageId: due.wakeUp.id,
        body: due.wakeUp.body,
        timestamp: now(webhook),
      }).pipe(
        Effect.provideService(HttpClient.HttpClient, webhook.http),
        Effect.matchEffect({
          onSuccess: () => settle(webhook, due),
          onFailure: (error) => settle(webhook, due, error),
        }),
      );
    }),
  );

/** Waits for a change, or for the time a failed wake-up is due again. */
const untilNextAttempt = (webhook: AgentWebhook) =>
  Effect.suspend(() => {
    const signalled = Queue.take(webhook.wakeUpSignal);
    return webhook.wakeUp === null
      ? signalled
      : Effect.raceFirst(
          signalled,
          Effect.sleep(
            Duration.millis(
              Math.max(1000, webhook.wakeUp.nextAttemptAt - now(webhook)),
            ),
          ),
        );
  });

const status = (webhook: AgentWebhook): SubscriptionStatus => {
  const subscription = webhook.saved.subscription;
  if (subscription === null) {
    return { mode: "none" };
  }
  return {
    mode: "webhook",
    id: subscription.id,
    refreshBefore: new Date(subscription.expiresAt).toISOString(),
    stalled: subscription.stalled,
    lastError: subscription.lastError,
  };
};
const unsubscribe = (webhook: AgentWebhook, url: string) =>
  Effect.suspend(() => {
    const subscription = webhook.saved.subscription;
    if (subscription === null || subscription.id !== subscriptionId(url)) {
      return Effect.fail(
        new ProtocolError(-32011, "Subscription not found", {
          kind: "subscription",
        }),
      );
    }
    return clearSubscription(webhook);
  });

const webhookOperations = (webhook: AgentWebhook): HarnessWebhookEvents => ({
  subscribe: (request) => subscribe(webhook, request),
  unsubscribe: (request) =>
    webhook.subscriptionLock.withPermits(1)(
      unsubscribe(webhook, request.delivery.url),
    ),
  status: webhook.subscriptionLock.withPermits(1)(
    Effect.sync(() => status(webhook)),
  ),
});

function writeSavedSubscription(
  webhook: Pick<AgentWebhook, "sql" | "agentKey">,
  saved: SavedSubscription,
) {
  return Effect.gen(function* () {
    const json = yield* Schema.encode(
      Schema.parseJson(savedSubscriptionSchema),
    )(saved);
    const queries = yield* makeQueries<WebhookTables>();
    yield* withAgentTransaction(
      webhook.agentKey,
      executeQuery(
        queries
          .insertInto("mcp_webhook_state")
          .values({ agent_key: webhook.agentKey, state: json })
          .onConflict((conflict) =>
            conflict.column("agent_key").doUpdateSet({ state: json }),
          ),
      ),
    );
  }).pipe(Effect.provideService(SqlClient.SqlClient, webhook.sql));
}

/** Loads the agent's subscription and posts wake-ups for it until the scope closes. */
export const makeWebhookEvents = Effect.fn("Mcp.makeWebhookEvents")(function* (
  agentKey: string,
) {
  const sql = yield* SqlClient.SqlClient;
  const harness = yield* SocialHarness;
  const http = (yield* HttpClient.HttpClient).pipe(
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
  const saved =
    row === undefined
      ? { subscription: null }
      : yield* Schema.decodeUnknown(Schema.parseJson(savedSubscriptionSchema))(
          row.state,
        );
  const webhook: AgentWebhook = {
    agentKey,
    sql,
    harness,
    http,
    clock: yield* Effect.clock,
    saved,
    subscriptionLock: yield* Effect.makeSemaphore(1),
    postLock: yield* Effect.makeSemaphore(1),
    generation: 0,
    inboxChangedSinceLastWakeUp: true,
    tokensInLastWakeUp: new Set(),
    wakeUp: null,
    wakeUpSignal: yield* Queue.sliding<undefined>(1),
  };
  yield* harness.inboxChanged.pipe(
    Stream.runForEach(() => noteInboxChanged(webhook)),
    Effect.forkScoped,
  );
  yield* postDueWakeUp(webhook).pipe(
    Effect.catchAll(() => Effect.logWarning("Webhook wake-up deferred")),
    Effect.zipRight(untilNextAttempt(webhook)),
    Effect.forever,
    Effect.forkScoped,
  );
  return webhookOperations(webhook);
}, Effect.mapError(persistenceFailure));
