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
 *
 * Each post is signed as MCP Events requires and goes through an HTTP client
 * that applies the callback policy: which addresses it may reach, how long it
 * may take and how much it may answer.
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
  Redacted,
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
  openCredential,
  sealCredential,
  withAgentTransaction,
} from "../../database/index.js";
import { ProtocolError } from "../../vendor/effect-mcp/McpSchema.js";
import { SocialHarness } from "../social-harness.js";
import {
  callbackReasons,
  type EventSubscribeInput,
  type HarnessWebhookEvents,
  INBOX_PENDING_EVENT,
  maximumEventBytes,
  type WebhookStatus,
} from "./contracts.js";

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
 * when invalid. Verification and delivery validate URLs before connecting.
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
    const client = yield* HttpClient.HttpClient;
    const response = yield* HttpClient.withScope(client)
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
 * Accepts transport delivery at 2xx headers and closes the response.
 * Response bodies cannot change that receipt.
 */
export const sendWebhook = Effect.fn("sendWebhook")((post: WebhookPost) =>
  boundedCallback(executeWebhook(post).pipe(Effect.asVoid)),
);

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
  /** The pending items the last wake-up counted. */
  announced: ReadonlySet<string>;
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
    runtime.announced = new Set();
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
  generation: number,
) =>
  runtime.gate.withPermits(1)(
    Effect.gen(function* () {
      const { url, secret } = input.delivery;
      const id = subscriptionId(url);
      yield* checkRegistration(runtime, id);
      if (generation !== runtime.generation) {
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

const subscribe = (runtime: WebhookRuntime, input: EventSubscribeInput) =>
  Effect.gen(function* () {
    const { url, secret } = input.delivery;
    if (webhookUrl(url) === undefined || !validWebhookSecret(secret)) {
      return yield* Effect.fail(
        new ProtocolError(-32602, "Invalid webhook URL or secret"),
      );
    }
    const id = subscriptionId(url);
    const generation = yield* runtime.gate.withPermits(1)(
      checkRegistration(runtime, id).pipe(Effect.map(() => runtime.generation)),
    );
    yield* verify(runtime, { id, url, secret });
    return yield* activateRegistration(runtime, input, generation);
  });

/** The delivery tokens of the items waiting in the inbox. */
const pendingInbox = (harness: SocialHarness["Type"]) =>
  Effect.gen(function* () {
    const tokens: string[] = [];
    let cursor: string | undefined;
    do {
      const page = yield* harness.listInbox(
        cursor === undefined ? {} : { cursor },
      );
      tokens.push(...page.items.map((item) => item.deliveryToken));
      cursor = page.nextCursor;
    } while (cursor !== undefined);
    return tokens;
  }).pipe(Effect.withSpan("pendingInbox"));

/** The event that tells a host how many inbox items are waiting. */
const inboxPendingEvent = (pendingCount: number, timestamp: number) =>
  ({
    eventId: `wake_${randomUUID()}`,
    name: INBOX_PENDING_EVENT,
    timestamp: new Date(timestamp).toISOString(),
    data: { pendingCount },
    cursor: null,
  }) as const;

/**
 * The wake-up to post now, if the inbox has an item the last wake-up did not
 * count. A change can be signalled while the inbox is being read, for an item
 * that read already returned; such a signal adds nothing to announce.
 */
const nextWake = (runtime: WebhookRuntime) =>
  Effect.gen(function* () {
    if (runtime.wake !== null || !runtime.unannounced) {
      return runtime.wake;
    }
    runtime.unannounced = false;
    const pending = yield* pendingInbox(runtime.harness).pipe(
      Effect.mapError(persistenceFailure),
    );
    const announced = runtime.announced;
    runtime.announced = new Set(pending);
    if (pending.every((token) => announced.has(token))) {
      return null;
    }
    const timestamp = now(runtime);
    const event = inboxPendingEvent(pending.length, timestamp);
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
  subscribe: (input) => subscribe(runtime, input),
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
export const makeWebhookEvents = Effect.fn("Mcp.makeWebhookEvents")(function* (
  agentKey: string,
  unlockKey: Redacted.Redacted<Uint8Array>,
) {
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
      : yield* openCredential(row.state, `webhook/${agentKey}`, unlockKey).pipe(
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
    announced: new Set(),
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
}, Effect.mapError(persistenceFailure));
