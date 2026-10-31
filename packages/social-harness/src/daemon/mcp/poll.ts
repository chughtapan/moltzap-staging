/** @file Bounded draft Events polling and an ephemeral, fenced native consumer lease. */

import { ProtocolError } from "@modelcontextprotocol/server";
import { Effect, Encoding, Schema } from "effect";
import { randomUUID } from "node:crypto";
import type { EventStore, InboxPage } from "../inbox/store.js";
import {
  type ConsumerMetadata,
  decodeConsumerMetadata,
  type EventPollInput,
  type PollResult,
} from "../../host-protocol/index.js";
import {
  HARNESS_CONSUMER_META_KEY,
  INBOX_ITEM_EVENT,
} from "../../host-protocol/names.js";
import {
  consumerConflict,
  type ConsumerOwner,
  currentConsumer,
  type PollLease,
} from "./consumer.js";
import { makeItemEvent } from "./inbox-event.js";

const POLL_INTERVAL_MILLIS = 1000;
const POLL_LEASE_MILLIS = 60_000;
const MAXIMUM_PAGE_ITEMS = 50;
const MAXIMUM_CURSOR_CHARACTERS = 512;
const MAXIMUM_REPLAY_AGE_MILLIS = 24 * 60 * 60_000;
const sequence = Schema.NonNegativeInt.pipe(
  Schema.lessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
);
const cursorSchema = Schema.Struct({
  generation: Schema.UUID,
  after: sequence,
  through: Schema.optionalWith(sequence, { exact: true }),
  observedAt: sequence,
});
type Cursor = typeof cursorSchema.Type;

interface PollRuntime {
  readonly store: EventStore;
  readonly gate: Effect.Semaphore;
  readonly owner: ConsumerOwner;
}

/** Poll and native receipt share one gate with push/webhook ownership changes. */
export interface HarnessPollEvents {
  readonly poll: (
    input: EventPollInput,
    metadata: unknown,
    authorize: Effect.Effect<void, ProtocolError>,
  ) => Effect.Effect<PollResult, ProtocolError>;
  readonly nativeOperation: <A, E>(
    metadata: unknown,
    operation: Effect.Effect<A, E>,
  ) => Effect.Effect<A, E | ProtocolError>;
  readonly status: Effect.Effect<{
    readonly mode: "none" | "poll";
    readonly id?: string;
  }>;
  readonly revoke: Effect.Effect<void>;
}

const now = (runtime: PollRuntime) =>
  runtime.owner.clock.unsafeCurrentTimeMillis();
const active = (runtime: PollRuntime): PollLease | undefined => {
  const owner = currentConsumer(runtime.owner);
  return owner.mode === "poll" ? owner.lease : undefined;
};
/** A stale lifetime needs explicit reacquisition and unread recovery. */
const consumerLost = () =>
  new ProtocolError(
    -32012,
    "Native consumer is no longer active. Acquire a new polling lease and recover the unread inbox. Docs: https://github.com/social-harness/moltzap/blob/main/docs/reference/spec/host/receiving.md#polling-consumer",
    {
      reason: "consumer-revoked",
    },
  );
/** Consumer metadata has a closed grammar independent of bearer authentication. */
const invalidMetadata = () =>
  new ProtocolError(
    -32602,
    "Invalid native consumer metadata. Echo the polling generation in request metadata. Docs: https://github.com/social-harness/moltzap/blob/main/docs/reference/spec/host/receiving.md#polling-consumer",
  );
const persistenceFailure = () =>
  new ProtocolError(-32603, "Event persistence failed");
const readMetadata = (metadata: unknown) =>
  decodeConsumerMetadata(metadata).pipe(Effect.mapError(invalidMetadata));
const requireConsumer = (
  runtime: PollRuntime,
  metadata?: ConsumerMetadata,
): Effect.Effect<PollLease, ProtocolError> =>
  Effect.suspend(() => {
    const current = active(runtime);
    if (
      metadata === undefined ||
      current === undefined ||
      metadata.generation !== current.generation
    ) {
      return Effect.fail(consumerLost());
    }
    return Effect.succeed(current);
  });
const encodeCursor = (cursor: Cursor): string =>
  Encoding.encodeBase64Url(new TextEncoder().encode(JSON.stringify(cursor)));
const decodeCursor = (cursor: string) =>
  cursor.length > MAXIMUM_CURSOR_CHARACTERS
    ? Effect.succeed(undefined)
    : Effect.suspend(() => Encoding.decodeBase64Url(cursor)).pipe(
        Effect.flatMap((bytes) =>
          Schema.decodeUnknown(Schema.parseJson(cursorSchema))(
            new TextDecoder().decode(bytes),
          ),
        ),
        Effect.catchAll(() => Effect.succeed(undefined)),
      );
const result = (
  generation: string,
  fields: Omit<PollResult, "_meta" | "nextPollMs">,
): PollResult => ({
  ...fields,
  nextPollMs: POLL_INTERVAL_MILLIS,
  _meta: { [HARNESS_CONSUMER_META_KEY]: { generation } },
});
const fresh = (runtime: PollRuntime, current: PollLease, truncated: boolean) =>
  runtime.store.readInboxSummary().pipe(
    Effect.catchTag("EndpointStoreError", () =>
      Effect.fail(persistenceFailure()),
    ),
    Effect.map((summary) =>
      result(current.generation, {
        events: [],
        truncated,
        hasMore: false,
        cursor: encodeCursor({
          generation: current.generation,
          after: summary.newestSequence,
          observedAt: now(runtime),
        }),
      }),
    ),
  );

/** A generation must be acquired explicitly; a late request cannot recreate its revoked lease. */
const acquireConsumer = (
  runtime: PollRuntime,
  input: EventPollInput,
  authorize: Effect.Effect<void, ProtocolError>,
  metadata?: ConsumerMetadata,
) =>
  Effect.gen(function* () {
    if (metadata !== undefined) {
      const current = yield* requireConsumer(runtime, metadata);
      yield* authorize.pipe(
        Effect.mapError((error) =>
          error.code === -32013 ? consumerLost() : error,
        ),
      );
      return current;
    }
    yield* authorize;
    if (input.cursor != null) {
      return yield* Effect.fail(consumerLost());
    }
    if (currentConsumer(runtime.owner).mode !== "none") {
      return yield* Effect.fail(consumerConflict());
    }
    const current = {
      generation: randomUUID(),
      expiresAt: now(runtime) + POLL_LEASE_MILLIS,
    };
    runtime.owner.current = { mode: "poll", lease: current };
    return current;
  });

const cursorIsCurrent = (
  runtime: PollRuntime,
  current: PollLease,
  cursor: Cursor,
  maxAgeMs: number,
): boolean => {
  const ageIsCurrent =
    cursor.observedAt <= now(runtime) &&
    now(runtime) - cursor.observedAt <= maxAgeMs;
  return (
    cursor.generation === current.generation &&
    cursor.after <= (cursor.through ?? Number.MAX_SAFE_INTEGER) &&
    ageIsCurrent
  );
};

const pageCursor = (
  runtime: PollRuntime,
  current: PollLease,
  cursor: Cursor,
  page: InboxPage,
): string =>
  encodeCursor({
    generation: current.generation,
    after: page.nextAfter ?? page.through,
    ...(page.nextAfter === undefined ? {} : { through: page.through }),
    observedAt: page.nextAfter === undefined ? now(runtime) : cursor.observedAt,
  });
const replayResult = (
  runtime: PollRuntime,
  current: PollLease,
  cursor: Cursor,
  page: InboxPage,
) =>
  Effect.forEach(page.items, (entry) => makeItemEvent(entry, now(runtime)), {
    concurrency: 1,
  }).pipe(
    Effect.map((events) =>
      result(current.generation, {
        events,
        truncated: false,
        hasMore: page.nextAfter !== undefined,
        cursor: pageCursor(runtime, current, cursor, page),
      }),
    ),
  );

/** Cursor advancement never retires work. Gaps trigger unread recovery, not a protocol error. */
const replay = (
  runtime: PollRuntime,
  input: EventPollInput,
  current: PollLease,
  cursor: Cursor,
) =>
  Effect.gen(function* () {
    const summary = yield* runtime.store.readInboxSummary();
    if (
      cursor.after > summary.newestSequence ||
      (cursor.through ?? 0) > summary.newestSequence
    ) {
      return yield* fresh(runtime, current, true);
    }
    const page = yield* runtime.store.readInbox({
      after: cursor.after,
      through: cursor.through,
      limit: Math.min(
        input.maxEvents ?? MAXIMUM_PAGE_ITEMS,
        MAXIMUM_PAGE_ITEMS,
      ),
    });
    return yield* replayResult(runtime, current, cursor, page);
  }).pipe(
    Effect.catchTag("EndpointStoreError", () =>
      Effect.fail(persistenceFailure()),
    ),
  );

const position = (
  runtime: PollRuntime,
  input: EventPollInput,
  current: PollLease,
  binding?: ConsumerMetadata,
) =>
  Effect.gen(function* () {
    if (binding?.release === true) {
      runtime.owner.current = { mode: "none" };
      return result(current.generation, {
        events: [],
        truncated: false,
        hasMore: false,
        cursor: encodeCursor({
          generation: current.generation,
          after: 0,
          observedAt: now(runtime),
        }),
      });
    }
    if (input.cursor == null) {
      return yield* fresh(runtime, current, false);
    }
    const cursor = yield* decodeCursor(input.cursor);
    const maxAgeMs = Math.min(
      input.maxAgeMs ?? MAXIMUM_REPLAY_AGE_MILLIS,
      MAXIMUM_REPLAY_AGE_MILLIS,
    );
    return yield* cursor === undefined ||
    !cursorIsCurrent(runtime, current, cursor, maxAgeMs)
      ? fresh(runtime, current, true)
      : replay(runtime, input, current, cursor);
  });

const poll = (
  runtime: PollRuntime,
  input: EventPollInput,
  metadata: unknown,
  authorize: Effect.Effect<void, ProtocolError>,
) =>
  runtime.gate.withPermits(1)(
    Effect.gen(function* () {
      if (input.name !== INBOX_ITEM_EVENT) {
        return yield* Effect.fail(
          new ProtocolError(
            -32011,
            "Event not found. Use the event advertised by events/list. Docs: https://github.com/social-harness/moltzap/blob/main/docs/reference/spec/host/receiving.md#polling-consumer",
            { kind: "event" },
          ),
        );
      }
      const binding = yield* readMetadata(metadata);
      const current = yield* acquireConsumer(
        runtime,
        input,
        authorize,
        binding,
      );
      return yield* position(runtime, input, current, binding).pipe(
        Effect.tap(() =>
          Effect.sync(() => {
            current.expiresAt = now(runtime) + POLL_LEASE_MILLIS;
          }),
        ),
        Effect.onError(() =>
          Effect.sync(() => {
            if (binding === undefined && active(runtime) === current) {
              runtime.owner.current = { mode: "none" };
            }
          }),
        ),
      );
    }),
  );

/** Validation and receipt commit are serialized with revocation; unbound inbox inspection remain independent. */
const nativeOperation = <A, E>(
  runtime: PollRuntime,
  metadata: unknown,
  operation: Effect.Effect<A, E>,
) =>
  readMetadata(metadata).pipe(
    Effect.flatMap((binding) => {
      if (binding === undefined) {
        return operation;
      }
      if (binding?.release === true) {
        return Effect.fail(invalidMetadata());
      }
      return runtime.gate.withPermits(1)(
        requireConsumer(runtime, binding).pipe(Effect.zipRight(operation)),
      );
    }),
  );

/**
 * Keeps only an ephemeral lease. Inbox content and receipt tombstones stay in
 * the existing store; daemon restart invalidates every old generation.
 */
export const makeHarnessPollEvents = (
  store: EventStore,
  gate: Effect.Semaphore,
  owner: ConsumerOwner,
): Effect.Effect<HarnessPollEvents> =>
  Effect.sync(() => {
    const runtime: PollRuntime = { store, gate, owner };
    const events: HarnessPollEvents = {
      poll: (input, metadata, authorize) =>
        poll(runtime, input, metadata, authorize),
      nativeOperation: (metadata, operation) =>
        nativeOperation(runtime, metadata, operation),
      status: Effect.sync(() => {
        const current = active(runtime);
        return current === undefined
          ? { mode: "none" as const }
          : { mode: "poll" as const, id: current.generation };
      }),
      revoke: gate.withPermits(1)(
        Effect.sync(() => {
          if (currentConsumer(owner).mode === "poll") {
            owner.current = { mode: "none" };
          }
        }),
      ),
    };
    return events;
  }).pipe(Effect.withSpan("makeHarnessPollEvents"));
