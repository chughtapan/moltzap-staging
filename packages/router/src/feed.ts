/** @file Volatile globally ordered Router feed with bounded retention and retry identity. */
import type {
  AgentId,
  EnvelopeId,
  LiveSignedMessage,
} from "@moltzap/social-harness";
import type { routerContract } from "@moltzap/social-harness/internal";
import { Data, Effect, Ref } from "effect";

/** Greatest private feed order representable by this Router process. */
export const maximumPrivateOrder = (1n << 128n) - 1n;

/** Stable view used for one poll scan. */
export interface FeedSnapshot {
  readonly entries: readonly FeedEntry[];
  readonly tailOrder: bigint;
  readonly greatestEvictedOrder: bigint;
}

/** Sole capability owning volatile order, retention, and retry identity. */
export interface RouterFeed {
  readonly accept: (input: FeedCandidate) => Effect.Effect<FeedAcceptResult>;
  readonly snapshot: Effect.Effect<FeedSnapshot>;
  readonly freshAppendReady: Effect.Effect<boolean>;
}

interface RouterFeedInput {
  readonly routerInstanceId: typeof routerContract.RouterInstanceId.Type;
  readonly retainedMessageCapacity: number;
  readonly retainedMessageByteCapacity: number;
}

/** An initial private order is outside the closed unsigned range. */
export class FeedOrderOutOfRangeError extends Data.TaggedError(
  "FeedOrderOutOfRangeError",
) {}

/**
 * Creates the sole owner of Router private order, retention, and retry state.
 *
 * @param input Process identity and finite retention bounds.
 * @param input.routerInstanceId Current volatile process identity.
 * @param input.retainedMessageCapacity Maximum retained message count.
 * @param input.retainedMessageByteCapacity Maximum retained message bytes.
 * @returns A scoped state capability for send and poll.
 */
export const makeRouterFeed = (
  input: RouterFeedInput,
): Effect.Effect<RouterFeed> => makeFeed(input, 0n);

/**
 * Creates a feed over an already-consumed private-order prefix.
 *
 * The prefix is treated as evicted history and must fit the unsigned
 * private-order range.
 *
 * @param input Process identity, finite retention bounds, and consumed order.
 * @returns A scoped state capability for send and poll.
 */
export const makeRouterFeedAtOrder = (
  input: RouterFeedInput & Readonly<{ initialTailOrder: bigint }>,
): Effect.Effect<RouterFeed, FeedOrderOutOfRangeError> => {
  if (
    input.initialTailOrder < 0n ||
    input.initialTailOrder > maximumPrivateOrder
  ) {
    return Effect.fail(new FeedOrderOutOfRangeError());
  }
  return makeFeed(input, input.initialTailOrder);
};

/**
 * Restricts one stable feed snapshot to entries after a private order.
 *
 * @param snapshot Stable feed state for one scan.
 * @param order Exclusive lower private-order bound.
 * @returns Entries with a greater private order.
 */
export const entriesAfter = (
  snapshot: FeedSnapshot,
  order: bigint,
): readonly FeedEntry[] =>
  snapshot.entries.filter((entry) => entry.order > order);

interface FeedEntry {
  readonly order: bigint;
  readonly signedMessage: LiveSignedMessage;
  readonly encodedMessageJcs: string;
  readonly encodedByteLength: number;
  readonly recipients: ReadonlySet<AgentId>;
  readonly senderAgentId: AgentId;
  readonly envelopeId: EnvelopeId;
  readonly signedMessageDigest: typeof routerContract.SignedMessageDigest.Type;
}

interface FeedState {
  readonly entries: readonly FeedEntry[];
  readonly retryIndex: ReadonlyMap<string, FeedEntry>;
  readonly retainedBytes: number;
  readonly tailOrder: bigint;
  readonly greatestEvictedOrder: bigint;
}

/** Result of one atomic feed acceptance attempt. */
type FeedAcceptResult =
  | Readonly<{
      kind: "result";
      result: typeof routerContract.RouterSendResult.Type;
      acceptedRecipients?: ReadonlySet<AgentId>;
    }>
  | Readonly<{ kind: "overloaded" }>;

interface FeedCandidate {
  readonly mode: "initial" | "retry";
  readonly signedMessage: LiveSignedMessage;
  readonly encodedMessageJcs: string;
  readonly encodedByteLength: number;
  readonly recipients: ReadonlySet<AgentId>;
  readonly senderAgentId: AgentId;
  readonly envelopeId: EnvelopeId;
  readonly signedMessageDigest: typeof routerContract.SignedMessageDigest.Type;
}

const retryKey = (senderAgentId: AgentId, envelopeId: EnvelopeId): string =>
  `${senderAgentId}\u0000${envelopeId}`;

const makeInitialState = (tailOrder: bigint): FeedState => ({
  entries: [],
  retryIndex: new Map(),
  retainedBytes: 0,
  tailOrder,
  greatestEvictedOrder: tailOrder,
});

const acceptedResult = (
  routerInstanceId: typeof routerContract.RouterInstanceId.Type,
  entry: FeedEntry,
): Extract<
  typeof routerContract.RouterSendResult.Type,
  { readonly kind: "accepted" }
> => ({
  kind: "accepted",
  routerInstanceId,
  signedMessageDigest: entry.signedMessageDigest,
});

const retryResult = (
  routerInstanceId: typeof routerContract.RouterInstanceId.Type,
  candidate: FeedCandidate,
  retryIndex: ReadonlyMap<string, FeedEntry>,
  key: string,
): FeedAcceptResult => {
  const retained = retryIndex.get(key);
  let result: typeof routerContract.RouterSendResult.Type;
  if (retained === undefined) {
    result = { kind: "retry_identity_unknown" };
  } else if (retained.encodedMessageJcs !== candidate.encodedMessageJcs) {
    result = { kind: "idempotency_conflict" };
  } else {
    result = acceptedResult(routerInstanceId, retained);
  }
  return { kind: "result", result };
};

const evictUntilWithinBounds = (input: {
  readonly entries: FeedEntry[];
  readonly retryIndex: Map<string, FeedEntry>;
  readonly initialBytes: number;
  readonly countCapacity: number;
  readonly byteCapacity: number;
}): Readonly<{
  retainedBytes: number;
  greatestEvictedOrder?: bigint;
}> => {
  let retainedBytes = input.initialBytes;
  let greatestEvictedOrder: bigint | undefined;
  while (
    input.entries.length > input.countCapacity ||
    retainedBytes > input.byteCapacity
  ) {
    const evicted = input.entries.shift();
    if (evicted === undefined) {
      break;
    }
    retainedBytes -= evicted.encodedByteLength;
    greatestEvictedOrder = evicted.order;
    const key = retryKey(evicted.senderAgentId, evicted.envelopeId);
    if (input.retryIndex.get(key) === evicted) {
      input.retryIndex.delete(key);
    }
  }
  return greatestEvictedOrder === undefined
    ? { retainedBytes }
    : { retainedBytes, greatestEvictedOrder };
};

const appendCandidate = (
  current: FeedState,
  candidate: FeedCandidate,
  key: string,
  input: {
    readonly routerInstanceId: typeof routerContract.RouterInstanceId.Type;
    readonly retainedMessageCapacity: number;
    readonly retainedMessageByteCapacity: number;
  },
): readonly [FeedAcceptResult, FeedState] => {
  const entry: FeedEntry = {
    ...candidate,
    order: current.tailOrder + 1n,
  };
  const entries = [...current.entries, entry];
  const retryIndex = new Map(current.retryIndex);
  retryIndex.set(key, entry);
  const eviction = evictUntilWithinBounds({
    entries,
    retryIndex,
    initialBytes: current.retainedBytes + candidate.encodedByteLength,
    countCapacity: input.retainedMessageCapacity,
    byteCapacity: input.retainedMessageByteCapacity,
  });
  const next: FeedState = {
    entries,
    retryIndex,
    retainedBytes: eviction.retainedBytes,
    tailOrder: entry.order,
    greatestEvictedOrder:
      eviction.greatestEvictedOrder ?? current.greatestEvictedOrder,
  };
  return [
    {
      kind: "result",
      result: acceptedResult(input.routerInstanceId, entry),
      acceptedRecipients: entry.recipients,
    },
    next,
  ];
};

const modifyFeed = (
  current: FeedState,
  candidate: FeedCandidate,
  input: {
    readonly routerInstanceId: typeof routerContract.RouterInstanceId.Type;
    readonly retainedMessageCapacity: number;
    readonly retainedMessageByteCapacity: number;
  },
): readonly [FeedAcceptResult, FeedState] => {
  const key = retryKey(candidate.senderAgentId, candidate.envelopeId);
  const retained = current.retryIndex.get(key);
  if (candidate.mode === "retry") {
    return [
      retryResult(input.routerInstanceId, candidate, current.retryIndex, key),
      current,
    ];
  }
  if (retained !== undefined) {
    return [
      {
        kind: "result",
        result: { kind: "idempotency_conflict" },
      },
      current,
    ];
  }
  if (current.tailOrder === maximumPrivateOrder) {
    return [{ kind: "overloaded" }, current];
  }
  return appendCandidate(current, candidate, key, input);
};

function makeFeed(
  input: RouterFeedInput,
  initialTailOrder: bigint,
): Effect.Effect<RouterFeed> {
  return Effect.gen(function* () {
    const state = yield* Ref.make<FeedState>(
      makeInitialState(initialTailOrder),
    );
    const service: RouterFeed = {
      accept: (candidate: FeedCandidate) =>
        Ref.modify(state, (current) => modifyFeed(current, candidate, input)),
      snapshot: Ref.get(state).pipe(
        Effect.map((current) => ({
          entries: current.entries,
          tailOrder: current.tailOrder,
          greatestEvictedOrder: current.greatestEvictedOrder,
        })),
      ),
      freshAppendReady: Ref.get(state).pipe(
        Effect.map((current) => current.tailOrder < maximumPrivateOrder),
      ),
    };
    return Object.freeze(service);
  }).pipe(Effect.withSpan("makeRouterFeed"));
}
