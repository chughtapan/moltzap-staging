/** @file Verified polling over the volatile feed, cursor codec, and bounded waiters. */
import type { AgentId, LiveSignedMessage } from "@moltzap/social-harness";
import { httpErrors, routerContract } from "@moltzap/social-harness/internal";
import canonicalize from "canonicalize";
import { Duration, Effect, Option, Schema } from "effect";
import type { PollCursorCodec } from "./poll-cursor.js";
import type { PollWaiters } from "./poll-waiters.js";
import { entriesAfter, type FeedSnapshot, type RouterFeed } from "./feed.js";

/* eslint-disable @typescript-eslint/naming-convention -- Closed HTTP error constructors retain their class names when bound from the service capability. */
const { InternalServerError, OverloadedError } = httpErrors;
/* eslint-enable @typescript-eslint/naming-convention -- Restore variable naming after binding error constructors. */

type PollFailure =
  | InstanceType<typeof httpErrors.OverloadedError>
  | InstanceType<typeof httpErrors.InternalServerError>;

interface VerifiedCaller {
  readonly callerAgentId: AgentId;
}

interface PollDependencies {
  readonly routerInstanceId: typeof routerContract.RouterInstanceId.Type;
  readonly feed: RouterFeed;
  readonly pollWaiters: PollWaiters;
  readonly cursorCodec: PollCursorCodec;
  readonly pollMessageLimit: number;
  readonly pollResponseByteLimit: number;
}

/** Poll domain operation after HTTP authentication. */
export interface RouterPoll {
  readonly handle: (
    request: typeof routerContract.RouterPollRequest.Type,
    verifiedRequest: VerifiedCaller,
  ) => Effect.Effect<typeof routerContract.RouterPollResult.Type, PollFailure>;
}

/**
 * Builds endpoint-wide bounded polling over one volatile global feed.
 *
 * @param input Current process state and finite poll bounds.
 * @param input.routerInstanceId Current volatile process identity.
 * @param input.feed Global volatile feed.
 * @param input.pollWaiters Addressed poll-waiter notifications.
 * @param input.cursorCodec Caller-bound cursor codec.
 * @param input.pollMessageLimit Maximum messages in one result.
 * @param input.pollResponseByteLimit Maximum complete result bytes.
 * @returns The authenticated poll domain operation.
 */
export const makeRouterPoll = (input: PollDependencies): RouterPoll =>
  Object.freeze({
    handle: (
      request: typeof routerContract.RouterPollRequest.Type,
      verifiedRequest: VerifiedCaller,
    ) => handlePoll(input, request, verifiedRequest),
  });

const HOLD_DURATION = Duration.seconds(25);
const utf8Encoder = new TextEncoder();

type PollBatch = Extract<
  typeof routerContract.RouterPollResult.Type,
  { readonly kind: "batch" }
>;

interface ScanResult {
  readonly result: PollBatch;
  readonly reachedTail: boolean;
  readonly lastScannedOrder: bigint;
}

interface ScanState {
  readonly selected: readonly LiveSignedMessage[];
  readonly lastScannedOrder: bigint;
}

const encodedResultLength = (
  result: typeof routerContract.RouterPollResult.Type,
): Effect.Effect<number, InstanceType<typeof httpErrors.InternalServerError>> =>
  Schema.encode(routerContract.RouterPollResult)(result).pipe(
    Effect.catchTag("ParseError", () => Effect.fail(new InternalServerError())),
    Effect.flatMap((encoded) => {
      const jcs = canonicalize(encoded);
      return jcs === undefined
        ? Effect.fail(new InternalServerError())
        : Effect.succeed(utf8Encoder.encode(jcs).byteLength);
    }),
  );

const addressedDataIsReady = (
  feed: RouterFeed,
  callerAgentId: AgentId,
  order: bigint,
): Effect.Effect<boolean> =>
  feed.snapshot.pipe(
    Effect.map(
      (snapshot) =>
        order < snapshot.greatestEvictedOrder ||
        entriesAfter(snapshot, order).some((entry) =>
          entry.recipients.has(callerAgentId),
        ),
    ),
  );

const makeBatch = (
  input: PollDependencies,
  callerAgentId: AgentId,
  signedMessages: readonly LiveSignedMessage[],
  lastScannedOrder: bigint,
): Effect.Effect<
  PollBatch,
  InstanceType<typeof httpErrors.InternalServerError>
> =>
  input.cursorCodec.encrypt({ agentId: callerAgentId, lastScannedOrder }).pipe(
    Effect.catchTag("PollCursorEncryptionError", () =>
      Effect.fail(new InternalServerError()),
    ),
    Effect.map(
      (pollCursor): PollBatch => ({
        kind: "batch",
        routerInstanceId: input.routerInstanceId,
        signedMessages,
        pollCursor,
      }),
    ),
  );

const scanEntry = (
  input: PollDependencies,
  callerAgentId: AgentId,
  state: ScanState,
  entry: FeedSnapshot["entries"][number],
): Effect.Effect<
  Option.Option<ScanState>,
  InstanceType<typeof httpErrors.InternalServerError>
> => {
  if (!entry.recipients.has(callerAgentId)) {
    return Effect.succeed(
      Option.some({
        ...state,
        lastScannedOrder: entry.order,
      }),
    );
  }
  if (state.selected.length >= input.pollMessageLimit) {
    return Effect.succeed(Option.none());
  }
  const selected = [...state.selected, entry.signedMessage];
  return makeBatch(input, callerAgentId, selected, entry.order).pipe(
    Effect.flatMap(encodedResultLength),
    Effect.map((byteLength) =>
      byteLength <= input.pollResponseByteLimit
        ? Option.some({
            selected,
            lastScannedOrder: entry.order,
          })
        : Option.none(),
    ),
  );
};

const scan = (
  input: PollDependencies,
  snapshot: FeedSnapshot,
  callerAgentId: AgentId,
  startOrder: bigint,
): Effect.Effect<
  ScanResult,
  InstanceType<typeof httpErrors.InternalServerError>
> =>
  Effect.gen(function* () {
    let state: ScanState = {
      selected: [],
      lastScannedOrder: startOrder,
    };
    let reachedTail = true;
    for (const entry of entriesAfter(snapshot, startOrder)) {
      const next = yield* scanEntry(input, callerAgentId, state, entry);
      if (Option.isNone(next)) {
        reachedTail = false;
        break;
      }
      state = next.value;
    }
    return {
      result: yield* makeBatch(
        input,
        callerAgentId,
        state.selected,
        state.lastScannedOrder,
      ),
      reachedTail,
      lastScannedOrder: state.lastScannedOrder,
    };
  });

const feedGap = (
  routerInstanceId: typeof routerContract.RouterInstanceId.Type,
): typeof routerContract.RouterPollResult.Type => ({
  kind: "feed_gap",
  routerInstanceId,
});

const holdAtTail = (
  input: PollDependencies,
  callerAgentId: AgentId,
  startOrder: bigint,
): Effect.Effect<void, InstanceType<typeof httpErrors.OverloadedError>> =>
  input.pollWaiters
    .awaitSignal(
      callerAgentId,
      addressedDataIsReady(input.feed, callerAgentId, startOrder),
    )
    .pipe(
      Effect.raceFirst(Effect.sleep(HOLD_DURATION)),
      Effect.catchTag("HeldPollCapacityError", () =>
        Effect.fail(new OverloadedError()),
      ),
    );

const continueFromOrder = (
  input: PollDependencies,
  callerAgentId: AgentId,
  startOrder: bigint,
): Effect.Effect<typeof routerContract.RouterPollResult.Type, PollFailure> =>
  Effect.gen(function* () {
    const snapshot = yield* input.feed.snapshot;
    if (startOrder > snapshot.tailOrder) {
      return { kind: "cursor_invalid" };
    }
    if (startOrder < snapshot.greatestEvictedOrder) {
      return feedGap(input.routerInstanceId);
    }
    const first = yield* scan(input, snapshot, callerAgentId, startOrder);
    const hasImmediateResult =
      first.result.signedMessages.length > 0 || !first.reachedTail;
    if (hasImmediateResult) {
      return first.result;
    }
    yield* holdAtTail(input, callerAgentId, first.lastScannedOrder);
    const finalSnapshot = yield* input.feed.snapshot;
    if (first.lastScannedOrder < finalSnapshot.greatestEvictedOrder) {
      return feedGap(input.routerInstanceId);
    }
    return (yield* scan(
      input,
      finalSnapshot,
      callerAgentId,
      first.lastScannedOrder,
    )).result;
  });

const continuePoll = (
  input: PollDependencies,
  callerAgentId: AgentId,
  pollCursor: typeof routerContract.PollCursor.Type,
): Effect.Effect<typeof routerContract.RouterPollResult.Type, PollFailure> =>
  input.cursorCodec.decrypt(pollCursor, callerAgentId).pipe(
    Effect.option,
    Effect.flatMap(
      Option.match({
        onNone: () =>
          Effect.succeed<typeof routerContract.RouterPollResult.Type>({
            kind: "cursor_invalid",
          }),
        onSome: (order) => continueFromOrder(input, callerAgentId, order),
      }),
    ),
  );

function handlePoll(
  input: PollDependencies,
  request: typeof routerContract.RouterPollRequest.Type,
  verifiedRequest: VerifiedCaller,
): Effect.Effect<typeof routerContract.RouterPollResult.Type, PollFailure> {
  const callerAgentId = verifiedRequest.callerAgentId;
  if (request.pollCursor !== undefined) {
    return continuePoll(input, callerAgentId, request.pollCursor);
  }
  return input.feed.snapshot.pipe(
    Effect.flatMap((snapshot) =>
      makeBatch(input, callerAgentId, [], snapshot.tailOrder),
    ),
  );
}
