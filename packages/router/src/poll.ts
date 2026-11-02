/** @file Router polling, authenticated continuations and cancellable long-poll waits. */
import {
  type AgentId,
  AgentId as AgentIdSchema,
  type LiveSignedMessage,
} from "@moltzap/social-harness";
import { httpErrors, routerContract } from "@moltzap/social-harness/internal";
import canonicalize from "canonicalize";
import {
  Data,
  Deferred,
  Duration,
  Effect,
  Encoding,
  Option,
  Ref,
  Schema,
} from "effect";
import { compactDecrypt, CompactEncrypt } from "jose";
import { createSecretKey } from "node:crypto";
import {
  entriesAfter,
  type FeedSnapshot,
  maximumPrivateOrder,
  type RouterFeed,
} from "./feed.js";

/** Allows one waiting poll per agent within the process-wide capacity. */
export interface PollWaiters {
  readonly awaitSignal: (
    agentId: AgentId,
    addressedDataIsReady: Effect.Effect<boolean>,
  ) => Effect.Effect<void, HeldPollCapacityError>;
  readonly notify: (recipients: ReadonlySet<AgentId>) => Effect.Effect<void>;
  readonly activeCount: Effect.Effect<number>;
}

/** No poll-waiter slot is immediately available. */
class HeldPollCapacityError extends Data.TaggedError("HeldPollCapacityError") {}

const utf8Encoder = new TextEncoder();

/** Waiting, delivery and cancellation release the same reserved poll slot. */
// eslint-disable-next-line agent-code-guard/require-span-on-exported-effect -- Effect.fn names the operation's span; nested generators do not need another copy.
export const makePollWaiters = Effect.fn("makePollWaiters")(function* (
  capacity: number,
) {
  const waiters = yield* Ref.make<Waiters>(new Map());
  const service: PollWaiters = {
    awaitSignal: (agentId, addressedDataIsReady) =>
      Effect.gen(function* () {
        const deferred = yield* Deferred.make<undefined>();
        const registered = yield* Ref.modify(
          waiters,
          (current): readonly [boolean, Waiters] => {
            if (current.has(agentId) || current.size >= capacity) {
              return [false, current];
            }
            return [true, new Map(current).set(agentId, deferred)];
          },
        );
        if (!registered) {
          return yield* Effect.fail(new HeldPollCapacityError());
        }
        return yield* Effect.gen(function* () {
          if (!(yield* addressedDataIsReady)) {
            yield* Deferred.await(deferred);
          }
        }).pipe(
          Effect.ensuring(
            Ref.update(waiters, (current) => {
              if (current.get(agentId) !== deferred) {
                return current;
              }
              const next = new Map(current);
              next.delete(agentId);
              return next;
            }),
          ),
        );
      }),
    notify: (recipients) =>
      Ref.modify(waiters, (current): readonly [readonly Waiter[], Waiters] => {
        const next = new Map(current);
        const found: Waiter[] = [];
        for (const agentId of recipients) {
          const deferred = next.get(agentId);
          if (deferred !== undefined) {
            found.push(deferred);
            next.delete(agentId);
          }
        }
        return [found, next];
      }).pipe(
        Effect.flatMap((detached) =>
          Effect.forEach(
            detached,
            (deferred) => Deferred.succeed(deferred, undefined),
            { concurrency: 128, discard: true },
          ),
        ),
      ),
    activeCount: Ref.get(waiters).pipe(Effect.map((current) => current.size)),
  };
  return Object.freeze(service);
});

type Waiter = Deferred.Deferred<undefined>;
type Waiters = ReadonlyMap<AgentId, Waiter>;

/** Cursor construction failed before a result could be returned. */
class PollCursorEncryptionError extends Data.TaggedError(
  "PollCursorEncryptionError",
) {}

/** A cursor does not authenticate for this caller and process. */
class PollCursorInvalidError extends Data.TaggedError(
  "PollCursorInvalidError",
) {}

/** A continuation works only for its original caller and Router instance. */
export interface PollCursorCodec {
  readonly encrypt: (input: {
    readonly agentId: AgentId;
    readonly lastScannedOrder: bigint;
  }) => Effect.Effect<
    typeof routerContract.PollCursor.Type,
    PollCursorEncryptionError
  >;
  readonly decrypt: (
    cursor: typeof routerContract.PollCursor.Type,
    callerAgentId: AgentId,
  ) => Effect.Effect<bigint, PollCursorInvalidError>;
}

/**
 * Binds cursors to their caller and Router instance. Imports the key once
 * as a non-extractable AES-GCM key.
 */
export const makePollCursorCodec = (input: {
  readonly key: Uint8Array;
  readonly routerInstanceId: typeof routerContract.RouterInstanceId.Type;
}): PollCursorCodec => {
  const key = createSecretKey(new Uint8Array(input.key)).toCryptoKey(
    { name: "AES-GCM" },
    false,
    ["encrypt", "decrypt"],
  );
  const routerInstanceId = input.routerInstanceId;
  return Object.freeze({
    encrypt: ({
      agentId,
      lastScannedOrder,
    }: {
      readonly agentId: AgentId;
      readonly lastScannedOrder: bigint;
    }) => encodePollCursor(key, routerInstanceId, agentId, lastScannedOrder),
    decrypt: (
      cursor: typeof routerContract.PollCursor.Type,
      callerAgentId: AgentId,
    ) => decodePollCursor(key, routerInstanceId, cursor, callerAgentId),
  });
};

/** Returns a fresh random identity for one Router process. */
export const generateRouterInstanceId =
  (): typeof routerContract.RouterInstanceId.Type =>
    Schema.decodeUnknownSync(routerContract.RouterInstanceId)(
      `rti_${Encoding.encodeBase64Url(
        globalThis.crypto.getRandomValues(
          new Uint8Array(routerContract.routerInstanceIdByteLength),
        ),
      )}`,
    );

/** Returns a fresh random 256-bit key for one Router process. */
export const generatePollCursorKey = (): Uint8Array =>
  globalThis.crypto.getRandomValues(new Uint8Array(32));

const utf8Decoder = new TextDecoder("utf-8", {
  fatal: true,
  ignoreBOM: true,
});

const orderText = Schema.String.pipe(
  Schema.pattern(/^(?:0|[1-9]\d*)$/),
  Schema.filter((value) => BigInt(value) <= maximumPrivateOrder),
);

const cursorPlaintext = Schema.Struct({
  agentId: AgentIdSchema,
  routerInstanceId: routerContract.RouterInstanceId,
  lastScannedOrder: orderText,
}).annotations({
  parseOptions: {
    exact: true,
    onExcessProperty: "error",
  },
});

const canonicalPlaintext = (input: {
  readonly agentId: AgentId;
  readonly routerInstanceId: typeof routerContract.RouterInstanceId.Type;
  readonly lastScannedOrder: bigint;
}): string | undefined =>
  canonicalize({
    agentId: input.agentId,
    routerInstanceId: input.routerInstanceId,
    lastScannedOrder: input.lastScannedOrder.toString(10),
  });

const invalidCursor = () => new PollCursorInvalidError();

function encodePollCursor(
  key: CryptoKey,
  routerInstanceId: typeof routerContract.RouterInstanceId.Type,
  agentId: AgentId,
  lastScannedOrder: bigint,
): Effect.Effect<
  typeof routerContract.PollCursor.Type,
  PollCursorEncryptionError
> {
  return Effect.gen(function* () {
    if (lastScannedOrder < 0n || lastScannedOrder > maximumPrivateOrder) {
      return yield* Effect.fail(new PollCursorEncryptionError());
    }
    const plaintext = canonicalPlaintext({
      agentId,
      routerInstanceId,
      lastScannedOrder,
    });
    if (plaintext === undefined) {
      return yield* Effect.fail(new PollCursorEncryptionError());
    }
    const compact = yield* Effect.tryPromise({
      try: () =>
        new CompactEncrypt(utf8Encoder.encode(plaintext))
          .setProtectedHeader(routerContract.pollCursorProtectedHeader)
          .encrypt(key),
      catch: () => new PollCursorEncryptionError(),
    });
    return yield* Schema.decodeUnknown(routerContract.PollCursor)(
      `${routerContract.pollCursorPrefix}${compact}`,
    ).pipe(
      Effect.catchTag("ParseError", () =>
        Effect.fail(new PollCursorEncryptionError()),
      ),
    );
  });
}

const plaintextIsCurrent = (input: {
  readonly canonical: string;
  readonly text: string;
  readonly agentId: AgentId;
  readonly callerAgentId: AgentId;
  readonly routerInstanceId: typeof routerContract.RouterInstanceId.Type;
  readonly currentRouterInstanceId: typeof routerContract.RouterInstanceId.Type;
}): boolean => {
  if (input.canonical !== input.text) {
    return false;
  }
  return (
    input.agentId === input.callerAgentId &&
    input.routerInstanceId === input.currentRouterInstanceId
  );
};

type CursorPlaintext = typeof cursorPlaintext.Type;

const decodeCursorPlaintext = (
  text: string,
): Effect.Effect<CursorPlaintext, PollCursorInvalidError> =>
  Schema.decodeUnknown(Schema.parseJson(cursorPlaintext))(text, {
    exact: true,
    onExcessProperty: "error",
  }).pipe(Effect.catchTag("ParseError", () => Effect.fail(invalidCursor())));

function decodePollCursor(
  key: CryptoKey,
  routerInstanceId: typeof routerContract.RouterInstanceId.Type,
  cursor: typeof routerContract.PollCursor.Type,
  callerAgentId: AgentId,
): Effect.Effect<bigint, PollCursorInvalidError> {
  return Effect.gen(function* () {
    const compact = cursor.slice(routerContract.pollCursorPrefix.length);
    if (
      compact.split(".", 1)[0] !==
      routerContract.encodedPollCursorProtectedHeader
    ) {
      return yield* Effect.fail(invalidCursor());
    }
    const decrypted = yield* Effect.tryPromise({
      try: () => compactDecrypt(compact, key),
      catch: invalidCursor,
    });
    const text = yield* Effect.try({
      try: () => utf8Decoder.decode(decrypted.plaintext),
      catch: invalidCursor,
    });
    const value = yield* decodeCursorPlaintext(text);
    const canonical = canonicalize({
      agentId: value.agentId,
      routerInstanceId: value.routerInstanceId,
      lastScannedOrder: value.lastScannedOrder,
    });
    if (
      canonical === undefined ||
      !plaintextIsCurrent({
        canonical,
        text,
        agentId: value.agentId,
        callerAgentId,
        routerInstanceId: value.routerInstanceId,
        currentRouterInstanceId: routerInstanceId,
      })
    ) {
      return yield* Effect.fail(invalidCursor());
    }
    return BigInt(value.lastScannedOrder);
  });
}

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

/** Returns messages for the authenticated caller or a continuation failure. */
export type RouterPoll = (
  request: typeof routerContract.RouterPollRequest.Type,
  verifiedRequest: VerifiedCaller,
) => Effect.Effect<typeof routerContract.RouterPollResult.Type, PollFailure>;

/** Returns bounded pages, waiting at the feed's end until delivery or timeout. */
export const makeRouterPoll =
  (input: PollDependencies): RouterPoll =>
  (request, verifiedRequest) =>
    handlePoll(input, request, verifiedRequest);

const HOLD_DURATION = Duration.seconds(25);

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
