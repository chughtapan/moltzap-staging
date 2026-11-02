/** @file Deterministic clock for durable nonce admission tests. */
import { Clock, Effect } from "effect";
/** Nonce claims observe a fixed instant independently of wall time. */
export const clockAt = (milliseconds: number): Clock.Clock => {
  const live = Clock.make();
  return {
    [Clock.ClockTypeId]: Clock.ClockTypeId,
    unsafeCurrentTimeMillis: () => milliseconds,
    currentTimeMillis: Effect.succeed(milliseconds),
    unsafeCurrentTimeNanos: () => BigInt(milliseconds) * 1_000_000n,
    currentTimeNanos: Effect.succeed(BigInt(milliseconds) * 1_000_000n),
    sleep: (duration) => live.sleep(duration),
  };
};
