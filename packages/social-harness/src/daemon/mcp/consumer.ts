/** @file One exclusive owner for push subscriptions, polling leases and callbacks. */

import {
  ProtocolError,
  type ServerContext,
} from "@modelcontextprotocol/server";
import { type Clock, type Deferred, Effect } from "effect";

/** Push transport resources stay attached until request finalizers finish. */
export interface PushSubscription {
  readonly context: ServerContext;
  readonly stop: Deferred.Deferred<undefined>;
  readonly closed: Deferred.Deferred<undefined>;
  ready: boolean;
  previousSequence: number;
}

/** Native requests retain this generation until release, expiry or revocation. */
export interface PollLease {
  readonly generation: string;
  expiresAt: number;
}

type Consumer =
  | { readonly mode: "none" }
  | { readonly mode: "push"; readonly push: PushSubscription }
  | { readonly mode: "poll"; readonly lease: PollLease }
  | {
      readonly mode: "webhook";
      readonly lease: { readonly expiresAt: number };
    };

/** Every transport consults this state before claiming the shared consumer. */
export interface ConsumerOwner {
  readonly clock: Clock.Clock;
  current: Consumer;
}

/** Each handler owns one state and the clock that determines lease expiry. */
export const makeConsumerOwner = Effect.map(
  Effect.clock,
  (clock): ConsumerOwner => ({
    clock,
    current: { mode: "none" },
  }),
);

/** A fresh receiver must wait for the current consumer to release its lease. */
export const consumerConflict = () =>
  new ProtocolError(
    -32013,
    "Runtime subscription already active. Release it or wait for its lease to expire. Docs: https://github.com/social-harness/moltzap/blob/main/docs/reference/spec/host/receiving.md#polling-consumer",
    {
      limit: "subscriptions",
      max: 1,
      reason: "already-listening",
    },
  );

/** Expiry releases occupancy while retained callback progress remains durable. */
export function currentConsumer(owner: ConsumerOwner): Consumer {
  const current = owner.current;
  if (
    (current.mode === "poll" || current.mode === "webhook") &&
    current.lease.expiresAt <= owner.clock.unsafeCurrentTimeMillis()
  ) {
    owner.current = { mode: "none" };
  }
  return owner.current;
}
