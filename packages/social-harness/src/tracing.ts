/** @file Tracks background work between attempts, including idle waits. */

import { Effect, Exit } from "effect";

/** Background work included in activity traces. */
export type RetryKind =
  | "proposal"
  | "catch-up"
  | "reanchor"
  | "outbound"
  | "firewall-check"
  | "webhook";
/** How a tracked operation finished. */
export type RetryOutcome =
  | "certified"
  | "superseded"
  | "recovered"
  | "sent"
  | "checked"
  | "delivered"
  | "gave-up"
  | "interrupted"
  | "failed";

/** Uses the supplied outcome for success, otherwise marks failure or interruption. */
export function exitOutcome(
  exit: Exit.Exit<unknown, unknown>,
  succeeded: RetryOutcome,
): RetryOutcome {
  if (Exit.isSuccess(exit)) {
    return succeeded;
  }
  return Exit.isInterrupted(exit) ? "interrupted" : "failed";
}

/** Opens an activity that `retrySettled` closes under the same kind and ID. */
export function retryPending(kind: RetryKind, id: string): Effect.Effect<void> {
  return Effect.withSpan(Effect.void, "retry.pending", {
    captureStackTrace: false,
    attributes: { kind, id },
  });
}

/** Closes the matching `retryPending` activity. */
export function retrySettled(
  kind: RetryKind,
  id: string,
  outcome: RetryOutcome,
): Effect.Effect<void> {
  return Effect.withSpan(Effect.void, "retry.settled", {
    captureStackTrace: false,
    attributes: { kind, id, outcome },
  });
}

/** Pairs activity markers even when work fails or is cancelled. */
export function markRetries<A, E, R>(
  kind: RetryKind,
  id: string,
  outcome: (exit: Exit.Exit<A, E>) => RetryOutcome,
): (work: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R> {
  return (work) =>
    Effect.uninterruptibleMask((restore) =>
      retryPending(kind, id).pipe(
        Effect.zipRight(restore(work)),
        Effect.onExit((exit) => retrySettled(kind, id, outcome(exit))),
      ),
    );
}
