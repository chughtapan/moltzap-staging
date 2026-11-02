/** @file Marks work that remains in progress while a trace has no other activity. */

import { Effect, Exit } from "effect";

/** Categories observed by the daemon activity reader. */
export type RetryKind =
  | "proposal"
  | "catch-up"
  | "reanchor"
  | "outbound"
  | "firewall-check"
  | "webhook";
/** A settled marker records why the matching work ended. */
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

/** Domain-specific success is supplied by the work's owner. */
export function exitOutcome(
  exit: Exit.Exit<unknown, unknown>,
  succeeded: RetryOutcome,
): RetryOutcome {
  if (Exit.isSuccess(exit)) {
    return succeeded;
  }
  return Exit.isInterrupted(exit) ? "interrupted" : "failed";
}

/** Readers pair pending and settled counts per category and identity. */
export function retryPending(kind: RetryKind, id: string): Effect.Effect<void> {
  return Effect.withSpan(Effect.void, "retry.pending", {
    captureStackTrace: false,
    attributes: { kind, id },
  });
}

/** Every pending marker receives one settled marker, including on cancellation. */
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

/** Cancellation can interrupt the work after its opening marker, while retaining its closing marker. */
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
