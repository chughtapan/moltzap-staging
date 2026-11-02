/** @file A scripted Router transmit over an endpoint store's durable outbox. */

import { type Duration, Effect, Queue } from "effect";
import { decodeCanonical } from "../encoding/canonical-json.js";
import { SignedMessage } from "../identity/envelope/signed-message.js";
import {
  protocolStore,
  type TestDatabase,
} from "./communication-test-database.js";

/**
 * Transmit one outbox row the way the Router worker does: begin it, complete
 * it, and hand its envelope to the Router. A row another transmit already
 * began is inactive and forwards nothing.
 * @param store Store that owns the outbox row.
 * @param outbound Receives each envelope the Router accepts.
 * @param envelopeId Outbox row to transmit.
 * @param transit Time the envelope spends between beginning and completing.
 * @returns Completion once the row is handled.
 */
export function forwardStoredOutbound(
  store: TestDatabase,
  outbound: Queue.Queue<SignedMessage>,
  envelopeId: string,
  transit?: Duration.DurationInput,
): Effect.Effect<void> {
  return store.run(
    protocolStore.beginOutbound(envelopeId).pipe(
      Effect.flatMap((attempt) => {
        switch (attempt.kind) {
          case "inactive":
            return Effect.void;
          case "pending":
            return decodeCanonical(
              SignedMessage,
              attempt.outbound.canonicalSignedMessage,
            ).pipe(
              Effect.tap(() =>
                transit === undefined ? Effect.void : Effect.sleep(transit),
              ),
              Effect.flatMap((message) =>
                protocolStore
                  .completeOutbound(attempt.outbound)
                  .pipe(Effect.zipRight(Queue.offer(outbound, message))),
              ),
              Effect.asVoid,
            );
          default: {
            const exhaustive: never = attempt;
            return exhaustive;
          }
        }
      }),
      Effect.orDie,
    ),
  );
}
