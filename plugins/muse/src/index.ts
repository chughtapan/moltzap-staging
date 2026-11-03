/** @file Publishes persistent event references for Muse's native hook and worker. */
import { FileSystem } from "@effect/platform";
import { Effect } from "effect";
import { randomUUID } from "node:crypto";
import { isAbsolute, join } from "node:path";
import { MuseWakeError } from "./errors.js";
import { writeWork } from "./pending-events.js";

/** The native worker retires a reference after retrieving its saved event. */
export { acceptMuseWork, detectMuseWork } from "./pending-events.js";

/**
 * Requires an absolute directory. The returned callback completes after
 * publishing a reference that survives receiver shutdown, before model processing.
 */
export const makeMuseWakeup = Effect.fn("Muse.makeWakeup")(function* (
  directory: string,
) {
  if (!isAbsolute(directory)) {
    return yield* new MuseWakeError({ reason: "invalid-configuration" });
  }
  const fs = yield* FileSystem.FileSystem;
  yield* fs.makeDirectory(directory, { recursive: true, mode: 0o700 }).pipe(
    Effect.catchTags({
      SystemError: () =>
        Effect.fail(new MuseWakeError({ reason: "invalid-configuration" })),
      BadArgument: () =>
        Effect.fail(new MuseWakeError({ reason: "invalid-configuration" })),
    }),
  );
  return (entry: { readonly deliveryToken: string }) =>
    Effect.gen(function* () {
      const attempt = yield* Effect.sync(randomUUID);
      const path = join(directory, attempt);
      yield* fs.makeDirectory(path, { mode: 0o700 });
      yield* writeWork(fs, path, {
        attempt,
        eventId: entry.deliveryToken,
      }).pipe(
        Effect.onError(() =>
          fs.remove(path, { recursive: true, force: true }).pipe(Effect.ignore),
        ),
      );
    }).pipe(
      Effect.catchTags({
        ParseError: () =>
          Effect.fail(new MuseWakeError({ reason: "decode-failed" })),
        SystemError: () =>
          Effect.fail(new MuseWakeError({ reason: "transport-failed" })),
        BadArgument: () =>
          Effect.fail(new MuseWakeError({ reason: "transport-failed" })),
      }),
    );
}, Effect.withSpan("makeMuseWakeup"));
