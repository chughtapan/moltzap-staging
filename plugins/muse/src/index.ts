/** @file Publishes persistent event references for Muse's native hook and worker. */
import { FileSystem } from "@effect/platform";
import { Effect } from "effect";
import { randomUUID } from "node:crypto";
import { isAbsolute, join } from "node:path";
import { MuseWakeError } from "./errors.js";
import { writeWork } from "./handoff-files.js";

/** The native worker retires a reference after retrieving its saved event. */
export { acceptMuseWork, detectMuseWork } from "./worker.js";

/** Returns after publishing a persistent reference, before model processing. */
export const makeMuseWakeup = Effect.fn("Muse.makeWakeup")(function* (
  directory: string,
) {
  if (!isAbsolute(directory)) {
    return yield* new MuseWakeError({ reason: "invalid-configuration" });
  }
  const fs = yield* FileSystem.FileSystem;
  yield* fs
    .makeDirectory(directory, { recursive: true, mode: 0o700 })
    .pipe(
      Effect.mapError(
        () => new MuseWakeError({ reason: "invalid-configuration" }),
      ),
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
      Effect.mapError(() => new MuseWakeError({ reason: "transport-failed" })),
    );
});
