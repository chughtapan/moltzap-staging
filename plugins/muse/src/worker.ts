/** @file Muse reads persistent work references and retires them after native acceptance. */
import { FileSystem } from "@effect/platform";
import { Effect, Schema } from "effect";
import { join } from "node:path";
import { MuseWakeError } from "./errors.js";
import { readWork } from "./handoff-files.js";

/** Pending references survive receiver shutdown and may be offered again until accepted. */
export const detectMuseWork = Effect.fn("Muse.detectWork")(function* (
  directory: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const names = yield* fs.readDirectory(directory).pipe(
    Effect.catchTag("SystemError", (error) =>
      error.reason === "NotFound"
        ? Effect.succeed([])
        : Effect.fail(new MuseWakeError({ reason: "transport-failed" })),
    ),
    Effect.mapError(() => new MuseWakeError({ reason: "transport-failed" })),
  );
  const references = yield* Effect.forEach(
    names.filter(Schema.is(Schema.UUID)).sort(),
    (attempt) => readWork(fs, join(directory, attempt)),
    { concurrency: 1 },
  );
  return references.filter((reference) => reference !== undefined);
});

/** Retires local work only. Reading and acknowledging the daemon's inbox remain independent. */
export const acceptMuseWork = Effect.fn("Muse.acceptWork")(function* (
  directory: string,
  attempt: string,
  eventId: string,
) {
  yield* Schema.decodeUnknown(Schema.UUID)(attempt).pipe(
    Effect.mapError(() => new MuseWakeError({ reason: "refused" })),
  );
  const fs = yield* FileSystem.FileSystem;
  const path = join(directory, attempt);
  const work = yield* readWork(fs, path);
  if (work === undefined) return;
  if (work.attempt !== attempt || work.eventId !== eventId) {
    return yield* new MuseWakeError({ reason: "refused" });
  }
  yield* fs
    .remove(path, { recursive: true, force: true })
    .pipe(
      Effect.mapError(() => new MuseWakeError({ reason: "transport-failed" })),
    );
});
