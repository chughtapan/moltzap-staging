/** @file Retains event references until the native worker accepts them. */

import { FileSystem } from "@effect/platform";
import { Effect, Schema } from "effect";
import { join } from "node:path";
import { MuseWakeError } from "./errors.js";

/** An attempt is a local correlation value, not authentication or delivery authority. */
const museWork = Schema.Struct({
  attempt: Schema.UUID,
  eventId: Schema.NonEmptyString,
});

/** The detector passes the saved lookup identity to a native worker. */
export type MuseWork = typeof museWork.Type;

/** Publication is atomic so the detector cannot observe a partially written reference. */
export const writeWork = Effect.fn("Muse.writeWork")(function* (
  fs: FileSystem.FileSystem,
  directory: string,
  work: MuseWork,
) {
  const bytes = yield* Schema.encode(Schema.parseJson(museWork))(work);
  const staging = join(directory, "publishing");
  yield* fs.writeFileString(staging, bytes, { mode: 0o600, flag: "wx" });
  yield* fs.rename(staging, join(directory, "work.json"));
});

/**
 * Returns undefined when no reference is published. Damaged references and
 * other I/O failures fail the read.
 */
function readWork(fs: FileSystem.FileSystem, directory: string) {
  return readOptionalFile(fs, join(directory, "work.json")).pipe(
    Effect.flatMap((bytes) =>
      bytes === undefined
        ? Effect.succeed(undefined)
        : Schema.decode(Schema.parseJson(museWork))(bytes).pipe(
            Effect.catchTag("ParseError", () =>
              Effect.fail(new MuseWakeError({ reason: "decode-failed" })),
            ),
          ),
    ),
  );
}

/** Absence is normal while a reference is being published or released. Other I/O failures remain failures. */
function readOptionalFile(fs: FileSystem.FileSystem, path: string) {
  return fs.readFileString(path).pipe(
    Effect.catchTags({
      SystemError: (error) =>
        error.reason === "NotFound"
          ? Effect.succeed(undefined)
          : Effect.fail(new MuseWakeError({ reason: "transport-failed" })),
      BadArgument: () =>
        Effect.fail(new MuseWakeError({ reason: "transport-failed" })),
    }),
  );
}

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
    Effect.catchTag("BadArgument", () =>
      Effect.fail(new MuseWakeError({ reason: "transport-failed" })),
    ),
  );
  const references = yield* Effect.forEach(
    names
      .filter(Schema.is(Schema.UUID))
      .sort((left, right) => left.localeCompare(right)),
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
    Effect.catchTag("ParseError", () =>
      Effect.fail(new MuseWakeError({ reason: "refused" })),
    ),
  );
  const fs = yield* FileSystem.FileSystem;
  const path = join(directory, attempt);
  const work = yield* readWork(fs, path);
  if (work === undefined) {
    return;
  }
  if (work.attempt !== attempt || work.eventId !== eventId) {
    return yield* new MuseWakeError({ reason: "refused" });
  }
  yield* fs.remove(path, { recursive: true, force: true }).pipe(
    Effect.catchTags({
      SystemError: () =>
        Effect.fail(new MuseWakeError({ reason: "transport-failed" })),
      BadArgument: () =>
        Effect.fail(new MuseWakeError({ reason: "transport-failed" })),
    }),
  );
});
