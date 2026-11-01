/** @file Atomic pending files keep Muse triggers available across failed wakes and restarts. */
import { FileSystem } from "@effect/platform";
import { Console, Effect, Schema } from "effect";
import { randomUUID } from "node:crypto";
import { join } from "node:path";
import { MuseWakeError } from "./errors.js";

const reference = Schema.parseJson(
  Schema.Struct({ eventId: Schema.NonEmptyString }),
);
const pendingName = Schema.pattern(
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.json$/,
)(Schema.String);

/** A staging name is invisible to detection until the complete file is renamed. */
export const publishReference = Effect.fn("Muse.publishReference")(
  function* (fs: FileSystem.FileSystem, directory: string, eventId: string) {
    const bytes = yield* Schema.encode(reference)({ eventId });
    const temporary = yield* fs.makeTempFileScoped({
      directory,
      prefix: ".publishing-",
    });
    yield* fs.chmod(temporary, 0o600);
    yield* fs.writeFileString(temporary, bytes);
    const name = yield* Effect.sync(randomUUID);
    yield* fs.rename(temporary, join(directory, `${name}.json`));
  },
  Effect.scoped,
  Effect.catchTags({
    BadArgument: () =>
      Effect.fail(new MuseWakeError({ reason: "transport-failed" })),
    SystemError: () =>
      Effect.fail(new MuseWakeError({ reason: "transport-failed" })),
    ParseError: () =>
      Effect.fail(new MuseWakeError({ reason: "decode-failed" })),
  }),
);

/**
 * Returns pending event IDs and their local paths without consuming them.
 * A failed hook or lookup can retry. The worker removes a file after successful
 * read_event; until then the same file may wake it more than once.
 */
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
  const work = yield* Effect.forEach(
    names
      .filter(Schema.is(pendingName))
      .sort((left, right) => left.localeCompare(right)),
    (name) => readReference(fs, join(directory, name)),
    { concurrency: 1 },
  );
  return work.filter((item) => item !== undefined);
});

function readReference(fs: FileSystem.FileSystem, path: string) {
  return Effect.gen(function* () {
    const bytes = yield* fs.readFileString(path);
    const work = yield* Schema.decode(reference)(bytes, {
      onExcessProperty: "error",
    });
    return { ...work, path };
  }).pipe(
    Effect.catchTags({
      SystemError: (error) =>
        error.reason === "NotFound"
          ? Effect.succeed(undefined)
          : reportUnusable(path, "transport-failed"),
      BadArgument: () => reportUnusable(path, "transport-failed"),
      ParseError: () => reportUnusable(path, "decode-failed"),
    }),
  );
}

function reportUnusable(path: string, reason: MuseWakeError["reason"]) {
  return Console.error(
    `Muse skipped pending file ${path} (${reason}). Docs: https://github.com/social-harness/moltzap/tree/main/plugins/muse#trigger-failed`,
  ).pipe(Effect.as(undefined));
}
