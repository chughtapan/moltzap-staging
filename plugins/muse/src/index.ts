/** @file Muse queues retained event references for its credential-free native hook. */
import { FileSystem } from "@effect/platform";
import { Effect } from "effect";
import { isAbsolute } from "node:path";
import { MuseWakeError } from "./errors.js";
import { publishReference } from "./pending.js";

/** Detection leaves work available until the native worker reads its retained event. */
export { detectMuseWork } from "./pending.js";

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
      BadArgument: () =>
        Effect.fail(new MuseWakeError({ reason: "transport-failed" })),
      SystemError: () =>
        Effect.fail(new MuseWakeError({ reason: "transport-failed" })),
    }),
  );
  return (entry: { readonly deliveryToken: string }) =>
    publishReference(fs, directory, entry.deliveryToken);
});
