/** @file Real pending files survive receiver and hook failures without a worker receipt. */
import { Command, FileSystem } from "@effect/platform";
import { NodeContext } from "@effect/platform-node";
import { scopedLive as it } from "@effect/vitest";
import { ConfigProvider, Effect, Schema } from "effect";
import { randomUUID } from "node:crypto";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- Only POSIX permission constants are imported; all filesystem operations use Effect.
import { constants } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect } from "vitest";
import { detectMuseWork, makeMuseWakeup } from "./index.js";

const eventId = `dlv_${"A".repeat(43)}`;
const anotherEventId = `dlv_${"B".repeat(42)}A`;
const fixture = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const root = yield* fs.makeTempDirectoryScoped({ prefix: "sharness-muse-" });
  const directory = join(root, "pending");
  const wake = yield* makeMuseWakeup(directory);
  const receive = (id = eventId) => wake({ deliveryToken: id });
  return { fs, root, directory, wake, receive };
});
type Fixture = Effect.Effect.Success<typeof fixture>;

function publishesBeforeReturning() {
  return inDirectory(({ directory, receive }) =>
    Effect.gen(function* () {
      yield* receive();
      expect(
        (yield* detectMuseWork(directory)).map((work) => work.eventId),
      ).toEqual([eventId]);
    }),
  );
}

function survivesRestart() {
  return inDirectory(({ directory, receive }) =>
    Effect.gen(function* () {
      yield* receive().pipe(Effect.scoped);
      yield* makeMuseWakeup(directory).pipe(Effect.scoped);
      expect(
        (yield* detectMuseWork(directory)).map((work) => work.eventId),
      ).toEqual([eventId]);
    }),
  );
}

function retainsUntilRead() {
  return inDirectory(({ fs, directory, receive }) =>
    Effect.gen(function* () {
      yield* receive();
      const pending = yield* detectMuseWork(directory);

      expect(yield* detectMuseWork(directory)).toEqual(pending);
      const [work] = pending;
      if (work === undefined) {
        return yield* Effect.dieMessage("Expected pending Muse work");
      }
      yield* fs.remove(work.path);
      expect(yield* detectMuseWork(directory)).toEqual([]);
    }),
  );
}

function keepsMultipleArrivals() {
  return inDirectory(({ directory, receive }) =>
    Effect.gen(function* () {
      yield* Effect.all([receive(), receive(anotherEventId)], {
        concurrency: 2,
      });
      const pending = yield* detectMuseWork(directory);

      expect(
        pending
          .map((work) => work.eventId)
          .sort((left, right) => left.localeCompare(right)),
      ).toEqual([eventId, anotherEventId]);
    }),
  );
}

function protectsPendingReference() {
  return inDirectory(({ fs, directory, receive }) =>
    Effect.acquireUseRelease(
      // eslint-disable-next-line sonarjs/file-permissions -- The isolated test restores the umask after reproducing default file creation.
      Effect.sync(() => process.umask(0o022)),
      () =>
        Effect.gen(function* () {
          // eslint-disable-next-line sonarjs/file-permissions -- Only synthetic state beneath the private temporary root is made traversable.
          yield* fs.chmod(directory, 0o755);
          yield* receive();
          const [work] = yield* detectMuseWork(directory);
          if (work === undefined) {
            return yield* Effect.dieMessage("Expected pending Muse work");
          }

          expect((yield* fs.stat(work.path)).mode & 0o777).toBe(
            constants.S_IRUSR | constants.S_IWUSR,
          );
        }),
      (previous) => Effect.sync(() => process.umask(previous)),
    ),
  );
}

function refusesFailedPublication() {
  return inDirectory(({ fs, directory, receive }) =>
    Effect.gen(function* () {
      yield* fs.remove(directory, { recursive: true });
      yield* fs.writeFileString(directory, "unavailable queue");

      expect(yield* receive().pipe(Effect.flip)).toMatchObject({
        reason: "transport-failed",
      });
    }),
  );
}

function isolatesUnusableFiles() {
  return inDirectory(({ fs, directory, receive }) =>
    Effect.gen(function* () {
      const corrupt = join(directory, `${randomUUID()}.json`);
      yield* fs.writeFileString(corrupt, "{");
      yield* fs.writeFileString(join(directory, ".publishing-incomplete"), "{");
      yield* receive();

      expect(
        (yield* detectMuseWork(directory)).map((work) => work.eventId),
      ).toEqual([eventId]);
      expect(yield* fs.readFileString(corrupt)).toBe("{");
    }),
  );
}

function silentHook() {
  return inDirectory((fixture) =>
    Effect.gen(function* () {
      const runHook = yield* makeHook(fixture);
      expect(
        yield* Schema.decode(Schema.parseJson(Schema.Unknown))(
          yield* runHook().pipe(Command.string),
        ),
      ).toEqual({});
    }),
  );
}

function hookRetry() {
  return inDirectory((fixture) =>
    Effect.gen(function* () {
      const runHook = yield* makeHook(fixture);
      yield* fixture.receive();
      expect(yield* runHook("1").pipe(Command.exitCode)).toBe(1);
      const output = yield* runHook().pipe(Command.string);

      expect(
        yield* Schema.decode(Schema.parseJson(Schema.Unknown))(output),
      ).toEqual({
        references: yield* detectMuseWork(fixture.directory),
      });
      expect(
        (yield* detectMuseWork(fixture.directory)).map((work) => work.eventId),
      ).toEqual([eventId]);
    }),
  );
}

function makeHook({ fs, root, directory }: Fixture) {
  return Effect.gen(function* () {
    const runtime = join(root, "runtime.sh");
    yield* fs.writeFileString(
      runtime,
      'wake() { printf \'%s\\n\' "$2"; return "${MUSE_TEST_WAKE_EXIT:-0}"; }\nsilent() { printf \'%s\\n\' "$2"; }\n',
    );
    return (exit = "0") =>
      Command.make(
        "bash",
        fileURLToPath(new URL("../hook.sh", import.meta.url)),
      ).pipe(
        Command.env({
          HATCH_HOOK_RUNTIME: runtime,
          SHARNESS_MUSE_STATE_DIR: directory,
          SHARNESS_NODE_EXECUTABLE: process.execPath,
          SHARNESS_MUSE_COMMAND: fileURLToPath(
            new URL("../dist/main.js", import.meta.url),
          ),
          MUSE_TEST_WAKE_EXIT: exit,
          SHARNESS_MCP_TOKEN: "",
        }),
      );
  });
}

function inDirectory<A, E, R>(
  run: (fixture: Fixture) => Effect.Effect<A, E, R>,
) {
  return Effect.flatMap(fixture, run).pipe(
    Effect.provide(NodeContext.layer),
    Effect.withConfigProvider(ConfigProvider.fromMap(new Map())),
  );
}

describe("Muse pending triggers", () => {
  it(
    "publishes the retained reference before returning custody",
    publishesBeforeReturning,
  );
  it(
    "retains queued work across receiver shutdown and restart",
    survivesRestart,
  );
  it(
    "keeps work discoverable until the worker removes its file",
    retainsUntilRead,
  );
  it("keeps concurrent arrivals in separate files", keepsMultipleArrivals);
  it(
    "protects references in an existing traversable state directory",
    protectsPendingReference,
  );
  it(
    "leaves delivery pending when publication fails",
    refusesFailedPublication,
  );
  it(
    "returns usable work alongside corrupt and incomplete files",
    isolatesUnusableFiles,
  );
  it("keeps an empty installed hook silent", silentHook);
  it("retries a failed native wake without dropping pending work", hookRetry);
});
