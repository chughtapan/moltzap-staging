/** @file Real pending files survive receiver and hook failures without a worker receipt. */
import { Command, FileSystem } from "@effect/platform";
import { NodeContext } from "@effect/platform-node";
import { scopedLive as it } from "@effect/vitest";
import { DeliveryAcknowledgeError } from "@moltzap/social-harness";
import { ConfigProvider, Effect, Ref, Schema } from "effect";
import { randomUUID } from "node:crypto";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- Only POSIX permission constants are imported; all filesystem operations use Effect.
import { constants } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect } from "vitest";
import { detectMuseWork, makeMuseWakeup } from "./index.js";

const eventId = `evt_${"A".repeat(43)}`;
const anotherEventId = `evt_${"B".repeat(43)}`;
const fixture = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const root = yield* fs.makeTempDirectoryScoped({ prefix: "sharness-muse-" });
  const directory = join(root, "pending");
  const wake = yield* makeMuseWakeup(directory);
  const receipts = yield* Ref.make(0);
  const receive = (id = eventId) =>
    wake({
      eventId: id,
      acknowledge: Ref.update(receipts, (count) => count + 1),
    });
  return { fs, root, directory, wake, receipts, receive };
});
type Fixture = Effect.Effect.Success<typeof fixture>;

function queuesBeforeAcknowledging() {
  return inDirectory(({ fs, directory, wake }) =>
    wake({
      eventId,
      acknowledge: Effect.gen(function* () {
        expect(
          (yield* detectMuseWork(directory)).map((work) => work.eventId),
        ).toEqual([eventId]);
      }).pipe(Effect.provideService(FileSystem.FileSystem, fs), Effect.orDie),
    }),
  );
}

function survivesRestart() {
  return inDirectory(({ directory, receive, receipts }) =>
    Effect.gen(function* () {
      yield* receive().pipe(Effect.scoped);
      yield* makeMuseWakeup(directory).pipe(Effect.scoped);

      expect(yield* Ref.get(receipts)).toBe(1);
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
  return inDirectory(({ directory, receive, receipts }) =>
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
      expect(yield* Ref.get(receipts)).toBe(2);
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

function retainsOnReceiptFailure() {
  return inDirectory(({ directory, wake }) =>
    Effect.gen(function* () {
      const failure = new DeliveryAcknowledgeError({
        reason: "consumer-revoked",
      });
      const result = yield* wake({
        eventId,
        acknowledge: Effect.fail(failure),
      }).pipe(Effect.flip);

      expect(result).toBe(failure);
      expect(
        (yield* detectMuseWork(directory)).map((work) => work.eventId),
      ).toEqual([eventId]);
    }),
  );
}

function refusesFailedPublication() {
  return inDirectory(({ fs, directory, receive, receipts }) =>
    Effect.gen(function* () {
      yield* fs.remove(directory, { recursive: true });
      yield* fs.writeFileString(directory, "unavailable queue");

      expect(yield* receive().pipe(Effect.flip)).toMatchObject({
        reason: "transport-failed",
      });
      expect(yield* Ref.get(receipts)).toBe(0);
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

function hookRetry() {
  return inDirectory(({ fs, root, directory, receive }) =>
    Effect.gen(function* () {
      const runtime = join(root, "runtime.sh");
      yield* fs.writeFileString(
        runtime,
        'wake() { printf \'%s\\n\' "$2"; return "${MUSE_TEST_WAKE_EXIT:-0}"; }\nsilent() { printf \'%s\\n\' "$2"; }\n',
      );
      const runHook = (exit = "0") =>
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
            SHARNESS_RUNTIME_TOKEN: "",
          }),
        );

      expect(
        yield* Schema.decode(Schema.parseJson(Schema.Unknown))(
          yield* runHook().pipe(Command.string),
        ),
      ).toEqual({});
      yield* receive();
      expect(yield* runHook("1").pipe(Command.exitCode)).toBe(1);
      const output = yield* runHook().pipe(Command.string);

      expect(
        yield* Schema.decode(Schema.parseJson(Schema.Unknown))(output),
      ).toEqual({
        references: yield* detectMuseWork(directory),
      });
      expect(
        (yield* detectMuseWork(directory)).map((work) => work.eventId),
      ).toEqual([eventId]);
    }),
  );
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
    "publishes the retained reference before acknowledging",
    queuesBeforeAcknowledging,
  );
  it(
    "retains acknowledged work across receiver shutdown and restart",
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
    "preserves queued work when acknowledgment fails",
    retainsOnReceiptFailure,
  );
  it(
    "leaves delivery pending when publication fails",
    refusesFailedPublication,
  );
  it(
    "returns usable work alongside corrupt and incomplete files",
    isolatesUnusableFiles,
  );
  it(
    "runs the built detector without credentials and retries a failed native wake",
    hookRetry,
  );
});
