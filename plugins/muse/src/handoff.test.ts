/** @file Persistent references survive receiver shutdown and remain available until accepted. */
import { FileSystem } from "@effect/platform";
import { NodeFileSystem } from "@effect/platform-node";
import { scopedLive as it } from "@effect/vitest";
import { Effect } from "effect";
import { describe, expect } from "vitest";
import { acceptMuseWork, detectMuseWork, makeMuseWakeup } from "./index.js";

const deliveryToken = `dlv_${"A".repeat(43)}`;
const fixture = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const directory = yield* fs.makeTempDirectoryScoped({
    prefix: "sharness-muse-",
  });
  return { directory, fs };
});

// @agent-code-guard/regression-only: Persistent handoff, stale acknowledgments and receiver shutdown exercise separate lifecycle cases.
describe("Muse work queue", () => {
  it("keeps a published reference after the receiver scope closes", () =>
    Effect.gen(function* () {
      const { directory } = yield* fixture;
      yield* Effect.scoped(
        Effect.gen(function* () {
          const wake = yield* makeMuseWakeup(directory);
          yield* wake({ deliveryToken });
        }),
      );
      const first = yield* detectMuseWork(directory);
      expect(first).toHaveLength(1);
      const work = first[0];
      if (work === undefined) {
        return yield* Effect.dieMessage("Published work is missing");
      }
      expect(work.eventId).toBe(deliveryToken);
      expect(yield* detectMuseWork(directory)).toEqual(first);
      yield* acceptMuseWork(directory, work.attempt, work.eventId);
      expect(yield* detectMuseWork(directory)).toEqual([]);
      yield* acceptMuseWork(directory, work.attempt, work.eventId);
    }).pipe(Effect.provide(NodeFileSystem.layer)));

  it("refuses a mismatched reference without removing the pending work", () =>
    Effect.gen(function* () {
      const { directory } = yield* fixture;
      const wake = yield* makeMuseWakeup(directory);
      yield* wake({ deliveryToken });
      const [work] = yield* detectMuseWork(directory);
      if (work === undefined) {
        return yield* Effect.dieMessage("Published work is missing");
      }
      expect(
        yield* acceptMuseWork(directory, work.attempt, "another-event").pipe(
          Effect.flip,
        ),
      ).toMatchObject({ reason: "refused" });
      expect(yield* detectMuseWork(directory)).toEqual([work]);
    }).pipe(Effect.provide(NodeFileSystem.layer)));

  it("retains independent publications of the same event", () =>
    Effect.gen(function* () {
      const { directory } = yield* fixture;
      const wake = yield* makeMuseWakeup(directory);
      yield* wake({ deliveryToken });
      yield* wake({ deliveryToken });
      const work = yield* detectMuseWork(directory);
      expect(work).toHaveLength(2);
      expect(new Set(work.map((entry) => entry.attempt)).size).toBe(
        work.length,
      );
      expect(work.map((entry) => entry.eventId)).toEqual([
        deliveryToken,
        deliveryToken,
      ]);
    }).pipe(Effect.provide(NodeFileSystem.layer)));

  it("reports a failed publication", () =>
    Effect.gen(function* () {
      const { directory, fs } = yield* fixture;
      const wake = yield* makeMuseWakeup(directory);
      yield* fs.remove(directory, { recursive: true });
      yield* fs.writeFileString(directory, "not a directory");
      expect(yield* wake({ deliveryToken }).pipe(Effect.flip)).toMatchObject({
        reason: "transport-failed",
      });
    }).pipe(Effect.provide(NodeFileSystem.layer)));
});
