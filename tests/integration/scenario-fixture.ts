/** @file Vitest lifecycle adapter for scoped process traces. */
import type { FileSystem } from "@effect/platform";
import { NodeContext } from "@effect/platform-node";
import { Effect, Fiber, type Scope } from "effect";
import { expect, type TestContext } from "vitest";

/** Teardown waits for every scoped child after a test is interrupted. */
const TEARDOWN_TIMEOUT_MS = 60_000;

/**
 * The vitest test function of a real-process trace that must assert. A test
 * that times out is failed without waiting for its body, and the worker can
 * exit before an interrupted trace's scope has stopped its children, which
 * then outlive the run. The trace therefore runs as a fiber that an
 * `onTestFinished` hook interrupts, waiting until its scope has stopped every
 * child.
 * @param trace A trace whose scope owns every process it starts.
 * @returns The test function, whose promise settles with the trace.
 */
export function processTrace<E>(
  trace: Effect.Effect<void, E, FileSystem.FileSystem | Scope.Scope>,
) {
  return (context: TestContext) => {
    expect.hasAssertions();
    const fiber = Effect.runFork(
      Effect.scoped(trace).pipe(Effect.provide(NodeContext.layer)),
    );
    context.onTestFinished(
      () => Effect.runPromise(Fiber.interrupt(fiber).pipe(Effect.asVoid)),
      TEARDOWN_TIMEOUT_MS,
    );
    return Effect.runPromise(Fiber.join(fiber));
  };
}
