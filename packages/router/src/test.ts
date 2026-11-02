/** @file Condition-only process composition for adjacent wire releases. */
import { WireVersions } from "@moltzap/social-harness/test";
import { Effect, Layer } from "effect";
import { RouterServer } from "./index.js";
import { runRouterMain } from "./main.js";

/**
 * Starts a fixture process with explicit emitted, live and history versions
 * and the ordinary Router environment settings. Start Node with both
 * `sharness-test` and `sharness-internal` conditions. Startup failure is
 * reported to stderr with a nonzero exit code.
 */
export function startRouter(versions: {
  readonly emitted: string;
  readonly live: readonly string[];
  readonly history: readonly string[];
}): void {
  Layer.launch(RouterServer.layer).pipe(
    Effect.provideService(WireVersions, versions),
    Effect.withSpan("startRouter"),
    runRouterMain,
  );
}
