/** @file Condition-only process composition for adjacent wire releases. */
import { WireVersions } from "@moltzap/social-harness/test";
import { Effect, Layer } from "effect";
import { layer } from "./index.js";
import { runRegistryMain } from "./main.js";

/**
 * Starts Registry with explicit wire compatibility sets and its ordinary
 * environment settings.
 *
 * Start Node with both `sharness-test` and `sharness-internal` conditions.
 * The supplied history extends the library's built-in retained history.
 * Startup failures print their message to stderr and produce a nonzero exit.
 */
export function startRegistry(versions: {
  readonly emitted: string;
  readonly live: readonly string[];
  readonly history: readonly string[];
}): void {
  Layer.launch(layer).pipe(
    Effect.provideService(WireVersions, versions),
    Effect.withSpan("startRegistry"),
    runRegistryMain,
  );
}
