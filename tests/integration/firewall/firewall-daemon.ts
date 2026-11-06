/**
 * @file A daemon process composed exactly as `sharnessd`, whose text rules
 * the marked provider judges. Its message policy and norms come from the
 * settings document the fixture wrote; every `SHARNESS_*` setting is read as
 * `sharnessd` reads it.
 */

import { NodeRuntime } from "@effect/platform-node";
import { daemonLayer } from "@moltzap/social-harness/service";
import { DecisionProviderOverride } from "@moltzap/social-harness/test";
import { ConfigProvider, Effect, Layer } from "effect";
import { markedProvider } from "./policy-fixture.js";

Layer.launch(daemonLayer).pipe(
  Effect.provideService(DecisionProviderOverride, markedProvider),
  Effect.withConfigProvider(ConfigProvider.fromEnv()),
  NodeRuntime.runMain,
);
