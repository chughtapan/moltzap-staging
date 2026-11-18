/**
 * @file A daemon process composed exactly as `sharnessd`, whose text rules
 * the marked provider judges. Its message policy and norms come from the
 * settings file the fixture wrote; every `SHARNESS_*` setting is read as
 * `sharnessd` reads it.
 */

import { NodeRuntime } from "@effect/platform-node";
import { runDaemon } from "@social-harness/social-harness/service";
import { DecisionProviderOverride } from "@social-harness/social-harness/test";
import { ConfigProvider, Effect } from "effect";
import { markedProvider } from "./policy-fixture.js";

runDaemon.pipe(
  Effect.provideService(DecisionProviderOverride, markedProvider),
  Effect.withConfigProvider(ConfigProvider.fromEnv()),
  NodeRuntime.runMain,
);
