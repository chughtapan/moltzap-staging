/** @file Runs the Grok receiver. A process supervisor owns restarts. */
import { FetchHttpClient } from "@effect/platform";
import { NodeRuntime } from "@effect/platform-node";
import { packageVersion } from "@moltzap/social-harness";
import { Config, Data, Effect } from "effect";
import { acquireMcpConnection } from "#sharness-mcp/mcp-client";
import { deliverToGrokBot, validateGrokRoutine } from "./index.js";

/** Configuration errors contain neither the private routine destination nor credentials. */
class GrokConfigurationError extends Data.TaggedError(
  "GrokConfigurationError",
) {
  override get message(): string {
    return "Grok receiver configuration is missing or invalid. Docs: https://github.com/social-harness/moltzap/tree/main/plugins/grokbot#configuration";
  }
}

const mcpCredential = Config.redacted("SHARNESS_MCP_TOKEN");
const routineKey = Config.redacted("SHARNESS_GROK_ROUTINE_KEY");

const program = Effect.gen(function* () {
  const config = yield* Config.all({
    endpoint: Config.url("SHARNESS_MCP_URL"),
    credential: mcpCredential,
    url: Config.url("SHARNESS_GROK_ROUTINE_URL"),
    key: routineKey,
  }).pipe(
    Effect.catchTag("ConfigError", () =>
      Effect.fail(new GrokConfigurationError()),
    ),
  );
  const routine = yield* validateGrokRoutine({
    url: config.url,
    key: config.key,
  });
  const connection = yield* acquireMcpConnection(
    { endpoint: config.endpoint, credential: config.credential },
    { name: "sharness-grokbot", version: packageVersion },
  );
  yield* connection.consumeInbox(
    { onReady: Effect.logInfo("Grok receiver ready") },
    (entry) => deliverToGrokBot(routine, entry),
  );
});

NodeRuntime.runMain(
  program.pipe(Effect.provide(FetchHttpClient.layer), Effect.scoped),
);
