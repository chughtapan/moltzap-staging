/** @file Runs the Instinct receiver. The process supervisor owns restarts. */
import { FetchHttpClient } from "@effect/platform";
import { NodeRuntime } from "@effect/platform-node";
import { packageVersion } from "@moltzap/social-harness";
import { Config, Data, Effect } from "effect";
import { acquireMcpConnection } from "#sharness-mcp/mcp-client";
import { deliverToInstinct } from "./index.js";

/** Configuration diagnostics must not include an invalid URL or secret value. */
class InstinctConfigurationError extends Data.TaggedError(
  "InstinctConfigurationError",
) {
  override get message(): string {
    return "Instinct receiver configuration is missing or invalid. Docs: https://github.com/social-harness/moltzap/tree/main/plugins/instinct#configuration";
  }
}

const mcpCredential = Config.redacted("SHARNESS_MCP_TOKEN");
const agentMailKey = Config.redacted("SHARNESS_AGENTMAIL_API_KEY");

const program = Effect.gen(function* () {
  const config = yield* Config.all({
    endpoint: Config.url("SHARNESS_MCP_URL"),
    credential: mcpCredential,
    apiKey: agentMailKey,
    inboxId: Config.string("SHARNESS_AGENTMAIL_INBOX_ID"),
    recipient: Config.string("SHARNESS_INSTINCT_ADDRESS"),
  }).pipe(
    Effect.catchTag("ConfigError", () =>
      Effect.fail(new InstinctConfigurationError()),
    ),
  );
  const connection = yield* acquireMcpConnection(
    { endpoint: config.endpoint, credential: config.credential },
    { name: "sharness-instinct", version: packageVersion },
  );
  yield* connection.consumeInbox(
    { onReady: Effect.logInfo("Instinct receiver ready") },
    (entry) => deliverToInstinct(config, entry),
  );
});

NodeRuntime.runMain(
  program.pipe(Effect.provide(FetchHttpClient.layer), Effect.scoped),
);
