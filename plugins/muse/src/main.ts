/** @file Remote Muse receiver and credential-free native detector/worker commands. */
import { FetchHttpClient } from "@effect/platform";
import { NodeFileSystem, NodeRuntime } from "@effect/platform-node";
import { packageVersion } from "@moltzap/social-harness";
import { Config, Console, Effect, Layer } from "effect";
import { isAbsolute } from "node:path";
import { acquireMcpConnection } from "#sharness-mcp/mcp-client";
import { MuseWakeError } from "./errors.js";
import { acceptMuseWork, detectMuseWork, makeMuseWakeup } from "./index.js";

const mcpCredential = Config.redacted("SHARNESS_MCP_TOKEN");

const program = Effect.gen(function* () {
  const operation = yield* Config.literal(
    "receive",
    "detect",
    "accept",
  )("SHARNESS_MUSE_OPERATION").pipe(Config.withDefault("receive"));
  const directory = yield* Config.string("SHARNESS_MUSE_STATE_DIR");
  if (!isAbsolute(directory)) {
    return yield* new MuseWakeError({ reason: "invalid-configuration" });
  }
  switch (operation) {
    case "receive":
      return yield* receive(directory);
    case "accept": {
      const attempt = yield* Config.string("SHARNESS_MUSE_ATTEMPT");
      const eventId = yield* Config.string("SHARNESS_MUSE_EVENT_ID");
      return yield* acceptMuseWork(directory, attempt, eventId);
    }
    case "detect": {
      const references = yield* detectMuseWork(directory);
      return yield* Effect.forEach(
        references,
        (reference) => Console.log(JSON.stringify(reference)),
        { concurrency: 1, discard: true },
      );
    }
    default: {
      const exhaustive: never = operation;
      return exhaustive;
    }
  }
}).pipe(
  Effect.catchTag("ConfigError", () =>
    Effect.fail(new MuseWakeError({ reason: "invalid-configuration" })),
  ),
);

function receive(directory: string) {
  return Effect.gen(function* () {
    const config = yield* Config.all({
      endpoint: Config.url("SHARNESS_MCP_URL"),
      credential: mcpCredential,
    });
    const wake = yield* makeMuseWakeup(directory);
    const connection = yield* acquireMcpConnection(config, {
      name: "sharness-muse",
      version: packageVersion,
    });
    yield* connection.consumeInbox(
      { onReady: Effect.logInfo("Muse receiver ready") },
      wake,
    );
  });
}

NodeRuntime.runMain(
  program.pipe(
    Effect.provide(Layer.mergeAll(NodeFileSystem.layer, FetchHttpClient.layer)),
    Effect.scoped,
  ),
);
