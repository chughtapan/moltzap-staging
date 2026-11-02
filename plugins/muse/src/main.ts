/** @file Remote Muse receiver and credential-free pending-work detector. */
import { FetchHttpClient } from "@effect/platform";
import { NodeFileSystem, NodeRuntime } from "@effect/platform-node";
import { Config, Console, Effect, Layer } from "effect";
import { isAbsolute } from "node:path";
import { MuseWakeError } from "./errors.js";
import { detectMuseWork, makeMuseWakeup } from "./index.js";

const mcpCredential = Config.redacted("SHARNESS_MCP_TOKEN");

const program = Effect.gen(function* () {
  const operation = yield* Config.literal(
    "receive",
    "detect",
  )("SHARNESS_MUSE_OPERATION").pipe(Config.withDefault("receive"));
  const directory = yield* Config.string("SHARNESS_MUSE_STATE_DIR");
  if (!isAbsolute(directory)) {
    return yield* new MuseWakeError({ reason: "invalid-configuration" });
  }
  switch (operation) {
    case "receive":
      return yield* receive(directory);
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

/** Native detector ticks avoid loading network clients and daemon service modules. */
function receive(directory: string) {
  return Effect.gen(function* () {
    const config = yield* Config.all({
      endpoint: Config.url("SHARNESS_MCP_URL"),
      credential: mcpCredential,
    });
    const [{ acquireMcpConnection }, { packageVersion }] = yield* Effect.all(
      [
        Effect.tryPromise({
          try: () => import("#sharness-mcp/mcp"),
          catch: () => new MuseWakeError({ reason: "invalid-configuration" }),
        }),
        Effect.tryPromise({
          try: () => import("@moltzap/social-harness"),
          catch: () => new MuseWakeError({ reason: "invalid-configuration" }),
        }),
      ],
      { concurrency: 2 },
    );
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
