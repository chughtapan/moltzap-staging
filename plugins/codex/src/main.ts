#!/usr/bin/env node
/** @file The native process and its borrowed MCP authority share one supervised lifetime. */
import { FileSystem } from "@effect/platform";
import { NodeContext, NodeRuntime } from "@effect/platform-node";
import { Config, Effect } from "effect";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { acquireCodexReceiver } from "./host.js";

const program = Effect.gen(function* () {
  const options = yield* Config.all({
    binary: Config.string("SHARNESS_CODEX_BINARY").pipe(
      Config.withDefault("codex"),
    ),
    directory: Config.string("SHARNESS_CODEX_DIRECTORY"),
    server: Config.string("SHARNESS_CODEX_MCP_SERVER"),
    threadFile: Config.string("SHARNESS_CODEX_THREAD_FILE"),
    instructionsFile: Config.string("SHARNESS_CODEX_INSTRUCTIONS_FILE"),
  });
  const fs = yield* FileSystem.FileSystem;
  const ownerInstructions = yield* fs.readFileString(options.instructionsFile);
  const skills = fileURLToPath(new URL("./skills/", import.meta.url));
  const folders = yield* fs.readDirectory(skills);
  const instructions = yield* Effect.forEach(
    [...folders].sort((left, right) => left.localeCompare(right)),
    (folder) => fs.readFileString(join(skills, folder, "SKILL.md")),
    { concurrency: 1 },
  );
  const receive = yield* acquireCodexReceiver({
    ...options,
    instructions: [...instructions, ownerInstructions].join("\n\n"),
  });
  yield* receive;
});

NodeRuntime.runMain(
  program.pipe(Effect.scoped, Effect.provide(NodeContext.layer)),
);
