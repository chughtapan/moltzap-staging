/** @file Packs the package, installs the tarball in a scratch project, and checks that the installed `sharnessd` prints its usage and that the shipped message policy is the repository's. */
import { Command, FileSystem } from "@effect/platform";
import { NodeContext, NodeRuntime } from "@effect/platform-node";
import { Console, Data, Effect, Schema } from "effect";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const policy = join("config", "firewall", "message-policy.json");

/** A check of the packed package that did not hold. */
class PackCheckError extends Data.TaggedError("PackCheckError")<{
  readonly message: string;
}> {}

/** Runs a command in `cwd` with its output shown, and fails unless it exits with zero. */
const run = (cwd: string, command: string, ...args: readonly string[]) =>
  Command.make(command, ...args).pipe(
    Command.workingDirectory(cwd),
    Command.stdout("inherit"),
    Command.stderr("inherit"),
    Command.exitCode,
    Effect.filterOrFail(
      (code) => code === 0,
      (code) =>
        new PackCheckError({
          message: `${[command, ...args].join(" ")} exited with ${code}`,
        }),
    ),
  );

const check = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const scratch = yield* fs.makeTempDirectoryScoped({
    prefix: "sharness-social-harness-pack-",
  });
  const { name, packageManager } = yield* fs
    .readFileString(join(root, "package.json"))
    .pipe(
      Effect.flatMap(
        Schema.decodeUnknown(
          Schema.parseJson(
            Schema.Struct({
              name: Schema.String,
              packageManager: Schema.String,
            }),
          ),
        ),
      ),
    );
  yield* run(root, "pnpm", "pack", "--pack-destination", scratch);
  const [tarball] = yield* fs.readDirectory(scratch);
  const consumer = join(scratch, "consumer");
  yield* fs.makeDirectory(consumer);
  yield* fs.writeFileString(
    join(consumer, "package.json"),
    JSON.stringify({
      name: "sharness-daemon-packed-consumer",
      private: true,
      packageManager,
      dependencies: { [name]: `file:${join(scratch, tarball ?? "")}` },
    }),
  );
  yield* run(consumer, "pnpm", "install", "--prefer-offline");
  const usage = yield* Command.make(
    join(consumer, "node_modules", ".bin", "sharnessd"),
    "--help",
  ).pipe(Command.workingDirectory(consumer), Command.string);
  if (!usage.includes("Usage: sharnessd")) {
    return yield* new PackCheckError({
      message: `expected the installed sharnessd --help to print its usage, got:\n${usage}`,
    });
  }
  const installed = join(consumer, "node_modules", name, policy);
  if (
    (yield* fs.readFileString(installed)) !==
    (yield* fs.readFileString(join(root, policy)))
  ) {
    return yield* new PackCheckError({
      message: "the packed message policy differs from the repository's",
    });
  }
  yield* Console.log("social-harness package consumer check passed");
}).pipe(Effect.scoped, Effect.withSpan("packedPackageCheck"));

NodeRuntime.runMain(check.pipe(Effect.provide(NodeContext.layer)));
