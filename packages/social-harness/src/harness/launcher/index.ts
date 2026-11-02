/**
 * @file The `sharnessd-launcher` process: each start picks the release to
 * run, then runs that release's `sharnessd` with the launcher's environment
 * and exits with its status.
 */

import { Command, Path } from "@effect/platform";
import { Console, Effect } from "effect";
import { fileURLToPath } from "node:url";
import type { LauncherConfiguration } from "./selection.js";
import { launcherHelp, loadConfiguration } from "./configuration.js";
import { LAUNCHER_DOCS, launcherError, type LauncherError } from "./errors.js";
import { prepare, type Prepared } from "./prepare.js";

/**
 * Runs the launcher for the arguments after the executable: `--help` prints
 * usage, `prepare` prints the chosen release as one JSON line, and no
 * argument also starts its daemon. It returns the process exit status: 0
 * after `prepare`, otherwise the daemon's.
 */
export const main = Effect.fn("Launcher.main")(function* (
  argv: readonly string[],
) {
  if (argv.includes("--help")) {
    yield* Console.log(launcherHelp);
    return 0;
  }
  const [command] = argv;
  if (command !== undefined && command !== "prepare") {
    return yield* launcherError(
      "configuration",
      `unknown command ${command}; run sharnessd-launcher or sharnessd-launcher prepare. Docs: ${LAUNCHER_DOCS}`,
    );
  }
  const configuration = yield* absolute(yield* loadConfiguration);
  const prepared = yield* Effect.scoped(
    prepare(configuration, yield* seedPrefix(configuration)),
  );
  yield* Console.error(describe(prepared));
  if (command === "prepare") {
    yield* Console.log(JSON.stringify(prepared));
    return 0;
  }
  return yield* Command.make(process.execPath, prepared.daemon).pipe(
    Command.stdin("inherit"),
    Command.stdout("inherit"),
    Command.stderr("inherit"),
    Command.exitCode,
    Effect.mapError(() =>
      launcherError("start", `could not start ${prepared.daemon}`),
    ),
  );
});

/**
 * The settings with the directory and seed resolved against the working
 * directory, since `npm install` runs from inside the launcher directory.
 *
 * @param configuration The settings as configured.
 * @returns The settings with absolute paths.
 */
function absolute(
  configuration: LauncherConfiguration,
): Effect.Effect<LauncherConfiguration, never, Path.Path> {
  return Effect.map(Path.Path, (path) => ({
    ...configuration,
    directory: path.resolve(configuration.directory),
    ...(configuration.seed === undefined
      ? {}
      : { seed: path.resolve(configuration.seed) }),
  }));
}

/**
 * The prefix the image installed.
 *
 * @param configuration The launcher's settings.
 * @returns `SHARNESS_LAUNCHER_SEED`, or the directory whose `node_modules`
 *   holds this package.
 */
function seedPrefix(
  configuration: LauncherConfiguration,
): Effect.Effect<string, LauncherError, Path.Path> {
  return Effect.gen(function* () {
    if (configuration.seed !== undefined) {
      return configuration.seed;
    }
    const path = yield* Path.Path;
    const packageRoot = path.resolve(
      path.dirname(fileURLToPath(import.meta.url)),
      "../../..",
    );
    const nodeModules = path.dirname(path.dirname(packageRoot));
    if (path.basename(nodeModules) !== "node_modules") {
      return yield* launcherError(
        "seed",
        `${packageRoot} is not installed under node_modules; set SHARNESS_LAUNCHER_SEED. Docs: ${LAUNCHER_DOCS}`,
      );
    }
    return path.dirname(nodeModules);
  });
}

function describe(prepared: Prepared): string {
  const release = `${prepared.releasePackage}@${prepared.version}`;
  if (prepared.previous === undefined) {
    return `sharnessd-launcher: running ${release} (${prepared.kept ?? "current"})`;
  }
  const restart = prepared.restartHost
    ? "; the host must restart to load the new channel"
    : "";
  return `sharnessd-launcher: moved from ${prepared.previous} to ${release}${restart}`;
}
