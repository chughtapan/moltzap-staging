/** @file Loads launcher settings, selects an installed release and starts its daemon. */
import { Command, Path } from "@effect/platform";
import {
  Config,
  type ConfigError,
  Console,
  Effect,
  Match,
  Option,
  Schema,
} from "effect";
import { fileURLToPath } from "node:url";
import {
  DAEMON_PACKAGE,
  LAUNCHER_DOCS,
  type LauncherConfiguration,
  launcherError,
  type LauncherError,
  prepare,
  type Prepared,
  type ReleasePackage,
  type Selection,
  UPGRADES_GUIDE,
} from "./installation.js";

const RELEASE_PACKAGES = [
  DAEMON_PACKAGE,
  "@moltzap/openclaw-channel",
] as const satisfies readonly ReleasePackage[];

const path = Schema.String.pipe(
  Schema.minLength(1),
  Schema.filter((value) => !value.includes("\u0000")),
);

const calendarVersion = Schema.String.pipe(Schema.pattern(/^\d+\.\d+\.\d+$/u));

const distTag = Schema.String.pipe(Schema.pattern(/^[a-z][a-z0-9-]*$/u));

const origin = Schema.String.pipe(
  Schema.filter(
    (value) => URL.canParse(value) && new URL(value).origin === value,
  ),
);

const fields = {
  directory: Schema.Config("SHARNESS_LAUNCHER_DIRECTORY", path),
  releasePackage: Schema.Config(
    "SHARNESS_LAUNCHER_PACKAGE",
    Schema.Literal(...RELEASE_PACKAGES),
  ).pipe(Config.withDefault(DAEMON_PACKAGE)),
  version: Schema.Config("SHARNESS_LAUNCHER_VERSION", calendarVersion).pipe(
    Config.option,
  ),
  upgrade: Schema.Config(
    "SHARNESS_LAUNCHER_UPGRADE",
    Schema.Literal("on", "off"),
  ).pipe(Config.withDefault("on" as const)),
  distTag: Schema.Config("SHARNESS_LAUNCHER_DIST_TAG", distTag).pipe(
    Config.withDefault("latest"),
  ),
  registry: Schema.Config("SHARNESS_LAUNCHER_REGISTRY", origin).pipe(
    Config.withDefault("https://registry.npmjs.org"),
  ),
  seed: Schema.Config("SHARNESS_LAUNCHER_SEED", path).pipe(Config.option),
};

/** The usage the launcher prints for `--help`. */
export const launcherHelp = [
  "Usage: sharnessd-launcher [prepare] [--help]",
  "",
  "Chooses the release to run, then starts its sharnessd with the remaining",
  "environment; `prepare` only chooses, installs and prints the choice as JSON.",
  "",
  "Environment:",
  "  SHARNESS_LAUNCHER_DIRECTORY  required; the home-volume directory the launcher owns",
  `  SHARNESS_LAUNCHER_PACKAGE    ${RELEASE_PACKAGES.join(" or ")}; default ${DAEMON_PACKAGE}`,
  "  SHARNESS_LAUNCHER_VERSION    an exact release to run; disables following the dist-tag",
  "  SHARNESS_LAUNCHER_UPGRADE    on or off; default on",
  "  SHARNESS_LAUNCHER_DIST_TAG   the dist-tag followed; default latest",
  "  SHARNESS_LAUNCHER_REGISTRY   the npm registry origin; default https://registry.npmjs.org",
  "  SHARNESS_LAUNCHER_SEED       the prefix the image installed; default the launcher's own",
  "",
  `Docs: ${LAUNCHER_DOCS}`,
].join("\n");

/** The settings, or a `configuration` failure naming the invalid setting. */
export const loadConfiguration = Config.all(fields).pipe(
  Effect.map(
    (settings): LauncherConfiguration => ({
      directory: settings.directory,
      releasePackage: settings.releasePackage,
      registry: settings.registry,
      selection: selectionOf(settings),
      ...Option.match(settings.seed, {
        onNone: () => ({}),
        onSome: (seed) => ({ seed }),
      }),
    }),
  ),
  Effect.mapError((error) =>
    launcherError(
      "configuration",
      `${configurationMessage(error)}. Docs: ${UPGRADES_GUIDE}#launcher-setting`,
    ),
  ),
);

function selectionOf(settings: {
  readonly version: Option.Option<string>;
  readonly upgrade: "on" | "off";
  readonly distTag: string;
}): Selection {
  return Option.match(settings.version, {
    onSome: (version): Selection => ({ kind: "pinned", version }),
    onNone: (): Selection =>
      settings.upgrade === "off"
        ? { kind: "off" }
        : { kind: "follow", distTag: settings.distTag },
  });
}

function configurationMessage(error: ConfigError.ConfigError): string {
  return Match.value(error).pipe(
    Match.discriminatorsExhaustive("_op")({
      And: (cause) =>
        `${configurationMessage(cause.left)}; ${configurationMessage(cause.right)}`,
      Or: (cause) =>
        `${configurationMessage(cause.left)} or ${configurationMessage(cause.right)}`,
      MissingData: (cause) => `${cause.path.join("_")} is required`,
      InvalidData: (cause) => `${cause.path.join("_")} is invalid`,
      SourceUnavailable: () => "the environment is unavailable",
      Unsupported: () => "the environment is unavailable",
    }),
  );
}

/**
 * Prints usage for `--help`. Otherwise selects a release and prints it as one
 * JSON line for `prepare`, or starts its daemon with no argument. Returns zero
 * for help and preparation, or the daemon's exit status.
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

/** Resolves paths before npm changes the working directory during installation. */
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

/** Defaults to the install prefix containing this launcher's `node_modules`. */
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
      "../..",
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
