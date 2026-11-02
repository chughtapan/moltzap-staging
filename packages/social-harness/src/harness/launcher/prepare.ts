/**
 * @file One launcher start: read or seed the record, check it offline,
 * resolve the candidate release, take it only when the selection rules,
 * provenance and installation all succeed, and record the release that runs.
 * Any failure on the way to a candidate keeps the recorded release; only a
 * recorded release that fails its own checks stops the start.
 */

import {
  type CommandExecutor,
  FileSystem,
  type HttpClient,
  Path,
} from "@effect/platform";
import { Console, Effect, Option } from "effect";
import {
  LAUNCHER_DOCS,
  launcherError,
  type LauncherError,
  UPGRADES_GUIDE,
} from "./errors.js";
import {
  checkIntegrity,
  daemonExecutable,
  install,
  type LauncherRecord,
  liveWireVersions,
  lockDirectory,
  pointCurrent,
  pruneReleases,
  readableRelease,
  readRecord,
  seedRecord,
  writeRecord,
} from "./installation.js";
import {
  DAEMON_PACKAGE,
  download,
  type PublishedPackage,
  releaseClosure,
  taggedVersion,
} from "./npm.js";
import { verifyProvenance } from "./provenance.js";
import {
  choose,
  type KeepReason,
  type LauncherConfiguration,
} from "./selection.js";

/** The release one start runs, as `prepare` prints it. */
export interface Prepared {
  readonly releasePackage: string;
  readonly version: string;
  readonly prefix: string;
  readonly daemon: string;
  /** The release that ran before this start, when the start took a new one. */
  readonly previous?: string;
  /** Why the recorded release runs, when it does. */
  readonly kept?: KeepReason;
  /** Whether the code OpenClaw loads for the channel changed, so the gateway restarts. */
  readonly restartHost: boolean;
}

/** The services a start uses. */
export type LauncherServices =
  | FileSystem.FileSystem
  | Path.Path
  | HttpClient.HttpClient
  | CommandExecutor.CommandExecutor;

type Outcome =
  | { readonly kind: "keep"; readonly reason: KeepReason }
  | { readonly kind: "take"; readonly record: LauncherRecord };

/** The keep reason each candidate failure reports. */
const KEEP_REASONS: Readonly<Record<LauncherError["reason"], KeepReason>> = {
  provenance: "unverified",
  install: "uninstalled",
  integrity: "uninstalled",
  configuration: "unresolved",
  seed: "unresolved",
  record: "unresolved",
  registry: "unresolved",
  start: "unresolved",
};

/**
 * Runs on every start to choose, install and record its release, given the
 * launcher's settings and the prefix the image installed, holding the
 * launcher directory for the scope. It fails only when nothing is safe to
 * run. Only a newly installed release is made readable; image seeds supply
 * their own readable modes. The `current` link moves before the record commits,
 * so a start interrupted between them takes the release again and still
 * reports `restartHost`.
 */
export const prepare = Effect.fn("Launcher.prepare")(function* (
  configuration: LauncherConfiguration,
  seed: string,
) {
  yield* lockDirectory(configuration.directory);
  const recorded = yield* recordedRelease(configuration, seed);
  yield* checkIntegrity(recorded);
  const outcome = yield* candidate(configuration, recorded).pipe(
    Effect.catchAll((failure) =>
      Console.error(`sharnessd-launcher: ${failure.message}`).pipe(
        Effect.as<Outcome>({
          kind: "keep",
          reason: KEEP_REASONS[failure.reason],
        }),
      ),
    ),
  );
  const running = outcome.kind === "take" ? outcome.record : recorded;
  yield* pointCurrent(configuration.directory, running.prefix);
  yield* writeRecord(configuration.directory, running);
  if (outcome.kind === "take") {
    yield* pruneReleases(configuration.directory, running);
  }
  const prepared: Prepared = {
    releasePackage: running.releasePackage,
    version: running.version,
    prefix: running.prefix,
    daemon: yield* daemonExecutable(running.prefix),
    restartHost:
      outcome.kind === "take" && running.hostCodeHash !== recorded.hostCodeHash,
    ...(outcome.kind === "take"
      ? { previous: recorded.version }
      : { kept: outcome.reason }),
  };
  return prepared;
});

/**
 * The release the agent last ran. A record of the image's own install, which
 * caches no tarball, follows the image: a new image installs another release
 * under the same prefix, so the start records that one again instead of
 * refusing the prefix it no longer matches. A recorded prefix must contain
 * the daemon package before candidate failures can keep it running.
 */
function recordedRelease(
  configuration: LauncherConfiguration,
  seed: string,
): Effect.Effect<
  LauncherRecord,
  LauncherError,
  FileSystem.FileSystem | Path.Path
> {
  return readRecord(configuration.directory).pipe(
    Effect.flatMap(
      Option.match({
        onNone: () => seedRecord(seed, configuration.releasePackage),
        onSome: (record) =>
          Effect.gen(function* () {
            if (record.releasePackage !== configuration.releasePackage) {
              return yield* Effect.fail(
                launcherError(
                  "configuration",
                  `the recorded release follows ${record.releasePackage}, not ${configuration.releasePackage}; set SHARNESS_LAUNCHER_PACKAGE back to ${record.releasePackage}, or start with an empty launcher directory. Docs: ${UPGRADES_GUIDE}#launcher-package`,
                ),
              );
            }
            const fs = yield* FileSystem.FileSystem;
            const path = yield* Path.Path;
            const failure = launcherError(
              "configuration",
              `the recorded release in launcher directory ${configuration.directory} contains no daemon package (${DAEMON_PACKAGE}); start with an empty launcher directory. Docs: ${LAUNCHER_DOCS}`,
            );
            const installed = yield* fs
              .exists(
                path.join(
                  record.prefix,
                  "node_modules",
                  DAEMON_PACKAGE,
                  "package.json",
                ),
              )
              .pipe(Effect.mapError(() => failure));
            if (!installed) {
              return yield* failure;
            }
            return yield* record.prefix === seed && record.packages.length === 0
              ? seedRecord(seed, configuration.releasePackage)
              : Effect.succeed(record);
          }),
      }),
    ),
  );
}

/** The candidate release, decided against the recorded one and taken when it passes. */
function candidate(
  configuration: LauncherConfiguration,
  recorded: LauncherRecord,
): Effect.Effect<Outcome, LauncherError, LauncherServices> {
  return Effect.gen(function* () {
    const { selection, registry, releasePackage } = configuration;
    if (selection.kind === "off") {
      return { kind: "keep", reason: "upgrade-off" } as const;
    }
    const version =
      selection.kind === "pinned"
        ? selection.version
        : yield* taggedVersion(registry, releasePackage, selection.distTag);
    const closure = yield* releaseClosure(registry, releasePackage, version);
    if (!closure.some((entry) => entry.name === DAEMON_PACKAGE)) {
      return yield* launcherError(
        "registry",
        `release ${version} contains no daemon package (${DAEMON_PACKAGE}). Docs: ${UPGRADES_GUIDE}#release-contains-no-daemon-package`,
      );
    }
    const choice = choose(
      {
        version: recorded.version,
        liveWireVersions:
          selection.kind === "follow"
            ? yield* liveWireVersions(recorded.prefix)
            : [],
      },
      {
        version,
        wireVersion:
          selection.kind === "follow"
            ? yield* wireVersionOf(closure, version)
            : "",
      },
      selection,
    );
    if (choice.kind === "keep") {
      return choice;
    }
    return {
      kind: "take",
      record: yield* take(configuration, version, closure),
    } as const;
  });
}

/** The daemon manifest supplies the candidate wire version. */
function wireVersionOf(
  closure: readonly PublishedPackage[],
  version: string,
): Effect.Effect<string, LauncherError> {
  const daemon = closure.find((entry) => entry.name === DAEMON_PACKAGE);
  const wireVersion = daemon?.wireVersion;
  return wireVersion !== undefined
    ? Effect.succeed(wireVersion)
    : Effect.fail(
        launcherError(
          "registry",
          `release ${version} contains no wire version in its Client manifest. Docs: ${UPGRADES_GUIDE}#release-contains-no-wire-version`,
        ),
      );
}

/** Downloads and verifies every package of the release, then installs and checks it. */
function take(
  configuration: LauncherConfiguration,
  version: string,
  closure: readonly PublishedPackage[],
): Effect.Effect<LauncherRecord, LauncherError, LauncherServices> {
  return Effect.gen(function* () {
    const tarballs = yield* Effect.forEach(
      closure,
      (published) =>
        download(published.tarball).pipe(
          Effect.tap((bytes) =>
            verifyProvenance(published, bytes, {
              tufCachePath: `${configuration.directory}/sigstore`,
            }),
          ),
          Effect.map((bytes) => ({
            name: published.name,
            version: published.version,
            integrity: published.integrity,
            bytes,
          })),
        ),
      { concurrency: 4 },
    );
    const record = yield* install(configuration.directory, {
      releasePackage: configuration.releasePackage,
      version,
      tarballs,
      registry: configuration.registry,
    });
    yield* checkIntegrity(record);
    yield* readableRelease(record.prefix);
    return record;
  });
}
