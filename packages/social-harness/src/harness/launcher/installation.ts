/**
 * @file The launcher's state on the agent's home volume: the record of the
 * release the agent runs, the verified tarballs that release was installed
 * from, one installed prefix per release, and the `current` link hosts load
 * the channel through. An installed prefix is written once, from verified
 * tarballs, and only read after.
 */

import type { PlatformError } from "@effect/platform/Error";
import {
  Command,
  type CommandExecutor,
  FileSystem,
  Path,
} from "@effect/platform";
import { Effect, Option, Schema, Stream } from "effect";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- Effect FileSystem has no lstat; the walk must identify symlinks without following them.
import { lstat } from "node:fs/promises";
import { launcherError, type LauncherError, UPGRADES_GUIDE } from "./errors.js";
import { integrityOf } from "./integrity.js";
import { DAEMON_PACKAGE } from "./npm.js";

const RECORD_FORMAT = "org.social-harness/launcher-record/v1";

const installedPackage = Schema.Struct({
  name: Schema.String,
  version: Schema.String,
  integrity: Schema.String,
  tarball: Schema.String,
});

/** One package of a verified release, and the cached tarball it came from. */
export type InstalledPackage = typeof installedPackage.Type;

const recordSchema = Schema.Struct({
  format: Schema.Literal(RECORD_FORMAT),
  releasePackage: Schema.String,
  version: Schema.String,
  prefix: Schema.String,
  hostCodeHash: Schema.optional(Schema.String),
  /** Empty for the release the image installed, which the image vouches for. */
  packages: Schema.Array(installedPackage),
});

/** The release an agent runs, as the home volume records it. */
export type LauncherRecord = typeof recordSchema.Type;

/** One verified tarball, ready to cache and install. */
export interface VerifiedTarball {
  readonly name: string;
  readonly version: string;
  readonly integrity: string;
  readonly bytes: Uint8Array;
}

/** A verified release to install. */
export interface ReleaseToInstall {
  readonly releasePackage: string;
  readonly version: string;
  /** Every `@moltzap/*` tarball of the release. */
  readonly tarballs: readonly VerifiedTarball[];
  /** The registry the remaining dependencies come from. */
  readonly registry: string;
}

const RECORD_FILE = "launcher.json";
const LOCK_FILE = "launcher.lock";
const CURRENT_LINK = "current";

const manifestSchema = Schema.Struct({
  name: Schema.String,
  version: Schema.String,
  sharnessHostCodeHash: Schema.optional(Schema.String),
  exports: Schema.optional(Schema.Unknown),
});

type InstalledManifest = typeof manifestSchema.Type;

const clientExports = Schema.Struct({
  ".": Schema.Struct({ import: Schema.String }),
});

const liveWindow = Schema.Struct({
  LIVE_WIRE_VERSIONS: Schema.Array(Schema.String),
});

/** The record in launcher directory `directory`, none, or a `record` failure. */
export const readRecord = Effect.fn("Launcher.readRecord")(function* (
  directory: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const file = path.join(directory, RECORD_FILE);
  const exists = yield* fs
    .exists(file)
    .pipe(Effect.mapError(() => recordFailure(file)));
  if (!exists) {
    return Option.none<LauncherRecord>();
  }
  return yield* fs.readFileString(file).pipe(
    Effect.flatMap(Schema.decodeUnknown(Schema.parseJson(recordSchema))),
    Effect.map(Option.some),
    Effect.mapError(() => recordFailure(file)),
  );
});

/**
 * Replaces the record in `directory` atomically, so a crash leaves the old
 * or the new one, or fails with `record`.
 */
export const writeRecord = Effect.fn("Launcher.writeRecord")(
  function* (directory: string, record: LauncherRecord) {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const file = path.join(directory, RECORD_FILE);
    const temporary = `${file}.tmp`;
    yield* fs.makeDirectory(directory, { recursive: true });
    yield* fs.writeFileString(
      temporary,
      `${JSON.stringify(record, null, 2)}\n`,
    );
    yield* fs.rename(temporary, file);
  },
  Effect.mapError((failure) => launcherError("record", failure.message)),
);

/**
 * The record of the release the image installed under `prefix`, whose
 * `node_modules` holds `releasePackage`, or a `seed` failure.
 */
export const seedRecord = Effect.fn("Launcher.seedRecord")(
  function* (prefix: string, releasePackage: string) {
    const manifest = yield* installedManifest(prefix, releasePackage);
    return recordOf(releasePackage, prefix, manifest, []);
  },
  Effect.mapError((failure) =>
    launcherError(
      "seed",
      `${failure.message}; set SHARNESS_LAUNCHER_PACKAGE to the package the image installed, or SHARNESS_LAUNCHER_SEED to the prefix that holds it. Docs: ${UPGRADES_GUIDE}#launcher-seed`,
    ),
  ),
);

/**
 * Checks, without the network, that every cached tarball of `record` still
 * has the integrity it was verified with and that its prefix holds the
 * recorded release; fails with `integrity` otherwise.
 */
export const checkIntegrity = Effect.fn("Launcher.checkIntegrity")(
  function* (record: LauncherRecord) {
    const fs = yield* FileSystem.FileSystem;
    for (const installed of record.packages) {
      const bytes = yield* fs.readFile(installed.tarball);
      if (integrityOf(bytes) !== installed.integrity) {
        return yield* launcherError(
          "integrity",
          `${installed.tarball} no longer matches ${installed.name}@${installed.version}`,
        );
      }
    }
    const manifest = yield* installedManifest(
      record.prefix,
      record.releasePackage,
    );
    if (manifest.version !== record.version) {
      return yield* launcherError(
        "integrity",
        `${record.prefix} holds ${record.releasePackage}@${manifest.version}, not the recorded ${record.version}`,
      );
    }
  },
  Effect.mapError((failure) => launcherError("integrity", failure.message)),
);

/**
 * Caches the release's tarballs in `directory` and installs them into a new
 * prefix, fetching only their dependencies outside the scope from the
 * registry; fails with `install`.
 */
export const install = Effect.fn("Launcher.install")(
  function* (directory: string, release: ReleaseToInstall) {
    const path = yield* Path.Path;
    const packages = yield* cacheTarballs(
      path.join(directory, "tarballs"),
      release.tarballs,
    );
    const prefix = path.join(
      directory,
      "releases",
      `${fileName(release.releasePackage)}-${release.version}`,
    );
    yield* installInto(prefix, packages, release.registry);
    const fs = yield* FileSystem.FileSystem;
    // eslint-disable-next-line sonarjs/file-permissions -- This directory holds only installed public package code shared with another host user.
    yield* fs.chmod(path.dirname(prefix), 0o755);
    const manifest = yield* installedManifest(prefix, release.releasePackage);
    return recordOf(release.releasePackage, prefix, manifest, packages);
  },
  Effect.mapError((failure) => launcherError("install", failure.message)),
);

/**
 * Holds `directory` for one start for the scope's lifetime, so two
 * launchers on one volume never interleave their reads and writes. A lock
 * left by a process that is gone, or by this process's own PID after a
 * restart, is taken over; one held by a live process fails with `record`.
 */
export const lockDirectory = Effect.fn("Launcher.lockDirectory")(function* (
  directory: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const lock = path.join(directory, LOCK_FILE);
  const exists = yield* fs
    .exists(directory)
    .pipe(Effect.mapError(() => recordFailure(directory)));
  if (!exists) {
    yield* fs
      .makeDirectory(directory, { recursive: true })
      .pipe(Effect.mapError(() => recordFailure(directory)));
    // eslint-disable-next-line sonarjs/file-permissions -- The launcher creates this directory to share public package code with the host user.
    const openCreatedDirectory = fs.chmod(directory, 0o755);
    yield* openCreatedDirectory.pipe(
      Effect.mapError(() => recordFailure(directory)),
    );
  }
  const holder = yield* lockHolder(lock);
  if (holder !== undefined) {
    return yield* launcherError(
      "record",
      `another launcher, PID ${holder}, holds ${lock}`,
    );
  }
  yield* Effect.acquireRelease(
    fs
      .writeFileString(lock, `${process.pid}\n`, { flag: "wx" })
      .pipe(Effect.mapError(() => recordFailure(lock))),
    () => fs.remove(lock, { force: true }).pipe(Effect.ignore),
  );
});

/**
 * Points `current` in `directory` at `prefix` atomically, so a host that
 * loads the channel through it sees one whole release; fails with `record`.
 */
export const pointCurrent = Effect.fn("Launcher.pointCurrent")(
  function* (directory: string, prefix: string) {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const link = path.join(directory, CURRENT_LINK);
    const temporary = `${link}.tmp`;
    yield* fs.remove(temporary, { force: true });
    yield* fs.symlink(prefix, temporary);
    yield* fs.rename(temporary, link);
  },
  Effect.mapError((failure) => launcherError("record", failure.message)),
);

/**
 * Runs after the launcher installs a release, before recording it. Package
 * code contains no secrets and must load from a host running as another user.
 * Symbolic links are left alone: a workspace link may dangle,
 * and following one would change modes outside the release.
 */
export const readableRelease = Effect.fn("Launcher.readableRelease")(
  function* (prefix: string) {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const entries = [
      "",
      ...(yield* fs.readDirectory(prefix, { recursive: true })),
    ];
    for (const entry of entries) {
      const target = path.join(prefix, entry);
      const info = yield* Effect.tryPromise(() => lstat(target));
      if (info.isSymbolicLink()) {
        continue;
      }
      const read =
        info.isDirectory() || (info.mode & 0o111) !== 0 ? 0o555 : 0o444;
      if ((info.mode & read) !== read) {
        yield* fs.chmod(target, info.mode | read);
      }
    }
  },
  Effect.mapError((failure) => launcherError("install", failure.message)),
);

/**
 * Removes every installed prefix and cached tarball in `directory` that
 * `record` does not name, so the volume holds only the release that runs;
 * fails with `record`.
 */
export const pruneReleases = Effect.fn("Launcher.pruneReleases")(
  function* (directory: string, record: LauncherRecord) {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const kept = new Set([
      record.prefix,
      ...record.packages.map((installed) => installed.tarball),
    ]);
    for (const folder of ["releases", "tarballs"]) {
      const parent = path.join(directory, folder);
      const entries = (yield* fs.exists(parent))
        ? yield* fs.readDirectory(parent)
        : [];
      for (const entry of entries) {
        const target = path.join(parent, entry);
        if (!kept.has(target)) {
          yield* fs.remove(target, { recursive: true, force: true });
        }
      }
    }
  },
  Effect.mapError((failure) => launcherError("record", failure.message)),
);

/**
 * The wire versions the daemon installed under `prefix` admits, read from
 * the Client root, or an `integrity` failure.
 */
export const liveWireVersions = Effect.fn("Launcher.liveWireVersions")(
  function* (prefix: string) {
    const path = yield* Path.Path;
    const manifest = yield* installedManifest(prefix, DAEMON_PACKAGE);
    const entry = yield* Schema.decodeUnknown(clientExports)(manifest.exports);
    const module = path.join(
      prefix,
      "node_modules",
      DAEMON_PACKAGE,
      entry["."].import,
    );
    const loaded: unknown = yield* Effect.tryPromise(() => import(module));
    const window = yield* Schema.decodeUnknown(liveWindow)(loaded);
    return window.LIVE_WIRE_VERSIONS;
  },
  Effect.mapError(() =>
    launcherError(
      "integrity",
      "the recorded release holds no readable Client package",
    ),
  ),
);

/**
 * The `sharnessd` executable installed under `prefix`.
 *
 * @param prefix An installed release.
 * @returns The path the launcher runs with the current Node.
 */
export const daemonExecutable = (
  prefix: string,
): Effect.Effect<string, never, Path.Path> =>
  Effect.map(Path.Path, (path) =>
    path.join(prefix, "node_modules", DAEMON_PACKAGE, "bin", "sharnessd"),
  );

function cacheTarballs(
  cache: string,
  tarballs: readonly VerifiedTarball[],
): Effect.Effect<
  readonly InstalledPackage[],
  LauncherError,
  FileSystem.FileSystem | Path.Path
> {
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    yield* fs.makeDirectory(cache, { recursive: true });
    return yield* Effect.forEach(
      tarballs,
      (tarball) => {
        const file = path.join(
          cache,
          `${fileName(tarball.name)}-${tarball.version}.tgz`,
        );
        return writeAtomically(file, tarball.bytes).pipe(
          Effect.as({
            name: tarball.name,
            version: tarball.version,
            integrity: tarball.integrity,
            tarball: file,
          }),
        );
      },
      { concurrency: 1 },
    );
  }).pipe(
    Effect.mapError(() => launcherError("install", `could not write ${cache}`)),
  );
}

/** Installs `packages` into a staging directory, then moves it to `prefix`. */
function installInto(
  prefix: string,
  packages: readonly InstalledPackage[],
  registry: string,
): Effect.Effect<
  void,
  LauncherError,
  FileSystem.FileSystem | CommandExecutor.CommandExecutor
> {
  const staging = `${prefix}.partial`;
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    yield* fs.remove(staging, { recursive: true, force: true });
    yield* fs.makeDirectory(staging, { recursive: true });
    yield* fs.writeFileString(
      `${staging}/package.json`,
      `${JSON.stringify({ private: true })}\n`,
    );
    const exitCode = yield* npmInstall(staging, packages, registry);
    if (exitCode !== 0) {
      return yield* launcherError(
        "install",
        `npm install exited ${exitCode} in ${staging}`,
      );
    }
    yield* fs.remove(prefix, { recursive: true, force: true });
    yield* fs.rename(staging, prefix);
  }).pipe(
    Effect.catchTags({
      BadArgument: () => Effect.fail(installFailure(prefix)),
      SystemError: () => Effect.fail(installFailure(prefix)),
    }),
  );
}

/**
 * Runs `npm install` of the cached tarballs in `staging`. Install scripts
 * never run; npm's progress output is dropped and its errors reach stderr.
 */
function npmInstall(
  staging: string,
  packages: readonly InstalledPackage[],
  registry: string,
): Effect.Effect<number, LauncherError, CommandExecutor.CommandExecutor> {
  return Effect.scoped(
    Effect.gen(function* () {
      const npm = yield* Command.make(
        "npm",
        "install",
        "--ignore-scripts",
        "--no-audit",
        "--no-fund",
        "--no-progress",
        "--omit=dev",
        `--registry=${registry}`,
        ...packages.map((installed) => installed.tarball),
      ).pipe(
        Command.workingDirectory(staging),
        Command.stderr("inherit"),
        Command.start,
      );
      const [, code] = yield* Effect.all(
        [Stream.runDrain(npm.stdout), npm.exitCode],
        { concurrency: 2 },
      );
      return code;
    }),
  ).pipe(
    Effect.mapError(() =>
      launcherError("install", `npm install could not run in ${staging}`),
    ),
  );
}

function installedManifest(
  prefix: string,
  name: string,
): Effect.Effect<
  InstalledManifest,
  LauncherError,
  FileSystem.FileSystem | Path.Path
> {
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const file = path.join(prefix, "node_modules", name, "package.json");
    const refuse = launcherError(
      "install",
      `${file} is not a readable ${name} manifest`,
    );
    const manifest = yield* fs.readFileString(file).pipe(
      Effect.flatMap(Schema.decodeUnknown(Schema.parseJson(manifestSchema))),
      Effect.mapError(() => refuse),
    );
    return manifest.name === name ? manifest : yield* refuse;
  });
}

function recordOf(
  releasePackage: string,
  prefix: string,
  manifest: InstalledManifest,
  packages: readonly InstalledPackage[],
): LauncherRecord {
  return {
    format: RECORD_FORMAT,
    releasePackage,
    version: manifest.version,
    prefix,
    ...(manifest.sharnessHostCodeHash === undefined
      ? {}
      : { hostCodeHash: manifest.sharnessHostCodeHash }),
    packages,
  };
}

/**
 * Writes `bytes` to `file` through a temporary file and a rename, so a
 * tarball another recorded release names is never seen half written.
 */
function writeAtomically(
  file: string,
  bytes: Uint8Array,
): Effect.Effect<void, PlatformError, FileSystem.FileSystem> {
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const temporary = `${file}.partial`;
    yield* fs.writeFile(temporary, bytes);
    yield* fs.rename(temporary, file);
  });
}

function fileName(name: string): string {
  return name.replace("@", "").replace("/", "-");
}

function installFailure(prefix: string): LauncherError {
  return launcherError("install", `could not install into ${prefix}`);
}

/** The PID of a live process other than this one holding `lock`, after removing a stale lock. */
function lockHolder(
  lock: string,
): Effect.Effect<string | undefined, LauncherError, FileSystem.FileSystem> {
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    if (!(yield* fs.exists(lock))) {
      return undefined;
    }
    const holder = (yield* fs.readFileString(lock)).trim();
    if (holder !== String(process.pid) && processIsAlive(holder)) {
      return holder;
    }
    yield* fs.remove(lock, { force: true });
    return undefined;
  }).pipe(Effect.mapError(() => recordFailure(lock)));
}

function processIsAlive(pid: string): boolean {
  const id = Number(pid);
  if (!Number.isInteger(id) || id <= 0) {
    return false;
  }
  try {
    process.kill(id, 0);
    return true;
  } catch (signalError: unknown) {
    return (
      signalError instanceof Error &&
      "code" in signalError &&
      signalError.code === "EPERM"
    );
  }
}

function recordFailure(file: string): LauncherError {
  return launcherError("record", `${file} could not be read`);
}
