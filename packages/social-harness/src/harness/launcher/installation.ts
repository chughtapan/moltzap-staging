/** @file Downloads, verifies and installs the release selected for an agent. */
import type { PlatformError } from "@effect/platform/Error";
import {
  Command,
  type CommandExecutor,
  FileSystem,
  HttpClient,
  type HttpClientResponse,
  Path,
} from "@effect/platform";
import {
  bundleFromJSON,
  bundleToJSON,
  isBundleWithDsseEnvelope,
} from "@sigstore/bundle";
import { Console, Data, Effect, Option, Schema, Stream } from "effect";
import { createHash } from "node:crypto";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- The installation walk needs lstat to inspect symlinks without following them.
import { lstat } from "node:fs/promises";
import { gunzipSync } from "node:zlib";
import { verify } from "sigstore";

/**
 * The certificate subject alternative name of the one release workflow the
 * launcher trusts.
 */
const RELEASE_IDENTITY =
  "https://github.com/social-harness/moltzap/.github/workflows/publish.yml@refs/heads/main";
const CALENDAR_VERSION = /^(\d+)\.(\d+)\.(\d+)$/u;

/** Settings and release selection referenced by launcher help and failures. */
export const LAUNCHER_DOCS =
  "https://docs.moltzap.xyz/reference/services/daemon#launcher";

/** Startup failures link to this guide's diagnostic anchors. */
export const UPGRADES_GUIDE = "https://docs.moltzap.xyz/guides/upgrades";

/**
 * Registry and provenance failures keep the recorded release. Other failures
 * stop startup.
 */
export class LauncherError extends Data.TaggedError("LauncherError")<{
  readonly reason:
    | "configuration"
    | "seed"
    | "record"
    | "integrity"
    | "registry"
    | "provenance"
    | "install"
    | "start";
  readonly message: string;
}> {}

/**
 * Messages may include paths and versions. Keep file contents out of them.
 */
export const launcherError = (
  reason: LauncherError["reason"],
  message: string,
): LauncherError => new LauncherError({ reason, message });

/**
 * The SRI `sha512-` integrity of `bytes`, as npm records it.
 *
 * @param bytes A tarball.
 * @returns Its integrity.
 */
export function integrityOf(bytes: Uint8Array): string {
  return `sha512-${createHash("sha512").update(bytes).digest("base64")}`;
}

/** The manifest fields the launcher compares with the registry's metadata. */
const packedManifest = Schema.Struct({
  name: Schema.String,
  version: Schema.String,
  dependencies: Schema.optional(
    Schema.Record({ key: Schema.String, value: Schema.String }),
  ),
  sharnessHostCodeHash: Schema.optional(Schema.String),
  sharnessWireVersion: Schema.optional(Schema.String),
});

/** The fields of a packed manifest the launcher reads. */
export type PackedManifest = typeof packedManifest.Type;

const MANIFEST_PATH = "package/package.json";

const BLOCK = 512;

/**
 * The `package/package.json` an npm tarball holds.
 *
 * @param bytes A gzipped npm tarball.
 * @returns Its manifest, or a `provenance` failure for a tarball without a
 *   readable one.
 */
export const readPackedManifest = (
  bytes: Uint8Array,
): Effect.Effect<PackedManifest, LauncherError> =>
  Effect.try({
    try: () => manifestText(gunzipSync(bytes)),
    catch: () =>
      launcherError("provenance", "the tarball is not a gzipped tar"),
  }).pipe(
    Effect.flatMap((text) =>
      text === undefined
        ? Effect.fail(
            launcherError(
              "provenance",
              `the tarball holds no ${MANIFEST_PATH}`,
            ),
          )
        : Schema.decodeUnknown(Schema.parseJson(packedManifest))(text).pipe(
            Effect.mapError(() =>
              launcherError(
                "provenance",
                `the tarball's ${MANIFEST_PATH} is unreadable`,
              ),
            ),
          ),
    ),
  );

/**
 * Decides whether a start takes `candidate` over `recorded`. Following a
 * dist-tag takes only a newer release whose emitted wire version the
 * recorded daemon already admits, so a release outside the live window, which
 * every reset release is, never applies on its own and no start moves an
 * agent backwards. An exact pin is the operator's choice in either direction
 * and across a wire change; the endpoint store still refuses a schema newer
 * than the pinned daemon creates.
 *
 * @param recorded The release the agent ran last.
 * @param candidate The release the selection resolved to.
 * @param selection How the release is chosen.
 * @returns Whether to take the candidate, or why the recorded release runs.
 */
export function choose(
  recorded: RecordedRelease,
  candidate: CandidateRelease,
  selection: Exclude<Selection, { readonly kind: "off" }>,
): Choice {
  const order = compareVersions(candidate.version, recorded.version);
  if (order === undefined) {
    return { kind: "keep", reason: "unresolved" };
  }
  if (order === 0) {
    return { kind: "keep", reason: "current" };
  }
  if (selection.kind === "pinned") {
    return { kind: "take" };
  }
  if (order < 0) {
    return { kind: "keep", reason: "older" };
  }
  return recorded.liveWireVersions.includes(candidate.wireVersion)
    ? { kind: "take" }
    : { kind: "keep", reason: "outside-wire-window" };
}

/**
 * Orders two calendar versions `YYYY.MDD.N` by their numeric parts.
 *
 * @param left One version.
 * @param right The other version.
 * @returns A negative number, zero or a positive number as `left` is older
 *   than, equal to or newer than `right`; `undefined` when either is not a
 *   calendar version.
 */
export function compareVersions(
  left: string,
  right: string,
): number | undefined {
  const a = CALENDAR_VERSION.exec(left);
  const b = CALENDAR_VERSION.exec(right);
  if (a === null || b === null) {
    return undefined;
  }
  for (const index of [1, 2, 3]) {
    const difference = Number(a[index]) - Number(b[index]);
    if (difference !== 0) {
      return difference;
    }
  }
  return 0;
}

/** The text of the archive's manifest entry, walking ustar headers. */
function manifestText(archive: Uint8Array): string | undefined {
  const decoder = new TextDecoder();
  let offset = 0;
  while (offset + BLOCK <= archive.length) {
    const header = archive.subarray(offset, offset + BLOCK);
    const name = field(decoder, header, 0, 100);
    if (name === "") {
      return undefined;
    }
    const prefix = field(decoder, header, 345, 155);
    const size = Number.parseInt(field(decoder, header, 124, 12).trim(), 8);
    const start = offset + BLOCK;
    const path = prefix === "" ? name : `${prefix}/${name}`;
    if (path === MANIFEST_PATH && header[156] !== 0x78) {
      return decoder.decode(archive.subarray(start, start + size));
    }
    offset = start + Math.ceil(size / BLOCK) * BLOCK;
  }
  return undefined;
}

function field(
  decoder: TextDecoder,
  header: Uint8Array,
  start: number,
  length: number,
): string {
  const bytes = header.subarray(start, start + length);
  const end = bytes.indexOf(0);
  return decoder.decode(end === -1 ? bytes : bytes.subarray(0, end));
}

/** The scope every Social Harness package publishes under. */
const SCOPE = "@moltzap/";

/** The package that carries `sharnessd`. */
export const DAEMON_PACKAGE = "@moltzap/social-harness";

/** One published package version, with the fields the launcher reads. */
interface PublishedPackage {
  readonly name: string;
  readonly version: string;
  /** The SRI `sha512-` value npm records as `dist.integrity`. */
  readonly integrity: string;
  readonly tarball: string;
  readonly attestations?: string;
  /** The exact versions of the `@moltzap/*` packages it depends on. */
  readonly siblings: Readonly<Record<string, string>>;
  readonly hostCodeHash?: string;
  readonly wireVersion?: string;
}

/** A package name and the exact version a release pins it to. */
type Pin = readonly [name: string, version: string];

const manifest = Schema.Struct({
  name: Schema.String,
  version: Schema.String,
  dist: Schema.Struct({
    integrity: Schema.String.pipe(Schema.startsWith("sha512-")),
    tarball: Schema.String,
    attestations: Schema.optional(Schema.Struct({ url: Schema.String })),
  }),
  dependencies: Schema.optional(
    Schema.Record({ key: Schema.String, value: Schema.String }),
  ),
  sharnessHostCodeHash: Schema.optional(Schema.String),
  sharnessWireVersion: Schema.optional(Schema.String),
});

const distTags = Schema.Record({ key: Schema.String, value: Schema.String });

/**
 * The names and versions a release may pin. Both reach file names on the
 * home volume, so anything else is refused before it is used.
 */
const PACKAGE_NAME = /^@moltzap\/[a-z0-9][a-z0-9-]*$/u;

/** How long one registry read, body included, may take. */
const READ_TIMEOUT = "60 seconds";

/**
 * The version `tag` names for `name` on the registry at origin `registry`,
 * or a `registry` failure.
 */
const taggedVersion = Effect.fn("Launcher.taggedVersion")(function* (
  registry: string,
  name: string,
  tag: string,
) {
  const tags = yield* readJson(
    `${registry}/-/package/${encodedName(name)}/dist-tags`,
    distTags,
  );
  const version = tags[tag];
  if (version === undefined || !CALENDAR_VERSION.test(version)) {
    return yield* launcherError(
      "registry",
      `${name} has no calendar version on dist-tag ${tag}`,
    );
  }
  return version;
});

/**
 * The published manifest of `name@version`, or a `registry` failure when
 * the registry answers another package or version.
 */
const publishedPackage = Effect.fn("Launcher.publishedPackage")(function* (
  registry: string,
  name: string,
  version: string,
) {
  const published = yield* readJson(
    `${registry}/${encodedName(name)}/${version}`,
    manifest,
  );
  if (published.name !== name || published.version !== version) {
    return yield* launcherError(
      "registry",
      `the registry answered ${published.name}@${published.version} for ${name}@${version}`,
    );
  }
  const result = toPublished(published);
  const unexpected = Object.entries(result.siblings).find(
    ([sibling, pin]) =>
      !PACKAGE_NAME.test(sibling) || !CALENDAR_VERSION.test(pin),
  );
  if (unexpected !== undefined) {
    return yield* launcherError(
      "registry",
      `${name}@${version} pins ${unexpected[0]} to ${unexpected[1]}, not a calendar version of a scoped package`,
    );
  }
  return result;
});

/**
 * Every package the release `name@version` installs from this scope: the
 * release package first, then each exact sibling pin, transitively. Two
 * different pins of one package fail with `registry`.
 */
const releaseClosure = Effect.fn("Launcher.releaseClosure")(function* (
  registry: string,
  name: string,
  version: string,
) {
  const found = new Map<string, PublishedPackage>();
  let pending: readonly Pin[] = [[name, version]];
  while (pending.length > 0) {
    pending = yield* visit(registry, found, pending);
  }
  return [...found.values()];
});

/**
 * The bytes at `url`, or a `registry` failure.
 *
 * @param url A tarball URL.
 * @returns The bytes.
 */
const download = (
  url: string,
): Effect.Effect<Uint8Array, LauncherError, HttpClient.HttpClient> =>
  fetchOk(url).pipe(
    Effect.flatMap((response) => response.arrayBuffer),
    Effect.map((buffer) => new Uint8Array(buffer)),
    Effect.mapError(() =>
      launcherError("registry", `could not download ${url}`),
    ),
    withinTimeout(url),
  );

/**
 * The JSON at `url`, unvalidated, or a `registry` failure.
 *
 * @param url An attestations or metadata URL.
 * @returns The parsed body.
 */
const downloadJson = (
  url: string,
): Effect.Effect<unknown, LauncherError, HttpClient.HttpClient> =>
  fetchOk(url).pipe(
    Effect.flatMap((response) => response.json),
    Effect.mapError(() => launcherError("registry", `could not read ${url}`)),
    withinTimeout(url),
  );

function visit(
  registry: string,
  found: Map<string, PublishedPackage>,
  pending: readonly Pin[],
): Effect.Effect<readonly Pin[], LauncherError, HttpClient.HttpClient> {
  return Effect.forEach(
    pending,
    ([name, version]) => {
      const known = found.get(name);
      if (known === undefined) {
        return publishedPackage(registry, name, version).pipe(
          Effect.tap((published) =>
            Effect.sync(() => found.set(name, published)),
          ),
          Effect.map((published): readonly Pin[] =>
            Object.entries(published.siblings),
          ),
        );
      }
      return known.version === version
        ? Effect.succeed<readonly Pin[]>([])
        : Effect.fail(
            launcherError(
              "registry",
              `the release pins both ${name}@${known.version} and ${name}@${version}`,
            ),
          );
    },
    { concurrency: 4 },
  ).pipe(Effect.map((pins) => pins.flat()));
}

function toPublished(published: typeof manifest.Type): PublishedPackage {
  const wireVersion = published.sharnessWireVersion;
  return {
    name: published.name,
    version: published.version,
    integrity: published.dist.integrity,
    tarball: published.dist.tarball,
    siblings: Object.fromEntries(
      Object.entries(published.dependencies ?? {}).filter(([key]) =>
        key.startsWith(SCOPE),
      ),
    ),
    ...(published.dist.attestations === undefined
      ? {}
      : { attestations: published.dist.attestations.url }),
    ...(published.sharnessHostCodeHash === undefined
      ? {}
      : { hostCodeHash: published.sharnessHostCodeHash }),
    ...(wireVersion === undefined ? {} : { wireVersion }),
  };
}

function readJson<A, I>(
  url: string,
  schema: Schema.Schema<A, I>,
): Effect.Effect<A, LauncherError, HttpClient.HttpClient> {
  return downloadJson(url).pipe(
    Effect.flatMap((body) =>
      Schema.decodeUnknown(schema)(body).pipe(
        Effect.mapError(() =>
          launcherError("registry", `${url} answered an unexpected document`),
        ),
      ),
    ),
  );
}

function fetchOk(
  url: string,
): Effect.Effect<
  HttpClientResponse.HttpClientResponse,
  LauncherError,
  HttpClient.HttpClient
> {
  return HttpClient.get(url).pipe(
    Effect.mapError(() => launcherError("registry", `could not reach ${url}`)),
    Effect.flatMap((response) =>
      response.status === 200
        ? Effect.succeed(response)
        : Effect.fail(
            launcherError("registry", `${url} answered ${response.status}`),
          ),
    ),
  );
}

function withinTimeout(url: string) {
  return <A, R>(
    read: Effect.Effect<A, LauncherError, R>,
  ): Effect.Effect<A, LauncherError, R> =>
    read.pipe(
      Effect.timeoutFail({
        duration: READ_TIMEOUT,
        onTimeout: () => launcherError("registry", `${url} timed out`),
      }),
    );
}

function encodedName(name: string): string {
  return name.replace("/", "%2f");
}

/** The release the agent ran last, with the wire versions its daemon admits. */
export interface RecordedRelease {
  readonly version: string;
  readonly liveWireVersions: readonly string[];
}

/** A published release, with the wire version its daemon emits. */
export interface CandidateRelease {
  readonly version: string;
  readonly wireVersion: string;
}

/** Why a start keeps the recorded release. */
type KeepReason =
  | "current"
  | "older"
  | "outside-wire-window"
  | "upgrade-off"
  | "unresolved"
  | "unverified"
  | "uninstalled";

/** What one start runs. */
export type Choice =
  | { readonly kind: "keep"; readonly reason: KeepReason }
  | { readonly kind: "take" };

/** The packages a launcher may follow: the daemon alone, or the OpenClaw channel and the daemon it pins. */
export type ReleasePackage =
  | "@moltzap/social-harness"
  | "@moltzap/openclaw-channel";

/** The launcher's settings. */
export interface LauncherConfiguration {
  readonly directory: string;
  readonly releasePackage: ReleasePackage;
  readonly selection: Selection;
  readonly registry: string;
  /** The prefix the image installed, when it is not the launcher's own. */
  readonly seed?: string;
}

/** How the release to run is chosen. */
export type Selection =
  | { readonly kind: "follow"; readonly distTag: string }
  | { readonly kind: "pinned"; readonly version: string }
  | { readonly kind: "off" };

/** The OIDC issuer of GitHub Actions workload certificates. */
const RELEASE_ISSUER = "https://token.actions.githubusercontent.com";

const SLSA_PROVENANCE = "https://slsa.dev/provenance/v1";

const attestations = Schema.Struct({
  attestations: Schema.Array(
    Schema.Struct({ predicateType: Schema.String, bundle: Schema.Unknown }),
  ),
});

const statement = Schema.Struct({
  subject: Schema.Array(
    Schema.Struct({
      name: Schema.String,
      digest: Schema.Struct({ sha512: Schema.String }),
    }),
  ),
});

/** Where Sigstore keeps its trust root between starts. */
interface ProvenanceOptions {
  readonly tufCachePath: string;
}

/**
 * Checks that `bytes` are the tarball npm recorded for `published` and that
 * its SLSA provenance, signed for {@link RELEASE_IDENTITY}, covers them;
 * fails with `provenance` naming the package otherwise.
 */
const verifyProvenance = Effect.fn("Launcher.verifyProvenance")(function* (
  published: PublishedPackage,
  bytes: Uint8Array,
  options: ProvenanceOptions,
) {
  const refuse = refusal(published);
  if (integrityOf(bytes) !== published.integrity) {
    return yield* refuse("the tarball differs from the integrity npm recorded");
  }
  if (published.attestations === undefined) {
    return yield* refuse("npm holds no attestations");
  }
  const payload = yield* signedPayload(published.attestations, options, refuse);
  const subject = purl(published.name, published.version);
  const digest = createHash("sha512").update(bytes).digest("hex");
  const covered = payload.subject.some(
    (entry) => entry.name === subject && entry.digest.sha512 === digest,
  );
  if (!covered) {
    return yield* refuse(
      `its provenance does not cover ${subject} with these bytes`,
    );
  }
  const packed = yield* readPackedManifest(bytes).pipe(
    Effect.mapError((failure) => refuse(failure.message)),
  );
  const differing = differingField(published, packed);
  if (differing !== undefined) {
    return yield* refuse(
      `the registry's ${differing} differs from the signed package's`,
    );
  }
});

/**
 * The first field the registry reported for a package that its signed
 * manifest does not carry, so a registry cannot drop a sibling pin from the
 * verified closure or report another wire version or host-code hash.
 */
function differingField(
  published: PublishedPackage,
  packed: PackedManifest,
): string | undefined {
  const siblings = Object.entries(packed.dependencies ?? {}).filter(([name]) =>
    name.startsWith("@moltzap/"),
  );
  const checks: ReadonlyArray<readonly [string, boolean]> = [
    ["name", packed.name === published.name],
    ["version", packed.version === published.version],
    [
      "sibling pins",
      JSON.stringify(sorted(siblings)) ===
        JSON.stringify(sorted(Object.entries(published.siblings))),
    ],
    ["host-code hash", packed.sharnessHostCodeHash === published.hostCodeHash],
    ["wire version", packed.sharnessWireVersion === published.wireVersion],
  ];
  return checks.find(([, same]) => !same)?.[0];
}

function sorted(
  entries: ReadonlyArray<readonly [string, string]>,
): ReadonlyArray<readonly [string, string]> {
  return [...entries].sort(([a], [b]) => a.localeCompare(b));
}

/** The provenance statement at `url`, once its signature verifies. */
function signedPayload(
  url: string,
  options: ProvenanceOptions,
  refuse: (why: string) => LauncherError,
): Effect.Effect<typeof statement.Type, LauncherError, HttpClient.HttpClient> {
  return Effect.gen(function* () {
    const listed = yield* downloadJson(url).pipe(
      Effect.flatMap(Schema.decodeUnknown(attestations)),
      Effect.mapError(() => refuse("its attestations could not be read")),
    );
    const provenance = listed.attestations.find(
      (attestation) => attestation.predicateType === SLSA_PROVENANCE,
    );
    if (provenance === undefined) {
      return yield* refuse("npm holds no SLSA provenance");
    }
    const bundle = yield* Effect.try({
      try: () => bundleFromJSON(provenance.bundle),
      catch: () => refuse("its provenance bundle is malformed"),
    });
    if (!isBundleWithDsseEnvelope(bundle)) {
      return yield* refuse("its provenance carries no envelope");
    }
    yield* Effect.tryPromise({
      try: () =>
        verify(bundleToJSON(bundle), {
          certificateIssuer: RELEASE_ISSUER,
          certificateIdentityURI: RELEASE_IDENTITY,
          tufCachePath: options.tufCachePath,
        }),
      catch: () =>
        refuse(`its provenance is not signed by ${RELEASE_IDENTITY}`),
    });
    return yield* Schema.decodeUnknown(Schema.parseJson(statement))(
      bundle.content.dsseEnvelope.payload.toString("utf8"),
    ).pipe(
      Effect.mapError(() => refuse("its provenance statement is malformed")),
    );
  });
}

function refusal(published: PublishedPackage): (why: string) => LauncherError {
  return (why) =>
    launcherError(
      "provenance",
      `${published.name}@${published.version}: ${why}`,
    );
}

function purl(name: string, version: string): string {
  return `pkg:npm/${name.replace("@", "%40")}@${version}`;
}

const RECORD_FORMAT = "org.social-harness/launcher-record/v1";

const installedPackage = Schema.Struct({
  name: Schema.String,
  version: Schema.String,
  integrity: Schema.String,
  tarball: Schema.String,
});

/** One package of a verified release, and the cached tarball it came from. */
type InstalledPackage = typeof installedPackage.Type;

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
interface VerifiedTarball {
  readonly name: string;
  readonly version: string;
  readonly integrity: string;
  readonly bytes: Uint8Array;
}

/** A verified release to install. */
interface ReleaseToInstall {
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
const seedRecord = Effect.fn("Launcher.seedRecord")(
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
const checkIntegrity = Effect.fn("Launcher.checkIntegrity")(
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
const install = Effect.fn("Launcher.install")(
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
const lockDirectory = Effect.fn("Launcher.lockDirectory")(function* (
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
const pointCurrent = Effect.fn("Launcher.pointCurrent")(
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
const readableRelease = Effect.fn("Launcher.readableRelease")(
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
const pruneReleases = Effect.fn("Launcher.pruneReleases")(
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
const liveWireVersions = Effect.fn("Launcher.liveWireVersions")(
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
const daemonExecutable = (
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
