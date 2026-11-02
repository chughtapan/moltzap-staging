/**
 * @file Reads the npm registry for one release: the version a dist-tag
 * names, each published manifest of its `@moltzap/*` closure, the tarballs,
 * and their attestation bundles. Nothing here trusts what it reads; the
 * provenance check does.
 */

import { HttpClient, type HttpClientResponse } from "@effect/platform";
import { Effect, Schema } from "effect";
import { launcherError, type LauncherError } from "./errors.js";

/** The scope every Social Harness package publishes under. */
const SCOPE = "@moltzap/";

/** The package that carries `sharnessd`. */
export const DAEMON_PACKAGE = "@moltzap/social-harness";

/** One published package version, with the fields the launcher reads. */
export interface PublishedPackage {
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
const CALENDAR_VERSION = /^\d+\.\d+\.\d+$/u;

/** How long one registry read, body included, may take. */
const READ_TIMEOUT = "60 seconds";

/**
 * The version `tag` names for `name` on the registry at origin `registry`,
 * or a `registry` failure.
 */
export const taggedVersion = Effect.fn("Launcher.taggedVersion")(function* (
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
export const releaseClosure = Effect.fn("Launcher.releaseClosure")(function* (
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
export const download = (
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
export const downloadJson = (
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
