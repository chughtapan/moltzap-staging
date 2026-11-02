/**
 * @file What one launcher start runs, against an image-installed seed on a
 * temporary home volume and a registry answered in memory. Every candidate
 * here is refused before installation, so no test reaches npm or Sigstore.
 */

import {
  FileSystem,
  HttpClient,
  HttpClientResponse,
  Path,
} from "@effect/platform";
import { NodeContext } from "@effect/platform-node";
import { live as it } from "@effect/vitest";
import { Effect, Exit, Layer, Option } from "effect";
import { describe, expect } from "vitest";
import type { LauncherConfiguration } from "./selection.js";
import { readRecord, writeRecord } from "./installation.js";
import { integrityOf } from "./integrity.js";
import { prepare } from "./prepare.js";

/* eslint-disable agent-code-guard/no-hardcoded-assertion-literals -- versions and keep reasons are the launcher contract under test. */

const REGISTRY = "https://registry.test";
const CHANNEL = "@moltzap/openclaw-channel";
const WIRE = "2026.1019.2";

/** The answers of the in-memory registry, by URL. */
type Routes = Readonly<Record<string, () => Response>>;

const json = (body: unknown) => () => Response.json(body);

/** A published release of the channel and its daemon at `version`. */
const release = (version: string, wireVersion?: string): Routes => {
  const tarball = new TextEncoder().encode(`tarball ${version}`);
  const published = (name: string, dependencies: Record<string, string>) => ({
    name,
    version,
    dist: {
      integrity: integrityOf(new TextEncoder().encode("other bytes")),
      tarball: `${REGISTRY}/tarballs/${name}-${version}.tgz`,
      attestations: { url: `${REGISTRY}/attestations/${name}@${version}` },
    },
    dependencies,
  });
  return {
    [`${REGISTRY}/-/package/@moltzap%2fopenclaw-channel/dist-tags`]: json({
      latest: version,
    }),
    [`${REGISTRY}/@moltzap%2fopenclaw-channel/${version}`]: json(
      published(CHANNEL, { "@moltzap/social-harness": version, effect: "^3" }),
    ),
    [`${REGISTRY}/@moltzap%2fsocial-harness/${version}`]: json({
      ...published("@moltzap/social-harness", {}),
      sharnessWireVersion: wireVersion,
    }),
    ...Object.fromEntries(
      ["@moltzap/openclaw-channel", "@moltzap/social-harness"].map((name) => [
        `${REGISTRY}/tarballs/${name}-${version}.tgz`,
        () => new Response(tarball),
      ]),
    ),
  };
};

/** The registry, recording every URL it was asked for. */
const registry = (routes: Routes, asked: string[]) =>
  Layer.succeed(
    HttpClient.HttpClient,
    HttpClient.make((request) => {
      asked.push(request.url);
      const answer = routes[request.url];
      return Effect.succeed(
        HttpClientResponse.fromWeb(
          request,
          answer === undefined ? new Response("", { status: 404 }) : answer(),
        ),
      );
    }),
  );

/** Installed Client entry and wire metadata used by the launcher fixtures. */
const installedClientManifest = (wireVersion?: string) =>
  JSON.stringify({
    name: "@moltzap/social-harness",
    version: "2026.1021.1",
    type: "module",
    sharnessWireVersion: wireVersion,
    exports: { ".": { import: "./dist/index.js" } },
  });

/** A home volume and an image install of channel release `2026.1021.1`. */
const volume = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const root = yield* fs.makeTempDirectoryScoped();
  const seed = path.join(root, "image");
  const write = (relative: string, text: string) =>
    Effect.gen(function* () {
      const file = path.join(seed, "node_modules", relative);
      yield* fs.makeDirectory(path.dirname(file), { recursive: true });
      yield* fs.writeFileString(file, text);
    });
  yield* write(
    `${CHANNEL}/package.json`,
    JSON.stringify({
      name: CHANNEL,
      version: "2026.1021.1",
      sharnessHostCodeHash: "sha256:seed",
    }),
  );
  yield* write(
    "@moltzap/social-harness/package.json",
    installedClientManifest(WIRE),
  );
  yield* write(
    "@moltzap/social-harness/dist/index.js",
    `export const LIVE_WIRE_VERSIONS = [${JSON.stringify(WIRE)}];\n`,
  );
  return { home: path.join(root, "home"), seed };
});

const configuration = (
  home: string,
  selection: LauncherConfiguration["selection"],
): LauncherConfiguration => ({
  directory: home,
  releasePackage: CHANNEL,
  selection,
  registry: REGISTRY,
});

const follow = { kind: "follow", distTag: "latest" } as const;

const start = (routes: Routes, asked: string[] = []) =>
  Layer.merge(NodeContext.layer, registry(routes, asked));

const seedsTheRecord = () =>
  Effect.gen(function* () {
    const { home, seed } = yield* volume;
    const fs = yield* FileSystem.FileSystem;
    const prepared = yield* prepare(
      configuration(home, { kind: "off" }),
      seed,
    ).pipe(Effect.provide(start({})));
    expect(prepared).toMatchObject({
      version: "2026.1021.1",
      prefix: seed,
      kept: "upgrade-off",
      restartHost: false,
    });
    const record = yield* readRecord(home);
    expect(Option.map(record, (value) => value.version)).toEqual(
      Option.some("2026.1021.1"),
    );
    expect(yield* fs.readLink(`${home}/current`)).toBe(seed);
  }).pipe(Effect.scoped, Effect.provide(NodeContext.layer));

const keepsOutsideTheWireWindow = () =>
  Effect.gen(function* () {
    const { home, seed } = yield* volume;
    const asked: string[] = [];
    const prepared = yield* prepare(configuration(home, follow), seed).pipe(
      Effect.provide(start(release("2026.1101.1", "2026.1101.1"), asked)),
    );
    expect(prepared).toMatchObject({
      version: "2026.1021.1",
      kept: "outside-wire-window",
    });
    expect(asked.filter((url) => url.includes("/tarballs/"))).toEqual([]);
  }).pipe(Effect.scoped, Effect.provide(NodeContext.layer));

const keepsUnverifiedBytes = () =>
  Effect.gen(function* () {
    const { home, seed } = yield* volume;
    const prepared = yield* prepare(configuration(home, follow), seed).pipe(
      Effect.provide(start(release("2026.1025.1", WIRE))),
    );
    expect(prepared).toMatchObject({
      version: "2026.1021.1",
      kept: "unverified",
    });
    expect(prepared.previous).toBeUndefined();
  }).pipe(Effect.scoped, Effect.provide(NodeContext.layer));

const keepsWhenUnreachable = () =>
  Effect.gen(function* () {
    const { home, seed } = yield* volume;
    const prepared = yield* prepare(configuration(home, follow), seed).pipe(
      Effect.provide(start({})),
    );
    expect(prepared).toMatchObject({
      version: "2026.1021.1",
      kept: "unresolved",
    });
  }).pipe(Effect.scoped, Effect.provide(NodeContext.layer));

const keepsAnUnsafePin = () =>
  Effect.gen(function* () {
    const { home, seed } = yield* volume;
    const asked: string[] = [];
    const routes = release("2026.1025.1", WIRE);
    const prepared = yield* prepare(configuration(home, follow), seed).pipe(
      Effect.provide(
        start(
          {
            ...routes,
            [`${REGISTRY}/@moltzap%2fsocial-harness/2026.1025.1`]: json({
              name: "@moltzap/social-harness",
              version: "2026.1025.1",
              dist: {
                integrity: integrityOf(new TextEncoder().encode("client")),
                tarball: `${REGISTRY}/tarballs/client.tgz`,
              },
              dependencies: { "@moltzap/social-harness": "../../../escape" },
            }),
          },
          asked,
        ),
      ),
    );
    expect(prepared).toMatchObject({
      version: "2026.1021.1",
      kept: "unresolved",
    });
    expect(asked.filter((url) => url.includes("/tarballs/"))).toEqual([]);
  }).pipe(Effect.scoped, Effect.provide(NodeContext.layer));

const waitsForNoLiveLauncher = () =>
  Effect.gen(function* () {
    const { home, seed } = yield* volume;
    const fs = yield* FileSystem.FileSystem;
    yield* fs.makeDirectory(home, { recursive: true });
    yield* fs.writeFileString(`${home}/launcher.lock`, `${process.ppid}\n`);
    const held = yield* prepare(
      configuration(home, { kind: "off" }),
      seed,
    ).pipe(Effect.provide(start({})), Effect.exit);
    expect(JSON.stringify(held)).toContain('"reason":"record"');
    yield* fs.writeFileString(`${home}/launcher.lock`, "2147483646\n");
    const prepared = yield* prepare(
      configuration(home, { kind: "off" }),
      seed,
    ).pipe(Effect.provide(start({})));
    expect(prepared.kept).toBe("upgrade-off");
  }).pipe(Effect.scoped, Effect.provide(NodeContext.layer));

const refusesAChangedTarball = () =>
  Effect.gen(function* () {
    const { home, seed } = yield* volume;
    const fs = yield* FileSystem.FileSystem;
    const tarball = `${home}/tarballs/client.tgz`;
    yield* fs.makeDirectory(`${home}/tarballs`, { recursive: true });
    yield* fs.writeFileString(tarball, "changed");
    yield* fs.writeFileString(
      `${home}/launcher.json`,
      JSON.stringify({
        format: "org.social-harness/launcher-record/v1",
        releasePackage: CHANNEL,
        version: "2026.1021.1",
        prefix: seed,
        packages: [
          {
            name: "@moltzap/social-harness",
            version: "2026.1021.1",
            integrity: integrityOf(new TextEncoder().encode("verified")),
            tarball,
          },
        ],
      }),
    );
    const exit = yield* prepare(configuration(home, follow), seed).pipe(
      Effect.provide(start({})),
      Effect.exit,
    );
    expect(Exit.isFailure(exit)).toBe(true);
    expect(JSON.stringify(exit)).toContain('"reason":"integrity"');
  }).pipe(Effect.scoped, Effect.provide(NodeContext.layer));

const followsANewImage = () =>
  Effect.gen(function* () {
    const { home, seed } = yield* volume;
    yield* prepare(configuration(home, { kind: "off" }), seed).pipe(
      Effect.provide(start({})),
    );
    const fs = yield* FileSystem.FileSystem;
    yield* fs.writeFileString(
      `${seed}/node_modules/${CHANNEL}/package.json`,
      JSON.stringify({
        name: CHANNEL,
        version: "2026.1022.1",
        sharnessHostCodeHash: "sha256:next-image",
      }),
    );
    const prepared = yield* prepare(
      configuration(home, { kind: "off" }),
      seed,
    ).pipe(Effect.provide(start({})));
    expect(prepared).toMatchObject({
      version: "2026.1022.1",
      prefix: seed,
      kept: "upgrade-off",
    });
    const record = yield* readRecord(home);
    expect(Option.map(record, (value) => value.version)).toEqual(
      Option.some("2026.1022.1"),
    );
  }).pipe(Effect.scoped, Effect.provide(NodeContext.layer));

/** A channel record can point at a prefix carrying a different daemon package. */
const refusesAnUnlaunchableRecordedRelease = () =>
  Effect.gen(function* () {
    const { home, seed } = yield* volume;
    const installed = yield* volume;
    const fs = yield* FileSystem.FileSystem;
    yield* fs.rename(
      `${installed.seed}/node_modules/@moltzap/social-harness`,
      `${installed.seed}/node_modules/@moltzap/client`,
    );
    yield* fs.writeFileString(
      `${installed.seed}/node_modules/@moltzap/client/package.json`,
      JSON.stringify({ name: "@moltzap/client", version: "2026.1021.1" }),
    );
    yield* writeRecord(home, {
      format: "org.social-harness/launcher-record/v1",
      releasePackage: CHANNEL,
      version: "2026.1021.1",
      prefix: installed.seed,
      packages: [],
    });
    const failure = yield* prepare(configuration(home, follow), seed).pipe(
      Effect.provide(start(release("2026.1025.1", WIRE))),
      Effect.flip,
    );
    expect(failure.reason).toBe("configuration");
    expect(failure.message).toContain(home);
    expect(failure.message).toContain("start with an empty launcher directory");
  }).pipe(Effect.scoped, Effect.provide(NodeContext.layer));

// @agent-code-guard/regression-only: Each start outcome is one closed launcher rule, pinned by its example.
describe("a launcher start", () => {
  it(
    "refuses a recorded prefix holding the old daemon package",
    refusesAnUnlaunchableRecordedRelease,
  );
  it("seeds the record from the image install and runs it", seedsTheRecord);
  it(
    "keeps the recorded release when a newer one's wire version is outside its window, without downloading it",
    keepsOutsideTheWireWindow,
  );
  it(
    "keeps the recorded release when a candidate's bytes are not the ones npm recorded",
    keepsUnverifiedBytes,
  );
  it(
    "keeps the recorded release when the registry is unreachable",
    keepsWhenUnreachable,
  );
  it(
    "keeps the recorded release when a release pins a sibling to something other than a calendar version",
    keepsAnUnsafePin,
  );
  it(
    "refuses to prepare while another live launcher holds the directory, and takes over a stale lock",
    waitsForNoLiveLauncher,
  );
  it(
    "refuses to start a recorded release whose cached tarball changed",
    refusesAChangedTarball,
  );
  it(
    "records the image's new release when a new image installed one where the recorded one was",
    followsANewImage,
  );
});

/* eslint-enable agent-code-guard/no-hardcoded-assertion-literals -- Restore assertion rules outside the launcher contract tests. */

/** A library release without wire metadata is kept out before downloading bytes. */
const keepsMissingLibraryWireVersion = () =>
  Effect.gen(function* () {
    const { home, seed } = yield* volume;
    const asked: string[] = [];
    const prepared = yield* prepare(configuration(home, follow), seed).pipe(
      Effect.provide(start(release("2026.1101.1"), asked)),
    );
    expect(prepared).toMatchObject({
      version: "2026.1021.1",
      kept: "unresolved",
    });
    expect(asked.filter((url) => url.includes("/tarballs/"))).toEqual([]);
  }).pipe(Effect.scoped, Effect.provide(NodeContext.layer));

it(
  "keeps the recorded release when the Client manifest has no wire version",
  keepsMissingLibraryWireVersion,
);
