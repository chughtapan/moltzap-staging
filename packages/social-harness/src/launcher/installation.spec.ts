/** @file Launcher selection and installation preserve a usable recorded release. */

import {
  FileSystem,
  HttpClient,
  HttpClientResponse,
  Path,
} from "@effect/platform";
import { NodeContext } from "@effect/platform-node";
import { effect as effectIt, it, live as liveIt } from "@effect/vitest";
import { Effect, Exit, FastCheck as fc, Layer, Option } from "effect";
import { gzipSync } from "node:zlib";
import { describe, expect } from "vitest";
import {
  choose,
  compareVersions,
  integrityOf,
  type LauncherConfiguration,
  prepare,
  readPackedManifest,
  readRecord,
  writeRecord,
} from "./installation.js";

const recorded = {
  version: "2026.1021.1",
  liveWireVersions: ["2026.1019.2", "2026.1022.1"],
};
const follow = { kind: "follow", distTag: "latest" } as const;

/** A calendar version whose parts compare correctly as padded text. */
const sortKey = (version: string): string =>
  version
    .split(".")
    .map((part) => part.padStart(16, "0"))
    .join(".");

const calendarVersion = fc
  .tuple(
    fc.integer({ min: 1000, max: 9999 }),
    fc.integer({ min: 100, max: 9999 }),
    fc.nat(),
  )
  .map(([year, day, counter]) => `${year}.${day}.${counter}`);

describe("following a dist-tag", () => {
  it.prop(
    "should take only a newer release within the recorded wire window",
    [
      fc.oneof(calendarVersion, fc.constant(recorded.version)),
      fc.oneof(fc.constantFrom(...recorded.liveWireVersions), fc.string()),
    ],
    ([version, wireVersion]) => {
      const choice = choose(recorded, { version, wireVersion }, follow);
      const order = sortKey(version).localeCompare(sortKey(recorded.version));
      const admitted = recorded.liveWireVersions.includes(wireVersion);
      if (order === 0) {
        expect(choice).toEqual({ kind: "keep", reason: "current" });
      } else if (order < 0) {
        expect(choice).toEqual({ kind: "keep", reason: "older" });
      } else if (admitted) {
        expect(choice).toEqual({ kind: "take" });
      } else {
        expect(choice).toEqual({ kind: "keep", reason: "outside-wire-window" });
      }
    },
  );
});

describe("an exact pin", () => {
  it.prop(
    "should take a pinned release across version and wire changes",
    [calendarVersion, fc.string()],
    ([version, wireVersion]) => {
      const choice = choose(
        recorded,
        { version, wireVersion },
        { kind: "pinned", version },
      );
      expect(choice.kind === "take").toBe(version !== recorded.version);
    },
  );
});

describe("calendar versions", () => {
  it.prop(
    "should order versions by their numeric parts",
    [calendarVersion, calendarVersion],
    ([left, right]) => {
      const order = compareVersions(left, right) ?? 0;
      expect(Math.sign(order)).toBe(
        Math.sign(sortKey(left).localeCompare(sortKey(right))),
      );
    },
  );

  it("leave a version that is not a calendar version unresolved", () => {
    expect(
      choose(recorded, { version: "next", wireVersion: "2026.1019.2" }, follow),
    ).toEqual({ kind: "keep", reason: "unresolved" });
  });
});

/** A gzipped ustar archive holding `files`, as `npm pack` lays one out. */
const tarball = (files: Readonly<Record<string, string>>): Uint8Array => {
  const blocks: Uint8Array[] = [];
  for (const [name, text] of Object.entries(files)) {
    const body = new TextEncoder().encode(text);
    const header = new Uint8Array(512);
    header.set(new TextEncoder().encode(name), 0);
    header.set(
      new TextEncoder().encode(body.length.toString(8).padStart(11, "0")),
      124,
    );
    header[156] = 0x30;
    blocks.push(
      header,
      body,
      new Uint8Array((512 - (body.length % 512)) % 512),
    );
  }
  blocks.push(new Uint8Array(1024));
  return gzipSync(Buffer.concat(blocks));
};

const manifest = {
  name: "@moltzap/social-harness",
  version: "2026.1025.1",
  dependencies: { "@moltzap/social-harness": "2026.1025.1" },
};

// @agent-code-guard/regression-only: The tarball layout is npm's, pinned by its examples.
describe("a packed manifest", () => {
  effectIt("is read from package/package.json, past the other entries", () =>
    Effect.gen(function* () {
      const read = yield* readPackedManifest(
        tarball({
          "package/README.md": "x".repeat(700),
          "package/package.json": JSON.stringify(manifest),
        }),
      );
      expect(read).toMatchObject(manifest);
    }),
  );

  effectIt(
    "is missing from a tarball without one, or from bytes that are not a tarball",
    () =>
      Effect.gen(function* () {
        for (const bytes of [
          tarball({ "package/index.js": "" }),
          new TextEncoder().encode("not gzip"),
        ]) {
          const exit = yield* Effect.exit(readPackedManifest(bytes));
          expect(Exit.isFailure(exit)).toBe(true);
        }
      }),
  );
});

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
  liveIt(
    "refuses a recorded prefix holding the old daemon package",
    refusesAnUnlaunchableRecordedRelease,
  );
  liveIt("seeds the record from the image install and runs it", seedsTheRecord);
  liveIt(
    "keeps the recorded release when a newer one's wire version is outside its window, without downloading it",
    keepsOutsideTheWireWindow,
  );
  liveIt(
    "keeps the recorded release when a candidate's bytes are not the ones npm recorded",
    keepsUnverifiedBytes,
  );
  liveIt(
    "keeps the recorded release when the registry is unreachable",
    keepsWhenUnreachable,
  );
  liveIt(
    "keeps the recorded release when a release pins a sibling to something other than a calendar version",
    keepsAnUnsafePin,
  );
  liveIt(
    "refuses to prepare while another live launcher holds the directory, and takes over a stale lock",
    waitsForNoLiveLauncher,
  );
  liveIt(
    "refuses to start a recorded release whose cached tarball changed",
    refusesAChangedTarball,
  );
  liveIt(
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

liveIt(
  "keeps the recorded release when the Client manifest has no wire version",
  keepsMissingLibraryWireVersion,
);
