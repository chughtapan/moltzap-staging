#!/usr/bin/env node
/**
 * @file Tests for the release's package selection, content hash, and
 * host-code hash.
 *
 * Run by `pnpm nx run workspace:test:release`. The selection runs over an
 * in-memory workspace whose content hash includes each sibling pin, as a
 * packed manifest does; the hashes run over real temporary directories.
 */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import {
  chmodSync,
  mkdirSync,
  mkdtempSync,
  renameSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { test } from "node:test";
import { hostCodeHashOfDirectories } from "./loaded-plugin-code-hash.ts";
import {
  contentHashOfDirectory,
  contentHashOfTarball,
} from "./package-content-hash.ts";
import {
  classifyNpmView,
  type Manifest,
  type Recorded,
  selectPackages,
  type Workspace,
} from "./select-packages.ts";

const RELEASE = "2099.101.0";
const PREVIOUS = "2099.100.0";

/** A library and its adapter exercise dependency-ordered release selection. */
const DIRS = ["packages/social-harness", "plugins/openclaw"];

/**
 * Every workspace package name, as `hostCodeHashOfPackage` passes them: the
 * private services are workspace packages outside the release set.
 */
const WORKSPACE_NAMES: ReadonlySet<string> = new Set(
  [...DIRS, "plugins/nanoclaw", "packages/registry", "packages/router"].map(
    packageName,
  ),
);
/** Connector npm names retain their published channel suffix. */
function packageName(dir: string): string {
  const name = path.basename(dir);
  const suffix = dir.startsWith("plugins/") ? "-channel" : "";
  return `@moltzap/${name}${suffix}`;
}

const DEPENDENCIES = Object.fromEntries(
  DIRS.map((dir) => [
    dir,
    dir === "packages/social-harness" ? [] : ["packages/social-harness"],
  ]),
);

const MANIFEST = {
  name: "@moltzap/openclaw-channel",
  version: PREVIOUS,
  dependencies: { "@moltzap/social-harness": PREVIOUS },
};
const FILES = { "dist/index.js": "export {};\n", "openclaw.plugin.json": "{}" };

const CHANNEL_MANIFEST = MANIFEST;
const CLIENT_MANIFEST = {
  name: "@moltzap/social-harness",
  version: PREVIOUS,
  exports: {
    ".": { types: "./dist/index.d.ts", import: "./dist/index.js" },
    "./service": {
      types: "./dist/harness/index.d.ts",
      import: "./dist/harness/index.js",
    },
  },
  bin: { sharnessd: "./bin/sharnessd" },
};

const UNAVAILABLE: Recorded["kind"] = "unavailable";

/** Npm holds every PREVIOUS version with the hash it was published with. */
const AS_PUBLISHED = (): Recorded | undefined => undefined;

// @agent-code-guard/regression-only: Each case is one release scenario the publish workflow must get right: client-only, identity, unchanged, missing hash, npm outage, and misordered packages.

await test("a client-only change publishes the client and the channel that pins it", () => {
  const { workspace, versions } = fakeWorkspace(
    { "packages/social-harness": "library-changed" },
    AS_PUBLISHED,
  );
  assert.deepEqual(selectPackages(workspace, RELEASE, DIRS), {
    kind: "selected",
    dirs: ["packages/social-harness", "plugins/openclaw"],
  });
  assert.deepEqual(Object.fromEntries(versions), {
    "packages/social-harness": RELEASE,
    "plugins/openclaw": RELEASE,
  });
});

await test("a channel-only change leaves the Client at its previous release", () => {
  const { workspace, versions } = fakeWorkspace(
    { "plugins/openclaw": "channel-changed" },
    AS_PUBLISHED,
  );
  assert.deepEqual(selectPackages(workspace, RELEASE, DIRS), {
    kind: "selected",
    dirs: ["plugins/openclaw"],
  });
  assert.equal(versions.get("packages/social-harness"), PREVIOUS);
  assert.equal(versions.get("plugins/openclaw"), RELEASE);
});

await test("an unchanged workspace publishes nothing", () => {
  const { workspace } = fakeWorkspace({}, AS_PUBLISHED);
  assert.deepEqual(selectPackages(workspace, RELEASE, DIRS), {
    kind: "selected",
    dirs: [],
  });
});

await test("a version npm holds without a recorded hash is republished", () => {
  const { workspace } = fakeWorkspace({}, (name) =>
    name === "@moltzap/social-harness" ? { kind: "absent" } : undefined,
  );
  assert.deepEqual(selectPackages(workspace, RELEASE, DIRS), {
    kind: "selected",
    dirs: [...DIRS],
  });
});

await test("an npm outage stops the selection", () => {
  const outage: Recorded = { kind: "unavailable", message: "E503" };
  const { workspace } = fakeWorkspace({}, () => outage);
  assert.deepEqual(selectPackages(workspace, RELEASE, DIRS), outage);
});

await test("a package listed before a sibling it depends on stops the selection", () => {
  const { workspace, versions } = fakeWorkspace({}, AS_PUBLISHED);
  assert.deepEqual(
    selectPackages(workspace, RELEASE, [
      "plugins/openclaw",
      "packages/social-harness",
    ]),
    {
      kind: "out-of-order",
      message:
        "plugins/openclaw depends on @moltzap/social-harness, which must come before it",
    },
  );
  assert.equal(versions.get("plugins/openclaw"), PREVIOUS);
});

await test("the content hash ignores the version, the recorded hash, and key order", () => {
  assert.equal(
    hashOf(
      {
        sharnessContentHash: "sha256:old",
        dependencies: MANIFEST.dependencies,
        version: RELEASE,
        name: MANIFEST.name,
      },
      FILES,
    ),
    hashOf(MANIFEST, FILES),
  );
});

await test("the content hash changes with any packed file or sibling pin", () => {
  const base = hashOf(MANIFEST, FILES);
  assert.notEqual(
    hashOf(MANIFEST, { ...FILES, "dist/index.js": "export const a = 1;\n" }),
    base,
  );
  assert.notEqual(hashOf(MANIFEST, { ...FILES, "dist/extra.js": "" }), base);
  assert.notEqual(
    hashOf(
      { ...MANIFEST, dependencies: { "@moltzap/social-harness": RELEASE } },
      FILES,
    ),
    base,
  );
});

await test("the content hash changes with a file's executable bit", () => {
  const root = packedDirectory(MANIFEST, { ...FILES, "bin/tool": "" });
  const before = contentHashOfDirectory(root);
  chmodSync(path.join(root, "bin/tool"), 0o755);
  const after = contentHashOfDirectory(root);
  rmSync(root, { recursive: true, force: true });
  assert.notEqual(after, before);
});

await test("a packed tarball hashes as its extracted package does", () => {
  const parent = mkdtempSync(path.join(tmpdir(), "content-hash-tarball-"));
  const root = path.join(parent, "package");
  renameSync(packedDirectory(MANIFEST, { ...FILES, "bin/tool": "" }), root);
  chmodSync(path.join(root, "bin/tool"), 0o755);
  const tarball = path.join(parent, "pkg.tgz");
  // eslint-disable-next-line sonarjs/no-os-command-from-path -- The test packs with the system tar, as content-hash.ts extracts with it.
  const packed = spawnSync("tar", ["-czf", tarball, "-C", parent, "package"]);
  const hash = contentHashOfTarball(tarball);
  const expected = contentHashOfDirectory(root);
  rmSync(parent, { recursive: true, force: true });
  assert.equal(packed.status, 0);
  assert.equal(hash, expected);
});

await test("npm's answers classify as a hash, absent, or unavailable", () => {
  const subject = "@moltzap/social-harness@2099.100.0";
  assert.deepEqual(
    classifyNpmView({ status: 0, stdout: '"sha256:a"\n', stderr: "" }, subject),
    { kind: "hash", hash: "sha256:a" },
  );
  assert.deepEqual(
    classifyNpmView({ status: 0, stdout: "", stderr: "" }, subject),
    { kind: "absent" },
  );
  assert.deepEqual(
    classifyNpmView(
      {
        status: 1,
        stdout: JSON.stringify({ error: { code: "E404" } }),
        stderr: "",
      },
      subject,
    ),
    { kind: "absent" },
  );
  for (const failure of [
    { status: 1, stdout: JSON.stringify({ error: { code: "ECONNREFUSED" } }) },
    { status: null, stdout: "" },
  ]) {
    assert.equal(
      classifyNpmView({ ...failure, stderr: "connect failed" }, subject).kind,
      UNAVAILABLE,
    );
  }
});

await test("the host-code hash ignores Client modules the channel cannot reach", () => {
  const packages = hostPackages();
  const base = hostHash(packages);
  writeFileSync(
    path.join(packages.client, "dist/daemon/index.js"),
    "export const daemon = 2;\n",
  );
  writeFileSync(path.join(packages.client, "bin/sharnessd"), "changed\n");
  assert.equal(hostHash(packages), base);
  removePackages(packages);
});

await test("the host-code hash changes with any Client module the channel reaches", () => {
  const packages = hostPackages();
  const base = hostHash(packages);
  writeFileSync(
    path.join(packages.client, "dist/endpoint.js"),
    "export const endpoint = 2;\n",
  );
  assert.notEqual(hostHash(packages), base);
  removePackages(packages);
});

await test("the host-code hash ignores sibling versions and pins", () => {
  const packages = hostPackages();
  const base = hostHash(packages);
  writeFileSync(
    path.join(packages.channel, "package.json"),
    JSON.stringify({
      ...CHANNEL_MANIFEST,
      version: RELEASE,
      dependencies: { "@moltzap/social-harness": RELEASE },
    }),
  );
  writeFileSync(
    path.join(packages.client, "package.json"),
    JSON.stringify({ ...CLIENT_MANIFEST, version: RELEASE }),
  );
  assert.equal(hostHash(packages), base);
  removePackages(packages);
});

for (const section of [
  "devDependencies",
  "peerDependencies",
  "optionalDependencies",
] as const) {
  await test(`the host-code hash ignores non-closure workspace ${section} pins`, (t) => {
    const packages = hostPackages();
    t.after(() => {
      removePackages(packages);
    });
    writeFileSync(
      path.join(packages.channel, "package.json"),
      JSON.stringify({
        ...CHANNEL_MANIFEST,
        [section]: { "@moltzap/router": PREVIOUS },
      }),
    );
    const before = hostHash(packages);
    writeFileSync(
      path.join(packages.channel, "package.json"),
      JSON.stringify({
        ...CHANNEL_MANIFEST,
        [section]: { "@moltzap/router": RELEASE },
      }),
    );
    assert.equal(hostHash(packages), before);
  });
}

/** An in-memory workspace and the manifest version of each package. */
interface Fake {
  readonly workspace: Workspace;
  readonly versions: Map<string, string>;
}

/**
 * Every package was last published at PREVIOUS with the hash its source and
 * pins had then; `code` overrides a package's current source, and `recorded`
 * overrides what npm answers for a version.
 */
function fakeWorkspace(
  code: Readonly<Record<string, string>>,
  recorded: (name: string, version: string) => Recorded | undefined,
): Fake {
  const versions = new Map<string, string>(DIRS.map((dir) => [dir, PREVIOUS]));
  const published = new Map(
    DIRS.map((dir) => [
      `${packageName(dir)}@${PREVIOUS}`,
      packedHash(dir, `${dir}-source`, () => PREVIOUS),
    ]),
  );
  const workspace: Workspace = {
    manifest: (dir): Manifest => ({
      name: packageName(dir),
      version: versions.get(dir) ?? PREVIOUS,
      dependencies: Object.fromEntries(
        (DEPENDENCIES[dir] ?? []).map((sibling) => [
          packageName(sibling),
          "workspace:*",
        ]),
      ),
    }),
    contentHash: (dir) =>
      packedHash(
        dir,
        code[dir] ?? `${dir}-source`,
        (sibling) => versions.get(sibling) ?? PREVIOUS,
      ),
    recordedHash: (name, version) => {
      const hash = published.get(`${name}@${version}`);
      return (
        recorded(name, version) ??
        (hash === undefined ? { kind: "absent" } : { kind: "hash", hash })
      );
    },
    select: (dir, version) => {
      versions.set(dir, version);
    },
  };
  return { workspace, versions };
}

/** A fake content hash: the package's source and each sibling pin. */
function packedHash(
  dir: string,
  source: string,
  pin: (sibling: string) => string,
): string {
  const pins = (DEPENDENCIES[dir] ?? []).map(
    (sibling) => sibling + "@" + pin(sibling),
  );
  return `${source}|${pins.join(",")}`;
}

function hashOf(
  manifest: Readonly<Record<string, unknown>>,
  files: Readonly<Record<string, string>>,
): string {
  const root = packedDirectory(manifest, files);
  const hash = contentHashOfDirectory(root);
  rmSync(root, { recursive: true, force: true });
  return hash;
}

/** Extracted channel and Client packages for the host-code hash. */
interface HostPackages {
  readonly channel: string;
  readonly client: string;
}

/**
 * A channel whose plugin imports the Client root entry, and a Client whose
 * root reaches one endpoint module while `./service` and the binary sit
 * outside that graph.
 */
function hostPackages(): HostPackages {
  return {
    channel: packedDirectory(CHANNEL_MANIFEST, {
      "dist/plugin.js":
        'import { acquireHarnessEndpoint } from "@moltzap/social-harness";\nexport default acquireHarnessEndpoint;\n',
      "openclaw.plugin.json": "{}",
    }),
    client: packedDirectory(CLIENT_MANIFEST, {
      "dist/index.js":
        'export { endpoint as acquireHarnessEndpoint } from "./endpoint.js";\n',
      "dist/endpoint.js": "export const endpoint = 1;\n",
      "dist/daemon/index.js": "export const daemon = 1;\n",
      "bin/sharnessd": 'import "../dist/harness/index.js";\n',
    }),
  };
}

function hostHash(packages: HostPackages): string {
  return hostCodeHashOfDirectories(
    CHANNEL_MANIFEST.name,
    new Map([
      [CHANNEL_MANIFEST.name, packages.channel],
      [CLIENT_MANIFEST.name, packages.client],
    ]),
    WORKSPACE_NAMES,
  );
}

function removePackages(packages: HostPackages): void {
  rmSync(packages.channel, { recursive: true, force: true });
  rmSync(packages.client, { recursive: true, force: true });
}

function packedDirectory(
  manifest: Readonly<Record<string, unknown>>,
  files: Readonly<Record<string, string>>,
): string {
  const root = mkdtempSync(path.join(tmpdir(), "content-hash-test-"));
  writeFileSync(path.join(root, "package.json"), JSON.stringify(manifest));
  for (const [relative, text] of Object.entries(files)) {
    mkdirSync(path.dirname(path.join(root, relative)), { recursive: true });
    writeFileSync(path.join(root, relative), text);
  }
  return root;
}

await test("packed channel declarations do not pull the Client service into the host hash", () => {
  const packages = hostPackages();
  writeFileSync(
    path.join(packages.channel, "dist/plugin.d.ts"),
    'export * from "@moltzap/social-harness/service";\n',
  );
  const before = hostHash(packages);
  writeFileSync(
    path.join(packages.client, "dist/daemon/index.js"),
    "export const daemon = 2;\n",
  );
  assert.equal(hostHash(packages), before);
  removePackages(packages);
});

await test("the host hash leaves sharness-internal subpaths inactive", () => {
  const packages = hostPackages();
  writeFileSync(
    path.join(packages.client, "package.json"),
    JSON.stringify({
      ...CLIENT_MANIFEST,
      exports: {
        ...CLIENT_MANIFEST.exports,
        "./internal": { "sharness-internal": "./dist/harness/index.js" },
      },
    }),
  );
  writeFileSync(
    path.join(packages.channel, "dist/plugin.js"),
    'import "@moltzap/social-harness"; import "@moltzap/social-harness/internal";\n',
  );
  const before = hostHash(packages);
  writeFileSync(
    path.join(packages.client, "dist/daemon/index.js"),
    "export const daemon = 2;\n",
  );
  assert.equal(hostHash(packages), before);
  removePackages(packages);
});

await test("the host hash respects a blocked first matching Node condition", () => {
  const packages = hostPackages();
  writeFileSync(
    path.join(packages.client, "package.json"),
    JSON.stringify({
      ...CLIENT_MANIFEST,
      exports: { ".": { node: null, default: "./dist/harness/index.js" } },
    }),
  );
  const before = hostHash(packages);
  writeFileSync(
    path.join(packages.client, "dist/daemon/index.js"),
    "export const daemon = 2;\n",
  );
  assert.equal(hostHash(packages), before);
  removePackages(packages);
});

await test("the content hash covers sharnessWireVersion", () => {
  assert.notEqual(
    hashOf({ ...MANIFEST, sharnessWireVersion: "next" }, FILES),
    hashOf(MANIFEST, FILES),
  );
});
