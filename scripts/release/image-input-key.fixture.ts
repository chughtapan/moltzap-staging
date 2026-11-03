/** @file Committed workspace fixtures for image input contracts. */
import { Effect } from "effect";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- Synchronous Git fixtures exercise committed input behavior.
import { mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { imageInputs } from "./image-input-key.ts";
/** Resolved runtime graph shared by committed fixtures. */
export const lock = `lockfileVersion: '9.0'
importers:
  packages/router:
    dependencies:
      '@fixture/core':
        version: link:../social-harness
      plain:
        version: 0.97.0(effect@3.22.1)
      alias:
        version: external@1.0.0(peer@2.0.0)
    devDependencies:
      unused:
        version: 1.0.0
  packages/registry:
    dependencies:
      '@fixture/core':
        version: link:../social-harness
  images/daemon:
    dependencies:
      '@fixture/core':
        version: link:../../packages/social-harness
      '@fixture/channel':
        version: link:../../plugins/openclaw
  plugins/openclaw:
    dependencies:
      '@fixture/core':
        version: link:../../packages/social-harness
  packages/social-harness:
    optionalDependencies:
      leaf:
        version: 1.0.0
packages:
  plain@0.97.0:
    resolution: {integrity: sha512-plain}
  effect@3.22.1:
    resolution: {integrity: sha512-effect}
  unused@1.0.0:
    resolution: {integrity: sha512-unused}
  dev-leaf@1.0.0:
    resolution: {integrity: sha512-dev-leaf}
  external@1.0.0:
    resolution: {integrity: sha512-external}
  leaf@1.0.0:
    resolution: {integrity: sha512-leaf}
  peer@2.0.0:
    resolution: {integrity: sha512-peer}
snapshots:
  plain@0.97.0(effect@3.22.1):
    dependencies:
      effect: 3.22.1
  effect@3.22.1: {}
  unused@1.0.0:
    dependencies:
      dev-leaf: 1.0.0
  dev-leaf@1.0.0: {}
  external@1.0.0(peer@2.0.0):
    dependencies:
      peer: 2.0.0
  peer@2.0.0: {}
  leaf@1.0.0: {}
`;

/** Immutable multi-stage build with committed context inputs. */
export const dockerfile = `FROM node:24@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa AS build
COPY package.json pnpm-workspace.yaml pnpm-lock.yaml ./
COPY packages/router/ packages/router/
COPY packages/social-harness/src/index.ts packages/social-harness/src/index.ts
COPY packages/social-harness/src/internal.ts packages/social-harness/src/internal.ts
COPY scripts/build/ scripts/build/
FROM node:24@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
COPY --from=build /deploy/ /app/
`;

/** The daemon recipe deploys its seed package from the repository root. */
export const daemonDockerfile = `FROM node:24@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa AS build
COPY package.json pnpm-workspace.yaml pnpm-lock.yaml ./
COPY packages/social-harness/ packages/social-harness/
COPY plugins/openclaw/ plugins/openclaw/
COPY images/daemon/package.json images/daemon/package.json
FROM node:24@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
COPY --from=build --chown=node:node /deploy/ /opt/social-harness/launcher/seed/
ENTRYPOINT ["sharnessd-launcher"]
`;

/** Every fixture has both service names and a differently named library with condition-only exports. */
export function fixture(): string {
  const repo = mkdtempSync(path.join(tmpdir(), "image-inputs-"));
  const files = fixtureFiles(lock, dockerfile);
  for (const service of ["router", "registry"]) {
    files[`packages/${service}/package.json`] = JSON.stringify({
      name: `@fixture/${service}`,
      version: "1",
      dependencies: { "@fixture/core": "workspace:*" },
    });
    files[`packages/${service}/src/index.spec.ts`] =
      'import "@fixture/core/test";\n';
    files[`packages/${service}/src/index.ts`] =
      'import "@fixture/core";\nimport "@fixture/core/internal";\n';
  }
  for (const [file, text] of Object.entries(files)) {
    write(repo, file, text);
  }
  git(repo, ["init", "--quiet"]);
  commit(repo);
  return repo;
}

/** Writes fixture content before committing it. */
export function write(repo: string, file: string, text: string): void {
  mkdirSync(path.dirname(path.join(repo, file)), { recursive: true });
  writeFileSync(path.join(repo, file), text);
}

/** Records fixture inputs so HEAD represents the scenario. */
export function commit(repo: string): void {
  git(repo, ["add", "."]);
  git(repo, [
    "-c",
    "user.name=Fixture",
    "-c",
    "user.email=fixture@example.test",
    "-c",
    "commit.gpgsign=false",
    "commit",
    "--quiet",
    "-m",
    "Record fixture",
  ]);
}

/** Runs fixture Git commands and reports command failures. */
export function git(repo: string, args: readonly string[]): void {
  // eslint-disable-next-line sonarjs/no-os-command-from-path -- Fixtures exercise the installed Git's committed file semantics.
  const result = spawnSync("git", ["-c", "core.autocrlf=false", ...args], {
    cwd: repo,
    encoding: "utf8",
  });
  assert.equal(result.status, 0, result.stderr);
}

/** Returns the Router input key through the public API. */
export function key(repo: string): string {
  return Effect.runSync(imageInputs(repo, ["router"])).router?.key ?? "";
}

/** The Client manifest every fixture commits. */
export function clientManifest(): string {
  return JSON.stringify({
    name: "@fixture/core",
    version: "1",
    files: [
      "dist",
      "!dist/tsconfig.tsbuildinfo",
      "bin",
      "LICENSE",
      "NOTICE",
      "assets",
      "!assets/drafts",
    ],
    scripts: { lint: "node src/daemon/run.ts" },
    nx: {
      targets: {
        build: {
          options: {
            cwd: ".",
            command: "node tools/image-task.ts && node scripts/build/image.ts",
          },
        },
      },
    },
    exports: {
      ".": { types: "./dist/index.d.ts", default: "./dist/index.js" },
      "./service": { import: "./dist/harness/run.js" },
      "./test": { "sharness-test": { import: "./dist/test.js" } },
      "./internal": {
        "sharness-internal": {
          types: "./dist/internal.d.ts",
          import: "./dist/internal.js",
        },
      },
    },
  });
}

/** Committed build, runtime, and unreachable inputs for the image-key contracts. */
function fixtureFiles(
  lock: string,
  dockerfile: string,
): Record<string, string> {
  return {
    "package.json": JSON.stringify({
      packageManager: "pnpm@10.6.2",
      pnpm: { onlyBuiltDependencies: [] },
      scripts: { prepare: "node scripts/setup/install.ts" },
    }),
    ".npmrc": "strict-peer-dependencies=false\n",
    ".dockerignore": "node_modules\n",
    ".node-version": "24.18.0\n",
    "nx.json": "{}",
    "tsconfig.base.json": JSON.stringify({ extends: "./config/base.json" }),
    "config/base.json": "{}",
    "scripts/setup/install.ts": "export {};",
    "tools/image-task.ts": "export {};",
    "tools/heredoc-task.ts": "export {};",
    "assets/config": "initial",
    "pnpm-workspace.yaml":
      "packages:\n  - packages/*\n  - images/daemon\n  - plugins/*\n",
    "pnpm-lock.yaml": lock,
    "scripts/build/image.ts": "export {};\n",
    ...agentFiles(),
    "images/router/Dockerfile": dockerfile,
    "packages/social-harness/package.json": clientManifest(),
    "packages/social-harness/tsconfig.json": JSON.stringify({
      extends: "../../tsconfig.base.json",
      compilerOptions: { rootDir: "src", outDir: "dist" },
      include: ["src/**/*.ts"],
      exclude: ["src/**/*.spec.ts"],
    }),
    "packages/social-harness/src/index.ts": "export const publicValue = 1;\n",
    "packages/social-harness/src/internal.ts":
      'export { privateValue } from "./private.js";\n',
    "packages/social-harness/src/private.ts":
      "export const privateValue = 1;\n",
    "packages/social-harness/src/only-require.ts": "export {};",
    "packages/social-harness/src/only-node.ts": "export {};",
    "packages/social-harness/src/only-sync.ts": "export {};",
    "packages/social-harness/src/only-default.ts": "export {};",
    "packages/social-harness/src/test.ts": "export const testOnly = 1;\n",
    "packages/social-harness/src/harness/run.ts": "export const daemon = 1;\n",
  };
}

/** The daemon seeds both runtime dependencies; the host builds from its own context. */
function agentFiles(): Record<string, string> {
  return {
    "images/daemon/package.json": JSON.stringify({
      name: "@social-harness/daemon",
      private: true,
      dependencies: {
        "@fixture/core": "workspace:*",
        "@fixture/channel": "workspace:*",
      },
      files: ["package.json"],
      nx: { targets: { build: { executor: "nx:noop" } } },
    }),
    "images/daemon/Dockerfile": daemonDockerfile,
    "images/daemon/README.md": "Run the daemon.\n",
    "plugins/openclaw/package.json": JSON.stringify({
      name: "@fixture/channel",
      private: true,
      dependencies: { "@fixture/core": "workspace:*" },
      files: ["dist", "skills", "bin"],
      exports: { ".": "./dist/index.js" },
    }),
    "plugins/openclaw/tsconfig.json": JSON.stringify({
      extends: "../../tsconfig.base.json",
      compilerOptions: { rootDir: "src", outDir: "dist" },
      include: ["src/**/*.ts"],
    }),
    "plugins/openclaw/src/index.ts": 'import "@fixture/core";',
    "plugins/openclaw/bin/loader": "#!/usr/bin/env node\nconsole.log('load');",
    "plugins/openclaw/skills/connect/SKILL.md": "Connect to the daemon.",
    "images/openclaw-agent/Dockerfile":
      "FROM upstream@sha256:host\nCOPY entrypoint.sh /entrypoint.sh\n",
    "images/openclaw-agent/.dockerignore":
      "*\n!entrypoint.sh\n!patch-openclaw.ts\n!configure-channel.ts\n",
    "images/openclaw-agent/entrypoint.sh": "exec openclaw gateway\n",
    "images/openclaw-agent/README.md": "Run the host.\n",
  };
}
