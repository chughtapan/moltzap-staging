/** @file Published agent images bind their committed build inputs through the release API. */
import { Effect } from "effect";
import assert from "node:assert/strict";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- Synchronous Git fixtures exercise committed image-key behavior.
import { readFileSync, rmSync } from "node:fs";
import path from "node:path";
import { test } from "node:test";
import {
  clientManifest,
  commit,
  daemonDockerfile,
  fixture,
  lock,
  write,
} from "./image-input-key.fixture.ts";
import { imageInputs } from "./image-input-key.ts";

// @agent-code-guard/regression-only: Git fixtures pin rebuild behavior through the exported release API.

for (const [file, content, image] of [
  [
    "packages/social-harness/src/harness/run.ts",
    "export const daemon = 42;",
    "daemon",
  ],
  ["packages/social-harness/assets/policy.txt", "runtime policy", "daemon"],
  [
    "packages/social-harness/package.json",
    clientManifest().replace(
      '"version":"1"',
      '"version":"2","sharnessWireVersion":"2026.1026.5"',
    ),
    "daemon",
  ],
  ["plugins/openclaw/src/index.ts", "export const channel = 42;", "daemon"],
  [
    "plugins/openclaw/bin/loader",
    "#!/usr/bin/env node\nconsole.log('updated');",
    "daemon",
  ],
  [
    "plugins/openclaw/skills/connect/SKILL.md",
    "Updated skill instructions.",
    "daemon",
  ],
  [
    "images/daemon/Dockerfile",
    `${daemonDockerfile}\n# image configuration\n`,
    "daemon",
  ],
  [
    "images/daemon/package.json",
    JSON.stringify({
      name: "@social-harness/daemon",
      private: true,
      dependencies: {
        "@fixture/core": "workspace:*",
        "@fixture/channel": "workspace:*",
      },
      files: ["package.json"],
      nx: { targets: { build: { executor: "nx:noop" } } },
      description: "seed",
    }),
    "daemon",
  ],
  [
    "images/openclaw-agent/host.sh",
    "exec openclaw gateway --verbose",
    "openclaw-agent",
  ],
  [
    "images/openclaw-agent/Dockerfile",
    "FROM upstream@sha256:changed",
    "openclaw-agent",
  ],
  [
    "images/openclaw-agent/.dockerignore",
    "*\n!host.sh\n!patch-openclaw.ts\n",
    "openclaw-agent",
  ],
  [
    "images/openclaw-agent/patch-openclaw.ts",
    "export const patch = true;",
    "openclaw-agent",
  ],
  [
    "images/openclaw-agent/configure-channel.ts",
    "export const channel = true;",
    "openclaw-agent",
  ],
] as const) {
  await test(`${image} key binds ${file}`, (t) => {
    const repo = fixture();
    t.after(() => {
      rmSync(repo, { recursive: true, force: true });
    });
    const before = Effect.runSync(imageInputs(repo));
    write(repo, file, content);
    commit(repo);
    const after = Effect.runSync(imageInputs(repo));
    assert.notEqual(after[image]?.key, before[image]?.key);
    assert.equal(after.router?.key, before.router?.key);
    assert.equal(after.registry?.key, before.registry?.key);
  });
}

for (const [file, content, image] of [
  [
    "images/openclaw-agent/README.md",
    "Run the host with care.\n",
    "openclaw-agent",
  ],
  [
    "packages/social-harness/src/index.test.ts",
    'import { publicValue } from "./index.js";\n',
    "daemon",
  ],
  ["images/daemon/README.md", "Run the daemon with care.\n", "daemon"],
] as const) {
  await test(`${image} key covers only what reaches its image, so ${file} keeps it`, (t) => {
    const repo = fixture();
    t.after(() => {
      rmSync(repo, { recursive: true, force: true });
    });
    const before = Effect.runSync(imageInputs(repo));
    write(repo, file, content);
    commit(repo);
    assert.equal(
      Effect.runSync(imageInputs(repo))[image]?.key,
      before[image]?.key,
    );
  });
}

await test("daemon key binds transitive runtime integrity while the standalone host keeps its key", (t) => {
  const repo = fixture();
  t.after(() => {
    rmSync(repo, { recursive: true, force: true });
  });
  const before = Effect.runSync(imageInputs(repo));
  write(repo, "pnpm-lock.yaml", lock.replace("sha512-leaf", "sha512-new-leaf"));
  commit(repo);
  const after = Effect.runSync(imageInputs(repo));
  assert.notEqual(after.daemon?.key, before.daemon?.key);
  assert.equal(after["openclaw-agent"]?.key, before["openclaw-agent"]?.key);
});

for (const { file, content, changesKey } of [
  { file: "mcp.ts", content: "export const client = 2;", changesKey: true },
  { file: "inbox.ts", content: "export const inbox = 2;", changesKey: true },
  { file: "mcp.test.ts", content: "export const test = 2;", changesKey: false },
  {
    file: "README.md",
    content: "Build support documentation.",
    changesKey: false,
  },
]) {
  await test(`daemon key ${changesKey ? "binds" : "excludes"} build-only workspace ${file} without publishing it`, (t) => {
    const repo = buildOnlyWorkspace();
    t.after(() => {
      rmSync(repo, { recursive: true, force: true });
    });
    const before = Effect.runSync(imageInputs(repo));
    write(repo, `plugins/build-support/${file}`, content);
    commit(repo);
    const after = Effect.runSync(imageInputs(repo));
    assert.equal(after.daemon?.key !== before.daemon?.key, changesKey);
    assert.equal(after.router?.key, before.router?.key);
    assert.equal(after.registry?.key, before.registry?.key);
    assert.ok(after.daemon);
    assert.ok(
      after.daemon.modules.every(
        (input) => !input.startsWith("plugins/build-support/"),
      ),
    );
  });
}

/** A copied private workspace compiles into the plugin but is absent from its runtime dependency closure. */
function buildOnlyWorkspace(): string {
  const repo = fixture();
  const manifest: unknown = JSON.parse(
    readFileSync(path.join(repo, "plugins/openclaw/package.json"), "utf8"),
  );
  assert.ok(typeof manifest === "object" && manifest !== null);
  write(
    repo,
    "plugins/openclaw/package.json",
    JSON.stringify({
      ...manifest,
      devDependencies: { "@fixture/build-support": "workspace:*" },
    }),
  );
  write(
    repo,
    "plugins/build-support/package.json",
    JSON.stringify({ name: "@fixture/build-support", private: true }),
  );
  write(
    repo,
    "plugins/build-support/tsconfig.json",
    JSON.stringify({
      compilerOptions: { rootDir: ".", outDir: "dist" },
      include: ["mcp.ts", "inbox.ts"],
      exclude: ["*.test.ts"],
    }),
  );
  write(repo, "plugins/build-support/mcp.ts", "export const client = 1;");
  write(repo, "plugins/build-support/inbox.ts", "export const inbox = 1;");
  write(repo, "plugins/build-support/mcp.test.ts", "export const test = 1;");
  write(repo, "plugins/build-support/README.md", "Build support.");
  write(
    repo,
    "images/daemon/Dockerfile",
    daemonDockerfile.replace(
      "COPY plugins/openclaw/ plugins/openclaw/",
      "COPY plugins/openclaw/ plugins/openclaw/\nCOPY plugins/build-support/ plugins/build-support/",
    ),
  );
  write(
    repo,
    "pnpm-lock.yaml",
    lock
      .replace(
        "  plugins/openclaw:\n    dependencies:",
        "  plugins/openclaw:\n    devDependencies:\n      '@fixture/build-support':\n        version: link:../build-support\n    dependencies:",
      )
      .replace(
        "  packages/social-harness:\n",
        "  plugins/build-support: {}\n  packages/social-harness:\n",
      ),
  );
  commit(repo);
  return repo;
}
