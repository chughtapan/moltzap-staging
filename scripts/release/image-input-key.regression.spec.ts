/** @file Adversarial image input contracts against committed Git fixtures. */
import { Effect, Either } from "effect";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- Synchronous Git fixtures exercise committed input behavior.
import { readFileSync, rmSync } from "node:fs";
import path from "node:path";
import { test } from "node:test";
import { hostCodeHashOfPackage } from "./host-code-hash.ts";
import {
  commit,
  dockerfile,
  fixture,
  git,
  key,
  lock,
  write,
} from "./image-input-key.fixture.ts";
import {
  ImageInputError,
  imageInputs,
  runtimeVersionReads,
} from "./image-input-key.ts";

// @agent-code-guard/regression-only: Adversarial fixtures verify input sensitivity and typed refusal through the release API.

for (const [label, instruction] of [
  ["unpinned FROM", "FROM node:24-slim"],
  ["unpinned COPY image", "COPY --from=node:24 /bin/node /node"],
  [
    "unpinned mount image",
    "RUN --mount=type=bind,from=node:24,target=/node true",
  ],
  ["unpinned syntax frontend", "# syntax=docker/dockerfile:1"],
  ["undeclared COPY stage", "COPY --from=missing /app /app"],
  [
    "undeclared mount stage",
    "RUN --mount=type=bind,from=missing,target=/app true",
  ],
  ["later COPY stage", "COPY --from=build /app /app"],
  ["unscoped install", "RUN pnpm install --frozen-lockfile"],
  ["unscoped deploy", "RUN pnpm deploy /out"],
  [
    "install without dependency scope",
    "RUN pnpm install --filter @fixture/router",
  ],
  ["other service install", "RUN pnpm install --filter @fixture/registry..."],
  ["filtered package script", "RUN pnpm --filter @fixture/router run build"],
  ["directory package script", "RUN pnpm -C packages/router run build"],
  ["scope after run", "RUN pnpm run --filter @fixture/router build"],
  [
    "scoped script with unrelated install word",
    "RUN pnpm --filter @fixture/router run compile && echo install",
  ],
] as const) {
  await test(`refuses ${label}`, (t) => {
    const repo = fixture();
    t.after(() => {
      rmSync(repo, { recursive: true, force: true });
    });
    write(repo, "images/router/Dockerfile", `${instruction}\n${dockerfile}`);
    commit(repo);
    const error = refused(repo);
    assert.ok(error instanceof ImageInputError);
    assert.match(error.message, /images\/router\/Dockerfile|package.json/u);
  });
}

await test("COPY service directories never turn release prose or comments into commands", (t) => {
  const repo = fixture();
  t.after(() => {
    rmSync(repo, { recursive: true, force: true });
  });
  write(repo, "packages/router/README.md", "docs/guide.md");
  write(repo, "docs/guide.md", "CHANGELOG.md");
  write(repo, "CHANGELOG.md", "old");
  write(repo, "images/router/Dockerfile", `${dockerfile}\n# CHANGELOG.md\n`);
  commit(repo);
  const before = Effect.runSync(imageInputs(repo));
  write(repo, "CHANGELOG.md", "release");
  write(repo, "docs/guide.md", "release docs");
  commit(repo);
  assert.deepEqual(Effect.runSync(imageInputs(repo)), before);
});

await test("a full-line comment continuation cannot hide COPY", (t) => {
  const repo = fixture();
  t.after(() => {
    rmSync(repo, { recursive: true, force: true });
  });
  write(
    repo,
    "images/router/Dockerfile",
    dockerfile.replace(
      "COPY package.json",
      "# comment \\\nCOPY assets/ assets/\nCOPY package.json",
    ),
  );
  commit(repo);
  const before = key(repo);
  write(repo, "assets/config", "changed");
  commit(repo);
  assert.notEqual(key(repo), before);
});

await test("direct importer mappings bind versions even when both snapshots remain reachable", (t) => {
  const repo = fixture();
  t.after(() => {
    rmSync(repo, { recursive: true, force: true });
  });
  const base = lock.replace(
    "      alias:",
    "      second:\n        version: leaf@1.0.0\n      keeper:\n        version: external@1.0.0(peer@2.0.0)\n      alias:",
  );
  write(repo, "pnpm-lock.yaml", base);
  commit(repo);
  const before = key(repo);
  write(
    repo,
    "pnpm-lock.yaml",
    base.replace("version: external@1.0.0(peer@2.0.0)", "version: leaf@1.0.0"),
  );
  commit(repo);
  assert.notEqual(key(repo), before);
});

for (const [label, file, initial, changed] of [
  [
    "lock settings",
    "pnpm-lock.yaml",
    `settings: {autoInstallPeers: true}\n${lock}`,
    `settings: {autoInstallPeers: false}\n${lock}`,
  ],
  [
    "pnpmfile checksum",
    "pnpm-lock.yaml",
    `pnpmfileChecksum: first\n${lock}`,
    `pnpmfileChecksum: second\n${lock}`,
  ],
  [
    "pnpm hook",
    ".pnpmfile.cjs",
    "module.exports = {};",
    "module.exports = {hooks: {}};",
  ],
] as const) {
  await test(`${label} changes both keys`, (t) => {
    const repo = fixture();
    t.after(() => {
      rmSync(repo, { recursive: true, force: true });
    });
    write(repo, file, initial);
    commit(repo);
    const before = Effect.runSync(imageInputs(repo));
    write(repo, file, changed);
    commit(repo);
    const after = Effect.runSync(imageInputs(repo));
    assert.notEqual(after.router?.key, before.router?.key);
    assert.notEqual(after.registry?.key, before.registry?.key);
  });
}

for (const root of ["packages/router", "packages/social-harness"] as const) {
  await test(`${root} build tools bind their locked versions`, (t) => {
    const repo = fixture();
    t.after(() => {
      rmSync(repo, { recursive: true, force: true });
    });
    const file = `${root}/package.json`;
    const manifest: unknown = JSON.parse(
      readFileSync(path.join(repo, file), "utf8"),
    );
    assert.ok(typeof manifest === "object" && manifest !== null);
    write(
      repo,
      file,
      JSON.stringify({ ...manifest, devDependencies: { unused: "catalog:" } }),
    );
    const base =
      root === "packages/social-harness"
        ? lock.replace(
            "  packages/social-harness:",
            "  packages/social-harness:\n    devDependencies:\n      unused:\n        version: 1.0.0",
          )
        : lock;
    write(repo, "pnpm-lock.yaml", base);
    commit(repo);
    const before = key(repo);
    write(
      repo,
      "pnpm-lock.yaml",
      base
        .replaceAll("unused@1.0.0", "unused@2.0.0")
        .replaceAll(
          "      unused:\n        version: 1.0.0",
          "      unused:\n        version: 2.0.0",
        ),
    );
    commit(repo);
    assert.notEqual(key(repo), before);
  });
}

for (const [label, root, scripts, nx] of [
  [
    "service lifecycle",
    "packages/router",
    { postinstall: "node ../../tools/extra.ts" },
    {},
  ],
  [
    "sibling lifecycle",
    "packages/social-harness",
    { prepare: "node ../../tools/extra.ts" },
    {},
  ],
  [
    "package build cwd",
    "packages/social-harness",
    { build: "node ../../tools/extra.ts" },
    { targets: { build: { options: { cwd: "tools" } } } },
  ],
  [
    "object Nx command",
    "packages/social-harness",
    {},
    {
      targets: {
        build: {
          options: { cwd: ".", commands: [{ command: "node tools/extra.ts" }] },
        },
      },
    },
  ],
] as const) {
  await test(`${label} discovers its executable`, (t) => {
    const repo = fixture();
    t.after(() => {
      rmSync(repo, { recursive: true, force: true });
    });
    const file = `${root}/package.json`;
    const manifest: unknown = JSON.parse(
      readFileSync(path.join(repo, file), "utf8"),
    );
    assert.ok(typeof manifest === "object" && manifest !== null);
    write(repo, file, JSON.stringify({ ...manifest, scripts, nx }));
    write(repo, "tools/extra.ts", 'import "./leaf.js";');
    write(repo, "tools/leaf.ts", "export const value = 1;");
    commit(repo);
    const before = key(repo);
    write(repo, "tools/leaf.ts", "export const value = 2;");
    commit(repo);
    assert.notEqual(key(repo), before);
  });
}

await test("project.json build commands discover their executable", (t) => {
  const repo = fixture();
  t.after(() => {
    rmSync(repo, { recursive: true, force: true });
  });
  write(
    repo,
    "packages/social-harness/project.json",
    JSON.stringify({
      targets: {
        build: {
          executor: "nx:run-commands",
          options: { cwd: ".", command: "node tools/extra.ts" },
        },
      },
    }),
  );
  write(repo, "tools/extra.ts", "export const value = 1;");
  commit(repo);
  const before = key(repo);
  write(repo, "tools/extra.ts", "export const value = 2;");
  commit(repo);
  assert.notEqual(key(repo), before);
});

for (const [label, file, content] of [
  [
    "Nx executor",
    "packages/social-harness/project.json",
    JSON.stringify({ targets: { build: { executor: "@nx/js:tsc" } } }),
  ],
  [
    "Nx configurations",
    "packages/social-harness/package.json",
    JSON.stringify({
      name: "@fixture/core",
      exports: {
        ".": "./dist/index.js",
        "./internal": "./dist/internal.js",
        "./test": { "sharness-test": "./dist/index.js" },
      },
      nx: { targets: { build: { configurations: { production: {} } } } },
    }),
  ],
  [
    "missing relative config",
    "packages/social-harness/tsconfig.json",
    '{"extends":"./missing"}',
  ],
  [
    "missing package config",
    "packages/social-harness/tsconfig.json",
    '{"extends":"@unkeyed/preset/tsconfig.json"}',
  ],
] as const) {
  await test(`refuses ${label} with its file`, (t) => {
    const repo = fixture();
    t.after(() => {
      rmSync(repo, { recursive: true, force: true });
    });
    write(repo, file, content);
    commit(repo);
    const error = refused(repo);
    assert.ok(error instanceof ImageInputError);
    assert.ok(error.message.includes(file));
  });
}

await test("JSONC extensionless inheritance and references bind compiler inputs", (t) => {
  const repo = fixture();
  t.after(() => {
    rmSync(repo, { recursive: true, force: true });
  });
  write(
    repo,
    "packages/social-harness/tsconfig.json",
    '{// inherited layout\n"extends":"./layout", "references":[{"path":"../../config/ref"}],}',
  );
  write(
    repo,
    "packages/social-harness/layout.json",
    '{"compilerOptions":{"outDir":"dist","rootDir":"src",},}',
  );
  write(repo, "config/ref/tsconfig.json", "{}");
  commit(repo);
  const before = key(repo);
  write(
    repo,
    "config/ref/tsconfig.json",
    '{"compilerOptions":{"strict":true}}',
  );
  commit(repo);
  assert.notEqual(key(repo), before);
});

for (const [label, expression] of [
  [
    "joined manifest path",
    'readFileSync(path.join(import.meta.dirname, "..", "package.json"))',
  ],
  ["bracket npm version", 'process.env["npm_package_version"]'],
  [
    "destructured npm version",
    "(() => { const {npm_package_version: version} = process.env; return version; })()",
  ],
  [
    "default manifest re-export",
    '(() => {})(); export {default} from "../package.json"',
  ],
  [
    "wire version",
    '(() => { const manifest = require("../package.json"); return manifest.sharnessWireVersion; })()',
  ],
] as const) {
  await test(`runtime version gate detects ${label}`, (t) => {
    const repo = fixture();
    t.after(() => {
      rmSync(repo, { recursive: true, force: true });
    });
    write(repo, "packages/router/src/version.ts", `${expression};`);
    commit(repo);
    assert.deepEqual(Effect.runSync(runtimeVersionReads(repo)), [
      "packages/router/src/version.ts",
    ]);
  });
}

await test("runtime version gate excludes tests and modules reached only by tests", (t) => {
  const repo = fixture();
  t.after(() => {
    rmSync(repo, { recursive: true, force: true });
  });
  write(
    repo,
    "packages/router/src/version.spec.ts",
    'console.log(process.env.npm_package_version); import "@fixture/core/service";',
  );
  write(
    repo,
    "packages/social-harness/src/harness/run.ts",
    "console.log(process.env.npm_package_version);",
  );
  write(
    repo,
    "packages/router/package.json",
    JSON.stringify({
      name: "@fixture/router",
      exports: { "./test": { "sharness-test": "./dist/testing-entry.js" } },
    }),
  );
  write(
    repo,
    "packages/router/src/testing-entry.ts",
    "console.log(process.env.npm_package_version);",
  );
  commit(repo);
  assert.deepEqual(Effect.runSync(runtimeVersionReads(repo)), []);
});

await test("refuses a submodule beneath service inputs", (t) => {
  const repo = fixture();
  t.after(() => {
    rmSync(repo, { recursive: true, force: true });
  });
  // eslint-disable-next-line sonarjs/no-os-command-from-path -- Git creates a committed gitlink fixture.
  const head = spawnSync("git", ["rev-parse", "HEAD"], {
    cwd: repo,
    encoding: "utf8",
  }).stdout.trim();
  git(repo, [
    "update-index",
    "--add",
    "--cacheinfo",
    `160000,${head},packages/router/vendor`,
  ]);
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
    "gitlink",
  ]);
  const error = refused(repo);
  assert.ok(error instanceof ImageInputError);
  assert.match(error.message, /gitlink.*packages\/router\/vendor/u);
});

await test("inherited nondefault compiler directories resolve emitted exports", (t) => {
  const repo = fixture();
  t.after(() => {
    rmSync(repo, { recursive: true, force: true });
  });
  write(
    repo,
    "packages/social-harness/tsconfig.json",
    '{"extends":"./layout"}',
  );
  write(
    repo,
    "packages/social-harness/layout.json",
    '{"compilerOptions":{"outDir":"lib","rootDir":"source"}}',
  );
  write(
    repo,
    "packages/social-harness/package.json",
    '{"name":"@fixture/core","exports":{".":"./lib/index.js","./internal":"./lib/index.js","./test":{"sharness-test":"./lib/index.js"}}}',
  );
  write(
    repo,
    "packages/social-harness/source/index.ts",
    "export const value = 1;",
  );
  commit(repo);
  const before = key(repo);
  write(
    repo,
    "packages/social-harness/source/index.ts",
    "export const value = 2;",
  );
  commit(repo);
  assert.notEqual(key(repo), before);
});

for (const [label, instruction] of [
  ["current named stage", "COPY --from=build /app /app"],
  ["current numeric stage", "RUN --mount=type=bind,from=0,target=/app true"],
] as const) {
  await test(`refuses ${label} as an earlier stage`, (t) => {
    const repo = fixture();
    t.after(() => {
      rmSync(repo, { recursive: true, force: true });
    });
    write(
      repo,
      "images/router/Dockerfile",
      `FROM node@sha256:${"a".repeat(64)} AS build\n${instruction}`,
    );
    commit(repo);
    const error = refused(repo);
    assert.match(error.message, /images\/router\/Dockerfile line/u);
  });
}

for (const [label, script] of [
  ["shell install", "pnpm install"],
  ["nested scoped run", "pnpm --filter @fixture/router run compile"],
] as const) {
  await test(`refuses ${label} named by executable text`, (t) => {
    const repo = fixture();
    t.after(() => {
      rmSync(repo, { recursive: true, force: true });
    });
    write(
      repo,
      "images/router/Dockerfile",
      `${dockerfile}\nRUN sh tools/install.sh`,
    );
    write(repo, "tools/install.sh", script);
    commit(repo);
    const error = refused(repo);
    assert.match(error.message, /tools\/install.sh/u);
  });
}

await test("shell inline comments cannot name release inputs", (t) => {
  const repo = fixture();
  t.after(() => {
    rmSync(repo, { recursive: true, force: true });
  });
  write(repo, "images/router/Dockerfile", dockerfile);
  write(
    repo,
    "packages/router/package.json",
    JSON.stringify({
      name: "@fixture/router",
      nx: {
        targets: {
          build: { options: { cwd: ".", command: "sh tools/build.sh" } },
        },
      },
    }),
  );
  write(repo, "tools/build.sh", "true;# CHANGELOG.md\n");
  write(repo, "CHANGELOG.md", "first");
  commit(repo);
  const before = key(repo);
  write(repo, "CHANGELOG.md", "release");
  commit(repo);
  assert.equal(key(repo), before);
});

await test("refuses non-UTF-8 committed paths", (t) => {
  const repo = fixture();
  t.after(() => {
    rmSync(repo, { recursive: true, force: true });
  });
  // eslint-disable-next-line sonarjs/no-os-command-from-path -- Git creates a blob without an invalid filesystem path.
  const blob = spawnSync("git", ["hash-object", "-w", "--stdin"], {
    cwd: repo,
    input: "invalid",
    encoding: "utf8",
  });
  assert.equal(blob.status, 0, blob.stderr);
  // eslint-disable-next-line sonarjs/no-os-command-from-path -- Git accepts the fixture's raw path bytes through its index.
  const indexed = spawnSync("git", ["update-index", "-z", "--index-info"], {
    cwd: repo,
    input: Buffer.concat([
      Buffer.from(`100644 ${blob.stdout.trim()}\tpackages/router/`),
      Buffer.from([255, 0]),
    ]),
    encoding: "utf8",
  });
  assert.equal(indexed.status, 0, indexed.stderr);
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
    "invalid-path",
  ]);
  const error = refused(repo);
  assert.ok(error instanceof ImageInputError);
  assert.match(error.message, /Non-UTF-8/u);
});

await test("host hash ignores untracked workspace directories", (t) => {
  const repo = fixture();
  t.after(() => {
    rmSync(repo, { recursive: true, force: true });
  });
  write(repo, "packages/untracked/package.json", "invalid JSON");
  assert.doesNotThrow(() => hostCodeHashOfPackage(repo, "packages/router"));
});

await test("host hash refuses duplicate tracked workspace names", (t) => {
  const repo = fixture();
  t.after(() => {
    rmSync(repo, { recursive: true, force: true });
  });
  write(repo, "packages/duplicate/package.json", '{"name":"@fixture/router"}');
  commit(repo);
  const result = hostCodeHashOfPackage(repo, "packages/router");
  assert.notEqual(typeof result, "string");
  assert.ok(typeof result !== "string");
  assert.match(
    result.message,
    /Duplicate workspace package name @fixture\/router/u,
  );
});

function refused(repo: string): ImageInputError {
  return Either.match(Effect.runSync(Effect.either(imageInputs(repo))), {
    onLeft: (error) => error,
    onRight: () => assert.fail("Expected a typed refusal"),
  });
}

await test("shell executables retain the caller working directory", (t) => {
  const repo = fixture();
  t.after(() => {
    rmSync(repo, { recursive: true, force: true });
  });
  write(repo, "images/router/Dockerfile", dockerfile);
  write(
    repo,
    "packages/router/package.json",
    JSON.stringify({
      name: "@fixture/router",
      nx: {
        targets: {
          build: { options: { cwd: ".", command: "sh tools/build.sh" } },
        },
      },
    }),
  );
  write(repo, "tools/build.sh", "node tools/extra.ts");
  write(repo, "tools/extra.ts", 'import "./leaf.js";');
  write(repo, "tools/leaf.ts", "export const value = 1;");
  commit(repo);
  const before = key(repo);
  write(repo, "tools/leaf.ts", "export const value = 2;");
  commit(repo);
  assert.notEqual(key(repo), before);
});

await test("runtime version gate retains entries shared by active and test exports", (t) => {
  const repo = fixture();
  t.after(() => {
    rmSync(repo, { recursive: true, force: true });
  });
  write(
    repo,
    "packages/router/package.json",
    JSON.stringify({
      name: "@fixture/router",
      exports: {
        ".": {
          import: "./dist/shared-entry.js",
          "sharness-test": "./dist/shared-entry.js",
        },
      },
    }),
  );
  write(
    repo,
    "packages/router/src/shared-entry.ts",
    "console.log(process.env.npm_package_version);",
  );
  commit(repo);
  assert.deepEqual(Effect.runSync(runtimeVersionReads(repo)), [
    "packages/router/src/shared-entry.ts",
  ]);
});

await test("installed workspace lifecycle inputs bind keys without runtime imports", (t) => {
  const repo = fixture();
  t.after(() => {
    rmSync(repo, { recursive: true, force: true });
  });
  write(repo, "packages/router/src/index.ts", "export {};");
  write(repo, "packages/registry/src/index.ts", "export {};");
  write(repo, "packages/router/src/index.spec.ts", "export {};");
  write(repo, "packages/registry/src/index.spec.ts", "export {};");
  write(
    repo,
    "packages/social-harness/package.json",
    JSON.stringify({
      name: "@fixture/core",
      scripts: { postinstall: "node ../../tools/install.ts" },
    }),
  );
  write(repo, "tools/install.ts", "export const value = 1;");
  commit(repo);
  const before = key(repo);
  write(repo, "tools/install.ts", "export const value = 2;");
  commit(repo);
  assert.notEqual(key(repo), before);
});

for (const instruction of [
  "FROM node:24-slim",
  "COPY . /app",
  "COPY packages/social-harness/src/harness/run.ts packages/router/src/server.ts",
  "COPY packages/social-harness/src/index.ts ./",
  "COPY ./ /app",
  "COPY packages/* /app",
  "COPY https://example.test/archive /app",
  "ADD assets/ /app",
  "RUN npm i effect",
  "RUN pnpm i --filter @fixture/router...",
  "RUN pnpm install --frozen-lockfile --filter @fixture/router... --filter @fixture/registry...",
  "RUN pnpm install --frozen-lockfile --filter @fixture/registry...",
  "RUN pnpm install --frozen-lockfile --filter @fixture/router... -w",
  "RUN pnpm -r install",
  "RUN apt-get update",
  "RUN curl https://example.test",
  "RUN <<EOF\ntrue\nEOF",
  "RUN <<-EOF\ntrue\nEOF",
  "RUN pnpm install --frozen-lockfile --filter @fixture/router... && npm i effect",
  "COPY ./assets/ /app",
  "COPY ../outside /app",
  "COPY assets/? /app",
  "COPY assets/[ab] /app",
  'COPY ["assets/", "/app"]',
] as const) {
  await test(`Docker allowlist refuses ${instruction}`, (t) => {
    const repo = fixture();
    t.after(() => {
      rmSync(repo, { recursive: true, force: true });
    });
    write(
      repo,
      "images/router/Dockerfile",
      dockerfile.replace(
        "COPY package.json",
        `${instruction}\nCOPY package.json`,
      ),
    );
    commit(repo);
    const error = refused(repo);
    assert.ok(error instanceof ImageInputError);
    assert.match(error.message, /images\/router\/Dockerfile line 2:/u);
    assert.ok(error.message.includes(instruction.split("\n")[0] ?? ""));
  });
}

/** Real build recipes exercise the exported key API against committed fixture inputs, including the root licence they copy. */
function realImageFixture(): string {
  const repo = fixture();
  write(repo, "LICENSE", "license\n");
  write(repo, "NOTICE", "notice\n");
  for (const service of ["registry", "router"]) {
    const docker = readFileSync(
      path.join(import.meta.dirname, `../../images/${service}/Dockerfile`),
      "utf8",
    );
    write(
      repo,
      `images/${service}/Dockerfile`,
      docker.replaceAll("@moltzap/", "@fixture/"),
    );
  }
  for (const file of ["scripts/build/clean-dist.ts"]) {
    write(
      repo,
      file,
      readFileSync(path.join(import.meta.dirname, "../..", file), "utf8"),
    );
  }
  commit(repo);
  return repo;
}

function checkCopyInventory(
  docker: string,
  inventory: readonly string[],
  locked: readonly string[],
): void {
  const lines = docker
    .split("\n")
    .filter((line) => line.startsWith("COPY ") && !line.includes("--from="));
  const sources = lines.flatMap((line) =>
    line.slice(5).split(/\s+/u).slice(0, -1),
  );
  for (const source of sources) {
    const covered =
      source === "pnpm-lock.yaml"
        ? locked.length > 0
        : inventory.some(
            (input) => input === source || input.startsWith(source),
          );
    assert.ok(covered, `Unaccounted COPY source ${source}`);
  }
}

for (const service of ["registry", "router"] as const) {
  await test(`real ${service} Dockerfile inventories named COPY roots and changes only its key`, (t) => {
    const repo = realImageFixture();
    t.after(() => {
      rmSync(repo, { recursive: true, force: true });
    });
    const before = Effect.runSync(imageInputs(repo));
    const image = before[service];
    assert.ok(image);
    const inventory = [
      ...image.own,
      ...image.modules,
      ...image.image,
      ...image.tooling,
    ];
    const file = `images/${service}/Dockerfile`;
    const docker = readFileSync(path.join(repo, file), "utf8");
    checkCopyInventory(docker, inventory, image.locked);
    write(repo, file, `${docker}\n# smoke build recipe\n`);
    commit(repo);
    const after = Effect.runSync(imageInputs(repo));
    assert.notEqual(after[service]?.key, before[service]?.key);
    const other = service === "registry" ? "router" : "registry";
    assert.equal(after[other]?.key, before[other]?.key);
  });
}

await test("real Dockerfiles exclude daemon-only source and sibling version reads", (t) => {
  const repo = realImageFixture();
  t.after(() => {
    rmSync(repo, { recursive: true, force: true });
  });
  const before = Effect.runSync(imageInputs(repo));
  write(
    repo,
    "packages/social-harness/src/harness/run.ts",
    "console.log(process.env.npm_package_version);",
  );
  write(
    repo,
    "packages/social-harness/src/index.ts",
    'import manifest from "../package.json"; console.log(manifest.version);',
  );
  commit(repo);
  assert.deepEqual(Effect.runSync(runtimeVersionReads(repo)), []);
  const changedSibling = Effect.runSync(imageInputs(repo));
  assert.notEqual(changedSibling.router?.key, before.router?.key);
  write(
    repo,
    "packages/social-harness/src/harness/run.ts",
    "export const changed = true;",
  );
  commit(repo);
  const after = Effect.runSync(imageInputs(repo));
  assert.deepEqual(after.registry, changedSibling.registry);
  assert.deepEqual(after.router, changedSibling.router);
  assert.notEqual(after.daemon?.key, changedSibling.daemon?.key);
});

/** The Client build tsconfig the real Dockerfile rows start from. */
const BUILD_TSCONFIG =
  '{"extends":"../../tsconfig.base.json","compilerOptions":{"rootDir":"src","outDir":"dist"}}\n';

for (const [file, baseline, content, changesKey] of [
  [
    "integration/daemon.spec.ts",
    "baseline",
    "daemon-only integration test",
    false,
  ],
  ["safer-architecture.config.json", "{}", '{"layers":[]}', false],
  ["vitest.config.ts", "export {};", "export const changed = 1;", false],
  ["tsconfig.test.json", "{}", '{"include":["src"]}', false],
  ["typedoc.json", "{}", '{"entryPoints":[]}', false],
  ["AGENTS.md", "baseline", "daemon-only instructions", false],
  ["assets/drafts/notes.md", "draft", "excluded draft edit", false],
  ["LICENSE", "license", "changed license", true],
  ["assets/schema.json", "{}", '{"shipped":true}', true],
  [
    "tsconfig.json",
    BUILD_TSCONFIG,
    '{"extends":"../../tsconfig.base.json","compilerOptions":{"rootDir":"src","outDir":"dist","strict":true}}',
    true,
  ],
] as const) {
  await test(`real Dockerfiles bind copied Client files by what the build reads or packs: ${file} ${changesKey ? "changes" : "preserves"} both keys`, (t) => {
    const repo = realImageFixture();
    t.after(() => {
      rmSync(repo, { recursive: true, force: true });
    });
    const target = `packages/social-harness/${file}`;
    write(repo, target, baseline);
    commit(repo);
    const before = Effect.runSync(imageInputs(repo));
    write(repo, target, content);
    commit(repo);
    const after = Effect.runSync(imageInputs(repo));
    assert.equal(after.router?.key !== before.router?.key, changesKey);
    assert.equal(after.registry?.key !== before.registry?.key, changesKey);
  });
}

await test("a copied sibling without a files list is refused", (t) => {
  const repo = realImageFixture();
  t.after(() => {
    rmSync(repo, { recursive: true, force: true });
  });
  const manifest = readFileSync(
    path.join(repo, "packages/social-harness/package.json"),
    "utf8",
  );
  const withoutFiles = manifest.replace(/"files":\[[^\]]*\],/u, "");
  assert.notEqual(withoutFiles, manifest);
  write(repo, "packages/social-harness/package.json", withoutFiles);
  commit(repo);
  assert.match(
    refused(repo).message,
    /packages\/social-harness\/package\.json declares no files list/u,
  );
});

await test("adding PGlite runtime dependencies changes only the Registry image", (t) => {
  const repo = realImageFixture();
  t.after(() => {
    rmSync(repo, { recursive: true, force: true });
  });
  const before = Effect.runSync(imageInputs(repo));
  write(
    repo,
    "packages/registry/package.json",
    JSON.stringify({
      name: "@fixture/registry",
      dependencies: {
        "@fixture/core": "workspace:*",
        "@electric-sql/pglite": "catalog:",
        "@electric-sql/pglite-socket": "catalog:",
      },
    }),
  );
  write(
    repo,
    "pnpm-lock.yaml",
    lock
      .replace(
        "  packages/registry:\n    dependencies:",
        "  packages/registry:\n    dependencies:\n      '@electric-sql/pglite':\n        version: 0.4.4\n      '@electric-sql/pglite-socket':\n        version: 0.1.4",
      )
      .replace(
        "packages:\n",
        "packages:\n  '@electric-sql/pglite@0.4.4':\n    resolution: {integrity: sha512-pglite}\n  '@electric-sql/pglite-socket@0.1.4':\n    resolution: {integrity: sha512-socket}\n",
      )
      .replace(
        "snapshots:\n",
        "snapshots:\n  '@electric-sql/pglite@0.4.4': {}\n  '@electric-sql/pglite-socket@0.1.4': {}\n",
      ),
  );
  commit(repo);
  const after = Effect.runSync(imageInputs(repo));
  assert.notEqual(after.registry?.key, before.registry?.key);
  assert.equal(after.router?.key, before.router?.key);
});

await test("current services have no own manifest version reads", () => {
  assert.deepEqual(
    Effect.runSync(
      runtimeVersionReads(path.resolve(import.meta.dirname, "../..")),
    ),
    [],
  );
});

await test("root build tool resolution changes bind both images", (t) => {
  const repo = realImageFixture();
  t.after(() => {
    rmSync(repo, { recursive: true, force: true });
  });
  write(
    repo,
    "package.json",
    JSON.stringify({
      packageManager: "pnpm@10.6.2",
      devDependencies: { nx: "catalog:" },
    }),
  );
  const initial = lock
    .replace(
      "importers:\n",
      "importers:\n  .:\n    devDependencies:\n      nx:\n        version: 1.0.0\n",
    )
    .replace(
      "packages:\n",
      "packages:\n  nx@1.0.0:\n    resolution: {integrity: sha512-nx}\n",
    )
    .replace("snapshots:\n", "snapshots:\n  nx@1.0.0: {}\n");
  write(repo, "pnpm-lock.yaml", initial);
  commit(repo);
  const before = Effect.runSync(imageInputs(repo));
  write(
    repo,
    "pnpm-lock.yaml",
    initial
      .replaceAll("nx@1.0.0", "nx@2.0.0")
      .replace(
        "version: 1.0.0\n  packages/router",
        "version: 2.0.0\n  packages/router",
      ),
  );
  commit(repo);
  const after = Effect.runSync(imageInputs(repo));
  assert.notEqual(after.registry?.key, before.registry?.key);
  assert.notEqual(after.router?.key, before.router?.key);
});

await test("host hash returns malformed tracked manifest failures", (t) => {
  const repo = fixture();
  t.after(() => {
    rmSync(repo, { recursive: true, force: true });
  });
  write(repo, "packages/router/package.json", "{broken");
  commit(repo);
  const result = hostCodeHashOfPackage(repo, "packages/router");
  assert.ok(typeof result !== "string");
  assert.match(result.message, /SyntaxError/u);
});
