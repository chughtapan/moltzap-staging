#!/usr/bin/env node
/** @file Image key contracts run in process against committed temporary workspaces. */
import { Effect, Either } from "effect";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- Synchronous Git fixtures exercise the in-process release API.
import {
  chmodSync,
  closeSync,
  openSync,
  readFileSync,
  rmSync,
  symlinkSync,
} from "node:fs";
import path from "node:path";
import { test } from "node:test";
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

// @agent-code-guard/regression-only: Git fixtures pin image rebuild and refusal behavior through the release API.

const command = path.join(import.meta.dirname, "image-input-key.ts");
/** Regular-file output also works where the sandbox denies subprocess pipe polling. */
function run(repo: string, services: readonly string[] = []) {
  const out = path.join(repo, ".git", "key.stdout");
  const err = path.join(repo, ".git", "key.stderr");
  const stdout = openSync(out, "w");
  const stderr = openSync(err, "w");
  const result = spawnSync(process.execPath, [command, ...services], {
    cwd: repo,
    stdio: ["ignore", stdout, stderr],
    timeout: 30_000,
  });
  closeSync(stdout);
  closeSync(stderr);
  assert.ifError(result.error);
  return {
    status: result.status,
    stdout: readFileSync(out, "utf8"),
    stderr: readFileSync(err, "utf8"),
  };
}

const changes = [
  [
    "test-only sibling module",
    "packages/social-harness/src/test.ts",
    "export const testOnly = 2;",
    false,
    false,
  ],
  [
    "root pnpm installation policy",
    "package.json",
    JSON.stringify({
      packageManager: "pnpm@10.6.2",
      pnpm: { onlyBuiltDependencies: ["native"] },
      scripts: { prepare: "node scripts/setup/install.ts" },
    }),
    true,
    true,
  ],
  ["npm configuration", ".npmrc", "strict-peer-dependencies=true", true, true],
  ["Docker ignore rules", ".dockerignore", "node_modules\n.tmp", true, true],
  ["Nx configuration", "nx.json", '{"parallel":2}', true, true],
  [
    "base TypeScript config",
    "tsconfig.base.json",
    '{"extends":"./config/base.json","compilerOptions":{}}',
    true,
    true,
  ],
  [
    "inherited TypeScript config",
    "config/base.json",
    '{"compilerOptions":{"strict":true}}',
    true,
    true,
  ],
  [
    "sibling TypeScript config",
    "packages/social-harness/tsconfig.json",
    '{"extends":"../../tsconfig.base.json","compilerOptions":{"rootDir":"src","outDir":"dist","strict":true}}',
    true,
    true,
  ],
  ["Node toolchain", ".node-version", "24.19.0", true, true],
  [
    "build command file outside scripts/build",
    "tools/image-task.ts",
    "export const changed = true;",
    true,
    true,
  ],
  [
    "install lifecycle executable",
    "scripts/setup/install.ts",
    "export const changed = true;",
    true,
    true,
  ],
  [
    "daemon-only source",
    "packages/social-harness/src/harness/run.ts",
    "export const daemon = 2;",
    false,
    false,
  ],
  [
    "Router source",
    "packages/router/src/index.ts",
    'import "@fixture/core/internal"; export const router = 2;',
    true,
    false,
  ],
  [
    "Dockerfile",
    "images/router/Dockerfile",
    `${dockerfile}\n# image configuration\n`,
    true,
    false,
  ],
  [
    "runtime external bump",
    "pnpm-lock.yaml",
    lock
      .replaceAll("  leaf@1.0.0:", "  leaf@1.0.1:")
      .replace("version: 1.0.0\npackages:", "version: 1.0.1\npackages:"),
    true,
    true,
  ],
  [
    "transitive runtime integrity",
    "pnpm-lock.yaml",
    lock.replace("sha512-peer", "sha512-new"),
    true,
    false,
  ],
  [
    "condition-only internal module",
    "packages/social-harness/src/private.ts",
    "export const privateValue = 2;",
    true,
    true,
  ],
  [
    "default-condition module",
    "packages/social-harness/src/index.ts",
    "export const publicValue = 2;",
    true,
    true,
  ],
  [
    "version-only manifest",
    "packages/router/package.json",
    JSON.stringify({
      name: "@fixture/router",
      version: "2",
      dependencies: { "@fixture/core": "workspace:*" },
    }),
    false,
    false,
  ],
  [
    "sibling version-only manifest",
    "packages/social-harness/package.json",
    "",
    false,
    false,
  ],
  [
    "unrelated dev dependency",
    "pnpm-lock.yaml",
    lock
      .replaceAll("unused@1.0.0", "unused@2.0.0")
      .replaceAll("dev-leaf@1.0.0", "dev-leaf@2.0.0")
      .replaceAll(
        "version: 1.0.0\n  packages/registry",
        "version: 2.0.0\n  packages/registry",
      )
      .replace("dev-leaf: 1.0.0", "dev-leaf: 2.0.0"),
    false,
    false,
  ],
  [
    "build script",
    "scripts/build/image.ts",
    "export const build = 2;",
    true,
    true,
  ],
  [
    "pnpm release",
    "package.json",
    JSON.stringify({
      packageManager: "pnpm@10.6.3",
      pnpm: { onlyBuiltDependencies: [] },
      scripts: { prepare: "node scripts/setup/install.ts" },
    }),
    true,
    true,
  ],
] as const;

for (const [label, file, content, changesKey, registryChanges] of changes) {
  await test(`Router key ${changesKey ? "changes" : "stays equal"} for ${label}`, (t) => {
    const repo = fixture();
    t.after(() => {
      rmSync(repo, { recursive: true, force: true });
    });
    const beforeRegistry = Effect.runSync(imageInputs(repo)).registry?.key;
    const before = key(repo);
    const text =
      content ||
      readFileSync(path.join(repo, file), "utf8").replace(
        '"version":"1"',
        '"version":"2"',
      );
    write(repo, file, text);
    commit(repo);
    assert.equal(
      Effect.runSync(imageInputs(repo)).registry?.key === beforeRegistry,
      !registryChanges,
      `Registry key for ${label}`,
    );
    if (changesKey) {
      assert.notEqual(key(repo), before);
    } else {
      assert.equal(key(repo), before);
    }
  });
}

await test("uncommitted edits do not change a key, while committed executable mode does", (t) => {
  const repo = fixture();
  t.after(() => {
    rmSync(repo, { recursive: true, force: true });
  });
  const before = key(repo);
  write(repo, "packages/router/src/index.ts", "uncommitted");
  assert.equal(key(repo), before);
  git(repo, ["restore", "packages/router/src/index.ts"]);
  chmodSync(path.join(repo, "scripts/build/image.ts"), 0o755);
  commit(repo);
  assert.notEqual(key(repo), before);
});

await test("refuses an export whose source is absent", (t) => {
  const repo = fixture();
  t.after(() => {
    rmSync(repo, { recursive: true, force: true });
  });
  write(
    repo,
    "packages/social-harness/package.json",
    JSON.stringify({
      name: "@fixture/core",
      exports: { ".": "./dist/absent.js" },
    }),
  );
  commit(repo);
  const result = run(repo);
  assert.notEqual(result.status, 0);
  assert.equal(result.stdout, "");
  assert.match(
    result.stderr,
    /Missing workspace export target packages\/social-harness\/dist\/absent.js/u,
  );
});

for (const [label, content, message] of [
  [
    "unknown lockfile version",
    lock.replace("9.0", "10.0"),
    /Unsupported pnpm-lock.yaml version/u,
  ],
  [
    "missing importer",
    lock.replace("packages/social-harness:", "packages/absent:"),
    /no importer packages\/social-harness/u,
  ],
  [
    "missing snapshot",
    lock.replace("  leaf@1.0.0: {}", "  absent@1.0.0: {}"),
    /no snapshot leaf@1.0.0/u,
  ],
  [
    "missing resolution",
    lock.replace(
      "  leaf@1.0.0:\n    resolution: {integrity: sha512-leaf}\n",
      "",
    ),
    /no package leaf@1.0.0/u,
  ],
] as const) {
  await test(`refuses ${label}`, (t) => {
    const repo = fixture();
    t.after(() => {
      rmSync(repo, { recursive: true, force: true });
    });
    write(repo, "pnpm-lock.yaml", content);
    commit(repo);
    const result = Effect.runSync(Effect.either(imageInputs(repo)));
    Either.match(result, {
      onLeft: (error) => {
        assert.match(error.message, message);
      },
      onRight: () => {
        assert.fail("Expected refusal");
      },
    });
  });
}

await test("CLI prints the published image keys", (t) => {
  const repo = fixture();
  t.after(() => {
    rmSync(repo, { recursive: true, force: true });
  });
  const result = run(repo);
  assert.equal(result.status, 0, result.stderr);
  assert.match(result.stdout, /"registry":"sha256:[0-9a-f]{64}"/u);
  assert.match(result.stdout, /"router":"sha256:[0-9a-f]{64}"/u);
  assert.match(result.stdout, /"daemon":"sha256:[0-9a-f]{64}"/u);
  assert.match(result.stdout, /"openclaw-agent":"sha256:[0-9a-f]{64}"/u);
});

await test("CLI prints only the requested services, whatever another recipe holds", (t) => {
  const repo = fixture();
  t.after(() => {
    rmSync(repo, { recursive: true, force: true });
  });
  write(repo, "images/daemon/Dockerfile", "FROM node:24-slim\n");
  commit(repo);
  const all = run(repo);
  assert.match(
    all.stderr,
    /Unsupported Dockerfile form in images\/daemon\/Dockerfile/u,
  );
  const result = run(repo, ["registry", "router"]);
  assert.equal(result.status, 0, result.stderr);
  assert.match(
    result.stdout,
    /^\{"registry":"sha256:[0-9a-f]{64}","router":"sha256:[0-9a-f]{64}"\}\n$/u,
  );
});

await test("CLI refuses an unknown service", (t) => {
  const repo = fixture();
  t.after(() => {
    rmSync(repo, { recursive: true, force: true });
  });
  const result = run(repo, ["registry", "nanoclaw-agent"]);
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /Unknown image service/u);
});

await test("checkout produces well-formed keys in process", () => {
  const keys = Effect.runSync(
    imageInputs(path.resolve(import.meta.dirname, "../..")),
  );
  for (const input of Object.values(keys)) {
    assert.match(input.key, /^sha256:[0-9a-f]{64}$/u);
  }
});

await test("release-shaped metadata edits in own and sibling manifests preserve both keys", (t) => {
  const repo = fixture();
  t.after(() => {
    rmSync(repo, { recursive: true, force: true });
  });
  const before = Effect.runSync(imageInputs(repo, ["registry", "router"]));
  for (const service of ["router", "registry", "social-harness"]) {
    const file = `packages/${service}/package.json`;
    const manifest: unknown = JSON.parse(
      readFileSync(path.join(repo, file), "utf8"),
    );
    assert.ok(typeof manifest === "object" && manifest !== null);
    write(
      repo,
      file,
      JSON.stringify({
        ...manifest,
        version: "2",
        sharnessContentHash: "sha256:new",
        sharnessHostCodeHash: "sha256:new",
        sharnessWireVersion: "new",
      }),
    );
  }
  commit(repo);
  assert.deepEqual(
    Effect.runSync(imageInputs(repo, ["registry", "router"])),
    before,
  );
});

await test("sibling manifest non-version fields bind both images", (t) => {
  const repo = fixture();
  t.after(() => {
    rmSync(repo, { recursive: true, force: true });
  });
  const before = Effect.runSync(imageInputs(repo));
  const file = "packages/social-harness/package.json";
  write(
    repo,
    file,
    readFileSync(path.join(repo, file), "utf8").replace(
      '"version":"1"',
      '"version":"1","type":"module"',
    ),
  );
  commit(repo);
  const after = Effect.runSync(imageInputs(repo));
  assert.notEqual(after.router?.key, before.router?.key);
  assert.notEqual(after.registry?.key, before.registry?.key);
});

await test("runtime version gate names only reached modules consuming manifest versions", (t) => {
  const repo = fixture();
  t.after(() => {
    rmSync(repo, { recursive: true, force: true });
  });
  write(
    repo,
    "packages/router/src/version.ts",
    'import manifest from "../package.json" with { type: "json" }; console.log(manifest.version);',
  );
  write(
    repo,
    "packages/registry/src/version.ts",
    'import { version as release } from "../package.json"; console.log(release);',
  );
  write(
    repo,
    "packages/social-harness/src/harness/run.ts",
    'import manifest from "../../package.json"; console.log(manifest.version);',
  );
  write(
    repo,
    "packages/router/src/reexport.ts",
    'export { version as release } from "../package.json";',
  );
  write(
    repo,
    "packages/social-harness/src/index.ts",
    'import manifest from "../package.json"; console.log(manifest.version);',
  );
  commit(repo);
  assert.deepEqual(Effect.runSync(runtimeVersionReads(repo)), [
    "packages/registry/src/version.ts",
    "packages/router/src/reexport.ts",
    "packages/router/src/version.ts",
  ]);
  assert.match(key(repo), /^sha256:/u);
});

for (const [label, instruction] of [
  ["JSON ADD", 'ADD ["assets/", "/config"]'],
  ["CRLF continuation", "COPY \\\r\n assets/ /assets"],
  ["spaced continuation", "COPY \\ \t\n assets/ /assets"],
  ["comment continuation", "COPY \\\n\n # ignored\n assets/ /assets"],
  ["heredoc RUN", "RUN <<EOF\nnode tools/heredoc-task.ts\nEOF"],
  ["entrypoint", 'ENTRYPOINT ["node", "assets/config"]'],
  ["lowercase copy", "copy assets/ assets/"],
  ["lowercase run", "run pnpm nx run @fixture/router:build"],
] as const) {
  await test(`refuses ${label}`, (t) => {
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
    Either.match(Effect.runSync(Effect.either(imageInputs(repo))), {
      onLeft: (error) => {
        assert.ok(error instanceof ImageInputError);
        assert.match(error.message, /images\/router\/Dockerfile line 2:/u);
      },
      onRight: () => assert.fail("Expected refusal"),
    });
  });
}

await test("refuses a well-formed COPY of an uncommitted source at its line", (t) => {
  const repo = fixture();
  t.after(() => {
    rmSync(repo, { recursive: true, force: true });
  });
  write(
    repo,
    "images/router/Dockerfile",
    dockerfile.replace(
      "COPY package.json",
      "COPY assets/uncommitted assets/uncommitted\nCOPY package.json",
    ),
  );
  commit(repo);
  write(repo, "assets/uncommitted", "uncommitted");
  Either.match(Effect.runSync(Effect.either(imageInputs(repo))), {
    onLeft: (error) => {
      assert.ok(error instanceof ImageInputError);
      assert.match(
        error.message,
        /images\/router\/Dockerfile line 2: COPY assets\/uncommitted assets\/uncommitted/u,
      );
    },
    onRight: () => assert.fail("Expected refusal"),
  });
});

await test("refuses symlink image inputs", (t) => {
  const repo = fixture();
  t.after(() => {
    rmSync(repo, { recursive: true, force: true });
  });
  symlinkSync(
    "../../../../outside.ts",
    path.join(repo, "packages/router/src/link.ts"),
  );
  commit(repo);
  const result = Effect.runSync(Effect.either(imageInputs(repo)));
  Either.match(result, {
    onLeft: (error) => {
      assert.match(error.message, /symlink image input/u);
    },
    onRight: () => {
      assert.fail("Expected refusal");
    },
  });
});

for (const [label, manifest, source, mutation] of [
  [
    "constant dynamic import",
    { exports: { "./internal": { "sharness-internal": "./dist/private.js" } } },
    "await import(`@fixture/core/internal`);",
    "packages/social-harness/src/private.ts",
  ],
  [
    "active require conditional target",
    { exports: { ".": { require: "./dist/only-require.js" } } },
    'require("@fixture/core");',
    "packages/social-harness/src/only-require.ts",
  ],
  [
    "active node conditional target",
    {
      exports: {
        ".": {
          node: "./dist/only-node.js",
          "module-sync": "./dist/only-sync.js",
          default: "./dist/only-default.js",
        },
      },
    },
    'import "@fixture/core";',
    "packages/social-harness/src/only-node.ts",
  ],
  [
    "active module-sync conditional target",
    {
      exports: {
        ".": {
          node: "./dist/only-node.js",
          "module-sync": "./dist/only-sync.js",
          default: "./dist/only-default.js",
        },
      },
    },
    'import "@fixture/core";',
    "packages/social-harness/src/only-sync.ts",
  ],
  [
    "active default conditional target",
    {
      exports: {
        ".": {
          node: "./dist/only-node.js",
          "module-sync": "./dist/only-sync.js",
          default: "./dist/only-default.js",
        },
      },
    },
    'import "@fixture/core";',
    "packages/social-harness/src/only-default.ts",
  ],
  [
    "legacy main",
    { main: "./dist/private.js" },
    'import "@fixture/core";',
    "packages/social-harness/src/private.ts",
  ],
  [
    "wildcard subpaths",
    { exports: { "./*": "./dist/*.js" } },
    'import "@fixture/core/private";',
    "packages/social-harness/src/private.ts",
  ],
  [
    "package imports aliases",
    {
      exports: { ".": "./dist/index.js" },
      imports: { "#private": "./dist/private.js" },
    },
    'import "@fixture/core";',
    "packages/social-harness/src/private.ts",
  ],
] as const) {
  await test(`${label} reach their runtime modules`, (t) => {
    const repo = fixture();
    t.after(() => {
      rmSync(repo, { recursive: true, force: true });
    });
    const file = "packages/social-harness/package.json";
    const original: unknown = JSON.parse(
      readFileSync(path.join(repo, file), "utf8"),
    );
    assert.ok(typeof original === "object" && original !== null);
    write(
      repo,
      file,
      JSON.stringify({ ...original, exports: undefined, ...manifest }),
    );
    for (const service of ["router", "registry"]) {
      write(repo, `packages/${service}/src/index.ts`, source);
      write(repo, `packages/${service}/src/index.test.ts`, "export {};");
    }
    write(repo, "packages/social-harness/src/internal.ts", "export {};");
    write(repo, "packages/social-harness/src/index.ts", 'import "#private";');
    if (label !== "package imports aliases") {
      write(repo, "packages/social-harness/src/index.ts", "export {};");
    }
    commit(repo);
    const before = key(repo);
    write(repo, mutation, "export const changed = true;");
    commit(repo);
    assert.notEqual(key(repo), before);
  });
}

for (const [label, content, message] of [
  [
    "file directory dependency",
    lock.replace(
      "version: external@1.0.0(peer@2.0.0)",
      "version: file:../../vendor",
    ),
    /Directory dependency/u,
  ],
  [
    "directory resolution",
    lock.replace(
      "resolution: {integrity: sha512-leaf}",
      "resolution: {type: directory, directory: vendor}",
    ),
    /Directory dependency/u,
  ],
] as const) {
  await test(`refuses ${label} in process`, (t) => {
    const repo = fixture();
    t.after(() => {
      rmSync(repo, { recursive: true, force: true });
    });
    write(repo, "pnpm-lock.yaml", content);
    commit(repo);
    Either.match(Effect.runSync(Effect.either(imageInputs(repo))), {
      onLeft: (error) => {
        assert.match(error.message, message);
      },
      onRight: () => {
        assert.fail("Expected refusal");
      },
    });
  });
}

await test("missing workspace subpaths and malformed manifests retain file-specific typed errors", (t) => {
  const repo = fixture();
  t.after(() => {
    rmSync(repo, { recursive: true, force: true });
  });
  write(
    repo,
    "packages/router/src/index.ts",
    'import "@fixture/core/missing";',
  );
  commit(repo);
  Either.match(Effect.runSync(Effect.either(imageInputs(repo))), {
    onLeft: (error) => {
      assert.match(
        error.message,
        /^Missing workspace export target packages\/social-harness\/package.json \(@fixture\/core\/missing\)$/u,
      );
    },
    onRight: () => {
      assert.fail("Expected refusal");
    },
  });
  write(repo, "packages/router/package.json", "{broken");
  commit(repo);
  Either.match(Effect.runSync(Effect.either(imageInputs(repo))), {
    onLeft: (error) => {
      assert.match(
        error.message,
        /Invalid JSON in packages\/router\/package.json/u,
      );
      assert.ok(error.cause instanceof SyntaxError);
    },
    onRight: () => {
      assert.fail("Expected refusal");
    },
  });
});

await test("root pnpm options in Docker RUN are refused", (t) => {
  const repo = fixture();
  t.after(() => {
    rmSync(repo, { recursive: true, force: true });
  });
  write(repo, "scripts/custom/task.ts", "export {};");
  write(
    repo,
    "package.json",
    JSON.stringify({
      packageManager: "pnpm@10.6.2",
      scripts: { custom: "node scripts/custom/task.ts" },
    }),
  );
  write(
    repo,
    "images/router/Dockerfile",
    `${dockerfile}\nRUN pnpm -w --silent --workspace-root run --silent custom\n`,
  );
  commit(repo);
  Either.match(Effect.runSync(Effect.either(imageInputs(repo))), {
    onLeft: (error) => {
      assert.ok(error instanceof ImageInputError);
    },
    onRight: () => assert.fail("Expected refusal"),
  });
});

await test("copied directory symlinks hash the link without resolving it", (t) => {
  const repo = fixture();
  t.after(() => {
    rmSync(repo, { recursive: true, force: true });
  });
  write(repo, "assets/linked", "initial");
  symlinkSync("../../../assets", path.join(repo, "packages/router/src/link"));
  commit(repo);
  const before = key(repo);
  write(repo, "assets/linked", "changed");
  commit(repo);
  assert.equal(key(repo), before);
});

for (const route of ["relative import", "symlink module"] as const) {
  await test(`${route} outside the service keys transitive modules and version reads`, (t) => {
    const repo = fixture();
    t.after(() => {
      rmSync(repo, { recursive: true, force: true });
    });
    write(repo, "packages/shared/entry.ts", 'import "./leaf.js";');
    write(
      repo,
      "packages/shared/leaf.ts",
      "console.log(process.env.npm_package_version);",
    );
    if (route === "relative import") {
      write(
        repo,
        "packages/router/src/index.ts",
        'import "../../shared/entry.js";',
      );
    } else {
      symlinkSync(
        "../../shared/entry.ts",
        path.join(repo, "packages/router/src/link.ts"),
      );
    }
    commit(repo);
    const before = key(repo);
    assert.deepEqual(Effect.runSync(runtimeVersionReads(repo)), []);
    write(repo, "packages/shared/leaf.ts", "export const changed = true;");
    commit(repo);
    assert.notEqual(key(repo), before);
  });
}

for (const [label, header, bin] of [
  ["manifest bin", "", { tool: "bin/tool" }],
  ["Node shebang", "#!/usr/bin/env node\n", undefined],
] as const) {
  await test(`${label} without an extension traverses imports and consumes versions`, (t) => {
    const repo = fixture();
    t.after(() => {
      rmSync(repo, { recursive: true, force: true });
    });
    write(
      repo,
      "packages/router/package.json",
      JSON.stringify({ name: "@fixture/router", bin }),
    );
    write(
      repo,
      "packages/router/bin/tool",
      `${header}import "@fixture/core/service"; console.log(process.env.npm_package_version);`,
    );
    commit(repo);
    const before = key(repo);
    assert.deepEqual(Effect.runSync(runtimeVersionReads(repo)), [
      "packages/router/bin/tool",
    ]);
    write(
      repo,
      "packages/social-harness/src/harness/run.ts",
      "export const changed = true;",
    );
    commit(repo);
    assert.notEqual(key(repo), before);
  });
}

for (const [label, expression] of [
  ["fs string path", 'readFileSync("../package.json", "utf8")'],
  ["fs URL path", 'fs.readFile(new URL("../package.json", import.meta.url))'],
  ["npm version environment", "process.env.npm_package_version"],
  [
    "spaced npm version environment",
    "process.env /* version */ .npm_package_version",
  ],
] as const) {
  await test(`runtime version gate detects ${label}`, (t) => {
    const repo = fixture();
    t.after(() => {
      rmSync(repo, { recursive: true, force: true });
    });
    write(
      repo,
      "packages/router/src/version.ts",
      `console.log(${expression});`,
    );
    commit(repo);
    assert.deepEqual(Effect.runSync(runtimeVersionReads(repo)), [
      "packages/router/src/version.ts",
    ]);
  });
}

for (const [label, instruction, message] of [
  [
    "non-default Docker escape",
    "# escape=`\nCOPY assets/ /assets",
    /images\/router\/Dockerfile line 2:/u,
  ],
  [
    "default-source bind mount",
    "RUN --mount=type=bind,target=/context cat /context/assets/config",
    /images\/router\/Dockerfile line 2:/u,
  ],
  [
    "explicit-source bind mount",
    "RUN --mount=type=bind,source=assets,target=/context cat /context/config",
    /images\/router\/Dockerfile line 2:/u,
  ],
  [
    "malformed JSON ADD",
    'ADD ["assets/",]',
    /images\/router\/Dockerfile line 2:/u,
  ],
  [
    "missing context source",
    "COPY missing/ missing/",
    /images\/router\/Dockerfile line 2: COPY missing\/ missing\//u,
  ],
] as const) {
  await test(`refuses ${label} with a file-specific typed error`, (t) => {
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
    const result = Effect.runSync(Effect.either(imageInputs(repo)));
    Either.match(result, {
      onLeft: (error) => {
        assert.ok(error instanceof ImageInputError);
        assert.match(error.message, message);
      },
      onRight: () => {
        assert.fail("Expected refusal");
      },
    });
  });
}

for (const source of [
  'import { type T } from "@fixture/core/private";',
  'export { type T } from "@fixture/core/private";',
] as const) {
  await test(`${source} retains the runtime edge for keys and version reads`, (t) => {
    const repo = fixture();
    t.after(() => {
      rmSync(repo, { recursive: true, force: true });
    });
    write(repo, "packages/router/src/index.ts", source);
    write(repo, "packages/registry/src/index.ts", "export {};");
    write(
      repo,
      "packages/social-harness/package.json",
      JSON.stringify({
        name: "@fixture/core",
        exports: { "./*": "./dist/*.js" },
      }),
    );
    write(
      repo,
      "packages/social-harness/src/private.ts",
      "export type T = string;",
    );
    commit(repo);
    const before = key(repo);
    write(
      repo,
      "packages/social-harness/src/private.ts",
      "export type T = string; console.log(process.env.npm_package_version);",
    );
    commit(repo);
    assert.notEqual(key(repo), before);
    assert.deepEqual(Effect.runSync(runtimeVersionReads(repo)), []);
  });
}

for (const name of [
  "Dockerfile.release",
  "Dockerfile.json",
  "release.Dockerfile",
  "Containerfile",
  "Containerfile.release",
] as const) {
  await test(`${name} context inputs change the key`, (t) => {
    const repo = fixture();
    t.after(() => {
      rmSync(repo, { recursive: true, force: true });
    });
    write(
      repo,
      `images/router/${name}`,
      dockerfile.replace(
        "COPY package.json",
        "COPY assets/ assets/\nCOPY package.json",
      ),
    );
    commit(repo);
    const before = key(repo);
    write(repo, "assets/config", "changed");
    commit(repo);
    assert.notEqual(key(repo), before);
  });
}

for (const [label, file, content, message] of [
  [
    "a reached workspace importer outside accounted package roots",
    "pnpm-lock.yaml",
    lock
      .replace("packages:\n", "  tests/library: {}\npackages:\n")
      .replace("      peer: 2.0.0", "      library: link:../tests/library"),
    /^Unaccounted workspace importer tests\/library\/package\.json in pnpm-lock\.yaml$/u,
  ],
  [
    "a workspace link outside accounted package roots",
    "pnpm-lock.yaml",
    lock
      .replaceAll("link:../social-harness", "link:../../tests/library")
      .replace("packages/social-harness:", "tests/library:"),
    /^Unaccounted workspace link @fixture\/core to tests\/library\/package\.json in pnpm-lock\.yaml$/u,
  ],
  [
    "an aliased workspace link that module resolution cannot account for",
    "pnpm-lock.yaml",
    lock.replaceAll("'@fixture/core':", "'@fixture/alias':"),
    /@fixture\/alias.*packages\/social-harness\/package.json/u,
  ],
  [
    "a missing reached relative module with its repository path",
    "packages/router/src/index.ts",
    'import "../../shared/missing.js";',
    /packages\/shared\/missing.js/u,
  ],
] as const) {
  await test(`refuses ${label}`, (t) => {
    const repo = fixture();
    t.after(() => {
      rmSync(repo, { recursive: true, force: true });
    });
    write(repo, file, content);
    commit(repo);
    Either.match(Effect.runSync(Effect.either(imageInputs(repo))), {
      onLeft: (error) => {
        assert.ok(error instanceof ImageInputError);
        assert.match(error.message, message);
      },
      onRight: () => {
        assert.fail("Expected refusal");
      },
    });
  });
}

await import("./image-input-key.regression.test.ts");

await import("./image-input-key.agents.test.ts");
