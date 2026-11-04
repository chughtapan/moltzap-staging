/** @file Committed image inputs determine which image content changes. */

import { Effect, Either } from "effect";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- Synchronous filesystem fixtures exercise committed inputs and filesystem permission failures.
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
  clientManifest,
  commit,
  daemonDockerfile,
  dockerfile,
  fixture,
  git,
  hostDockerfile,
  key,
  lock,
  write,
} from "./image-input-key.fixture.ts";
import {
  ImageInputError,
  imageInputs,
  runtimeVersionReads,
} from "./image-input-key.ts";
import { hostCodeHashOfPackage } from "./loaded-plugin-code-hash.ts";

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
    {
      exports: { "./internal": { "sharness-internal": "./dist/private.js" } },
    },
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
      write(repo, `packages/${service}/src/index.spec.ts`, "export {};");
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
      JSON.stringify({
        ...manifest,
        devDependencies: { unused: "catalog:" },
      }),
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
          options: {
            cwd: ".",
            commands: [{ command: "node tools/extra.ts" }],
          },
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
const buildTsconfig =
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
    buildTsconfig,
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

// @agent-code-guard/regression-only: Git fixtures pin rebuild behavior through the exported release API.

for (const instruction of [
  "COPY images/openclaw-agent/entrypoint.sh /opt/social-harness/agent/",
  "RUN --mount=type=bind,source=assets,target=/probe cat /probe/config",
  "RUN --network=none --mount=type=bind,source=assets,target=/probe cat /probe/config",
]) {
  await test(`host recipe refuses unaccounted runtime context input: ${instruction}`, (t) => {
    const repo = fixture();
    t.after(() => {
      rmSync(repo, { recursive: true, force: true });
    });
    write(
      repo,
      "images/openclaw-agent/Dockerfile",
      `${hostDockerfile}\n${instruction}\n`,
    );
    commit(repo);
    const result = Effect.runSync(
      Effect.either(imageInputs(repo, ["openclaw-agent"])),
    );
    Either.match(result, {
      onLeft: (error) => {
        assert.match(
          error.message,
          /Unsupported Dockerfile form in images\/openclaw-agent\/Dockerfile/u,
        );
      },
      onRight: () => {
        assert.fail("Expected refusal");
      },
    });
  });
}

for (const [file, content, image] of [
  [
    "packages/social-harness/src/harness/run.ts",
    "export const daemon = 42;",
    "daemon",
  ],
  [
    "packages/social-harness/src/harness/run.ts",
    "export const daemon = 42;",
    "openclaw-agent",
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
  [
    "plugins/openclaw/src/index.ts",
    "export const channel = 42;",
    "openclaw-agent",
  ],
  [
    "plugins/openclaw/bin/loader",
    "#!/usr/bin/env node\nconsole.log('updated');",
    "openclaw-agent",
  ],
  [
    "plugins/openclaw/skills/connect/SKILL.md",
    "Updated skill instructions.",
    "openclaw-agent",
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
      },
      files: ["package.json"],
      nx: { targets: { build: { executor: "nx:noop" } } },
      description: "installed daemon",
    }),
    "daemon",
  ],
  [
    "images/openclaw-agent/entrypoint.sh",
    "exec openclaw gateway --verbose",
    "openclaw-agent",
  ],
  [
    "images/openclaw-agent/Dockerfile",
    `${hostDockerfile}\n# updated host recipe\n`,
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
    if (file.startsWith("plugins/openclaw/")) {
      assert.equal(after.daemon?.key, before.daemon?.key);
    }
  });
}

for (const [file, content, image] of [
  [
    "images/openclaw-agent/README.md",
    "Run the host with care.\n",
    "openclaw-agent",
  ],
  [
    "packages/social-harness/src/index.spec.ts",
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

await test("installed daemon and host keys bind transitive runtime integrity", (t) => {
  const repo = fixture();
  t.after(() => {
    rmSync(repo, { recursive: true, force: true });
  });
  const before = Effect.runSync(imageInputs(repo));
  write(repo, "pnpm-lock.yaml", lock.replace("sha512-leaf", "sha512-new-leaf"));
  commit(repo);
  const after = Effect.runSync(imageInputs(repo));
  assert.notEqual(after.daemon?.key, before.daemon?.key);
  assert.notEqual(after["openclaw-agent"]?.key, before["openclaw-agent"]?.key);
});

for (const { file, content, changesKey } of [
  {
    file: "mcp-client.ts",
    content: "export const client = 2;",
    changesKey: true,
  },
  {
    file: "inbox-wakeups.ts",
    content: "export const inbox = 2;",
    changesKey: true,
  },
  {
    file: "mcp.spec.ts",
    content: "export const test = 2;",
    changesKey: false,
  },
  {
    file: "README.md",
    content: "Build support documentation.",
    changesKey: false,
  },
]) {
  await test(`host key ${changesKey ? "binds" : "excludes"} build-only workspace ${file} without publishing it`, (t) => {
    const repo = buildOnlyWorkspace();
    t.after(() => {
      rmSync(repo, { recursive: true, force: true });
    });
    const before = Effect.runSync(imageInputs(repo));
    write(repo, `plugins/build-support/${file}`, content);
    commit(repo);
    const after = Effect.runSync(imageInputs(repo));
    assert.equal(
      after["openclaw-agent"]?.key !== before["openclaw-agent"]?.key,
      changesKey,
    );
    assert.equal(after.router?.key, before.router?.key);
    assert.equal(after.registry?.key, before.registry?.key);
    assert.ok(after["openclaw-agent"]);
    assert.ok(
      after["openclaw-agent"].modules.every(
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
      include: ["mcp-client.ts", "inbox-wakeups.ts"],
      exclude: ["*.spec.ts"],
    }),
  );
  write(
    repo,
    "plugins/build-support/mcp-client.ts",
    "export const client = 1;",
  );
  write(
    repo,
    "plugins/build-support/inbox-wakeups.ts",
    "export const inbox = 1;",
  );
  write(repo, "plugins/build-support/mcp.spec.ts", "export const test = 1;");
  write(repo, "plugins/build-support/README.md", "Build support.");
  write(
    repo,
    "images/openclaw-agent/Dockerfile",
    hostDockerfile.replace(
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
