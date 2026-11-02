/** @file Channel configuration writes remain restartable after an abandoned temporary write. */

import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import {
  mkdir,
  mkdtemp,
  readdir,
  readFile,
  rm,
  symlink,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const run = promisify(execFile);
const script = fileURLToPath(
  new URL("./configure-channel.ts", import.meta.url),
);

test("abandoned channel writes do not block installing the image channel", async (context) => {
  const root = await mkdtemp(join(tmpdir(), "social-harness-channel-config-"));
  context.after(() => rm(root, { recursive: true, force: true }));
  const path = join(root, "openclaw.json");
  const stale = `${path}.channel-${String(process.pid)}`;
  const abandoned = join(root, ".social-harness-channel-abandoned");
  await writeFile(stale, "unfinished configuration");
  await mkdir(abandoned);
  await writeFile(join(abandoned, "openclaw.json"), "unfinished configuration");
  await writeFile(
    path,
    JSON.stringify({
      plugins: {
        load: {
          paths: [],
        },
      },
    }),
  );
  await symlink(root, join(root, "current"));

  await assert.doesNotReject(
    run(process.execPath, [script], {
      env: {
        ...process.env,
        SHARNESS_LAUNCHER_DIRECTORY: root,
        OPENCLAW_CONFIG_PATH: path,
      },
    }),
  );

  assert.deepEqual(JSON.parse(await readFile(path, "utf8")), {
    plugins: {
      load: {
        paths: [`${root}/current/node_modules/@moltzap/openclaw-channel`],
      },
    },
  });
  assert.deepEqual(
    (await readdir(root)).sort((left, right) => left.localeCompare(right)),
    [
      ".social-harness-channel-abandoned",
      "current",
      "openclaw.json",
      `openclaw.json.channel-${String(process.pid)}`,
    ],
  );
  assert.equal(await readFile(stale, "utf8"), "unfinished configuration");
});

test("a failed configuration rename keeps the old configuration and removes temporary files", async (context) => {
  const root = await mkdtemp(join(tmpdir(), "social-harness-channel-config-"));
  context.after(() => rm(root, { recursive: true, force: true }));
  const path = join(root, "openclaw.json");
  const original = JSON.stringify({
    plugins: {
      load: { paths: [] },
    },
  });
  await writeFile(path, original);
  await symlink(root, join(root, "current"));
  const preload = `
    import filesystem from "node:fs/promises";
    import { syncBuiltinESMExports } from "node:module";
    filesystem.rename = () => Promise.reject(new Error("injected configuration rename failure"));
    syncBuiltinESMExports();
  `;

  await assert.rejects(
    run(
      process.execPath,
      [
        "--import",
        `data:text/javascript,${encodeURIComponent(preload)}`,
        script,
      ],
      {
        env: {
          ...process.env,
          SHARNESS_LAUNCHER_DIRECTORY: root,
          OPENCLAW_CONFIG_PATH: path,
        },
      },
    ),
    { code: 1 },
  );

  assert.equal(await readFile(path, "utf8"), original);
  assert.deepEqual(await readdir(root), ["current", "openclaw.json"]);
});

test("initializes channel loading in a stock host configuration", async (context) => {
  const root = await mkdtemp(join(tmpdir(), "social-harness-stock-config-"));
  context.after(() => rm(root, { recursive: true, force: true }));
  const path = join(root, "openclaw.json");
  await writeFile(
    path,
    JSON.stringify({ channels: { sharness: { enabled: true } } }),
  );
  await symlink(root, join(root, "current"));
  await assert.doesNotReject(
    run(process.execPath, [script], {
      env: {
        ...process.env,
        SHARNESS_LAUNCHER_DIRECTORY: root,
        OPENCLAW_CONFIG_PATH: path,
      },
    }),
  );
  assert.partialDeepStrictEqual(JSON.parse(await readFile(path, "utf8")), {
    plugins: {
      load: {
        paths: [`${root}/current/node_modules/@moltzap/openclaw-channel`],
      },
    },
  });
});
