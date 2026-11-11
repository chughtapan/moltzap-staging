/** @file Channel configuration adds the image channel and replaces the file whole or not at all. */

import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdtemp, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const run = promisify(execFile);
const script = fileURLToPath(
  new URL("./configure-channel.ts", import.meta.url),
);

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
          OPENCLAW_CONFIG_PATH: path,
        },
      },
    ),
    { code: 1 },
  );

  assert.equal(await readFile(path, "utf8"), original);
  assert.deepEqual(await readdir(root), ["openclaw.json"]);
});

test("initializes channel loading in a stock host configuration", async (context) => {
  const root = await mkdtemp(join(tmpdir(), "social-harness-stock-config-"));
  context.after(() => rm(root, { recursive: true, force: true }));
  const path = join(root, "openclaw.json");
  await writeFile(
    path,
    JSON.stringify({ channels: { sharness: { enabled: true } } }),
  );
  await assert.doesNotReject(
    run(process.execPath, [script], {
      env: {
        ...process.env,
        OPENCLAW_CONFIG_PATH: path,
      },
    }),
  );
  assert.partialDeepStrictEqual(JSON.parse(await readFile(path, "utf8")), {
    plugins: {
      load: {
        paths: [
          "/opt/social-harness/node_modules/@social-harness/openclaw-channel",
        ],
      },
    },
  });
  assert.deepEqual(await readdir(root), ["openclaw.json"]);
});
