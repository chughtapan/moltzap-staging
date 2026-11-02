/** @file The generic OpenClaw patch produces JavaScript that Node can parse. */

import { execFile } from "node:child_process";
import { cp, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { prepareOpenClawDist } from "../tests/openclaw-gateway-fixture.ts";
import { applyOpenClawPatch } from "./patch-openclaw.ts";

/** OpenClaw's dist as the image's shared dependency installs it. */
const installedDist = fileURLToPath(
  new URL("node_modules/openclaw/dist/", import.meta.url),
);

const run = promisify(execFile);

test("every bundle the patch rewrites still parses", async () => {
  const root = await mkdtemp(join(tmpdir(), "openclaw-patch-syntax-"));
  try {
    await cp(installedDist, root, { recursive: true });
    await prepareOpenClawDist(root);
    const applied = await applyOpenClawPatch(root);
    await Promise.all(
      [...new Set(applied.flatMap((edit) => edit.files))].map((file) =>
        run(process.execPath, ["--check", join(root, file)]),
      ),
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
