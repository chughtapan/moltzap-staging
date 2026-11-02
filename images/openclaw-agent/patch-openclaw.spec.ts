/** @file The OpenClaw patch refuses an empty dist and a command without a dist argument, without using an installed OpenClaw host. */

import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { applyOpenClawPatch } from "./patch-openclaw.ts";

const script = fileURLToPath(new URL("./patch-openclaw.ts", import.meta.url));
const run = promisify(execFile);

test("a dist that carries none of the anchors is refused", async () => {
  const root = await mkdtemp(join(tmpdir(), "openclaw-patch-test-"));
  try {
    await assert.rejects(applyOpenClawPatch(root), /gate: .* found nothing/u);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("the command refuses a call without a dist", async () => {
  await assert.rejects(
    run(process.execPath, [script]),
    /usage: patch-openclaw/u,
  );
});
