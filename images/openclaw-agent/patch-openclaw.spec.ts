/** @file Patch behavior, refusal without mutation and generated JavaScript validity. */

import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import {
  cp,
  mkdtemp,
  readdir,
  readFile,
  rm,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { prepareOpenClawDist } from "./dist-fixture.ts";
import { applyOpenClawPatch, type Edit, EDITS } from "./patch-openclaw.ts";

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

/** OpenClaw's dist from the image workspace's installed release. */
const installedDist = fileURLToPath(
  new URL("node_modules/openclaw/dist/", import.meta.url),
);

test("a patched dist is refused and left as it was", async () => {
  const root = await stagedDist();
  try {
    await applyOpenClawPatch(root);
    const patched = await snapshot(root);
    await assert.rejects(applyOpenClawPatch(root), /anchors do not match/u);
    assert.deepEqual(await snapshot(root), patched);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("an anchor found in multiple files is refused", async () => {
  const root = await stagedDist();
  try {
    await writeFile(join(root, "copy.mjs"), editNamed("gate").before);
    await assert.rejects(
      applyOpenClawPatch(root),
      /gate: expected the anchor once in one file, found .+ x1, .+ x1/u,
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("the command records the base image and every applied edit in the marker", async () => {
  const root = await stagedDist();
  try {
    const marker = join(root, "marker", "openclaw-patch.json");
    await run(process.execPath, [
      script,
      root,
      "--base-image",
      "example/openclaw@sha256:0",
      "--marker",
      marker,
    ]);
    const written = decodeMarker(JSON.parse(await readFile(marker, "utf8")));
    assert.equal(written.baseImage, "example/openclaw@sha256:0");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("a dry run reports the edits and leaves the bundles unchanged", async () => {
  const root = await stagedDist();
  try {
    const before = await snapshot(root);
    await applyOpenClawPatch(root, { dryRun: true });
    assert.deepEqual(await snapshot(root), before);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

/**
 * An isolated copy of the installed release with its packaged workers
 * materialized as the image ships them. The caller removes it.
 */
async function stagedDist(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), "openclaw-patch-test-"));
  await cp(installedDist, root, { recursive: true });
  await prepareOpenClawDist(root);
  return root;
}

/** Every file under `root` with its text. */
async function snapshot(root: string): Promise<Map<string, string>> {
  const names = await readdir(root, { recursive: true, withFileTypes: true });
  const files = names
    .filter((entry) => entry.isFile())
    .map((entry) => join(entry.parentPath, entry.name));
  return new Map(
    await Promise.all(
      files.map(
        async (file): Promise<[string, string]> => [
          file,
          await readFile(file, "utf8"),
        ],
      ),
    ),
  );
}

/** The patch's edit called `id`; a test that names an absent edit fails here. */
function editNamed(id: string): Edit {
  const edit = EDITS.find((candidate) => candidate.id === id);
  assert.ok(edit, `the patch has no edit ${id}`);
  return edit;
}

function decodeMarker(value: unknown) {
  assert.ok(
    typeof value === "object" && value !== null && "baseImage" in value,
  );
  return value;
}

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
