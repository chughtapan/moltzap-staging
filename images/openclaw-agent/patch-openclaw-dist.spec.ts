/** @file The OpenClaw patch applies once to the installed dist, protects bundle anchors, records its marker, and leaves bundles unchanged on a dry run. */

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
import { prepareOpenClawDist } from "../tests/openclaw-gateway-fixture.ts";
import { isRecord } from "../tests/test-support.ts";
import { applyOpenClawPatch, type Edit, EDITS } from "./patch-openclaw.ts";

/** OpenClaw's dist as the catalog's `openclaw` release installs it for images/tests. */
const installedDist = fileURLToPath(
  new URL("node_modules/openclaw/dist/", import.meta.url),
);

const script = fileURLToPath(new URL("./patch-openclaw.ts", import.meta.url));

const run = promisify(execFile);

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

test("the update recovery helper is neither counted nor edited", async () => {
  const root = await stagedDist();
  try {
    const helper = join(root, "package-update-activation-recovery.mjs");
    const anchor = editNamed("gate").before;
    await writeFile(helper, anchor);
    await applyOpenClawPatch(root);
    assert.equal(await readFile(helper, "utf8"), anchor);
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
    assert.deepEqual(
      new Set(written.editIds),
      new Set(EDITS.map((edit) => edit.id)),
    );
    assert.equal(written.editIds.length, EDITS.length);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("a dry run reports the edits and leaves the bundles unchanged", async () => {
  const root = await stagedDist();
  try {
    const before = await snapshot(root);
    const applied = await applyOpenClawPatch(root, { dryRun: true });
    assert.equal(applied.length, EDITS.length);
    assert.deepEqual(await snapshot(root), before);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

/** The marker's base image and the ids of the edits it records. */
interface DecodedMarker {
  readonly baseImage: unknown;
  readonly editIds: unknown[];
}

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

/** Reads the marker file's JSON, failing the test when its shape is wrong. */
function decodeMarker(value: unknown): DecodedMarker {
  assert.ok(isRecord(value), "the marker is a JSON object");
  const edits = value.edits;
  assert.ok(Array.isArray(edits), "the marker lists its edits");
  return {
    baseImage: value.baseImage,
    editIds: edits.map((edit: unknown) =>
      isRecord(edit) ? edit.id : undefined,
    ),
  };
}
