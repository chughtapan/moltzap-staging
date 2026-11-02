/** @file Materializes OpenClaw worker archives for patch tests. */
import { execFile } from "node:child_process";
import { mkdir, readdir, rm } from "node:fs/promises";
import { join } from "node:path";
import { promisify } from "node:util";
const run = promisify(execFile);
/** Expands the workers shipped by OpenClaw into the isolated test copy. */
export async function prepareOpenClawDist(dist: string) {
  const archives = join(dist, "worker-artifacts");
  const target = join(dist, "worker");
  await rm(target, { recursive: true, force: true });
  await mkdir(target, { recursive: true });
  for (const entry of await readdir(archives, { withFileTypes: true })) {
    if (entry.isFile() && entry.name.endsWith(".tar.gz")) {
      await run("tar", [
        "--extract",
        "--gzip",
        "--file",
        join(archives, entry.name),
        "--directory",
        target,
      ]);
    }
  }
}
