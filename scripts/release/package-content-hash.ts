#!/usr/bin/env node
/**
 * @file Hashes package contents selected by pnpm pack.
 * Usage: `node scripts/release/package-content-hash.ts DIR` or `--tarball FILE`.
 * DIR is repository-relative. Prints `sha256:` and a hex digest. The tarball
 * form reads no workspace and needs no installation.
 *
 * The release records `sharnessContentHash` to detect changes. The hash covers
 * packed paths, executable bits and file bytes. Manifests omit version and the
 * recorded hashes, but retain workspace dependency pins, so a sibling release
 * changes its dependents' hashes.
 *
 * Run from the repository root after `pnpm build`. Packing or extraction failure
 * exits 1.
 */
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
  mkdirSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";

import { readCompatibilityVersion } from "./read-wire-version.ts";

/** The manifest field the release writes the content hash into. */
export const CONTENT_HASH_FIELD = "sharnessContentHash";

/** The manifest field the release writes the host-code hash into. */
export const HOST_CODE_HASH_FIELD = "sharnessHostCodeHash";

/** One failed packing step, with the output of the command that failed. */
export interface PackFailure {
  readonly kind: "pack-failed";
  readonly message: string;
}

/** Whether a hashed manifest keeps its workspace dependency entries. */
export type ManifestView =
  | { readonly kind: "keep" }
  | { readonly kind: "drop"; readonly names: ReadonlySet<string> };

/** Manifest fields a release writes without changing what a package is. */
export const UNHASHED_FIELDS: ReadonlySet<string> = new Set([
  "version",
  CONTENT_HASH_FIELD,
  HOST_CODE_HASH_FIELD,
]);

const DEPENDENCY_SECTIONS = [
  "dependencies",
  "devDependencies",
  "optionalDependencies",
  "peerDependencies",
];

/**
 * Packs a repository-relative directory from the current workspace and hashes
 * the published contents.
 */
export function contentHashOfPackage(
  repo: string,
  dir: string,
): string | PackFailure {
  return withScratch((scratch) => {
    const root = packInto(repo, dir, scratch);
    return typeof root === "string" ? contentHashOfDirectory(root) : root;
  });
}

/** Uses the same content hash as contentHashOfPackage without reading a workspace. */
export function contentHashOfTarball(file: string): string | PackFailure {
  return withScratch((scratch) => {
    const extracted =
      run("tar", ["-xzf", path.resolve(file), "-C", scratch], {
        cwd: scratch,
      }) ?? path.join(scratch, "package");
    return typeof extracted === "string"
      ? contentHashOfDirectory(extracted)
      : extracted;
  });
}

/**
 * Requires an extracted package directory. Hashes file paths in byte order,
 * executable bits and file contents.
 */
export function contentHashOfDirectory(root: string): string {
  return digestOf(
    packedFiles(root).map((relative) =>
      fileEntry(root, relative, relative, { kind: "keep" }),
    ),
  );
}

/** Provides a fresh scratch directory and removes it after the callback returns. */
export function withScratch<T>(body: (scratch: string) => T): T {
  const scratch = mkdtempSync(path.join(tmpdir(), "content-hash-"));
  const result = body(scratch);
  rmSync(scratch, { recursive: true, force: true });
  return result;
}

/**
 * Packs a repository-relative directory into scratch and returns its extracted
 * package directory or the failed step.
 */
export function packInto(
  repo: string,
  dir: string,
  scratch: string,
): string | PackFailure {
  const destination = path.join(scratch, dir);
  mkdirSync(destination, { recursive: true });
  const packed =
    run("pnpm", ["pack", "--pack-destination", destination], {
      cwd: path.join(repo, dir),
    }) ?? tarball(destination);
  if (typeof packed !== "string") {
    return packed;
  }
  if (dir === "packages/social-harness") {
    const version = readCompatibilityVersion(repo);
    if ("error" in version) {
      return { kind: "pack-failed", message: version.error };
    }
    const failure = recordPackedWireVersion(packed, version.value);
    if (failure !== undefined) {
      return failure;
    }
  }
  return (
    run("tar", ["-xzf", packed, "-C", destination], { cwd: destination }) ??
    path.join(destination, "package")
  );
}

/**
 * Updates only the archived manifest, preserving pnpm's file selection and
 * workspace pins. Excludes macOS file metadata when recreating the archive.
 */
export function recordPackedWireVersion(
  archive: string,
  version: string,
): PackFailure | undefined {
  const scratch = mkdtempSync(path.join(tmpdir(), "sharness-wire-pack-"));
  try {
    const failure = run("tar", ["-xzf", archive, "-C", scratch], {
      cwd: scratch,
    });
    if (failure !== undefined) {
      return failure;
    }
    const manifestPath = path.join(scratch, "package", "package.json");
    const manifest: unknown = JSON.parse(readFileSync(manifestPath, "utf8"));
    if (typeof manifest !== "object" || manifest === null) {
      return {
        kind: "pack-failed",
        message: "packed Client manifest is not an object",
      };
    }
    writeFileSync(
      manifestPath,
      `${JSON.stringify({ ...manifest, sharnessWireVersion: version }, null, 2)}\n`,
    );
    return run(
      "env",
      ["COPYFILE_DISABLE=1", "tar", "-czf", archive, "-C", scratch, "package"],
      { cwd: scratch },
    );
  } finally {
    rmSync(scratch, { recursive: true, force: true });
  }
}

/**
 * Every file under an extracted package, as `/`-separated relative paths in
 * byte order.
 */
export function packedFiles(root: string): readonly string[] {
  return readdirSync(root, { recursive: true, withFileTypes: true })
    .filter((entry) => entry.isFile())
    .map((entry) =>
      path
        .relative(root, path.join(entry.parentPath, entry.name))
        .split(path.sep)
        .join("/"),
    )
    .sort(byteOrder);
}

/**
 * Includes the label, executable bit and content digest. A package.json uses
 * the supplied manifest view.
 */
export function fileEntry(
  root: string,
  relative: string,
  label: string,
  view: ManifestView,
): string {
  const file = path.join(root, relative);
  const executable = (statSync(file).mode & 0o111) === 0 ? "-" : "x";
  const digest = createHash("sha256")
    .update(hashedBytes(file, relative, view))
    .digest("hex");
  return `${label}\0${executable}\0${digest}\n`;
}

/** Returns `sha256:` and a hex digest, independent of entry order. */
export function digestOf(entries: readonly string[]): string {
  const digest = createHash("sha256");
  for (const entry of [...entries].sort(byteOrder)) {
    digest.update(entry);
  }
  return `sha256:${digest.digest("hex")}`;
}

/** JSON with object keys sorted at every depth, so key order never matters. */
export function sortedJson(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${value.map(sortedJson).join(",")}]`;
  }
  if (value !== null && typeof value === "object") {
    const members = Object.entries(value)
      .sort(([left], [right]) => byteOrder(left, right))
      .map(([key, item]) => `${JSON.stringify(key)}:${sortedJson(item)}`);
    return `{${members.join(",")}}`;
  }
  return JSON.stringify(value);
}

/** Compare paths without locale-dependent collation. */
export function byteOrder(left: string, right: string): number {
  if (left === right) {
    return 0;
  }
  return left < right ? -1 : 1;
}

/** Returns command output with failures, or undefined on success. */
function run(
  command: string,
  args: readonly string[],
  options: { readonly cwd: string },
): PackFailure | undefined {
  const result = spawnSync(command, args, {
    cwd: options.cwd,
    encoding: "utf8",
  });
  if (result.status === 0) {
    return undefined;
  }
  return {
    kind: "pack-failed",
    message: `${command} ${args.join(" ")} in ${options.cwd} failed: ${result.stderr || result.stdout || String(result.error)}`,
  };
}

function tarball(directory: string): string | PackFailure {
  const archives = readdirSync(directory).filter((name) =>
    name.endsWith(".tgz"),
  );
  const [archive] = archives;
  if (archive === undefined || archives.length !== 1) {
    return {
      kind: "pack-failed",
      message: `expected one tarball in ${directory}, found ${archives.length}`,
    };
  }
  return path.join(directory, archive);
}

/**
 * The bytes the hash reads for one file: a manifest without its unhashed
 * fields, and without sibling entries when the view drops them; every other
 * file as packed.
 */
function hashedBytes(
  file: string,
  relative: string,
  view: ManifestView,
): Buffer {
  const bytes = readFileSync(file);
  if (path.posix.basename(relative) !== "package.json") {
    return bytes;
  }
  const manifest: unknown = JSON.parse(bytes.toString("utf8"));
  if (typeof manifest !== "object" || manifest === null) {
    return bytes;
  }
  const kept = Object.entries(manifest)
    .filter(([key]) => !UNHASHED_FIELDS.has(key))
    .map(([key, value]): [string, unknown] =>
      view.kind === "drop" && DEPENDENCY_SECTIONS.includes(key)
        ? [key, withoutSiblings(value, view.names)]
        : [key, value],
    );
  return Buffer.from(sortedJson(Object.fromEntries(kept)));
}

function withoutSiblings(
  section: unknown,
  siblings: ReadonlySet<string>,
): unknown {
  if (typeof section !== "object" || section === null) {
    return section;
  }
  return Object.fromEntries(
    Object.entries(section).filter(([name]) => !siblings.has(name)),
  );
}

if (import.meta.main) {
  const args = process.argv.slice(2);
  const [first, second] = args;
  if (first === undefined || (first === "--tarball" && second === undefined)) {
    console.error(
      "usage: content-hash.ts DIR | content-hash.ts --tarball FILE",
    );
    process.exit(2);
  }
  const hash =
    first === "--tarball" && second !== undefined
      ? contentHashOfTarball(second)
      : contentHashOfPackage(process.cwd(), first);
  if (typeof hash !== "string") {
    console.error(hash.message);
    process.exit(1);
  }
  console.log(hash);
}
