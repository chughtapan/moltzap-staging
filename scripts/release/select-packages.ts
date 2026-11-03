#!/usr/bin/env node
/**
 * @file Selects changed packages and writes their release versions and hashes.
 *
 * Usage: `node scripts/release/select-packages.ts VERSION DIR...`, where each
 * DIR names a repository-relative package directory in dependency order.
 * Run from the repository root after `pnpm build`. Prints selected directories
 * separated by spaces.
 *
 * A missing or changed published content hash selects a package. Updating its
 * manifest before packing dependents lets their hashes include the new pin.
 * The release rules are in `docs/reference/spec/releases.md`.
 *
 * Exits 1 for invalid dependency order or a packing failure, and 2 when npm
 * fails without a 404. Registry failures must not select unchanged packages.
 */
import { spawnSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import {
  CONTENT_HASH_FIELD,
  contentHashOfPackage,
  type PackFailure,
} from "./package-content-hash.ts";

/** One package manifest as the selection reads it. */
export interface Manifest {
  readonly name: string;
  readonly version: string;
  /** Combined production, optional and peer dependency edges. */
  readonly dependencies: Readonly<Record<string, string>>;
}

/** What npm recorded for one published version. */
export type Recorded =
  | { readonly kind: "hash"; readonly hash: string }
  | { readonly kind: "absent" }
  | { readonly kind: "unavailable"; readonly message: string };

/** The workspace and registry operations the selection depends on. */
export interface Workspace {
  readonly manifest: (dir: string) => Manifest;
  readonly contentHash: (dir: string) => string | PackFailure;
  readonly recordedHash: (name: string, version: string) => Recorded;
  readonly select: (dir: string, version: string, hash: string) => void;
}

/** The exit status and output of one `npm view` run. */
export interface NpmViewResult {
  readonly status: number | null;
  readonly stdout: string;
  readonly stderr: string;
}

/** A package listed before a workspace sibling it depends on. */
export interface OutOfOrder {
  readonly kind: "out-of-order";
  readonly message: string;
}

/** The selected directories, or why the selection stopped. */
export type Selection =
  | { readonly kind: "selected"; readonly dirs: readonly string[] }
  | PackFailure
  | OutOfOrder
  | { readonly kind: "unavailable"; readonly message: string };

/**
 * Requires package directories after the siblings they depend on. Updates each
 * selected manifest before examining dependents. A later failure leaves earlier
 * selections applied.
 */
export function selectPackages(
  workspace: Workspace,
  version: string,
  dirs: readonly string[],
): Selection {
  const misordered = outOfOrder(workspace, dirs);
  if (misordered !== undefined) {
    return misordered;
  }
  const selected: string[] = [];
  for (const dir of dirs) {
    const manifest = workspace.manifest(dir);
    const hash = workspace.contentHash(dir);
    if (typeof hash !== "string") {
      return hash;
    }
    const recorded = workspace.recordedHash(manifest.name, manifest.version);
    if (recorded.kind === "unavailable") {
      return recorded;
    }
    if (recorded.kind === "absent" || recorded.hash !== hash) {
      workspace.select(dir, version, hash);
      selected.push(dir);
    }
  }
  return { kind: "selected", dirs: selected };
}

/** Reads packages under the repository root and queries the public npm registry. */
export function repositoryWorkspace(repo: string): Workspace {
  return {
    manifest: (dir) => {
      const manifest = readManifest(repo, dir);
      return {
        name: String(manifest.name),
        version: String(manifest.version),
        dependencies: {
          ...stringRecord(manifest.dependencies),
          ...stringRecord(manifest.optionalDependencies),
          ...stringRecord(manifest.peerDependencies),
        },
      };
    },
    contentHash: (dir) => contentHashOfPackage(repo, dir),
    recordedHash: npmRecordedHash,
    select: (dir, version, hash) => {
      const manifest = readManifest(repo, dir);
      writeFileSync(
        manifestPath(repo, dir),
        `${JSON.stringify({ ...manifest, version, [CONTENT_HASH_FIELD]: hash }, null, 2)}\n`,
      );
    },
  };
}

/**
 * Treats an npm 404 or a published version without a hash as absent.
 * Other npm failures mean the registry is unavailable.
 */
export function classifyNpmView(
  result: NpmViewResult,
  subject: string,
): Recorded {
  const output = result.stdout.trim();
  if (result.status === 0) {
    const value: unknown = output === "" ? undefined : JSON.parse(output);
    return typeof value === "string"
      ? { kind: "hash", hash: value }
      : { kind: "absent" };
  }
  if (isNotFound(output)) {
    return { kind: "absent" };
  }
  return {
    kind: "unavailable",
    message: `npm view ${subject} failed without a 404: ${output || result.stderr}`,
  };
}

function outOfOrder(
  workspace: Workspace,
  dirs: readonly string[],
): OutOfOrder | undefined {
  const positions = new Map(
    dirs.map((dir, index) => [workspace.manifest(dir).name, index] as const),
  );
  for (const [index, dir] of dirs.entries()) {
    const later = Object.keys(workspace.manifest(dir).dependencies).find(
      (dependency) => (positions.get(dependency) ?? -1) > index,
    );
    if (later !== undefined) {
      return {
        kind: "out-of-order",
        message: `${dir} depends on ${later}, which must come before it`,
      };
    }
  }
  return undefined;
}

function readManifest(repo: string, dir: string): Record<string, unknown> {
  const manifest: unknown = JSON.parse(
    readFileSync(manifestPath(repo, dir), "utf8"),
  );
  return typeof manifest === "object" && manifest !== null
    ? Object.fromEntries(Object.entries(manifest))
    : {};
}

function manifestPath(repo: string, dir: string): string {
  return path.join(repo, dir, "package.json");
}

function stringRecord(value: unknown): Readonly<Record<string, string>> {
  if (typeof value !== "object" || value === null) {
    return {};
  }
  return Object.fromEntries(
    Object.entries(value).filter(
      (entry): entry is [string, string] => typeof entry[1] === "string",
    ),
  );
}

/** The `sharnessContentHash` npm holds for `name@version`. */
function npmRecordedHash(name: string, version: string): Recorded {
  const result = spawnSync(
    // eslint-disable-next-line sonarjs/no-os-command-from-path -- Release tooling runs the npm the runner's toolchain put on PATH.
    "npm",
    ["view", `${name}@${version}`, CONTENT_HASH_FIELD, "--json"],
    { encoding: "utf8" },
  );
  return classifyNpmView(result, `${name}@${version}`);
}

/** Whether npm's JSON error output reports a 404. */
function isNotFound(output: string): boolean {
  if (output === "") {
    return false;
  }
  const parsed: unknown = JSON.parse(output);
  if (typeof parsed !== "object" || parsed === null || !("error" in parsed)) {
    return false;
  }
  const { error } = parsed;
  return (
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    error.code === "E404"
  );
}

if (import.meta.main) {
  const [version, ...dirs] = process.argv.slice(2);
  if (version === undefined || dirs.length === 0) {
    console.error("usage: select-packages.ts VERSION DIR...");
    process.exit(2);
  }
  const selection = selectPackages(
    repositoryWorkspace(process.cwd()),
    version,
    dirs,
  );
  if (selection.kind === "selected") {
    console.log(selection.dirs.join(" "));
  } else {
    console.error(selection.message);
    process.exit(selection.kind === "unavailable" ? 2 : 1);
  }
}
