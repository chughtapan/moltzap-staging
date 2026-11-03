#!/usr/bin/env node
/**
 * @file Hashes the packed code a host loads for a plugin.
 * Usage: `node scripts/release/loaded-plugin-code-hash.ts DIR` with a repository-relative
 * package directory. Prints `sha256:` and a hex digest.
 *
 * The launcher compares `sharnessHostCodeHash` to decide whether to restart
 * OpenClaw. The hash covers every plugin file and sibling modules reachable
 * through static or dynamic imports and package exports. Manifests omit version,
 * recorded hashes and workspace dependency pins, so a sibling version bump
 * without reachable code changes leaves the hash unchanged.
 *
 * Run from the repository root after `pnpm build`. Packing or extraction failure
 * exits 1.
 */

import { Either } from "effect";
import { readFileSync } from "node:fs";
import path from "node:path";
import { workspacePackages } from "../repo/packages.ts";
import { moduleGraph } from "./module-graph.ts";
import {
  digestOf,
  fileEntry,
  packedFiles,
  type PackFailure,
  packInto,
  withScratch,
} from "./package-content-hash.ts";

/**
 * Packs a repository-relative plugin and its runtime workspace dependencies
 * before hashing the code the host loads.
 */
export function hostCodeHashOfPackage(
  repo: string,
  dir: string,
): string | PackFailure {
  const directories = workspaceDirectories(repo);
  if (!(directories instanceof Map)) {
    return directories;
  }
  const host = manifestName(repo, dir);
  return withScratch((scratch) => {
    const roots = new Map<string, string>();
    for (const name of siblingClosure(repo, host, directories)) {
      const sibling = directories.get(name) ?? dir;
      const root = packInto(repo, sibling, scratch);
      if (typeof root !== "string") {
        return root;
      }
      roots.set(name, root);
    }
    return hostCodeHashOfDirectories(host, roots, new Set(directories.keys()));
  });
}

/**
 * Requires extracted package roots for the host's runtime dependency closure.
 * Workspace names identify pins to omit from every dependency section.
 */
export function hostCodeHashOfDirectories(
  host: string,
  roots: ReadonlyMap<string, string>,
  workspaceNames: ReadonlySet<string>,
): string {
  const hostRoot = roots.get(host) ?? "";
  const seeds = packedFiles(hostRoot).map((relative) => ({
    name: host,
    relative,
  }));
  return digestOf(
    moduleGraph(seeds, roots, {
      conditions: ["node", "module-sync", "import", "default"],
      all: false,
    }).map((module) =>
      fileEntry(
        roots.get(module.name) ?? "",
        module.relative,
        `${module.name}/${module.relative}`,
        { kind: "drop", names: workspaceNames },
      ),
    ),
  );
}

/** Reports inventory and manifest decoding failures as packing failures. */
function workspaceDirectories(repo: string): Map<string, string> | PackFailure {
  return Either.match(
    Either.try({
      try: () => packageDirectories(repo),
      catch: (cause): PackFailure => ({
        kind: "pack-failed",
        message: String(cause),
      }),
    }),
    { onLeft: (failure) => failure, onRight: (directories) => directories },
  );
}

function packageDirectories(repo: string): Map<string, string> | PackFailure {
  const directories = new Map<string, string>();
  for (const { dir, name } of workspacePackages(repo)) {
    if (directories.has(name)) {
      return {
        kind: "pack-failed",
        message: `Duplicate workspace package name ${name} in ${dir}/package.json`,
      };
    }
    directories.set(name, dir);
  }
  return directories;
}

function manifestName(repo: string, dir: string): string {
  return String(readManifest(repo, dir)?.name);
}

function siblingClosure(
  repo: string,
  name: string,
  directories: ReadonlyMap<string, string>,
): ReadonlySet<string> {
  const closure = new Set<string>();
  const pending = [name];
  for (let next = pending.pop(); next !== undefined; next = pending.pop()) {
    const dir = directories.get(next);
    if (closure.has(next) || dir === undefined) {
      continue;
    }
    closure.add(next);
    const dependencies = readManifest(repo, dir)?.dependencies;
    if (typeof dependencies === "object" && dependencies !== null) {
      pending.push(
        ...Object.keys(dependencies).filter((dependency) =>
          directories.has(dependency),
        ),
      );
    }
  }
  return closure;
}

function readManifest(
  repo: string,
  dir: string,
): Record<string, unknown> | undefined {
  return readJson(path.join(repo, dir, "package.json"));
}

function readJson(file: string): Record<string, unknown> | undefined {
  const value: unknown = JSON.parse(readFileSync(file, "utf8"));
  return typeof value === "object" && value !== null
    ? Object.fromEntries(Object.entries(value))
    : undefined;
}

if (import.meta.main) {
  const [dir] = process.argv.slice(2);
  if (dir === undefined) {
    process.stderr.write("usage: loaded-plugin-code-hash.ts DIR\n");
    process.exit(2);
  }
  const hash = hostCodeHashOfPackage(process.cwd(), dir);
  if (typeof hash !== "string") {
    process.stderr.write(`${hash.message}\n`);
    process.exit(1);
  }
  process.stdout.write(`${hash}\n`);
}
