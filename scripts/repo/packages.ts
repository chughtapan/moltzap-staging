/** @file The workspace root package and the packages `pnpm-workspace.yaml` selects, with the metadata their manifests own. */
import { Data } from "effect";
import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import path from "node:path";
import { parse } from "yaml";
import { isJsonObject, objectAt, readJsonObject, stringAt } from "./json.ts";

/** Git could not list the tracked manifests, so the inventory would be blind. */
class UnlistedWorkspace extends Data.TaggedError("UnlistedWorkspace")<{
  readonly message: string;
}> {}

/** The tracked workspace packages, with the dependency and export metadata their manifests own. */
export function workspacePackages(workspaceRoot: string) {
  return workspaceDirectories(
    readFileSync(path.join(workspaceRoot, "pnpm-workspace.yaml"), "utf8"),
    trackedManifests(workspaceRoot),
  ).map((dir) => {
    const manifest = readJsonObject(
      path.join(workspaceRoot, dir, "package.json"),
    );
    return {
      dir,
      name: stringAt(manifest, "name") ?? "",
      dependencies: objectAt(manifest, "dependencies"),
      exports: objectAt(manifest, "exports"),
    };
  });
}

/**
 * The package directories pnpm selects from `workspaceYaml` among the
 * repository-relative `files`: the root, and each `package.json` directory
 * that a `packages` glob includes and no `!` glob excludes.
 */
function workspaceDirectories(
  workspaceYaml: string,
  files: Iterable<string>,
): string[] {
  const workspace: unknown = parse(workspaceYaml);
  const listed: unknown = isJsonObject(workspace)
    ? workspace.packages
    : undefined;
  const globs = Array.isArray(listed)
    ? listed.filter((glob): glob is string => typeof glob === "string")
    : [];
  const selected = (directory: string, excluded: boolean) =>
    globs.some(
      (glob) =>
        glob.startsWith("!") === excluded &&
        path.posix.matchesGlob(directory, glob.replace(/^!/u, "")),
    );
  return [...files]
    .filter((file) => path.posix.basename(file) === "package.json")
    .map((file) => path.posix.dirname(file))
    .filter(
      (directory) =>
        directory === "." ||
        (selected(directory, false) && !selected(directory, true)),
    );
}

function trackedManifests(workspaceRoot: string): string[] {
  const listed = spawnSync(
    // eslint-disable-next-line sonarjs/no-os-command-from-path -- Git supplies the tracked workspace inventory.
    "git",
    ["ls-files", "-z", "--", ":(glob)**/package.json"],
    { cwd: workspaceRoot },
  );
  if (listed.status !== 0) {
    throw new UnlistedWorkspace({
      message: listed.stderr?.toString("utf8") || String(listed.error),
    });
  }
  return new TextDecoder("utf-8", { fatal: true })
    .decode(listed.stdout)
    .split("\0")
    .filter(Boolean);
}
