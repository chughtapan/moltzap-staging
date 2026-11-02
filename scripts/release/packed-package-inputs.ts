/** @file Finds committed files and TypeScript sources that contribute to a packed package. */
import path from "node:path";
import {
  ImageInputError,
  readJson,
  type Tree,
  under,
  workspaceRoots,
} from "./image-input-tree.ts";
import { asStrings, sourceGlobs } from "./typescript-config.ts";

/** Runtime siblings bind packed assets; copied build-only roots bind their compiled sources. */
export function includedCopy(
  tree: Tree,
  file: string,
  roots: readonly string[],
  buildOnly: ReadonlyMap<string, ReadonlySet<string>>,
): boolean {
  const own = workspaceRoots(tree).get(tree.service ?? "");
  const sibling = roots.find(
    (root) => root !== "." && root !== own && file.startsWith(`${root}/`),
  );
  if (sibling === undefined) {
    return true;
  }
  const compiled = buildOnly.get(sibling);
  if (compiled !== undefined) {
    return compiled.has(file);
  }
  const relative = path.posix.relative(sibling, file);
  return (
    !/^(?:src|bin)\//u.test(relative) && packedFile(tree, sibling, relative)
  );
}

/**
 * Whether pnpm packs a source file of a workspace package.
 * The package must declare `files`: without it pnpm packs whatever its ignore
 * files allow, which an image key cannot bind.
 */
export function packedFile(
  tree: Tree,
  root: string,
  relative: string,
): boolean {
  const declared = readJson(tree, `${root}/package.json`).files;
  if (!Array.isArray(declared)) {
    throw new ImageInputError({
      message: `Copied workspace package ${root}/package.json declares no files list; an image binds a copied sibling by what it packs`,
    });
  }
  const patterns = asStrings(declared);
  return (
    /^(?:package\.json|readme(?:\..*)?|licen[cs]e(?:\..*)?|notice(?:\..*)?)$/iu.test(
      relative,
    ) ||
    (patterns.some(
      (pattern) => !pattern.startsWith("!") && packedPath(relative, pattern),
    ) &&
      !patterns.some(
        (pattern) =>
          pattern.startsWith("!") && packedPath(relative, pattern.slice(1)),
      ))
  );
}

/** Committed sources a package's `tsconfig.json` compiles into what it packs. */
export function compiledSources(tree: Tree, directory: string): string[] {
  const config = `${directory}/tsconfig.json`;
  if (!tree.files.has(config)) {
    return [];
  }
  const { files, include, exclude } = sourceGlobs(tree, config);
  return under(tree, directory).filter(
    (file) =>
      files.includes(file) ||
      (/\.[cm]?tsx?$/u.test(file) &&
        include.some((pattern) => packedPath(file, pattern)) &&
        !exclude.some((pattern) => packedPath(file, pattern))),
  );
}

function packedPath(file: string, pattern: string): boolean {
  const normalized = pattern.replace(/^\.\//u, "").replace(/\/$/u, "");
  return (
    path.posix.matchesGlob(file, normalized) ||
    path.posix.matchesGlob(file, `${normalized}/**`)
  );
}
