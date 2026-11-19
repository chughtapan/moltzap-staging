/**
 * @file Removes stale outputs before compiling the calling package.
 *
 * Run from the package directory before `tsc -b`. TypeScript retains outputs
 * for deleted sources and can consider the build current after those outputs
 * are removed. Clearing `dist/` also removes its build stamp, forcing a rebuild
 * that cannot publish or resolve orphaned modules.
 */
import { rmSync } from "node:fs";
import { resolve } from "node:path";

rmSync(resolve("dist"), { force: true, recursive: true, maxRetries: 3 });
