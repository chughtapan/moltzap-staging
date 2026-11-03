/**
 * @file Installs the workspace channel into the patched upstream NanoClaw
 * tree. Workspace runtime dependencies are supplied by the Docker context.
 * Usage: `node install-channel.ts APP_ROOT CHANNEL_SOURCE`.
 */
import { copyFile, cp, mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";

const [appRoot, channelSource] = process.argv.slice(2);
if (appRoot === undefined || channelSource === undefined) {
  throw new TypeError("usage: install-channel.ts APP_ROOT CHANNEL_SOURCE");
}

await copyFile(channelSource, join(appRoot, "src/channels/sharness.ts"));
await mkdir(join(appRoot, "src/sharness-mcp"), { recursive: true });
await cp(
  join(dirname(channelSource), "mcp"),
  join(appRoot, "src/sharness-mcp"),
  { recursive: true },
);
await prepareManifest(join(appRoot, "package.json"));
await prepareTypescript(join(appRoot, "tsconfig.json"));
await allowBetterSqliteBuild(join(appRoot, "pnpm-workspace.yaml"));

/** Upstream development hooks do not run inside the image builder. */
async function prepareManifest(packagePath: string) {
  const manifest = decodeManifest(await readFile(packagePath, "utf8"));
  delete manifest.scripts.prepare;
  const dependencies = isObject(manifest.object.dependencies)
    ? manifest.object.dependencies
    : {};
  for (const name of [
    "@moltzap/social-harness",
    "@modelcontextprotocol/client",
    "effect",
  ]) {
    dependencies[name] = `link:../../workspace/node_modules/${name}`;
  }
  manifest.object.dependencies = dependencies;
  const imports = isObject(manifest.object.imports)
    ? manifest.object.imports
    : {};
  imports["#sharness-mcp/*"] = {
    types: "./src/sharness-mcp/*.ts",
    default: "./dist/sharness-mcp/*.js",
  };
  manifest.object.imports = imports;
  await writeFile(packagePath, `${JSON.stringify(manifest.object, null, 2)}\n`);
}

/** Add `better-sqlite3` to the workspace's `onlyBuiltDependencies`, whose install script builds its native addon. */
async function allowBetterSqliteBuild(workspacePath: string) {
  const workspace = await readFile(workspacePath, "utf8");
  if (workspace.includes("  - better-sqlite3\n")) {
    return;
  }
  const updated = workspace.replace(
    "onlyBuiltDependencies:\n",
    "onlyBuiltDependencies:\n  - better-sqlite3\n",
  );
  if (updated === workspace) {
    throw new Error("NanoClaw workspace has no onlyBuiltDependencies list");
  }
  await writeFile(workspacePath, updated);
}

function decodeManifest(text: string): {
  readonly object: Record<string, unknown>;
  readonly scripts: Record<string, unknown>;
} {
  const parsed: unknown = JSON.parse(text);
  if (!isObject(parsed) || !isObject(parsed.scripts)) {
    throw new Error("NanoClaw package.json is not an object with scripts");
  }
  return { object: parsed, scripts: parsed.scripts };
}

/** Shared source compiles under NanoClaw's unchanged src root and dist tree. */
async function prepareTypescript(path: string) {
  const object: unknown = JSON.parse(await readFile(path, "utf8"));
  if (!isObject(object)) {
    throw new Error("NanoClaw tsconfig.json is not an object");
  }
  const options = isObject(object.compilerOptions)
    ? object.compilerOptions
    : {};
  const paths = isObject(options.paths) ? options.paths : {};
  paths["#sharness-mcp/*"] = ["./src/sharness-mcp/*.ts"];
  options.paths = paths;
  object.compilerOptions = options;
  await writeFile(path, `${JSON.stringify(object, null, 2)}\n`);
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
