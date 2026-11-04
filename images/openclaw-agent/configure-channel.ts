/** @file Loads the image's installed channel before OpenClaw reads its configuration. */

import { mkdtemp, readFile, rename, rm, writeFile } from "node:fs/promises";
import { basename, dirname, join } from "node:path";

const IMAGE_CHANNEL =
  "/opt/social-harness/node_modules/@moltzap/openclaw-channel";

async function configureChannel(): Promise<undefined> {
  const path = process.env.OPENCLAW_CONFIG_PATH;
  if (path === undefined || path.length === 0) {
    throw new Error(
      "OPENCLAW_CONFIG_PATH is required. Docs: https://github.com/social-harness/moltzap/tree/main/images/openclaw-agent#openclaw-host-image",
    );
  }
  const config: unknown = JSON.parse(await readFile(path, "utf8"));
  if (!selectChannel(config)) {
    return;
  }
  await writeConfiguration(path, config);
}

/** Each write gets its own directory beside the configuration, so abandoned writes cannot block a restart. */
async function writeConfiguration(
  path: string,
  config: unknown,
): Promise<undefined> {
  const directory = await mkdtemp(
    join(dirname(path), ".social-harness-channel-"),
  );
  const temporary = join(directory, basename(path));
  try {
    await writeFile(temporary, JSON.stringify(config, null, 2) + "\n", {
      mode: 0o600,
      flag: "wx",
    });
    await rename(temporary, path);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}

/** Reconciles persisted settings on every process start without rewriting unchanged configuration. */
function selectChannel(config: unknown): boolean {
  const load = pluginLoad(config);
  if (load === undefined) {
    return false;
  }
  const paths: unknown = load.paths ?? [];
  if (!isStringArray(paths)) {
    return false;
  }
  const selected = [
    ...paths.filter((path) => path !== IMAGE_CHANNEL),
    IMAGE_CHANNEL,
  ];
  if (JSON.stringify(selected) === JSON.stringify(paths)) {
    return false;
  }
  load.paths = selected;
  return true;
}

function pluginLoad(config: unknown): Record<string, unknown> | undefined {
  if (!isRecord(config)) {
    return undefined;
  }
  config.plugins ??= {};
  if (!isRecord(config.plugins)) {
    return undefined;
  }
  config.plugins.load ??= {};
  return isRecord(config.plugins.load) ? config.plugins.load : undefined;
}

function isStringArray(value: unknown): value is string[] {
  return (
    Array.isArray(value) && value.every((path) => typeof path === "string")
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

await configureChannel().catch((cause: unknown) => {
  console.error(
    "[social harness openclaw host] channel configuration failed; not starting OpenClaw",
    cause,
  );
  process.exitCode = 1;
});
