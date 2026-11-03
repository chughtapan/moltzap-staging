#!/usr/bin/env node
/**
 * @file Builds stock NanoClaw from a verified upstream archive, then adds
 * this workspace's channel source, skills and daemon runtime closure.
 * The image:build Nx target prepares the workspace runtime dependencies
 * before invoking this script.
 */
import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import {
  copyFile,
  mkdir,
  mkdtemp,
  readFile,
  rm,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs, promisify } from "node:util";

const exec = promisify(execFile);
const imageRoot = dirname(fileURLToPath(import.meta.url));
const BUILD_TIMEOUT_MILLIS = 45 * 60 * 1_000;
const BUILD_OUTPUT_BYTES = 32 * 1024 * 1024;
const IMAGE_REFERENCE = /^[a-z0-9][a-z0-9._/-]*:\w[\w.-]{0,127}$/u;
const USAGE =
  "usage: build.ts --tag NAME:TAG | build.ts stage DIRECTORY | build.ts lock. Docs: https://github.com/social-harness/moltzap/tree/main/images/nanoclaw-agent#nanoclaw-host-image";

/** Stock NanoClaw: the upstream revision and the sha256 of its GitHub archive. */
export const NANOCLAW_SOURCE = Object.freeze({
  revision: "54d9d9a50c0e572fa3969d63ab87a4dd3d75cc6f",
  sha256: "68663a0a06feb64d2366b7d6770a2ad9afc82765c9f1e8f0245bd4cbabeb1006",
});

/**
 * Pins the multi-platform base image digest behind the upstream Dockerfile's
 * image name, so its floating tag cannot reach the build.
 */
export const NANOCLAW_BASE_IMAGE = Object.freeze({
  name: "node:22-slim",
  digest:
    "sha256:43ac6c60b8f89723f746e8a92ce91abd5017e627ce1ddfe4238355d3a30b772c",
});

/** Selects staging verified inputs, resolving the lockfile, or loading a tagged image. */
export type BuildRequest =
  | { readonly mode: "stage"; readonly directory: string }
  | { readonly mode: "lock" }
  | { readonly mode: "build"; readonly tag: string };

/**
 * Requires an empty or absent directory. Writes the verified NanoClaw tree,
 * channel source and inputs.json pins for the image builds.
 */
export async function stageInputs(directory: string) {
  const source = join(directory, "source");
  const channel = join(directory, "channel");
  await mkdir(source, { recursive: true });
  await mkdir(channel, { recursive: true });
  await stageNanoClawTree(directory, source);
  await copyFile(
    join(imageRoot, "../../plugins/nanoclaw/src/channels/sharness.ts"),
    join(channel, "sharness.ts"),
  );
  await mkdir(join(channel, "mcp"), { recursive: true });
  await Promise.all(
    ["mcp-client.ts", "inbox-wakeups.ts"].map((name) =>
      copyFile(
        join(imageRoot, "../../plugins/shared", name),
        join(channel, "mcp", name),
      ),
    ),
  );
  await writeFile(
    join(directory, "inputs.json"),
    `${JSON.stringify({
      revision: NANOCLAW_SOURCE.revision,
      archiveSha256: NANOCLAW_SOURCE.sha256,
      baseImage: NANOCLAW_BASE_IMAGE.name,
      baseImageDigest: NANOCLAW_BASE_IMAGE.digest,
    })}\n`,
  );
}

/** The GitHub archive URL of the pinned upstream revision. */
export function nanoClawSourceUrl(source: typeof NANOCLAW_SOURCE): string {
  return `https://github.com/nanocoai/nanoclaw/archive/${source.revision}.tar.gz`;
}

/** Rejects non-2xx responses and SHA-256 mismatches before returning bytes. */
export async function downloadVerified(
  url: string,
  sha256: string,
  fetchImpl: typeof fetch = fetch,
): Promise<Buffer> {
  const response = await fetchImpl(url, { redirect: "follow" });
  if (!response.ok) {
    throw new Error(
      `download of ${url} failed: ${String(response.status)} ${response.statusText}`,
    );
  }
  const body = Buffer.from(await response.arrayBuffer());
  const actual = createHash("sha256").update(body).digest("hex");
  if (actual !== sha256) {
    throw new Error(`sha256 of ${url} is ${actual}, expected ${sha256}`);
  }
  return body;
}

/**
 * Throws TypeError with usage for invalid arguments, including an image
 * reference without a tag.
 */
export function parseBuildArguments(args: readonly string[]): BuildRequest {
  const parsed = parseOrUsage(args);
  const request =
    parsed.tag === undefined
      ? positionalRequest(parsed.positionals)
      : tagRequest(parsed.tag, parsed.positionals);
  if (request === undefined) {
    throw new TypeError(USAGE);
  }
  return request;
}

/** Refuses FROM entries outside NANOCLAW_BASE_IMAGE.name so every base stays pinned. */
export function requirePinnedBase(dockerfile: string): void {
  const bases = [...dockerfile.matchAll(/^FROM\s+(\S+)/gmu)].map(
    (match) => match[1] ?? "",
  );
  if (
    bases.length === 0 ||
    bases.some((base) => base !== NANOCLAW_BASE_IMAGE.name)
  ) {
    throw new Error(
      `stock NanoClaw builds FROM ${bases.join(", ") || "nothing"}, but only ${NANOCLAW_BASE_IMAGE.name} is pinned`,
    );
  }
}

/** The driver name in `docker buildx inspect` stdout. */
export function buildxDriver(inspectOutput: string): string {
  const driver = /^Driver:\s*(\S+)/mu.exec(inspectOutput)?.[1];
  if (driver === undefined) {
    throw new Error("docker buildx inspect reported no driver");
  }
  return driver;
}

async function main() {
  const request = parseBuildArguments(process.argv.slice(2));
  switch (request.mode) {
    case "stage":
      await stageInputs(request.directory);
      return;
    case "lock":
      await writeLockfile();
      return;
    case "build":
      await buildImage(request.tag);
      return;
    default:
      request satisfies never;
  }
}

/** `parseArgs` in strict mode, with any parse error replaced by the usage line. */
function parseOrUsage(args: readonly string[]): {
  readonly tag?: string;
  readonly positionals: readonly string[];
} {
  try {
    const { values, positionals } = parseArgs({
      args: [...args],
      strict: true,
      allowPositionals: true,
      options: { tag: { type: "string" } },
    });
    return values.tag === undefined
      ? { positionals }
      : { tag: values.tag, positionals };
  } catch (error) {
    throw new TypeError(USAGE, { cause: error });
  }
}

function positionalRequest(
  positionals: readonly string[],
): BuildRequest | undefined {
  const [mode, directory, ...rest] = positionals;
  if (rest.length > 0) {
    return undefined;
  }
  if (mode === "stage" && directory !== undefined) {
    return { mode: "stage", directory };
  }
  if (mode === "lock" && directory === undefined) {
    return { mode: "lock" };
  }
  return undefined;
}

function tagRequest(
  tag: string,
  positionals: readonly string[],
): BuildRequest | undefined {
  return positionals.length === 0 && IMAGE_REFERENCE.test(tag)
    ? { mode: "build", tag }
    : undefined;
}

async function stageNanoClawTree(directory: string, source: string) {
  report(`downloading NanoClaw ${NANOCLAW_SOURCE.revision}`);
  const archive = join(directory, "nanoclaw.tar.gz");
  await writeFile(
    archive,
    await downloadVerified(
      nanoClawSourceUrl(NANOCLAW_SOURCE),
      NANOCLAW_SOURCE.sha256,
    ),
  );
  await exec("tar", ["-xzf", archive, "--strip-components=1", "-C", source], {
    timeout: 2 * 60 * 1_000,
  });
  await rm(archive);
  requirePinnedBase(
    await readFile(join(source, "container/Dockerfile"), "utf8"),
  );
}

/**
 * Resolves without installing, using the pnpm release in NanoClaw's
 * packageManager. Writes pnpm-lock.yaml beside this script.
 */
async function writeLockfile() {
  const staging = await mkdtemp(join(tmpdir(), "nanoclaw-agent-lock-"));
  try {
    await resolvePreparedTree(staging);
    await copyFile(
      join(staging, "source", "pnpm-lock.yaml"),
      join(imageRoot, "pnpm-lock.yaml"),
    );
  } finally {
    await rm(staging, { recursive: true, force: true });
  }
}

async function resolvePreparedTree(staging: string) {
  const source = join(staging, "source");
  await stageInputs(staging);
  await exec("git", ["apply", join(imageRoot, "nanoclaw-v2.3.0.patch")], {
    cwd: source,
  });
  await exec(
    process.execPath,
    [
      join(imageRoot, "install-channel.ts"),
      source,
      join(staging, "channel", "sharness.ts"),
    ],
    { cwd: source },
  );
  await copyFile(
    join(imageRoot, "pnpm-lock.yaml"),
    join(source, "pnpm-lock.yaml"),
  );
  report("resolving the prepared tree's dependencies");
  await exec(
    "corepack",
    ["pnpm", "install", "--lockfile-only", "--ignore-scripts"],
    {
      cwd: source,
      env: { ...process.env, COREPACK_ENABLE_DOWNLOAD_PROMPT: "0" },
      timeout: BUILD_TIMEOUT_MILLIS,
      maxBuffer: BUILD_OUTPUT_BYTES,
    },
  );
}

async function buildImage(tag: string) {
  await requireDockerDriver();
  const staging = await mkdtemp(join(tmpdir(), "nanoclaw-agent-"));
  try {
    await stageInputs(staging);
    const agentBaseImage = await buildAgentBase(join(staging, "source"));
    report(`building ${tag}`);
    await exec("docker", imageBuildArguments(tag, staging, agentBaseImage), {
      timeout: BUILD_TIMEOUT_MILLIS,
      maxBuffer: BUILD_OUTPUT_BYTES,
    });
    process.stdout.write(
      `${JSON.stringify({
        image: tag,
        sourceRevision: NANOCLAW_SOURCE.revision,
        sourceArchiveDigest: `sha256:${NANOCLAW_SOURCE.sha256}`,
        agentBaseImage,
      })}\n`,
    );
  } finally {
    await rm(staging, { recursive: true, force: true });
  }
}

async function requireDockerDriver() {
  const { stdout } = await exec("docker", ["buildx", "inspect"], {
    timeout: 30_000,
  });
  const driver = buildxDriver(stdout);
  if (driver !== "docker") {
    throw new Error(
      `the NanoClaw image needs buildx's docker driver, but the current builder uses ${driver}`,
    );
  }
}

async function buildAgentBase(source: string): Promise<string> {
  const image = `nanoclaw-agent-base:${NANOCLAW_SOURCE.revision.slice(0, 16)}`;
  const lockDigest = createHash("sha256")
    .update(await readFile(join(source, "container/agent-runner/bun.lock")))
    .digest("hex");
  report(`building stock NanoClaw agent base ${image}`);
  await exec(
    "docker",
    [
      "buildx",
      "build",
      "--load",
      "--tag",
      image,
      "--build-arg",
      `AGENT_RUNNER_LOCK_SHA256=${lockDigest}`,
      "--build-arg",
      `IMAGE_SOURCE=nanoclaw@${NANOCLAW_SOURCE.revision}`,
      "--build-context",
      `${NANOCLAW_BASE_IMAGE.name}=docker-image://${NANOCLAW_BASE_IMAGE.name}@${NANOCLAW_BASE_IMAGE.digest}`,
      "--file",
      join(source, "container/Dockerfile"),
      join(source, "container"),
    ],
    { timeout: BUILD_TIMEOUT_MILLIS, maxBuffer: BUILD_OUTPUT_BYTES },
  );
  return image;
}

function imageBuildArguments(
  tag: string,
  staging: string,
  agentBaseImage: string,
): string[] {
  return [
    "buildx",
    "build",
    "--load",
    "--tag",
    tag,
    "--build-context",
    `nanoclaw-source=${join(staging, "source")}`,
    "--build-context",
    `social-harness-channel=${join(staging, "channel")}`,
    "--build-context",
    `social-harness-workspace=${join(imageRoot, "../.workspace")}`,
    "--build-arg",
    `NANOCLAW_AGENT_IMAGE=${agentBaseImage}`,
    "--build-arg",
    `NANOCLAW_SOURCE_REVISION=${NANOCLAW_SOURCE.revision}`,
    "--build-arg",
    `NANOCLAW_SOURCE_ARCHIVE_SHA256=${NANOCLAW_SOURCE.sha256}`,
    imageRoot,
  ];
}

/** Write one progress line to stderr, which keeps stdout for the build's JSON line. */
function report(message: string): void {
  process.stderr.write(`[nanoclaw-agent image] ${message}\n`);
}

if (import.meta.main) {
  await main();
}
