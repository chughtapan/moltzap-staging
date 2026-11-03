/**
 * @file Starts NanoClaw from persistent state and exposes its CLI over TCP.
 * The host's exit determines the container's exit.
 */
import type { Stats } from "node:fs";
import { type ChildProcess, spawn } from "node:child_process";
import {
  lstat,
  mkdir,
  readFile,
  readlink,
  symlink,
  unlink,
} from "node:fs/promises";
import { dirname, isAbsolute, resolve } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import {
  type Gateway,
  type GatewayAddress,
  startGateway,
} from "./cli-gateway.ts";
import { type McpServer, provisionNanoClaw } from "./initialize-agent-group.ts";

const APP_ROOT = "/opt/social-harness/nanoclaw/app";
const PRELOAD = "/opt/social-harness/nanoclaw/preload.ts";
const DEFAULT_CONFIG = "/run/social-harness/bootstrap/nanoclaw/runtime.json";
const DEFAULT_STATE = "/var/lib/social-harness/nanoclaw";
const STARTUP_TIMEOUT_MILLIS = 120_000;
const SOCKET_POLL_MILLIS = 50;
const MAX_PORT = 65_535;
const INVALID_CONFIG =
  "invalid Social Harness NanoClaw application configuration";
/** The project directories NanoClaw writes in its state directory. */
const STATE_DIRECTORIES = ["data", "groups", "store", "tmp"];
/** The read-only application surfaces NanoClaw expects beside its state. */
const APP_SURFACES = ["container", "scripts", "templates", "package.json"];

interface ApplicationConfig {
  readonly agentName: string;
  readonly stateDirectory: string;
  readonly workspaceDirectory: string;
  readonly modelId?: string;
  readonly gateway: GatewayAddress;
  readonly mcpServers: readonly McpServer[];
}

interface Supervision {
  gateway?: Gateway;
}

process.exitCode = await main();

/** A host stopped by SIGTERM or SIGINT exits the container with zero. */
async function main(): Promise<number> {
  const config = await loadConfig();
  await materializeProject(config);
  process.chdir(config.stateDirectory);
  await provisionNanoClaw(config, { appRoot: APP_ROOT });

  const host = startHost(config);
  const supervision: Supervision = {};
  process.once("SIGTERM", () => {
    stopHost(host, supervision, "SIGTERM");
  });
  process.once("SIGINT", () => {
    stopHost(host, supervision, "SIGINT");
  });

  const socketPath = `${config.stateDirectory}/data/cli.sock`;
  await waitForSocket(socketPath, host);
  const gateway = await startGateway(config.gateway, socketPath);
  supervision.gateway = gateway;
  return await hostExit(host, gateway);
}

/**
 * Requires the application's stateDirectory to match DEPLOYMENT_NANOCLAW_STATE
 * or the default state mount.
 */
async function loadConfig(): Promise<ApplicationConfig> {
  const configPath = process.env.DEPLOYMENT_NANOCLAW_CONFIG ?? DEFAULT_CONFIG;
  const parsed: unknown = JSON.parse(await readFile(configPath, "utf8"));
  const config = decodeConfig(parsed);
  const configuredState =
    process.env.DEPLOYMENT_NANOCLAW_STATE ?? DEFAULT_STATE;
  if (resolve(config.stateDirectory) !== resolve(configuredState)) {
    throw new Error(
      "NanoClaw state directory disagrees with the application environment",
    );
  }
  return config;
}

function decodeConfig(value: unknown): ApplicationConfig {
  if (
    !isRecord(value) ||
    value.apiVersion !== "org.social-harness/nanoclaw-application/v1"
  ) {
    throw new Error(INVALID_CONFIG);
  }
  const { modelId } = value;
  if (modelId !== undefined && typeof modelId !== "string") {
    throw new Error(INVALID_CONFIG);
  }
  return {
    agentName: nonEmptyString(value.agentName),
    stateDirectory: absolutePath(value.stateDirectory),
    workspaceDirectory: absolutePath(value.workspaceDirectory),
    ...(modelId === undefined ? {} : { modelId }),
    gateway: decodeGateway(value.gateway),
    mcpServers: decodeMcpServers(value.mcpServers),
  };
}

function decodeGateway(value: unknown): GatewayAddress {
  if (!isRecord(value)) {
    throw new Error(INVALID_CONFIG);
  }
  const { host, port } = value;
  if (typeof host !== "string" || !isPort(port)) {
    throw new Error(INVALID_CONFIG);
  }
  return { host, port };
}

function isPort(value: unknown): value is number {
  return (
    Number.isInteger(value) && Number(value) >= 1 && Number(value) <= MAX_PORT
  );
}

function decodeMcpServers(value: unknown): readonly McpServer[] {
  if (!Array.isArray(value)) {
    throw new Error(INVALID_CONFIG);
  }
  return value.map((server: unknown) => decodeMcpServer(server));
}

function decodeMcpServer(value: unknown): McpServer {
  if (!isRecord(value) || typeof value.name !== "string") {
    throw new Error(INVALID_CONFIG);
  }
  const { name, url, command, args, env } = value;
  if ("url" in value) {
    if (typeof url !== "string") {
      throw new Error(INVALID_CONFIG);
    }
    return { name, url };
  }
  if (
    typeof command !== "string" ||
    !isStringArray(args) ||
    !isStringRecord(env)
  ) {
    throw new Error(INVALID_CONFIG);
  }
  return { name, command, args, env };
}

function nonEmptyString(value: unknown): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(INVALID_CONFIG);
  }
  return value;
}

function absolutePath(value: unknown): string {
  if (typeof value !== "string" || !isAbsolute(value)) {
    throw new Error(INVALID_CONFIG);
  }
  return value;
}

function isStringArray(value: unknown): value is readonly string[] {
  return (
    Array.isArray(value) &&
    value.every((item: unknown) => typeof item === "string")
  );
}

function isStringRecord(
  value: unknown,
): value is Readonly<Record<string, string>> {
  return (
    isRecord(value) &&
    Object.values(value).every((item: unknown) => typeof item === "string")
  );
}

function isRecord(value: unknown): value is Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

async function materializeProject(
  config: ApplicationConfig,
): Promise<undefined> {
  await mkdir(config.stateDirectory, { recursive: true });
  for (const directory of STATE_DIRECTORIES) {
    await mkdir(`${config.stateDirectory}/${directory}`, { recursive: true });
  }
  for (const surface of APP_SURFACES) {
    await ensureLink(
      `${config.stateDirectory}/${surface}`,
      `${APP_ROOT}/${surface}`,
    );
  }
}

/** Replaces an existing link but refuses other files to preserve agent data. */
async function ensureLink(target: string, source: string): Promise<undefined> {
  const info = await pathInfo(target);
  if (info?.isSymbolicLink() === true) {
    const linked = resolve(dirname(target), await readlink(target));
    if (linked === resolve(source)) {
      return undefined;
    }
    await unlink(target);
  } else if (info !== undefined) {
    throw new Error(
      `NanoClaw state surface already exists and is not a link: ${target}`,
    );
  }
  await mkdir(dirname(target), { recursive: true });
  await symlink(source, target);
}

function startHost(config: ApplicationConfig): ChildProcess {
  const environment = {
    ...process.env,
    ASSISTANT_NAME: config.agentName,
    CONTAINER_IMAGE: "social-harness-nanoclaw-agent:embedded",
    DEFAULT_AGENT_PROVIDER: "claude",
    NANOCLAW_GATEWAY_PROVIDER: "social-harness-process",
    NANOCLAW_RUNTIME_DRIVER: "social-harness-process",
    NANOCLAW_TEMPLATES_DIR: `${config.stateDirectory}/templates`,
    NODE_ENV: "production",
    TMPDIR: `${config.stateDirectory}/tmp`,
  };
  return spawn(
    process.execPath,
    ["--import", PRELOAD, `${APP_ROOT}/dist/index.js`],
    { cwd: config.stateDirectory, env: environment, stdio: "inherit" },
  );
}

/**
 * Closes the gateway and forwards the signal. A gateway that starts after the
 * signal remains open until the host exits.
 */
function stopHost(
  host: ChildProcess,
  supervision: Supervision,
  signal: NodeJS.Signals,
): void {
  supervision.gateway?.close();
  if (host.exitCode === null) {
    host.kill(signal);
  }
}

/** Wait for the host's CLI socket. A host that exits first fails startup. */
async function waitForSocket(
  socketPath: string,
  host: ChildProcess,
): Promise<undefined> {
  const deadline = Date.now() + STARTUP_TIMEOUT_MILLIS;
  while (Date.now() < deadline) {
    if (host.exitCode !== null || host.signalCode !== null) {
      throw new Error(
        `NanoClaw host exited before its CLI socket was ready (${String(host.exitCode ?? host.signalCode)})`,
      );
    }
    const info = await pathInfo(socketPath);
    if (info?.isSocket() === true) {
      return undefined;
    }
    await delay(SOCKET_POLL_MILLIS);
  }
  throw new Error(
    `NanoClaw CLI socket was not ready within ${String(STARTUP_TIMEOUT_MILLIS)}ms`,
  );
}

function hostExit(host: ChildProcess, gateway: Gateway): Promise<number> {
  return new Promise((resolveExit, reject) => {
    host.once("error", reject);
    host.once("exit", (code, signal) => {
      gateway.close();
      resolveExit(
        code ?? (signal === "SIGTERM" || signal === "SIGINT" ? 0 : 1),
      );
    });
  });
}

async function pathInfo(path: string): Promise<Stats | undefined> {
  try {
    return await lstat(path);
  } catch (error) {
    if (error instanceof Error && "code" in error && error.code === "ENOENT") {
      return undefined;
    }
    throw error;
  }
}
