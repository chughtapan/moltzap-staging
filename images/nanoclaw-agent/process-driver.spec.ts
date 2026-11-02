/**
 * @file Exercises the NanoClaw image's process session driver through its
 * public driver interface, with a real agent process and real mount links on
 * a temporary directory.
 */
import assert from "node:assert/strict";
import {
  mkdir,
  mkdtemp,
  readFile,
  realpath,
  rm,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test, { type TestContext } from "node:test";
import { setTimeout as delay } from "node:timers/promises";

import {
  createProcessSessionDriver,
  materializeProcessMounts,
  type SessionDriver,
  type SessionSpec,
} from "./process-driver.ts";

/**
 * The agent the session runs: it records its working directory and
 * environment in `observed.json` and exits 7.
 */
const AGENT_SCRIPT = `
  require("node:fs").writeFileSync(
    "observed.json",
    JSON.stringify({ cwd: process.cwd(), env: process.env }),
  );
  process.exit(7);
`;
/**
 * A descendant of the agent that ignores `SIGTERM` and writes its pid to
 * its parent once it does. It ends on its own after 30 s, so one that a
 * failing run leaves behind still ends.
 */
const DESCENDANT_SCRIPT = `
  process.on("SIGTERM", () => undefined);
  process.stdout.write(String(process.pid));
  setTimeout(() => undefined, 30_000);
`;
/** How long an ended process may take to be reaped. */
const REAP_TIMEOUT_MILLIS = 5_000;
const REAP_POLL_MILLIS = 25;

test("runs one shared session and reports its terminal state", async (t) => {
  const root = await temporaryRoot(t);
  const workingDirectory = join(root, "container/workspace/agent");
  await mkdir(workingDirectory, { recursive: true });
  const driver = createProcessSessionDriver({
    policy: {},
    validateSpec: () => undefined,
    specInvalid: invalid,
    onHostShutdown: () => undefined,
    resolveContainerPath: (path) => join(root, "container", path),
    workingDirectory: "/workspace/agent",
    baseEnvironment: { BASE_VALUE: "base" },
  });
  const ended = nextTerminalEvent(driver);

  const handle = await driver.prepare(sessionSpec());
  assert.strictEqual(await driver.prepare(sessionSpec()), handle);
  await handle.start();
  assert.deepEqual(await handle.status(), { phase: "running" });
  await ended;

  assert.partialDeepStrictEqual(
    JSON.parse(await readFile(join(workingDirectory, "observed.json"), "utf8")),
    {
      cwd: await realpath(workingDirectory),
      env: {
        BASE_VALUE: "base",
        SPEC_VALUE: "spec",
        CONTRIBUTED_VALUE: "contributed",
      },
    },
  );
  assert.deepEqual(await handle.status(), {
    phase: "failed",
    failure: { kind: "started-then-died", retryable: false, exitCode: 7 },
  });
  await assert.rejects(
    driver.prepare(sessionSpec(AGENT_SCRIPT, "other")),
    /already bound/u,
  );
});

test("an agent inherits the host's Claude Code sign-in variables, telemetry settings and provider proxy", async (t) => {
  const root = await temporaryRoot(t);
  const workingDirectory = join(root, "container/workspace/agent");
  await mkdir(workingDirectory, { recursive: true });
  const forwarded = {
    ANTHROPIC_API_KEY: "api-key",
    CLAUDE_CODE_OAUTH_TOKEN: "token",
    CLAUDE_CODE_ENABLE_TELEMETRY: "1",
    OTEL_EXPORTER_OTLP_ENDPOINT: "https://receiver.invalid:4318",
    OTEL_TRACES_EXPORTER: "otlp",
    HTTPS_PROXY: "https://gateway.invalid:3128",
    https_proxy: "https://gateway.invalid:3128",
    NO_PROXY: "localhost",
    no_proxy: "localhost",
    NODE_EXTRA_CA_CERTS: "/run/social-harness/bootstrap/provider-proxy-ca.pem",
  };
  setHostEnvironment(t, { ...forwarded, HOST_ONLY_VALUE: "host" });
  const driver = createProcessSessionDriver({
    policy: {},
    validateSpec: () => undefined,
    specInvalid: invalid,
    onHostShutdown: () => undefined,
    resolveContainerPath: (path) => join(root, "container", path),
    workingDirectory: "/workspace/agent",
  });
  const ended = nextTerminalEvent(driver);

  await (await driver.prepare(sessionSpec())).start();
  await ended;

  const observed: unknown = JSON.parse(
    await readFile(join(workingDirectory, "observed.json"), "utf8"),
  );
  assert.partialDeepStrictEqual(observed, { env: forwarded });
});

test("the host's shutdown stops the running agent, reports it stopped, and prepares no other session", async (t) => {
  const root = await temporaryRoot(t);
  const workingDirectory = join(root, "container/workspace/agent");
  await mkdir(workingDirectory, { recursive: true });
  const shutdownCallbacks: Array<() => Promise<undefined>> = [];
  const driver = createProcessSessionDriver({
    policy: {},
    validateSpec: () => undefined,
    specInvalid: invalid,
    onHostShutdown: (stop) => {
      shutdownCallbacks.push(stop);
    },
    resolveContainerPath: (path) => join(root, "container", path),
    workingDirectory: "/workspace/agent",
  });
  const handle = await driver.prepare(
    sessionSpec("setInterval(() => {}, 1000);"),
  );
  t.after(() => handle.stop());
  await handle.start();

  await Promise.all(shutdownCallbacks.map((stop) => stop()));

  assert.deepEqual(await handle.status(), { phase: "stopped" });
  await assert.rejects(driver.prepare(sessionSpec()), /shutting down/u);
});

test("the host's shutdown ends every process in the agent's process group, even one that ignores SIGTERM", async (t) => {
  const root = await temporaryRoot(t);
  const workingDirectory = join(root, "container/workspace/agent");
  await mkdir(workingDirectory, { recursive: true });
  const shutdownCallbacks: Array<() => Promise<undefined>> = [];
  const driver = createProcessSessionDriver({
    policy: {},
    validateSpec: () => undefined,
    specInvalid: invalid,
    onHostShutdown: (stop) => {
      shutdownCallbacks.push(stop);
    },
    resolveContainerPath: (path) => join(root, "container", path),
    workingDirectory: "/workspace/agent",
  });
  const handle = await driver.prepare(
    sessionSpec(agentWithDescendant("setInterval(() => {}, 1000);")),
  );
  await handle.start();
  const descendant = await descendantPid(workingDirectory);

  await Promise.all(shutdownCallbacks.map((stop) => stop()));

  assert.equal(
    isRunning(descendant),
    false,
    `the agent's descendant ${String(descendant)} outlived the host's shutdown`,
  );
});

test("an agent that exits on its own ends every process in its process group", async (t) => {
  const root = await temporaryRoot(t);
  const workingDirectory = join(root, "container/workspace/agent");
  await mkdir(workingDirectory, { recursive: true });
  const driver = createProcessSessionDriver({
    policy: {},
    validateSpec: () => undefined,
    specInvalid: invalid,
    onHostShutdown: () => undefined,
    resolveContainerPath: (path) => join(root, "container", path),
    workingDirectory: "/workspace/agent",
  });
  const ended = nextTerminalEvent(driver);

  await (
    await driver.prepare(sessionSpec(agentWithDescendant("process.exit(0);")))
  ).start();
  await ended;
  const descendant = await descendantPid(workingDirectory);

  assert.equal(
    await reaped(descendant),
    true,
    `the agent's descendant ${String(descendant)} outlived the agent`,
  );
});

test("preserves a non-empty mount target", async (t) => {
  const root = await temporaryRoot(t);
  const source = join(root, "source");
  const target = join(root, "container/workspace");
  await Promise.all([mkdir(source), mkdir(target, { recursive: true })]);
  await writeFile(join(target, "owned.txt"), "keep");

  await assert.rejects(
    materializeProcessMounts(
      [{ hostPath: source, containerPath: "/workspace" }],
      {
        resolveContainerPath: (path) => join(root, "container", path),
        specInvalid: invalid,
      },
    ),
    /non-empty mount target/u,
  );
  assert.equal(await readFile(join(target, "owned.txt"), "utf8"), "keep");
});

function invalid(detail: string): Error {
  return Object.assign(new Error(detail), {
    kind: "spec-invalid",
    retryable: false,
    detail,
  });
}

function sessionSpec(
  agentScript = AGENT_SCRIPT,
  sessionId = "session",
): SessionSpec {
  return {
    key: { installSlug: "install", agentGroupId: "agent", sessionId },
    containers: [
      {
        role: "agent",
        env: { SPEC_VALUE: "spec" },
        contributedEnv: { CONTRIBUTED_VALUE: "contributed" },
        command: [process.execPath],
        args: ["--eval", agentScript],
        mounts: [],
      },
    ],
    stopGraceSeconds: 1,
  };
}

/**
 * An agent that starts `DESCENDANT_SCRIPT`, writes the descendant's pid to
 * `descendant.pid` once it ignores `SIGTERM`, then runs `next`.
 */
function agentWithDescendant(next: string): string {
  return `
    const descendant = require("node:child_process").spawn(
      process.execPath,
      ["--eval", ${JSON.stringify(DESCENDANT_SCRIPT)}],
      { stdio: ["ignore", "pipe", "ignore"] },
    );
    descendant.stdout.once("data", (pid) => {
      require("node:fs").writeFileSync("descendant.pid", pid);
      ${next}
    });
  `;
}

/** The pid the agent in `workingDirectory` recorded for its descendant. */
async function descendantPid(workingDirectory: string): Promise<number> {
  const path = join(workingDirectory, "descendant.pid");
  const deadline = Date.now() + REAP_TIMEOUT_MILLIS;
  while (Date.now() < deadline) {
    const recorded = await readFile(path, "utf8").catch(() => "");
    if (recorded.length > 0) {
      return Number(recorded);
    }
    await delay(REAP_POLL_MILLIS);
  }
  throw new Error(`the agent recorded no descendant in ${path}`);
}

/** Whether `pid` is gone within `REAP_TIMEOUT_MILLIS`. */
async function reaped(pid: number): Promise<boolean> {
  const deadline = Date.now() + REAP_TIMEOUT_MILLIS;
  while (isRunning(pid)) {
    if (Date.now() >= deadline) {
      return false;
    }
    await delay(REAP_POLL_MILLIS);
  }
  return true;
}

/** Whether a process `pid` exists, as `kill(pid, 0)` reports. */
function isRunning(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    if (error instanceof Error && "code" in error && error.code === "ESRCH") {
      return false;
    }
    throw error;
  }
}

function nextTerminalEvent(driver: SessionDriver): Promise<undefined> {
  return new Promise((resolveEvent) => {
    const watch = driver.watchSessions("install", (event) => {
      if (event.kind === "terminal") {
        watch.stop();
        resolveEvent(undefined);
      }
    });
  });
}

/** Sets host variables for one test and restores them when it ends. */
function setHostEnvironment(
  t: TestContext,
  values: Readonly<Record<string, string>>,
): void {
  const saved = Object.keys(values).map((name) => ({
    name,
    value: process.env[name],
  }));
  Object.assign(process.env, values);
  t.after(() => {
    for (const { name, value } of saved) {
      if (value === undefined) {
        Reflect.deleteProperty(process.env, name);
      } else {
        process.env[name] = value;
      }
    }
  });
}

async function temporaryRoot(t: TestContext): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), "social-harness-process-driver-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  return root;
}
