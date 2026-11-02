/** @file The OpenClaw host wrapper prepares OpenClaw's state before the gateway and fails closed. */

import assert from "node:assert/strict";
import { type ChildProcess, spawn } from "node:child_process";
import { once } from "node:events";
import { closeSync, mkdtempSync, openSync, readFileSync } from "node:fs";
import {
  chmod,
  mkdir,
  readFile,
  rm,
  symlink,
  writeFile,
} from "node:fs/promises";
import { constants, tmpdir } from "node:os";
import { delimiter, dirname, join } from "node:path";
import { afterEach, beforeEach, test } from "node:test";
import { fileURLToPath } from "node:url";

import { isStringArray } from "../tests/test-support.ts";

const HOST = fileURLToPath(new URL("./host.sh", import.meta.url));
const PLAN = "/run/social-harness/bootstrap/secrets-plan.json";
const STEP_FAILURE_EXIT_CODE = 3;
/** Unlike 128 plus any signal number, so it can only be the gateway's own exit. */
const GATEWAY_OWN_EXIT_CODE = 42;
const KILLED_EXIT_CODE = 128 + constants.signals.SIGKILL;
const OPENCLAW_COMMANDS = [
  "doctor",
  "models",
  "migrate",
  "secrets",
  "gateway",
] as const;

/**
 * This process's environment with its own `node` first on PATH, since the
 * wrapper finds `node` there, and without its Codex home, so a developer's
 * own Codex login never reaches the wrapper. Each run gets an empty HOME
 * unless the test names one.
 */
const hostEnvironment: Record<string, string> = Object.fromEntries([
  ...Object.entries(process.env).filter(
    (entry): entry is [string, string] =>
      entry[0] !== "CODEX_HOME" && entry[1] !== undefined,
  ),
  ["PATH", [dirname(process.execPath), process.env.PATH ?? ""].join(delimiter)],
]);
/** The directories of the running test. */
const directories: string[] = [];
/** The hosts the running test spawned, each leading its own process group. */
const hosts: ChildProcess[] = [];
/**
 * Whether the running test's cleanup has begun. A cancelled test's body
 * keeps going after it, so directories and hosts refuse to start from then
 * on rather than outlive the cleanup that should have removed them.
 */
let ending = false;

beforeEach(() => {
  ending = false;
});

/**
 * Kill the test's hosts and wait for them to exit, then remove its
 * directories. A test awaits its host, but a cancelled or timed-out test
 * does not, so its host may still be running; one hook kills first and
 * removes after, so that host cannot write under a removed directory. It
 * takes the test's hosts and directories before anything else, so it never
 * reaches the next test's.
 */
afterEach(async () => {
  ending = true;
  const finished = directories.splice(0);
  await Promise.all(hosts.splice(0).map((host) => killGroup(host)));
  for (const directory of finished) {
    await rm(directory, { recursive: true, force: true });
  }
});

test("a failed doctor ends the host with its code and never starts the gateway", async () => {
  const fake = await fakeOpenClaw({ doctor: "fail" });
  const { code, stderr } = await runHost({
    OPENCLAW_ENTRY: fake.entry,
    OPENCLAW_SECRETS_PLAN: PLAN,
  });
  assert.equal(code, STEP_FAILURE_EXIT_CODE);
  assert.match(stderr, /doctor exited 3; not starting the gateway/u);
  assert.equal(await gatewayStarted(fake.record), false);
});

test("a missing configuration path ends the host with a clear error", async () => {
  const { code, stderr } = await runHost({ OPENCLAW_CONFIG_PATH: "" });
  assert.equal(code, 1);
  assert.match(stderr, /OPENCLAW_CONFIG_PATH is required/u);
});

test("a missing launcher release ends the host with a clear error", async () => {
  const { code, stderr } = await runHost({
    SHARNESS_LAUNCHER_DIRECTORY: temporaryDirectory("missing-release-"),
  });
  assert.equal(code, 1);
  assert.match(stderr, /launcher current release is missing/u);
});

test("a start selects the image channel configuration", async () => {
  const fake = await fakeOpenClaw();
  const root = temporaryDirectory("social-harness-channel-config-");
  const configPath = join(root, "openclaw.json");
  const config = JSON.stringify({
    plugins: {
      load: {
        paths: [
          "/opt/social-harness/launcher/current/node_modules/@moltzap/openclaw-channel",
        ],
      },
    },
  });
  await writeFile(configPath, config);

  const { code } = await runHost({
    OPENCLAW_ENTRY: fake.entry,
    OPENCLAW_CONFIG_PATH: configPath,
  });

  assert.equal(code, 0);
  assert.match(
    await readFile(configPath, "utf8"),
    /social-harness-launcher-.+current/u,
  );
});

test("a failed apply ends the host with its code and never starts the gateway", async () => {
  const fake = await fakeOpenClaw({ secrets: "fail" });
  const { code, stderr } = await runHost({
    OPENCLAW_ENTRY: fake.entry,
    OPENCLAW_SECRETS_PLAN: PLAN,
  });
  assert.equal(code, STEP_FAILURE_EXIT_CODE);
  assert.match(stderr, /secrets apply exited 3; not starting the gateway/u);
  assert.equal(await gatewayStarted(fake.record), false);
});

test("a signal-killed apply counts as a failed apply and never starts the gateway", async () => {
  const fake = await fakeOpenClaw({ secrets: "die" });
  const { code, stderr } = await runHost({
    OPENCLAW_ENTRY: fake.entry,
    OPENCLAW_SECRETS_PLAN: PLAN,
  });
  assert.equal(code, KILLED_EXIT_CODE);
  assert.match(
    stderr,
    new RegExp(`secrets apply exited ${String(KILLED_EXIT_CODE)}`, "u"),
  );
  assert.equal(await gatewayStarted(fake.record), false);
});

for (const signal of ["SIGTERM", "SIGINT"] as const) {
  test(`${signal} sent to the host reaches the gateway at the same pid, and the gateway's exit status is the host's`, async () => {
    const fake = await fakeOpenClaw({ gateway: "wait" });

    const { pid, code } = await runHost(
      { OPENCLAW_ENTRY: fake.entry },
      signalOnceGatewayStarts(fake.record, signal),
    );

    assert.equal(code, GATEWAY_OWN_EXIT_CODE);
    assert.deepEqual((await invocations(fake.record)).at(-1), { signal, pid });
  });
}

test("a missing OpenClaw entry ends the host before a gateway", async () => {
  const root = temporaryDirectory("social-harness-openclaw-host-");
  const { code, stderr } = await runHost({
    OPENCLAW_ENTRY: join(root, "absent.mjs"),
  });
  assert.equal(code, 1);
  assert.match(stderr, /exited 1; not starting the gateway/u);
});

test("a delivered Codex login is imported into an agent without an OpenAI profile before the gateway starts", async () => {
  const fake = await fakeOpenClaw();
  const home = await homeWithCodexLogin();

  const { code } = await runHost({ OPENCLAW_ENTRY: fake.entry, HOME: home });

  const commands = await commandsRun(fake.record);
  const imported = commands.findIndex(
    (command) => command.slice(0, 3).join(" ") === "migrate apply codex",
  );
  const importCommand = commands[imported] ?? [];
  assert.equal(code, 0);
  assert.notEqual(
    imported,
    -1,
    `no Codex import among ${JSON.stringify(commands)}`,
  );
  assert.equal(
    importCommand[importCommand.indexOf("--from") + 1],
    join(home, ".codex"),
  );
  assert.ok(
    imported < commands.findIndex((command) => command[0] === "gateway"),
    "the import runs before the gateway",
  );
});

test("an agent that already holds an OpenAI profile keeps it and is not imported again", async () => {
  const fake = await fakeOpenClaw({ models: "profile" });

  const { code } = await runHost({
    OPENCLAW_ENTRY: fake.entry,
    HOME: await homeWithCodexLogin(),
  });

  assert.equal(code, 0);
  assert.equal(
    (await commandsRun(fake.record)).some(
      (command) => command[0] === "migrate",
    ),
    false,
  );
  assert.equal(await gatewayStarted(fake.record), true);
});

test("a failed auth profile listing ends the host", async () => {
  const fake = await fakeOpenClaw({ models: "fail" });
  const { code, stderr } = await runHost({
    OPENCLAW_ENTRY: fake.entry,
    HOME: await homeWithCodexLogin(),
  });
  assert.equal(code, 1);
  assert.match(stderr, /auth profile listing failed/u);
});

test("a failed Codex login import ends the host with its code and never starts the gateway", async () => {
  const fake = await fakeOpenClaw({ migrate: "fail" });

  const { code, stderr } = await runHost({
    OPENCLAW_ENTRY: fake.entry,
    HOME: await homeWithCodexLogin(),
  });

  assert.equal(code, STEP_FAILURE_EXIT_CODE);
  assert.match(
    stderr,
    /codex login import exited 3; not starting the gateway/u,
  );
  assert.equal(await gatewayStarted(fake.record), false);
});

/** An OpenClaw command the stand-in tells apart by its first argument. */
type OpenClawCommand = (typeof OPENCLAW_COMMANDS)[number];

/**
 * How the stand-in behaves for one command. `fail` exits nonzero, `die` ends
 * the process with an untrappable signal so the wrapper sees no exit code,
 * `wait` records the signal the process receives and its pid before exiting
 * with the gateway's own code, and `profile` lists one `openai` auth profile.
 */
type FakeBehaviour = "ok" | "profile" | "fail" | "die" | "wait";

/** The stand-in's behaviour per OpenClaw command; an absent one succeeds. */
interface FakeOptions {
  readonly doctor?: "ok" | "fail";
  readonly models?: "ok" | "profile" | "fail";
  readonly migrate?: "ok" | "fail";
  readonly secrets?: "ok" | "fail" | "die";
  readonly gateway?: "ok" | "wait";
}

/** Paths of the stand-in's entry and of the record it appends to. */
interface FakeOpenClaw {
  readonly entry: string;
  readonly record: string;
}

/** How the wrapper ended, its pid, and what it wrote to stderr. */
interface HostExit {
  readonly pid?: number;
  readonly code: number | null;
  readonly signal: NodeJS.Signals | null;
  readonly stderr: string;
}

/**
 * Stands in for `/app/openclaw.mjs`: appends every invocation's arguments to
 * a record, then behaves as `options` says for the command its first
 * argument names. Signal handlers are installed before the first record
 * line, so a test that signals once a line exists never reaches a fake that
 * would die of the signal instead. `models` lists no `openai` auth profile
 * unless `options` asks for one.
 */
async function fakeOpenClaw(options: FakeOptions = {}): Promise<FakeOpenClaw> {
  const root = temporaryDirectory("social-harness-openclaw-host-");
  const entry = join(root, "openclaw.mjs");
  const record = join(root, "record.ndjson");
  await writeFile(
    entry,
    [
      "#!/usr/bin/env node",
      'import { appendFile } from "node:fs/promises";',
      'const signalled = new Promise((resolve) => { for (const name of ["SIGTERM", "SIGINT"]) process.once(name, () => resolve(name)); });',
      `const record = ${JSON.stringify(record)};`,
      'await appendFile(record, JSON.stringify(process.argv.slice(2)) + "\\n");',
      ...OPENCLAW_COMMANDS.map((command) => commandBranch(command, options)),
    ].join("\n"),
  );
  await chmod(entry, 0o755);
  return { entry, record };
}

/** The stand-in's branch for `command`, as `options` configures it. */
function commandBranch(command: OpenClawCommand, options: FakeOptions): string {
  const configured = options[command];
  const body =
    configured === undefined && command === "models"
      ? listProfiles([])
      : fakeBody(configured ?? "ok");
  return [
    `if (process.argv[2] === ${JSON.stringify(command)}) {`,
    body,
    "}",
  ].join("\n");
}

/** The stand-in's source for one behaviour. */
function fakeBody(behaviour: FakeBehaviour): string {
  switch (behaviour) {
    case "ok":
      return "process.exit(0);";
    case "profile":
      return listProfiles([{ id: "openai:default" }]);
    case "fail":
      return `process.exit(${String(STEP_FAILURE_EXIT_CODE)});`;
    case "die":
      return 'process.kill(process.pid, "SIGKILL"); await new Promise(() => {});';
    case "wait":
      return [
        "const keepAlive = setInterval(() => {}, 1000);",
        "const signal = await signalled;",
        "clearInterval(keepAlive);",
        'await appendFile(record, JSON.stringify({ signal, pid: process.pid }) + "\\n");',
        `process.exit(${String(GATEWAY_OWN_EXIT_CODE)});`,
      ].join("\n");
    default:
      return behaviour satisfies never;
  }
}

/** Stand-in source that prints OpenClaw's auth listing JSON for `profiles`. */
function listProfiles(profiles: readonly object[]): string {
  return `process.stdout.write(${JSON.stringify(JSON.stringify({ profiles }))}); process.exit(0);`;
}

/**
 * A HOME holding a delivered Codex login, as the CODEX_AUTH_JSON credential
 * writes it.
 */
async function homeWithCodexLogin(): Promise<string> {
  const home = temporaryDirectory("social-harness-openclaw-home-");
  await mkdir(join(home, ".codex"));
  await writeFile(join(home, ".codex", "auth.json"), "{}");
  return home;
}

/**
 * Runs the wrapper with `environment` added to the host environment, and
 * hands the child to `onStart` so a test can signal it.
 */
async function runHost(
  environment: Record<string, string>,
  onStart?: (child: ChildProcess) => void,
): Promise<HostExit> {
  const launcher = temporaryDirectory("social-harness-launcher-");
  await mkdir(join(launcher, "seed"));
  await symlink(join(launcher, "seed"), join(launcher, "current"));
  const configPath = join(launcher, "openclaw.json");
  await writeFile(configPath, "{}");
  const diagnosticPath = join(launcher, "host.stderr");
  const diagnosticFile = openSync(diagnosticPath, "w");
  return await new Promise((resolve, reject) => {
    refuseAfterEnd();
    const child = spawn("/bin/bash", [HOST], {
      detached: true,
      env: {
        ...hostEnvironment,
        SHARNESS_LAUNCHER_DIRECTORY: launcher,
        OPENCLAW_CONFIG_PATH: configPath,
        HOME: temporaryDirectory("social-harness-openclaw-home-"),
        ...environment,
      },
      stdio: ["ignore", "ignore", diagnosticFile],
    });
    hosts.push(child);
    child.once("error", reject);
    child.once("close", (code, signal) => {
      closeSync(diagnosticFile);
      const pid = child.pid;
      resolve({
        ...(pid === undefined ? {} : { pid }),
        code,
        signal,
        stderr: readFileSync(diagnosticPath, "utf8"),
      });
    });
    onStart?.(child);
  });
}

/**
 * Signals the wrapper exactly once, as soon as the gateway has started.
 * Interval callbacks overlap under load, so the flag, not `clearInterval`,
 * is what keeps a second signal from reaching a fake whose one-shot handler
 * is already spent. The poll ends with the wrapper, so a wrapper that never
 * starts the gateway fails its test instead of hanging it.
 */
function signalOnceGatewayStarts(
  record: string,
  signal: NodeJS.Signals,
): (child: ChildProcess) => void {
  return (child) => {
    let sent = false;
    const poll = setInterval(() => {
      void (async () => {
        if ((await gatewayStarted(record)) && !sent) {
          sent = true;
          clearInterval(poll);
          child.kill(signal);
        }
      })();
    }, 20);
    child.once("exit", () => {
      clearInterval(poll);
    });
  };
}

/** Whether the host has started the gateway. */
async function gatewayStarted(record: string): Promise<boolean> {
  return (await commandsRun(record)).some(
    (command) => command[0] === "gateway",
  );
}

/** The commands the host ran through the stand-in, in order. */
async function commandsRun(record: string): Promise<string[][]> {
  return (await invocations(record)).filter(isStringArray);
}

/** The stand-in's parsed record lines, empty before the first invocation. */
async function invocations(record: string): Promise<unknown[]> {
  const text = await readFile(record, "utf8").catch(() => "");
  return text
    .split("\n")
    .filter((line) => line.length > 0)
    .map((line): unknown => JSON.parse(line));
}

/** A fresh directory under the system's temporary directory, which `afterEach` removes. */
function temporaryDirectory(prefix: string): string {
  refuseAfterEnd();
  const directory = mkdtempSync(join(tmpdir(), prefix));
  directories.push(directory);
  return directory;
}

/** Fail a cancelled test's body that tries to start something after its cleanup began. */
function refuseAfterEnd(): void {
  if (ending) {
    throw new Error("the test ended before this started");
  }
}

/**
 * SIGKILL the process group `host` leads, so a preparation step it is
 * running dies with it, and wait for the host's exit. The group is signalled
 * even after its leader exited, for a step left running: Linux keeps a
 * group id reserved while any member lives, so a group that is still there
 * is this one, and ESRCH means it has already stopped.
 */
async function killGroup(host: ChildProcess): Promise<undefined> {
  if (host.pid === undefined) {
    return;
  }
  const running = host.exitCode === null && host.signalCode === null;
  const exited = running ? once(host, "exit") : undefined;
  killGroupMembers(host.pid);
  await exited;
}

/** SIGKILL process group `leader`; a group with no member left is already stopped. */
function killGroupMembers(leader: number): void {
  try {
    process.kill(-leader, "SIGKILL");
  } catch (cause) {
    if (
      !(cause instanceof Error && "code" in cause && cause.code === "ESRCH")
    ) {
      throw cause;
    }
  }
}
