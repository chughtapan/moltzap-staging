/** @file The install-channel.ts script installs the channel and pins the core packages at the one version it is given. */
import assert from "node:assert/strict";
import { type ChildProcess, execFile } from "node:child_process";
import { once } from "node:events";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test, type TestContext } from "node:test";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const run = promisify(execFile);
const prepare = fileURLToPath(new URL("./install-channel.ts", import.meta.url));
/**
 * The cleanup hook's own timeout. Without one, a hook takes the run's
 * `--test-timeout`, and a hook cut off before its `rm` leaves the root.
 */
const CLEANUP_MILLIS = 30_000;

test("the workspace channel is installed into the host", async (t) => {
  const { root, app, runPrepare } = await fixture(t);

  await runPrepare([app, join(root, "sharness.ts")]);

  assert.equal(
    await readFile(join(app, "src/channels/sharness.ts"), "utf8"),
    "export const channel = 1;\n",
  );
  assert.equal(
    await readFile(join(app, "src/sharness-mcp/mcp-client.ts"), "utf8"),
    "export const client = 1;\n",
  );
  assert.equal(
    await readFile(join(app, "src/sharness-mcp/inbox-wakeups.ts"), "utf8"),
    "export const inbox = 1;\n",
  );
});

/** A NanoClaw tree under a temporary root, and how to run install-channel.ts on it. */
interface Fixture {
  readonly root: string;
  readonly app: string;
  /** Runs install-channel.ts with `args`, settling as the script exits. */
  readonly runPrepare: (args: readonly string[]) => Promise<unknown>;
}

/**
 * A stock-shaped NanoClaw tree with a `prepare` hook, and a channel file
 * beside it, under a root that one hook removes once `t` ends. A cancelled
 * or timed-out test neither waits for its tree to be written nor for its
 * install-channel.ts run, and both keep going, so the hook refuses later runs,
 * waits for the tree, kills a run still going and only then removes the
 * root. It is registered before the root exists, since node:test never runs
 * a hook registered on a test that has already ended.
 */
async function fixture(t: TestContext): Promise<Fixture> {
  const allocated = mkdtemp(join(tmpdir(), "nanoclaw-prepare-"));
  const written = writeTree(allocated);
  const runs: ChildProcess[] = [];
  let ended = false;
  t.after(
    async () => {
      ended = true;
      await written.catch(() => undefined);
      await Promise.all(runs.map((child) => killed(child)));
      await rm(await allocated, { recursive: true, force: true });
    },
    { timeout: CLEANUP_MILLIS },
  );
  const root = await written;
  return {
    root,
    app: join(root, "app"),
    runPrepare: async (args) => {
      if (ended) {
        throw new Error("the test ended before install-channel.ts started");
      }
      const pending = run(process.execPath, [prepare, ...args]);
      runs.push(pending.child);
      return await pending;
    },
  };
}

/** Write the NanoClaw tree and the channel file under the root `allocated` names. */
async function writeTree(allocated: Promise<string>): Promise<string> {
  const root = await allocated;
  const app = join(root, "app");
  await mkdir(join(app, "src/channels"), { recursive: true });
  await writeFile(
    join(app, "package.json"),
    JSON.stringify({
      scripts: { prepare: "husky" },
      dependencies: { zod: "3.0.0" },
    }),
  );
  await writeFile(
    join(app, "pnpm-workspace.yaml"),
    "onlyBuiltDependencies:\n  - esbuild\n",
  );
  await writeFile(join(root, "sharness.ts"), "export const channel = 1;\n");
  await mkdir(join(root, "mcp"));
  await writeFile(
    join(root, "mcp/mcp-client.ts"),
    "export const client = 1;\n",
  );
  await writeFile(
    join(root, "mcp/inbox-wakeups.ts"),
    "export const inbox = 1;\n",
  );
  await writeFile(
    join(app, "tsconfig.json"),
    JSON.stringify({ compilerOptions: { rootDir: "./src", outDir: "./dist" } }),
  );
  return root;
}

/** SIGKILL `child` unless it has already exited, and wait for its exit. */
async function killed(child: ChildProcess): Promise<undefined> {
  if (child.exitCode !== null || child.signalCode !== null) {
    return;
  }
  const exited = once(child, "exit");
  child.kill("SIGKILL");
  await exited;
}
