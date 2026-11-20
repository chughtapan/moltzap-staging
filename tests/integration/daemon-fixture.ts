/**
 * @file One `sharnessd` process that hosts every agent of a test.
 *
 * `sharedDaemon()` prepares a daemon's settings, database and OAuth issuer
 * and returns it without starting it:
 *
 * - `directory`: the directory holding its settings file.
 * - `start()`: runs the process until its scope closes or `stopProcess` stops
 *   it. Starting again serves the same database.
 * - `startupFailure()`: runs a process that must exit before it listens, and
 *   returns its exit code and output.
 * - `configure(changes)`: changes the settings the next start reads.
 * - `signInAs(owner)`: an MCP connection as the owner's agent, and the agent
 *   `status` reports for it. The owner `SERVER_AGENT_OWNER` is the server
 *   agent's, and any other is a client's.
 *
 * A suite drives the signed-in agents as their hosts do, through the official
 * MCP client and the tool helpers of `src/__tests__/daemon.ts`. `received`
 * reads what the inbox event delivers to an agent. `processTrace` makes a
 * trace a vitest test whose children stop with it.
 */

import type { TestContext } from "vitest";
import { FileSystem } from "@effect/platform";
import { NodeContext, NodeFileSystem } from "@effect/platform-node";
import {
  Client,
  StreamableHTTPClientTransport,
} from "@modelcontextprotocol/client";
import { EventsClientExtension } from "@modelcontextprotocol/client/ext/events";
import {
  Console,
  Data,
  Duration,
  Effect,
  Exit,
  Fiber,
  Redacted,
  type Scope,
  Stream,
} from "effect";
import { type ChildProcess, spawn } from "node:child_process";
import { createServer } from "node:net";
import { basename, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
  callTool,
  decodeInboxEntry,
  type SignedIn,
} from "../../src/__tests__/daemon.js";
import {
  issuerFixture,
  openTestDatabase,
  requiredScope,
  restrictedLogin,
} from "../../src/__tests__/test.js";
import { makeFirewall } from "../../src/firewall.js";
import { AgentName } from "../../src/messages.js";
import { openDaemon } from "../../src/server.js";

/** The name of the event whose occurrences are an agent's inbox entries. */
export const INBOX_EVENT = "org.social-harness/inbox.pending";

/** A fixture failure, with the child's output when it has any. */
export class ProcessTestError extends Data.TaggedError("ProcessTestError")<{
  readonly message: string;
  readonly cause?: unknown;
}> {}

/** The owner whose token signs the server agent in, as the `serverAgent` setting names it. */
export const SERVER_AGENT_OWNER = "server-agent-owner";

/** The name of the server agent of a daemon whose settings name no other. */
export const SERVER_AGENT_NAME = "server-agent";

/** A child process and everything it has printed. */
export interface RunningProcess {
  readonly child: ChildProcess;
  readonly logs: () => string;
}

/** A signed-in owner, with the Events extension of its connection. */
export interface Subscriber extends SignedIn {
  readonly events: EventsClientExtension;
}

/** Disposable children listen only on loopback. */
const LOOPBACK_HOST = "127.0.0.1";

const SHUTDOWN_TIMEOUT = Duration.seconds(5);

/** A child must become ready within thirty seconds, with its output on failure. */
const READINESS_TIMEOUT = Duration.seconds(30);

const POLL_INTERVAL = Duration.millis(25);

const WORKSPACE_ROOT = resolve(
  dirname(fileURLToPath(import.meta.url)),
  "../..",
);

/** Children use the current Node installation and system executables. */
const CHILD_PATH = `${dirname(process.execPath)}:/usr/bin:/bin`;

const DAEMON_COMMAND: readonly [string, readonly string[]] = [
  process.execPath,
  [join(WORKSPACE_ROOT, "dist/main.js")],
];

const failed =
  (message: string) =>
  (cause: unknown): ProcessTestError =>
    new ProcessTestError({ message, cause });

/** A free loopback port, released before a child binds it. */
const reservePort = Effect.async<number, ProcessTestError>((resume) => {
  const server = createServer();
  server.once("error", (cause) => {
    resume(Effect.fail(failed("could not reserve a port")(cause)));
  });
  server.listen(0, LOOPBACK_HOST, () => {
    const address = server.address();
    server.close(() => {
      resume(
        address === null || typeof address === "string"
          ? Effect.fail(failed("reserved listener exposed no port")(address))
          : Effect.succeed(address.port),
      );
    });
  });
});

/** Whether `child` has exited on its own or by a signal. */
const hasExited = (child: ChildProcess): boolean =>
  child.exitCode !== null || child.signalCode !== null;

/** Waits for the child and its output streams to close. */
const waitForExit = (running: RunningProcess): Effect.Effect<void> =>
  hasExited(running.child) &&
  running.child.stdout?.closed === true &&
  running.child.stderr?.closed === true
    ? Effect.void
    : Effect.async<undefined>((resume) => {
        const onClose = (): void => {
          resume(Effect.succeed(undefined));
        };
        running.child.once("close", onClose);
        return Effect.sync(() => {
          running.child.removeListener("close", onClose);
        });
      });

/**
 * Stops a child, killing it after its shutdown grace. A scope runs this
 * release uninterruptibly, and a race does not finish until its loser stops,
 * so both sides are interruptible: otherwise every teardown waits out the
 * full grace for a child that has already exited.
 */
export const stopProcess = Effect.fn("stopProcess")(function* (
  running: RunningProcess,
) {
  if (hasExited(running.child)) {
    return;
  }
  running.child.kill("SIGTERM");
  const stopped = yield* Effect.raceFirst(
    waitForExit(running).pipe(Effect.as(true), Effect.interruptible),
    Effect.sleep(SHUTDOWN_TIMEOUT).pipe(Effect.as(false), Effect.interruptible),
  );
  if (!stopped) {
    running.child.kill("SIGKILL");
    yield* waitForExit(running);
  }
});

/**
 * Starts a child that the enclosing scope stops. The child sees only
 * `environment` and a `PATH`. A scope closed by a failure or an interruption,
 * as a timed-out test's is, first prints the child's output.
 */
const managedProcess = Effect.fn("managedProcess")(function* (
  executable: string,
  processArguments: readonly string[],
  environment: Readonly<Record<string, string>>,
) {
  const child = spawn(executable, processArguments, {
    cwd: WORKSPACE_ROOT,
    env: { PATH: CHILD_PATH, ...environment },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let output = "";
  const collect = (chunk: string): void => {
    output += chunk;
  };
  child.stdout.setEncoding("utf8").on("data", collect);
  child.stderr.setEncoding("utf8").on("data", collect);
  const running: RunningProcess = { child, logs: () => output };
  yield* Effect.addFinalizer((exit) =>
    Exit.isFailure(exit)
      ? Console.error(
          `${basename(executable)} (pid ${child.pid}) output when the trace failed:\n${output}`,
        ).pipe(Effect.zipRight(stopProcess(running)))
      : stopProcess(running),
  );
  return running;
}, Effect.uninterruptible);

/**
 * Whether the daemon printed its listening line for `endpoint`. It prints the
 * line only after binding, so another process holding a released port cannot
 * satisfy readiness the way it could answer a connection.
 */
const daemonListening = (running: RunningProcess, endpoint: URL) =>
  Effect.sync(() =>
    running.logs().includes(`sharnessd listening on ${endpoint.href} (`),
  );

/** Waits until the daemon has printed its listening line, and fails with its output when it exits first or thirty seconds pass. */
const awaitListening = Effect.fn("awaitListening")(
  (running: RunningProcess, endpoint: URL) =>
    Effect.gen(function* () {
      while (!hasExited(running.child)) {
        if (yield* daemonListening(running, endpoint)) {
          return;
        }
        yield* Effect.sleep(POLL_INTERVAL);
      }
      return yield* new ProcessTestError({
        message: `process exited before listening\n${running.logs()}`,
      });
    }).pipe(
      Effect.timeoutFail({
        duration: READINESS_TIMEOUT,
        onTimeout: () =>
          new ProcessTestError({
            message: `process timed out before listening\n${running.logs()}`,
          }),
      }),
    ),
);

/** Runs a daemon that must exit instead of serving, and gives its exit code and output. */
const startupFailure = (
  spawned: Effect.Effect<RunningProcess, never, Scope.Scope>,
) =>
  Effect.gen(function* () {
    const running = yield* spawned;
    yield* waitForExit(running).pipe(
      Effect.timeoutFail({
        duration: READINESS_TIMEOUT,
        onTimeout: () =>
          new ProcessTestError({
            message: `daemon did not exit\n${running.logs()}`,
          }),
      }),
    );
    return { exitCode: running.child.exitCode, logs: running.logs() };
  }).pipe(Effect.withSpan("startupFailure"));

/**
 * Writes `initial` as the settings file at `file` and returns the
 * operation that changes it.
 */
const settingsDocument = (
  file: string,
  initial: Readonly<Record<string, unknown>>,
) =>
  Effect.gen(function* () {
    const filesystem = yield* FileSystem.FileSystem;
    let document = initial;
    const configure = (changes: Readonly<Record<string, unknown>>) =>
      Effect.suspend(() => {
        document = { ...document, ...changes };
        return filesystem.writeFileString(file, JSON.stringify(document));
      }).pipe(Effect.mapError(failed("could not write the daemon settings")));
    yield* configure({});
    return configure;
  });

/** Connects to the daemon at `endpoint` as the holder of `bearer` and reads its agent from `status`. */
const signIn = (endpoint: URL, bearer: Redacted.Redacted) =>
  Effect.gen(function* () {
    const events = new EventsClientExtension();
    const mcp = new Client(
      { name: "integration-test", version: "test" },
      { versionNegotiation: { mode: "auto" }, extensions: [events] },
    );
    yield* Effect.acquireRelease(
      Effect.tryPromise(() =>
        mcp.connect(
          new StreamableHTTPClientTransport(endpoint, {
            requestInit: {
              headers: { Authorization: `Bearer ${Redacted.value(bearer)}` },
            },
          }),
        ),
      ),
      () => Effect.tryPromise(() => mcp.close()).pipe(Effect.ignore),
    );
    const { agentName } = yield* callTool({ mcp }, "status");
    const subscriber: Subscriber = { name: agentName, mcp, events };
    return subscriber;
  }).pipe(
    Effect.mapError(failed("could not sign in to the daemon")),
    Effect.withSpan("signIn"),
  );

/**
 * Creates the daemon's tables as the login of `url`,
 * which may create tables, so the daemon serves them through a login that
 * may not.
 */
const preparedBy = (url: Redacted.Redacted, issuer: string) =>
  openDaemon(url, {
    issuer,
    subject: SERVER_AGENT_OWNER,
    name: AgentName.make(SERVER_AGENT_NAME),
    firewall: makeFirewall({
      me: AgentName.make(SERVER_AGENT_NAME),
      historyLimit: 1,
    }),
  }).pipe(
    Effect.scoped,
    Effect.mapError(failed("could not prepare the daemon's database")),
  );

/**
 * Prepares one daemon whose server agent's owner and clients' owners sign in
 * with tokens from its own test issuer. The scope removes its directory and
 * stops its issuer and every process and connection it started.
 */
export const sharedDaemon = Effect.fn("sharedDaemon")(function* () {
  const filesystem = yield* FileSystem.FileSystem;
  const directory = yield* filesystem
    .makeTempDirectoryScoped({ prefix: "sharness-daemon-" })
    .pipe(Effect.mapError(failed("could not create the daemon directory")));
  const issuer = yield* issuerFixture().pipe(
    Effect.mapError(failed("could not start the test issuer")),
  );
  const port = yield* reservePort;
  const endpoint = new URL(`http://${LOOPBACK_HOST}:${port}/mcp`);
  const resource = `https://${LOOPBACK_HOST}:${port}/mcp`;
  const database = yield* openTestDatabase();
  yield* preparedBy(database.url, issuer.issuer);
  const restrictedUrl = yield* restrictedLogin(database);
  const environment = {
    SHARNESS_CONFIG_FILE: join(directory, "settings.json"),
    SHARNESS_DATABASE_URL: Redacted.value(restrictedUrl),
  };
  const configure = yield* settingsDocument(environment.SHARNESS_CONFIG_FILE, {
    port,
    host: LOOPBACK_HOST,
    authentication: {
      resource,
      issuer: issuer.issuer,
      scope: requiredScope,
    },
    serverAgent: { name: SERVER_AGENT_NAME, subject: SERVER_AGENT_OWNER },
  });
  const spawned = managedProcess(...DAEMON_COMMAND, environment);
  const bearer = (owner: string) =>
    issuer
      .token(owner, resource)
      .pipe(
        Effect.map(Redacted.make),
        Effect.mapError(failed(`could not issue a token for ${owner}`)),
      );
  return {
    directory,
    start: () =>
      spawned.pipe(
        Effect.tap((running) => awaitListening(running, endpoint)),
        Effect.withSpan("start"),
      ),
    startupFailure: () => startupFailure(spawned),
    configure,
    signInAs: (owner: string) =>
      Effect.flatMap(bearer(owner), (presented) => signIn(endpoint, presented)),
  };
}, Effect.provide(NodeFileSystem.layer));

/** Teardown waits for every scoped child after a test is interrupted. */
const TEARDOWN_TIMEOUT_MS = 60_000;

/**
 * The vitest test function of a real-process trace. A test that times out is
 * failed without waiting for its body, and the worker can exit before an
 * interrupted trace's scope has stopped its children, which then outlive the
 * run. The trace therefore runs as a fiber that an `onTestFinished` hook
 * interrupts, waiting until its scope has stopped every child.
 * @param trace A trace whose scope owns every process it starts.
 * @returns The test function, whose promise settles with the trace.
 */
export function processTrace<E>(
  trace: Effect.Effect<void, E, FileSystem.FileSystem | Scope.Scope>,
) {
  return (context: TestContext) => {
    const fiber = Effect.runFork(
      Effect.scoped(trace).pipe(Effect.provide(NodeContext.layer)),
    );
    context.onTestFinished(
      () => Effect.runPromise(Fiber.interrupt(fiber).pipe(Effect.asVoid)),
      TEARDOWN_TIMEOUT_MS,
    );
    return Effect.runPromise(Fiber.join(fiber));
  };
}

/**
 * The agent's inbox entries after `cursor`, as its subscription to the inbox
 * event delivers them in `delivery` mode, or in the mode the client prefers.
 */
export function inboxStream(
  { events }: Pick<Subscriber, "events">,
  cursor: string,
  delivery?: "poll" | "push",
) {
  return Stream.unwrap(
    Effect.tryPromise(() =>
      events.subscribe({ name: INBOX_EVENT, cursor, delivery }),
    ).pipe(
      Effect.map((subscription) =>
        Stream.fromAsyncIterable(subscription, (cause) => cause),
      ),
    ),
  ).pipe(
    Stream.orDie,
    Stream.mapEffect((event) =>
      decodeInboxEntry(event.data).pipe(Effect.orDie),
    ),
  );
}

/** The first `count` messages the inbox event delivers to the agent, from its first entry. */
export const received = (agent: Pick<Subscriber, "events">, count: number) =>
  inboxStream(agent, "0").pipe(
    Stream.take(count),
    Stream.runCollect,
    Effect.map((entries) => [...entries].map((entry) => entry.message)),
  );
