/**
 * @file One `sharnessd` process that hosts every agent of a test.
 *
 * `sharedDaemon(options)` prepares a daemon's settings, database and OAuth
 * issuer and returns it without starting it:
 *
 * - `endpoint`, `directory`, `environment`: where the daemon listens, the
 *   directory holding its settings file, SQLite database and credential
 *   files, and the environment its process reads.
 * - `start(environment?)`: runs the process until its scope closes or
 *   `stopProcess` stops it. Starting again serves the same database.
 * - `startupFailure(environment?)`: runs a process that must exit before it
 *   listens, and returns its exit code and output.
 * - `configure(changes)`: changes the settings the next start reads.
 * - `bearer(owner)` and `serverAgentBearer`: what the host of a named owner,
 *   or of the server agent in client/server mode, presents.
 * - `signIn(bearer)` and `signInAs(owner)`: an MCP connection, and the agent
 *   `status` reports for it.
 *
 * The options are settings that replace the fixture's, such as the mode, the
 * firewall variant of the daemon, and a PostgreSQL database in place of the
 * SQLite file. `managedProcess`, `awaitReady` and `daemonListening` start and
 * await a daemon that runs some other way, such as in a container.
 */

import { FileSystem } from "@effect/platform";
import { NodeFileSystem } from "@effect/platform-node";
import {
  acquireMcpConnection,
  AgentAddress,
  type AgentId,
  type AgentName,
  callTool,
  type McpConnection,
} from "@social-harness/social-harness";
import { prepareDatabase } from "@social-harness/social-harness/service";
import {
  ConfigProvider,
  Console,
  Data,
  Duration,
  Effect,
  Exit,
  Redacted,
  Ref,
  type Scope,
} from "effect";
import { type ChildProcess, spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import { createRequire } from "node:module";
import { createServer } from "node:net";
import { basename, dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import {
  markedPolicy,
  writeMessagePolicy,
} from "../firewall/policy-fixture.js";
import { issuerFixture, requiredScope } from "./oauth-fixture.js";

/** A fixture failure, with the child's output when it has any. */
export class ProcessTestError extends Data.TaggedError("ProcessTestError")<{
  readonly message: string;
  readonly cause?: unknown;
}> {}

/** A child process and everything it has printed. */
export interface RunningProcess {
  readonly child: ChildProcess;
  readonly logs: () => string;
}

/** What `sharedDaemon` prepares differently from its defaults. */
export interface DaemonOptions {
  /**
   * Settings that replace the fixture's: `mode` for client/server mode,
   * `authentication` for a local bearer, a message policy, norms.
   */
  readonly settings?: Readonly<Record<string, unknown>>;
  /**
   * Runs `firewall/firewall-daemon.ts`, whose text rules the marked provider
   * judges, with the marked message policy unless the settings name another.
   */
  readonly firewall?: true;
  /** A PostgreSQL schema from `acquirePostgres`, in place of a SQLite file. */
  readonly database?: {
    readonly configureLocator: Redacted.Redacted;
    readonly runtimeLocator: Redacted.Redacted;
  };
}

/** A signed-in owner: its agent and an MCP connection as that agent. */
export interface SignedIn {
  /** The agent as `status` reports it and as messages name it. */
  readonly agent: { readonly agentId: AgentId; readonly agentName: AgentName };
  /** How other agents address this one. */
  readonly address: AgentAddress;
  readonly connection: McpConnection;
  /** A file holding the bearer the agent signed in with, for a host to read. */
  readonly credentialFile: string;
}

/** A prepared daemon. The file comment lists what each member is for. */
export interface Daemon {
  readonly endpoint: URL;
  readonly directory: string;
  readonly environment: Readonly<Record<string, string>>;
  readonly start: (
    environment?: Readonly<Record<string, string>>,
  ) => Effect.Effect<RunningProcess, ProcessTestError, Scope.Scope>;
  readonly startupFailure: (
    environment?: Readonly<Record<string, string>>,
  ) => Effect.Effect<
    { readonly exitCode: number | null; readonly logs: string },
    ProcessTestError,
    Scope.Scope
  >;
  readonly configure: (
    changes: Readonly<Record<string, unknown>>,
  ) => Effect.Effect<void, ProcessTestError>;
  readonly bearer: (
    owner: string,
  ) => Effect.Effect<Redacted.Redacted, ProcessTestError>;
  readonly serverAgentBearer: Redacted.Redacted;
  readonly signIn: (
    bearer: Redacted.Redacted,
  ) => Effect.Effect<SignedIn, ProcessTestError, Scope.Scope>;
  readonly signInAs: (
    owner: string,
  ) => Effect.Effect<SignedIn, ProcessTestError, Scope.Scope>;
}

/** Disposable children listen only on loopback. */
export const LOOPBACK_HOST = "127.0.0.1";

const SHUTDOWN_TIMEOUT = Duration.seconds(5);

/** A child must become ready within thirty seconds, with its output on failure. */
const READINESS_TIMEOUT = Duration.seconds(30);

const POLL_INTERVAL = Duration.millis(25);

const WORKSPACE_ROOT = resolve(
  dirname(fileURLToPath(import.meta.url)),
  "../../..",
);

/** Children use the current Node installation and system executables. */
const CHILD_PATH = `${dirname(process.execPath)}:/usr/bin:/bin`;

/** Resolves the tsx loader independently of a child's working directory. */
const require = createRequire(import.meta.url);

const DAEMON_COMMAND: readonly [string, readonly string[]] = [
  process.execPath,
  [join(WORKSPACE_ROOT, "dist/main.js")],
];

const FIREWALL_DAEMON_COMMAND: readonly [string, readonly string[]] = [
  process.execPath,
  [
    "--conditions=sharness-test",
    "--import",
    require.resolve("tsx"),
    join(WORKSPACE_ROOT, "tests/integration/firewall/firewall-daemon.ts"),
  ],
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
export const managedProcess = Effect.fn("managedProcess")(function* (
  executable: string,
  processArguments: readonly string[],
  environment: Readonly<Record<string, string>> = {},
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
export const daemonListening = (running: RunningProcess, endpoint: URL) =>
  Effect.sync(() =>
    running.logs().includes(`sharnessd listening on ${endpoint.href} (`),
  );

/**
 * Polls `probe` until it reads true, and fails with the child's output when
 * the child exits first or `timeout` passes. `phase` names what the child had
 * yet to do.
 */
export const awaitReady = Effect.fn("awaitReady")(
  (
    running: RunningProcess,
    probe: Effect.Effect<boolean>,
    phase: string,
    timeout: Duration.DurationInput = READINESS_TIMEOUT,
  ) =>
    Effect.gen(function* () {
      while (!hasExited(running.child)) {
        if ((yield* probe) && !hasExited(running.child)) {
          return;
        }
        yield* Effect.sleep(POLL_INTERVAL);
      }
      return yield* new ProcessTestError({
        message: `process exited before ${phase}\n${running.logs()}`,
      });
    }).pipe(
      Effect.timeoutFail({
        duration: timeout,
        onTimeout: () =>
          new ProcessTestError({
            message: `process timed out before ${phase}\n${running.logs()}`,
          }),
      }),
    ),
);

/** Runs a daemon that must exit before it listens on `endpoint`. */
const startupFailure = (
  spawned: Effect.Effect<RunningProcess, never, Scope.Scope>,
  endpoint: URL,
) =>
  Effect.gen(function* () {
    const running = yield* spawned;
    const listened = yield* awaitReady(
      running,
      daemonListening(running, endpoint),
      "listening",
    ).pipe(
      Effect.as(true),
      Effect.catchIf(
        () => hasExited(running.child),
        () => Effect.succeed(false),
      ),
    );
    if (listened) {
      return yield* new ProcessTestError({
        message: `daemon listened unexpectedly\n${running.logs()}`,
      });
    }
    yield* waitForExit(running);
    return { exitCode: running.child.exitCode, logs: running.logs() };
  }).pipe(Effect.withSpan("startupFailure"));

/**
 * Writes `initial` as the settings document at `file` and returns the
 * operation that changes it.
 */
const settingsDocument = (
  file: string,
  initial: Readonly<Record<string, unknown>>,
) =>
  Effect.gen(function* () {
    const filesystem = yield* FileSystem.FileSystem;
    const write = (document: Readonly<Record<string, unknown>>) =>
      filesystem
        .writeFileString(file, JSON.stringify(document))
        .pipe(Effect.mapError(failed("could not write the daemon settings")));
    const document = yield* Ref.make(initial);
    yield* write(initial);
    return (changes: Readonly<Record<string, unknown>>) =>
      Ref.updateAndGet(document, (current) => ({
        ...current,
        ...changes,
      })).pipe(Effect.flatMap(write));
  });

/**
 * Connects to the daemon at `endpoint` as the holder of `bearer`, reads its
 * agent from `status` and saves the bearer in `directory`.
 */
const signIn = (endpoint: URL, directory: string, bearer: Redacted.Redacted) =>
  Effect.gen(function* () {
    const filesystem = yield* FileSystem.FileSystem;
    const connection = yield* acquireMcpConnection(
      { endpoint, credential: bearer },
      { name: "integration-test", version: "test" },
    );
    const status = yield* callTool(connection.client, {
      name: "status",
      arguments: {},
    });
    const credentialFile = join(directory, `${status.agentName}.credential`);
    yield* filesystem.writeFileString(credentialFile, Redacted.value(bearer), {
      mode: 0o600,
    });
    return {
      agent: { agentId: status.agentId, agentName: status.agentName },
      address: AgentAddress.make(`agent:${status.agentName}`),
      connection,
      credentialFile,
    };
  }).pipe(
    Effect.mapError(failed("could not sign in to the daemon")),
    Effect.provide(NodeFileSystem.layer),
    Effect.withSpan("signIn"),
  );

/** Creates the daemon's tables and encryption check as deployment does. */
const prepare = (environment: Readonly<Record<string, string>>) =>
  prepareDatabase.pipe(
    Effect.withConfigProvider(
      ConfigProvider.fromMap(new Map(Object.entries(environment))),
    ),
    Effect.mapError(failed("could not prepare the daemon database")),
  );

/**
 * Prepares one daemon in P2P mode whose owners sign in with tokens from its
 * own test issuer. The scope removes its directory and stops its issuer and
 * every process and connection it started.
 */
export const sharedDaemon = Effect.fn("sharedDaemon")(function* (
  options: DaemonOptions = {},
) {
  const filesystem = yield* FileSystem.FileSystem;
  const directory = yield* filesystem
    .makeTempDirectoryScoped({ prefix: "sharness-daemon-" })
    .pipe(Effect.mapError(failed("could not create the daemon directory")));
  const issuer = yield* issuerFixture.pipe(
    Effect.mapError(failed("could not start the test issuer")),
  );
  const port = yield* reservePort;
  const endpoint = new URL(`http://${LOOPBACK_HOST}:${port}/mcp`);
  const resource = `https://${LOOPBACK_HOST}:${port}/mcp`;
  const serverAgentBearer = Redacted.make(
    randomBytes(32).toString("base64url"),
  );
  const sqlite = pathToFileURL(
    join(directory, "sharnessd.sqlite3"),
  ).href.replace(/^file:/u, "sqlite:");
  const environment = {
    SHARNESS_CONFIG_FILE: join(directory, "settings.json"),
    SHARNESS_SERVER_AGENT_CREDENTIAL: Redacted.value(serverAgentBearer),
    SHARNESS_DATABASE_URL: Redacted.value(
      options.database?.runtimeLocator ?? Redacted.make(sqlite),
    ),
    SHARNESS_DATABASE_KEY: randomBytes(32).toString("base64url"),
  };
  const configure = yield* settingsDocument(environment.SHARNESS_CONFIG_FILE, {
    port,
    host: LOOPBACK_HOST,
    authentication: {
      kind: "oauth",
      resource,
      issuer: issuer.issuer,
      scope: requiredScope,
    },
    ...(options.firewall === true &&
    options.settings?.messagePolicyFile === undefined
      ? yield* writeMessagePolicy(directory, markedPolicy()).pipe(
          Effect.mapError(failed("could not write the message policy")),
        )
      : {}),
    ...options.settings,
  });
  yield* prepare({
    ...environment,
    SHARNESS_DATABASE_URL: Redacted.value(
      options.database?.configureLocator ?? Redacted.make(sqlite),
    ),
  });
  const spawned = (changes: Readonly<Record<string, string>>) =>
    managedProcess(
      ...(options.firewall === true ? FIREWALL_DAEMON_COMMAND : DAEMON_COMMAND),
      { ...environment, ...changes },
    );
  const bearer = (owner: string) =>
    issuer
      .token(owner, resource)
      .pipe(
        Effect.map(Redacted.make),
        Effect.mapError(failed(`could not issue a token for ${owner}`)),
      );
  const daemon: Daemon = {
    endpoint,
    directory,
    environment,
    start: (changes = {}) =>
      spawned(changes).pipe(
        Effect.tap((running) =>
          awaitReady(running, daemonListening(running, endpoint), "listening"),
        ),
        Effect.withSpan("start"),
      ),
    startupFailure: (changes = {}) =>
      startupFailure(spawned(changes), endpoint),
    configure,
    bearer,
    serverAgentBearer,
    signIn: (presented) => signIn(endpoint, directory, presented),
    signInAs: (owner) =>
      Effect.flatMap(bearer(owner), (presented) =>
        signIn(endpoint, directory, presented),
      ),
  };
  return daemon;
}, Effect.provide(NodeFileSystem.layer));
