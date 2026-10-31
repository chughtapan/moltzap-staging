/** @file Shared real-process fixtures for the cross-package suites. */

import type { startRegistry } from "@moltzap/registry/test";
import type { startRouter } from "@moltzap/router/test";
import { FileSystem, HttpClient } from "@effect/platform";
import { NodeFileSystem, NodeHttpClient } from "@effect/platform-node";
import {
  Client,
  ProtocolError,
  StreamableHTTPClientTransport,
} from "@modelcontextprotocol/client";
import {
  AgentName,
  AgentSigningAuthority,
  Ed25519PublicKey,
  InboundItem,
  type MessageAddressInput,
  PrincipalId,
} from "@moltzap/social-harness";
import { OperationId } from "@moltzap/social-harness/service";
import {
  managementReadConversationResultSchema,
  type managementRegisterRequestSchema,
  managementRegisterResultSchema,
  managementSearchConversationsResultSchema,
  managementStatusResultSchema,
  WireVersions,
} from "@moltzap/social-harness/test";
import {
  Console,
  Data,
  Duration,
  Effect,
  Exit,
  Option,
  Redacted,
  Ref,
  Schema,
  Scope,
} from "effect";
import { type ChildProcess, spawn } from "node:child_process";
import { generateKeyPairSync, randomBytes } from "node:crypto";
import { createRequire } from "node:module";
import { createServer } from "node:net";
import { basename, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

type ManagementRegisterRequest = typeof managementRegisterRequestSchema.Type;

type ManagementRegisterResult = typeof managementRegisterResultSchema.Type;

type ManagementStatusResult = typeof managementStatusResultSchema.Type;

type ManagementSearchConversationsResult =
  typeof managementSearchConversationsResultSchema.Type;

type ManagementReadConversationResult =
  typeof managementReadConversationResultSchema.Type;

/** Every child composition receives an explicit emitted version and acceptance sets. */
interface ProcessWireVersions {
  readonly emitted: string;
  readonly live: readonly string[];
  readonly history: readonly string[];
}

/** The mode of the firewall a daemon fixture installs; absent for `sharnessd` itself. */
export type FirewallMode = "guidance" | "enforcement";

/** One closed process-fixture failure with captured child output when available. */
export class ProcessTestError extends Data.TaggedError("ProcessTestError")<{
  readonly message: string;
  readonly cause?: unknown;
}> {}

/** Captured output remains available after the child stops. */
export interface RunningProcess {
  readonly child: ChildProcess;
  readonly logs: () => string;
  readonly stderr: () => string;
}

/** Shared Registry and Router coordinates for two endpoint daemons. */
export interface ProcessInfrastructure {
  readonly registryOrigin: URL;
  readonly routerOrigin: URL;
  readonly registrySignerPublicKeyJson: string;
  readonly temporaryDirectory: string;
  /**
   * Stops the Router and starts a fresh one on the same origin. The Router
   * keeps its state in memory, so the new process has a new
   * `RouterInstanceId` and no envelopes, as a redeployed Router does.
   */
  readonly restartRouter: Effect.Effect<void, ProcessTestError>;
  /** Pauses the current Router until the returned release function is called. */
  readonly holdRouter: Effect.Effect<() => void>;
  /** Recreates the empty in-memory Registry database and restarts both network services. */
  readonly resetNetwork: Effect.Effect<void, ProcessTestError>;
}

/**
 * Exact reusable process inputs for one daemon identity and state directory.
 * A fixture with a `firewall` mode runs `firewall-daemon.ts`, which installs
 * that firewall; one without runs `sharnessd`.
 */
export interface DaemonProcessFixture {
  readonly agentName: AgentName;
  readonly endpoint: URL;
  readonly environment: Readonly<Record<string, string>>;
  readonly stateDirectory: string;
  readonly agentPrivateKeyFile: string;
  readonly admissionCredentialFile: string;
  readonly mcpCredentialFile: string;
  readonly mcpCredential: Redacted.Redacted;
  readonly firewall?: FirewallMode;
  readonly wireVersions?: ProcessWireVersions;
}

/** Private management calls used to bootstrap and inspect a real daemon. */
export interface DaemonManagementClient {
  readonly register: (
    input: ManagementRegisterRequest,
  ) => Effect.Effect<ManagementRegisterResult, ProcessTestError>;
  readonly status: () => Effect.Effect<
    ManagementStatusResult,
    ProcessTestError
  >;
  readonly searchConversations: () => Effect.Effect<
    ManagementSearchConversationsResult,
    ProcessTestError
  >;
  readonly readConversation: (
    address: MessageAddressInput,
  ) => Effect.Effect<ManagementReadConversationResult, ProcessTestError>;
  readonly readEvent: (
    eventId: string,
  ) => Effect.Effect<InboundItem, ProcessTestError>;
  readonly callExpectingProtocolError: (
    name: string,
    toolArguments: Readonly<Record<string, unknown>>,
  ) => Effect.Effect<DaemonProtocolError, ProcessTestError>;
}

/** Stable JSON-RPC error fields observed by the real MCP client. */
interface DaemonProtocolError {
  readonly code: number;
  readonly data?: unknown;
}

/** The Registry and Router executables one infrastructure runs. */
export interface NetworkBinaries {
  readonly registry: string;
  readonly router: string;
}

/** Exit code and captured output of a daemon that stopped before listening. */
export interface DaemonStartupFailure {
  readonly exitCode: number | null;
  readonly logs: string;
}

/** Resolve the fixture's tsx loader independently of a child process's working directory. */
const require = createRequire(import.meta.url);

/** Disposable children listen only on loopback. */
export const LOOPBACK_HOST = "127.0.0.1";

/** Concurrent registration keeps this many daemon identities active. */
export const CONCURRENT_DAEMONS = 16;
/** Bounded boots limit process startup pressure while ports wait to bind. */
export const DAEMON_BOOT_CONCURRENCY = 4;
/** Router admission allows concurrent management and protocol requests per daemon. */
const REQUESTS_PER_DAEMON = 4;
/** Each daemon can hold its active poll and a replacement poll. */
const HELD_POLLS_PER_DAEMON = 2;
const ADMISSION_CREDENTIAL = "client-process-admission";
const READY_STATUS = 204;

const SHUTDOWN_TIMEOUT = Duration.seconds(5);

/** A live child must become ready within thirty seconds, with logs on failure. */
const READINESS_TIMEOUT = Duration.seconds(30);

const POLL_ATTEMPT_TIMEOUT = Duration.seconds(1);

const POLL_INTERVAL = Duration.millis(25);

const MODERN_PROTOCOL_VERSION = "2026-07-28";

const WORKSPACE_ROOT = resolve(
  dirname(fileURLToPath(import.meta.url)),
  "../../..",
);

/** Children use the current Node installation and system executables. */
const CHILD_PATH = `${dirname(process.execPath)}:/usr/bin:/bin`;

const PGLITE_BINARY = join(
  WORKSPACE_ROOT,
  "tests/integration/node_modules/.bin/pglite-server",
);

const REGISTRY_BINARY = join(
  WORKSPACE_ROOT,
  "packages/registry/bin/sharness-registry",
);

/** The Router executable runs the built package. */
export const ROUTER_BINARY = join(
  WORKSPACE_ROOT,
  "packages/router/bin/sharness-router",
);

const DAEMON_BINARY = join(
  WORKSPACE_ROOT,
  "packages/social-harness/bin/sharnessd",
);

const FIREWALL_DAEMON = join(
  WORKSPACE_ROOT,
  "tests/integration/firewall/firewall-daemon.ts",
);

/** Fixture failures preserve causal errors for diagnostics. */
export const processTestError = (
  message: string,
  cause?: unknown,
): ProcessTestError => new ProcessTestError({ message, cause });

/** Release an ephemeral listener before starting a child on its port. */
export const reservePort = Effect.async<number, ProcessTestError>((resume) => {
  const server = createServer();
  const onError = (error: Error): void => {
    resume(Effect.fail(processTestError("could not reserve a port", error)));
  };
  server.once("error", onError);
  server.listen(0, LOOPBACK_HOST, () => {
    server.removeListener("error", onError);
    const address = server.address();
    if (address === null || typeof address === "string") {
      server.close();
      resume(
        Effect.fail(processTestError("reserved listener exposed no port")),
      );
      return;
    }
    server.close((error) => {
      resume(
        error === undefined
          ? Effect.succeed(address.port)
          : Effect.fail(
              processTestError("could not release reserved port", error),
            ),
      );
    });
  });
  return Effect.sync(() => {
    if (server.listening) {
      server.close();
    }
  });
});

/** Capture both output streams while excluding the parent environment. */
const startProcess = (
  executable: string,
  processArguments: readonly string[],
  environment: Readonly<Record<string, string>> = {},
  cwd = WORKSPACE_ROOT,
): RunningProcess => {
  const child = spawn(executable, processArguments, {
    cwd,
    env: { PATH: CHILD_PATH, ...environment },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let output = "";
  let stderr = "";
  child.stdout?.setEncoding("utf8");
  child.stderr?.setEncoding("utf8");
  child.stdout?.on("data", (chunk: string) => {
    output += chunk;
  });
  child.stderr?.on("data", (chunk: string) => {
    stderr += chunk;
    output += chunk;
  });
  return { child, logs: () => output, stderr: () => stderr };
};

/** Whether `child` has exited on its own or by a signal. */
export const hasExited = (child: ChildProcess): boolean =>
  child.exitCode !== null || child.signalCode !== null;

/** Waits for the child and its output streams to close; cancellation removes the listener. */
export const waitForExit = (running: RunningProcess): Effect.Effect<void> => {
  if (
    hasExited(running.child) &&
    running.child.stdout?.closed &&
    running.child.stderr?.closed
  ) {
    return Effect.void;
  }
  return Effect.async<undefined>((resume) => {
    const onExit = (): void => {
      resume(Effect.succeed(undefined));
    };
    running.child.once("close", onExit);
    return Effect.sync(() => {
      running.child.removeListener("close", onExit);
    });
  });
};

/**
 * Stops one exact acquired child, escalating only after its shutdown grace.
 * A scope runs this release uninterruptibly, and a race does not finish until
 * its loser stops, so both sides are interruptible: otherwise every teardown
 * waits out the full grace for a child that has already exited.
 */
export const stopProcess = (running: RunningProcess): Effect.Effect<void> =>
  Effect.gen(function* () {
    if (hasExited(running.child)) {
      return;
    }
    running.child.kill("SIGTERM");
    const stopped = yield* Effect.raceFirst(
      waitForExit(running).pipe(Effect.as(true), Effect.interruptible),
      Effect.sleep(SHUTDOWN_TIMEOUT).pipe(
        Effect.as(false),
        Effect.interruptible,
      ),
    );
    if (!stopped) {
      running.child.kill("SIGKILL");
      yield* waitForExit(running);
    }
  }).pipe(Effect.withSpan("stopProcess"));

/**
 * Starts a child that the trace's enclosing scope stops. A scope closed by
 * interruption, as a timed-out test's is, first prints the child's output, so
 * one run shows where a hang stood.
 */
export const managedProcess = (
  executable: string,
  processArguments: readonly string[],
  environment: Readonly<Record<string, string>> = {},
  cwd = WORKSPACE_ROOT,
) =>
  Effect.acquireRelease(
    Effect.sync(() =>
      startProcess(executable, processArguments, environment, cwd),
    ),
    (running, exit) =>
      Exit.isInterrupted(exit)
        ? Console.error(
            `${basename(executable)} (pid ${running.child.pid}) output when interrupted:\n${running.logs()}`,
          ).pipe(Effect.zipRight(stopProcess(running)))
        : stopProcess(running),
  );

/**
 * Whether the child printed its own listening line. A child prints it only
 * after binding, so another process holding a released reservation cannot
 * satisfy readiness the way it could answer a connect or health probe.
 */
const announced = (running: RunningProcess, line: string) =>
  Effect.sync(() => running.logs().includes(line));

/** PGlite announces its listener only after its WASM database accepts connections. */
const pgliteListening = (running: RunningProcess, port: number) =>
  announced(running, `PGLiteSocketServer listening on {"port":${port},`);

/** The daemon announces its MCP endpoint once its loopback listener is bound. */
export const daemonListening = (running: RunningProcess, endpoint: URL) =>
  announced(running, `sharnessd listening on ${endpoint.href} (`);

/** Whether `origin` answers its health check as ready. */
const isHealthy = (origin: URL) =>
  HttpClient.get(new URL("/healthz", origin)).pipe(
    Effect.map((response) => response.status === READY_STATUS),
    Effect.timeout(POLL_ATTEMPT_TIMEOUT),
    Effect.orElseSucceed(() => false),
    Effect.scoped,
    Effect.provide(NodeHttpClient.layer),
  );

/** A Registry or Router is ready once it announced its own listener on `origin` and answers its health check as ready. */
export const serviceReady = (
  running: RunningProcess,
  service: "sharness-registry" | "sharness-router",
  origin: URL,
) =>
  announced(running, `${service} listening on ${origin.origin} (`).pipe(
    Effect.flatMap((listening) =>
      listening ? isHealthy(origin) : Effect.succeed(false),
    ),
  );

/**
 * Polls `probe` until it reads true, and fails with the child's output once
 * the child exits first or the readiness deadline expires.
 * @param running Child that must become ready.
 * @param probe One readiness check of the child.
 * @param phase What the child had yet to do, for the failure message.
 * @param timeout How long the child has to become ready.
 * @returns Completion once `probe` reads true.
 */
export const awaitReady = (
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
    return yield* Effect.fail(
      processTestError(`process exited before ${phase}\n${running.logs()}`),
    );
  }).pipe(
    Effect.timeoutFail({
      duration: timeout,
      onTimeout: () =>
        processTestError(
          `process timed out before ${phase}\n${running.logs()}`,
        ),
    }),
    Effect.withSpan("awaitReady"),
  );

/** Each fixture receives a fresh Ed25519 signing identity. */
export const makePrivateKeyPem = (): string => {
  const { privateKey } = generateKeyPairSync("ed25519");
  return privateKey.export({ format: "pem", type: "pkcs8" });
};

/** Import private key material only inside disposable process fixtures. */
export const makeSigningAuthority = (privateKeyPem?: string) =>
  AgentSigningAuthority.fromPkcs8(
    Redacted.make(privateKeyPem ?? makePrivateKeyPem()),
  );

const acquireTemporaryDirectory = FileSystem.FileSystem.pipe(
  Effect.flatMap((fs) =>
    Effect.acquireRelease(
      fs.makeTempDirectory({ prefix: "sharness-client-process-" }),
      (directory) =>
        fs
          .remove(directory, { recursive: true, force: true })
          .pipe(Effect.ignore),
    ),
  ),
  Effect.provide(NodeFileSystem.layer),
  Effect.mapError((cause) =>
    processTestError("could not create temporary directory", cause),
  ),
);

const writePrivateFile = (path: string, content: string) =>
  FileSystem.FileSystem.pipe(
    Effect.flatMap((fs) => fs.writeFileString(path, content, { mode: 0o600 })),
    Effect.provide(NodeFileSystem.layer),
    Effect.mapError((cause) =>
      processTestError(`could not write ${path}`, cause),
    ),
  );

const makeDirectory = (path: string) =>
  FileSystem.FileSystem.pipe(
    Effect.flatMap((fs) => fs.makeDirectory(path, { recursive: true })),
    Effect.provide(NodeFileSystem.layer),
    Effect.mapError((cause) =>
      processTestError(`could not create ${path}`, cause),
    ),
  );

const encodePublicKey = (
  publicKey: Ed25519PublicKey,
): Effect.Effect<string, ProcessTestError> =>
  Schema.encode(Ed25519PublicKey)(publicKey).pipe(
    Effect.map((encoded) => JSON.stringify(encoded)),
    Effect.mapError((cause) =>
      processTestError("could not encode Registry signer", cause),
    ),
  );

/** A PGlite Registry uses one SQL connection because its backend is one session. */
export const startPgliteRegistry = (input: {
  readonly postgresqlPort: number;
  readonly registryPort: number;
  readonly registryKeyPath: string;
  readonly admissionCredential: string;
  readonly command?: readonly [string, readonly string[]];
}) =>
  Effect.gen(function* () {
    const postgresql = yield* startPglite(input.postgresqlPort);
    const command: readonly [string, readonly string[]] = input.command ?? [
      REGISTRY_BINARY,
      [],
    ];
    const registry = yield* managedProcess(
      command[0],
      command[1],
      {
        SHARNESS_REGISTRY_HOST: LOOPBACK_HOST,
        SHARNESS_REGISTRY_PORT: String(input.registryPort),
        SHARNESS_REGISTRY_SQL_POOL_SIZE: "1",
        SHARNESS_REGISTRY_POSTGRESQL_URL: `postgresql://postgres:postgres@${LOOPBACK_HOST}:${input.postgresqlPort}/postgres`,
        SHARNESS_REGISTRY_ADMISSION_CREDENTIAL: input.admissionCredential,
        SHARNESS_REGISTRY_SIGNING_PRIVATE_KEY_PATH: input.registryKeyPath,
      },
      join(WORKSPACE_ROOT, "packages/registry"),
    );
    yield* awaitReady(
      registry,
      serviceReady(
        registry,
        "sharness-registry",
        new URL(`http://${LOOPBACK_HOST}:${input.registryPort}`),
      ),
      "health response",
    ).pipe(
      Effect.mapError((error) =>
        processTestError(
          `${error.message}\nPGlite output:\n${postgresql.logs()}`,
          error,
        ),
      ),
    );
    return registry;
  }).pipe(Effect.withSpan("startPgliteRegistry"));

const prepareInfrastructure = Effect.gen(function* () {
  const configuredVersions = yield* Effect.serviceOption(WireVersions);
  const versions = yield* Option.match(configuredVersions, {
    onNone: () => Effect.succeed(Option.none<ProcessWireVersions>()),
    onSome: (sets) =>
      sets.emitted === undefined
        ? Effect.fail(
            processTestError(
              "injected WireVersions is missing its emitted version",
            ),
          )
        : Effect.succeed(Option.some({ ...sets, emitted: sets.emitted })),
  });
  const temporaryDirectory = yield* acquireTemporaryDirectory;
  const registryKeyPath = join(temporaryDirectory, "registry-key.pem");
  const registryPrivateKey = makePrivateKeyPem();
  yield* writePrivateFile(registryKeyPath, registryPrivateKey);
  const registryAuthority = yield* makeSigningAuthority(
    registryPrivateKey,
  ).pipe(
    Effect.mapError((cause) =>
      processTestError("could not load Registry signer", cause),
    ),
  );
  const registrySignerPublicKeyJson = yield* encodePublicKey(
    AgentSigningAuthority.publicKey(registryAuthority),
  );
  const [postgresqlPort, registryPort, routerPort] = yield* Effect.all(
    [reservePort, reservePort, reservePort] as const,
    { concurrency: 3 },
  );
  const registryOrigin = new URL(`http://${LOOPBACK_HOST}:${registryPort}`);
  const routerOrigin = new URL(`http://${LOOPBACK_HOST}:${routerPort}`);

  return {
    versions,
    temporaryDirectory,
    registryKeyPath,
    registrySignerPublicKeyJson,
    postgresqlPort,
    registryPort,
    routerPort,
    registryOrigin,
    routerOrigin,
  };
});

/**
 * Starts disposable PGlite, Registry, and Router processes from `binaries`,
 * such as a published release's executables. The Registry waits for PGlite
 * to listen; the Router reads the Registry only to serve a request, so it
 * starts alongside them. An injected `WireVersions` runs the workspace
 * Registry and Router under those sets instead.
 */
export const acquireProcessInfrastructureFrom = (
  binaries: NetworkBinaries,
): Effect.Effect<ProcessInfrastructure, ProcessTestError, Scope.Scope> =>
  Effect.gen(function* () {
    const prepared = yield* prepareInfrastructure;
    const {
      registryOrigin,
      routerOrigin,
      registrySignerPublicKeyJson,
      temporaryDirectory,
    } = prepared;
    const scope = yield* Effect.scope;
    const startRegistry = startNetworkRegistry(prepared, binaries).pipe(
      Scope.extend(scope),
    );
    const startRouter = startNetworkRouter(prepared, binaries).pipe(
      Scope.extend(scope),
    );
    const [firstRegistry, firstRouter] = yield* Effect.all(
      [startRegistry, startRouter],
      {
        concurrency: 2,
      },
    );
    const registry = yield* Ref.make(firstRegistry);
    const router = yield* Ref.make(firstRouter);
    const restartRouter = Ref.get(router).pipe(
      Effect.flatMap(stopProcess),
      Effect.zipRight(startRouter),
      Effect.flatMap((restarted) => Ref.set(router, restarted)),
    );

    const resetNetwork = Effect.gen(function* () {
      yield* stopProcess(yield* Ref.get(router));
      yield* Scope.close(yield* Ref.get(registry), Exit.void);
      yield* Ref.set(registry, yield* startRegistry);
      yield* Ref.set(router, yield* startRouter);
    });

    return {
      registryOrigin,
      routerOrigin,
      registrySignerPublicKeyJson,
      temporaryDirectory,
      restartRouter,
      holdRouter: pauseRouter(router),
      resetNetwork,
    };
  }).pipe(Effect.withSpan("acquireProcessInfrastructureFrom"));

/** Starts disposable PGlite, Registry, and Router processes from the workspace. */
export const acquireProcessInfrastructure = acquireProcessInfrastructureFrom({
  registry: REGISTRY_BINARY,
  router: ROUTER_BINARY,
});

/** Creates one independent daemon key, credential file, and durable state path. */
export const makeDaemonProcessFixture = (
  infrastructure: ProcessInfrastructure,
  name: string,
  firewall?: FirewallMode,
): Effect.Effect<DaemonProcessFixture, ProcessTestError> =>
  Effect.gen(function* () {
    const agentName = yield* Schema.decodeUnknown(AgentName)(name).pipe(
      Effect.mapError((cause) =>
        processTestError("invalid test agent name", cause),
      ),
    );
    const port = yield* reservePort;
    const directory = join(infrastructure.temporaryDirectory, name);
    const stateDirectory = join(directory, "state");
    const keyPath = join(directory, "agent-key.pem");
    const credentialPath = join(directory, "admission.txt");
    yield* makeDirectory(stateDirectory);
    const credentials = yield* makeMcpCredential(directory);
    yield* Effect.all(
      [
        writePrivateFile(keyPath, makePrivateKeyPem()),
        writePrivateFile(credentialPath, ADMISSION_CREDENTIAL),
      ] as const,
      { concurrency: 2, discard: true },
    );
    return {
      agentName,
      endpoint: new URL(`http://${LOOPBACK_HOST}:${port}/mcp`),
      stateDirectory,
      agentPrivateKeyFile: keyPath,
      admissionCredentialFile: credentialPath,
      ...credentials,
      environment: {
        SHARNESS_STATE_DIRECTORY: stateDirectory,
        SHARNESS_MCP_PORT: String(port),
        SHARNESS_MCP_CREDENTIAL_FILE: credentials.mcpCredentialFile,
        SHARNESS_REGISTRY_ORIGIN: infrastructure.registryOrigin.origin,
        SHARNESS_REGISTRY_SIGNER_PUBLIC_KEY:
          infrastructure.registrySignerPublicKeyJson,
        SHARNESS_ROUTER_ORIGIN: infrastructure.routerOrigin.origin,
        SHARNESS_AGENT_PRIVATE_KEY_FILE: keyPath,
        SHARNESS_ADMISSION_CREDENTIAL_FILE: credentialPath,
        ...(firewall === undefined
          ? {}
          : { SHARNESS_TEST_FIREWALL_MODE: firewall }),
      },
      ...(firewall === undefined ? {} : { firewall }),
    };
  }).pipe(Effect.withSpan("makeDaemonProcessFixture"));

function makeMcpCredential(directory: string) {
  return Effect.gen(function* () {
    const mcpCredentialFile = join(directory, "mcp-credential");
    const mcpCredential = Redacted.make(randomBytes(32).toString("base64url"));
    yield* writePrivateFile(mcpCredentialFile, Redacted.value(mcpCredential));
    return { mcpCredentialFile, mcpCredential };
  });
}

/** The command that runs one fixture's daemon. */
const daemonCommand = (
  fixture: DaemonProcessFixture,
): readonly [string, readonly string[]] =>
  fixture.firewall === undefined
    ? [DAEMON_BINARY, []]
    : [process.execPath, ["--import", require.resolve("tsx"), FIREWALL_DAEMON]];

/** Starts one sharnessd and returns only after its guarded loopback port listens. */
export const acquireDaemonProcess = (
  fixture: DaemonProcessFixture,
  executable?: string,
): Effect.Effect<RunningProcess, ProcessTestError, Scope.Scope> =>
  Effect.gen(function* () {
    const baseCommand =
      executable === undefined
        ? daemonCommand(fixture)
        : ([executable, []] as const);
    const [binary, args] =
      fixture.wireVersions !== undefined
        ? launchCommand("Daemon", fixture.wireVersions)
        : baseCommand;
    const running = yield* managedProcess(
      binary,
      args,
      fixture.environment,
      join(WORKSPACE_ROOT, "packages/social-harness"),
    );
    yield* awaitReady(
      running,
      daemonListening(running, fixture.endpoint),
      "listening",
    );
    return running;
  }).pipe(Effect.withSpan("acquireDaemonProcess"));

/** Reserve each port and boot its daemon in one bounded step; release children in parallel. */
export const acquireDaemonProcesses = (infrastructure: ProcessInfrastructure) =>
  Effect.forEach(
    [...Array(CONCURRENT_DAEMONS).keys()],
    (index) =>
      makeDaemonProcessFixture(
        infrastructure,
        `concurrent-agent-${index}`,
      ).pipe(Effect.tap(acquireDaemonProcess)),
    { concurrency: DAEMON_BOOT_CONCURRENCY },
  ).pipe(Effect.parallelFinalizers);

/**
 * Starts one sharnessd that must stop before it listens. A daemon that listens
 * instead fails the fixture.
 */
export const awaitDaemonStartupFailure = (
  fixture: DaemonProcessFixture,
  command?: {
    readonly executable: string;
    readonly args: readonly string[];
    readonly cwd?: string;
  },
): Effect.Effect<DaemonStartupFailure, ProcessTestError, Scope.Scope> =>
  Effect.gen(function* () {
    const [executable, processArguments] =
      command === undefined
        ? daemonCommand(fixture)
        : ([command.executable, command.args] as const);
    const running = yield* managedProcess(
      executable,
      processArguments,
      fixture.environment,
      command?.cwd,
    );
    const listened = yield* awaitReady(
      running,
      daemonListening(running, fixture.endpoint),
      "listening",
    ).pipe(
      Effect.as(true),
      Effect.catchAll((error) =>
        hasExited(running.child) ? Effect.succeed(false) : Effect.fail(error),
      ),
    );
    if (listened) {
      return yield* Effect.fail(
        processTestError(`daemon listened unexpectedly\n${running.logs()}`),
      );
    }
    return { exitCode: running.child.exitCode, logs: running.logs() };
  }).pipe(Effect.withSpan("awaitDaemonStartupFailure"));

const makeIdentifier = (prefix: "opn_" | "prn_"): string =>
  `${prefix}${randomBytes(16).toString("base64url")}`;

/** Creates exact one-shot bootstrap fields for a fixture's configured identity. */
export const makeRegistrationRequest = (
  fixture: DaemonProcessFixture,
): ManagementRegisterRequest => ({
  operationId: Schema.decodeUnknownSync(OperationId)(makeIdentifier("opn_")),
  principalId: Schema.decodeUnknownSync(PrincipalId)(makeIdentifier("prn_")),
  agentName: fixture.agentName,
});

const callAndDecode = <A, I>(input: {
  readonly client: Client;
  readonly name: string;
  readonly arguments: Readonly<Record<string, unknown>>;
  readonly schema: Schema.Schema<A, I>;
}): Effect.Effect<A, ProcessTestError> =>
  Effect.tryPromise({
    try: (signal) =>
      input.client.callTool(
        { name: input.name, arguments: input.arguments },
        { signal },
      ),
    catch: (cause) => processTestError(`${input.name} call failed`, cause),
  }).pipe(
    Effect.filterOrFail(
      (result) => result.isError !== true,
      () => processTestError(`${input.name} returned an MCP error`),
    ),
    Effect.flatMap((result) =>
      Schema.decodeUnknown(input.schema)(result.structuredContent).pipe(
        Effect.mapError((cause) =>
          processTestError(`${input.name} returned invalid content`, cause),
        ),
      ),
    ),
  );

const closeManagementClient = (client: Client): Effect.Effect<void> =>
  Effect.tryPromise({
    try: () => client.close(),
    catch: (cause) =>
      processTestError("could not close management client", cause),
  }).pipe(Effect.ignore);

const callExpectingProtocolError = (
  client: Client,
  name: string,
  toolArguments: Readonly<Record<string, unknown>>,
): Effect.Effect<DaemonProtocolError, ProcessTestError> =>
  Effect.tryPromise({
    try: (signal) =>
      client.callTool({ name, arguments: toolArguments }, { signal }),
    catch: (cause) => processTestError(`${name} call failed`, cause),
  }).pipe(
    Effect.matchEffect({
      onFailure: (error) =>
        ProtocolError.isInstance(error.cause)
          ? Effect.succeed({ code: error.cause.code, data: error.cause.data })
          : Effect.fail(error),
      onSuccess: () =>
        Effect.fail(processTestError(`${name} unexpectedly succeeded`)),
    }),
  );

/** Acquires a authenticated daemon MCP session against one real daemon. */
export const acquireDaemonManagementClient = (
  fixture: Pick<DaemonProcessFixture, "endpoint" | "mcpCredential">,
): Effect.Effect<DaemonManagementClient, ProcessTestError, Scope.Scope> =>
  Effect.gen(function* () {
    const client = new Client(
      { name: "sharness-process-acceptance", version: "1.0.0" },
      {
        capabilities: {},
        versionNegotiation: { mode: { pin: MODERN_PROTOCOL_VERSION } },
      },
    );
    yield* Effect.acquireRelease(
      Effect.tryPromise({
        try: (signal) =>
          client.connect(
            new StreamableHTTPClientTransport(fixture.endpoint, {
              requestInit: {
                headers: {
                  Authorization: `Bearer ${Redacted.value(fixture.mcpCredential)}`,
                },
                redirect: "error",
              },
            }),
            {
              signal,
            },
          ),
        catch: (cause) =>
          processTestError("could not connect management client", cause),
      }),
      () => closeManagementClient(client),
    );
    return managementOperations(client);
  }).pipe(Effect.withSpan("acquireDaemonManagementClient"));

function startNetworkRegistry(
  input: Effect.Effect.Success<typeof prepareInfrastructure>,
  binaries: NetworkBinaries,
) {
  return Effect.gen(function* () {
    const { versions, postgresqlPort, registryPort, registryKeyPath } = input;

    const command: readonly [string, readonly string[]] = Option.isNone(
      versions,
    )
      ? [binaries.registry, []]
      : launchCommand("Registry", versions.value);
    const registryScope = yield* Effect.acquireRelease(
      Scope.make(),
      (acquired) => Scope.close(acquired, Exit.void),
    );
    yield* startPgliteRegistry({
      postgresqlPort,
      registryPort,
      registryKeyPath,
      admissionCredential: ADMISSION_CREDENTIAL,
      command,
    }).pipe(Scope.extend(registryScope));
    return registryScope;
  });
}

function startNetworkRouter(
  input: Effect.Effect.Success<typeof prepareInfrastructure>,
  binaries: NetworkBinaries,
) {
  return Effect.gen(function* () {
    const {
      versions,
      routerPort,
      registryOrigin,
      registrySignerPublicKeyJson,
      routerOrigin,
    } = input;
    const routerCommand: readonly [string, readonly string[]] = Option.isNone(
      versions,
    )
      ? [binaries.router, []]
      : launchCommand("RouterServer", versions.value);
    return yield* managedProcess(
      routerCommand[0],
      routerCommand[1],
      {
        SHARNESS_ROUTER_HOST: LOOPBACK_HOST,
        SHARNESS_ROUTER_PORT: String(routerPort),
        SHARNESS_ROUTER_REGISTRY_ORIGIN: registryOrigin.origin,
        SHARNESS_ROUTER_REGISTRY_SIGNER_PUBLIC_KEY: registrySignerPublicKeyJson,
        SHARNESS_ROUTER_REQUEST_CONCURRENCY_LIMIT: String(
          CONCURRENT_DAEMONS * REQUESTS_PER_DAEMON,
        ),
        SHARNESS_ROUTER_HELD_POLL_CAPACITY: String(
          CONCURRENT_DAEMONS * HELD_POLLS_PER_DAEMON,
        ),
      },
      join(WORKSPACE_ROOT, "packages/router"),
    ).pipe(
      Effect.tap((router) =>
        awaitReady(
          router,
          serviceReady(router, "sharness-router", routerOrigin),
          "health response",
        ),
      ),
    );
  });
}

function managementOperations(client: Client): DaemonManagementClient {
  const management: DaemonManagementClient = {
    readEvent: (eventId) =>
      callAndDecode({
        client,
        name: "read_event",
        arguments: { eventId },
        schema: Schema.Struct({ item: InboundItem }),
      }).pipe(Effect.map(({ item }) => item)),
    register: (request) =>
      callAndDecode({
        client,
        name: "register",
        arguments: { ...request },
        schema: managementRegisterResultSchema,
      }),
    status: () =>
      callAndDecode({
        client,
        name: "status",
        arguments: {},
        schema: managementStatusResultSchema,
      }),
    searchConversations: () =>
      callAndDecode({
        client,
        name: "search_conversations",
        arguments: {},
        schema: managementSearchConversationsResultSchema,
      }),
    readConversation: (address) =>
      callAndDecode({
        client,
        name: "read_conversation",
        arguments: { address },
        schema: managementReadConversationResultSchema,
      }),
    callExpectingProtocolError: (name, toolArguments) =>
      callExpectingProtocolError(client, name, toolArguments),
  };

  return management;
}

/** Child compositions select wire versions before their service imports resolve. */
function launchCommand(
  service: "Registry" | "RouterServer" | "Daemon",
  versions: ProcessWireVersions,
): readonly [string, readonly string[]] {
  let script: string;
  if (service === "Registry") {
    script = `import { startRegistry } from "@moltzap/registry/test"; startRegistry(${JSON.stringify(versions satisfies Parameters<typeof startRegistry>[0])});`;
  } else if (service === "RouterServer") {
    script = `import { startRouter } from "@moltzap/router/test"; startRouter(${JSON.stringify(versions satisfies Parameters<typeof startRouter>[0])});`;
  } else {
    script = serviceScript(service, versions);
  }
  return [
    process.execPath,
    [
      "--conditions=sharness-test",
      ...(service === "Daemon" ? [] : ["--conditions=sharness-internal"]),
      "--input-type=module",
      "-e",
      script,
    ],
  ];
}

/** Daemon compositions select their test wire policy before process startup. */
function serviceScript(
  service: "Daemon",
  versions: ProcessWireVersions,
): string {
  return `import { NodeRuntime } from "@effect/platform-node";
import { WireVersions } from "@moltzap/social-harness/test";
import { ${service} } from "@moltzap/social-harness/service";
import { ConfigProvider, Effect, Layer } from "effect";
Layer.launch(${service}.layer).pipe(Effect.provideService(WireVersions, ${JSON.stringify(versions)}), Effect.withConfigProvider(ConfigProvider.fromEnv()), NodeRuntime.runMain);`;
}

function startPglite(port: number) {
  return managedProcess(PGLITE_BINARY, [
    "--db=memory://",
    `--port=${port}`,
    `--host=${LOOPBACK_HOST}`,
    "--max-connections=20",
  ]).pipe(
    Effect.tap((running) =>
      awaitReady(running, pgliteListening(running, port), "listening"),
    ),
  );
}

function pauseRouter(router: Ref.Ref<RunningProcess>) {
  return Ref.get(router).pipe(
    Effect.map((running) => {
      running.child.kill("SIGSTOP");
      return () => {
        running.child.kill("SIGCONT");
      };
    }),
  );
}
