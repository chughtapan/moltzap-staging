/** @file One daemon in the daemon image under test, and promise access to its agents for node:test suites. */
import {
  MAXIMUM_WAIT_SECONDS,
  type Message,
  type SendResult,
} from "@social-harness/social-harness";
import { Duration, Effect, Exit, Scope } from "effect";
import { execFile } from "node:child_process";
import { chmod } from "node:fs/promises";
import { promisify } from "node:util";
import {
  awaitReady,
  callTool,
  type Daemon,
  daemonListening,
  inboxCursor,
  managedProcess,
  ProcessTestError,
  send,
  SERVER_AGENT_OWNER,
  sharedDaemon,
  type SignedIn,
} from "../daemon-fixture.ts";

const docker = promisify(execFile);

/** A runner other builds share gives the container sixty seconds to listen. */
const CONTAINER_READINESS_TIMEOUT = Duration.seconds(60);

/**
 * One owner's agent: what a host container needs to connect as it, and the
 * test's own connection. `next` reads the entries that became readable after
 * the sign-in, each once; a host connected as the same agent reads them too.
 */
export interface AgentAccess {
  readonly agent: SignedIn["agent"];
  readonly address: SignedIn["address"];
  readonly mcp: URL;
  /** A file any container user can read, holding the bearer the agent signed in with. */
  readonly mcpCredentialFile: string;
  readonly send: (input: unknown) => Promise<SendResult>;
  readonly next: (count: number) => Promise<readonly Message[]>;
}

/** One scope owns the daemon container and every agent's connection. */
export interface DaemonAccess {
  /** The container's name, whose network a host container joins. */
  readonly container: string;
  /** Signs in the server agent's owner, or the named owner of a client. */
  readonly signIn: (owner?: string) => Promise<AgentAccess>;
  readonly close: () => Promise<undefined>;
}

/**
 * Prepares one daemon and runs it in a `DAEMON_INTEGRATION_IMAGE` container;
 * the suites that call it skip unless that variable names a built image. The
 * container uses the host's network, because the daemon answers only requests
 * addressed to its loopback endpoint and reaches its database and the issuer
 * of its owners' tokens on the host's loopback. A failed start closes what it
 * had started.
 */
export async function openDaemon(): Promise<DaemonAccess> {
  const scope = Effect.runSync(Scope.make());
  const inScope = <A, E>(effect: Effect.Effect<A, E, Scope.Scope>) =>
    Effect.runPromise(Scope.extend(effect, scope));
  const close = () =>
    Effect.runPromise(Scope.close(scope, Exit.void).pipe(Effect.as(undefined)));
  try {
    const daemon = await inScope(
      sharedDaemon().pipe(Effect.tap(startDaemonContainer)),
    );
    return {
      container: containerName(daemon),
      signIn: async (owner = SERVER_AGENT_OWNER) => {
        const signedIn = await inScope(daemon.signInAs(owner));
        await chmod(signedIn.credentialFile, 0o644);
        const position = {
          cursor: await Effect.runPromise(inboxCursor(signedIn.connection)),
        };
        return {
          agent: signedIn.agent,
          address: signedIn.address,
          mcp: daemon.endpoint,
          mcpCredentialFile: signedIn.credentialFile,
          send: (input) => Effect.runPromise(send(signedIn, input)),
          next: (count) =>
            Effect.runPromise(nextItems(signedIn, position, count)),
        };
      },
      close,
    };
  } catch (cause) {
    await close();
    throw cause;
  }
}

/** The next `count` inbox entries after `position`, which moves past every entry read. */
function nextItems(
  signedIn: SignedIn,
  position: { cursor: string },
  count: number,
) {
  return Effect.gen(function* () {
    const messages: Message[] = [];
    while (messages.length < count) {
      const page = yield* callTool(signedIn.connection.mcp, {
        name: "receive_messages",
        arguments: { cursor: position.cursor, wait: MAXIMUM_WAIT_SECONDS },
      });
      messages.push(...page.entries.map(({ message }) => message));
      // eslint-disable-next-line require-atomic-updates -- One `next` call reads at a time.
      position.cursor = page.nextCursor;
    }
    return messages.slice(0, count);
  }).pipe(
    Effect.timeoutFail({
      duration: "60 seconds",
      onTimeout: () =>
        new ProcessTestError({
          message: `timed out awaiting ${String(count)} deliveries`,
        }),
    }),
  );
}

/** Runs the prepared daemon in a container that mounts its directory, which the image's user must be able to read. */
function startDaemonContainer(daemon: Daemon) {
  const image = process.env.DAEMON_INTEGRATION_IMAGE ?? "";
  const container = containerName(daemon);
  return Effect.gen(function* () {
    yield* Effect.tryPromise({
      try: () => chmod(daemon.directory, 0o755),
      catch: (cause) =>
        new ProcessTestError({
          message: "could not prepare daemon mount",
          cause,
        }),
    });
    // eslint-disable-next-line agent-code-guard/acquire-release-requires-scope -- openDaemon owns the enclosing scope and removes the container after its attached process stops.
    yield* Effect.acquireRelease(
      executeDocker([
        "create",
        "--network=host",
        "--name",
        container,
        "--volume",
        `${daemon.directory}:${daemon.directory}:ro`,
        ...Object.entries(daemon.environment).flatMap(([name, value]) => [
          "--env",
          `${name}=${value}`,
        ]),
        image,
      ]),
      () =>
        executeDocker(["rm", "--force", "--volumes", container]).pipe(
          Effect.orDie,
        ),
    );
    const running = yield* managedProcess("docker", [
      "start",
      "--attach",
      container,
    ]);
    yield* Effect.addFinalizer(() =>
      executeDocker(["stop", "--timeout", "5", container]).pipe(Effect.orDie),
    );
    yield* awaitReady(
      running,
      daemonListening(running, daemon.endpoint),
      "listening",
      CONTAINER_READINESS_TIMEOUT,
    );
  }).pipe(Effect.withSpan("startDaemonContainer"));
}

function containerName(daemon: Daemon) {
  return `sharness-daemon-${daemon.endpoint.port}`;
}

function executeDocker(args: readonly string[]) {
  return Effect.tryPromise({
    try: () => docker("docker", args),
    catch: (cause) =>
      new ProcessTestError({ message: `docker ${args[0]} failed`, cause }),
  });
}
