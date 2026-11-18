/** @file Runs a prepared daemon in the daemon image under test. */
import { Duration, Effect } from "effect";
import { execFile } from "node:child_process";
import { chmod } from "node:fs/promises";
import { promisify } from "node:util";
import {
  awaitReady,
  type Daemon,
  daemonListening,
  managedProcess,
  ProcessTestError,
} from "../daemon/daemon-fixture.ts";

const docker = promisify(execFile);

/** A runner other builds share gives the container sixty seconds to listen. */
const CONTAINER_READINESS_TIMEOUT = Duration.seconds(60);

/**
 * Runs the prepared daemon in a `DAEMON_INTEGRATION_IMAGE` container named
 * `sharness-daemon-` and its port; the suites that call it skip unless that
 * variable names a built image. The container uses the host's network,
 * because the daemon answers only requests addressed to its loopback endpoint
 * and reaches its database and the issuer of its owners' tokens on the host's
 * loopback.
 */
export const startDaemonContainer = (daemon: Daemon) => {
  const image = process.env.DAEMON_INTEGRATION_IMAGE ?? "";
  const container = `sharness-daemon-${daemon.endpoint.port}`;
  return Effect.gen(function* () {
    yield* prepareDaemonMount(daemon);
    // eslint-disable-next-line agent-code-guard/acquire-release-requires-scope -- openDaemon owns the enclosing scope and removes the container after its attached process stops.
    yield* Effect.acquireRelease(
      executeDocker(containerArguments(daemon, image, container)),
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
    return running;
  }).pipe(Effect.withSpan("startDaemonContainer"));
};

/** Lets the image's user read the daemon's directory. */
function prepareDaemonMount(daemon: Daemon) {
  return Effect.tryPromise({
    try: () => chmod(daemon.directory, 0o755),
    catch: (cause) =>
      new ProcessTestError({
        message: "could not prepare daemon mount",
        cause,
      }),
  });
}

function containerArguments(daemon: Daemon, image: string, container: string) {
  return [
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
  ];
}

function executeDocker(args: readonly string[]) {
  return Effect.tryPromise({
    try: () => docker("docker", args),
    catch: (cause) =>
      new ProcessTestError({ message: `docker ${args[0]} failed`, cause }),
  });
}
