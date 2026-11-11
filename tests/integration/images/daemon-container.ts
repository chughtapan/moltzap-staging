/** @file Runs a prepared daemon in the daemon image under test. */
import { Duration, Effect } from "effect";
import { execFile } from "node:child_process";
import { randomBytes } from "node:crypto";
import { chmod } from "node:fs/promises";
import { join } from "node:path";
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
 * `sharness-daemon-` and its port, or as a local process when that variable
 * is unset. The container uses the host's network, because the daemon answers
 * only requests addressed to its loopback endpoint and checks its owners'
 * tokens against an issuer on the host's loopback. Its database is a copy of
 * the prepared one in a volume the image's user owns.
 */
export const startDaemonContainer = (daemon: Daemon) => {
  const image = process.env.DAEMON_INTEGRATION_IMAGE;
  if (image === undefined) {
    return daemon.start();
  }
  const container = `sharness-daemon-${daemon.endpoint.port}`;
  const volume = `sharness-daemon-state-${randomBytes(16).toString("hex")}`;
  return Effect.gen(function* () {
    yield* prepareDaemonMount(daemon);
    // eslint-disable-next-line agent-code-guard/acquire-release-requires-scope -- startNetwork owns the enclosing scope and releases the volume after the container.
    yield* Effect.acquireRelease(
      executeDocker(["volume", "create", volume]),
      () => executeDocker(["volume", "rm", volume]).pipe(Effect.orDie),
    );
    yield* executeDocker([
      "run",
      "--rm",
      "--user",
      "0",
      "--entrypoint",
      "sh",
      "--volume",
      `${daemon.directory}:/source:ro`,
      "--mount",
      `type=volume,src=${volume},dst=/state`,
      image,
      "-c",
      "cp /source/sharnessd.sqlite3 /state/sharnessd.sqlite3 && chown 1000:1000 /state /state/sharnessd.sqlite3",
    ]);
    // eslint-disable-next-line agent-code-guard/acquire-release-requires-scope -- startNetwork owns the enclosing scope and removes the container after its attached process stops.
    yield* Effect.acquireRelease(
      executeDocker(containerArguments(daemon, image, container, volume)),
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

/** Lets the image's user read the daemon's directory and database. */
function prepareDaemonMount(daemon: Daemon) {
  return Effect.tryPromise({
    try: async () => {
      await chmod(daemon.directory, 0o755);
      await chmod(join(daemon.directory, "sharnessd.sqlite3"), 0o644);
    },
    catch: (cause) =>
      new ProcessTestError({
        message: "could not prepare daemon mount",
        cause,
      }),
  });
}

function containerArguments(
  daemon: Daemon,
  image: string,
  container: string,
  volume: string,
) {
  return [
    "create",
    "--network=host",
    "--name",
    container,
    "--volume",
    `${daemon.directory}:${daemon.directory}:ro`,
    "--mount",
    `type=volume,src=${volume},dst=/home/node`,
    ...Object.entries(daemon.environment).flatMap(([name, value]) => [
      "--env",
      `${name}=${value}`,
    ]),
    "--env",
    "SHARNESS_DATABASE_URL=sqlite:///home/node/sharnessd.sqlite3",
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
