/** @file Runs a shared daemon fixture in the image under test. */
import { Duration, Effect } from "effect";
import { execFile } from "node:child_process";
import { randomBytes } from "node:crypto";
import { chmod } from "node:fs/promises";
import { dirname } from "node:path";
import { promisify } from "node:util";
import {
  acquireDaemonProcess,
  awaitReady,
  daemonListening,
  type DaemonProcessFixture,
  managedProcess,
  processTestError,
} from "../daemon/daemon-fixture.ts";

const docker = promisify(execFile);

/**
 * A cohort starts all its daemon containers at once on a runner other builds
 * share, so each container gets sixty seconds to listen.
 */
const CONTAINER_READINESS_TIMEOUT = Duration.seconds(60);

/**
 * Runs the fixture's daemon in the `DAEMON_INTEGRATION_IMAGE` container, or as
 * a local process when that variable is unset. Docker initializes the home
 * volume with the image user's ownership.
 */
export const startDaemonContainer = (fixture: DaemonProcessFixture) => {
  const image = process.env.DAEMON_INTEGRATION_IMAGE;
  if (image === undefined) {
    return acquireDaemonProcess(fixture);
  }
  const container = `sharness-daemon-${fixture.endpoint.port}`;
  const volume = `sharness-daemon-state-${randomBytes(16).toString("hex")}`;
  return Effect.gen(function* () {
    yield* prepareDaemonMount(fixture);
    // eslint-disable-next-line agent-code-guard/acquire-release-requires-scope -- startNetwork owns the enclosing scope and releases the volume after the container.
    yield* Effect.acquireRelease(
      executeDocker(["volume", "create", volume]),
      () => executeDocker(["volume", "rm", volume]).pipe(Effect.orDie),
    );
    // eslint-disable-next-line agent-code-guard/acquire-release-requires-scope -- startNetwork owns the enclosing scope and removes the container after its attached process stops.
    yield* Effect.acquireRelease(
      executeDocker(containerArguments(fixture, image, container, volume)),
      () =>
        executeDocker(["rm", "--force", "--volumes", container]).pipe(
          Effect.orDie,
        ),
    );
    const daemon = yield* managedProcess("docker", [
      "start",
      "--attach",
      container,
    ]);
    yield* Effect.addFinalizer(() =>
      executeDocker(["stop", "--timeout", "5", container]).pipe(Effect.orDie),
    );
    yield* awaitReady(
      daemon,
      daemonListening(daemon, fixture.endpoint),
      "listening",
      CONTAINER_READINESS_TIMEOUT,
    );
    return daemon;
  }).pipe(Effect.withSpan("startDaemonContainer"));
};

function prepareDaemonMount(fixture: DaemonProcessFixture) {
  const directory = dirname(fixture.stateDirectory);
  return Effect.tryPromise({
    try: async () => {
      await chmod(dirname(directory), 0o755);
      await chmod(directory, 0o755);
      await chmod(fixture.agentPrivateKeyFile, 0o644);
      await chmod(fixture.admissionCredentialFile, 0o644);
      await chmod(fixture.mcpCredentialFile, 0o644);
    },
    catch: (cause) => processTestError("could not prepare daemon mount", cause),
  });
}

function containerArguments(
  fixture: DaemonProcessFixture,
  image: string,
  container: string,
  volume: string,
) {
  const directory = dirname(fixture.stateDirectory);
  return [
    "create",
    "--network=host",
    "--name",
    container,
    "--volume",
    `${directory}:${directory}:ro`,
    "--mount",
    `type=volume,src=${volume},dst=/home/node`,
    "--env",
    "SHARNESS_LAUNCHER_UPGRADE=off",
    ...Object.entries(fixture.environment).flatMap(([name, value]) => [
      "--env",
      `${name}=${value}`,
    ]),
    "--env",
    "SHARNESS_STATE_DIRECTORY=/home/node/sharness-state",
    image,
  ];
}

function executeDocker(args: readonly string[]) {
  return Effect.tryPromise({
    try: () => docker("docker", args),
    catch: (cause) => processTestError(`docker ${args[0]} failed`, cause),
  });
}
