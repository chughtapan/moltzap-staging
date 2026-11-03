/**
 * @file Published peers and the current daemon certify messages in both
 * directions, through the current network and through the previous release's
 * network.
 */

import { FileSystem } from "@effect/platform";
import { NodeContext } from "@effect/platform-node";
import {
  AgentAddress,
  LIVE_WIRE_VERSIONS,
  WIRE_VERSION,
} from "@moltzap/social-harness";
import { HISTORY_WIRE_VERSIONS } from "@moltzap/social-harness/test";
import { Data, Effect, Fiber, Option, Schedule, Schema, Stream } from "effect";
import { join, posix, resolve } from "node:path";
import { expect, it, type TestContext } from "vitest";
import {
  acquireMcpConnection,
  callTool,
} from "../../../plugins/shared/mcp-client.js";
import {
  installPackedConsumer,
  PackGateError,
  packWorkspaceClosure,
  runCommand,
} from "../../../scripts/test/packed-workspace.js";
import {
  acquireDaemonManagementClient,
  acquireDaemonProcess,
  acquireProcessInfrastructure,
  acquireProcessInfrastructureFrom,
  type DaemonProcessFixture,
  makeDaemonProcessFixture,
  makeRegistrationRequest,
  ProcessTestError,
} from "../daemon/daemon-fixture.js";
import { inboxStream } from "../mcp-inbox.js";

// @agent-code-guard/regression-only: Regression cases exercise the reviewed release selection, reset boundary, missing metadata, and registry outage failures; the real process exchange checks interoperability.

const RESET_SKIP_REASON =
  "No post-reset release is published; the published-peers gate starts with the first release after the reset.";
const workspaceRoot = resolve(import.meta.dirname, "../../..");
const packages = {
  "@moltzap/social-harness": join(workspaceRoot, "packages/social-harness"),
};

/**
 * The first package release after the last protocol reset. A reset change
 * moves it above the release it drops, so the gate skips that release rather
 * than failing on it.
 */
const FIRST_POST_RESET_RELEASE = Option.some("2026.1029.2");
const PRE_RESET_WIRE_VERSION = "pre-reset";

function preResetRelease(
  version: string,
  firstPostResetRelease?: string,
): boolean {
  if (firstPostResetRelease === undefined) {
    return true;
  }
  const parts = version.split(".").map(Number);
  const boundary = firstPostResetRelease.split(".").map(Number);
  for (const [index, part] of parts.entries()) {
    const minimum = boundary[index];
    if (minimum !== undefined && part !== minimum) {
      return part < minimum;
    }
  }
  return false;
}

/** Only registry availability failures can waive this network-dependent gate. */
function npmOutage(error: unknown): boolean {
  return (
    error instanceof Error &&
    /EAI_AGAIN|ENOTFOUND|ECONNRESET|ECONNREFUSED|ETIMEDOUT|E(?:408|429|500|502|503|504)|ERR_PNPM_FETCH_(?:408|429|500|502|503|504)|FETCH_ERROR|fetch failed/iu.test(
      error.message,
    )
  );
}

/** Wire metadata is read without downloading or installing release closures. */
function releaseWireVersion(
  version: string,
  view = npmView,
  firstPostResetRelease = FIRST_POST_RESET_RELEASE,
) {
  return view([`@moltzap/social-harness@${version}`]).pipe(
    Effect.flatMap(
      Schema.decodeUnknown(
        Schema.parseJson(
          Schema.Struct({
            sharnessWireVersion: Schema.optional(Schema.String),
          }),
        ),
      ),
    ),
    Effect.flatMap(({ sharnessWireVersion }) => {
      if (sharnessWireVersion !== undefined) {
        return Effect.succeed(sharnessWireVersion);
      }
      return preResetRelease(
        version,
        Option.getOrUndefined(firstPostResetRelease),
      )
        ? Effect.succeed(PRE_RESET_WIRE_VERSION)
        : Effect.fail(
            new ProcessTestError({
              message: `@moltzap/social-harness@${version} is missing published sharnessWireVersion metadata`,
            }),
          );
    }),
  );
}

/** Peer installation includes exact library-owned package pins from its published manifest. */
function releaseClosure(version: string, view = npmView) {
  return view([`@moltzap/social-harness@${version}`, "dependencies"]).pipe(
    Effect.flatMap(
      Schema.decodeUnknown(
        Schema.parseJson(
          Schema.Record({ key: Schema.String, value: Schema.String }),
        ),
      ),
    ),
    Effect.map(
      (pins): Readonly<Record<string, string>> => ({
        ...Object.fromEntries(
          Object.entries(pins).filter(([name]) => name.startsWith("@moltzap/")),
        ),
        "@moltzap/social-harness": version,
      }),
    ),
  );
}

function exchange(from: DaemonProcessFixture, to: DaemonProcessFixture) {
  return Effect.scoped(
    Effect.gen(function* () {
      const sender = yield* acquireMcpConnection(
        { endpoint: from.endpoint, credential: from.mcpCredential },
        { name: "process-test", version: "test" },
      );
      const receiver = yield* acquireMcpConnection(
        { endpoint: to.endpoint, credential: to.mcpCredential },
        { name: "process-test", version: "test" },
      );
      const waiting = yield* Stream.runHead(inboxStream(receiver)).pipe(
        Effect.forkScoped,
      );
      const text = `certified from ${from.agentName} to ${to.agentName}`;
      yield* callTool(sender.client, {
        name: "send_message",
        arguments: {
          input: {
            to: Schema.decodeUnknownSync(AgentAddress)(`agent:${to.agentName}`),
            text,
          },
        },
      });
      const received = yield* Fiber.join(waiting).pipe(
        Effect.flatMap(
          Option.match({
            onNone: () =>
              Effect.fail(
                new ProcessTestError({
                  message: "peer ended before certified delivery",
                }),
              ),
            onSome: Effect.succeed,
          }),
        ),
      );
      expect(received.item).toMatchObject({
        kind: "multicast",
        message: { content: [{ type: "text", text }] },
      });
    }),
  );
}

/** Calendar releases use npm's semver ordering; an unpublished name has no peers. */
function publishedVersions(view = npmView) {
  return view(["@moltzap/social-harness", "versions"]).pipe(
    Effect.catchIf(
      (error) => error instanceof Error && /\bE404\b/u.test(error.message),
      () => Effect.succeed("[]"),
    ),
    Effect.flatMap((output) =>
      Schema.decodeUnknown(
        Schema.parseJson(
          Schema.Union(Schema.Array(Schema.String), Schema.String),
        ),
      )(output.trim() === "" ? "[]" : output),
    ),
    Effect.map((versions) =>
      (typeof versions === "string" ? [versions] : versions).filter((version) =>
        /^\d{4}\.\d{3,4}\.\d+$/u.test(version),
      ),
    ),
  );
}

/** An npm operation exhausted its transport retries, rather than returning invalid metadata. */
class NpmOutageError extends Data.TaggedError("NpmOutageError")<{
  readonly message: string;
}> {}

function npmUnavailable(error: unknown) {
  return Effect.fail(new NpmOutageError({ message: String(error) }));
}

function npmView(args: readonly string[]) {
  return runCommand("npm", [
    "view",
    ...args,
    "--json",
    "--fetch-retries=0",
    "--fetch-timeout=15000",
  ]).pipe(retryNpm);
}

function retryNpm<A, E, R>(operation: Effect.Effect<A, E, R>) {
  return operation.pipe(
    Effect.retry({
      schedule: Schedule.exponential("1 second").pipe(
        Schedule.intersect(Schedule.recurs(2)),
      ),
      while: npmOutage,
    }),
    Effect.catchIf(npmOutage, npmUnavailable),
  );
}

function installCandidate(temporaryRoot: string, version: string) {
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const root = yield* fs.makeTempDirectoryScoped({
      directory: temporaryRoot,
      prefix: `peer-${version}-`,
    });
    return yield* installPackedConsumer({
      temporaryRoot: root,
      name: "published-peers-peer",
      archives: {},
      dependencies: yield* releaseClosure(version),
    });
  }).pipe(retryNpm);
}

/**
 * Stop at the explicit reset boundary, retaining the oldest live release. A
 * previous release outside retained history skips the gate only when the
 * reset boundary places it before the reset; otherwise the current build
 * dropped a version it still owes the previous release, and the gate fails.
 */
function selectReleases(
  versions: readonly string[],
  readVersion = releaseWireVersion,
  sets: {
    readonly live: readonly string[];
    readonly history: readonly string[];
  } = {
    live: LIVE_WIRE_VERSIONS,
    history: HISTORY_WIRE_VERSIONS,
  },
  firstPostResetRelease = FIRST_POST_RESET_RELEASE,
) {
  return Effect.gen(function* () {
    const previous = versions.at(-1);
    if (previous === undefined) {
      return [];
    }
    const previousWireVersion = yield* readVersion(previous);
    if (!sets.history.includes(previousWireVersion)) {
      const boundary = Option.getOrUndefined(firstPostResetRelease);
      if (boundary !== undefined && preResetRelease(previous, boundary)) {
        return [];
      }
      return yield* new ProcessTestError({
        message: `release ${previous} emits wire version ${previousWireVersion}, which this build neither admits nor retains; only a reset that moves FIRST_POST_RESET_RELEASE above ${previous} may drop it`,
      });
    }
    expect(sets.live).toContain(previousWireVersion);
    const oldest = yield* oldestLiveRelease(versions, readVersion, sets);
    return [...new Set([previous, oldest ?? previous])];
  });
}

/** The oldest release before the reset boundary whose wire version is live. */
function oldestLiveRelease<E, R>(
  versions: readonly string[],
  readVersion: (version: string) => Effect.Effect<string, E, R>,
  sets: {
    readonly live: readonly string[];
    readonly history: readonly string[];
  },
) {
  return Effect.gen(function* () {
    let oldest: string | undefined;
    for (const version of [...versions].reverse().slice(1)) {
      const wireVersion = yield* readVersion(version);
      if (!sets.history.includes(wireVersion)) {
        break;
      }
      if (sets.live.includes(wireVersion)) {
        oldest = version;
      }
    }
    return oldest;
  });
}

/**
 * Names the gate leg a failure came from: a fixture failure carries the leg in
 * its message, and every failure logs it.
 */
function inLeg(leg: string) {
  return <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    effect.pipe(
      Effect.catchIf(
        (error): error is E & ProcessTestError =>
          error instanceof ProcessTestError,
        (error) =>
          Effect.fail(
            new ProcessTestError({
              message: `${leg}: ${error.message}`,
              cause: error,
            }),
          ),
      ),
      Effect.tapError(() =>
        Effect.logError(`published-peers leg failed: ${leg}`),
      ),
    );
}

/**
 * The previous release's Registry and Router, which a daemon that upgrades
 * before its network must still reach. Releases ship both services only as
 * images, so each image's installed tree is copied out and its entrypoint
 * runs as a process with this Node, through the same harness as the current
 * network.
 */
function releaseNetwork(version: string, temporaryRoot: string) {
  const images = releaseImages(version);
  return Effect.all(
    {
      registry: imageEntrypoint(
        images.registry,
        join(temporaryRoot, `registry-${version}`),
      ),
      router: imageEntrypoint(
        images.router,
        join(temporaryRoot, `router-${version}`),
      ),
    },
    { concurrency: 2 },
  );
}

/**
 * The release's own indexes, through the version tags its release workflow
 * adds right after npm publication. A release whose tags are still missing
 * fails this gate until that workflow finishes.
 */
function releaseImages(version: string) {
  return {
    registry: `ghcr.io/social-harness/registry:${version}`,
    router: `ghcr.io/social-harness/router:${version}`,
  };
}

const imageConfig = Schema.parseJson(
  Schema.Struct({
    WorkingDir: Schema.String,
    Entrypoint: Schema.Tuple(Schema.String),
    Cmd: Schema.optional(Schema.NullOr(Schema.Array(Schema.String))),
  }),
);

/**
 * Copies `image`'s working directory out of a created container that never
 * starts and returns its entrypoint's path inside the copy. The pull runs
 * before the container exists, so an interrupted pull leaves nothing behind.
 */
function imageEntrypoint(image: string, destination: string) {
  return Effect.gen(function* () {
    yield* runCommand("docker", ["pull", "--quiet", image]);
    const config = yield* runCommand("docker", [
      "image",
      "inspect",
      "--format",
      "{{json .Config}}",
      image,
    ]).pipe(Effect.flatMap(Schema.decodeUnknown(imageConfig)));
    const entrypoint = posix.relative(
      config.WorkingDir,
      posix.resolve(config.WorkingDir, config.Entrypoint[0]),
    );
    if (
      entrypoint === "" ||
      entrypoint.startsWith("..") ||
      (config.Cmd?.length ?? 0) > 0
    ) {
      return yield* new ProcessTestError({
        message: `${image} must run one entrypoint file inside its working directory ${config.WorkingDir} with no arguments; it has ${JSON.stringify(config.Entrypoint)} and ${JSON.stringify(config.Cmd)}`,
      });
    }
    yield* Effect.acquireUseRelease(
      runCommand("docker", ["create", image]).pipe(
        Effect.map((container) => container.trim()),
      ),
      (container) =>
        runCommand("docker", [
          "cp",
          `${container}:${config.WorkingDir}`,
          destination,
        ]),
      (container) =>
        runCommand("docker", ["rm", container]).pipe(Effect.ignore),
    );
    const copied = join(destination, entrypoint);
    if (!(yield* (yield* FileSystem.FileSystem).exists(copied))) {
      return yield* new ProcessTestError({
        message: `${image}'s entrypoint ${config.Entrypoint[0]} is missing from its copied working directory`,
      });
    }
    return copied;
  });
}

function installCurrent(temporaryRoot: string) {
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const root = yield* fs.makeTempDirectoryScoped({
      directory: temporaryRoot,
      prefix: "current-",
    });
    const { archives } = yield* packWorkspaceClosure(packages, root);
    const currentRoot = join(root, "current");
    yield* fs.makeDirectory(currentRoot);
    return yield* installPackedConsumer({
      temporaryRoot: currentRoot,
      name: "published-peers-current",
      archives,
    });
  }).pipe(retryNpm);
}

/** One published daemon and one current daemon certify through `infrastructure`. */
function certifyPair(
  infrastructure: Effect.Effect.Success<typeof acquireProcessInfrastructure>,
  version: string,
  consumer: string,
  current: string,
) {
  return Effect.scoped(
    Effect.gen(function* () {
      const peer = yield* makeDaemonProcessFixture(
        infrastructure,
        `peer-${version.replaceAll(".", "-")}`,
      );
      const local = yield* makeDaemonProcessFixture(
        infrastructure,
        `local-${version.replaceAll(".", "-")}`,
      );
      yield* acquireDaemonProcess(
        peer,
        join(consumer, "node_modules/.bin/sharnessd"),
      );
      yield* acquireDaemonProcess(
        local,
        join(current, "node_modules/.bin/sharnessd"),
      );
      for (const fixture of [peer, local]) {
        const management = yield* acquireDaemonManagementClient(fixture);
        yield* management.register(makeRegistrationRequest(fixture));
      }
      yield* exchange(local, peer);
      yield* exchange(peer, local);
    }),
  );
}

const mixedRelease = Effect.scoped(
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const temporaryRoot = yield* fs.makeTempDirectoryScoped({
      prefix: "published-peers-",
    });
    const versions = yield* publishedVersions();
    const releases = yield* selectReleases(versions);
    if (releases.length === 0) {
      return versions.length === 0
        ? "No calendar release is published under @moltzap/social-harness; the published-peers gate starts after its first release."
        : RESET_SKIP_REASON;
    }
    yield* runCommand("docker", ["version"]).pipe(
      inLeg("Docker, which runs the previous release's network"),
    );
    const current = yield* installCurrent(temporaryRoot);
    const currentNetwork = yield* acquireProcessInfrastructure;
    for (const version of releases) {
      const consumer = yield* installCandidate(temporaryRoot, version);
      yield* certifyPair(currentNetwork, version, consumer, current).pipe(
        inLeg(`release ${version} daemon through the current network`),
      );
      if (version === releases[0]) {
        yield* Effect.scoped(
          releaseNetwork(version, temporaryRoot).pipe(
            Effect.flatMap(acquireProcessInfrastructureFrom),
            Effect.flatMap((previousNetwork) =>
              certifyPair(previousNetwork, version, consumer, current),
            ),
          ),
        ).pipe(
          inLeg(
            `current daemon through the network of previous release ${version}`,
          ),
        );
      }
    }
    return undefined;
  }),
).pipe(
  Effect.catchTag("NpmOutageError", (error) =>
    Effect.succeed(
      `npm outage after retries; published-peers gate skipped: ${String(error)}`,
    ),
  ),
  Effect.provide(NodeContext.layer),
);

function runMixedRelease(context: TestContext) {
  const fiber = Effect.runFork(mixedRelease);
  context.onTestFinished(
    () => Effect.runPromise(Fiber.interrupt(fiber).pipe(Effect.asVoid)),
    60_000,
  );
  return Effect.runPromise(
    Fiber.join(fiber).pipe(
      Effect.tap((reason) =>
        reason === undefined
          ? Effect.void
          : Effect.logWarning(reason).pipe(
              Effect.zipRight(Effect.sync(() => context.skip(reason))),
            ),
      ),
      Effect.asVoid,
    ),
  );
}

it(
  "certifies with the previous and oldest in-window published releases and through the previous network",
  runMixedRelease,
  600_000,
);

it("selects the oldest live peer rather than an older retained peer", () =>
  Effect.runPromise(
    selectReleases(
      ["2099.101.0", "2099.102.0", "2099.103.0", "2099.104.0"],
      (version) =>
        Effect.succeed(
          new Map([
            ["2099.104.0", "new"],
            ["2099.101.0", "retired"],
          ]).get(version) ?? "old",
        ),
      { live: ["old", "new"], history: ["retired", "old", "new"] },
    ).pipe(
      Effect.provide(NodeContext.layer),
      Effect.tap((releases) =>
        Effect.sync(() => {
          expect(releases).toEqual(["2099.104.0", "2099.102.0"]);
        }),
      ),
    ),
  ));

it("runs a single peer when the previous release is also the oldest live release", () =>
  Effect.runPromise(
    selectReleases(
      ["2099.101.0", "2099.102.0"],
      (version) =>
        Effect.succeed(version === "2099.101.0" ? "pre-reset" : "new"),
      { live: ["new"], history: ["new"] },
    ).pipe(
      Effect.provide(NodeContext.layer),
      Effect.tap((releases) =>
        Effect.sync(() => {
          expect(releases).toEqual(["2099.102.0"]);
        }),
      ),
    ),
  ));

it.each(["", "[]", '"0.0.0-bootstrap.0"', '["0.0.0-bootstrap.0"]'])(
  "starts without peers when npm has no calendar history (%s)",
  (output) =>
    Effect.runPromise(
      publishedVersions(() => Effect.succeed(output)).pipe(
        Effect.flatMap((versions) => selectReleases(versions)),
        Effect.provide(NodeContext.layer),
        Effect.tap((releases) =>
          Effect.sync(() => {
            expect(releases).toEqual([]);
          }),
        ),
        Effect.asVoid,
      ),
    ),
);

it("starts without peers when the package name is unpublished", () =>
  Effect.runPromise(
    publishedVersions(() =>
      Effect.fail(new PackGateError({ message: '{"error":{"code":"E404"}}' })),
    ).pipe(
      Effect.flatMap((versions) => selectReleases(versions)),
      Effect.provide(NodeContext.layer),
      Effect.tap((releases) =>
        Effect.sync(() => {
          expect(releases).toEqual([]);
        }),
      ),
      Effect.asVoid,
    ),
  ));

it.each(['"2099.101.0"', '["0.0.0-bootstrap.0","2099.101.0"]'])(
  "selects a published calendar peer after bootstrap (%s)",
  (output) =>
    Effect.runPromise(
      publishedVersions(() => Effect.succeed(output)).pipe(
        Effect.flatMap((versions) =>
          selectReleases(versions, () => Effect.succeed(WIRE_VERSION)),
        ),
        Effect.provide(NodeContext.layer),
        Effect.tap((releases) =>
          Effect.sync(() => {
            expect(releases).toEqual(["2099.101.0"]);
          }),
        ),
        Effect.asVoid,
      ),
    ),
);

it("skips when the reset boundary places the previous release before the reset", () =>
  Effect.runPromise(
    selectReleases(
      ["2099.101.0"],
      () => Effect.succeed(PRE_RESET_WIRE_VERSION),
      {
        live: ["new"],
        history: ["new"],
      },
      Option.some("2099.102.0"),
    ).pipe(
      Effect.provide(NodeContext.layer),
      Effect.tap((releases) =>
        Effect.sync(() => {
          expect(releases).toEqual([]);
        }),
      ),
    ),
  ));

it.each([Option.none<string>(), Option.some("2099.101.0")])(
  "fails when the previous release's version left history without a reset boundary above it (%o)",
  (boundary) =>
    Effect.runPromise(
      selectReleases(
        ["2099.101.0"],
        () => Effect.succeed("old"),
        { live: ["new"], history: ["new"] },
        boundary,
      ).pipe(
        Effect.flip,
        Effect.tap((error) =>
          Effect.sync(() => {
            expect(error).toBeInstanceOf(ProcessTestError);
          }),
        ),
        Effect.provide(NodeContext.layer),
        Effect.asVoid,
      ),
    ),
);

it.each(["2099.101.0", "2099.102.0"])(
  "fails when post-reset release %s omits wire metadata",
  (version) =>
    Effect.runPromise(
      releaseWireVersion(
        version,
        () => Effect.succeed("{}"),
        Option.some("2099.101.0"),
      ).pipe(
        Effect.flip,
        Effect.tap((error) =>
          Effect.sync(() => {
            expect(error).toMatchObject({
              _tag: "ProcessTestError",
              message: `@moltzap/social-harness@${version} is missing published sharnessWireVersion metadata`,
            });
          }),
        ),
        Effect.provide(NodeContext.layer),
        Effect.asVoid,
      ),
    ),
);

it.each([
  ["2026.1019.1", Option.none<string>()],
  ["2099.101.0", Option.none<string>()],
  ["2099.100.0", Option.some("2099.101.0")],
])(
  "treats metadata-free %s as pre-reset before boundary %s",
  (version, boundary) =>
    Effect.runPromise(
      releaseWireVersion(version, () => Effect.succeed("{}"), boundary).pipe(
        Effect.tap((wireVersion) =>
          Effect.sync(() => {
            expect(wireVersion).toBe(PRE_RESET_WIRE_VERSION);
          }),
        ),
        Effect.provide(NodeContext.layer),
        Effect.asVoid,
      ),
    ),
);

it("reads published wire metadata at the reset boundary", () =>
  Effect.runPromise(
    releaseWireVersion(
      "2099.101.0",
      () =>
        Effect.succeed(JSON.stringify({ sharnessWireVersion: WIRE_VERSION })),
      Option.some("2099.101.0"),
    ).pipe(
      Effect.tap((wireVersion) =>
        Effect.sync(() => {
          expect(wireVersion).toBe(WIRE_VERSION);
        }),
      ),
      Effect.provide(NodeContext.layer),
    ),
  ));

it.each([
  ["npm error code E503", true],
  ["npm error code EAI_AGAIN", true],
  ["ERR_PNPM_FETCH_503 Service Unavailable - 503", true],
  ["ERR_PNPM_FETCH_429 Too Many Requests - 429", true],
  ["ERR_PNPM_FETCH_404 Not Found - 404", false],
  ["npm error code E404", false],
  ["invalid release metadata", false],
])("classifies registry availability: %s", (message, outage) => {
  expect(npmOutage(new Error(message))).toBe(outage);
});
