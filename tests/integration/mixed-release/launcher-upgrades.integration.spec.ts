/** @file Real packed library releases upgrade twice through one persistent launcher record. */
import { FileSystem } from "@effect/platform";
import { NodeContext } from "@effect/platform-node";
import { WIRE_VERSION } from "@moltzap/social-harness";
import { Effect, Schema } from "effect";
import { S_IROTH, S_IXOTH } from "node:constants";
import { createHash } from "node:crypto";
import { join, resolve } from "node:path";
import { expect, it } from "vitest";
import {
  extractPackedArchive,
  installPackedConsumer,
  PackGateError,
  packProductClosure,
  runCommand,
} from "../../../scripts/test/packed-workspace.js";
import {
  acquireLoopbackServer,
  type IncomingMessage,
  type ServerResponse,
} from "../loopback-server.js";

const workspaceRoot = resolve(import.meta.dirname, "../../..");
const prepared = Schema.parseJson(
  Schema.Struct({
    version: Schema.String,
    prefix: Schema.String,
    previous: Schema.optional(Schema.String),
    daemon: Schema.String,
  }),
);
const manifestSchema = Schema.parseJson(
  Schema.Struct({
    name: Schema.String,
    version: Schema.String,
    dependencies: Schema.Record({ key: Schema.String, value: Schema.String }),
    sharnessWireVersion: Schema.String,
    sharnessContentHash: Schema.optional(Schema.String),
  }),
);

type Reply = Readonly<{ bytes: Uint8Array; contentType: string }>;

function serveRelease(
  replies: ReadonlyMap<string, Reply>,
  request: IncomingMessage,
  response: ServerResponse,
) {
  const reply = replies.get(request.url ?? "");
  if (reply === undefined) {
    response.writeHead(307, {
      location: `https://registry.npmjs.org${request.url ?? "/"}`,
    });
    response.end();
    return;
  }
  response.writeHead(200, { "content-type": reply.contentType });
  response.end(reply.bytes);
}

/** Local metadata and tarball serving keeps release installation on the ordinary HTTP path. */
const registry = (replies: ReadonlyMap<string, Reply>) =>
  Effect.gen(function* () {
    const { port } = yield* acquireLoopbackServer((request, response) =>
      Effect.sync(() => {
        serveRelease(replies, request, response);
      }),
    );
    return { origin: `http://127.0.0.1:${port}` };
  });

const json = (value: unknown): Reply => ({
  bytes: new TextEncoder().encode(JSON.stringify(value)),
  contentType: "application/json",
});

/** The verifier stand-in leaves the real bundle parser, subject binding and packed manifest checks active. */
function attestation(version: string, digest: string): Reply {
  return json({
    attestations: [
      {
        predicateType: "https://slsa.dev/provenance/v1",
        bundle: {
          mediaType: "application/vnd.dev.sigstore.bundle.v0.3+json",
          verificationMaterial: {
            certificate: { rawBytes: "AQ==" },
            tlogEntries: [],
          },
          dsseEnvelope: {
            payloadType: "application/vnd.in-toto+json",
            payload: Buffer.from(
              JSON.stringify({
                subject: [
                  {
                    name: `pkg:npm/%40moltzap/social-harness@${version}`,
                    digest: { sha512: digest },
                  },
                ],
              }),
            ).toString("base64"),
            signatures: [{ sig: "AQ==", keyid: "fixture" }],
          },
        },
      },
    ],
  });
}

/** Only transient registry failures waive this network-dependent installation gate. */
function npmOutage(error: unknown): boolean {
  return (
    error instanceof Error &&
    /EAI_AGAIN|ENOTFOUND|ECONNRESET|ECONNREFUSED|ETIMEDOUT|E(?:408|429|500|502|503|504)|ERR_PNPM_FETCH_(?:408|429|500|502|503|504)|FETCH_ERROR|fetch failed/iu.test(
      error.message,
    )
  );
}

interface ReleaseSource {
  readonly scratch: string;
  readonly source: string;
  readonly manifestFile: string;
  readonly original: Readonly<Record<string, unknown>>;
  readonly replies: Map<string, Reply>;
  readonly origin: string;
}

const prepareReleaseSource = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const scratch = yield* fs.makeTempDirectoryScoped({
    prefix: "launcher-upgrades-",
  });
  const { archives } = yield* packProductClosure(
    workspaceRoot,
    "packages/social-harness",
    scratch,
  );
  const archive = archives["@moltzap/social-harness"];
  if (archive === undefined) {
    return yield* new PackGateError({ message: "Client tarball is missing" });
  }
  const source = yield* extractPackedArchive(archive, scratch);
  const manifestFile = join(source, "package.json");
  const text = yield* fs.readFileString(manifestFile);
  const original = yield* Schema.decodeUnknown(
    Schema.parseJson(
      Schema.Record({ key: Schema.String, value: Schema.Unknown }),
    ),
  )(text);
  const replies = new Map<string, Reply>();
  const { origin } = yield* registry(replies);
  return { scratch, source, manifestFile, original, replies, origin };
});

function packRelease(release: ReleaseSource, version: string) {
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const manifest = {
      ...release.original,
      version,
      sharnessWireVersion: WIRE_VERSION,
    };
    yield* fs.writeFileString(release.manifestFile, JSON.stringify(manifest));
    yield* runCommand(
      "npm",
      ["pack", "--ignore-scripts", "--pack-destination", release.scratch],
      { cwd: release.source },
    );
    const tarball = join(
      release.scratch,
      `moltzap-social-harness-${version}.tgz`,
    );
    const bytes = yield* fs.readFile(tarball);
    const digest = createHash("sha512").update(bytes).digest();
    const admitted = yield* Schema.decodeUnknown(manifestSchema)(
      JSON.stringify(manifest),
    );
    release.replies.set(
      `/@moltzap%2fsocial-harness/${version}`,
      json({
        ...admitted,
        dist: {
          integrity: `sha512-${digest.toString("base64")}`,
          tarball: `${release.origin}/tarballs/${version}`,
          attestations: { url: `${release.origin}/attestations/${version}` },
        },
      }),
    );
    release.replies.set(`/tarballs/${version}`, {
      bytes,
      contentType: "application/octet-stream",
    });
    release.replies.set(
      `/attestations/${version}`,
      attestation(version, digest.toString("hex")),
    );
    return tarball;
  });
}

interface LauncherInstallation {
  readonly seed: string;
  readonly home: string;
  readonly origin: string;
  readonly prefix: string;
  readonly previous: string;
}

function installSeed(
  release: ReleaseSource,
  firstArchive: string,
  first: string,
) {
  return Effect.gen(function* () {
    const seed = yield* installPackedConsumer({
      temporaryRoot: release.scratch,
      name: "launcher-upgrade-consumer",
      archives: { "@moltzap/social-harness": firstArchive },
    });
    return {
      seed,
      home: join(release.scratch, "home"),
      origin: release.origin,
      prefix: seed,
      previous: first,
    };
  });
}

function verifyUpgrade(
  installation: LauncherInstallation,
  version: string,
  pinned: boolean,
) {
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const { seed, home, prefix, previous, origin } = installation;
    const output = yield* runCommand(
      process.execPath,
      [
        "--import",
        join(import.meta.dirname, "../daemon/launcher-provenance.ts"),
        join(
          prefix,
          "node_modules/@moltzap/social-harness/bin/sharnessd-launcher",
        ),
        "prepare",
      ],
      {
        cwd: prefix,
        env: {
          SHARNESS_LAUNCHER_DIRECTORY: home,
          SHARNESS_LAUNCHER_SEED: seed,
          SHARNESS_LAUNCHER_PACKAGE: "@moltzap/social-harness",
          SHARNESS_LAUNCHER_UPGRADE: "on",
          ...(pinned ? { SHARNESS_LAUNCHER_VERSION: version } : {}),
          SHARNESS_LAUNCHER_REGISTRY: origin,
        },
      },
    );
    const choice = yield* Schema.decodeUnknown(prepared)(output.trim());
    expect(choice.version).toBe(version);
    expect(choice.previous).toBe(previous);
    expect(choice.prefix).not.toBe(prefix);
    expect(yield* fs.exists(choice.daemon)).toBe(true);
    yield* assertPublicRelease(choice.prefix, choice.daemon);
    const recorded = yield* fs
      .readFileString(join(home, "launcher.json"))
      .pipe(
        Effect.flatMap(
          Schema.decodeUnknown(
            Schema.parseJson(Schema.Struct({ version: Schema.String })),
          ),
        ),
      );
    expect(recorded.version).toBe(version);
    return { ...installation, prefix: choice.prefix, previous: version };
  });
}

/** Host users can traverse and read public code installed under the launcher's private umask. */
function assertPublicRelease(prefix: string, daemon: string) {
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const packageRoot = join(prefix, "node_modules/@moltzap/social-harness");
    const directoryAccess = S_IROTH | S_IXOTH;
    for (const directory of [
      prefix,
      join(prefix, "node_modules"),
      join(prefix, "node_modules/@moltzap"),
      packageRoot,
      join(packageRoot, "bin"),
      join(packageRoot, "dist"),
    ]) {
      expect((yield* fs.stat(directory)).mode & directoryAccess).toBe(
        directoryAccess,
      );
    }
    for (const file of [
      daemon,
      join(packageRoot, "package.json"),
      join(packageRoot, "dist/index.js"),
    ]) {
      expect((yield* fs.stat(file)).mode & S_IROTH).toBe(S_IROTH);
    }
  });
}

function consecutiveUpgrades() {
  return Effect.scoped(
    Effect.gen(function* () {
      const release = yield* prepareReleaseSource;
      const first = "2099.101.0";
      const firstArchive = yield* packRelease(release, first);
      yield* packRelease(release, "2099.101.1");
      yield* packRelease(release, "2099.101.2");
      const seed = yield* installSeed(release, firstArchive, first);
      const upgraded = yield* verifyUpgrade(seed, "2099.101.1", true);
      release.replies.set(
        "/-/package/@moltzap%2fsocial-harness/dist-tags",
        json({ latest: "2099.101.2" }),
      );
      yield* verifyUpgrade(upgraded, "2099.101.2", false);
    }),
  );
}

it(
  "performs two consecutive launcher upgrades between real packed installations",
  ({ skip }) =>
    Effect.runPromise(
      consecutiveUpgrades().pipe(
        Effect.provide(NodeContext.layer),
        Effect.catchIf(npmOutage, (error) =>
          Effect.sync(() => {
            skip(
              true,
              `npm outage; launcher upgrade gate skipped: ${String(error)}`,
            );
          }),
        ),
      ),
    ),
  600_000,
);
