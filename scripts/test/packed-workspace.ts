/** @file Packs workspace packages and installs them into isolated consumers. */
import {
  Command,
  type CommandExecutor,
  FileSystem,
  type Error as PlatformError,
} from "@effect/platform";
import { NodeContext, NodeRuntime } from "@effect/platform-node";
import { Cause, Data, Effect, type ParseResult, Schema, Stream } from "effect";
import { join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parse } from "yaml";
import { WIRE_VERSION } from "../../packages/social-harness/src/version.ts";
import { recordPackedWireVersion } from "../release/package-content-hash.ts";
import { workspacePackages } from "../repo/packages.ts";

/** Services every pack gate step runs against. */
export type GateServices =
  | CommandExecutor.CommandExecutor
  | FileSystem.FileSystem;

/** A pack gate check that did not hold. */
export class PackGateError extends Data.TaggedError("PackGateError")<{
  readonly message: string;
}> {}

/** Every way a gate step fails: a check, a file or process, or a decode. */
export type GateError =
  | PackGateError
  | PlatformError.PlatformError
  | ParseResult.ParseError;

/**
 * Options for {@link runCommand}. `env` entries override the parent
 * environment; an empty `NODE_PATH` adds no search paths, which is how a
 * consumer check drops the parent's.
 */
interface RunOptions {
  readonly cwd?: string;
  readonly env?: Readonly<Record<string, string>>;
}

/** Package names mapped to the tarball each one packed into. */
export type Archives = Readonly<Record<string, string>>;

/** Description of the consumer project {@link installPackedConsumer} builds. */
interface ConsumerInput {
  /** Scratch directory owned by the caller. */
  readonly temporaryRoot: string;
  readonly name: string;
  readonly archives: Archives;
  readonly dependencies?: Readonly<Record<string, string>>;
  readonly devDependencies?: Readonly<Record<string, string>>;
}

/**
 * Fail with `detail` unless the checked fact holds.
 * @param holds Checked fact.
 * @param detail Failure message.
 */
export function requireCondition(
  holds: boolean,
  detail: string,
): Effect.Effect<void, PackGateError> {
  return Effect.succeed(holds).pipe(
    Effect.filterOrFail(
      (fact) => fact,
      () => new PackGateError({ message: detail }),
    ),
    Effect.asVoid,
  );
}

/**
 * Run one command to completion and return its standard output, failing when
 * it exits non-zero. The failure carries both output streams, because tools
 * such as `tsc` print their diagnostics on standard output.
 * @param command Executable name or path.
 * @param args Arguments passed verbatim.
 * @param options Working directory and environment overrides.
 */
export function runCommand(
  command: string,
  args: readonly string[],
  options: RunOptions = {},
): Effect.Effect<string, GateError, CommandExecutor.CommandExecutor> {
  return runProcess(command, args, options).pipe(
    Effect.tap(({ stdout, stderr, exitCode }) =>
      requireCondition(
        exitCode === 0,
        `${[command, ...args].join(" ")} exited with ${exitCode}:\n${stdout}${stderr}`,
      ),
    ),
    Effect.map(({ stdout }) => stdout),
    Effect.withSpan("packedWorkspace.runCommand"),
  );
}

/** What a finished process printed and how it exited. */
export interface Finished {
  readonly stdout: string;
  readonly stderr: string;
  readonly exitCode: number;
}

/**
 * Run one command to completion and report both output streams and its exit
 * code, for a check that expects a particular failure.
 * @param command Executable name or path.
 * @param args Arguments passed verbatim.
 * @param options Working directory and environment overrides.
 */
export function runProcess(
  command: string,
  args: readonly string[],
  options: RunOptions = {},
): Effect.Effect<Finished, GateError, CommandExecutor.CommandExecutor> {
  return Effect.scoped(
    Effect.gen(function* () {
      const child = yield* Command.start(configure(command, args, options));
      const [stdout, stderr, exitCode] = yield* Effect.all(
        [collect(child.stdout), collect(child.stderr), child.exitCode],
        { concurrency: 3 },
      );
      return { stdout, stderr, exitCode };
    }),
  ).pipe(Effect.withSpan("packedWorkspace.runProcess"));
}

/**
 * Read and decode one JSON file.
 * @param path File to read.
 * @param schema Shape the parsed JSON must have.
 */
export function readJsonFile<A, I>(
  path: string,
  schema: Schema.Schema<A, I>,
): Effect.Effect<A, GateError, FileSystem.FileSystem> {
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const text = yield* fs.readFileString(path);
    return yield* Schema.decodeUnknown(Schema.parseJson(schema))(text);
  }).pipe(Effect.withSpan("packedWorkspace.readJsonFile"));
}

/**
 * Run a pack gate as the process entry point: provide the Node services and a
 * scratch directory that is removed with everything under it when the gate
 * ends on any path, print the line the gate returns when it passes, and exit
 * non-zero with the failure, or the defect's cause, when it does not.
 * @param prefix Scratch directory name prefix.
 * @param gate The gate program, given the scratch directory; it returns the
 * success line to print.
 */
export function runGate(
  prefix: string,
  gate: (
    temporaryRoot: string,
  ) => Effect.Effect<string, GateError, GateServices>,
): void {
  const program = Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      return yield* gate(yield* fs.makeTempDirectoryScoped({ prefix }));
    }),
  ).pipe(
    Effect.flatMap((line) =>
      Effect.sync(() => process.stdout.write(`${line}\n`)),
    ),
    Effect.tapErrorCause((cause) =>
      Effect.sync(() =>
        process.stderr.write(
          `${Cause.isFailType(cause) ? cause.error.message : Cause.pretty(cause)}\n`,
        ),
      ),
    ),
    Effect.withSpan("packedWorkspace.runGate"),
  );
  NodeRuntime.runMain(program.pipe(Effect.provide(NodeContext.layer)), {
    disableErrorReporting: true,
  });
}

/**
 * Derive the packages to pack from the source dependency graph.
 * @param workspaceRoot Source workspace root.
 * @param entry Repository-relative directory of the product whose
 * transitive closure is packed.
 * @param temporaryRoot Scratch directory owned by the caller.
 * @returns Package names mapped to tarball paths.
 */
export function packProductClosure(
  workspaceRoot: string,
  entry: string,
  temporaryRoot: string,
) {
  const products = workspacePackages(workspaceRoot);
  const initial = products.find(({ dir }) => dir === entry);
  if (initial === undefined) {
    return Effect.fail(
      new PackGateError({ message: `Unknown product ${entry}` }),
    );
  }
  const closure = new Map<string, (typeof products)[number]>();
  function visit(product: (typeof products)[number]): void {
    if (closure.has(product.name)) {
      return;
    }
    closure.set(product.name, product);
    for (const dependency of products.filter((candidate) =>
      Object.hasOwn(product.dependencies, candidate.name),
    )) {
      visit(dependency);
    }
  }
  visit(initial);
  return packWorkspaceClosure(
    Object.fromEntries(
      [...closure.values()].map(({ dir, name }) => [
        name,
        join(workspaceRoot, dir),
      ]),
    ),
    temporaryRoot,
  );
}

/**
 * Consumer installs use the workspace's Effect override and its TypeScript and
 * OpenClaw catalog entries, including npm aliases, so they exercise the
 * versions the workspace builds.
 * @param workspaceRoot Source workspace root.
 * @returns Dependency specifiers for the isolated consumer.
 */
export function readConsumerPins(workspaceRoot: string) {
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const text = yield* fs.readFileString(
      join(workspaceRoot, "pnpm-workspace.yaml"),
    );
    const value = yield* Effect.try({
      try: (): unknown => parse(text),
      catch: (cause) => new PackGateError({ message: String(cause) }),
    });
    const workspace = yield* Schema.decodeUnknown(
      Schema.Struct({
        catalog: Schema.Struct({
          openclaw: Schema.String,
          typescript: Schema.String,
        }),
        overrides: Schema.Struct({ effect: Schema.String }),
      }),
    )(value);
    return {
      effect: workspace.overrides.effect,
      openclaw: workspace.catalog.openclaw,
      typescript: workspace.catalog.typescript,
    };
  }).pipe(Effect.withSpan("packedWorkspace.readConsumerPins"));
}

/**
 * Read one text file, failing with `missing` when the file system refuses.
 * @param path File to read.
 * @param missing Failure message when the file is absent or unreadable.
 */
export function readText(
  path: string,
  missing: string,
): Effect.Effect<string, GateError, FileSystem.FileSystem> {
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    return yield* fs.readFileString(path);
  }).pipe(
    Effect.catchTag("SystemError", () =>
      Effect.fail(new PackGateError({ message: missing })),
    ),
    Effect.withSpan("packedWorkspace.readText"),
  );
}

/**
 * Invoke the installed compiler through its declared executable; catalog aliases
 * may expose a versioned command name.
 * @param consumerRoot Isolated consumer with TypeScript installed.
 */
export function checkConsumerTypes(consumerRoot: string) {
  return Effect.gen(function* () {
    const compilerRoot = join(consumerRoot, "node_modules", "typescript");
    const compiler = yield* readJsonFile(
      join(compilerRoot, "package.json"),
      Schema.Struct({
        bin: Schema.Record({ key: Schema.String, value: Schema.String }),
      }),
    );
    const target = Object.entries(compiler.bin).find(([name]) =>
      /^tsc\d*$/u.test(name),
    )?.[1];
    if (target === undefined) {
      return yield* new PackGateError({
        message: "TypeScript declares no compiler executable",
      });
    }
    yield* runCommand(
      process.execPath,
      [
        join(compilerRoot, target),
        "--project",
        join(consumerRoot, "tsconfig.json"),
      ],
      { cwd: consumerRoot },
    );
  }).pipe(Effect.withSpan("packedWorkspace.checkConsumerTypes"));
}

/**
 * Create a consumer project under `temporaryRoot/consumer` that depends on
 * the packed tarballs plus `dependencies`, install it with the tarballs
 * overriding every workspace name.
 * @param input Consumer description.
 * @returns The consumer directory.
 */
export function installPackedConsumer(
  input: ConsumerInput,
): Effect.Effect<string, GateError, GateServices> {
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const consumerRoot = join(input.temporaryRoot, "consumer");
    yield* fs.makeDirectory(consumerRoot);
    yield* writeConsumerProject(consumerRoot, input);
    yield* runCommand(
      "pnpm",
      ["install", "--no-frozen-lockfile", "--prefer-offline"],
      { cwd: consumerRoot },
    );
    return consumerRoot;
  }).pipe(Effect.withSpan("packedWorkspace.installPackedConsumer"));
}

/**
 * Pack the source roots into a scratch directory for consumer installation.
 * @param packageRoots Package names mapped to source roots.
 * @param temporaryRoot Scratch directory owned by the caller.
 * @returns Package names mapped to tarball paths.
 */
export function packWorkspaceClosure(
  packageRoots: Readonly<Record<string, string>>,
  temporaryRoot: string,
): Effect.Effect<{ readonly archives: Archives }, GateError, GateServices> {
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const destination = join(temporaryRoot, "tarballs");
    yield* fs.makeDirectory(destination);
    const packed = yield* Effect.forEach(
      Object.entries(packageRoots),
      ([name, root]) =>
        packWorkspacePackage(root, destination).pipe(
          Effect.tap((archive) => recordWireVersion(name, archive)),
          Effect.map((archive): readonly [string, string] => [name, archive]),
        ),
      { concurrency: 4 },
    );
    return { archives: Object.fromEntries(packed) };
  }).pipe(Effect.withSpan("packedWorkspace.packWorkspaceClosure"));
}

/** Writes the wire version into the packed Client manifest, as the release does. */
function recordWireVersion(
  name: string,
  archive: string,
): Effect.Effect<void, GateError> {
  if (name !== "@moltzap/social-harness") {
    return Effect.void;
  }
  return Effect.try({
    try: () => recordPackedWireVersion(archive, WIRE_VERSION),
    catch: (cause) => new PackGateError({ message: String(cause) }),
  }).pipe(
    Effect.flatMap((failure) =>
      failure === undefined
        ? Effect.void
        : Effect.fail(new PackGateError({ message: failure.message })),
    ),
  );
}

function configure(
  command: string,
  args: readonly string[],
  options: RunOptions,
): Command.Command {
  const base = Command.make(command, ...args).pipe(
    Command.env(options.env ?? {}),
  );
  return options.cwd === undefined
    ? base
    : Command.workingDirectory(base, options.cwd);
}

function collect<E>(
  stream: Stream.Stream<Uint8Array, E>,
): Effect.Effect<string, E> {
  return stream.pipe(Stream.decodeText(), Stream.mkString);
}

function packWorkspacePackage(
  packageRoot: string,
  destination: string,
): Effect.Effect<string, GateError, CommandExecutor.CommandExecutor> {
  return Effect.gen(function* () {
    const stdout = yield* runCommand(
      "pnpm",
      ["pack", "--pack-destination", destination],
      { cwd: packageRoot },
    );
    const printed = nonEmptyLines(stdout).at(-1);
    if (printed === undefined) {
      return yield* new PackGateError({
        message: "pnpm pack returned no archive",
      });
    }
    return resolve(packageRoot, printed);
  });
}

function nonEmptyLines(text: string): readonly string[] {
  return text
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0);
}

function writeConsumerProject(
  consumerRoot: string,
  input: ConsumerInput,
): Effect.Effect<void, GateError, FileSystem.FileSystem> {
  const devDependencies = input.devDependencies ?? {};
  const localPackages = Object.fromEntries(
    Object.entries(input.archives).map(([packageName, archive]) => [
      packageName,
      `file:${relative(consumerRoot, archive)}`,
    ]),
  );
  const manifest = {
    name: input.name,
    version: "0.0.0",
    private: true,
    type: "module",
    pnpm: { onlyBuiltDependencies: ["better-sqlite3"] },
    dependencies: { ...localPackages, ...input.dependencies },
    ...(Object.keys(devDependencies).length === 0 ? {} : { devDependencies }),
  };
  const workspace = [
    'packages: ["."]',
    "overrides:",
    ...Object.entries(localPackages).map(
      ([packageName, specifier]) =>
        `  ${JSON.stringify(packageName)}: ${JSON.stringify(specifier)}`,
    ),
    "",
  ].join("\n");
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const { packageManager } = yield* readJsonFile(
      fileURLToPath(new URL("../../package.json", import.meta.url)),
      Schema.Struct({ packageManager: Schema.String }),
    );
    yield* fs.writeFileString(
      join(consumerRoot, "package.json"),
      `${JSON.stringify({ ...manifest, packageManager }, null, 2)}\n`,
    );
    yield* fs.writeFileString(
      join(consumerRoot, "pnpm-workspace.yaml"),
      workspace,
    );
  });
}
