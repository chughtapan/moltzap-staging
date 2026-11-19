/** @file Packs workspace packages and installs them into isolated consumers. */
import {
  Command,
  type CommandExecutor,
  FileSystem,
  type Error as PlatformError,
} from "@effect/platform";
import { NodeContext, NodeRuntime } from "@effect/platform-node";
import { Cause, Data, Effect, type ParseResult, Schema, Stream } from "effect";
import { join, posix, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parse } from "yaml";

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
 * Run one command to completion and report both output streams and its exit
 * code, for a check that expects a particular failure.
 * @param command Executable name or path.
 * @param args Arguments passed verbatim.
 * @param options Working directory and environment overrides.
 */
export const runProcess = Effect.fn("packedWorkspace.runProcess")(function* (
  command: string,
  args: readonly string[],
  options: RunOptions = {},
) {
  const child = yield* Command.start(configure(command, args, options));
  const [stdout, stderr, exitCode] = yield* Effect.all(
    [collect(child.stdout), collect(child.stderr), child.exitCode],
    { concurrency: 3 },
  );
  return { stdout, stderr, exitCode };
}, Effect.scoped);

/**
 * Run one command to completion and return its standard output, failing when
 * it exits non-zero. The failure carries both output streams, because tools
 * such as `tsc` print their diagnostics on standard output.
 * @param command Executable name or path.
 * @param args Arguments passed verbatim.
 * @param options Working directory and environment overrides.
 */
export const runCommand = Effect.fn("packedWorkspace.runCommand")(
  (command: string, args: readonly string[], options: RunOptions = {}) =>
    runProcess(command, args, options).pipe(
      Effect.tap(({ stdout, stderr, exitCode }) =>
        requireCondition(
          exitCode === 0,
          `${[command, ...args].join(" ")} exited with ${exitCode}:\n${stdout}${stderr}`,
        ),
      ),
      Effect.map(({ stdout }) => stdout),
    ),
);

/**
 * Read and decode one JSON file.
 * @param path File to read.
 * @param schema Shape the parsed JSON must have.
 */
const readJsonFile = Effect.fn("packedWorkspace.readJsonFile")(function* <A, I>(
  path: string,
  schema: Schema.Schema<A, I>,
) {
  const fs = yield* FileSystem.FileSystem;
  const text = yield* fs.readFileString(path);
  return yield* Schema.decodeUnknown(Schema.parseJson(schema))(text);
});

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
 * The workspace root package and the tracked packages `pnpm-workspace.yaml`
 * selects: the root, and each `package.json` directory that a `packages` glob
 * includes and no `!` glob excludes.
 * @param workspaceRoot Source workspace root.
 * @returns Each package's directory, name, dependencies and exports.
 */
export const workspacePackages = Effect.fn("packedWorkspace.workspacePackages")(
  function* (workspaceRoot: string) {
    const stringKeyed = Schema.optionalWith(
      Schema.Record({ key: Schema.String, value: Schema.Unknown }),
      { default: () => ({}) },
    );
    const { packages: globs } = yield* readWorkspaceSettings(
      workspaceRoot,
      Schema.Struct({ packages: Schema.Array(Schema.String) }),
    );
    const selected = (directory: string, excluded: boolean) =>
      globs.some(
        (glob) =>
          glob.startsWith("!") === excluded &&
          posix.matchesGlob(directory, glob.replace(/^!/u, "")),
      );
    const tracked = yield* runCommand(
      "git",
      ["ls-files", "-z", "--", ":(glob)**/package.json"],
      { cwd: workspaceRoot },
    );
    return yield* Effect.forEach(
      tracked
        .split("\0")
        .filter(Boolean)
        .map((file) => posix.dirname(file))
        .filter(
          (directory) =>
            directory === "." ||
            (selected(directory, false) && !selected(directory, true)),
        ),
      (dir) =>
        readJsonFile(
          join(workspaceRoot, dir, "package.json"),
          Schema.Struct({
            name: Schema.optionalWith(Schema.String, { default: () => "" }),
            dependencies: stringKeyed,
            exports: stringKeyed,
          }),
        ).pipe(Effect.map((manifest) => ({ dir, ...manifest }))),
      { concurrency: 8 },
    );
  },
);

/**
 * Derive the packages to pack from the source dependency graph.
 * @param workspaceRoot Source workspace root.
 * @param entry Repository-relative directory of the product whose
 * transitive closure is packed.
 * @param temporaryRoot Scratch directory owned by the caller.
 * @returns Package names mapped to tarball paths.
 */
export const packProductClosure = Effect.fn(
  "packedWorkspace.packProductClosure",
)(function* (workspaceRoot: string, entry: string, temporaryRoot: string) {
  const products = yield* workspacePackages(workspaceRoot);
  const initial = products.find(({ dir }) => dir === entry);
  if (initial === undefined) {
    return yield* new PackGateError({ message: `Unknown product ${entry}` });
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
  return yield* packWorkspaceClosure(
    Object.fromEntries(
      [...closure.values()].map(({ dir, name }) => [
        name,
        join(workspaceRoot, dir),
      ]),
    ),
    temporaryRoot,
  );
});

/**
 * Consumer installs use the workspace's Effect override, so they exercise the
 * version the workspace builds.
 * @param workspaceRoot Source workspace root.
 * @returns Dependency specifiers for the isolated consumer.
 */
export const readConsumerPins = Effect.fn("packedWorkspace.readConsumerPins")(
  function* (workspaceRoot: string) {
    const workspace = yield* readWorkspaceSettings(
      workspaceRoot,
      Schema.Struct({
        overrides: Schema.Struct({ effect: Schema.String }),
      }),
    );
    return {
      effect: workspace.overrides.effect,
    };
  },
);

/**
 * Create a consumer project under `temporaryRoot/consumer` that depends on
 * the packed tarballs plus `dependencies`, install it with the tarballs
 * overriding every workspace name.
 * @param input Consumer description.
 * @returns The consumer directory.
 */
export const installPackedConsumer = Effect.fn(
  "packedWorkspace.installPackedConsumer",
)(function* (input: ConsumerInput) {
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
});

/**
 * Pack the source roots into a scratch directory for consumer installation.
 * @param packageRoots Package names mapped to source roots.
 * @param temporaryRoot Scratch directory owned by the caller.
 * @returns Package names mapped to tarball paths.
 */
function packWorkspaceClosure(
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
          Effect.map((archive): readonly [string, string] => [name, archive]),
        ),
      { concurrency: 4 },
    );
    return { archives: Object.fromEntries(packed) };
  }).pipe(Effect.withSpan("packedWorkspace.packWorkspaceClosure"));
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
      fileURLToPath(new URL("../package.json", import.meta.url)),
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

/** Reads `pnpm-workspace.yaml` and decodes the settings `schema` names. */
function readWorkspaceSettings<A, I>(
  workspaceRoot: string,
  schema: Schema.Schema<A, I>,
): Effect.Effect<A, GateError, FileSystem.FileSystem> {
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const text = yield* fs.readFileString(
      join(workspaceRoot, "pnpm-workspace.yaml"),
    );
    const value = yield* Effect.try({
      try: (): unknown => parse(text),
      catch: (cause) => new PackGateError({ message: String(cause) }),
    });
    return yield* Schema.decodeUnknown(schema)(value);
  });
}
