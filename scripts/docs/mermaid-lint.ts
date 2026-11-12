/**
 * @file Discovers documented Mermaid diagrams and validates them with `mmdc`.
 */
import {
  Command,
  type CommandExecutor,
  FileSystem,
  Path,
} from "@effect/platform";
import { Data, Effect, Either, Stream, String as StringOps } from "effect";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

/**
 * Workspace-relative trees covered by the gate. Add a new documentation
 * location here so its diagrams are checked.
 */
export const MERMAID_ROOTS = ["docs", "packages", "plugins"] as const;

/**
 * Identifies an input the gate could not inspect. Callers must report this
 * failure separately from diagram errors so an incomplete check cannot pass.
 */
export class MermaidGateError extends Data.TaggedError("MermaidGateError")<{
  readonly reason: string;
  readonly path: string;
  readonly cause: unknown;
}> {}

/** Diagram text with its source file and the opening fence's 1-based line. */
export interface MermaidBlock {
  readonly file: string;
  readonly startLine: number;
  readonly body: string;
}

/** A diagram's validation or renderer failure, tied to its source location. */
export interface MermaidFailure {
  readonly block: MermaidBlock;
  readonly message: string;
}

/** Directories that never hold reviewable documentation. */
const SKIP_DIRS = new Set(["node_modules", "dist", ".git"]);

interface WalkCtx {
  readonly fs: FileSystem.FileSystem;
  readonly path: Path.Path;
  readonly out: string[];
}

interface ExtractorState {
  inFence: boolean;
  fenceLang: string | null;
  blockStart: number;
  bodyLines: string[];
  out: MermaidBlock[];
}

const MMDC_BIN = "mmdc";

/**
 * Chrome launch flags for the renderer. `mmdc` merges this file into its
 * `puppeteer.launch()` options.
 */
const PUPPETEER_CONFIG = resolve(
  dirname(fileURLToPath(import.meta.url)),
  "mermaid-puppeteer.json",
);

/** Lines of mmdc stderr kept in a failure message; the rest is stack noise. */
const STDERR_LINES_KEPT = 6;

interface MmdcRun {
  readonly exitCode: number;
  readonly stderr: string;
}

/**
 * Requires each configured root to be a directory before traversal.
 * The first inaccessible or invalid root fails in declared input order.
 */
export function requireRoots(
  fs: FileSystem.FileSystem,
  roots: readonly string[],
): Effect.Effect<void, MermaidGateError> {
  return Effect.forEach(
    roots,
    (root) =>
      fs.stat(root).pipe(
        Effect.mapError(gateError("cannot open configured root", root)),
        Effect.flatMap((info) =>
          info.type === "Directory"
            ? Effect.void
            : Effect.fail(
                new MermaidGateError({
                  reason: "configured root is not a directory",
                  path: root,
                  cause: info.type,
                }),
              ),
        ),
      ),
    { discard: true, concurrency: 1 },
  );
}

/**
 * Markdown and TypeScript files below `roots`, sorted for stable output.
 */
export const collectDiagramFiles = Effect.fn("collectDiagramFiles")(function* (
  fs: FileSystem.FileSystem,
  path: Path.Path,
  roots: readonly string[],
) {
  const ctx: WalkCtx = { fs, path, out: [] };
  for (const root of roots) {
    yield* walkInto(ctx, root);
  }
  return ctx.out.sort((left, right) => StringOps.localeCompare(right)(left));
});

/**
 * Extracts Mermaid blocks labelled relative to `workspaceRoot`.
 * An unreadable file fails the collection instead of contributing no blocks.
 */
export const collectBlocks = Effect.fn("collectBlocks")(function* (
  fs: FileSystem.FileSystem,
  path: Path.Path,
  workspaceRoot: string,
  files: readonly string[],
) {
  const out: MermaidBlock[] = [];
  for (const file of files) {
    const source = yield* fs
      .readFileString(file)
      .pipe(Effect.mapError(gateError("cannot read file", file)));
    out.push(
      ...extractMermaidBlocks(path.relative(workspaceRoot, file), source),
    );
  }
  return out;
});

/**
 * Extracts fenced Mermaid text with original source line numbers.
 * TypeScript inputs contribute only JSDoc blocks. Fences indented by four or
 * more characters and fences for other languages contribute no diagrams.
 */
export function extractMermaidBlocks(
  file: string,
  source: string,
): readonly MermaidBlock[] {
  if (file.endsWith(".ts")) {
    return [...source.matchAll(/\/\*\*[\s\S]*?\*\//gu)].flatMap((match) => {
      const startLine = source.slice(0, match.index).split("\n").length - 1;
      const body = match[0].replace(/^[ \t]*\* ?/gmu, "");
      return extractFencedBlocks(file, body, startLine);
    });
  }
  return extractFencedBlocks(file, source);
}

/**
 * Validates one diagram, returning null on success or a source-linked failure.
 * Failure to stage the input fails the Effect. Renderer and diagram errors
 * return a `MermaidFailure` so callers can collect failures across diagrams.
 */
export const lintBlock = Effect.fn("lintBlock")(function* (
  block: MermaidBlock,
  tempDir: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const slug = `${block.file.replace(/[^a-zA-Z0-9]/g, "_")}-${block.startLine}`;
  const inputPath = path.resolve(tempDir, `${slug}.mmd`);
  const outputPath = path.resolve(tempDir, `${slug}.svg`);
  yield* prepareInput(fs, tempDir, inputPath, block.body);
  const result = yield* runMmdc(inputPath, outputPath);
  yield* cleanup(fs, inputPath, outputPath);
  return interpretResult(block, result);
});

function extractFencedBlocks(
  file: string,
  source: string,
  startLine = 0,
): readonly MermaidBlock[] {
  const lines = StringOps.split(source, "\n");
  const state: ExtractorState = {
    inFence: false,
    fenceLang: null,
    blockStart: -1,
    bodyLines: [],
    out: [],
  };
  for (let i = 0; i < lines.length; i++) {
    processLine(file, lines[i] ?? "", i + startLine, state);
  }
  return state.out;
}

function processLine(
  file: string,
  line: string,
  lineIx: number,
  state: ExtractorState,
): void {
  const fenceMatch = /^([ \t]*)(```+|~~~+)([A-Za-z0-9_-]*)\s*$/.exec(line);
  if (fenceMatch) {
    handleFence(file, fenceMatch, lineIx, state);
    return;
  }
  if (state.inFence && state.fenceLang === "mermaid") {
    state.bodyLines.push(line);
  }
}

function handleFence(
  file: string,
  match: RegExpMatchArray,
  lineIx: number,
  state: ExtractorState,
): void {
  const indent = match[1] ?? "";
  const lang = match[3] ?? "";
  if (!state.inFence) {
    if (StringOps.length(indent) >= 4) {
      return;
    }
    state.inFence = true;
    state.fenceLang = lang || null;
    state.blockStart = lineIx + 1;
    state.bodyLines = [];
    return;
  }
  if (state.fenceLang === "mermaid") {
    state.out.push({
      file,
      startLine: state.blockStart,
      body: state.bodyLines.join("\n"),
    });
  }
  state.inFence = false;
  state.fenceLang = null;
}

/**
 * A failed write must stop validation because the reusable temp path may
 * still contain a previous diagram.
 */
function prepareInput(
  fs: FileSystem.FileSystem,
  tempDir: string,
  inputPath: string,
  body: string,
): Effect.Effect<void, MermaidGateError> {
  return Effect.gen(function* () {
    yield* fs
      .makeDirectory(tempDir, { recursive: true })
      .pipe(
        Effect.mapError(gateError("cannot create temp directory", tempDir)),
      );
    yield* fs
      .writeFileString(inputPath, body)
      .pipe(Effect.mapError(gateError("cannot write temp input", inputPath)));
  });
}

/**
 * Keeps stderr to distinguish diagram errors from browser launch failures.
 * Inherited stdout and drained stderr prevent a full pipe from blocking the
 * renderer.
 */
function runMmdc(
  inputPath: string,
  outputPath: string,
): Effect.Effect<
  Either.Either<MmdcRun, unknown>,
  never,
  CommandExecutor.CommandExecutor
> {
  return Effect.scoped(
    Effect.gen(function* () {
      const proc = yield* Command.make(
        MMDC_BIN,
        "--input",
        inputPath,
        "--output",
        outputPath,
        "--puppeteerConfigFile",
        PUPPETEER_CONFIG,
        "--quiet",
      ).pipe(Command.stdout("inherit"), Command.stderr("pipe"), Command.start);
      const stderr = yield* proc.stderr.pipe(
        Stream.decodeText(),
        Stream.mkString,
      );
      const exitCode = yield* proc.exitCode;
      return { exitCode, stderr } satisfies MmdcRun;
    }),
  ).pipe(Effect.either);
}

/**
 * Cleanup is best-effort because failed diagrams may leave no SVG. Each
 * validation requires a successful input write, so leftovers cannot mask it.
 */
function cleanup(
  fs: FileSystem.FileSystem,
  inputPath: string,
  outputPath: string,
): Effect.Effect<void> {
  return Effect.gen(function* () {
    yield* fs.remove(inputPath).pipe(Effect.catchAll(() => Effect.void));
    yield* fs.remove(outputPath).pipe(Effect.catchAll(() => Effect.void));
  });
}

function interpretResult(
  block: MermaidBlock,
  result: Either.Either<MmdcRun, unknown>,
): MermaidFailure | null {
  return Either.match(result, {
    onLeft: (e) => ({ block, message: `mmdc launch failed: ${String(e)}` }),
    onRight: (run) =>
      run.exitCode === 0
        ? null
        : {
            block,
            message: `mmdc exited ${String(run.exitCode)}${formatStderr(run.stderr)}`,
          },
  });
}

/**
 * Keeps leading diagnostic lines while dropping repeated renderer stack frames.
 */
function formatStderr(stderr: string): string {
  const lines = StringOps.split(stderr, "\n")
    .map(StringOps.trimEnd)
    .filter((line) => StringOps.isNonEmpty(line) && !/^\s*at\s/.test(line))
    .slice(0, STDERR_LINES_KEPT);
  if (lines.length === 0) {
    return "";
  }
  const indented = lines.map((line) => `    ${line}`).join("\n");
  return `\n${indented}`;
}

function gateError(reason: string, path: string) {
  return (cause: unknown): MermaidGateError =>
    new MermaidGateError({ reason, path, cause });
}

function walkInto(
  ctx: WalkCtx,
  dir: string,
): Effect.Effect<void, MermaidGateError> {
  return Effect.gen(function* () {
    const entries = yield* ctx.fs
      .readDirectory(dir)
      .pipe(Effect.mapError(gateError("cannot read directory", dir)));
    for (const name of entries) {
      yield* visitEntry(ctx, dir, name);
    }
  });
}

function visitEntry(
  ctx: WalkCtx,
  dir: string,
  name: string,
): Effect.Effect<void, MermaidGateError> {
  return Effect.gen(function* () {
    if (SKIP_DIRS.has(name)) {
      return;
    }
    const full = ctx.path.resolve(dir, name);
    const info = yield* ctx.fs
      .stat(full)
      .pipe(Effect.mapError(gateError("cannot stat", full)));
    if (info.type === "Directory") {
      yield* walkInto(ctx, full);
      return;
    }
    if (
      [".md", ".mdx", ".ts"].some((extension) =>
        StringOps.endsWith(extension)(name),
      )
    ) {
      ctx.out.push(full);
    }
  });
}
