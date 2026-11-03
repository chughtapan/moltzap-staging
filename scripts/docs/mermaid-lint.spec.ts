/** @file Documentation diagrams are extracted and checked with explicit input failures. */

import { FileSystem, Path } from "@effect/platform";
import { NodeContext } from "@effect/platform-node";
import { Effect, Either } from "effect";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- Synchronous filesystem fixtures exercise committed inputs and filesystem permission failures.
import {
  chmodSync,
  mkdirSync,
  mkdtempSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  collectBlocks,
  collectDiagramFiles,
  extractMermaidBlocks,
  lintBlock,
  type MermaidGateError,
  requireRoots,
} from "./mermaid-lint.ts";

describe("extractMermaidBlocks", () => {
  // @agent-code-guard/regression-only: fixed syntax and gate failure fixtures exercise distinct regressions.
  it("returns empty list when the source has no fenced blocks", () => {
    expect(extractMermaidBlocks("doc.md", "Plain prose.\n")).toEqual([]);
  });

  it("extracts JSDoc diagrams with source line numbers and indentation", () => {
    const source = [
      "const value = 1;",
      "/**",
      " * Flow.",
      " * ```mermaid",
      " * sequenceDiagram",
      " *   A->>B: hi",
      " * ```",
      " */",
    ].join("\n");
    expect(extractMermaidBlocks("flow.ts", source)).toEqual([
      { file: "flow.ts", startLine: 4, body: "sequenceDiagram\n  A->>B: hi" },
    ]);
  });

  it("finds a single triple-backtick mermaid block", () => {
    const source = [
      "# Heading",
      "",
      "```mermaid",
      "sequenceDiagram",
      "  A->>B: hi",
      "```",
      "",
    ].join("\n");
    expect(extractMermaidBlocks("doc.md", source)).toEqual([
      {
        file: "doc.md",
        startLine: 3,
        body: "sequenceDiagram\n  A->>B: hi",
      },
    ]);
  });

  it("finds a triple-tilde mermaid block", () => {
    const source = ["~~~mermaid", "flowchart TD", "  X --> Y", "~~~", ""].join(
      "\n",
    );
    expect(extractMermaidBlocks("doc.md", source)).toEqual([
      {
        file: "doc.md",
        startLine: 1,
        body: "flowchart TD\n  X --> Y",
      },
    ]);
  });

  it("ignores fences indented by 4+ spaces", () => {
    const source = [
      "Snippet inline:",
      "",
      "    ```mermaid",
      "    flowchart TD",
      "    ```",
      "",
    ].join("\n");
    expect(extractMermaidBlocks("doc.md", source)).toEqual([]);
  });

  it("ignores blocks of other languages", () => {
    const source = [
      "```ts",
      "const mermaid = 'something';",
      "```",
      "",
      "```mermaid",
      "sequenceDiagram",
      "```",
      "",
    ].join("\n");
    const blocks = extractMermaidBlocks("doc.md", source);
    expect(blocks).toHaveLength(1);
    // eslint-disable-next-line agent-code-guard/no-hardcoded-assertion-literals -- The expected text is the observable output for this fixed input fixture.
    expect(blocks[0]?.startLine).toBe(5);
  });

  it("collects multiple mermaid blocks with correct line numbers", () => {
    const source = [
      "```mermaid",
      "A",
      "```",
      "",
      "```mermaid",
      "B",
      "```",
      "",
    ].join("\n");
    const blocks = extractMermaidBlocks("doc.md", source);
    expect(blocks).toHaveLength(2);
    expect(blocks[0]?.startLine).toBe(1);
    // eslint-disable-next-line agent-code-guard/no-hardcoded-assertion-literals -- The expected text is the observable output for this fixed input fixture.
    expect(blocks[1]?.startLine).toBe(5);
  });
});

const diagramBlock = [
  "```mermaid",
  "flowchart TD",
  "  A --> B",
  "```",
  "",
].join("\n");

let sandbox: string;

beforeAll(() => {
  sandbox = mkdtempSync(join(tmpdir(), "mermaid-gate-"));
});

afterAll(() => {
  rmSync(sandbox, { recursive: true, force: true });
});

const runGate = <A>(
  body: (
    fs: FileSystem.FileSystem,
    path: Path.Path,
  ) => Effect.Effect<A, MermaidGateError>,
) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      return yield* body(fs, path);
    }).pipe(Effect.provide(NodeContext.layer), Effect.either),
  );

const failure = <A>(result: Either.Either<A, MermaidGateError>) => {
  return Either.match(result, {
    onLeft: (error) => error,
    onRight: () => {
      throw new Error("expected the gate to fail, but it succeeded");
    },
  });
};

describe("requireRoots", () => {
  // eslint-disable-next-line agent-code-guard/async-keyword -- Vitest awaits this Promise boundary; the implementation under test remains an Effect.
  it("accepts a real directory", async () => {
    const dir = join(sandbox, "real-root");
    mkdirSync(dir, { recursive: true });
    const result = await runGate((fs) => requireRoots(fs, [dir]));
    expect(
      Either.match(result, { onLeft: () => false, onRight: () => true }),
    ).toBe(true);
  });

  /** The underlying cause must explain why the named root could not be reached. */
  // eslint-disable-next-line agent-code-guard/async-keyword -- Vitest awaits this Promise boundary; the implementation under test remains an Effect.
  it("fails and names a root that does not exist", async () => {
    const missing = join(sandbox, "absent-root");
    const error = failure(await runGate((fs) => requireRoots(fs, [missing])));
    // eslint-disable-next-line agent-code-guard/no-hardcoded-assertion-literals -- The expected text is the observable output for this fixed input fixture.
    expect(error._tag).toBe("MermaidGateError");
    expect(error.path).toBe(missing);
    expect(String(error.cause)).toContain(missing);
  });

  // eslint-disable-next-line agent-code-guard/async-keyword -- Vitest awaits this Promise boundary; the implementation under test remains an Effect.
  it("fails when a root is a file rather than a directory", async () => {
    const file = join(sandbox, "root-is-a-file.md");
    writeFileSync(file, diagramBlock);
    const error = failure(await runGate((fs) => requireRoots(fs, [file])));
    // eslint-disable-next-line agent-code-guard/no-hardcoded-assertion-literals -- The expected text is the observable output for this fixed input fixture.
    expect(error.reason).toContain("not a directory");
    expect(error.path).toBe(file);
  });
});

describe("collectDiagramFiles", () => {
  // eslint-disable-next-line agent-code-guard/async-keyword -- Vitest awaits this Promise boundary; the implementation under test remains an Effect.
  it("descends subdirectories and skips node_modules", async () => {
    const root = join(sandbox, "tree");
    mkdirSync(join(root, "nested"), { recursive: true });
    mkdirSync(join(root, "node_modules"), { recursive: true });
    mkdirSync(join(root, "packages"), { recursive: true });
    writeFileSync(
      join(root, "packages", "flow.ts"),
      "/**\n * ```mermaid\n * flowchart TD\n * A --> B\n * ```\n */",
    );
    writeFileSync(join(root, "top.md"), diagramBlock);
    writeFileSync(join(root, "nested", "deep.mdx"), diagramBlock);
    writeFileSync(join(root, "node_modules", "vendor.md"), diagramBlock);

    const result = await runGate((fs, path) =>
      collectDiagramFiles(fs, path, [root]),
    );
    const value = Either.getOrThrowWith(result, (error) => error);
    expect(value).toEqual([
      join(root, "nested", "deep.mdx"),
      join(root, "packages", "flow.ts"),
      join(root, "top.md"),
    ]);
  });

  // eslint-disable-next-line agent-code-guard/async-keyword -- Vitest awaits this Promise boundary; the implementation under test remains an Effect.
  it("fails instead of yielding no files when a directory cannot be listed", async () => {
    const absent = join(sandbox, "absent-tree");
    const error = failure(
      await runGate((fs, path) => collectDiagramFiles(fs, path, [absent])),
    );
    // eslint-disable-next-line agent-code-guard/no-hardcoded-assertion-literals -- The expected text is the observable output for this fixed input fixture.
    expect(error.reason).toContain("cannot read directory");
    expect(error.path).toBe(absent);
  });

  /** A dangling entry fails during per-entry stat. */
  // eslint-disable-next-line agent-code-guard/async-keyword -- Vitest awaits this Promise boundary; the implementation under test remains an Effect.
  it("fails on an entry that cannot be stat'ed", async () => {
    const root = join(sandbox, "dangling-tree");
    mkdirSync(root, { recursive: true });
    const dangling = join(root, "dangling.md");
    symlinkSync(join(sandbox, "no-such-target"), dangling);
    const error = failure(
      await runGate((fs, path) => collectDiagramFiles(fs, path, [root])),
    );
    // eslint-disable-next-line agent-code-guard/no-hardcoded-assertion-literals -- The expected text is the observable output for this fixed input fixture.
    expect(error.reason).toBe("cannot stat");
    expect(error.path).toBe(dangling);
  });

  /** Root ignores mode bits, so unreadable fixtures require an unprivileged user. */
  const unreadable = process.getuid?.() === 0 ? it.skip : it;
  unreadable(
    "fails on a subtree that exists but cannot be opened",
    // eslint-disable-next-line agent-code-guard/async-keyword -- Vitest awaits this Promise boundary; the implementation under test remains an Effect.
    async () => {
      const root = join(sandbox, "locked-tree");
      const locked = join(root, "locked");
      mkdirSync(locked, { recursive: true });
      writeFileSync(join(locked, "hidden.md"), diagramBlock);
      chmodSync(locked, 0o000);
      try {
        const error = failure(
          await runGate((fs, path) => collectDiagramFiles(fs, path, [root])),
        );
        expect(error.path).toBe(locked);
      } finally {
        chmodSync(locked, 0o755);
      }
    },
  );
});

describe("collectBlocks", () => {
  // eslint-disable-next-line agent-code-guard/async-keyword -- Vitest awaits this Promise boundary; the implementation under test remains an Effect.
  it("extracts blocks labelled relative to the workspace root", async () => {
    const root = join(sandbox, "blocks");
    mkdirSync(root, { recursive: true });
    const file = join(root, "doc.md");
    writeFileSync(file, diagramBlock);

    const result = await runGate((fs, path) =>
      collectBlocks(fs, path, root, [file]),
    );
    const value = Either.getOrThrowWith(result, (error) => error);
    expect(value).toHaveLength(1);
    // eslint-disable-next-line agent-code-guard/no-hardcoded-assertion-literals -- The expected text is the observable output for this fixed input fixture.
    expect(value[0]?.file).toBe("doc.md");
  });

  // eslint-disable-next-line agent-code-guard/async-keyword -- Vitest awaits this Promise boundary; the implementation under test remains an Effect.
  it("fails instead of yielding no blocks when a file cannot be read", async () => {
    const absent = join(sandbox, "absent-doc.md");
    const error = failure(
      await runGate((fs, path) => collectBlocks(fs, path, sandbox, [absent])),
    );
    // eslint-disable-next-line agent-code-guard/no-hardcoded-assertion-literals -- The expected text is the observable output for this fixed input fixture.
    expect(error.reason).toContain("cannot read file");
    expect(error.path).toBe(absent);
  });
});

describe("lintBlock", () => {
  const blockUnderTest = {
    file: "doc.md",
    startLine: 1,
    body: "flowchart TD\n  A --> B",
  };

  /** Staging failures do not require a browser. */
  const stage = (tempDir: string) =>
    Effect.runPromise(
      lintBlock(blockUnderTest, tempDir).pipe(
        Effect.provide(NodeContext.layer),
        Effect.either,
      ),
    );

  // eslint-disable-next-line agent-code-guard/async-keyword -- Vitest awaits this Promise boundary; the implementation under test remains an Effect.
  it("fails when the temp directory cannot be created", async () => {
    const tempDir = join(sandbox, "temp-is-a-file");
    writeFileSync(tempDir, "not a directory");
    const error = failure(await stage(tempDir));
    // eslint-disable-next-line agent-code-guard/no-hardcoded-assertion-literals -- The expected text is the observable output for this fixed input fixture.
    expect(error.reason).toBe("cannot create temp directory");
    expect(error.path).toBe(tempDir);
  });
});
