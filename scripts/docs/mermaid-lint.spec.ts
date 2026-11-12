/** @file Documentation diagrams are extracted and checked with explicit input failures. */

import { FileSystem, Path } from "@effect/platform";
import { NodeContext } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { Effect, FastCheck as fc } from "effect";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- Synchronous filesystem fixtures build the trees and broken entries the gate reads.
import {
  mkdirSync,
  mkdtempSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, beforeAll } from "vitest";
import {
  collectBlocks,
  collectDiagramFiles,
  extractMermaidBlocks,
  lintBlock,
  type MermaidBlock,
  type MermaidGateError,
  requireRoots,
} from "./mermaid-lint.ts";

/** Lines of prose or diagram text: never a fence, and safe inside a JSDoc comment. */
const textLines = fc.array(fc.stringMatching(/^[A-Za-z >:-]{0,12}$/), {
  maxLength: 3,
});

/** A stretch of prose followed by one closed fence. */
interface Stretch {
  readonly prose: readonly string[];
  readonly indent: number;
  readonly marker: string;
  readonly language: string;
  readonly body: readonly string[];
}

/** A document as stretches in source order. */
const stretches = fc.array(
  fc.record<Stretch>({
    prose: textLines,
    indent: fc.nat({ max: 6 }),
    marker: fc.constantFrom("```", "~~~", "`````"),
    language: fc.oneof(
      fc.constant("mermaid"),
      fc.constantFrom("", "ts", "mermaidjs"),
    ),
    body: textLines,
  }),
);

/**
 * The source lines of `document`, whose first line is numbered `firstLine`,
 * and the diagrams a reader finds in it: the text inside each fence labelled
 * mermaid and indented by fewer than four spaces, at the fence's line.
 */
const layOut = (
  file: string,
  document: readonly Stretch[],
  firstLine: number,
) => {
  const lines: string[] = [];
  const diagrams: MermaidBlock[] = [];
  for (const { prose, indent, marker, language, body } of document) {
    lines.push(...prose);
    if (language === "mermaid" && indent < 4) {
      const startLine = firstLine + lines.length;
      diagrams.push({ file, startLine, body: body.join("\n") });
    }
    const fence = " ".repeat(indent) + marker;
    lines.push(fence + language, ...body, fence);
  }
  return { lines, diagrams };
};

describe("extractMermaidBlocks", () => {
  it("extracts exactly the mermaid fences indented by fewer than four spaces, each with its opening line and inner text", () => {
    fc.assert(
      fc.property(stretches, (document) => {
        const { lines, diagrams } = layOut("doc.md", document, 1);
        expect(extractMermaidBlocks("doc.md", lines.join("\n"))).toEqual(
          diagrams,
        );
      }),
    );
  });

  it("extracts from TypeScript only the fences inside a JSDoc comment, numbered by source line", () => {
    fc.assert(
      fc.property(stretches, stretches, (code, comment) => {
        const loose = layOut("flow.ts", code, 1).lines;
        const documented = layOut("flow.ts", comment, loose.length + 2);
        const source = [
          ...loose,
          "/**",
          ...documented.lines.map((line) => ` * ${line}`),
          " */",
        ];
        expect(extractMermaidBlocks("flow.ts", source.join("\n"))).toEqual(
          documented.diagrams,
        );
      }),
    );
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

/**
 * Runs a gate step against the real filesystem, yielding its value, or the
 * path it could not inspect.
 */
const runGate = <A>(
  body: (
    fs: FileSystem.FileSystem,
    path: Path.Path,
  ) => Effect.Effect<A, MermaidGateError>,
) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    return yield* body(fs, path);
  }).pipe(
    Effect.provide(NodeContext.layer),
    Effect.mapError((error) => error.path),
    Effect.merge,
  );

describe("requireRoots", () => {
  it.live.prop(
    "accepts roots exactly when every one is a directory, and otherwise names the first that is a file or missing",
    [fc.array(fc.constantFrom("directory", "file", "missing"))],
    ([kinds]) =>
      Effect.gen(function* () {
        const base = mkdtempSync(join(sandbox, "roots-"));
        const roots = kinds.map((kind, index) => {
          const root = join(base, `root-${String(index)}`);
          if (kind === "directory") {
            mkdirSync(root);
          }
          if (kind === "file") {
            writeFileSync(root, diagramBlock);
          }
          return root;
        });

        const rejected = kinds.findIndex((kind) => kind !== "directory");

        expect(yield* runGate((fs) => requireRoots(fs, roots))).toBe(
          rejected === -1 ? undefined : roots[rejected],
        );
      }),
  );

  /** The report prints the cause to say why the root could not be reached. */
  it.live(
    "keeps the filesystem's own failure as the cause when a root cannot be opened",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const missing = join(sandbox, "absent-root");

        const error = yield* Effect.flip(requireRoots(fs, [missing]));

        expect(error.cause).toEqual(yield* Effect.flip(fs.stat(missing)));
      }).pipe(Effect.provide(NodeContext.layer)),
  );
});

describe("collectDiagramFiles", () => {
  /** Directories the gate never looks inside. */
  const skipped = ["node_modules", "dist", ".git"];
  const visited = (directories: readonly string[]) =>
    !directories.some((name) => skipped.includes(name));

  /** Where an entry of a generated tree sits below its root. */
  const directories = fc.array(
    fc.constantFrom("nested", "packages", ...skipped),
    { maxLength: 2 },
  );

  /**
   * The two roots are passed in the reverse of their path order, so the
   * expected order never depends on the order the filesystem lists entries in.
   */
  it.live.prop(
    "collects exactly the .md, .mdx and .ts files outside node_modules, dist and .git, sorted by path whatever order the roots are given in",
    [
      fc.uniqueArray(
        fc.record({
          root: fc.constantFrom("docs", "plugins"),
          under: directories,
          name: fc.constantFrom("a.md", "b.mdx", "c.ts", "d.tsx", "e.txt"),
        }),
        { selector: ({ root, under, name }) => join(root, ...under, name) },
      ),
    ],
    ([files]) =>
      Effect.gen(function* () {
        const base = mkdtempSync(join(sandbox, "tree-"));
        const roots = [join(base, "plugins"), join(base, "docs")];
        for (const root of roots) {
          mkdirSync(root);
        }
        for (const { root, under, name } of files) {
          mkdirSync(join(base, root, ...under), { recursive: true });
          writeFileSync(join(base, root, ...under, name), diagramBlock);
        }

        expect(
          yield* runGate((fs, path) => collectDiagramFiles(fs, path, roots)),
        ).toEqual(
          files
            .filter(
              ({ under, name }) =>
                visited(under) && /\.(md|mdx|ts)$/.test(name),
            )
            .map(({ root, under, name }) => join(base, root, ...under, name))
            .sort((left, right) => left.localeCompare(right)),
        );
      }),
  );

  it.live.prop(
    "fails naming a dangling link, unless the link sits under node_modules, dist or .git",
    [directories],
    ([under]) =>
      Effect.gen(function* () {
        const root = mkdtempSync(join(sandbox, "dangling-"));
        mkdirSync(join(root, ...under), { recursive: true });
        const dangling = join(root, ...under, "dangling.md");
        symlinkSync(join(sandbox, "no-such-target"), dangling);

        expect(
          yield* runGate((fs, path) => collectDiagramFiles(fs, path, [root])),
        ).toEqual(visited(under) ? dangling : []);
      }),
  );

  it.live(
    "fails instead of yielding no files when a directory cannot be listed",
    () =>
      Effect.gen(function* () {
        const absent = join(sandbox, "absent-tree");
        expect(
          yield* runGate((fs, path) => collectDiagramFiles(fs, path, [absent])),
        ).toBe(absent);
      }),
  );
});

describe("collectBlocks", () => {
  it.live.prop(
    "labels each file's diagrams by its path below the workspace root, in file order, and fails naming the first unreadable file",
    [fc.array(fc.option(fc.nat({ max: 2 })), { maxLength: 5 })],
    ([diagramCounts]) =>
      Effect.gen(function* () {
        const root = mkdtempSync(join(sandbox, "blocks-"));
        mkdirSync(join(root, "guides"));
        const documents = diagramCounts.map((count, index) => ({
          count,
          label: join("guides", `doc-${String(index)}.md`),
        }));
        for (const { count, label } of documents) {
          if (count !== null) {
            writeFileSync(join(root, label), diagramBlock.repeat(count));
          }
        }
        const files = documents.map(({ label }) => join(root, label));
        const unreadable = documents.find(({ count }) => count === null);

        expect(
          yield* runGate((fs, path) => collectBlocks(fs, path, root, files)),
        ).toEqual(
          unreadable
            ? join(root, unreadable.label)
            : documents.flatMap(({ count, label }) =>
                Array<unknown>(count ?? 0).fill(
                  expect.objectContaining({ file: label }),
                ),
              ),
        );
      }),
  );
});

describe("lintBlock", () => {
  const blockUnderTest = {
    file: "doc.md",
    startLine: 1,
    body: "flowchart TD\n  A --> B",
  };

  /** Staging failures do not require a browser. */
  const stage = (tempDir: string) =>
    lintBlock(blockUnderTest, tempDir).pipe(
      Effect.provide(NodeContext.layer),
      Effect.flip,
    );

  it.live("fails when the temp directory cannot be created", () =>
    Effect.gen(function* () {
      const tempDir = join(sandbox, "temp-is-a-file");
      writeFileSync(tempDir, "not a directory");
      expect((yield* stage(tempDir)).path).toBe(tempDir);
    }),
  );
});
