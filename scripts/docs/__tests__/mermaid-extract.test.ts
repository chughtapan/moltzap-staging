/** @file Unit tests for workspace Mermaid block discovery and extraction. */

import { describe, expect, it } from "vitest";
import { extractMermaidBlocks } from "../mermaid-lint.ts";

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
