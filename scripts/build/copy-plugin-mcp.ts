/** @file Copies the compiled shared MCP client into each plugin artifact. */
import { cpSync } from "node:fs";
import { resolve } from "node:path";
cpSync(
  new URL("../../plugins/shared/dist", import.meta.url),
  resolve("dist/mcp"),
  {
    recursive: true,
    filter: (source) => !source.endsWith("tsconfig.tsbuildinfo"),
  },
);
