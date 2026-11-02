/** @file Vitest boundary for root-owned documentation tooling. */

import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

const SCRIPT_DIR = dirname(fileURLToPath(import.meta.url));
const WORKSPACE_ROOT = resolve(SCRIPT_DIR, "..", "..");

const config = defineConfig({
  root: WORKSPACE_ROOT,
  test: {
    include: [
      "scripts/docs/__tests__/mermaid-extract.spec.ts",
      "scripts/docs/__tests__/mermaid-gate.spec.ts",
    ],
  },
});

export default config;
