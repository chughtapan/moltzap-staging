/** @file Shared client tests exercise the protocol server and native acceptance boundary. */
import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";
export default defineConfig({
  resolve: {
    alias: {
      "@moltzap/social-harness": fileURLToPath(
        new URL("../../packages/social-harness/src/index.ts", import.meta.url),
      ),
    },
  },
  test: { include: ["*.spec.ts"] },
});
