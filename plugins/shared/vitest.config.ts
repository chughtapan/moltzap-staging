/** @file Shared client tests exercise the protocol server and native acceptance boundary. */
import { defineConfig } from "vitest/config";
import { fileURLToPath } from "node:url";
export default defineConfig({
  resolve: {
    alias: {
      "@moltzap/social-harness": fileURLToPath(
        new URL("../../packages/social-harness/src/index.ts", import.meta.url),
      ),
    },
  },
  test: { include: ["*.test.ts"] },
});
