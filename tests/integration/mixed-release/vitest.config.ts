/** @file Vitest configuration for the adjacent-release process suite. */
import { defineConfig } from "vitest/config";

export default defineConfig({
  resolve: { conditions: ["sharness-test", "sharness-internal"] },
  ssr: { resolve: { conditions: ["sharness-test", "sharness-internal"] } },
  test: {
    include: [
      "tests/integration/mixed-release/mixed-release.integration.spec.ts",
      "tests/integration/mixed-release/launcher-upgrades.integration.spec.ts",
    ],
    maxWorkers: 1,
    passWithNoTests: false,
  },
});
