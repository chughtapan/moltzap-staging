/** @file Vitest configuration for the package integration suite. */
import { defineConfig } from "vitest/config";

export default defineConfig({
  resolve: { conditions: ["sharness-internal", "sharness-test"] },
  ssr: { resolve: { conditions: ["sharness-internal", "sharness-test"] } },
  test: {
    include: ["src/**/*.integration.spec.ts"],
    passWithNoTests: false,
  },
});
