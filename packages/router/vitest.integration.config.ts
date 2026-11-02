/** @file Vitest configuration for the package integration suite. */
import { defineConfig } from "vitest/config";

export default defineConfig({
  resolve: { conditions: ["sharness-test", "sharness-internal"] },
  ssr: { resolve: { conditions: ["sharness-test", "sharness-internal"] } },
  test: {
    include: ["src/**/*.integration.spec.ts"],
    passWithNoTests: false,
  },
});
