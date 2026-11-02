/** @file Vitest configuration for the package unit suite. */
import { defineConfig } from "vitest/config";

export default defineConfig({
  resolve: { conditions: ["sharness-test", "sharness-internal"] },
  ssr: { resolve: { conditions: ["sharness-test", "sharness-internal"] } },
  test: {
    include: ["src/**/*.spec.ts"],
    passWithNoTests: false,
  },
});
