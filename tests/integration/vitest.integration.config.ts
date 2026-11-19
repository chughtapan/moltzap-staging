/** @file Vitest configuration for the cross-package process integration lane. */
import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["**/*.spec.ts"],
    maxWorkers: 2,
    expect: { requireAssertions: true },
    passWithNoTests: false,
  },
});
