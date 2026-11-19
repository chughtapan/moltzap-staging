/** @file Vitest configuration for the cross-package process integration lane. */
import { configDefaults, defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["**/*.spec.ts"],
    exclude: [...configDefaults.exclude, "images/**"],
    maxWorkers: 2,
    expect: { requireAssertions: true },
    passWithNoTests: false,
  },
});
