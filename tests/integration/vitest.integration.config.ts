/** @file Vitest configuration for the cross-package process integration lane. */
import { configDefaults, defineConfig } from "vitest/config";

export default defineConfig({
  resolve: { conditions: ["sharness-test"] },
  ssr: { resolve: { conditions: ["sharness-test"] } },
  test: {
    include: ["**/*.spec.ts"],
    exclude: [...configDefaults.exclude, "images/**"],
    maxWorkers: 2,
    passWithNoTests: false,
  },
});
